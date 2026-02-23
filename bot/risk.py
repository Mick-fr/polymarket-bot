"""
Module de contrôle des risques.
Vérifie chaque signal AVANT exécution pour protéger le capital.

Règles implémentées :
  1. Kill switch global  (exemption : SELL de liquidation d'une position existante)
  2. Taille d'ordre max (config)
  3. Solde suffisant
  4. Réserve minimale (10% ou 2 USDC)
  5. Perte quotidienne max
  6. Confiance minimale du signal
  7. [OBI] Fractional sizing : expo nette max par marché (configurable via BOT_MAX_EXPOSURE_PCT)
  8. [OBI] Inventory skewing : ratio > 0.6 → Ask only, ratio = 1.0 → liquidation
  9. [OBI] Circuit breaker global : valeur_portfolio < 90% du High Water Mark journalier

Circuit breaker — Valeur Portfolio :
  Le drawdown est calculé sur la VALEUR TOTALE DU PORTEFEUILLE :
    Portfolio = USDC liquides + valeur inventaire (shares × avg_price d'entrée)
  Cela évite les faux déclenchements lors d'un BUY normal :
    ex. 36 USDC → achète 10 shares @ 0.35 → USDC = 32.5, inventaire = 3.5 → portfolio = 36.
  Le circuit breaker ne se déclenche QUE si la valeur totale baisse réellement
  (perte de valeur des shares, mauvais fills, etc.).

Exemption de liquidation :
  Quand le kill switch est actif (déclenché manuellement ou par circuit breaker),
  les ordres SELL sur des positions existantes (qty_held > 0) restent autorisés.
  Objectif : ne jamais bloquer la réduction d'exposition en cas de crise.
  Les vérifications bypassées : kill switch, circuit breaker, réserve minimale,
  perte quotidienne, confiance, fractional sizing, inventory skewing.
  Les vérifications maintenues : taille max, solde suffisant (SELL ne coûte rien).
"""

import logging
from dataclasses import dataclass

from bot.config import BotConfig
from bot.strategy import Signal
from db.database import Database

logger = logging.getLogger("bot.risk")

# Constantes OBI risk — Set B (Balanced)
# NOTE : MAX_NET_EXPOSURE_PCT est maintenant lu depuis config.max_exposure_pct
# (variable d'env BOT_MAX_EXPOSURE_PCT, défaut 20%).
# La constante ci-dessous n'est plus utilisée — conservée pour référence.
_MAX_NET_EXPOSURE_PCT_LEGACY = 0.08
INVENTORY_SKEW_WARN    = 0.60   # Ratio > 60% → Ask only, cancel bids (ex: 70%)
INVENTORY_SKEW_LIQ     = 1.00   # Ratio = 100% → liquidation unilaterale
CIRCUIT_BREAKER_PCT    = 0.10   # Declenche si solde chute de 10% vs HWM


@dataclass
class RiskVerdict:
    """Résultat d'une vérification de risque."""
    approved:  bool
    reason:    str
    action:    str = "none"   # 'none' | 'cancel_bids' | 'liquidate' | 'kill_switch'


class RiskManager:
    """Contrôle des risques pré-exécution avec règles OBI étendues."""

    def __init__(self, config: BotConfig, db: Database):
        self.config = config
        self.db = db
        # En paper trading, le circuit breaker est désactivé pour ne pas
        # interrompre la simulation à cause des positions initiales corrompues
        self._paper_trading = getattr(config, "paper_trading", False)
        # 2026 V7.3.9 — mutable override for frozen config field
        self._max_exposure_pct: float = getattr(config, "max_exposure_pct", 0.20)
        self._last_agg_level: str = ""

    # 2026 V7.3.9 OVERRIDE CONSTANTES DB — rechargement dynamique chaque cycle
    AGGRESSIVITY_MAP = {
        "Conservative":     {"max_exposure_pct": 0.18},
        "Balanced":         {"max_exposure_pct": 0.25},
        "Aggressive":       {"max_exposure_pct": 0.35},
        "Very Aggressive":  {"max_exposure_pct": 0.45},
    }

    def reload_aggressivity(self):
        """Relit aggressivity_level depuis la DB et met à jour _max_exposure_pct.
        Utilise un attribut mutable car BotConfig est frozen."""
        try:
            level = self.db.get_aggressivity_level()
            params = self.AGGRESSIVITY_MAP.get(level)
            if params:
                self._max_exposure_pct = params["max_exposure_pct"]
            else:
                # Custom / AI-applied: read from DB
                db_expo = self.db.get_config("max_net_exposure_pct")
                if db_expo is not None:
                    self._max_exposure_pct = db_expo
            # Log only on change
            if level != self._last_agg_level:
                logger.info("[LIVE CONFIG] RiskManager: level=%s | max_expo=%.0f%%",
                            level, self._max_exposure_pct * 100)
                self._last_agg_level = level
        except Exception as e:
            logger.warning("[Config] reload_aggressivity error: %s", e)

    def check(self, signal: Signal, current_balance: float,
              portfolio_value: float = 0.0) -> RiskVerdict:
        """
        Vérifie si un signal est autorisé.
        Args:
            signal          : le signal à évaluer
            current_balance : solde USDC disponible (pour sizing, réserve, suffisance)
            portfolio_value : valeur totale (USDC + inventaire) pour le circuit breaker.
                              Si 0.0, utilise current_balance en fallback.
        Retourne RiskVerdict(approved, reason, action).
        """
        # Valeur utilisée pour le circuit breaker : portfolio total si fourni, sinon USDC seul
        cb_value = portfolio_value if portfolio_value > 0 else current_balance

        # ── 0. Exemption de liquidation ───────────────────────────────────────
        # Un SELL sur une position existante est toujours autorisé, même si le
        # kill switch est actif (circuit breaker ou manuel).
        # Raison : bloquer les SELL en cas de crise empire l'exposition nette.
        # On vérifie uniquement que qty_held > 0 pour éviter les faux SELL.
        if signal.side == "sell":
            qty_held = self.db.get_position(signal.token_id)
            if qty_held > 0:
                logger.info(
                    "[RiskManager] Exemption liquidation: SELL %s qty_held=%.2f"
                    " (kill_switch=%s)",
                    signal.token_id[:16], qty_held, self.db.get_kill_switch(),
                )
                return RiskVerdict(True, f"Liquidation autorisée: qty_held={qty_held:.2f}", "none")

        # ── 1. Kill switch ────────────────────────────────────────────────────
        if self.db.get_kill_switch():
            return RiskVerdict(False, "Kill switch activé", "none")

        # ── 2. Circuit breaker global (High Water Mark) ───────────────────────
        # Basé sur la valeur totale du portfolio (USDC + inventaire), PAS uniquement sur l'USDC.
        # Évite les faux déclenchements lors d'un BUY normal (argent converti en shares, pas perdu).
        # Désactivé en paper trading (le HWM peut être corrompu depuis une session précédente).
        if not self._paper_trading:
            cb = self._check_circuit_breaker(cb_value)
            if cb is not None:
                return cb

        # ── 3. Taille d'ordre max ─────────────────────────────────────────────
        order_cost = self._compute_order_cost(signal)
        # Tolérance 1% pour éviter les faux rejets dus à la précision flottante
        # (ex: size=10.41 × price=0.48 = 4.9968 → arrondi à 5.00, mais float peut donner 5.00000001)
        if order_cost > self.config.max_order_size * 1.01:
            return RiskVerdict(
                False,
                f"Ordre trop gros: {order_cost:.2f} USDC > max {self.config.max_order_size:.2f}",
                "none",
            )

        # ── 4. Solde suffisant ────────────────────────────────────────────────
        if order_cost > current_balance:
            return RiskVerdict(
                False,
                f"Solde insuffisant: {current_balance:.2f} USDC < {order_cost:.2f} requis",
                "none",
            )

        # ── 5. Réserve minimale (10% ou 2 USDC) ──────────────────────────────
        min_reserve = max(current_balance * 0.10, 2.0)
        if current_balance - order_cost < min_reserve:
            return RiskVerdict(
                False,
                f"Réserve insuffisante: resterait {current_balance - order_cost:.2f} < {min_reserve:.2f} USDC",
                "none",
            )

        # ── 6. Perte quotidienne max ──────────────────────────────────────────
        daily_loss = self.db.get_daily_loss()
        if daily_loss >= self.config.max_daily_loss:
            return RiskVerdict(
                False,
                f"Perte quotidienne max atteinte: {daily_loss:.2f} >= {self.config.max_daily_loss:.2f}",
                "none",
            )

        # ── 7. [SUPPRIMÉ] Confiance minimale ────────────────────────────────────
        # Le filtre confidence était inopérant : tous les signaux OBI ont une
        # confidence entre 0.70 et 1.00 (min = abs(0.20) + 0.50 = 0.70 > seuil 0.50).
        # Jamais un signal n'a été rejeté par cette règle. Supprimé pour clarté.

        # ── 8. Fractional sizing : expo nette max 5% du solde ─────────────────
        frac = self._check_fractional_sizing(signal, current_balance, order_cost)
        if frac is not None:
            return frac

        # ── 9. Inventory skewing ──────────────────────────────────────────────
        inv = self._check_inventory_skewing(signal, current_balance)
        if inv is not None:
            return inv

        logger.debug(
            "Risque OK: %s %s @ %s – coût %.2f USDC, perte jour %.2f",
            signal.side.upper(), signal.token_id[:16],
            signal.price or "market", order_cost, daily_loss,
        )
        return RiskVerdict(True, "Toutes les vérifications passées", "none")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _compute_order_cost(self, signal: Signal) -> float:
        if signal.order_type == "market":
            return signal.size
        return signal.size * (signal.price or 0.0)

    def _check_circuit_breaker(self, portfolio_value: float) -> RiskVerdict | None:
        """
        Circuit breaker global : si la VALEUR TOTALE DU PORTFOLIO chute de 10% vs le HWM
        journalier, déclenche le kill switch.

        portfolio_value = USDC liquides + valeur inventaire (shares × avg_price).
        Ce calcul évite les faux déclenchements lors d'un BUY normal :
          avant BUY : 36 USDC → après BUY : 32 USDC + 10 shares × 0.40 = 36 USDC (stable).
        Le CB ne se déclenche que si la valeur totale baisse réellement (perte sur les shares).
        """
        hwm = self.db.get_high_water_mark()
        if hwm <= 0 or portfolio_value <= 0:
            return None   # Pas encore de HWM (premier démarrage)

        drawdown = (hwm - portfolio_value) / hwm
        if drawdown >= CIRCUIT_BREAKER_PCT:
            logger.critical(
                "[RiskManager] CIRCUIT BREAKER: portfolio %.2f USDC = -%.1f%% vs HWM %.2f → kill switch",
                portfolio_value, drawdown * 100, hwm,
            )
            self.db.set_kill_switch(True)
            self.db.add_log(
                "CRITICAL", "risk",
                f"Circuit breaker déclenché: portfolio={portfolio_value:.2f} HWM={hwm:.2f} "
                f"drawdown={drawdown*100:.1f}%",
            )
            return RiskVerdict(
                False,
                f"Circuit breaker: drawdown {drawdown*100:.1f}% >= {CIRCUIT_BREAKER_PCT*100:.0f}%",
                "kill_switch",
            )
        return None

    def _get_current_exposure_usdc(self, token_id: str, mid_price: float) -> float:
        """Calcule l'exposition actuelle en USDC pour un token.

        Priorité de valorisation (du plus fiable au moins fiable) :
          1. mid_price du Signal courant (carnet d'ordres CLOB en temps réel)
          2. current_mid stocké en DB (mis à jour chaque cycle par strategy.py)
          3. avg_price DB clamped [0.01, 0.99] (prix d'entrée, dernier recours)
          4. Estimation conservatrice 0.50 (si avg_price corrompu)

        Les niveaux 2-4 s'appliquent quand le token n'est PAS dans les marchés
        éligibles ce cycle (pas de Signal → mid_price = 0).
        """
        qty_held = self.db.get_position(token_id)
        if qty_held <= 0:
            return 0.0

        # ── 1. Signal mid_price (temps réel, le plus fiable) ──────────────────
        if mid_price > 0.0:
            return qty_held * mid_price

        # ── 2. current_mid DB (mis à jour chaque cycle par strategy.py) ───────
        pos = self.db.get_position_row(token_id)
        if pos:
            stored_mid = float(pos.get("current_mid") or 0)
            if 0.01 <= stored_mid <= 0.99:
                logger.debug(
                    "[RiskManager] %s: mid_price absent, fallback current_mid_db=%.4f",
                    token_id[:16], stored_mid,
                )
                return qty_held * stored_mid

        # ── 3/4. avg_price DB avec garde-fou [0.01, 0.99] ─────────────────────
        raw = self.db.get_position_usdc(token_id)
        avg_price = raw / qty_held if qty_held > 0 else 0.0
        if avg_price < 0.01 or avg_price > 0.99:
            # avg_price corrompu → estimation conservatrice à 0.50
            logger.debug(
                "[RiskManager] %s: avg_price=%.4f hors bornes → fallback 0.50",
                token_id[:16], avg_price,
            )
            return qty_held * 0.50
        return raw

    def _check_fractional_sizing(self, signal: Signal, balance: float,
                                  order_cost: float) -> RiskVerdict | None:
        """
        L'exposition nette en USDC sur un marché ne doit pas dépasser
        config.max_exposure_pct * balance (défaut : 20%, configurable via BOT_MAX_EXPOSURE_PCT).
        Utilise le mid_price actuel du signal pour valoriser l'exposition existante.
        """
        max_exposure_pct = self._max_exposure_pct
        max_exposure = balance * max_exposure_pct
        # Exposition actuelle au prix de marché (mid), pas au prix d'achat
        current_exposure_usdc = self._get_current_exposure_usdc(
            signal.token_id, getattr(signal, "mid_price", 0.0)
        )

        # Delta USDC de ce nouvel ordre
        delta = order_cost if signal.side == "buy" else -order_cost
        net_exposure = abs(current_exposure_usdc + delta)

        if net_exposure > max_exposure:
            return RiskVerdict(
                False,
                f"Fractional sizing: expo nette {net_exposure:.2f} > max {max_exposure:.2f} USDC "
                f"({max_exposure_pct*100:.0f}% de {balance:.2f})",
                "none",
            )
        return None

    def _check_inventory_skewing(self, signal: Signal,
                                  balance: float) -> RiskVerdict | None:
        """
        Inventory skewing :
          ratio = exposition_nette_USDC / max_autorisée
          > 0.6 → Cancel bids, Ask only
          = 1.0 → Liquidation unilatérale (Ask only)
        Utilise le mid_price actuel du signal pour valoriser l'exposition existante.
        """
        max_exposure_pct = self._max_exposure_pct
        max_exposure = balance * max_exposure_pct
        if max_exposure <= 0:
            return None

        net_exposure = abs(self._get_current_exposure_usdc(
            signal.token_id, getattr(signal, "mid_price", 0.0)
        ))
        ratio         = net_exposure / max_exposure

        if ratio >= INVENTORY_SKEW_LIQ:
            # Liquidation unilatérale : seuls les Ask sont autorisés
            if signal.side == "buy":
                logger.warning(
                    "[RiskManager] Inventory ratio=%.2f >= 1.0 → liquidation, BID refusé: %s",
                    ratio, signal.token_id[:16],
                )
                return RiskVerdict(
                    False,
                    f"Inventory liquidation: ratio={ratio:.2f}, Ask uniquement",
                    "liquidate",
                )

        elif ratio >= INVENTORY_SKEW_WARN:
            # Ask only : on refuse les bids supplémentaires
            if signal.side == "buy":
                logger.info(
                    "[RiskManager] Inventory ratio=%.2f >= 0.7 → Ask only, BID refusé: %s",
                    ratio, signal.token_id[:16],
                )
                return RiskVerdict(
                    False,
                    f"Inventory skew: ratio={ratio:.2f} >= {INVENTORY_SKEW_WARN}, Ask only",
                    "cancel_bids",
                )

        return None

    def update_high_water_mark(self, balance: float):
        """Appeler à chaque cycle après avoir récupéré le solde."""
        self.db.update_high_water_mark(balance)
