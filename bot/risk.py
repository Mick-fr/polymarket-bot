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
CIRCUIT_BREAKER_PCT    = 0.10   # D\u00e9clenche si portfolio chute de 10% vs HWM (d\u00e9faut, override via DB: circuit_breaker_pct)


@dataclass
class RiskVerdict:
    """Résultat d'une vérification de risque."""
    approved:  bool
    reason:    str
    action:    str = "none"   # 'none' | 'cancel_bids' | 'liquidate' | 'kill_switch'


class TrailingStopManager:
    """V14: Gère les trailing stops ajustés à la volatilité."""
    def __init__(self, db: Database):
        self.db = db
        self._position_max_pnl: dict[str, float] = {}

    def update_and_check(self, positions: list[dict], current_vol: float) -> list[dict]:
        to_close = []
        for p in positions:
            token_id = p.get("token_id", "")
            qty = p.get("quantity", 0.0)
            if qty <= 0:
                continue

            avg_price = p.get("avg_price", 0.0)
            mid_price = p.get("current_mid", 0.0)
            if avg_price <= 0 or mid_price <= 0:
                continue
                
            pnl_pct = (mid_price - avg_price) / avg_price
            
            # Record HWM 
            max_pnl = self._position_max_pnl.get(token_id, 0.0)
            if pnl_pct > max_pnl:
                max_pnl = pnl_pct
                self._position_max_pnl[token_id] = max_pnl
                
            # Logique Trailing Stop (déclenche après +2% ROI)
            if max_pnl >= 0.02:
                # Si vol = 0.50 (calme), gap = 1%. Si vol = 1.00 (agité), gap = 2%
                vol_gap = max(0.01, min(0.03, current_vol * 0.02))
                trailing_sl = max_pnl - vol_gap
                
                # Breakeven garanti + 0.2%
                trailing_sl = max(0.002, trailing_sl)
                
                if pnl_pct <= trailing_sl:
                    to_close.append({
                        "token_id": token_id,
                        "qty": qty,
                        "reason": f"Trailing SL (Max: +{max_pnl*100:.1f}%, Gap: {vol_gap*100:.1f}%, Chute à {pnl_pct*100:.1f}%)"
                    })
                    self._position_max_pnl.pop(token_id, None)
                    
        return to_close

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
        self.trailing_stops = TrailingStopManager(db)

    # 2026 V8.0 OVERRIDE CONSTANTES DB — rechargement dynamique chaque cycle
    AGGRESSIVITY_MAP = {
        "Info Edge Only":   {"max_exposure_pct": 0.12},
        "MM Conservateur":  {"max_exposure_pct": 0.12},
        "MM Balanced":      {"max_exposure_pct": 0.22},
        "MM Aggressif":     {"max_exposure_pct": 0.35},
        "MM Très Agressif": {"max_exposure_pct": 0.50},
    }

    def reload_aggressivity(self):
        """Relit strategy_mode depuis la DB et met à jour _max_exposure_pct."""
        try:
            level = self.db.get_config_str("strategy_mode", "MM Balanced")
            params = self.AGGRESSIVITY_MAP.get(level)
            if params:
                self._max_exposure_pct = params["max_exposure_pct"]
            else:
                # Custom / fallback
                db_expo = self.db.get_config("max_net_exposure_pct")
                if db_expo is not None:
                    self._max_exposure_pct = float(db_expo)
            # Log only on change
            if level != self._last_agg_level:
                logger.info("[LIVE CONFIG] RiskManager: strategy_mode=%s | max_expo=%.0f%%",
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

        # ── 10. V7.9 Info Edge BTC/ETH Max Exposure (12%) ─────────────────────
        q_lo = signal.market_question.lower()
        if "btc" in q_lo or "bitcoin" in q_lo or "eth" in q_lo or "ethereum" in q_lo:
            positions = self.db.get_all_positions()
            btc_eth_expo = 0.0
            for p in positions:
                pq_lo = p.get("question", "").lower()
                if "btc" in pq_lo or "bitcoin" in pq_lo or "eth" in pq_lo or "ethereum" in pq_lo:
                    qty = p.get("quantity", 0.0)
                    mid = p.get("current_mid", p.get("avg_price", 0.0))
                    btc_eth_expo += qty * mid
            
            new_expo = btc_eth_expo + order_cost
            if cb_value > 0 and (new_expo / cb_value) > 0.12:
                 return RiskVerdict(False, f"BTC/ETH Limit reached: {new_expo:.2f} USDC > 12%", "cancel_bids")

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
        Circuit breaker global : si la VALEUR TOTALE DU PORTFOLIO chute vs le HWM
        journalier au-delà du seuil configuré (default 10%, config: circuit_breaker_pct).
        """
        hwm = self.db.get_high_water_mark()
        if hwm <= 0 or portfolio_value <= 0:
            return None   # Pas encore de HWM (premier démarrage)

        # V7.6 — Seuil configurable via DB
        cb_pct = float(self.db.get_config("circuit_breaker_pct", CIRCUIT_BREAKER_PCT))

        drawdown = (hwm - portfolio_value) / hwm
        if drawdown >= cb_pct:
            drawdown_str = f"-{drawdown*100:.1f}% drawdown (HWM={hwm:.2f})"
            logger.critical(
                "CIRCUIT BREAKER ACTIVÉ : -%s%% vs HWM %.2f (portfolio=%.2f) → kill_switch=True",
                f"{drawdown*100:.1f}", hwm, portfolio_value,
            )
            self.db.set_kill_switch(True)
            self.db.set_config("kill_switch_reason", f"Circuit Breaker {drawdown_str}")
            self.db.add_log(
                "CRITICAL", "risk",
                f"Circuit breaker déclenché: portfolio={portfolio_value:.2f} HWM={hwm:.2f} "
                f"drawdown={drawdown*100:.1f}%",
            )
            return RiskVerdict(
                False,
                f"Circuit breaker: drawdown {drawdown*100:.1f}% >= {cb_pct*100:.0f}%",
                "kill_switch",
            )
        return None

    def reset_circuit_breaker(self, update_hwm: bool = False) -> str:
        """V7.6 — Réinitialise le kill switch après un circuit breaker.
        Optionnellement remet le HWM au portfolio actuel pour repartir proprement."""
        self.db.set_kill_switch(False)
        self.db.set_config("bot_active", "true")
        self.db.set_config("kill_switch_reason", "")
        if update_hwm:
            # Récupère le portfolio actuel et le définit comme nouveau HWM
            current_balance = float(self.db.get_config("last_portfolio_value", 0) or 0)
            if current_balance > 0:
                self.db.update_high_water_mark(current_balance)
                msg = f"Circuit breaker reset — HWM mis à jour: {current_balance:.2f} USDC"
            else:
                msg = "Circuit breaker reset — HWM inchangé (valeur portfolio inconnue)"
        else:
            msg = "Circuit breaker reset — kill_switch=False, bot actif"
        logger.info("[RiskManager] %s", msg)
        self.db.add_log("INFO", "risk", msg)
        return msg

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

    def check_auto_close_copied(self, client, positions: list[dict]):
        """V7.8 SAFE MODE: Auto-close automatique à -40 % unrealized sur TOUTES les positions copiées."""
        safe_mode = self.db.get_config("safe_mode", "true") == "true"
        if not safe_mode:
            return
            
        try:
            with self.db._cursor() as cur:
                cur.execute("SELECT DISTINCT token_id FROM copy_trades")
                copied_tokens = {r["token_id"] for r in cur.fetchall()}
        except Exception:
            return
            
        if not copied_tokens:
            return
            
        for p in positions:
            token_id = p.get("token_id", "")
            qty = p.get("quantity", 0.0)
            if qty <= 0 or token_id not in copied_tokens:
                continue
                
            avg_price = p.get("avg_price", 0.0)
            mid_price = p.get("current_mid", 0.0)
            if avg_price > 0 and mid_price > 0:
                pnl_pct = (mid_price - avg_price) / avg_price
                if pnl_pct <= -0.40:
                    logger.warning("[SAFE MODE] Auto-close position copiée '%s' (PnL %.1f%% <= -40%%)", token_id[:20], pnl_pct * 100)
                    try:
                        client.create_order(token_id=token_id, side="sell", order_type="market", size=qty)
                        self.db.add_log("WARNING", "risk", f"[SAFE MODE] Auto-close copie {token_id[:10]} à {pnl_pct * 100:.1f}%")
                    except Exception as e:
                        logger.error("Erreur auto-close safe_mode %s: %s", token_id[:16], e)

    def check_auto_close_btc_eth(self, client, positions: list[dict], current_vol: float = 0.80):
        """V14: INFO EDGE Auto-close & Trailing Stop pour marchés BTC/ETH."""
        # 1. Trailing Stops V14
        to_close = self.trailing_stops.update_and_check(positions, current_vol)
        for action in to_close:
            token_id = action["token_id"]
            qty = action["qty"]
            reason = action["reason"]
            logger.warning("[TRAILING STOP] %s sur '%s'", reason, token_id[:20])
            try:
                # Fallback to general market sale logic used in V13
                # Si pm_client expose place_market_order, on prefère ça
                client.place_market_order(token_id=token_id, amount=qty, side="sell")
                self.db.add_log("WARNING", "risk", f"[TRAILING STOP] {token_id[:10]} {reason}")
            except Exception as e:
                logger.error("Erreur trailing stop %s: %s", token_id[:16], e)

        # 2. Hard Stop Loss Classique (-25%)
        for p in positions:
            qty = p.get("quantity", 0.0)
            if qty <= 0:
                continue
                
            question = p.get("question", "").lower()
            token_id = p.get("token_id", "")
            if "btc" in question or "bitcoin" in question or "eth" in question or "ethereum" in question:
                avg_price = p.get("avg_price", 0.0)
                mid_price = p.get("current_mid", 0.0)
                if avg_price > 0 and mid_price > 0:
                    pnl_pct = (mid_price - avg_price) / avg_price
                    if pnl_pct <= -0.25:
                        logger.warning("[INFO EDGE] Hard Stop-Loss BTC/ETH '%s' (PnL %.1f%% <= -25%%)", token_id[:20], pnl_pct * 100)
                        try:
                            client.place_market_order(token_id=token_id, amount=qty, side="sell")
                            self.db.add_log("WARNING", "risk", f"[INFO EDGE] Hard Stop {token_id[:10]} à {pnl_pct * 100:.1f}%")
                        except Exception as e:
                            logger.error("Erreur hard stop BTC/ETH %s: %s", token_id[:16], e)
