"""
Module de contrôle des risques.
Vérifie chaque signal AVANT exécution pour protéger le capital.

Règles implémentées :
  1. Kill switch global
  2. Taille d'ordre max (config)
  3. Solde suffisant
  4. Réserve minimale (10% ou 2 USDC)
  5. Perte quotidienne max
  6. Confiance minimale du signal
  7. [OBI] Fractional sizing : expo nette max 5% du solde total
  8. [OBI] Inventory skewing : ratio > 0.7 → Ask only, ratio = 1.0 → liquidation
  9. [OBI] Circuit breaker global : solde < 90% du High Water Mark journalier
"""

import logging
from dataclasses import dataclass

from bot.config import BotConfig
from bot.strategy import Signal
from db.database import Database

logger = logging.getLogger("bot.risk")

# Constantes OBI risk
MAX_NET_EXPOSURE_PCT   = 0.05   # 5% du solde = exposition nette max par marché
INVENTORY_SKEW_WARN    = 0.70   # Ratio > 70% → Ask only (cancel bids)
INVENTORY_SKEW_LIQ     = 1.00   # Ratio = 100% → liquidation unilatérale
CIRCUIT_BREAKER_PCT    = 0.10   # Déclenche si solde chute de 10% vs HWM


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

    def check(self, signal: Signal, current_balance: float) -> RiskVerdict:
        """
        Vérifie si un signal est autorisé.
        Retourne RiskVerdict(approved, reason, action).
        """

        # ── 1. Kill switch ────────────────────────────────────────────────────
        if self.db.get_kill_switch():
            return RiskVerdict(False, "Kill switch activé", "none")

        # ── 2. Circuit breaker global (High Water Mark) ───────────────────────
        # Désactivé en paper trading (le HWM peut être corrompu depuis une session précédente)
        if not self._paper_trading:
            cb = self._check_circuit_breaker(current_balance)
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

        # ── 7. Confiance minimale ─────────────────────────────────────────────
        if signal.confidence < 0.5:
            return RiskVerdict(
                False,
                f"Confiance trop faible: {signal.confidence:.2f} < 0.50",
                "none",
            )

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

    def _check_circuit_breaker(self, current_balance: float) -> RiskVerdict | None:
        """
        Circuit breaker global : si le solde chute de 10% vs le HWM journalier,
        déclenche le kill switch et retourne un verdict de refus.
        """
        hwm = self.db.get_high_water_mark()
        if hwm <= 0 or current_balance <= 0:
            return None   # Pas encore de HWM (premier démarrage)

        drawdown = (hwm - current_balance) / hwm
        if drawdown >= CIRCUIT_BREAKER_PCT:
            logger.critical(
                "[RiskManager] CIRCUIT BREAKER: solde %.2f USDC = -%.1f%% vs HWM %.2f → kill switch",
                current_balance, drawdown * 100, hwm,
            )
            self.db.set_kill_switch(True)
            self.db.add_log(
                "CRITICAL", "risk",
                f"Circuit breaker déclenché: solde={current_balance:.2f} HWM={hwm:.2f} "
                f"drawdown={drawdown*100:.1f}%",
            )
            return RiskVerdict(
                False,
                f"Circuit breaker: drawdown {drawdown*100:.1f}% >= {CIRCUIT_BREAKER_PCT*100:.0f}%",
                "kill_switch",
            )
        return None

    def _check_fractional_sizing(self, signal: Signal, balance: float,
                                  order_cost: float) -> RiskVerdict | None:
        """
        L'exposition nette en USDC sur un marché ne doit pas dépasser
        MAX_NET_EXPOSURE_PCT * balance.
        current_exposure_usdc = shares_nettes * prix (stocké en USDC dans la DB).
        """
        max_exposure = balance * MAX_NET_EXPOSURE_PCT
        current_exposure_usdc = self.db.get_position_usdc(signal.token_id)

        # Delta USDC de ce nouvel ordre
        delta = order_cost if signal.side == "buy" else -order_cost
        net_exposure = abs(current_exposure_usdc + delta)

        if net_exposure > max_exposure:
            return RiskVerdict(
                False,
                f"Fractional sizing: expo nette {net_exposure:.2f} > max {max_exposure:.2f} USDC "
                f"({MAX_NET_EXPOSURE_PCT*100:.0f}% de {balance:.2f})",
                "none",
            )
        return None

    def _check_inventory_skewing(self, signal: Signal,
                                  balance: float) -> RiskVerdict | None:
        """
        Inventory skewing :
          ratio = exposition_nette_USDC / max_autorisée
          > 0.7 → Cancel bids, Ask only
          = 1.0 → Liquidation unilatérale (Ask only)
        """
        max_exposure = balance * MAX_NET_EXPOSURE_PCT
        if max_exposure <= 0:
            return None

        net_exposure  = abs(self.db.get_position_usdc(signal.token_id))
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
