"""
Module de contrôle des risques.
Vérifie chaque signal AVANT exécution pour protéger le capital.
"""

import logging
from dataclasses import dataclass

from bot.config import BotConfig
from bot.strategy import Signal
from db.database import Database

logger = logging.getLogger("bot.risk")


@dataclass
class RiskVerdict:
    """Résultat d'une vérification de risque."""

    approved: bool
    reason: str


class RiskManager:
    """Contrôle des risques pré-exécution."""

    def __init__(self, config: BotConfig, db: Database):
        self.config = config
        self.db = db

    def check(self, signal: Signal, current_balance: float) -> RiskVerdict:
        """
        Vérifie si un signal est autorisé par les règles de risque.
        Retourne un RiskVerdict avec approved=True/False et la raison.
        """
        # 1. Kill switch actif ?
        if self.db.get_kill_switch():
            return RiskVerdict(False, "Kill switch activé")

        # 2. Montant de l'ordre dans les limites ?
        order_cost = signal.size if signal.order_type == "market" else (
            signal.size * signal.price if signal.price else 0
        )
        if order_cost > self.config.max_order_size:
            return RiskVerdict(
                False,
                f"Ordre trop gros: {order_cost:.2f} USDC > max {self.config.max_order_size:.2f}",
            )

        # 3. Solde suffisant ?
        if order_cost > current_balance:
            return RiskVerdict(
                False,
                f"Solde insuffisant: {current_balance:.2f} USDC < {order_cost:.2f} requis",
            )

        # 4. Garder un minimum vital sur le compte (10% du capital ou 2 USDC)
        min_reserve = max(current_balance * 0.10, 2.0)
        if current_balance - order_cost < min_reserve:
            return RiskVerdict(
                False,
                f"Réserve insuffisante après ordre: "
                f"resterait {current_balance - order_cost:.2f} < {min_reserve:.2f} USDC",
            )

        # 5. Perte quotidienne max atteinte ?
        daily_loss = self.db.get_daily_loss()
        if daily_loss >= self.config.max_daily_loss:
            return RiskVerdict(
                False,
                f"Perte quotidienne max atteinte: {daily_loss:.2f} >= {self.config.max_daily_loss:.2f} USDC",
            )

        # 6. Confiance minimale du signal
        if signal.confidence < 0.5:
            return RiskVerdict(
                False,
                f"Confiance trop faible: {signal.confidence:.2f} < 0.50",
            )

        logger.info(
            "Risque OK: %s %s @ %s – coût estimé %.2f USDC, perte jour %.2f",
            signal.side.upper(),
            signal.token_id[:16],
            signal.price or "market",
            order_cost,
            daily_loss,
        )
        return RiskVerdict(True, "Toutes les vérifications passées")
