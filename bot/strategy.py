"""
Stratégie de trading – PLACEHOLDER.

Cette stratégie factice ne fait AUCUN trade réel.
Elle illustre l'interface que toute stratégie doit implémenter
et log les opportunités qu'elle "détecte" sans agir.

Pour créer ta propre stratégie :
1. Hériter de BaseStrategy
2. Implémenter analyze() et generate_signals()
3. Remplacer DummyStrategy dans config ou trader.py
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from bot.polymarket_client import PolymarketClient

logger = logging.getLogger("bot.strategy")


@dataclass
class Signal:
    """Représente un signal de trading."""

    token_id: str
    market_id: str
    market_question: str
    side: str  # 'buy' ou 'sell'
    order_type: str  # 'limit' ou 'market'
    price: Optional[float]  # None pour les ordres market
    size: float  # Nombre de shares (limit) ou USDC (market)
    confidence: float  # 0.0 à 1.0
    reason: str


class BaseStrategy(ABC):
    """Interface de base pour toutes les stratégies."""

    def __init__(self, client: PolymarketClient):
        self.client = client

    @abstractmethod
    def analyze(self) -> list[Signal]:
        """
        Analyse les marchés et retourne une liste de signaux.
        Retourne une liste vide si aucune opportunité.
        """
        ...


class DummyStrategy(BaseStrategy):
    """
    Stratégie factice pour le développement.
    Scanne quelques marchés, log les prix, ne passe aucun ordre.
    """

    def analyze(self) -> list[Signal]:
        """Scanne les marchés et retourne toujours une liste vide."""
        signals: list[Signal] = []

        try:
            resp = self.client.get_markets()
            markets = resp.get("data", []) if isinstance(resp, dict) else []

            if not markets:
                logger.info("[DummyStrategy] Aucun marché récupéré.")
                return signals

            # Filtre les marchés actifs uniquement (évite les 404 sur carnets fermés)
            active_markets = [m for m in markets if m.get("active") is True]
            if not active_markets:
                logger.info("[DummyStrategy] Aucun marché actif trouvé.")
                return signals

            # On scanne les 5 premiers marchés actifs
            for market in active_markets[:5]:
                question = market.get("question", "?")
                tokens = market.get("tokens", [])
                condition_id = market.get("condition_id", "unknown")

                for token in tokens:
                    token_id = token.get("token_id", "")
                    outcome = token.get("outcome", "?")
                    if not token_id:
                        continue

                    mid = self.client.get_midpoint(token_id)
                    if mid is None:
                        continue

                    logger.info(
                        "[DummyStrategy] Marché: '%s' | %s = %.4f",
                        question[:60],
                        outcome,
                        mid,
                    )

                    # Exemple : si un outcome est très sous-évalué, on le signale
                    # (mais on ne passe PAS d'ordre – c'est un placeholder)
                    if mid < 0.10:
                        logger.info(
                            "[DummyStrategy] Signal potentiel détecté (non exécuté): "
                            "BUY %s @ %.4f – '%s'",
                            outcome, mid, question[:40],
                        )
                        # On pourrait ajouter à `signals` ici pour une vraie stratégie
                        # signals.append(Signal(...))

        except Exception as e:
            logger.error("[DummyStrategy] Erreur lors de l'analyse: %s", e)

        logger.info(
            "[DummyStrategy] Analyse terminée. %d signaux (placeholder = 0 exécutés).",
            len(signals),
        )
        return signals
