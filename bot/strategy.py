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
            # Polymarket retourne enable_order_book=False pour tous les marchés
            # dans /markets depuis leur migration vers le modèle hybride.
            # On filtre uniquement sur accepting_orders=True et on tente
            # le midpoint directement — get_midpoint() retourne None sur 404,
            # ce qui permet de découvrir les carnets actifs sans pré-filtrage.
            resp = self.client.get_markets()
            markets = resp.get("data", []) if isinstance(resp, dict) else []

            if not markets:
                logger.info("[DummyStrategy] Aucun marché récupéré.")
                return signals

            # Filtre sur accepting_orders uniquement
            candidates = [m for m in markets if m.get("accepting_orders") is True]
            logger.info("[DummyStrategy] %d marchés acceptant des ordres sur %d total.",
                        len(candidates), len(markets))

            # Scanne jusqu'à trouver 5 marchés avec un midpoint valide
            found = 0
            for market in candidates:
                if found >= 5:
                    break
                question = market.get("question", "?")
                tokens = market.get("tokens", [])

                for token in tokens:
                    token_id = token.get("token_id", "")
                    outcome = token.get("outcome", "?")
                    if not token_id:
                        continue

                    mid = self.client.get_midpoint(token_id)
                    if mid is None:
                        continue  # Pas de carnet actif pour ce token

                    found += 1
                    logger.info(
                        "[DummyStrategy] Marché: '%s' | %s = %.4f",
                        question[:60], outcome, mid,
                    )

                    if mid < 0.10:
                        logger.info(
                            "[DummyStrategy] Signal potentiel détecté (non exécuté): "
                            "BUY %s @ %.4f – '%s'",
                            outcome, mid, question[:40],
                        )

            if found == 0:
                logger.info("[DummyStrategy] Aucun carnet d'ordres actif trouvé parmi %d candidats.", len(candidates))

        except Exception as e:
            logger.error("[DummyStrategy] Erreur lors de l'analyse: %s", e)

        logger.info(
            "[DummyStrategy] Analyse terminée. %d signaux (placeholder = 0 exécutés).",
            len(signals),
        )
        return signals
