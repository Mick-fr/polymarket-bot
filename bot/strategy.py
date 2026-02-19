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
import urllib.request
import urllib.error
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from bot.polymarket_client import PolymarketClient

logger = logging.getLogger("bot.strategy")

GAMMA_API_URL = "https://gamma-api.polymarket.com/markets"


def _fetch_gamma_clob_markets(limit: int = 50) -> list[dict]:
    """
    Interroge l'API Gamma de Polymarket pour trouver les marchés
    avec un carnet d'ordres CLOB actif (enableOrderBook=true).

    Retourne une liste de dicts avec au moins :
      - question
      - clobTokenIds  (list[str])
      - bestBid / bestAsk
    """
    url = (
        f"{GAMMA_API_URL}"
        f"?closed=false&enableOrderBook=true"
        f"&order=volume24hr&ascending=false&limit={limit}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "polymarket-bot/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            # L'API peut retourner soit une liste directe, soit un objet {markets:[]}
            if isinstance(data, list):
                return data
            return data.get("markets", [])
    except Exception as e:
        logger.error("[GammaAPI] Erreur lors de la récupération des marchés: %s", e)
        return []


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
    Scanne les marchés CLOB actifs via l'API Gamma, log les prix,
    ne passe aucun ordre.
    """

    def analyze(self) -> list[Signal]:
        """Scanne les marchés CLOB actifs et retourne toujours une liste vide."""
        signals: list[Signal] = []

        try:
            # Étape 1 : récupérer les marchés CLOB actifs via l'API Gamma
            gamma_markets = _fetch_gamma_clob_markets(limit=50)

            if not gamma_markets:
                logger.info("[DummyStrategy] Aucun marché CLOB actif trouvé via Gamma API.")
                return signals

            logger.info(
                "[DummyStrategy] %d marchés CLOB actifs récupérés via Gamma API.",
                len(gamma_markets),
            )

            # Étape 2 : scanner jusqu'à 5 marchés avec un midpoint valide
            found = 0
            for market in gamma_markets:
                if found >= 5:
                    break

                question = market.get("question", "?")
                # L'API Gamma utilise camelCase
                clob_token_ids = market.get("clobTokenIds") or []
                best_bid = market.get("bestBid")
                best_ask = market.get("bestAsk")

                # clobTokenIds peut être une liste Python OU un string JSON (ex: '["123","456"]')
                if isinstance(clob_token_ids, str):
                    try:
                        clob_token_ids = json.loads(clob_token_ids)
                    except (json.JSONDecodeError, ValueError):
                        clob_token_ids = []

                if not clob_token_ids:
                    logger.debug(
                        "[DummyStrategy] Marché '%s' ignoré : pas de clobTokenIds.",
                        question[:60],
                    )
                    continue

                # Premier token = YES
                token_id = clob_token_ids[0] if isinstance(clob_token_ids, list) else str(clob_token_ids)
                mid = self.client.get_midpoint(token_id)

                # Fallback : calculer le midpoint depuis bestBid/bestAsk Gamma
                if mid is None and best_bid and best_ask:
                    try:
                        mid = (float(best_bid) + float(best_ask)) / 2.0
                        logger.debug(
                            "[DummyStrategy] Midpoint CLOB indisponible, "
                            "utilisation Gamma bid/ask: %.4f",
                            mid,
                        )
                    except (TypeError, ValueError):
                        mid = None

                if mid is None:
                    logger.debug(
                        "[DummyStrategy] Pas de prix disponible pour '%s'.", question[:60]
                    )
                    continue

                found += 1
                logger.info(
                    "[DummyStrategy] Marché: '%s' | YES = %.4f (bid=%s ask=%s)",
                    question[:60],
                    mid,
                    best_bid,
                    best_ask,
                )

                if mid < 0.10:
                    logger.info(
                        "[DummyStrategy] Signal potentiel détecté (non exécuté): "
                        "BUY YES @ %.4f – '%s'",
                        mid,
                        question[:40],
                    )

            if found == 0:
                logger.info(
                    "[DummyStrategy] Aucun prix disponible parmi %d marchés CLOB.",
                    len(gamma_markets),
                )

        except Exception as e:
            logger.error("[DummyStrategy] Erreur lors de l'analyse: %s", e)

        logger.info(
            "[DummyStrategy] Analyse terminée. %d signaux (placeholder = 0 exécutés).",
            len(signals),
        )
        return signals
