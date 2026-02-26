"""
Module de Copy-Trading.
DÉSACTIVÉ — L'API Goldsky n'est pas configurée.
Toutes les méthodes retournent des valeurs neutres pour éviter
toute génération de signaux basés sur des données fictives.
"""

import logging

logger = logging.getLogger(__name__)


class CopyTrader:
    def __init__(self, top_n: int = 10):
        self.top_n = top_n
        self.last_update = 0
        self.cache_ttl = 20 * 60
        self.top_wallets = set()

    def update_top_wallets(self):
        """Désactivé — API Goldsky non configurée."""
        pass

    def fetch_top_wallets(self) -> list[dict]:
        """Retourne une liste vide — module désactivé."""
        logger.debug("[CopyTrader] Module désactivé (API Goldsky non configurée)")
        return []

    def get_wallet_positions(self, wallet_addr: str) -> list[dict]:
        """Retourne une liste vide — module désactivé."""
        return []

    def get_market_direction(self, market_id: str) -> dict:
        """Retourne un signal neutre — module désactivé."""
        return {"confidence": 0.0, "direction": 0.0, "sizing": 0.0}
