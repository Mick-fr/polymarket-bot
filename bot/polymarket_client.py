"""
Wrapper autour de py-clob-client.
Encapsule toute l'interaction avec l'API Polymarket :
authentification, lecture de marchés, passage et annulation d'ordres.
"""

import logging
from typing import Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    BookParams,
    MarketOrderArgs,
    OpenOrderParams,
    OrderArgs,
    OrderType,
)
from py_clob_client.order_builder.constants import BUY, SELL

from bot.config import PolymarketConfig

logger = logging.getLogger("bot.polymarket")

# Mapping lisible pour les côtés d'ordre
SIDE_MAP = {"buy": BUY, "sell": SELL}


class PolymarketClient:
    """Client authentifié pour le CLOB Polymarket."""

    def __init__(self, config: PolymarketConfig):
        self._config = config
        self._client: Optional[ClobClient] = None

    def connect(self):
        """Initialise le client et dérive les credentials API (L1 → L2)."""
        logger.info("Connexion au CLOB Polymarket...")

        kwargs = {
            "host": self._config.host,
            "chain_id": self._config.chain_id,
            "key": self._config.private_key,
            "signature_type": self._config.signature_type,
        }
        if self._config.funder_address:
            kwargs["funder"] = self._config.funder_address

        self._client = ClobClient(**kwargs)

        # Dérive ou crée les credentials L2 pour les opérations de trading
        api_creds = self._client.create_or_derive_api_creds()
        self._client.set_api_creds(api_creds)

        logger.info("Connecté au CLOB Polymarket avec succès.")

    @property
    def client(self) -> ClobClient:
        if self._client is None:
            raise RuntimeError("Client non initialisé. Appeler connect() d'abord.")
        return self._client

    # ── Données de marché ────────────────────────────────────────

    def get_markets(self, next_cursor: str = "") -> dict:
        """Récupère la liste des marchés disponibles."""
        return self.client.get_markets(next_cursor=next_cursor)

    def get_order_book(self, token_id: str) -> dict:
        """Récupère le carnet d'ordres pour un token."""
        return self.client.get_order_book(token_id)

    def get_order_books(self, token_ids: list[str]) -> list[dict]:
        """Récupère plusieurs carnets d'ordres."""
        params = [BookParams(token_id=tid) for tid in token_ids]
        return self.client.get_order_books(params)

    def get_midpoint(self, token_id: str) -> Optional[float]:
        """Retourne le prix médian pour un token."""
        try:
            mid = self.client.get_midpoint(token_id)
            return float(mid) if mid else None
        except Exception as e:
            logger.debug("Midpoint indisponible pour %s: %s", token_id, e)
            return None

    def get_price(self, token_id: str, side: str = "buy") -> Optional[float]:
        """Retourne le meilleur prix bid ou ask."""
        try:
            price = self.client.get_price(token_id, side=side.upper())
            return float(price) if price else None
        except Exception as e:
            logger.debug("Prix indisponible pour %s: %s", token_id, e)
            return None

    # ── Passage d'ordres ─────────────────────────────────────────

    def place_limit_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str = "buy",
    ) -> dict:
        """
        Place un ordre limit GTC (Good Till Cancelled).
        - price : entre 0.01 et 0.99
        - size  : nombre de shares
        - side  : 'buy' ou 'sell'
        Retourne la réponse de l'API.
        """
        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=SIDE_MAP[side.lower()],
        )
        signed_order = self.client.create_order(order_args)
        resp = self.client.post_order(signed_order, OrderType.GTC)
        logger.info(
            "Ordre limit posté: %s %s shares @ %.2f sur %s → %s",
            side.upper(), size, price, token_id[:16], resp,
        )
        return resp

    def place_market_order(
        self,
        token_id: str,
        amount: float,
        side: str = "buy",
    ) -> dict:
        """
        Place un ordre market FOK (Fill Or Kill).
        - amount : montant en USDC
        - side   : 'buy' ou 'sell'
        Retourne la réponse de l'API.
        """
        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=amount,
            side=SIDE_MAP[side.lower()],
            order_type=OrderType.FOK,
        )
        signed_order = self.client.create_market_order(order_args)
        resp = self.client.post_order(signed_order, OrderType.FOK)
        logger.info(
            "Ordre market posté: %s %.2f USDC sur %s → %s",
            side.upper(), amount, token_id[:16], resp,
        )
        return resp

    # ── Gestion des ordres ───────────────────────────────────────

    def get_open_orders(self) -> list:
        """Récupère tous les ordres ouverts."""
        return self.client.get_orders(OpenOrderParams())

    def get_order(self, order_id: str) -> Optional[dict]:
        """Récupère le statut d'un ordre par son ID CLOB."""
        try:
            resp = self.client.get_order(order_id)
            return resp if isinstance(resp, dict) else None
        except Exception as e:
            logger.debug("get_order(%s) erreur: %s", order_id[:16], e)
            return None

    def cancel_order(self, order_id: str) -> dict:
        """Annule un ordre spécifique."""
        resp = self.client.cancel(order_id)
        logger.info("Ordre annulé: %s → %s", order_id, resp)
        return resp

    def cancel_all_orders(self) -> dict:
        """Annule tous les ordres ouverts."""
        resp = self.client.cancel_all()
        logger.info("Tous les ordres annulés → %s", resp)
        return resp

    # ── Utilitaires ──────────────────────────────────────────────

    def get_server_time(self) -> str:
        """Vérifie la connectivité en récupérant l'heure serveur."""
        return self.client.get_server_time()

    def is_alive(self, timeout: float = 10.0) -> bool:
        """Vérifie que l'API répond dans un délai donné (défaut 10s).
        Utilise un thread avec timeout pour éviter tout blocage indéfini."""
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.get_server_time)
                future.result(timeout=timeout)
            return True
        except (FuturesTimeout, Exception):
            return False
