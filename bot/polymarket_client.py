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

    # ── Allowances ERC-1155 ──────────────────────────────────────

    def get_conditional_allowance(self, token_id: str) -> dict:
        """
        Retourne le solde et l'allowance ERC-1155 pour un token spécifique.
        Retourne {} en cas d'erreur.
        """
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
            )
            return self.client.get_balance_allowance(params) or {}
        except Exception as e:
            logger.debug("get_conditional_allowance(%s): %s", token_id[:16], e)
            return {}

    def ensure_conditional_allowance(self, token_id: str) -> bool:
        """
        S'assure que le CTF Exchange a l'allowance ERC-1155 pour ce token.

        Polymarket utilise le standard ERC-1155 pour les shares de prédiction.
        Pour qu'un SELL passe, le contrat CTF Exchange doit avoir l'approbation
        de transférer les shares (setApprovalForAll). Sans ça → erreur 400
        "not enough balance / allowance".

        L'endpoint /balance-allowance/update déclenche cette approbation
        on-chain via le wallet signataire.

        Retourne True si l'allowance est déjà présente ou a été mise à jour.
        Retourne False en cas d'erreur.
        """
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

            # 1. Vérifier l'allowance actuelle
            info = self.get_conditional_allowance(token_id)
            allowance_raw = info.get("allowance") or info.get("Allowance") or "0"
            try:
                allowance = int(allowance_raw)
            except (ValueError, TypeError):
                allowance = 0

            if allowance > 0:
                logger.debug(
                    "[Allowance] Token %s: allowance OK (%s)", token_id[:16], allowance_raw
                )
                return True

            # 2. Allowance insuffisante → déclencher l'approbation
            logger.info(
                "[Allowance] Token %s: allowance=0 → approbation ERC-1155...", token_id[:16]
            )
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
            )
            resp = self.client.update_balance_allowance(params)
            logger.info(
                "[Allowance] Token %s: approbation envoyée → %s", token_id[:16], resp
            )
            return True

        except Exception as e:
            logger.warning(
                "[Allowance] Token %s: erreur ensure_allowance: %s", token_id[:16], e
            )
            return False

    def ensure_allowances_for_tokens(self, token_ids: list) -> dict:
        """
        Vérifie et met à jour les allowances ERC-1155 pour une liste de tokens.
        Appelé au démarrage pour tous les tokens en inventaire, et après chaque
        fill BUY pour préparer le SELL futur.

        Retourne un dict {token_id: bool} indiquant le résultat par token.
        """
        results = {}
        for token_id in token_ids:
            if not token_id:
                continue
            results[token_id] = self.ensure_conditional_allowance(token_id)
        ok = sum(1 for v in results.values() if v)
        ko = len(results) - ok
        if results:
            logger.info(
                "[Allowance] Vérification terminée: %d OK, %d échec(s) sur %d token(s)",
                ok, ko, len(results),
            )
        return results

    # ── Utilitaires ──────────────────────────────────────────────

    def get_server_time(self) -> str:
        """Vérifie la connectivité en récupérant l'heure serveur."""
        return self.client.get_server_time()

    def is_alive(self, timeout: float = 10.0) -> bool:
        """Vérifie que l'API répond dans un délai donné (défaut 10s).

        Utilise un Thread daemon : si get_server_time() bloque (TCP hang),
        le thread est abandonné après `timeout` secondes et le bot continue.
        Attention : ThreadPoolExecutor.__exit__ attend la fin du thread même
        après le timeout → bloque aussi. On utilise Thread.join(timeout) à la place.
        """
        import threading
        result = []

        def _check():
            try:
                self.get_server_time()
                result.append(True)
            except Exception:
                result.append(False)

        t = threading.Thread(target=_check, daemon=True)
        t.start()
        t.join(timeout=timeout)
        # Si le thread n'a pas fini dans le délai → timeout (API bloquée)
        if t.is_alive():
            logger.warning("[is_alive] get_server_time timeout (%.0fs) → API considérée morte", timeout)
            return False
        return bool(result and result[0])
