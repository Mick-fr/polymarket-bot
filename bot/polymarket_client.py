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
        # Cache des allowances ERC-1155 déjà confirmées dans cette session.
        # Évite de re-vérifier + re-approuver à chaque SELL (10 req/cycle inutiles).
        # Invalider en cas de redémarrage (recréation de l'objet = set vide).
        self._allowance_confirmed: set[str] = set()
        # Cache des tokens détectés comme neg-risk dans cette session.
        # get_neg_risk() coûte 1 requête HTTP par appel. Sans cache, chaque cycle
        # vérifie à nouveau tous les tokens neg-risk en inventaire (2 req × N tokens).
        # Un token neg-risk ne change pas de type au cours d'une session → cache permanent.
        self._neg_risk_confirmed: set[str] = set()
        # Tokens pour lesquels update_balance_allowance a été appelé mais allowance=0
        # persiste (probablement un contrat non standard ou délai API très long).
        # On ne re-tente pas indéfiniment : après _MAX_ALLOWANCE_RETRIES tentatives,
        # le token est mis en quarantaine → SELL bloqué comme s'il était neg-risk.
        self._allowance_retry_count: dict[str, int] = {}
        self._ALLOWANCE_MAX_RETRIES = 3  # Au-delà → traiter comme neg-risk

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

    def is_neg_risk_token(self, token_id: str) -> bool:
        """
        Retourne True si ce token utilise le mécanisme NegRisk de Polymarket.
        Les tokens neg-risk utilisent un contrat d'exchange différent (NegRisk Exchange)
        qui a ses propres règles d'approbation — l'approbation ERC-1155 standard
        ne couvre pas ce contrat. Ces tokens nécessitent une configuration via l'UI
        Polymarket ou une transaction directe avec le contrat NegRisk.

        Cache session : une fois détecté comme neg-risk, le token est mémorisé
        pour éviter un appel réseau à chaque cycle (coût : 1 req HTTP / appel).
        Un token neg-risk ne change pas de type en cours de session.
        """
        # Cache hit : déjà détecté comme neg-risk → retour immédiat sans réseau
        if token_id in self._neg_risk_confirmed:
            return True
        # Cache hit : déjà confirmé comme non-neg-risk (allowance standard OK)
        if token_id in self._allowance_confirmed:
            return False
        try:
            result = bool(self.client.get_neg_risk(token_id))
            if result:
                self._neg_risk_confirmed.add(token_id)
                logger.debug("[NegRisk] Token %s: confirmé neg-risk (mis en cache)", token_id[:16])
            return result
        except Exception as e:
            logger.debug("[NegRisk] Token %s: erreur get_neg_risk: %s", token_id[:16], e)
            return False

    def ensure_conditional_allowance(self, token_id: str) -> bool:
        """
        S'assure que le CTF Exchange a l'allowance ERC-1155 pour ce token.

        Polymarket utilise le standard ERC-1155 pour les shares de prédiction.
        Pour qu'un SELL passe, le contrat CTF Exchange doit avoir l'approbation
        de transférer les shares (setApprovalForAll). Sans ça → erreur 400
        "not enough balance / allowance".

        ATTENTION — Tokens neg-risk :
        Les marchés binaires groupés (neg-risk) utilisent le NegRisk Exchange,
        un contrat différent du CTF Exchange standard. L'endpoint
        /balance-allowance/update ne couvre PAS ce contrat. Ces tokens
        doivent être approuvés manuellement via l'UI Polymarket.
        Cette méthode détecte les tokens neg-risk et loggue un warning.

        Retourne True si l'allowance est présente ou mise à jour avec succès.
        Retourne False si neg-risk (non gérable automatiquement) ou erreur.
        """
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

            # 0b. Cache session : si déjà approuvé dans cette session, retourner True
            #     directement sans aucune requête réseau. Reset à chaque redémarrage.
            if token_id in self._allowance_confirmed:
                logger.debug(
                    "[Allowance] Token %s: allowance OK (cache session)", token_id[:16]
                )
                return True

            # 0. Détecter les tokens neg-risk — contrat différent, on ne peut pas
            #    appeler update_balance_allowance pour eux via cette API
            if self.is_neg_risk_token(token_id):
                logger.warning(
                    "[Allowance] Token %s: NEG-RISK détecté → allowance non gérée "
                    "automatiquement. Ce token utilise le NegRisk Exchange. "
                    "Approuver manuellement via l'UI Polymarket si SELL échoue.",
                    token_id[:16],
                )
                # On tente quand même update_balance_allowance : certaines versions
                # de l'API Polymarket le supportent pour les neg-risk également.
                # Si ça échoue, ce n'est pas bloquant (le warning suffit).
                try:
                    params = BalanceAllowanceParams(
                        asset_type=AssetType.CONDITIONAL,
                        token_id=token_id,
                    )
                    self.client.update_balance_allowance(params)
                    logger.info(
                        "[Allowance] Token %s (neg-risk): approbation envoyée → OK",
                        token_id[:16],
                    )
                except Exception:
                    pass
                return False  # Signale que ce token peut poser problème

            # 0c. Quarantaine : token ayant dépassé _ALLOWANCE_MAX_RETRIES tentatives
            #     d'approbation sans que l'allowance n'apparaisse en GET.
            #     Probable contrat non standard ou délai API extrêmement long.
            #     Traité comme neg-risk : SELL bloqué, warning loggué.
            retry_count = self._allowance_retry_count.get(token_id, 0)
            if retry_count >= self._ALLOWANCE_MAX_RETRIES:
                logger.warning(
                    "[Allowance] Token %s: QUARANTAINE (%d tentatives sans succès) → "
                    "allowance persistante à 0 malgré update. Approuver manuellement "
                    "via l'UI Polymarket ou vérifier le contrat de ce token.",
                    token_id[:16], retry_count,
                )
                return False

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
                self._allowance_confirmed.add(token_id)
                # Réinitialiser le compteur de retries si l'allowance finit par apparaître
                self._allowance_retry_count.pop(token_id, None)
                return True

            # 2. Allowance insuffisante → déclencher l'approbation
            #    Incrémenter le compteur de retries pour ce token
            self._allowance_retry_count[token_id] = retry_count + 1
            logger.info(
                "[Allowance] Token %s: allowance=0 → approbation ERC-1155... "
                "(tentative %d/%d)",
                token_id[:16], retry_count + 1, self._ALLOWANCE_MAX_RETRIES,
            )
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
            )
            resp = self.client.update_balance_allowance(params)
            logger.info(
                "[Allowance] Token %s: approbation envoyée → %s", token_id[:16], resp
            )
            # NOTE : on NE met PAS en cache ici car allowance=0 persiste malgré update.
            # On retourne True pour laisser passer CE SELL (l'update vient juste d'être
            # envoyé, il peut mettre quelques secondes à propager). Si l'erreur 400 se
            # répète au cycle suivant, le compteur augmentera jusqu'à quarantaine.
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
