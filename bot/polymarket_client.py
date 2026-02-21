"""
Wrapper autour de py-clob-client.
Encapsule toute l'interaction avec l'API Polymarket :
authentification, lecture de marchés, passage et annulation d'ordres.
"""

import logging
import time as _time
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

# FIXED: Adresses on-chain Polygon corrigées (source : Polymarket docs + etherscan).
# CTF Exchange ERC-1155 (contrat shares, isApprovedForAll) :
#   0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
# NegRisk CTF Exchange :
#   0xC5d563A36AE78145C45a50134d48A1215220f80a
# FIXED: spender réel pour balance-allowance API (clé dans le dict retourné) :
#   0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E  ← CTF Exchange principal (CLOB)
_CTF_EXCHANGE_ADDRESS      = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_NEG_RISK_CTF_EXCHANGE     = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
_CTF_EXCHANGE_SPENDER      = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"  # FIXED: bon spender CLOB
_POLYGON_RPC               = "https://polygon-rpc.com"

# ABI minimal ERC-1155 — uniquement isApprovedForAll (lecture seule, sans gas).
_ERC1155_ABI_MINIMAL = [
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    }
]


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
        # On ne re-tente pas indéfiniment : après _ALLOWANCE_MAX_RETRIES tentatives,
        # le token est mis en quarantaine → SELL bloqué comme s'il était neg-risk.
        self._allowance_retry_count: dict[str, int] = {}
        self._ALLOWANCE_MAX_RETRIES = 2
        # FIXED: si BYPASS_QUARANTINE=true dans l'env, la quarantine n'est jamais
        # déclenchée — utile pour débloquer manuellement sans redémarrage.
        import os as _os
        self._bypass_quarantine: bool = _os.getenv("BYPASS_QUARANTINE", "").lower() == "true"
        if self._bypass_quarantine:
            logger.warning("[Allowance] BYPASS_QUARANTINE=true — quarantine désactivée.")
        # Tokens pour lesquels update_balance_allowance vient d'être envoyé.
        # La propagation blockchain est asynchrone (typiquement 5-30 secondes).
        # Pendant ce délai, tout SELL doit être différé (retour False) pour éviter
        # l'erreur 400 "not enough balance / allowance".
        # Structure : {token_id: timestamp_envoi_float}
        # Nettoyé automatiquement quand l'allowance est confirmée en GET.
        self._allowance_pending_since: dict[str, float] = {}
        # Durée minimale d'attente après update_balance_allowance avant de retenter
        # un SELL. 40 secondes = marge confortable pour la propagation blockchain.
        self._ALLOWANCE_PROPAGATION_DELAY_S: float = 40.0
        # FIXED: web3 instance lazy-init pour check on-chain isApprovedForAll.
        # Initialisé à la première utilisation pour ne pas bloquer le démarrage
        # si polygon-rpc.com est lent.
        self._w3 = None
        self._ctf_contract = None
        # Tokens où les 3 mécanismes de mid ont tous échoué (midpoint + bid/ask + last-trade).
        # Ces tokens sont marqués comme "inactifs" : marché non tradé, book totalement vide,
        # probablement en attente de résolution. Strategy.py skipe l'analyse OBI pour eux
        # (évite 1 requête get_order_book inutile par cycle par token inactif).
        # Reset à chaque redémarrage. Nettoyé si le token retrouve un mid valide.
        self._inactive_tokens: set[str] = set()

    def _get_w3(self):
        """Retourne l'instance Web3 (lazy-init, singleton par session).

        On se connecte à Polygon via RPC public. L'appel est synchrone et
        bloquant mais ne se fait qu'une fois par session (ou à la première
        tentative de vérif on-chain). Timeout implicite via les paramètres
        de connexion de la bibliothèque web3.py.
        """
        if self._w3 is None:
            try:
                from web3 import Web3
                w3 = Web3(Web3.HTTPProvider(
                    _POLYGON_RPC,
                    request_kwargs={"timeout": 10},
                ))
                if w3.is_connected():
                    self._w3 = w3
                    logger.debug("[Allowance/web3] Connecté à Polygon RPC.")
                else:
                    logger.warning("[Allowance/web3] Polygon RPC non disponible.")
            except ImportError:
                logger.warning("[Allowance/web3] web3 non installé — vérif on-chain désactivée.")
            except Exception as e:
                logger.warning("[Allowance/web3] Erreur init web3: %s", e)
        return self._w3

    def _check_onchain_approval(self, owner_address: str, is_neg_risk: bool = False) -> Optional[bool]:
        """Vérifie on-chain (Polygon) si l'opérateur Exchange a l'approbation ERC-1155.

        Logique ERC-1155 Polymarket :
          - Le contrat qui détient les shares (account = CTF Exchange) est toujours
            _CTF_EXCHANGE_ADDRESS (0x4D97...).
          - L'opérateur autorisé à transférer varie selon le type de marché :
            * Token standard   → opérateur = CTF Exchange     (0x4D97...)
            * Token neg-risk   → opérateur = NegRisk Exchange (0xC5d5...)
          - L'appel est : ctf_contract.isApprovedForAll(owner_wallet, operator)

        FIXED: la version précédente passait spender comme adresse du contrat
        ET comme opérateur simultanément — ce n'est correct que pour les tokens
        standard. Pour neg-risk, l'opérateur est _NEG_RISK_CTF_EXCHANGE.

        Retourne True / False / None (None = web3 indisponible, pas de blocage).
        """
        w3 = self._get_w3()
        if w3 is None:
            return None
        try:
            from web3 import Web3
            # FIXED: opérateur correct selon le type de token
            operator = _NEG_RISK_CTF_EXCHANGE if is_neg_risk else _CTF_EXCHANGE_ADDRESS
            # Le contrat ERC-1155 (qui stocke les shares) est toujours CTF Exchange
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(_CTF_EXCHANGE_ADDRESS),
                abi=_ERC1155_ABI_MINIMAL,
            )
            approved = contract.functions.isApprovedForAll(
                Web3.to_checksum_address(owner_address),
                Web3.to_checksum_address(operator),
            ).call()
            logger.info(
                "[Allowance/web3] isApprovedForAll(owner=%s…, operator=%s…) = %s "
                "(%s)",
                owner_address[:8], operator[:8], approved,
                "neg-risk" if is_neg_risk else "standard",
            )
            return bool(approved)
        except Exception as e:
            logger.debug("[Allowance/web3] Erreur isApprovedForAll: %s", e)
            return None

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

    def get_last_trade_price(self, token_id: str) -> Optional[float]:
        """Retourne le prix du dernier trade via /last-trade-price.

        Utile comme dernier recours quand /midpoint ET get_price() retournent
        None (book vide mais marché pas encore résolu — ex: faible activité).
        Retourne un float dans [0.01, 0.99] ou None.
        """
        try:
            # py-clob-client expose get_last_trade_price sur certaines versions
            result = self.client.get_last_trade_price(token_id)
            if result is None:
                return None
            # Certaines versions retournent un dict {"price": "0.53"}, d'autres un float
            if isinstance(result, dict):
                price_raw = result.get("price") or result.get("Price")
            else:
                price_raw = result
            if price_raw is None:
                return None
            price_f = float(price_raw)
            if 0.01 <= price_f <= 0.99:
                return price_f
            return None
        except Exception as e:
            logger.debug("[MidRobust] %s: /last-trade-price erreur: %s", token_id[:16], e)
            return None

    def get_midpoint_robust(self, token_id: str) -> Optional[float]:
        """Retourne le mid de marché via trois mécanismes en cascade.

        1. /midpoint — endpoint dédié, le plus direct.
           → None si book vide (marché inactif).
        2. (best_bid + best_ask) / 2 via get_price(BUY/SELL).
           → Couvre les books partiels (/midpoint vide mais un côté dispo).
        3. FIXED: /last-trade-price — dernier prix traité, stable même sur
           marchés inactifs. Loggue "inactive" si seul ce mécanisme réussit.
        4. None → l'appelant conserve le current_mid DB stale (TTL 5min).

        Retourne un float dans [0.01, 0.99] ou None.
        """
        # ── Tentative 1 : /midpoint ────────────────────────────────────────────
        try:
            mid = self.client.get_midpoint(token_id)
            if mid is not None:
                mid_f = float(mid)
                if 0.01 <= mid_f <= 0.99:
                    return mid_f
        except Exception as e:
            logger.debug("[MidRobust] %s: /midpoint erreur: %s", token_id[:16], e)

        # ── Tentative 2 : (bid + ask) / 2 via get_price ───────────────────────
        try:
            bid = self.client.get_price(token_id, side="BUY")
            ask = self.client.get_price(token_id, side="SELL")
            if bid is not None and ask is not None:
                bid_f, ask_f = float(bid), float(ask)
                if 0.01 <= bid_f <= 0.99 and 0.01 <= ask_f <= 0.99 and bid_f < ask_f:
                    mid_f = (bid_f + ask_f) / 2.0
                    logger.debug(
                        "[MidRobust] %s: /midpoint vide → bid/ask mid=%.4f",
                        token_id[:16], mid_f,
                    )
                    return mid_f
            # Un seul côté disponible
            val = bid or ask
            if val is not None:
                val_f = float(val)
                if 0.01 <= val_f <= 0.99:
                    logger.debug(
                        "[MidRobust] %s: un côté bid/ask (val=%.4f) → approx mid",
                        token_id[:16], val_f,
                    )
                    return val_f
        except Exception as e:
            logger.debug("[MidRobust] %s: get_price fallback erreur: %s", token_id[:16], e)

        # FIXED: Tentative 3 : /last-trade-price (marché inactif mais pas résolu)
        last = self.get_last_trade_price(token_id)
        if last is not None:
            logger.debug(
                "[MidRobust] %s: book vide → last-trade-price=%.4f (marché inactif)",
                token_id[:16], last,
            )
            return last

        return None  # Les trois mécanismes ont échoué → mid DB stale conservé

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
            result = self.client.get_balance_allowance(params) or {}
            logger.info("RAW balance-allowance %s: %s", token_id[:16], result)  # LOG ADDED
            return result
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

        Retourne True  → allowance active, SELL peut être soumis.
        Retourne False → SELL doit être différé :
          - token neg-risk (contrat NegRisk Exchange, approbation manuelle requise)
          - update_balance_allowance envoyé, propagation blockchain en cours
            (délai de _ALLOWANCE_PROPAGATION_DELAY_S secondes)
          - token en quarantaine (max retries dépassé)
          - erreur réseau ou API
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

            # 0c. Token en attente de propagation blockchain : update_balance_allowance
            #     a été envoyé récemment mais la blockchain n'a pas encore propagé
            #     l'autorisation. Différer le SELL pour éviter l'erreur 400.
            import time as _time
            pending_since = self._allowance_pending_since.get(token_id)
            if pending_since is not None:
                elapsed = _time.time() - pending_since
                if elapsed < self._ALLOWANCE_PROPAGATION_DELAY_S:
                    logger.info(
                        "[Allowance] Token %s: approbation ERC-1155 en cours de propagation "
                        "(%.0fs / %.0fs) → SELL différé au prochain cycle.",
                        token_id[:16], elapsed, self._ALLOWANCE_PROPAGATION_DELAY_S,
                    )
                    return False  # SELL différé, pas bloqué définitivement
                # Délai écoulé → vérifier si l'allowance est maintenant active
                logger.debug(
                    "[Allowance] Token %s: délai propagation écoulé (%.0fs), re-vérification...",
                    token_id[:16], elapsed,
                )
                # Continuer vers le GET ci-dessous pour confirmer

            # 0d. Quarantaine : token ayant dépassé _ALLOWANCE_MAX_RETRIES tentatives
            #     d'approbation sans que l'allowance n'apparaisse en GET (même après délai).
            #     Avant de bloquer définitivement, on vérifie on-chain via web3 :
            #     l'UI Polymarket peut avoir approuvé depuis le dernier retry.
            retry_count = self._allowance_retry_count.get(token_id, 0)
            if retry_count >= self._ALLOWANCE_MAX_RETRIES:
                # NEW: BYPASS_QUARANTINE=true → skip quarantine, laisser passer
                if self._bypass_quarantine:
                    logger.info(
                        "[Allowance] Token %s: %d tentatives mais BYPASS_QUARANTINE=true "
                        "→ quarantine ignorée, tentative SELL.",
                        token_id[:16], retry_count,
                    )
                    # On ne reset pas retry_count ici pour continuer à logger
                    # Passer directement au GET (étape 1 ci-dessous)

                else:
                    # FIXED: vérification on-chain avant quarantine définitive
                    # Les adresses correctes sont désormais _CTF_EXCHANGE_ADDRESS et
                    # _NEG_RISK_CTF_EXCHANGE (corrigées dans les constantes module).
                    owner = getattr(self._config, "funder_address", "") or ""
                    is_neg = token_id in self._neg_risk_confirmed
                    operator_addr = _NEG_RISK_CTF_EXCHANGE if is_neg else _CTF_EXCHANGE_ADDRESS
                    if owner:
                        onchain = self._check_onchain_approval(owner, is_neg_risk=is_neg)
                        if onchain is True:
                            # Approuvé on-chain (ex: via UI Polymarket) → reset quarantine
                            logger.info(
                                "[Allowance] Token %s: ON-CHAIN confirmé (isApprovedForAll=True, "
                                "operator=%s) → reset quarantine, SELL autorisé.",
                                token_id[:16], operator_addr[:10],
                            )
                            self._allowance_confirmed.add(token_id)
                            self._allowance_retry_count.pop(token_id, None)
                            self._allowance_pending_since.pop(token_id, None)
                            return True
                        elif onchain is False:
                            logger.warning(
                                "[Allowance] Token %s: QUARANTAINE (%d tentatives) + "
                                "isApprovedForAll=False on-chain → SELL bloqué. "
                                "Approuver dans l'UI Polymarket pour l'opérateur: %s. "
                                "Ou relancer avec BYPASS_QUARANTINE=true.",
                                token_id[:16], retry_count, operator_addr,
                            )
                            return False
                        # onchain is None → web3 indisponible, quarantine classique
                    logger.warning(
                        "[Allowance] Token %s: QUARANTAINE (%d tentatives, "
                        "vérif on-chain indisponible) → SELL bloqué. "
                        "Relancer avec BYPASS_QUARANTINE=true si UI approuvé.",
                        token_id[:16], retry_count,
                    )
                    return False

            # 1. Vérifier l'allowance actuelle via GET
            info = self.get_conditional_allowance(token_id)
            # FIXED: bon spender = _CTF_EXCHANGE_SPENDER (0x4bFb...) — clé réelle dans le dict API
            is_neg = token_id in self._neg_risk_confirmed
            spender = _NEG_RISK_CTF_EXCHANGE if is_neg else _CTF_EXCHANGE_SPENDER  # FIXED
            _raw = (
                info.get(spender)
                or info.get(spender.lower())
                or info.get(_CTF_EXCHANGE_SPENDER)
                or info.get(_CTF_EXCHANGE_SPENDER.lower())
                or info.get("allowance")
                or info.get("Allowance")
                or "0"
            )
            logger.info(
                "[Allowance] Token %s: raw for spender %s = %r",  # LOG
                token_id[:16], spender, _raw,
            )
            try:
                allowance = int(str(_raw).strip()) if str(_raw).strip() else 0
            except (ValueError, TypeError):
                allowance = 0

            # FORCE fix: uint256.max (2^256-1) ou >= 2^255 = unlimited approval
            if allowance >= 2**255 or allowance == 2**256 - 1:
                logger.info(
                    "[Allowance] Token %s: Unlimited max uint detected → confirmed (skip approbation).",
                    token_id[:16],
                )
                self._allowance_confirmed.add(token_id)
                self._allowance_pending_since.pop(token_id, None)
                self._allowance_retry_count.pop(token_id, None)
                return True

            if allowance > 0:
                logger.debug("[Allowance] Token %s: allowance OK (%s)", token_id[:16], _raw)
                self._allowance_confirmed.add(token_id)
                self._allowance_pending_since.pop(token_id, None)
                self._allowance_retry_count.pop(token_id, None)
                return True

            # 2. Allowance toujours à 0 → envoyer update_balance_allowance
            #    et marquer le token comme "pending" (propagation en cours).
            #    Le SELL sera différé jusqu'à _ALLOWANCE_PROPAGATION_DELAY_S secondes.
            self._allowance_retry_count[token_id] = retry_count + 1
            logger.info(
                "[Allowance] Token %s: allowance=0 → envoi approbation ERC-1155 "
                "(tentative %d/%d) — SELL différé %ds pour propagation.",
                token_id[:16], retry_count + 1, self._ALLOWANCE_MAX_RETRIES,
                int(self._ALLOWANCE_PROPAGATION_DELAY_S),
            )
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
            )
            resp = self.client.update_balance_allowance(params)
            logger.info(
                "[Allowance] Token %s: approbation envoyée → %s", token_id[:16], resp
            )
            # Enregistrer le timestamp d'envoi → le prochain appel verra le délai
            # et différera le SELL sans erreur 400.
            self._allowance_pending_since[token_id] = _time.time()
            # Retourner False : le SELL de ce cycle est différé (l'update vient d'être
            # envoyé, la propagation n'est pas encore active). Le cycle suivant,
            # le check "pending_since" prendra le relais jusqu'à expiration du délai.
            return False

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
