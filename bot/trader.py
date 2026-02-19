"""
Boucle principale du bot de trading.
Gère le cycle : vérification → analyse → risque → exécution → log.
Robuste face aux erreurs réseau avec reconnexion automatique.
"""

import json
import logging
import time
import signal as os_signal
import sys
import urllib.request
from typing import Optional

from bot.config import AppConfig
from bot.polymarket_client import PolymarketClient
from bot.risk import RiskManager
from bot.strategy import BaseStrategy, DummyStrategy, Signal
from db.database import Database

logger = logging.getLogger("bot.trader")


class Trader:
    """Moteur principal du bot de trading."""

    def __init__(self, config: AppConfig, db: Database):
        self.config = config
        self.db = db
        self.pm_client = PolymarketClient(config.polymarket)
        self.risk = RiskManager(config.bot, db)
        self.strategy: Optional[BaseStrategy] = None
        self._running = False
        self._consecutive_errors = 0

    def start(self):
        """Démarre la boucle de trading."""
        logger.info("=" * 60)
        logger.info("DÉMARRAGE DU BOT POLYMARKET")
        logger.info("=" * 60)
        self.db.add_log("INFO", "trader", "Démarrage du bot")

        # Gestion propre de l'arrêt via SIGTERM/SIGINT (Docker stop)
        os_signal.signal(os_signal.SIGTERM, self._handle_shutdown)
        os_signal.signal(os_signal.SIGINT, self._handle_shutdown)

        # Connexion initiale
        self._connect()

        # Initialise la stratégie
        self.strategy = DummyStrategy(self.pm_client)
        logger.info("Stratégie chargée: %s", type(self.strategy).__name__)
        self.db.add_log("INFO", "trader", f"Stratégie: {type(self.strategy).__name__}")

        # Boucle principale
        self._running = True
        while self._running:
            try:
                self._cycle()
                self._consecutive_errors = 0
                time.sleep(self.config.bot.loop_interval)

            except KeyboardInterrupt:
                logger.info("Arrêt demandé par l'utilisateur (Ctrl+C)")
                break

            except Exception as e:
                self._handle_error(e)

        self._shutdown()

    def _connect(self):
        """Connexion au CLOB avec retry."""
        for attempt in range(1, self.config.bot.max_retries + 1):
            try:
                self.pm_client.connect()
                server_time = self.pm_client.get_server_time()
                logger.info("API connectée. Heure serveur: %s", server_time)
                self.db.add_log("INFO", "trader", "Connecté à Polymarket")
                return
            except Exception as e:
                logger.warning(
                    "Tentative %d/%d échouée: %s",
                    attempt, self.config.bot.max_retries, e,
                )
                self.db.add_log(
                    "WARNING", "trader",
                    f"Connexion échouée (tentative {attempt}): {e}",
                )
                if attempt < self.config.bot.max_retries:
                    time.sleep(self.config.bot.retry_delay)

        logger.critical("Impossible de se connecter après %d tentatives.", self.config.bot.max_retries)
        self.db.add_log("CRITICAL", "trader", "Échec de connexion – arrêt du bot")
        sys.exit(1)

    def _cycle(self):
        """Un cycle complet de la boucle de trading."""

        # 1. Vérifier le kill switch
        if self.db.get_kill_switch():
            logger.info("Kill switch activé – bot en pause.")
            self.db.add_log("INFO", "trader", "Cycle ignoré: kill switch actif")
            return

        # 2. Vérifier la connectivité API
        if not self.pm_client.is_alive():
            logger.warning("API Polymarket injoignable, tentative de reconnexion...")
            self.db.add_log("WARNING", "trader", "API injoignable – reconnexion")
            self._connect()

        # 3. Récupérer et enregistrer le solde
        balance = self._fetch_balance()
        if balance is not None:
            self.db.record_balance(balance)
            logger.info("Solde actuel: %.6f USDC", balance)
        else:
            # Fallback : dernier solde connu ou 0 si premier démarrage
            balance = self.db.get_latest_balance()
            if balance is None:
                logger.info("Solde non disponible (premier démarrage ou RPC inaccessible). Utilisation de 0.0 USDC.")
                balance = 0.0
            else:
                logger.debug("Solde RPC indisponible, utilisation du dernier solde DB: %.6f USDC", balance)

        # 4. Exécuter la stratégie
        signals = self.strategy.analyze()

        # 5. Pour chaque signal, vérifier le risque et exécuter
        for sig in signals:
            self._execute_signal(sig, balance)

        logger.debug("Cycle terminé. Prochain dans %ds.", self.config.bot.loop_interval)

    def _fetch_balance(self) -> Optional[float]:
        """
        Récupère le solde USDC sur Polygon via JSON-RPC.
        Essaie USDC natif (0x3c499c...) puis USDC.e bridgé (0x2791Bc...).
        Les deux ont 6 décimales.
        Retourne la somme des deux soldes.
        """
        _RPC_URLS = [
            "https://polygon-rpc.com",
            "https://rpc.ankr.com/polygon",
        ]
        _USDC_CONTRACTS = [
            ("USDC natif",   "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"),
            ("USDC.e bridgé","0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"),
        ]
        _BALANCEOF_SIG = "0x70a08231"  # keccak256("balanceOf(address)")[:4]

        def _call_rpc(rpc_url: str, contract: str, data: str) -> Optional[str]:
            payload = json.dumps({
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": contract, "data": data}, "latest"],
                "id": 1,
            }).encode()
            req = urllib.request.Request(
                rpc_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=8) as resp:
                body = json.loads(resp.read().decode())
                if "error" in body:
                    logger.debug("RPC error: %s", body["error"])
                    return None
                return body.get("result")

        try:
            from eth_account import Account
            wallet_addr = Account.from_key(self.config.polymarket.private_key).address
            addr_padded = wallet_addr[2:].lower().zfill(64)
            call_data = _BALANCEOF_SIG + addr_padded

            total = 0.0
            any_success = False

            for label, contract in _USDC_CONTRACTS:
                # Essayer chaque RPC en fallback
                result = None
                last_err = None
                for rpc_url in _RPC_URLS:
                    try:
                        result = _call_rpc(rpc_url, contract, call_data)
                        if result is not None:
                            break
                    except Exception as e:
                        last_err = e
                        continue

                if result and result != "0x":
                    try:
                        amount = int(result, 16) / 1_000_000
                        logger.debug("Solde %s: %.4f USDC (wallet %s)", label, amount, wallet_addr[:10])
                        total += amount
                        any_success = True
                    except ValueError as e:
                        logger.debug("Impossible de parser le résultat RPC (%s): %s", result, e)
                elif last_err:
                    logger.debug("RPC inaccessible pour %s: %s", label, last_err)

            if any_success:
                return total

            logger.warning("Tous les appels RPC ont échoué, utilisation du solde DB.")
            return self.db.get_latest_balance()

        except Exception as e:
            logger.warning("Erreur inattendue lecture solde: %s", e)
            return self.db.get_latest_balance()

    def _execute_signal(self, signal: Signal, current_balance: float):
        """Vérifie le risque et exécute un signal."""
        verdict = self.risk.check(signal, current_balance)
        if not verdict.approved:
            logger.info("Signal rejeté: %s – %s", signal.token_id[:16], verdict.reason)
            self.db.add_log("INFO", "risk", f"Rejeté: {verdict.reason}")
            self.db.record_order(
                market_id=signal.market_id,
                token_id=signal.token_id,
                side=signal.side,
                order_type=signal.order_type,
                price=signal.price,
                size=signal.size,
                amount_usdc=signal.size * (signal.price or 1.0),
                status="rejected",
                error=verdict.reason,
            )
            return

        # Passage de l'ordre
        local_id = self.db.record_order(
            market_id=signal.market_id,
            token_id=signal.token_id,
            side=signal.side,
            order_type=signal.order_type,
            price=signal.price,
            size=signal.size,
            amount_usdc=signal.size * (signal.price or 1.0),
            status="submitted",
        )

        try:
            if signal.order_type == "limit":
                resp = self.pm_client.place_limit_order(
                    token_id=signal.token_id,
                    price=signal.price,
                    size=signal.size,
                    side=signal.side,
                )
            else:
                resp = self.pm_client.place_market_order(
                    token_id=signal.token_id,
                    amount=signal.size,
                    side=signal.side,
                )

            order_id = resp.get("orderID") or resp.get("id") or str(resp)
            self.db.update_order_status(local_id, "filled", order_id=order_id)
            self.db.add_log(
                "INFO", "trader",
                f"Ordre exécuté: {signal.side.upper()} {signal.token_id[:16]} → {order_id}",
            )

        except Exception as e:
            logger.error("Erreur exécution ordre: %s", e)
            self.db.update_order_status(local_id, "error", error=str(e))
            self.db.add_log("ERROR", "trader", f"Erreur ordre: {e}")

    def _handle_error(self, error: Exception):
        """Gère les erreurs de la boucle principale avec backoff."""
        self._consecutive_errors += 1
        logger.error(
            "Erreur cycle (consécutive #%d): %s",
            self._consecutive_errors, error,
        )
        self.db.add_log(
            "ERROR", "trader",
            f"Erreur cycle #{self._consecutive_errors}: {error}",
        )

        if self._consecutive_errors >= self.config.bot.max_retries:
            # Pause longue après trop d'erreurs consécutives
            pause = self.config.bot.retry_delay * 10
            logger.warning(
                "%d erreurs consécutives. Pause longue de %ds...",
                self._consecutive_errors, pause,
            )
            self.db.add_log(
                "WARNING", "trader",
                f"Pause longue de {pause}s après {self._consecutive_errors} erreurs",
            )
            time.sleep(pause)
            self._consecutive_errors = 0
            # Tente une reconnexion
            try:
                self._connect()
            except Exception:
                pass
        else:
            time.sleep(self.config.bot.retry_delay)

    def _handle_shutdown(self, signum, frame):
        """Handler pour SIGTERM/SIGINT (arrêt propre via Docker)."""
        logger.info("Signal d'arrêt reçu (signal %d). Arrêt propre...", signum)
        self._running = False

    def _shutdown(self):
        """Nettoyage à l'arrêt."""
        logger.info("Arrêt du bot...")
        self.db.add_log("INFO", "trader", "Arrêt du bot")

        # Annuler les ordres ouverts par sécurité
        try:
            self.pm_client.cancel_all_orders()
            logger.info("Tous les ordres ouverts annulés.")
            self.db.add_log("INFO", "trader", "Ordres ouverts annulés à l'arrêt")
        except Exception as e:
            logger.warning("Erreur annulation ordres à l'arrêt: %s", e)

        logger.info("Bot arrêté proprement.")
        logger.info("=" * 60)
