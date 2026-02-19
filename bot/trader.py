"""
Boucle principale du bot de trading — OBI Market Making.
Cycle toutes les 5 secondes (polling), gère :
  - High Water Mark + circuit breaker
  - Inventaire positions post-fill
  - CTF Inverse Spread Arb (Ask_YES + Ask_NO < 1.00)
  - Annulation et re-cotation des ordres
  - Arrêt propre SIGTERM/SIGINT
"""

import logging
import time
import signal as os_signal
import sys
from typing import Optional

from bot.config import AppConfig
from bot.polymarket_client import PolymarketClient
from bot.risk import RiskManager
from bot.strategy import BaseStrategy, OBIMarketMakingStrategy, Signal
from db.database import Database

logger = logging.getLogger("bot.trader")

# Intervalle de polling en secondes (override de config pour OBI)
OBI_POLL_INTERVAL = 5


class Trader:
    """Moteur principal du bot de trading OBI."""

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
        logger.info("DÉMARRAGE DU BOT POLYMARKET — OBI Market Making")
        logger.info("=" * 60)
        self.db.add_log("INFO", "trader", "Démarrage du bot OBI")

        os_signal.signal(os_signal.SIGTERM, self._handle_shutdown)
        os_signal.signal(os_signal.SIGINT, self._handle_shutdown)

        self._connect()

        # Stratégie OBI avec accès à la DB pour cooldowns et inventaire
        self.strategy = OBIMarketMakingStrategy(
            client=self.pm_client,
            db=self.db,
            order_size_usdc=self.config.bot.max_order_size / 2,  # 50% de max_order_size par ordre
            max_markets=5,
        )
        logger.info("Stratégie chargée: %s", type(self.strategy).__name__)
        self.db.add_log("INFO", "trader", f"Stratégie: {type(self.strategy).__name__}")

        self._running = True
        while self._running:
            try:
                self._cycle()
                self._consecutive_errors = 0
                time.sleep(OBI_POLL_INTERVAL)

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
                logger.warning("Tentative %d/%d échouée: %s",
                               attempt, self.config.bot.max_retries, e)
                self.db.add_log("WARNING", "trader",
                               f"Connexion échouée (tentative {attempt}): {e}")
                if attempt < self.config.bot.max_retries:
                    time.sleep(self.config.bot.retry_delay)

        logger.critical("Impossible de se connecter après %d tentatives.",
                        self.config.bot.max_retries)
        self.db.add_log("CRITICAL", "trader", "Échec de connexion – arrêt du bot")
        sys.exit(1)

    def _cycle(self):
        """Un cycle complet de la boucle de trading."""

        # 1. Kill switch
        if self.db.get_kill_switch():
            logger.debug("Kill switch activé – bot en pause.")
            return

        # 2. Connectivité API
        if not self.pm_client.is_alive():
            logger.warning("API Polymarket injoignable, tentative de reconnexion...")
            self.db.add_log("WARNING", "trader", "API injoignable – reconnexion")
            self._connect()

        # 3. Solde + High Water Mark
        balance = self._fetch_balance()
        if balance is not None:
            self.db.record_balance(balance)
            self.risk.update_high_water_mark(balance)
            logger.info("Solde: %.4f USDC | HWM: %.4f USDC",
                        balance, self.db.get_high_water_mark())
        else:
            balance = self.db.get_latest_balance()
            if balance is None:
                logger.info("Solde non disponible au démarrage. Utilisation de 0.0 USDC.")
                balance = 0.0
            else:
                logger.debug("Solde CLOB indisponible, DB: %.4f USDC", balance)

        # 4. CTF Inverse Spread Arb (opportuniste, avant les signaux principaux)
        self._check_ctf_arb(balance)

        # 5. Stratégie OBI → signaux
        signals = self.strategy.analyze()

        # 6. Exécution avec gestion de l'inventaire post-fill
        for sig in signals:
            self._execute_signal(sig, balance)

        logger.debug("Cycle OBI terminé. Prochain dans %ds.", OBI_POLL_INTERVAL)

    def _fetch_balance(self) -> Optional[float]:
        """Récupère le solde USDC via l'API CLOB (COLLATERAL)."""
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            resp = self.pm_client.client.get_balance_allowance(params)
            raw = resp.get("balance") or resp.get("Balance") or "0"
            return int(raw) / 1_000_000
        except Exception as e:
            logger.debug("Erreur lecture solde CLOB: %s", e)
            return None

    def _check_ctf_arb(self, balance: float):
        """
        CTF Inverse Spread Arbitrage :
        Si Ask(YES) + Ask(NO) < 1.00 USDC, acheter les deux tokens
        pour un gain quasi-certain (les deux valent 1.00 à résolution).
        Utilise le CLOB API directement (market orders FOK).
        """
        if balance < 2.0:
            return   # Solde trop faible

        try:
            from bot.strategy import MarketUniverse
            markets = MarketUniverse().get_eligible_markets()

            for market in markets[:10]:   # Checker les 10 premiers marchés
                ask_yes = self.pm_client.get_price(market.yes_token_id, side="buy")
                ask_no  = self.pm_client.get_price(market.no_token_id,  side="buy")

                if ask_yes is None or ask_no is None:
                    continue

                combined = ask_yes + ask_no
                if combined < 0.99:   # Marge de 1 cent pour frais
                    profit_est = (1.0 - combined) * min(balance * 0.02, 5.0)
                    logger.info(
                        "[CTF-ARB] Opportunité détectée: Ask_YES=%.4f + Ask_NO=%.4f = %.4f < 1.00 "
                        "sur '%s' (profit estimé: ~$%.3f)",
                        ask_yes, ask_no, combined, market.question[:40], profit_est,
                    )
                    self.db.add_log(
                        "INFO", "trader",
                        f"CTF-ARB: {market.question[:60]} | combined={combined:.4f}",
                    )
                    # Taille de l'arb : 2% du solde, max 5 USDC
                    arb_size = min(balance * 0.02, 5.0)
                    self._execute_ctf_arb(market, arb_size, ask_yes, ask_no, balance)

        except Exception as e:
            logger.debug("[CTF-ARB] Erreur: %s", e)

    def _execute_ctf_arb(self, market, arb_size: float,
                          ask_yes: float, ask_no: float, balance: float):
        """Exécute l'arb CTF en plaçant deux market orders (FOK)."""
        from bot.strategy import Signal
        yes_shares = round(arb_size / ask_yes, 2)
        no_shares  = round(arb_size / ask_no,  2)

        for token_id, shares, price, label in [
            (market.yes_token_id, yes_shares, ask_yes, "YES"),
            (market.no_token_id,  no_shares,  ask_no,  "NO"),
        ]:
            sig = Signal(
                token_id=token_id,
                market_id=market.market_id,
                market_question=market.question,
                side="buy",
                order_type="market",
                price=price,
                size=arb_size,
                confidence=0.95,
                reason=f"CTF-ARB combined={ask_yes+ask_no:.4f}",
            )
            self._execute_signal(sig, balance)

    def _execute_signal(self, signal: Signal, current_balance: float):
        """Vérifie le risque, exécute, met à jour l'inventaire."""
        verdict = self.risk.check(signal, current_balance)

        if not verdict.approved:
            logger.debug("Signal rejeté [%s]: %s", signal.side.upper(), verdict.reason)
            # On ne logue en DB que les rejets non-triviaux
            if verdict.action in ("cancel_bids", "liquidate", "kill_switch"):
                self.db.add_log("WARNING", "risk", f"Rejeté [{verdict.action}]: {verdict.reason}")
            return

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
            status = resp.get("status", "filled")

            self.db.update_order_status(local_id, status, order_id=order_id)
            self.db.add_log(
                "INFO", "trader",
                f"Ordre {signal.side.upper()} {signal.token_id[:16]} "
                f"@ {signal.price or 'market'} → {order_id} [{status}]",
            )

            # Mise à jour de l'inventaire après fill confirmé
            if status in ("filled", "matched"):
                qty_delta = signal.size if signal.side == "buy" else -signal.size
                self.db.update_position(
                    token_id=signal.token_id,
                    market_id=signal.market_id,
                    question=signal.market_question,
                    side="YES",
                    quantity_delta=qty_delta,
                    fill_price=signal.price or 0.5,
                )
                logger.info(
                    "Position mise à jour: %s %+.2f shares @ %.4f",
                    signal.token_id[:16], qty_delta, signal.price or 0.5,
                )

        except Exception as e:
            logger.error("Erreur exécution ordre: %s", e)
            self.db.update_order_status(local_id, "error", error=str(e))
            self.db.add_log("ERROR", "trader", f"Erreur ordre: {e}")

    def _handle_error(self, error: Exception):
        """Backoff exponentiel sur erreurs consécutives."""
        self._consecutive_errors += 1
        logger.error("Erreur cycle #%d: %s", self._consecutive_errors, error)
        self.db.add_log("ERROR", "trader",
                       f"Erreur cycle #{self._consecutive_errors}: {error}")

        if self._consecutive_errors >= self.config.bot.max_retries:
            pause = self.config.bot.retry_delay * 10
            logger.warning("%d erreurs consécutives. Pause de %ds...",
                           self._consecutive_errors, pause)
            time.sleep(pause)
            self._consecutive_errors = 0
            try:
                self._connect()
            except Exception:
                pass
        else:
            time.sleep(self.config.bot.retry_delay)

    def _handle_shutdown(self, signum, frame):
        """SIGTERM/SIGINT → arrêt propre."""
        logger.info("Signal d'arrêt reçu (%d). Arrêt propre...", signum)
        self._running = False

    def _shutdown(self):
        """Annule tous les ordres ouverts et ferme proprement."""
        logger.info("Arrêt du bot...")
        self.db.add_log("INFO", "trader", "Arrêt du bot")
        try:
            self.pm_client.cancel_all_orders()
            logger.info("Tous les ordres ouverts annulés.")
            self.db.add_log("INFO", "trader", "Ordres annulés à l'arrêt")
        except Exception as e:
            logger.warning("Erreur annulation ordres: %s", e)
        logger.info("Bot arrêté proprement.")
        logger.info("=" * 60)
