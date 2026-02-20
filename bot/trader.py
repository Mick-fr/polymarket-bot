"""
Boucle principale du bot de trading — OBI Market Making.
Cycle toutes les 8 secondes (Set B), gere :
  - High Water Mark + circuit breaker
  - Reconciliation fills manques (get_order CLOB avant cancel)
  - Inventaire positions post-fill
  - CTF Inverse Spread Arb (Ask_YES + Ask_NO < 1.00)
  - Cancel systematique + re-cotation des ordres
  - Arret propre SIGTERM/SIGINT
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

# Intervalle de polling en secondes — Set B : 8s (ex: 5s)
# Trade-off gas vs latency : a 5s avec 5 marches = ~78$/j de gas.
# A 8s = ~49$/j. Le cancel conditionnel (Tweak 1) reduit encore de ~60%.
OBI_POLL_INTERVAL = 8


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
        # NOTE: le tracking des SELL de liquidation est maintenant basé sur la DB
        # (orders WHERE side='sell' AND status='live' AND token a une position),
        # ce qui survit aux redémarrages. Plus de set en mémoire.

    def start(self):
        """Démarre la boucle de trading."""
        logger.info("=" * 60)
        logger.info("DÉMARRAGE DU BOT POLYMARKET — OBI Market Making")
        logger.info("=" * 60)
        self.db.add_log("INFO", "trader", "Démarrage du bot OBI")

        if self.config.bot.paper_trading:
            logger.warning("=" * 60)
            logger.warning("MODE PAPER TRADING ACTIF — Aucun ordre réel ne sera passé")
            logger.warning("Solde fictif initial : %.2f USDC", self.config.bot.paper_balance)
            logger.warning("=" * 60)
            self.db.add_log("WARNING", "trader", "MODE PAPER TRADING ACTIF")
            if self.db.get_latest_balance() is None:
                self.db.record_balance(self.config.bot.paper_balance)

        os_signal.signal(os_signal.SIGTERM, self._handle_shutdown)
        os_signal.signal(os_signal.SIGINT, self._handle_shutdown)

        self._connect()

        # Reset HWM au démarrage pour éviter un circuit breaker immédiat
        # (le solde peut avoir changé entre deux sessions via fills ou dépôts)
        balance = self._fetch_balance()
        if balance is not None:
            old_hwm = self.db.get_high_water_mark()
            self.db.reset_high_water_mark()
            self.db.update_high_water_mark(balance)
            self.db.record_balance(balance)
            logger.info("HWM réinitialisé: %.2f → %.2f USDC (solde actuel)", old_hwm, balance)
            self.db.add_log("INFO", "trader", f"HWM reset: {old_hwm:.2f} → {balance:.2f}")

        # Stratégie OBI avec accès à la DB pour cooldowns et inventaire
        self.strategy = OBIMarketMakingStrategy(
            client=self.pm_client,
            db=self.db,
            max_order_size_usdc=self.config.bot.max_order_size,
            max_markets=5,
            paper_trading=self.config.bot.paper_trading,
            max_exposure_pct=self.config.bot.max_exposure_pct,
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
        logger.info("[Cycle] ── Début du cycle ──────────────────────────────")

        # 1. Kill switch
        logger.debug("[Cycle] Étape 1: kill switch")
        if self.db.get_kill_switch():
            logger.debug("Kill switch activé – bot en pause.")
            return

        # 2. Connectivité API — is_alive() avec timeout implicite de la lib
        logger.info("[Cycle] Étape 2: vérification connectivité API")
        if not self.pm_client.is_alive():
            logger.warning("API Polymarket injoignable, tentative de reconnexion...")
            self.db.add_log("WARNING", "trader", "API injoignable – reconnexion")
            self._connect()
        logger.info("[Cycle] Étape 2: API OK")

        # 3. Solde brut initial (avant cancel) — sert à calculer la valeur portfolio
        logger.info("[Cycle] Étape 3: lecture solde brut CLOB")
        balance_raw = self._fetch_balance()
        if balance_raw is None:
            balance_raw = self.db.get_latest_balance() or 0.0
        logger.info("[Cycle] Étape 3: solde brut = %.4f USDC", balance_raw)

        # 3b. Valeur totale du portfolio (USDC + inventaire au prix d'entrée)
        # → utilisée pour le High Water Mark et le circuit breaker.
        # Évite de déclencher le CB lors d'un BUY normal (argent converti en shares, pas perdu).
        # Uniquement DB locale : ne peut PAS bloquer sur réseau.
        logger.info("[Cycle] Étape 3b: calcul valeur portfolio (DB locale)")
        portfolio_value = self._compute_portfolio_value(balance_raw)
        self.risk.update_high_water_mark(portfolio_value)
        logger.info(
            "[Cycle] Étape 3b: portfolio=%.4f USDC (USDC=%.4f + inventaire) | HWM=%.4f",
            portfolio_value, balance_raw, self.db.get_high_water_mark(),
        )

        # 4. Cancel+replace : annuler les BUY ouverts AVANT de lire le solde disponible.
        # Cela libère les fonds verrouillés côté CLOB, permettant une lecture exacte.
        # (seulement en mode réel — en paper trading il n'y a pas d'ordres dans le carnet)
        logger.info("[Cycle] Étape 4: cancel+replace (mode réel)")
        if not self.config.bot.paper_trading:
            self._cancel_open_orders()
        logger.info("[Cycle] Étape 4: cancel+replace terminé")

        # 5. Solde disponible (après cancel → fonds BUY libérés dans le CLOB)
        # En cas d'échec du re-fetch, on soustrait le capital estimé verrouillé.
        logger.info("[Cycle] Étape 5: lecture solde disponible post-cancel")
        balance = self._fetch_available_balance(balance_raw)
        self.db.record_balance(balance)
        logger.info(
            "[Cycle] Étape 5: solde dispo=%.4f USDC | portfolio=%.4f USDC | HWM=%.4f USDC",
            balance, portfolio_value, self.db.get_high_water_mark(),
        )

        # 6. Stratégie OBI → signaux (balance passée pour sizing dynamique)
        #    Récupérer les marchés éligibles pour les partager avec CTF arb
        logger.info("[Cycle] Étape 6: analyse OBI → signaux")
        signals = self.strategy.analyze(balance=balance)
        logger.info("[Cycle] Étape 6: %d signal(s) généré(s)", len(signals))

        # 7. CTF Inverse Spread Arb (réutilise les marchés déjà chargés par la stratégie)
        logger.info("[Cycle] Étape 7: CTF arb check")
        eligible_markets = self.strategy.get_eligible_markets() if self.strategy else []
        self._check_ctf_arb(balance, eligible_markets)

        # 8. Exécution avec gestion de l'inventaire post-fill
        logger.info("[Cycle] Étape 8: exécution %d signal(s)", len(signals))
        for sig in signals:
            self._execute_signal(sig, balance, portfolio_value=portfolio_value)

        # 9. Snapshot de stratégie pour analytics
        logger.info("[Cycle] Étape 9: snapshot analytics")
        try:
            from bot.strategy import (OBI_BULLISH_THRESHOLD, OBI_SKEW_FACTOR,
                                       MIN_SPREAD, ORDER_SIZE_PCT)
            positions = self.db.get_all_positions()
            net_exposure = sum(p.get("quantity", 0) * (p.get("avg_price", 0) or 0)
                               for p in positions)
            # Compter ordres du cycle : submitted/matched/filled = exécutés, rejected = rejetés
            recent = self.db.get_recent_orders(limit=len(signals) * 2) if signals else []
            cycle_executed = sum(1 for o in recent if o.get("status") in ("filled", "matched", "live", "submitted"))
            cycle_rejected = sum(1 for o in recent if o.get("status") == "rejected")
            self.db.record_strategy_snapshot(
                obi_threshold=OBI_BULLISH_THRESHOLD,
                skew_factor=OBI_SKEW_FACTOR,
                min_spread=MIN_SPREAD,
                order_size_pct=ORDER_SIZE_PCT,
                balance_usdc=balance,
                total_positions=len(positions),
                net_exposure_usdc=net_exposure,
                markets_scanned=len(eligible_markets),
                signals_generated=len(signals),
                signals_executed=len(signals),
                signals_rejected=0,
            )
        except Exception as se:
            logger.debug("[Analytics] Erreur snapshot: %s", se)

        logger.info("[Cycle] ── Cycle terminé. Prochain dans %ds. ──────────", OBI_POLL_INTERVAL)

    def _reconcile_fills(self):
        """
        Réconciliation des fills manqués.
        Vérifie chaque ordre 'live' dans la DB via l'API CLOB.
        Si un ordre a été matché entre deux cycles, met à jour l'inventaire.
        """
        live_orders = self.db.get_live_orders()
        if not live_orders:
            return

        reconciled = 0
        for order in live_orders:
            clob_id = order.get("order_id")
            if not clob_id:
                continue

            try:
                clob_order = self.pm_client.get_order(clob_id)
                if clob_order is None:
                    continue

                status = clob_order.get("status", "")
                if status in ("matched", "filled"):
                    # Fill détecté ! Mettre à jour la DB et l'inventaire
                    local_id = order["id"]
                    token_id = order["token_id"]
                    market_id = order["market_id"]
                    side = order["side"]
                    size = order["size"]
                    price = order["price"] or 0.5

                    self.db.update_order_status(local_id, "matched", order_id=clob_id)

                    # Mise à jour inventaire
                    if side == "sell":
                        qty_held = self.db.get_position(token_id)
                        actual_size = min(size, max(0.0, qty_held))
                        if actual_size <= 0:
                            continue
                    else:
                        actual_size = size

                    qty_delta = actual_size if side == "buy" else -actual_size
                    self.db.update_position(
                        token_id=token_id,
                        market_id=market_id,
                        question=order.get("market_question", ""),
                        side="YES",
                        quantity_delta=qty_delta,
                        fill_price=price,
                    )
                    reconciled += 1
                    logger.info(
                        "[Reconcile] Fill detecte: %s %s %+.2f shares @ %.4f (%s)",
                        side.upper(), token_id[:16], qty_delta, price, clob_id[:16],
                    )
                    self.db.add_log(
                        "INFO", "trader",
                        f"[Reconcile] Fill: {side.upper()} {token_id[:16]} "
                        f"{qty_delta:+.2f} @ {price:.4f}",
                    )

                elif status in ("canceled", "cancelled"):
                    self.db.update_order_status(order["id"], "cancelled", order_id=clob_id)

            except Exception as e:
                logger.debug("[Reconcile] Erreur check %s: %s", clob_id[:16] if clob_id else "?", e)

        if reconciled > 0:
            logger.info("[Reconcile] %d fill(s) reconcilie(s) depuis le CLOB.", reconciled)

    def _get_liquidation_clob_ids(self) -> set[str]:
        """
        Retourne les order_ids CLOB des SELL de liquidation actifs depuis la DB.
        Un SELL est "de liquidation" si :
          - status = 'live' dans la table orders
          - side = 'sell'
          - le token a encore une position > 0 dans positions
        Basé sur la DB → survit aux redémarrages du bot.
        """
        try:
            live_sells = [
                o for o in self.db.get_live_orders()
                if o.get("side") == "sell" and o.get("order_id")
            ]
            liq_ids = set()
            for o in live_sells:
                qty = self.db.get_position(o["token_id"])
                if qty > 0:
                    liq_ids.add(o["order_id"])
            return liq_ids
        except Exception as e:
            logger.debug("[Liquidation] Erreur lecture DB: %s", e)
            return set()

    def _cancel_open_orders(self):
        """
        Cancel sélectif avant chaque cycle de cotation (live uniquement).
        Étape 1 : réconcilier les fills manqués.
        Étape 2 : annuler tous les ordres SAUF les SELL de liquidation en attente.

        Les SELL de liquidation sont identifiés via la DB (orders live + position > 0),
        ce qui survit aux redémarrages — aucune variable en mémoire n'est nécessaire.
        """
        # D'abord réconcilier les éventuels fills entre deux cycles
        self._reconcile_fills()

        try:
            open_orders = self._call_with_timeout(
                self.pm_client.get_open_orders,
                timeout=15.0,
                label="get_open_orders",
            )
            if not open_orders:
                logger.debug("[Cancel+Replace] Aucun ordre ouvert.")
                return

            # Récupérer les SELL de liquidation à préserver (depuis la DB)
            liquidation_ids = self._get_liquidation_clob_ids()

            # Séparer : ordres à annuler vs SELL de liquidation à préserver
            to_cancel = []
            preserved = []
            for o in open_orders:
                # L'API CLOB retourne un dict avec 'id' comme clé principale
                clob_id = (
                    o.get("id") or o.get("orderID") or o.get("order_id") or ""
                )
                if clob_id and clob_id in liquidation_ids:
                    preserved.append(clob_id)
                else:
                    to_cancel.append(clob_id)

            if preserved:
                logger.info(
                    "[Cancel+Replace] %d SELL de liquidation préservé(s): %s",
                    len(preserved),
                    [oid[:16] for oid in preserved],
                )

            if not to_cancel:
                logger.debug("[Cancel+Replace] Aucun ordre à annuler (tous préservés).")
                return

            logger.info(
                "[Cancel+Replace] %d ordre(s) -> annulation de %d, préservé: %d",
                len(open_orders), len(to_cancel), len(preserved),
            )

            if len(to_cancel) == len(open_orders):
                # Aucun SELL à préserver : cancel_all plus rapide (1 requête)
                self.pm_client.cancel_all_orders()
            else:
                # Annulation sélective : on ne cancel que les non-liquidation
                valid_ids = [oid for oid in to_cancel if oid]
                if valid_ids:
                    self.pm_client.client.cancel_orders(valid_ids)
                    logger.info("[Cancel+Replace] Annulés sélectivement: %s",
                                [oid[:16] for oid in valid_ids])

            self.db.add_log(
                "INFO", "trader",
                f"[Cancel+Replace] {len(to_cancel)} annulé(s), {len(preserved)} préservé(s)",
            )
        except Exception as e:
            logger.warning("[Cancel+Replace] Erreur annulation: %s", e)

    def _compute_portfolio_value(self, usdc_balance: float) -> float:
        """
        Calcule la valeur totale du portefeuille en USDC :
          Portfolio = USDC liquides + valeur de l'inventaire (shares × avg_price)

        Utilisé exclusivement pour le High Water Mark et le circuit breaker,
        afin d'éviter les faux déclenchements lors d'un BUY normal.
        Exemple : solde 32 USDC + 10 shares @ 0.45 = 32 + 4.5 = 36.5 USDC.

        Note : les shares sont valorisées au prix d'entrée (avg_price) et non
        au prix de marché, pour éviter des fluctuations HWM trop fréquentes.
        En cas d'erreur DB, retourne usdc_balance (mode dégradé safe).
        """
        try:
            positions = self.db.get_all_positions()
            inventory_value = sum(
                float(p.get("quantity", 0)) * float(p.get("avg_price") or 0)
                for p in positions
            )
            total = usdc_balance + inventory_value
            if inventory_value > 0:
                logger.debug(
                    "[Portfolio] USDC=%.4f + inventaire=%.4f → total=%.4f USDC "
                    "(%d position(s))",
                    usdc_balance, inventory_value, total, len(positions),
                )
            return total
        except Exception as e:
            logger.debug("[Portfolio] Erreur calcul valeur portfolio: %s", e)
            return usdc_balance

    def _call_with_timeout(self, fn, timeout: float = 15.0, label: str = ""):
        """
        Exécute fn() dans un Thread daemon avec un timeout strict.
        Si fn() ne répond pas dans `timeout` secondes, le thread est abandonné
        (daemon=True → ne bloque pas l'arrêt du processus) et une RuntimeError est levée.

        NE PAS utiliser ThreadPoolExecutor : son __exit__ attend la fin du thread
        même après le timeout du future → blocage identique au problème initial.
        """
        import threading
        result = []
        exc_holder = []

        def _run():
            try:
                result.append(fn())
            except Exception as e:
                exc_holder.append(e)

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=timeout)

        if t.is_alive():
            # Thread toujours bloqué → timeout dépassé
            logger.warning("[Timeout] Appel CLOB '%s' dépassé (%.0fs) → abandon",
                           label or getattr(fn, "__name__", str(fn)), timeout)
            raise RuntimeError(f"Timeout CLOB ({timeout}s): {label}")

        if exc_holder:
            raise exc_holder[0]

        if result:
            return result[0]
        return None

    def _fetch_balance(self) -> Optional[float]:
        """Récupère le solde USDC brut (total, fonds verrouillés inclus).

        En paper trading, le solde retourné est le cash résiduel uniquement.
        Les positions sont de l'inventaire, pas du cash disponible. Valoriser
        les positions au prix d'entrée créerait un double-comptage car chaque
        BUY débite déjà le cash et chaque SELL le crédite.
        """
        if self.config.bot.paper_trading:
            return self.db.get_latest_balance() or self.config.bot.paper_balance

        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)

            def _do_fetch():
                resp = self.pm_client.client.get_balance_allowance(params)
                raw = resp.get("balance") or resp.get("Balance") or "0"
                return int(raw) / 1_000_000

            return self._call_with_timeout(_do_fetch, timeout=15.0, label="get_balance_allowance")
        except Exception as e:
            logger.debug("Erreur lecture solde CLOB: %s", e)
            return None

    def _compute_locked_capital(self) -> float:
        """
        Estime le capital verrouillé dans les ordres BUY limit encore ouverts.
        Utilise la DB locale (orders WHERE status='live' AND side='buy').
        Utilisé en fallback si le re-fetch CLOB échoue après cancel.
        Retourne 0.0 en cas d'erreur (safe — risque de légère surestimation du solde).
        """
        try:
            live_buys = [
                o for o in self.db.get_live_orders()
                if o.get("side") == "buy"
            ]
            locked = sum(float(o.get("amount_usdc") or 0.0) for o in live_buys)
            if locked > 0:
                logger.debug("[Balance] Capital verrouillé dans %d BUY live: %.4f USDC",
                             len(live_buys), locked)
            return locked
        except Exception as e:
            logger.debug("[Balance] Erreur calcul capital verrouillé: %s", e)
            return 0.0

    def _fetch_available_balance(self, balance_before_cancel: float) -> float:
        """
        Retourne le solde USDC disponible pour de nouveaux ordres.

        Logique :
          1. En paper trading : retourner le cash résiduel de la DB.
          2. En live : re-fetcher le solde CLOB APRÈS le cancel des BUY.
             → Les fonds sont libérés côté CLOB, le solde reflète le réel disponible.
          3. Fallback (si re-fetch CLOB échoue) : balance_avant_cancel − locked_capital.

        Args:
            balance_before_cancel: solde brut lu avant le cancel+replace.
        """
        if self.config.bot.paper_trading:
            return self.db.get_latest_balance() or self.config.bot.paper_balance

        # Tentative de re-fetch après cancel (fonds devraient être libérés)
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)

            def _do_refetch():
                resp = self.pm_client.client.get_balance_allowance(params)
                raw = resp.get("balance") or resp.get("Balance") or "0"
                return int(raw) / 1_000_000

            available = self._call_with_timeout(_do_refetch, timeout=15.0,
                                                label="get_balance_allowance (post-cancel)")
            logger.debug("[Balance] Solde CLOB après cancel: %.4f USDC", available)
            return available
        except Exception as e:
            logger.debug("[Balance] Re-fetch CLOB échoué (%s), fallback DB - locked", e)

        # Fallback : solde avant cancel − capital encore verrouillé (ordres SELL préservés)
        locked = self._compute_locked_capital()
        available = max(0.0, balance_before_cancel - locked)
        logger.debug("[Balance] Solde disponible (fallback): %.4f - %.4f = %.4f USDC",
                     balance_before_cancel, locked, available)
        return available

    def _check_ctf_arb(self, balance: float, markets: list = None):
        """
        CTF Inverse Spread Arbitrage :
        Si Ask(YES) + Ask(NO) < 1.00 USDC, acheter les deux tokens
        pour un gain quasi-certain (les deux valent 1.00 à résolution).
        Utilise le CLOB API directement (market orders FOK).
        Réutilise les marchés déjà chargés par la stratégie (pas de double appel Gamma).
        """
        if balance < 2.0:
            return   # Solde trop faible

        try:
            if not markets:
                return

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

    def _execute_signal(self, signal: Signal, current_balance: float,
                        portfolio_value: float = 0.0):
        """Vérifie le risque, exécute, met à jour l'inventaire."""
        verdict = self.risk.check(signal, current_balance, portfolio_value=portfolio_value)

        if not verdict.approved:
            logger.info(
                "Signal rejeté [%s %s @ %.4f]: %s",
                signal.side.upper(), signal.token_id[:16],
                signal.price or 0.0, verdict.reason,
            )
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
            if self.config.bot.paper_trading:
                resp = self._simulate_order(signal)
            elif signal.order_type == "limit":
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

            # ── Normalisation du statut CLOB ───────────────────────────────────
            # Paper trading : fill simulé immédiat → "filled"
            # Réel (limite GTC) : Polymarket retourne "live" (posé dans le carnet),
            #                     "matched" (partiellement/totalement exécuté),
            #                     "delayed" (file d'attente matching engine).
            # On ne met à jour l'inventaire que sur fill confirmé (matched).
            # Un ordre "live" reste ouvert → sera annulé au prochain cancel+replace.
            raw_status = resp.get("status", "")
            if self.config.bot.paper_trading:
                status = "filled"       # Simulation : fill instantané
            elif raw_status in ("matched", "filled"):
                status = "matched"      # Fill confirmé côté CLOB
            elif raw_status == "live":
                status = "live"         # Ordre posé, pas encore matché
            elif raw_status == "delayed":
                status = "delayed"      # En file, traiter comme live
            else:
                # Statut inconnu ou vide → on suppose "live" (conservateur)
                status = "live"
                logger.warning(
                    "Statut ordre inconnu '%s' pour %s → traité comme 'live'",
                    raw_status, order_id,
                )

            self.db.update_order_status(local_id, status, order_id=order_id)
            self.db.add_log(
                "INFO", "trader",
                f"Ordre {signal.side.upper()} {signal.token_id[:16]} "
                f"@ {signal.price or 'market'} → {order_id} [{status}]",
            )

            if status == "live":
                logger.info(
                    "Ordre posé dans le carnet (live): %s %s @ %.4f → %s",
                    signal.side.upper(), signal.token_id[:16],
                    signal.price or 0.0, order_id,
                )
                # Pas de mise à jour d'inventaire : l'ordre n'est pas encore rempli.
                # Le SELL de liquidation sera détecté via la DB au prochain cycle
                # (_get_liquidation_clob_ids) et préservé du cancel+replace.
                if signal.side == "sell" and order_id:
                    logger.info(
                        "[Liquidation] SELL posé dans le carnet, sera préservé via DB: %s",
                        order_id[:16],
                    )
                return

            # Mise à jour de l'inventaire après fill confirmé (matched ou paper filled)

            if status in ("filled", "matched"):
                # Plafonner le SELL à la quantité réellement détenue (évite positions négatives)
                if signal.side == "sell":
                    qty_held = self.db.get_position(signal.token_id)
                    actual_size = min(signal.size, max(0.0, qty_held))
                    if actual_size <= 0:
                        logger.debug("SELL ignoré pour inventaire: qty_held=%.2f", qty_held)
                        self.db.update_order_status(local_id, "rejected", error="qty_held=0")
                        return
                else:
                    actual_size = signal.size
                qty_delta = actual_size if signal.side == "buy" else -actual_size
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
                # Paper trading : mise à jour du solde fictif (sur actual_size)
                if self.config.bot.paper_trading:
                    cost = actual_size * (signal.price or 0.5)
                    balance_delta = -cost if signal.side == "buy" else cost
                    current = self.db.get_latest_balance() or self.config.bot.paper_balance
                    self.db.record_balance(max(0.0, current + balance_delta))

                # ── Analytics : ouvrir/fermer un trade round-trip ──
                try:
                    if signal.side == "buy":
                        self.db.open_trade(
                            open_order_id=local_id,
                            market_id=signal.market_id,
                            token_id=signal.token_id,
                            question=signal.market_question,
                            open_price=signal.price or 0.5,
                            open_size=actual_size,
                            obi_at_open=getattr(signal, "obi_value", None),
                            regime_at_open=getattr(signal, "obi_regime", None),
                            spread_at_open=getattr(signal, "spread_at_signal", None),
                            volume_24h_at_open=getattr(signal, "volume_24h", None),
                            mid_price_at_open=getattr(signal, "mid_price", None),
                        )
                    elif signal.side == "sell":
                        trade_id = self.db.close_trade(
                            token_id=signal.token_id,
                            close_order_id=local_id,
                            close_price=signal.price or 0.5,
                            close_size=actual_size,
                        )
                        if trade_id:
                            logger.info("[Analytics] Trade #%d ferme (SELL @ %.4f)",
                                        trade_id, signal.price or 0.5)
                except Exception as te:
                    logger.debug("[Analytics] Erreur enregistrement trade: %s", te)

        except Exception as e:
            logger.error("Erreur exécution ordre: %s", e)
            self.db.update_order_status(local_id, "error", error=str(e))
            self.db.add_log("ERROR", "trader", f"Erreur ordre: {e}")

    def _simulate_order(self, signal: Signal) -> dict:
        """Simule un fill immédiat pour le paper trading (aucun ordre réel envoyé)."""
        import uuid
        order_id = f"sim_{uuid.uuid4().hex[:12]}"
        fill_price = signal.price if signal.price else 0.5
        logger.info(
            "[PAPER] Ordre simulé: %s %s %.2f shares @ %.4f → %s",
            signal.side.upper(), signal.token_id[:16], signal.size, fill_price, order_id,
        )
        self.db.add_log(
            "INFO", "paper",
            f"[PAPER] {signal.side.upper()} {signal.token_id[:16]} "
            f"@ {fill_price:.4f} × {signal.size:.2f} → {order_id}",
        )
        return {"orderID": order_id, "id": order_id, "status": "filled"}

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
