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
import threading
from typing import Optional

from bot.config import AppConfig
from bot.polymarket_client import PolymarketClient
from bot.risk import RiskManager
from bot.strategy import BaseStrategy, OBIMarketMakingStrategy, Signal
from db.database import Database
from bot.telegram import send_alert  # 2026 FINAL TELEGRAM

logger = logging.getLogger("bot.trader")

# Intervalle de polling en secondes — Set B : 8s (ex: 5s)
# Trade-off gas vs latency : a 5s avec 5 marches = ~78$/j de gas.
# A 8s = ~49$/j. Le cancel conditionnel (Tweak 1) reduit encore de ~60%.
OBI_POLL_INTERVAL = 4


class Trader:
    """Moteur principal du bot de trading OBI."""

    # Nombre de cycles entre deux purges automatiques de la DB.
    # À 8s/cycle : 10800 cycles/jour. On purge toutes les 1350 cycles ≈ 3h.
    # Assez fréquent pour éviter une DB volumineuse, assez rare pour ne pas
    # perturber la boucle principale (la purge prend typiquement < 50ms).
    _DB_PURGE_INTERVAL_CYCLES: int = 1350  # ≈ 3 heures à 8s/cycle

    # TTL du cache des mids d'inventaire pour les 200 OK (secondes).
    # get_midpoint() coûte 1 requête HTTP par token. Avec 20 positions = 20 req/cycle
    # à 8s/cycle → trop agressif (rate-limit Polymarket ~120 req/min).
    # On rafraîchit toutes les 60s les mids connus.
    _INVENTORY_MID_TTL_OK_S: float = 60.0
    # CHANGED: TTL distinct pour les 404 (book vide / marché inactif).
    # Inutile de re-fetch un token sans liquidité à chaque cycle.
    # 5 min = largement suffisant, évite ~90% des requêtes inutiles.
    _INVENTORY_MID_TTL_404_S: float = 300.0

    def __init__(self, config: AppConfig, db: Database):
        self.config = config
        self.db = db
        self.pm_client = PolymarketClient(config.polymarket)
        self.risk = RiskManager(config.bot, db)
        self.strategy: Optional[BaseStrategy] = None
        self._running = False
        self._consecutive_errors = 0
        # Compteur de cycles depuis la dernière purge DB
        self._cycles_since_purge: int = 0
        
        # V15 Throttling Caches
        self.cached_balance: float = 0.0
        self.cached_portfolio_value: float = 0.0
        self.cached_kill_switch: bool = False
        self.cached_bot_active: bool = True
        self.cached_strategy_mode: str = "Info Edge Only"
        self.active_targets: list[str] = []
        # Cache timestamps pour _refresh_inventory_mids().
        # {token_id: timestamp_dernière_tentative} — distinct selon le résultat :
        #   - 200 OK  → TTL _INVENTORY_MID_TTL_OK_S  (60s)
        #   - 404/err → TTL _INVENTORY_MID_TTL_404_S (300s)  # CHANGED
        # Reset à chaque redémarrage → toutes positions fetchées au 1er cycle.
        self._mid_last_fetched: dict[str, float] = {}
        # Set des tokens ayant retourné None/404 lors de la dernière tentative.
        # Utilisé pour choisir le bon TTL (404 = marché sans liquidité = moins urgent).
        # FIXED: était {} (dict vide) — corrigé en set() pour cohérence avec .add()/.discard()
        self._mid_404_tokens: set[str] = set()
        # NOTE: le tracking des SELL de liquidation est maintenant basé sur la DB
        # (orders WHERE side='sell' AND status='live' AND token a une position),
        # ce qui survit aux redémarrages. Plus de set en mémoire.

        # Upgrade 3 — Limit maker cancel-replace watcher.
        # {order_id: {token_id, market_id, market_question, ts_posted, local_db_id,
        #             limit_price, shares, usdc_amount, requeue_count}}
        self._sprint_pending_makers: dict = {}
        self._sprint_pending_lock = threading.Lock()

        # Upgrade 5 — TP/SL sur tick WS Binance (sub-seconde).
        # Cache positions sprint rafraîchi toutes les 10s depuis DB.
        # SELL lancé en thread daemon → ne bloque pas le tick callback.
        self._tpsl_sprint_cache:   list  = []
        self._tpsl_cache_ts:       float = 0.0
        self._tpsl_last_check_ts:  float = 0.0   # rate-limiter 200ms
        self._tpsl_selling:        set   = set()  # tokens SELL en cours
        self._tpsl_partial_exited: set   = set()  # V30: tokens ayant déjà eu un partial exit
        self._tpsl_lock = threading.Lock()
        # Compteur d'erreurs 404 consécutives par token_id.
        # Après 3×404, la position est zeroed en DB (zombie detection).
        self._tpsl_zombie_errors:  dict  = {}     # token_id → int

        # Pré-initialisé à None pour éviter AttributeError si un tick WS Binance
        # arrive avant que start() ait chargé la stratégie info_edge.
        self.info_edge_strategy = None

        # V25: cache end_date_ts des tokens sprint pour le exit forcé pré-expiration.
        # {token_id: end_date_ts} — peuplé par _maintenance_loop depuis universe cache.
        self._sprint_expiry_cache: dict = {}

    def start(self):
        """Lance la boucle principale et bloque le thread (jusqu'a CTRL-C)."""
        # V15 Log Silencing
        logging.getLogger("httpx").setLevel(logging.WARNING)
        
        logger.info("=" * 60)
        logger.info("[V18.1] Info Edge Only ... | Edge min 4.5% | Maturity 0-90min | Vol>100")
        logger.info("=" * 60)
        self.db.add_log("INFO", "trader", "Démarrage du bot OBI")

        # 2026 V7.6 — Startup: warn if kill switch was already active, don't auto-clear
        if self.db.get_kill_switch():
            reason = self.db.get_config("kill_switch_reason", "") or "inconnu"
            logger.warning(
                "Kill switch \u00e9tait actif au d\u00e9marrage (raison: %s). "
                "R\u00e9initialisation manuelle requise via dashboard (/api/reset-kill-switch).",
                reason
            )
        else:
            # V7.5.1 — Force default active state at startup only if not already paused
            self.db.set_kill_switch(False)
            self.db.set_config("bot_active", "true")
            logger.info("[Startup] bot_active=true, kill_switch=false \u2014 defaults set")

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

        # Purge des données anciennes au démarrage (nettoyage DB)
        self._run_db_purge()

        self._connect()

        # V19: Telemetry background flusher & Absolute Kill Switch
        def _telemetry_worker():
            while self._running:
                # 1. Flush telemetry
                if getattr(self, "info_edge_strategy", None):
                    try:
                        self.info_edge_strategy.flush_telemetry()
                    except Exception as e:
                        import traceback
                        logger.warning("[Telemetry] Erreur lors du flush buffer: %s - %s", e, traceback.format_exc(limit=1))
                        
                # 2. V19 Absolute Kill Switch: Watchdog BinanceWS
                if getattr(self, "binance_ws", None) and getattr(self.config.bot, "paper_trading", False) is False:
                    try:
                        age = self.binance_ws.age_seconds
                        if age > 5.0 and self.pm_client.is_alive():
                            has_live = len(self.db.get_live_orders()) > 0
                            if has_live:
                                logger.critical("[V19 KILL SWITCH] 🚨 Perte Binance WS (%.1fs). Annulation URGENCE de tous les ordres!", age)
                                self.pm_client.cancel_all_orders()
                                self.db.add_log("CRITICAL", "trader", f"V19 Kill Switch: BinanceWS data age {age:.1f}s")
                                # Mark as canceled in DB to avoid ghost orders
                                for o in self.db.get_live_orders():
                                    if o.get("order_id"):
                                        self.db.update_order_status_by_clob_id(o["order_id"], "cancelled")
                    except Exception as e:
                        logger.error("[V19 KILL SWITCH] Erreur watchdog: %s", e)
                        
                time.sleep(1.0)
                
        self._telemetry_thread = threading.Thread(target=_telemetry_worker, daemon=True, name="TelemetryFlusher")
        self._telemetry_thread.start()

        # Vérifier et mettre à jour les allowances ERC-1155 pour tous les tokens
        # en inventaire au démarrage (évite les erreur 400 sur les SELL existants)
        if not self.config.bot.paper_trading:
            self._ensure_inventory_allowances()
            # Synchroniser les quantités DB avec le solde CLOB réel.
            # Corrige les phantoms (DB qty >> CLOB qty) qui gonflent la valorisation.
            self._sync_positions_from_clob()

        # V18.1 Cleanup de l'etat (garde le flag, supprime le HWM hardcodé 106.13)
        if self.db.get_config("v18_1_cleaned") != "true":
            with self.db._cursor() as cur:
                cur.execute("DELETE FROM bot_state WHERE key IN ('high_water_mark', 'max_drawdown')")
                cur.execute("DELETE FROM balance_history")
            self.db.set_config("max_drawdown", "0.0")
            self.db.set_config("v18_1_cleaned", "true")
            logger.info("[V18.1] DB nettoyée.")

        # HWM = solde live au démarrage → nouvelle baseline (drawdown remis à 0).
        # Reset volontaire 2026-02-28 : l'ancien HWM forcé 106.13 était périmé.
        balance = self._fetch_balance()
        if balance is not None:
            self.db.set_config("high_water_mark", str(round(balance, 4)))
            self.db.set_config("kill_switch", "false")  # reset kill switch si actif
            self.db.record_balance(balance)
            logger.info("[RESET HWM] Baseline = %.2f USDC — drawdown remis à 0, kill switch off", balance)
            self.db.add_log("INFO", "trader", f"RESET HWM: nouvelle baseline {balance:.2f} USDC")

        # Stratégie OBI avec accès à la DB pour cooldowns et inventaire
        self.strategy = OBIMarketMakingStrategy(
            client=self.pm_client,
            db=self.db,
            max_order_size_usdc=self.config.bot.max_order_size,
            max_markets=5,
            paper_trading=self.config.bot.paper_trading,
            max_exposure_pct=self.config.bot.max_exposure_pct,
            stop_loss_pct=self.config.bot.position_stop_loss_pct,
        )
        logger.info("Stratégie chargée: %s", type(self.strategy).__name__)
        self.db.add_log("INFO", "trader", f"Stratégie: {type(self.strategy).__name__}")

        # V10.0 BINANCE WS CLIENT (daemon thread)
        from bot.binance_ws import BinanceWSClient
        # V13 Event-Driven Callback Hook
        self.binance_ws = BinanceWSClient(on_tick_callback=self._on_price_tick)
        self.binance_ws.start()
        logger.info("[V13.0] BinanceWSClient démarré en Event-Driven (bookTicker BTC/ETH + callback direct)")

        # V10.0 INFO EDGE STRATEGY (avec Binance WS)
        from bot.strategy import InfoEdgeStrategy
        self.info_edge_strategy = InfoEdgeStrategy(
            client=self.pm_client,
            db=self.db,
            max_markets=20,
            max_order_size_usdc=self.config.bot.max_order_size,
            binance_ws=self.binance_ws
        )

        # V10.2 — Au startup : si mode Info Edge Only, loguer le switch immédiat
        _startup_mode = self.db.get_config_str("strategy_mode", "MM Balanced")
        if _startup_mode == "Info Edge Only":
            logger.info("[V10.3] Info Edge Only optimisé 100$ | Edge min 12.5%% | Maturity 5-90min | Vol>520")
            self.db.add_log("INFO", "trader", "[V10.3] InfoEdgeStrategy forcé au startup")

        # 2026 V7.0 SCALING
        self.positions = self.db.get_all_positions() if self.db else []
        self.cash = balance
        eligible = self.strategy.get_eligible_markets() if self.strategy else []
        logger.info(f"[STARTUP] V7.0 SCALING — {len(self.positions)} positions | Cash {self.cash:.2f} USDC | {len(eligible)} marchés")
        # 2026 TOP BOT UPGRADE WS — start WebSocket for real-time order books
        if not self.config.bot.paper_trading:
            try:
                ws_tokens = [m.yes_token_id for m in eligible] if eligible else []
                if ws_tokens:
                    self.pm_client.ws_client.start(
                        ws_tokens,
                        on_update_callback=self._on_ws_book_update
                    )
                    # 2026 FINAL
                    logger.info("[WS] Connected + subscribed to %d markets", len(ws_tokens))
            except Exception as e:
                logger.warning("[WS] Impossible de démarrer le WebSocket: %s", e)

        self._last_signal_ts = {} # V13 Rate Limiter par token
        
        self._running = True
        logger.info("[V13.0] Basculement en mode Event-Driven Sniper. En attente de signaux WS...")
        
        # V19: Isolate _maintenance_loop in a dedicated background thread to preserve GIL and main thread signals
        def _bg_maintenance():
            # Initial slight delay strictly for connection settling
            time.sleep(5.0)
            while self._running:
                try:
                    self._maintenance_loop()
                    self._consecutive_errors = 0
                except Exception as e:
                    import traceback
                    logger.error("[Maintenance] Crash de la boucle de fond : %s - %s", e, traceback.format_exc(limit=2))
                    self._handle_error(e)
                    
                # Sleep in small chunks to allow quick exit on shutdown
                for _ in range(60):
                    if not self._running:
                        break
                    time.sleep(1.0)
                    
        self._maintenance_thread = threading.Thread(target=_bg_maintenance, daemon=True, name="MaintenanceLoop")
        self._maintenance_thread.start()

        # Upgrade 3 — Sprint maker cancel-replace watcher (poll toutes les 5s)
        def _bg_sprint_maker_watcher():
            time.sleep(2.0)
            while self._running:
                try:
                    self._check_sprint_pending_makers()
                except Exception as e_sm:
                    logger.debug("[SprintMaker] Watcher erreur: %s", e_sm)
                time.sleep(5.0)

        threading.Thread(
            target=_bg_sprint_maker_watcher,
            daemon=True,
            name="SprintMakerWatcher",
        ).start()

        while self._running:
            try:
                time.sleep(1.0)
            except KeyboardInterrupt:
                logger.info("Arrêt demandé par l'utilisateur (Ctrl+C)")
                break
            except Exception as e:
                self._handle_error(e)

        self._shutdown()

    def _ensure_inventory_allowances(self):
        """
        Vérifie et met à jour les allowances ERC-1155 (CONDITIONAL) pour tous les
        tokens actuellement en inventaire dans la DB.

        Problème résolu : quand le bot tente de SELL un token, Polymarket exige que
        le contrat CTF Exchange ait l'approbation ERC-1155 (setApprovalForAll) pour
        transférer les shares. Si cette approbation manque → erreur 400
        "not enough balance / allowance".

        Cette méthode est appelée :
          - Au démarrage (pour les positions accumulées lors de sessions précédentes)
          - Après chaque fill BUY confirmé (pour préparer le SELL futur)

        L'appel à update_balance_allowance() déclenche la transaction on-chain
        de setApprovalForAll via le wallet signataire du bot.
        """
        try:
            positions = self.db.get_all_positions()
            if not positions:
                logger.debug("[Allowance] Aucune position en inventaire à vérifier.")
                return

            token_ids = [p["token_id"] for p in positions if p.get("token_id")]
            logger.info(
                "[Allowance] Vérification ERC-1155 pour %d token(s) en inventaire...",
                len(token_ids),
            )
            self.db.add_log(
                "INFO", "trader",
                f"Vérification allowances ERC-1155 pour {len(token_ids)} position(s)",
            )

            results = self._call_with_timeout(
                lambda: self.pm_client.ensure_allowances_for_tokens(token_ids),
                timeout=30.0,
                label="ensure_allowances_for_tokens",
            )

            if results:
                failures = [tid for tid, ok in results.items() if not ok]
                if failures:
                    logger.warning(
                        "[Allowance] %d token(s) sans allowance confirmée: %s",
                        len(failures),
                        [t[:16] for t in failures],
                    )
                    self.db.add_log(
                        "WARNING", "trader",
                        f"Allowance ERC-1155 manquante pour {len(failures)} token(s)",
                    )
        except Exception as e:
            logger.warning("[Allowance] Erreur vérification inventaire: %s", e)

    def _sync_positions_from_clob(self):
        """Synchronise la quantité des positions DB avec le solde CLOB réel.

        Problème : la DB locale peut afficher qty=20 alors que le CLOB ne connaît
        que 0.12 shares (fills partiels non enregistrés, résolution de marché,
        trades externes). Cela gonfle la valorisation et génère des SELL trop
        grandes → erreurs 400.

        Algorithme :
          Pour chaque position DB avec quantity > 0 :
            1. Fetch balance CLOB via get_conditional_allowance(token_id)
            2. clob_qty = balance / 1e6
            3. Si clob_qty < db_qty * 0.5 → désync significatif :
               - Mettre à jour DB avec clob_qty
               - Logger WARNING avec l'écart
          Seuil 50% pour ignorer les écarts mineurs (rounding, partiel récent).
        """
        try:
            positions = self.db.get_all_positions()
            if not positions:
                logger.debug("[Sync] Aucune position à synchroniser.")
                return

            logger.info("[Sync] Synchronisation CLOB→DB pour %d position(s)...", len(positions))
            synced = 0
            for pos in positions:
                token_id = pos.get("token_id", "")
                db_qty = float(pos.get("quantity", 0.0))
                if db_qty <= 0 or not token_id:
                    continue
                try:
                    info = self.pm_client.get_conditional_allowance(token_id)
                    raw_balance = info.get("balance", "0") or "0"
                    clob_qty = float(raw_balance) / 1e6
                    locked_qty = self.db.get_live_sell_qty(token_id)
                    total_clob_qty = clob_qty + locked_qty

                    if abs(total_clob_qty - db_qty) > 0.001:
                        # Désync détecté entre CLOB et DB
                        logger.warning(
                            "[Sync] DÉSYNC token %s: DB=%.4f → CLOB=%.4f (dispo=%.4f + bloqué=%.4f). Correction DB.",
                            token_id[:16], db_qty, total_clob_qty, clob_qty, locked_qty
                        )
                        self.db.add_log(
                            "WARNING", "trader",
                            f"Sync CLOB: token {token_id[:16]} DB={db_qty:.4f} → CLOB={total_clob_qty:.4f}",
                        )
                        self.db.set_position_quantity(token_id, total_clob_qty)
                        synced += 1
                    else:
                        logger.debug(
                            "[Sync] token %s OK: DB=%.4f CLOB=%.4f",
                            token_id[:16], db_qty, total_clob_qty,
                        )
                except Exception as exc:
                    logger.debug("[Sync] Erreur fetch CLOB pour %s: %s", token_id[:16], exc)

            if synced:
                logger.info("[Sync] %d position(s) corrigée(s) depuis le CLOB.", synced)
                self.db.add_log("INFO", "trader", f"Sync CLOB: {synced} position(s) corrigée(s)")
            else:
                logger.info("[Sync] Toutes les positions DB sont cohérentes avec le CLOB.")

        except Exception as e:
            logger.warning("[Sync] Erreur synchronisation CLOB→DB: %s", e)

    def _connect(self):
        """Connexion au CLOB avec retry."""
        for attempt in range(1, self.config.bot.max_retries + 1):
            try:
                self.pm_client.connect()
                server_time = self.pm_client.get_server_time()
                
                # V15.2 Time Drift Check
                drift = abs(int(time.time()) - server_time)
                if drift > 1.0:
                    logger.warning("⚠️ TIME DRIFT DETECTED: %ds. Synchronisez votre horloge NTP immédiatement", drift)
                    self.db.add_log("WARNING", "trader", f"TIME DRIFT DETECTED: {drift}s")
                
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

    def _on_price_tick(self, symbol: str, mid: float):
        """V13.0: Déclenché asynchronement depuis BinanceWSClient à chaque tick de prix BTC/ETH."""
        try:
            self._on_price_tick_internal(symbol, mid)
        except Exception as e:
            import traceback
            logger.error("[TRADER] 🚨 SILENT CRASH IN _on_price_tick 🚨 : %s\n%s", e, traceback.format_exc())

    def _on_price_tick_internal(self, symbol: str, mid: float):
        tick_start_ts = time.time()  # V14.0 Latency Trace Start
        if symbol != "BTCUSDT":
            return
            
        # Kill switch check (V20.1 cached values)
        kill_switch = self.cached_kill_switch
        bot_active = self.cached_bot_active
        if kill_switch or not bot_active:
            return

        # Upgrade 5 — TP/SL sprint sub-seconde (indépendant du mode stratégie)
        # Tourne sur chaque tick BTCUSDT → latence ≤ 500ms vs 60s maintenance.
        if not self.config.bot.paper_trading:
            try:
                self._check_sprint_tp_sl_fast()
            except Exception as e_tpsl5:
                logger.debug("[TP/SL FAST] Erreur: %s", e_tpsl5)

        strategy_mode = self.cached_strategy_mode
        if strategy_mode != "Info Edge Only":
            return # Le mode sniper event-driven n'est actif que sur Info Edge Only
            
        if not self.info_edge_strategy:
            return
            
        # V15 Static Targets: Fetch from cache populated by _maintenance_loop
        # We NO LONGER return early if not sprint_ids, because analyze() MUST run 
        # minimally to update the Dashboard's global telemetry (MOM, OBI, SPOT).
        sprint_ids = self.active_targets
        
        # V15 Balance Throttling: Use cached balance instead of API calls
        balance = self.cached_balance
        
        # Le signal génère lui-même les appels base DB minimalistes (V12.12 opti)
        signals = self.info_edge_strategy.analyze(balance=balance, target_market_ids=sprint_ids)
        
        if not signals:
            return
            
        portfolio_value = self.cached_portfolio_value
        
        # Exécution immédiate
        residual_balance = balance
        for sig in signals:
            # RATE LIMITER: 1 tir toutes les 2.0 secondes maximum par jeton pour éviter le spam 429
            now = time.time()
            if now - self._last_signal_ts.get(sig.token_id, 0.0) < 2.0:
                continue
                
            self._last_signal_ts[sig.token_id] = now
                
            approved = self._execute_signal(sig, residual_balance, portfolio_value=portfolio_value, collect_batch=[], tick_start_ts=tick_start_ts)
            if approved:
                if sig.side == "buy" and sig.price:
                    order_cost = sig.size * sig.price
                    residual_balance = max(0.0, residual_balance - order_cost)
                    self.cached_balance = max(0.0, self.cached_balance - order_cost) # V15: Subtract locally
                elif sig.side == "buy" and sig.order_type == "market":
                    residual_balance = max(0.0, residual_balance - sig.size)
                    self.cached_balance = max(0.0, self.cached_balance - sig.size) # V15: Subtract locally


    def _maintenance_loop(self):
        """V13.0: Tâche de fond lente pour la revalorisation et les allowances (60s)."""
        logger.info("[Maintenance] ── Exécution chronjob 60s ────────────────")

        # 1. Connectivité API
        if not self.pm_client.is_alive():
            logger.warning("API Polymarket injoignable, reconnexion...")
            self._connect()

        # 2. Rechargements configs / Redémarrage (via DB flags)
        if self.db.get_config_str("bot_restart_requested", "false") == "true":
            logger.info("[MAINTENANCE] Redémarrage du bot demandé via dashboard. Arrêt propre...")
            self.db.set_config("bot_restart_requested", "false") # Clear flag
            self._running = False
            sys.exit(0) # Exit pour déclencher le redémarrage Docker

        if self.db.get_config_str("bot_reload_aggressivity", "false") == "true":
            logger.info("[MAINTENANCE] Rechargement de la configuration d'agressivité demandé via dashboard.")
            self.db.set_config("bot_reload_aggressivity", "false") # Clear flag
            try:
                self.risk.reload_aggressivity()
                if self.strategy:
                    self.strategy.reload_sizing()
            except Exception as e:
                import traceback
                logger.error("[MAINTENANCE] Erreur lors du rechargement des configs: %s - %s", e, traceback.format_exc(limit=1))

        try:
            self.db.merge_positions()
        except Exception:
            pass

        # 3. Calculs portefeuilles et HWM
        balance_raw = self._fetch_balance()
        if balance_raw is None:
            balance_raw = self.db.get_latest_balance() or 0.0

        self._refresh_inventory_mids()
        portfolio_value = self._compute_portfolio_value(balance_raw)
        self.risk.update_high_water_mark(portfolio_value)
        self._log_inventory_breakdown(balance_raw, portfolio_value)
        
        balance = self._fetch_available_balance(balance_raw)
        
        # V20.1: Cache update for hot-path _on_price_tick to avoid DB blocking
        self.cached_balance = balance
        self.cached_portfolio_value = portfolio_value
        self.cached_kill_switch = self.db.get_kill_switch()
        self.cached_bot_active = self.db.get_config("bot_active", "true") != "false"
        self.cached_strategy_mode = self.db.get_config_str("strategy_mode", "MM Balanced")
        
        self.db.record_balance(balance)
        
        # V15 Static Target Population
        if self.info_edge_strategy:
            eligible = self.info_edge_strategy._universe.get_eligible_markets()
            sprint_ids = []
            for m in eligible:
                minutes_to_expiry = m.days_to_expiry * 1440
                q_lower = m.question.lower()
                is_btc = ("bitcoin" in q_lower or "btc" in q_lower)
                if is_btc and minutes_to_expiry <= 5.5:
                    sprint_ids.append(m.market_id)
                    # V25: cache end_date_ts pour exit forcé pré-expiration
                    if m.end_date_ts > 0:
                        self._sprint_expiry_cache[m.yes_token_id] = m.end_date_ts
                        self._sprint_expiry_cache[m.no_token_id]  = m.end_date_ts
            self.active_targets = sprint_ids

        # 4.a V14.0 Anti-Stale AI Sentiment isolation
        try:
            from bot.ai_edge import get_ai_global_sentiment_bias
            bias = get_ai_global_sentiment_bias()
            if bias != 1.0:
                self.db.set_config("live_ai_sentiment_bias", round(bias, 3))
                logger.debug("[AI Bias] Sentiment rafraîchi via Grok: x%.2f", bias)
        except Exception as e_ai:
            logger.debug("[AI Bias] Erreur: %s", e_ai)

        # 4.b Safe Mode et Liquidations partiels
        if not self.config.bot.paper_trading:
            self._maybe_liquidate_partial(balance, portfolio_value)
            try:
                self.risk.check_auto_close_copied(self.pm_client, self.db.get_all_positions())
            except Exception as e_ac:
                logger.error("[SAFE MODE] Erreur check_auto_close_copied: %s", e_ac)

        # 4.c TP/SL positions sprint BTC/ETH
        if not self.config.bot.paper_trading:
            try:
                self._check_sprint_tp_sl()
            except Exception as e_tpsl:
                logger.error("[TP/SL] Erreur inattendue: %s", e_tpsl)

        # 5. Purge DB
        self._cycles_since_purge += 1
        if self._cycles_since_purge >= self._DB_PURGE_INTERVAL_CYCLES:
            self._run_db_purge()
            self._cycles_since_purge = 0
            
        # 6. V18 Hourly Analytics Alerts (Sharpe & Drawdown)
        import time
        now = time.time()
        if not hasattr(self, "_last_analytics_alert_ts"):
            self._last_analytics_alert_ts = now - 3600  # Trigger on first loop
            
        if now - self._last_analytics_alert_ts >= 3600:
            self._last_analytics_alert_ts = now
            try:
                from bot.analytics import TradeAnalytics
                from bot.telegram import send_alert
                analytics = TradeAnalytics(self.db)
                summary = analytics.compute_performance_summary()
                sharpe = summary.get("sharpe_ratio")
                drawdown = summary.get("max_drawdown_pct")
                
                alerts_triggered = []
                if sharpe is not None and sharpe < 1.5 and summary.get("total_trades", 0) > 10:
                    alerts_triggered.append(f"Sharpe Ratio critique: {sharpe:.2f} < 1.5")
                if drawdown is not None and drawdown > 3.0:
                    alerts_triggered.append(f"Drawdown critique: {drawdown:.2f}% > 3.0%")
                    
                if alerts_triggered:
                    msg = "⚠️ [V18 ALARME_ANALYTICS] ⚠️\n" + "\n".join(alerts_triggered)
                    send_alert(msg)
                    logger.warning(msg.replace('\n', ' '))
            except Exception as e:
                logger.error("[V18 Analytics] Erreur check alertes horaires: %s", e)
            
        logger.info("[Maintenance] ── Fin chronjob ──────────────────────────")

    def _run_db_purge(self, days: int = 30):
        """Lance la purge des données anciennes de la DB de manière non-bloquante.

        Appelé :
          - Au démarrage du bot (nettoyage initial avant connexion CLOB)
          - Périodiquement toutes les _DB_PURGE_INTERVAL_CYCLES cycles (≈ 3h)

        La purge est synchrone mais rapide (SQLite DELETE avec index < 50ms).
        Elle ne bloque pas le cycle de trading car elle s'exécute après la
        completion de toutes les étapes actives du cycle.

        Args:
            days: Rétention des logs/ordres/snapshots en jours (défaut 30).
                  Les trades fermés sont conservés 2× plus longtemps (60 jours).
        """
        try:
            logger.info("[Purge] Nettoyage DB (données > %d jours)...", days)

            # 1. Sanitisation des positions corrompues (avg_price hors bornes)
            san = self.db.sanitize_positions()
            if san["corrected_avg_price"] > 0:
                logger.warning(
                    "[Purge] %d position(s) avec avg_price corrompu (hors [0,1]) "
                    "remis à 0.50 : %s",
                    san["corrected_avg_price"],
                    [(d["token_id"], f"était {d['old_avg']:.4f}") for d in san["details"]],
                )
                self.db.add_log(
                    "WARNING", "trader",
                    f"[Purge] avg_price corrigé sur {san['corrected_avg_price']} position(s) "
                    f"(valeurs hors bornes → 0.50)",
                )
            if san["removed_zero_qty"] > 0:
                logger.info(
                    "[Purge] %d position(s) orpheline(s) supprimée(s) (qty <= 0)",
                    san["removed_zero_qty"],
                )

            # 2. Purge des données anciennes
            result = self.db.purge_old_data(days=days)
            total = result.get("rows_deleted", 0)
            by_table = result.get("by_table", {})
            if total > 0:
                details = ", ".join(
                    f"{tbl}:{cnt}" for tbl, cnt in by_table.items() if cnt > 0
                )
                logger.info(
                    "[Purge] %d ligne(s) supprimée(s) — %s",
                    total, details,
                )
                self.db.add_log(
                    "INFO", "trader",
                    f"[Purge] DB nettoyée: {total} ligne(s) — {details}",
                )
            else:
                logger.debug("[Purge] Aucune donnée à supprimer (DB propre).")
        except Exception as e:
            logger.warning("[Purge] Erreur lors du nettoyage DB: %s", e)

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

            # Mettre à jour le statut en DB pour les ordres annulés.
            # Sans ça, les ordres BUY annulés restent 'live' en DB → _has_live_sell()
            # peut les trouver faussement et bloquer de futurs SELL sur ces tokens.
            for clob_id in to_cancel:
                if clob_id:
                    try:
                        self.db.update_order_status_by_clob_id(clob_id, "cancelled")
                    except Exception:
                        pass  # Méthode optionnelle — pas bloquant

            self.db.add_log(
                "INFO", "trader",
                f"[Cancel+Replace] {len(to_cancel)} annulé(s), {len(preserved)} préservé(s)",
            )
        except Exception as e:
            logger.warning("[Cancel+Replace] Erreur annulation: %s", e)

    # ── Sizing adaptatif & liquidation partielle ─────────────────────────────

    def _get_effective_max_exposure(self, balance: float) -> float:
        """Retourne le max_exposure_pct effectif pour ce cycle.

        Si le cash disponible est très bas (< 10 USDC), on élargit temporairement
        le plafond d'exposition à 35% pour éviter que tous les signaux BUY soient
        rejetés en fractional sizing quand l'inventaire dépasse déjà la limite normale.

        Ce n'est PAS persisté en config — actif uniquement pour le cycle courant.
        Valeur par défaut : risk._max_exposure_pct (lu depuis DB chaque cycle).
        """
        # 2026 V7.3.9 — read from mutable RiskManager attr, not frozen config
        base_pct = self.risk._max_exposure_pct
        if balance < 10.0:
            adaptive_pct = 0.35
            if adaptive_pct > base_pct:
                logger.info(
                    "[SizingAdaptatif] Cash bas (%.2f USDC) → max_expo élargi "
                    "%.0f%% → %.0f%% pour ce cycle.",
                    balance, base_pct * 100, adaptive_pct * 100,
                )
                # Patch temporaire sur le RiskManager pour ce cycle uniquement
                self.risk._max_exposure_pct = adaptive_pct
                return adaptive_pct
        else:
            # Restaurer la valeur DB si elle avait été patchée
            if self.risk._max_exposure_pct != base_pct:
                self.risk._max_exposure_pct = base_pct
        return base_pct

    def _check_sprint_tp_sl_fast(self) -> None:
        """Upgrade 5 — TP/SL sprint sur tick Binance (~500ms).

        Lit le mid depuis le WS Polymarket (zéro appel REST) puis déclenche
        un SELL asynchrone si TP ou SL atteint.

        Rate-limité à 200ms : même pendant une rafale de ticks, on ne fait
        pas plus de 5 vérifications/s (vs une toutes les 60s avant).

        La méthode _check_sprint_tp_sl() (60s) reste active comme filet de
        sécurité pour les tokens sans WS mid disponible.
        """
        TP_RATIO_BASE    = 1.75   # TP de base (cappé dynamiquement si avg élevé)
        SL_RATIO_BASE    = 0.30   # SL de base (durci dynamiquement à t<60s)
        TP_PARTIAL_RATIO = 1.40   # V30: seuil sortie partielle 50%
        TP_PARTIAL_PCT   = 0.50   # V30: fraction vendue au premier palier
        RATE_LIMIT_S     = 0.20   # max 5 checks/s
        CACHE_TTL_S      = 10.0   # refresh positions DB toutes les 10s

        now = time.time()

        # Rate-limiter : ne pas tourner à chaque tick Binance
        if now - self._tpsl_last_check_ts < RATE_LIMIT_S:
            return
        self._tpsl_last_check_ts = now

        # Rafraîchir le cache positions sprint depuis DB (toutes les 10s)
        with self._tpsl_lock:
            if now - self._tpsl_cache_ts >= CACHE_TTL_S:
                try:
                    all_pos = self.db.get_all_positions()
                    self._tpsl_sprint_cache = [
                        p for p in all_pos
                        if any(kw in (p.get("question") or "").lower()
                               for kw in ("btc", "bitcoin"))
                        and float(p.get("quantity") or 0) > 0.01
                        and float(p.get("avg_price") or 0) > 0.01
                    ]
                    self._tpsl_cache_ts = now
                except Exception:
                    return
            cache = list(self._tpsl_sprint_cache)

        if not cache:
            return

        ws_client = getattr(self.pm_client, "ws_client", None)
        if ws_client is None or not ws_client.running:
            return  # WS Polymarket indispo → filet de sécurité 60s prend le relais

        for p in cache:
            token_id = p.get("token_id", "")
            qty      = float(p.get("quantity") or 0)
            avg      = float(p.get("avg_price") or 0)
            if qty <= 0.01 or avg <= 0.01 or not token_id:
                continue

            # Skip si SELL déjà en cours pour ce token
            with self._tpsl_lock:
                if token_id in self._tpsl_selling:
                    continue

            # Éviter double-SELL : vérifier le carnet DB
            if self.db.has_live_sell(token_id):
                continue

            # Mid temps réel depuis WS Polymarket (O(1))
            mid_ws = ws_client.get_midpoint(token_id)

            # V26: fallback REST CLOB cache si WS pas encore warm pour ce token
            if mid_ws is None or not (0.01 <= mid_ws <= 0.99):
                _ies = getattr(self, "info_edge_strategy", None)
                _rc  = getattr(_ies, "_rest_clob_mid_cache", {})
                _re  = _rc.get(token_id)
                if _re and (now - _re[1]) < 15.0:
                    mid_ws = _re[0]
                    logger.debug("[TP/SL FAST] REST mid fallback %.4f pour %s",
                                 mid_ws, token_id[:16])
                else:
                    continue  # aucune source de mid → 60s chrono prend le relais

            ratio = mid_ws / avg

            # V26: TP dynamique — cappé à 0.97/avg pour les positions à fort avg.
            # Bug V25: TP_RATIO=1.75 jamais atteint quand avg>0.57 (seuil >1.0).
            # Exemple: avg=0.69 → tp_ratio = min(1.75, 0.97/0.69) = 1.406
            #          → TP se déclenche à mid=0.97 au lieu de jamais.
            tp_ratio = min(TP_RATIO_BASE, 0.97 / max(avg, 0.01))

            # V26: SL dynamique — plus serré à l'approche de l'expiration.
            # t<60s: SL=0.45× (exit à -55%) | t<30s: SL=0.55× (exit à -45%)
            _exp_ts    = self._sprint_expiry_cache.get(token_id, 0.0)
            _secs_left = (_exp_ts - now) if _exp_ts > 0 else 9999.0
            sl_ratio   = 0.55 if _secs_left < 30.0 else (0.45 if _secs_left < 60.0 else SL_RATIO_BASE)

            # V30: Partial exit — vendre 50% à ratio 1.40, laisser courir 50%
            with self._tpsl_lock:
                _is_partial_done = token_id in self._tpsl_partial_exited

            sell_fraction = 1.0
            if _secs_left < 12.0:
                reason = f"EXPIRY_EXIT(t={_secs_left:.0f}s)"
            elif ratio >= tp_ratio:
                reason = f"TP×{ratio:.2f}(th={tp_ratio:.2f})"
            elif ratio >= TP_PARTIAL_RATIO and not _is_partial_done:
                reason        = f"PARTIAL_TP×{ratio:.2f}(50%→hold)"
                sell_fraction = TP_PARTIAL_PCT
            elif ratio <= sl_ratio:
                reason = f"SL_DYN×{ratio:.2f}(th={sl_ratio:.2f})"
            else:
                continue

            # Marquer "SELL en cours" avant de lancer le thread
            with self._tpsl_lock:
                self._tpsl_selling.add(token_id)

            logger.info(
                "[TP/SL FAST] 🎯 %s déclenché | %s | mid_ws=%.3f avg=%.3f",
                reason, token_id[:16], mid_ws, avg,
            )

            # SELL asynchrone — ne bloque pas le tick callback
            def _do_sell(tid=token_id, q=qty, m=mid_ws, r=reason, a=avg, sf=sell_fraction):
                try:
                    # 1. Allowance ERC-1155 (NegRisk bypass identique au 60s chrono)
                    try:
                        allowance_ok = self._call_with_timeout(
                            lambda: self.pm_client.ensure_conditional_allowance(tid),
                            timeout=8.0,
                            label=f"tpsl_fast_allow({tid[:16]})",
                        )
                        if not allowance_ok:
                            if self.pm_client.is_neg_risk_token(tid):
                                allowance_ok = True  # NegRisk bypass
                    except Exception:
                        allowance_ok = True  # on tente quand même

                    if not allowance_ok:
                        logger.debug("[TP/SL FAST] Allowance non confirmée %s → différé", tid[:16])
                        return

                    # 2. Quantité réelle CLOB (× fraction si partial exit)
                    try:
                        _clob    = self.pm_client.get_conditional_allowance(tid)
                        clob_qty = float(_clob.get("balance", "0") or "0") / 1e6
                    except Exception:
                        clob_qty = q
                    base_qty = clob_qty if clob_qty > 0.01 else q
                    sell_qty = round(min(base_qty, q) * sf, 2)
                    if sell_qty <= 0:
                        return

                    # 3. SELL market
                    usdc_amount = round(sell_qty * m, 4)
                    resp = self._call_with_timeout(
                        lambda: self.pm_client.place_market_order(tid, usdc_amount, "sell"),
                        timeout=12.0,
                        label=f"tpsl_fast_sell({tid[:16]})",
                    )
                    status = resp.get("status", "?") if isinstance(resp, dict) else "?"
                    logger.info(
                        "[TP/SL FAST] ✅ SELL %s | qty=%.2f(×%.0f%%) mid=%.3f avg=%.3f → %s",
                        r, sell_qty, sf * 100, m, a, status,
                    )
                    self.db.add_log(
                        "INFO", "trader",
                        f"[TP/SL FAST] {r} SELL {tid[:16]} {sell_qty:.2f}sh @ {m:.3f}",
                    )
                    from bot.telegram import send_alert
                    send_alert(
                        f"[TP/SL] {r} SELL {sell_qty:.2f}sh @ {m:.3f} | {tid[:8]}"
                    )
                    # V30: Partial exit → mettre à jour la quantité résiduelle en DB
                    #       Full exit   → zéro DB (comportement précédent)
                    if sf < 1.0:
                        remaining_qty = round(q * (1.0 - sf), 2)
                        try:
                            self.db.set_position_quantity(tid, remaining_qty)
                        except Exception as _ez:
                            logger.debug("[TP/SL FAST] Erreur partial update qty: %s", _ez)
                        with self._tpsl_lock:
                            self._tpsl_partial_exited.add(tid)
                            self._tpsl_cache_ts = 0.0   # forcer refresh cache
                    else:
                        try:
                            self.db.set_position_quantity(tid, 0.0)
                        except Exception as _ez:
                            logger.debug("[TP/SL FAST] Erreur zero post-SELL: %s", _ez)
                        with self._tpsl_lock:
                            self._tpsl_partial_exited.discard(tid)
                            self._tpsl_cache_ts = 0.0

                except Exception as e_sell:
                    err_s = str(e_sell)
                    if "no match" in err_s.lower():
                        # Marché expiré ou fermé → zéro DB immédiat, pas d'erreur
                        logger.warning(
                            "[TP/SL FAST] Marché expiré (no match) %s → position zeroed DB",
                            tid[:16],
                        )
                        try:
                            self.db.set_position_quantity(tid, 0.0)
                        except Exception as ez:
                            logger.debug("[TP/SL FAST] Erreur zero no-match: %s", ez)
                        with self._tpsl_lock:
                            self._tpsl_partial_exited.discard(tid)
                            self._tpsl_zombie_errors.pop(tid, None)
                    elif "404" in err_s or "No orderbook" in err_s:
                        with self._tpsl_lock:
                            cnt = self._tpsl_zombie_errors.get(tid, 0) + 1
                            self._tpsl_zombie_errors[tid] = cnt
                        if cnt >= 3:
                            logger.warning(
                                "[TP/SL FAST] 🧟 Zombie %s (%d×404) → zeroed en DB",
                                tid[:16], cnt,
                            )
                            try:
                                self.db.set_position_quantity(tid, 0.0)
                            except Exception as ez:
                                logger.debug("[TP/SL FAST] Erreur zero zombie: %s", ez)
                        else:
                            logger.error(
                                "[TP/SL FAST] Erreur SELL %s: %s (%d/3)",
                                tid[:16], e_sell, cnt,
                            )
                    else:
                        with self._tpsl_lock:
                            self._tpsl_zombie_errors.pop(tid, None)
                        logger.error("[TP/SL FAST] Erreur SELL %s: %s", tid[:16], e_sell)
                finally:
                    with self._tpsl_lock:
                        self._tpsl_selling.discard(tid)

            threading.Thread(
                target=_do_sell,
                daemon=True,
                name=f"TPSL_{token_id[:8]}",
            ).start()

    def _check_sprint_tp_sl(self) -> None:
        """Take Profit / Stop Loss pour les positions sprint BTC/ETH.

        Déclenche un SELL market total sur les positions dont le ratio
        current_mid / avg_price dépasse les seuils configurés :
          - TP_RATIO = 1.75 : mid >= avg × 1.75  (gain ≥ +75%)
          - SL_RATIO = 0.30 : mid <= avg × 0.30  (perte ≥ -70%)

        Ne touche pas aux positions déjà couvertes par un SELL live.
        Utilise le current_mid DB mis à jour par _refresh_inventory_mids().
        """
        TP_RATIO_BASE    = 1.75   # TP base (cappé dynamiquement si avg élevé)
        SL_RATIO_BASE    = 0.30   # SL base (durci dynamiquement à t<60s)
        TP_PARTIAL_RATIO = 1.40   # V30: seuil sortie partielle 50%
        TP_PARTIAL_PCT   = 0.50   # V30: fraction vendue au premier palier

        try:
            positions = self.db.get_all_positions()
        except Exception as e:
            logger.warning("[TP/SL] Erreur lecture positions: %s", e)
            return

        for p in positions:
            question = (p.get("question") or "").lower()
            if not any(kw in question for kw in ("btc", "bitcoin", "eth", "ethereum")):
                continue

            token_id = p.get("token_id", "")
            qty      = float(p.get("quantity") or 0)
            avg      = float(p.get("avg_price") or 0)
            mid      = float(p.get("current_mid") or 0)

            if qty <= 0.01 or avg <= 0.01 or not (0.01 <= mid <= 0.99):
                continue

            if self.db.has_live_sell(token_id):
                continue

            ratio = mid / avg

            # V26: TP dynamique (identique fast path)
            tp_ratio_slow = min(TP_RATIO_BASE, 0.97 / max(avg, 0.01))

            # V26: SL dynamique + V25 expiry exit
            _exp_ts_slow    = self._sprint_expiry_cache.get(token_id, 0.0)
            _secs_left_slow = (_exp_ts_slow - time.time()) if _exp_ts_slow > 0 else 9999.0
            sl_ratio_slow   = 0.55 if _secs_left_slow < 30.0 else (0.45 if _secs_left_slow < 60.0 else SL_RATIO_BASE)

            # V30: Partial exit — vendre 50% à ratio 1.40, laisser courir 50%
            with self._tpsl_lock:
                _is_partial_done_slow = token_id in self._tpsl_partial_exited

            sell_fraction_slow = 1.0
            if _secs_left_slow < 12.0:
                reason = f"EXPIRY_EXIT(t={_secs_left_slow:.0f}s)"
            elif ratio >= tp_ratio_slow:
                reason = f"TP×{ratio:.2f}(th={tp_ratio_slow:.2f})"
            elif ratio >= TP_PARTIAL_RATIO and not _is_partial_done_slow:
                reason             = f"PARTIAL_TP×{ratio:.2f}(50%→hold)"
                sell_fraction_slow = TP_PARTIAL_PCT
            elif ratio <= sl_ratio_slow:
                reason = f"SL_DYN×{ratio:.2f}(th={sl_ratio_slow:.2f})"
            else:
                continue

            # Vérification allowance on-chain avant SELL
            try:
                allowance_ok = self._call_with_timeout(
                    lambda tid=token_id: self.pm_client.ensure_conditional_allowance(tid),
                    timeout=8.0,
                    label=f"tp_sl_allowance({token_id[:16]})",
                )
            except Exception as e:
                logger.warning("[TP/SL] Allowance check échoué %s: %s → ignoré", token_id[:16], e)
                continue
            if not allowance_ok:
                logger.debug("[TP/SL] %s: allowance non confirmée → différé", token_id[:16])
                continue

            # Quantité réelle du CLOB × fraction (partial ou full)
            try:
                _clob = self.pm_client.get_conditional_allowance(token_id)
                clob_qty = float(_clob.get("balance", "0") or "0") / 1e6
            except Exception:
                clob_qty = qty
            base_qty_slow = clob_qty if clob_qty > 0.01 else qty
            sell_qty = round(min(base_qty_slow, qty) * sell_fraction_slow, 2)
            if sell_qty <= 0:
                continue

            usdc_amount = round(sell_qty * mid, 4)
            try:
                resp = self._call_with_timeout(
                    lambda tid=token_id, amt=usdc_amount: self.pm_client.place_market_order(
                        token_id=tid, amount=amt, side="sell"
                    ),
                    timeout=12.0,
                    label=f"tp_sl_sell({token_id[:16]})",
                )
                status = resp.get("status", "?") if isinstance(resp, dict) else "?"
                logger.info(
                    "[TP/SL] 🎯 SELL %s | %s | qty=%.2f(×%.0f%%) mid=%.3f avg=%.3f → %s",
                    reason,
                    (p.get("question") or "")[:35],
                    sell_qty, sell_fraction_slow * 100, mid, avg, status,
                )
                # V30: Partial → mettre à jour quantité résiduelle | Full → zéro DB
                if sell_fraction_slow < 1.0:
                    remaining_qty = round(qty * (1.0 - sell_fraction_slow), 2)
                    try:
                        self.db.set_position_quantity(token_id, remaining_qty)
                    except Exception as _ez:
                        logger.debug("[TP/SL] Erreur partial update qty: %s", _ez)
                    with self._tpsl_lock:
                        self._tpsl_partial_exited.add(token_id)
                else:
                    # V24c: Zéro DB immédiatement après SELL réussi.
                    try:
                        self.db.set_position_quantity(token_id, 0.0)
                    except Exception as _ez:
                        logger.debug("[TP/SL] Erreur zero post-SELL: %s", _ez)
                    with self._tpsl_lock:
                        self._tpsl_partial_exited.discard(token_id)
            except Exception as e:
                err_s = str(e)
                if "no match" in err_s.lower():
                    # Marché expiré ou fermé → zéro DB immédiat, pas d'erreur
                    logger.warning(
                        "[TP/SL] Marché expiré (no match) %s → position zeroed DB",
                        token_id[:16],
                    )
                    try:
                        self.db.set_position_quantity(token_id, 0.0)
                    except Exception as ez:
                        logger.debug("[TP/SL] Erreur zero no-match: %s", ez)
                    with self._tpsl_lock:
                        self._tpsl_partial_exited.discard(token_id)
                        self._tpsl_zombie_errors.pop(token_id, None)
                elif "404" in err_s or "No orderbook" in err_s:
                    cnt = self._tpsl_zombie_errors.get(token_id, 0) + 1
                    self._tpsl_zombie_errors[token_id] = cnt
                    if cnt >= 3:
                        logger.warning(
                            "[TP/SL] 🧟 Zombie %s (%d×404) → zeroed en DB",
                            token_id[:16], cnt,
                        )
                        try:
                            self.db.set_position_quantity(token_id, 0.0)
                        except Exception as ez:
                            logger.debug("[TP/SL] Erreur zero zombie: %s", ez)
                    else:
                        logger.error(
                            "[TP/SL] Erreur SELL %s sur %s: %s (%d/3)",
                            reason, token_id[:16], e, cnt,
                        )
                else:
                    self._tpsl_zombie_errors.pop(token_id, None)
                    # V24b: FOK 400 → bloquer re-entrée strategy 10 min
                    # Évite de pyramider sur une position en SL qu'on ne peut pas vendre.
                    if "400" in err_s and self.strategy and hasattr(self.strategy, "_last_quote_ts"):
                        self.strategy._last_quote_ts[token_id] = time.time() + 600.0
                        logger.warning(
                            "[TP/SL] FOK 400 → re-entrée bloquée 10min %s", token_id[:16]
                        )
                    logger.error("[TP/SL] Erreur SELL %s sur %s: %s", reason, token_id[:16], e)

    # ── Upgrade 3 — Sprint maker cancel-replace watcher ──────────────────────────

    def _check_sprint_pending_makers(self) -> None:
        """Surveille les limit maker orders sprint (5s poll).

        Statechart par ordre :
          live          → attend fill pendant FILL_TIMEOUT_S secondes
          live + timeout → cancel + requote au mid WS  (requeue_count=1)
          live + requeue timeout → cancel + fallback market FOK
          matched/filled → enregistre position en DB, nettoie
          cancelled/expired → nettoie
        """
        # V32: timeout continu fonction du temps restant dans le sprint.
        # Plus le sprint expire bientôt, plus on est agressif.
        # Formule : FILL = clamp(t_left/25, 2.0, 8.0)
        #           REQUOTE = clamp(FILL/2.5, 1.5, 3.0)
        # Exemples :
        #   t=200s → FILL=8.0s  REQUOTE=3.0s
        #   t=120s → FILL=4.8s  REQUOTE=1.9s
        #   t= 60s → FILL=2.4s  REQUOTE=1.5s
        #   t= 25s → FILL=2.0s  REQUOTE=1.5s
        with self._sprint_pending_lock:
            pending = dict(self._sprint_pending_makers)
        if not pending:
            return

        now = time.time()
        for order_id, info in pending.items():
            age = now - info["ts_posted"]

            # Timeout continu selon temps restant du sprint
            _tid_exp   = self._sprint_expiry_cache.get(info.get("token_id", ""), 0.0)
            _t_left    = (_tid_exp - now) if _tid_exp > 0 else 9999.0
            FILL_TIMEOUT_S    = max(2.0, min(8.0, _t_left / 25.0))
            REQUOTE_TIMEOUT_S = max(1.5, min(3.0, FILL_TIMEOUT_S / 2.5))

            _is_up_down = ("$" not in info.get("market_question", ""))
            try:
                order = self.pm_client.get_order(order_id)
                if order is None:
                    with self._sprint_pending_lock:
                        self._sprint_pending_makers.pop(order_id, None)
                    continue

                status = (order.get("status") or "").lower()

                # ── Ordre annulé / expiré par Polymarket ─────────────────────
                if status in ("cancelled", "canceled", "expired"):
                    logger.info("[SprintMaker] Ordre %s %s → nettoyage.", order_id[:12], status)
                    with self._sprint_pending_lock:
                        self._sprint_pending_makers.pop(order_id, None)
                    continue

                # ── Fill confirmé ─────────────────────────────────────────────
                if status in ("matched", "filled"):
                    taking = float(order.get("takingAmount") or info["shares"])
                    making = float(order.get("makingAmount") or info["usdc_amount"])
                    actual_price = (making / taking) if taking > 0 else info["limit_price"]
                    self.db.update_position(
                        token_id=info["token_id"],
                        market_id=info["market_id"],
                        question=info["market_question"],
                        side="YES",
                        quantity_delta=taking,
                        fill_price=actual_price,
                    )
                    if info.get("local_db_id"):
                        self.db.update_order_status(info["local_db_id"], "filled", order_id)
                    self.db.add_log(
                        "INFO", "trader",
                        f"[SprintMaker] FILL {info['token_id'][:16]} "
                        f"{taking:.2f}sh @ {actual_price:.4f} = {making:.2f}USDC",
                    )
                    logger.info(
                        "[SprintMaker] ✅ Fill confirmé %s %.2fsh @ %.4f | age=%.1fs",
                        info["token_id"][:16], taking, actual_price, age,
                    )
                    from bot.telegram import send_alert
                    send_alert(
                        f"[SprintMaker] FILL {taking:.2f}sh @ {actual_price:.4f} "
                        f"= {making:.2f}USDC | {order_id[:8]}"
                    )
                    with self._sprint_pending_lock:
                        self._sprint_pending_makers.pop(order_id, None)
                    continue

                # Fix Bug2+3: UP/DOWN — CLOB user-to-user, limit jamais fillé par AMM.
                # Garder vivant jusqu'à t<20s (critère absolu), puis abandon sans FOK.
                if _is_up_down:
                    if _t_left > 20.0:
                        continue  # Limit toujours vivant, prochain poll dans 5s
                    # t_left < 20s : abandon propre sans FOK
                    logger.warning(
                        "[SprintMaker] UP/DOWN t<20s → abandon limit sans FOK %s (t_left=%.0fs)",
                        info["token_id"][:16], _t_left,
                    )
                    try:
                        self.pm_client.cancel_order(order_id)
                    except Exception:
                        pass
                    with self._sprint_pending_lock:
                        self._sprint_pending_makers.pop(order_id, None)
                    _st_u = getattr(self, "info_edge_strategy", None)
                    if _st_u and hasattr(_st_u, "_sniper_anti_spam"):
                        _st_u._sniper_anti_spam.pop(info.get("market_id", ""), None)
                        logger.info("[SprintMaker] Anti-spam cleared %s (UP/DOWN zéro fill)",
                                    info.get("market_id", "?")[:12])
                    continue

                # ── Timeout premier essai → cancel + requote au mid ───────────
                if age > FILL_TIMEOUT_S and info["requeue_count"] == 0:
                    logger.info(
                        "[SprintMaker] ⏱ Timeout %.1fs → cancel+requote mid | %s",
                        age, info["token_id"][:16],
                    )
                    try:
                        self.pm_client.cancel_order(order_id)
                    except Exception as e_c:
                        logger.debug("[SprintMaker] cancel err: %s", e_c)

                    # V27: Prix requote — 3 sources par priorité décroissante
                    # 1. WS Polymarket book (O(1), temps réel)
                    # 2. REST CLOB cache (5s lag, mais frais)
                    # 3. FOK market direct (fallback ultime)
                    new_price = None

                    # Source 1 : WS book mid
                    if hasattr(self.pm_client, "ws_client") and self.pm_client.ws_client.running:
                        mid_ws = self.pm_client.ws_client.get_midpoint(info["token_id"])
                        if mid_ws and 0.02 <= mid_ws <= 0.98:
                            new_price = round(mid_ws, 3)

                    # Source 2 : REST CLOB cache (si WS indispo)
                    if new_price is None:
                        _ies2 = getattr(self, "info_edge_strategy", None)
                        _rcc2 = getattr(_ies2, "_rest_clob_mid_cache", {})
                        _re2  = _rcc2.get(info["token_id"])
                        if _re2 and (now - _re2[1]) < 15.0:
                            new_price = round(_re2[0], 3)
                            logger.debug("[V27 SprintMaker] Requote via REST cache %.3f %s",
                                         new_price, info["token_id"][:16])

                    if new_price is None:
                        # Fix Bug2+3: UP/DOWN sans mid → abandon sans FOK + libère anti-spam
                        if ("$" not in info.get("market_question", "")):
                            logger.warning(
                                "[SprintMaker] UP/DOWN t<20s → abandon limit sans FOK %s",
                                info["token_id"][:16],
                            )
                            try:
                                self.pm_client.cancel_order(order_id)
                            except Exception:
                                pass
                            with self._sprint_pending_lock:
                                self._sprint_pending_makers.pop(order_id, None)
                            _st3 = getattr(self, "info_edge_strategy", None)
                            if _st3 and hasattr(_st3, "_sniper_anti_spam"):
                                _st3._sniper_anti_spam.pop(info.get("market_id", ""), None)
                                logger.info("[SprintMaker] Anti-spam cleared %s (UP/DOWN zero fill)",
                                            info.get("market_id", "?")[:12])
                            continue
                        logger.info(
                            "[SprintMaker] Aucun mid (WS+REST indispos) → fallback market %s",
                            info["token_id"][:16],
                        )
                        try:
                            _fm_resp = self.pm_client.place_market_order(
                                info["token_id"], info["usdc_amount"], "buy"
                            )
                            if isinstance(_fm_resp, dict) and _fm_resp.get("status") == "matched":
                                _taking = float(_fm_resp.get("takingAmount") or 0)
                                _making = float(_fm_resp.get("makingAmount") or info["usdc_amount"])
                                _fp = (_making / _taking) if _taking > 0 else info["limit_price"]
                                self.db.update_position(
                                    token_id=info["token_id"],
                                    market_id=info["market_id"],
                                    question=info["market_question"],
                                    qty_delta=_taking,
                                    fill_price=_fp,
                                )
                                logger.info(
                                    "[SprintMaker] 📈 Fallback market (no-mid) OK %s "
                                    "%.2f sh @ %.4f",
                                    info["token_id"][:16], _taking, _fp,
                                )
                        except Exception as e_m:
                            logger.warning("[SprintMaker] Fallback market err: %s", e_m)
                        with self._sprint_pending_lock:
                            self._sprint_pending_makers.pop(order_id, None)
                        continue

                    shares = max(5.0, round(info["usdc_amount"] / new_price, 2))
                    try:
                        resp2 = self.pm_client.place_limit_order(
                            info["token_id"], new_price, shares, "buy"
                        )
                        new_oid = resp2.get("orderID") or resp2.get("id") or ""
                        if new_oid:
                            new_info = {
                                **info,
                                "ts_posted":    time.time(),
                                "limit_price":  new_price,
                                "shares":       shares,
                                "requeue_count": 1,
                            }
                            with self._sprint_pending_lock:
                                self._sprint_pending_makers.pop(order_id, None)
                                self._sprint_pending_makers[new_oid] = new_info
                            logger.info(
                                "[SprintMaker] ♻ Requote @ %.3f (%s sh) | order=%s",
                                new_price, shares, new_oid[:12],
                            )
                        else:
                            with self._sprint_pending_lock:
                                self._sprint_pending_makers.pop(order_id, None)
                    except Exception as e_rq:
                        logger.warning("[SprintMaker] Requote err: %s", e_rq)
                        with self._sprint_pending_lock:
                            self._sprint_pending_makers.pop(order_id, None)
                    continue

                # ── Timeout après requote → fallback market ───────────────────
                if age > REQUOTE_TIMEOUT_S and info["requeue_count"] >= 1:
                    # Fix Bug2+3: UP/DOWN → pas de FOK (AMM thin), abandon + libère anti-spam
                    if ("$" not in info.get("market_question", "")):
                        logger.warning(
                            "[SprintMaker] UP/DOWN requote timeout → abandon sans FOK %s",
                            info["token_id"][:16],
                        )
                        try:
                            self.pm_client.cancel_order(order_id)
                        except Exception:
                            pass
                        with self._sprint_pending_lock:
                            self._sprint_pending_makers.pop(order_id, None)
                        _st4 = getattr(self, "info_edge_strategy", None)
                        if _st4 and hasattr(_st4, "_sniper_anti_spam"):
                            _st4._sniper_anti_spam.pop(info.get("market_id", ""), None)
                            logger.info("[SprintMaker] Anti-spam cleared %s (UP/DOWN requote timeout)",
                                        info.get("market_id", "?")[:12])
                        continue
                    logger.info(
                        "[SprintMaker] ⏱ Requote timeout → fallback market %s",
                        info["token_id"][:16],
                    )
                    try:
                        self.pm_client.cancel_order(order_id)
                    except Exception:
                        pass
                    try:
                        _fm2_resp = self.pm_client.place_market_order(
                            info["token_id"], info["usdc_amount"], "buy"
                        )
                        if isinstance(_fm2_resp, dict) and _fm2_resp.get("status") == "matched":
                            _taking2 = float(_fm2_resp.get("takingAmount") or 0)
                            _making2 = float(_fm2_resp.get("makingAmount") or info["usdc_amount"])
                            _fp2 = (_making2 / _taking2) if _taking2 > 0 else info["limit_price"]
                            self.db.update_position(
                                token_id=info["token_id"],
                                market_id=info["market_id"],
                                question=info["market_question"],
                                qty_delta=_taking2,
                                fill_price=_fp2,
                            )
                            logger.info(
                                "[SprintMaker] 📈 Fallback market OK %s %.2f sh @ %.4f",
                                info["token_id"][:16], _taking2, _fp2,
                            )
                        else:
                            logger.warning(
                                "[SprintMaker] Fallback market non matchée %s: %s",
                                info["token_id"][:16], _fm2_resp,
                            )
                    except Exception as e_m2:
                        logger.warning("[SprintMaker] Fallback market (req) err: %s", e_m2)
                    with self._sprint_pending_lock:
                        self._sprint_pending_makers.pop(order_id, None)
                    continue

            except Exception as e:
                logger.debug("[SprintMaker] Watcher err order %s: %s", order_id[:12], e)

    def _maybe_liquidate_partial(self, balance: float, portfolio_value: float) -> None:
        """Déclenche une liquidation partielle si le cash est trop bas vs l'inventaire.

        Condition : cash < 10 USDC ET inv_mid > cash × 5.
        Action : market SELL 20% des positions dont val_mid > 1 USDC,
                 dans la limite de 3 positions par cycle (évite le flood).

        Utilise current_mid DB (mis à jour par _refresh_inventory_mids) pour
        calculer val_mid. Ne touche pas aux positions déjà avec un SELL live.

        Ne fait rien en paper trading — appelé seulement depuis _cycle() en mode réel.
        """
        inv_value = portfolio_value - balance
        if balance >= 10.0 or inv_value <= balance * 5:
            return  # Pas de déclenchement

        try:
            positions = self.db.get_all_positions()
        except Exception as e:
            logger.warning("[Liquidation] Erreur lecture positions: %s", e)
            return

        # Positions éligibles : val_mid > 1 USDC, pas de SELL live déjà en place
        candidates = []
        for p in positions:
            token_id = p.get("token_id", "")
            db_qty   = float(p.get("quantity") or 0)
            mid      = float(p.get("current_mid") or 0)
            if db_qty <= 0 or not (0.01 <= mid <= 0.99):
                continue
            # Utiliser la balance CLOB comme source de vérité pour qty
            try:
                _clob_info = self.pm_client.get_conditional_allowance(token_id)
                clob_qty = float(_clob_info.get("balance", "0") or "0") / 1e6
            except Exception:
                clob_qty = db_qty  # fallback DB si CLOB indisponible
            qty = clob_qty if clob_qty > 0 else db_qty
            # Sync DB si désync bidirectionnelle détectée
            if abs(clob_qty - db_qty) / max(db_qty, 0.001) > 0.5:
                logger.warning(
                    "[Liquidation] Sync on-the-fly %s: DB=%.4f → CLOB=%.4f",
                    token_id[:16], db_qty, clob_qty,
                )
                self.db.set_position_quantity(token_id, clob_qty)
            val_mid = qty * mid
            if val_mid < 1.0:
                continue
            # Pas de SELL déjà dans le carnet pour ce token
            if self.db.has_live_sell(token_id):
                continue
            candidates.append({
                "token_id": token_id,
                "qty":      qty,
                "mid":      mid,
                "val_mid":  val_mid,
                "question": (p.get("question") or token_id[:20])[:40],
            })

        if not candidates:
            logger.info(
                "[Liquidation] Cash bas (%.2f USDC, inv=%.2f) mais aucune position "
                "éligible (val_mid > 1 USDC sans SELL live).",
                balance, inv_value,
            )
            return

        # Tri par val_mid décroissant : liquider les plus grosses d'abord
        candidates.sort(key=lambda x: x["val_mid"], reverse=True)
        max_to_sell = 2  # FIXED: réduit 3→2 /cycle pour éviter le flood d'ordres market
        sold = 0

        logger.warning(
            "[Liquidation] ⚠ DÉCLENCHEMENT: cash=%.2f USDC, inv_mid=%.2f USDC "
            "(ratio ×%.1f ≥ ×5). Liquidation partielle (20%% × %d position(s)).",
            balance, inv_value,
            inv_value / balance if balance > 0 else 0,
            min(len(candidates), max_to_sell),
        )

        for pos in candidates[:max_to_sell]:
            token_id = pos["token_id"]

            # FIXED: qty bornée par deux contraintes :
            #   a) 20% de la position existante
            #   b) max USDC = cash * 0.10 / mid (on ne liquide jamais plus que
            #      10% du cash disponible en valeur, pour rester conservateur)
            qty_20pct = round(pos["qty"] * 0.20, 2)
            max_qty_by_cash = (balance * 0.10 / pos["mid"]) if pos["mid"] > 0 else qty_20pct
            qty_sell = min(qty_20pct, max_qty_by_cash)
            qty_sell = max(qty_sell, 1.0)   # minimum 1 share
            qty_sell = min(qty_sell, pos["qty"])  # jamais plus que détenu
            qty_sell = round(qty_sell, 2)

            # FIXED: vérification allowance on-chain AVANT de poster le SELL market
            # Un SELL market sans allowance → erreur 400, perte d'un cycle.
            try:
                allowance_ok = self._call_with_timeout(
                    lambda tid=token_id: self.pm_client.ensure_conditional_allowance(tid),
                    timeout=8.0,
                    label=f"pre_sell_allowance_check({token_id[:16]})",
                )
            except Exception as allow_err:
                logger.warning(
                    "[Liquidation] Check allowance échoué pour %s: %s → SELL ignoré ce cycle.",
                    token_id[:16], allow_err,
                )
                continue
            if not allowance_ok:
                logger.info(
                    "[Liquidation] %s: allowance non confirmée → SELL différé "
                    "(propagation en cours ou quarantine).",
                    token_id[:16],
                )
                continue

            try:
                usdc_amount = round(qty_sell * pos["mid"], 4)
                resp = self._call_with_timeout(
                    lambda tid=token_id, amt=usdc_amount: self.pm_client.place_market_order(
                        token_id=tid, amount=amt, side="sell"
                    ),
                    timeout=12.0,
                    label=f"liquidation_partial({token_id[:16]})",
                )
                order_id = resp.get("orderID") or resp.get("id") if isinstance(resp, dict) else None
                logger.info(
                    "[Liquidation] SELL market %s qty=%.2f (%.4f USDC) → order_id=%s",
                    pos["question"], qty_sell, usdc_amount, order_id or "?",
                )

                try:
                    self.db.record_order(
                        market_id=token_id,
                        token_id=token_id,
                        side="sell",
                        order_type="market",
                        price=pos["mid"],
                        size=qty_sell,
                        amount_usdc=usdc_amount,
                        status="live",
                        order_id=order_id or None,
                    )
                except Exception as db_err:
                    logger.warning(
                        "[Liquidation] record_order échoué %s: %s (order_id=%s)",
                        token_id[:16], db_err, order_id,
                    )

                # FIXED: poll 3×5s au lieu de sleep(10) fixe.
                # Un FOK market se résout en < 2s sur Polymarket ; on poll
                # get_conditional_allowance pour détecter que le fill a réduit
                # le solde de shares (la balance-allowance inclut les shares restants).
                fill_confirmed = False
                for poll_attempt in range(1, 4):
                    try:
                        time.sleep(5)
                        allowance_info = self._call_with_timeout(
                            lambda tid=token_id: self.pm_client.get_conditional_allowance(tid),
                            timeout=5.0,
                            label=f"poll_post_sell_{poll_attempt}({token_id[:16]})",
                        )
                        if allowance_info is not None:
                            fill_confirmed = True
                            break
                        logger.debug(
                            "[Liquidation] Poll %d/3 pour %s: pas de réponse, retry...",
                            poll_attempt, token_id[:16],
                        )
                    except Exception as poll_err:
                        logger.debug(
                            "[Liquidation] Poll %d/3 échoué %s: %s",
                            poll_attempt, token_id[:16], poll_err,
                        )

                if fill_confirmed:
                    try:
                        self.db.update_position(
                            token_id=token_id,
                            market_id=token_id,
                            question=pos["question"],
                            side="YES",
                            quantity_delta=-qty_sell,
                            fill_price=pos["mid"],
                        )
                        logger.info(
                            "[Liquidation] DB mise à jour: %s qty −%.2f (fill confirmé)",
                            token_id[:16], qty_sell,
                        )
                    except Exception as upd_err:
                        logger.warning(
                            "[Liquidation] update_position échoué %s: %s "
                            "(réconciliation automatique au prochain cycle)",
                            token_id[:16], upd_err,
                        )
                else:
                    logger.warning(
                        "[Liquidation] %s: poll 3×5s sans réponse — "
                        "position DB non mise à jour ce cycle (réconciliation auto).",
                        token_id[:16],
                    )

                sold += 1
            except Exception as e:
                logger.warning(
                    "[Liquidation] Échec SELL market %s: %s", token_id[:16], e
                )

        logger.info("[Liquidation] %d/%d SELL market soumis ce cycle.", sold, len(candidates[:max_to_sell]))

    def _refresh_inventory_mids(self) -> dict[str, float]:
        """Rafraîchit les mids de marché pour toutes les positions sans mid récent.

        Appelé à l'étape 3b de chaque cycle, AVANT _compute_portfolio_value().

        Stratégie de cache à deux vitesses (évite rate-limit Polymarket) :
          • 200 OK  → TTL _INVENTORY_MID_TTL_OK_S  (60s)  : re-fetch dans 60s
          • 404/err → TTL _INVENTORY_MID_TTL_404_S (300s) : book vide, inutile
            de re-interroger souvent. On conserve le dernier current_mid DB connu
            comme fallback (mid stale vaut mieux que avg_price corrompu).

        Retourne dict {token_id: mid_retenu} incluant les mids déjà en cache DB
        (pour le log global inv_mid vs inv_avg).
        """
        now = time.time()
        refreshed: dict[str, float] = {}   # mids effectivement fetchés ce cycle

        # ── Lecture des positions ─────────────────────────────────────────────
        try:
            positions = self.db.get_all_positions()
        except Exception as e:
            logger.warning("[MidRefresh] Erreur lecture positions DB: %s", e)
            return refreshed
        if not positions:
            return refreshed

        # ── Sélection des tokens à fetcher ────────────────────────────────────
        to_fetch: list[str] = []
        skipped_cache: int  = 0

        for p in positions:
            token_id = p.get("token_id")
            if not token_id:
                continue
            qty = float(p.get("quantity") or 0)
            if qty <= 0:
                continue

            last_t    = self._mid_last_fetched.get(token_id, 0.0)
            age_s     = now - last_t
            # CHANGED: TTL selon le dernier résultat (404 → plus long délai)
            ttl       = self._INVENTORY_MID_TTL_404_S if token_id in self._mid_404_tokens \
                        else self._INVENTORY_MID_TTL_OK_S

            if age_s >= ttl:
                to_fetch.append(token_id)
            else:
                skipped_cache += 1

        if not to_fetch:
            logger.debug(
                "[MidRefresh] Cache valide pour %d position(s) — aucun fetch nécessaire.",
                skipped_cache,
            )
            return refreshed

        logger.info(
            "[MidRefresh] Fetch mid pour %d/%d position(s) (cache OK: %d)...",
            len(to_fetch), len(positions), skipped_cache,
        )

        # ── Fetch token par token ─────────────────────────────────────────────
        n_ok    = 0
        n_404   = 0
        n_err   = 0

        for token_id in to_fetch:
            try:
                # CHANGED: get_midpoint_robust() = /midpoint puis fallback (bid+ask)/2
                mid = self._call_with_timeout(
                    lambda tid=token_id: self.pm_client.get_midpoint_robust(tid),
                    timeout=8.0,
                    label=f"get_midpoint_robust({token_id[:16]})",
                )

                if mid is not None and 0.01 <= float(mid) <= 0.99:
                    # ── Mid valide (via /midpoint, bid+ask, ou last-trade) ─────
                    mid_f = float(mid)
                    self.db.update_position_mid(token_id, mid_f)
                    self._mid_last_fetched[token_id] = now
                    self._mid_404_tokens.discard(token_id)
                    # Token redevenu actif → retirer de la liste des tokens inactifs
                    self.pm_client._inactive_tokens.discard(token_id)
                    refreshed[token_id] = mid_f
                    n_ok += 1

                else:
                    # Les 3 mécanismes ont échoué (book totalement vide, inactif).
                    # → TTL long, on conserve le current_mid DB stale comme fallback.
                    # → Marquer comme inactif : strategy.py skipe l'OBI pour ce token.
                    # Ne PAS écraser current_mid en DB ici.
                    self._mid_last_fetched[token_id] = now
                    self._mid_404_tokens.add(token_id)
                    was_active = token_id not in self.pm_client._inactive_tokens
                    self.pm_client._inactive_tokens.add(token_id)
                    if was_active:
                        logger.info(
                            "[MidRefresh] %s: tous mécanismes échoués (midpoint+bid/ask+last-trade) "
                            "→ marqué inactif (OBI skip).",
                            token_id[:16],
                        )
                    else:
                        logger.debug(
                            "[MidRefresh] %s: toujours inactif → TTL 5min.",
                            token_id[:16],
                        )
                    n_404 += 1

            except Exception as e:
                # Erreur réseau / timeout → TTL court (réessai au prochain cycle)
                self._mid_last_fetched[token_id] = now - self._INVENTORY_MID_TTL_OK_S + 16
                logger.debug("[MidRefresh] %s: erreur réseau: %s", token_id[:16], e)
                n_err += 1

        logger.info(
            "[MidRefresh] Terminé: %d OK, %d vide/404 (TTL 5min), %d erreur réseau "
            "sur %d token(s) fetchés.",
            n_ok, n_404, n_err, len(to_fetch),
        )
        return refreshed

    def _log_inventory_breakdown(self, usdc_balance: float, portfolio_value: float):
        """Loggue le détail de chaque position en inventaire (niveau INFO).

        Affiche pour chaque position :
          - question (tronquée à 35 chars)
          - qty, avg (prix entrée DB), val_avg (qty × avg)
          - mid (current_mid DB) + source : "API" / "cache Xmin" / "N/A→avg"
          - val_mid (qty × mid) si disponible, sinon "→avg"
          - flag ⚠ si avg hors bornes

        Log global final : Mids récupérés X/N, inv_mid vs inv_avg.
        Warning si cash bas et inventaire >> cash (suggestion liquidation).
        """
        try:
            positions = self.db.get_all_positions()
            if not positions:
                return

            now = time.time()
            lines          = []
            inv_mid_total  = 0.0   # valorisation mid (quand disponible)
            inv_avg_total  = 0.0   # valorisation avg (toujours calculée)
            n_mid_ok       = 0
            n_mid_missing  = 0

            for p in positions:
                token_id = p.get("token_id", "")
                qty      = float(p.get("quantity") or 0)
                avg      = float(p.get("avg_price") or 0)
                mid      = float(p.get("current_mid") or 0)
                val_avg  = qty * avg if 0.01 <= avg <= 0.99 else qty * 0.50

                inv_avg_total += val_avg

                # ── Source du mid ──────────────────────────────────────────────
                mid_valid = 0.01 <= mid <= 0.99
                if token_id in self._mid_404_tokens:
                    mid_src = "404→0"
                    val_mid = 0.0
                    n_mid_missing += 1
                elif mid_valid:
                    last_t    = self._mid_last_fetched.get(token_id, 0.0)
                    age_min   = (now - last_t) / 60.0
                    # CHANGED: label "API" si fetché ce cycle (<2min), sinon "cache Xmin"
                    if age_min < 2.0:
                        mid_src = "API"
                    else:
                        mid_src = f"cache {age_min:.0f}min"
                    val_mid   = qty * mid
                    inv_mid_total += val_mid
                    n_mid_ok  += 1
                else:
                    mid_src = "N/A→avg"
                    val_mid = None
                    inv_mid_total += val_avg   # fallback avg dans le total
                    n_mid_missing += 1

                # ── Flags ──────────────────────────────────────────────────────
                flags = []
                if avg < 0.01 or avg > 0.99:
                    flags.append("⚠ avg hors bornes")
                if token_id in self._mid_404_tokens:
                    flags.append("book vide")
                flag_str = "  " + ", ".join(flags) if flags else ""

                question = (p.get("question") or token_id[:20])[:35]
                if val_mid is not None:
                    lines.append(
                        f"  {question:<35} qty={qty:>7.2f}"
                        f"  avg={avg:.4f}  val_avg={val_avg:>7.4f}"
                        f"  mid={mid:.4f} ({mid_src})  val_mid={val_mid:>7.4f} USDC{flag_str}"
                    )
                else:
                    lines.append(
                        f"  {question:<35} qty={qty:>7.2f}"
                        f"  avg={avg:.4f}  val_avg={val_avg:>7.4f}"
                        f"  mid=N/A ({mid_src}){flag_str}"
                    )

            inventory_total = portfolio_value - usdc_balance
            logger.info(
                "[Portfolio] Breakdown inventaire (%.4f USDC total, %d position(s)):\n%s",
                inventory_total, len(positions), "\n".join(lines),
            )

            # CHANGED: log global inv_mid vs inv_avg pour diagnostic rapide
            logger.info(
                "[Portfolio] Mids récupérés: %d/%d OK, %d absent(s)/404 "
                "→ inv_mid=%.2f USDC vs inv_avg=%.2f USDC (écart: %+.2f)",
                n_mid_ok, len(positions), n_mid_missing,
                inv_mid_total, inv_avg_total,
                inv_mid_total - inv_avg_total,
            )

            # CHANGED: warning si cash bas et inventaire très supérieur au cash
            if usdc_balance < 10.0 and inv_mid_total > usdc_balance * 4:
                logger.warning(
                    "[Portfolio] ⚠ CASH BAS: cash=%.2f USDC, inv_mid=%.2f USDC "
                    "(ratio ×%.1f). Envisager liquidation partielle (SELL 20%% des "
                    "positions > 1 USDC).",
                    usdc_balance, inv_mid_total,
                    inv_mid_total / usdc_balance if usdc_balance > 0 else 0,
                )

            # 2026 V6 SCALING : Daily Telegram Summary at 00:05 UTC & Alertes Critiques
            hwm = self.db.get_high_water_mark()
            start_balance = self.db.get_latest_balance() or self.config.bot.paper_balance
            
            now_utc = datetime.now(timezone.utc)
            if now_utc.hour == 0 and now_utc.minute >= 5:
                last_daily = getattr(self, "_last_daily_summary_date", None)
                if last_daily != now_utc.date():
                    trades = self.db.get_closed_trades(1000)
                    wins = sum(1 for t in trades if t.get("pnl", 0) > 0)
                    winrate = (wins / len(trades) * 100) if trades else 0.0
                    dd = (hwm - portfolio_value) / hwm if hwm > 0 else 0.0
                    dpnl = portfolio_value - start_balance
                    from bot.telegram import send_alert
                    send_alert(f"📊 DAILY SUMMARY\nPnL Jour: {dpnl:+.2f}$\nWinrate: {winrate:.1f}%\nMax Drawdown: -{dd*100:.1f}%\nPortfolio: ${portfolio_value:.2f}")
                    
                    # V17.0 Alertes Telemetry : Sharpe Ratio
                    from bot.analytics import TradeAnalytics
                    analytics = TradeAnalytics(self.db)
                    metrics = analytics.get_overall_metrics()
                    sharpe = metrics.get('sharpe_ratio', 0.0)
                    if 0 < sharpe < 1.5:
                        send_alert(f"⚠️ SHARPE RATIO ALERT: {sharpe:.2f} < 1.5 (Performance sous-optimale)")
                        
                    self._last_daily_summary_date = now_utc.date()
            
            drawdown = (hwm - portfolio_value) / hwm if hwm > 0 else 0
            
            # Critical Drawdown Alert > 3% (V17.0)
            if drawdown > 0.03 and not getattr(self, "_dd_alerted", False):
                from bot.telegram import send_alert
                send_alert(f"🚨 CRITICAL DRAWDOWN: -{drawdown*100:.1f}%\nPortfolio: ${portfolio_value:.2f} (HWM: ${hwm:.2f})")
                self._dd_alerted = True
            elif drawdown < 0.02:
                self._dd_alerted = False

        except Exception as e:
            logger.debug("[Portfolio] Erreur breakdown: %s", e)

    def _compute_portfolio_value(self, usdc_balance: float) -> float:
        """
        Calcule la valeur totale du portefeuille en USDC.
        V19: Refactor strict basé sur le VRAI mid_price du carnet WS de Polymarket
        (si dispo) pour éviter le ghost balance lié aux décalages de DB avg_price.
        """
        try:
            positions = self.db.get_all_positions()
            inventory_value = 0.0
            skipped = []
            
            for p in positions:
                qty = float(p.get("quantity") or 0)
                if qty <= 0:
                    continue
                    
                token_id = p.get("token_id", "")
                if token_id in self._mid_404_tokens:
                    continue

                mid = 0.0
                
                # 1. WS Temps Réel (Le plus précis pour le HWM)
                if getattr(self.pm_client, "ws_client", None) and getattr(self.pm_client.ws_client, "running", False):
                    # PolymarketWSClient get_order_book
                    ob = self.pm_client.ws_client.get_order_book(token_id)
                    if ob and ob.get("mid"):
                        live_mid = ob["mid"]
                        if 0.01 < live_mid < 0.99:
                            mid = live_mid
                            
                # 2. DB current_mid (Délai max: 60s)
                if mid == 0.0:
                    stored_mid = float(p.get("current_mid") or 0)
                    if 0.01 <= stored_mid <= 0.99:
                        mid = stored_mid
                        
                # 3. Fallback d'avg_price fortement pénalisé
                # Empêche un faux trigger HWM si le marché a dump et on n'a plus de mid
                if mid == 0.0:
                    raw_price = float(p.get("avg_price") or 0)
                    if 0.01 <= raw_price <= 0.99:
                        mid = raw_price * 0.95
                    else:
                        skipped.append((token_id[:16], qty, raw_price))
                        continue
                        
                inventory_value += qty * mid

            total = usdc_balance + inventory_value
            if inventory_value > 0:
                logger.debug(
                    "[Portfolio] USDC=%.4f + inventaire=%.4f → total=%.4f USDC "
                    "(%d valides, %d exclues)",
                    usdc_balance, inventory_value, total,
                    len(positions) - len(skipped), len(skipped),
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
                    # R7: Taille arb augmentée — edge mathématiquement garanti
                    arb_size = min(balance * 0.03, self.config.bot.max_order_size)
                    self._execute_ctf_arb(market, arb_size, ask_yes, ask_no, balance)

        except Exception as e:
            logger.debug("[CTF-ARB] Erreur: %s", e)

    def _execute_ctf_arb(self, market, arb_size: float,
                          ask_yes: float, ask_no: float, balance: float):
        """Exécute l'arb CTF en plaçant deux market orders (FOK).

        R7: Le PnL CTF Arb est isolé du PnL market-making via un tag 'CTF-ARB'
        dans la reason du signal ET un compteur dédié en DB (ctf_arb_pnl, ctf_arb_count).
        """
        from bot.strategy import Signal
        combined = ask_yes + ask_no
        est_profit_per_share = 1.0 - combined
        yes_shares = round(arb_size / ask_yes, 2)
        no_shares  = round(arb_size / ask_no,  2)
        total_cost = yes_shares * ask_yes + no_shares * ask_no
        est_pnl = (min(yes_shares, no_shares) * 1.0) - total_cost

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
                reason=f"CTF-ARB combined={combined:.4f}",
            )
            self._execute_signal(sig, balance)

        # R7: Tracking isolé du PnL CTF Arb
        try:
            prev_pnl = float(self.db.get_config("ctf_arb_pnl", "0.0") or 0)
            prev_count = int(self.db.get_config("ctf_arb_count", "0") or 0)
            self.db.set_config("ctf_arb_pnl", str(round(prev_pnl + est_pnl, 4)))
            self.db.set_config("ctf_arb_count", str(prev_count + 1))
            logger.info(
                "[CTF-ARB] Exécuté: %s | coût=%.4f USDC | profit_est=%.4f USDC "
                "| cumul=%d arbs, PnL=%.4f USDC",
                market.question[:35], total_cost, est_pnl,
                prev_count + 1, prev_pnl + est_pnl,
            )
        except Exception as e:
            logger.debug("[CTF-ARB] Erreur tracking PnL: %s", e)

    def _has_live_sell(self, token_id: str) -> bool:
        """
        Retourne True si un ordre SELL limit est déjà dans le carnet CLOB pour
        ce token (status='live' dans la DB).

        Utilisé pour éviter le double-SELL : quand un SELL de liquidation est
        préservé d'un cycle précédent, les shares sont déjà "engagées" dans
        cet ordre. Tenter un deuxième SELL sur le même token consommerait
        plus de shares que disponibles → erreur 400 "not enough balance".

        Utilise db.has_live_sell() qui ne filtre PAS par âge (contrairement à
        get_live_orders() qui expire les ordres > 60s). Un SELL de liquidation
        peut rester dans le carnet plusieurs heures sans se filler.

        Exemple observé : Cavaliers (5 shares) SELL @ 0.56 préservé (cycle 1).
        Au cycle 3, la stratégie génère un nouveau SELL @ 0.56 → erreur 400 car
        le SELL du cycle 1 est encore ouvert et les 5 shares sont déjà engagées.
        """
        try:
            return self.db.has_live_sell(token_id)
        except Exception:
            return False

    # 2026 TOP BOT UPGRADE WS
    def _on_ws_book_update(self, token_id: str):
        """Callback déclenché par le WebSocket à chaque mise à jour de carnet.

        Persiste le mid WS directement en DB et invalide le TTL HTTP pour ce token,
        ce qui supprime les appels HTTP redondants dans _refresh_inventory_mids()
        pour tous les tokens actifs sur le WebSocket.
        """
        mid = self.pm_client.ws_client.get_midpoint(token_id)
        if mid is not None and 0.01 <= mid <= 0.99:
            try:
                self.db.update_position_mid(token_id, mid)
            except Exception:
                pass  # Position absente en DB : pas critique, sera créée au prochain fill
            self._mid_last_fetched[token_id] = time.time()
            self._mid_404_tokens.discard(token_id)
            logger.debug("[WS] Book update %s — mid=%.4f persisté en DB", token_id[:16], mid)
        else:
            logger.debug("[WS] Book update reçu pour %s (mid indisponible)", token_id[:16])

    def _latency_trace(func):
        """V14: Deécorateur pour tracer la latence d'exécution depuis le tick Binance."""
        import functools
        import time
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            tick_start = kwargs.get("tick_start_ts", time.time())
            result = func(self, *args, **kwargs)
            if result and tick_start > 0:
                diff_ms = (time.time() - tick_start) * 1000.0
                if hasattr(self, 'db'):
                    try:
                        import json
                        latencies = json.loads(self.db.get_config("live_execution_latency", "[]") or "[]")
                        latencies.append(round(diff_ms, 1))
                        self.db.set_config("live_execution_latency", json.dumps(latencies[-5:]))
                    except Exception:
                        pass
                if diff_ms > 200.0:
                    sig = args[0] if args else None
                    tok = sig.token_id[:8] if sig else "?"
                    msg = f"WS->Order Latency > 200ms ({diff_ms:.1f}ms) sur {tok}! Bottleneck détecté."
                    logger.warning("[PERF_WARNING] %s", msg)
                    if hasattr(self, 'db'):
                        self.db.add_log("WARNING", "perf", msg)
            return result
        return wrapper

    @_latency_trace
    def _execute_signal(self, signal: Signal, current_balance: float,
                        portfolio_value: float = 0.0,
                        collect_batch: list = None,
                        tick_start_ts: float = 0.0) -> bool:
        """Vérifie le risque, exécute, met à jour l'inventaire.
        Retourne True si le signal a été approuvé par le RiskManager, False sinon."""

        # ── Garde SELL : éviter le double-SELL sur même token ──────────────
        # Si un SELL limit est déjà dans le carnet pour ce token (préservé du
        # cycle précédent), les shares sont déjà engagées → ne pas en envoyer
        # un deuxième, sinon erreur 400 "not enough balance / allowance".
        if signal.side == "sell" and signal.order_type == "limit" and not self.config.bot.paper_trading:
            if self._has_live_sell(signal.token_id):
                logger.info(
                    "[Execute] SELL skippé: ordre SELL live déjà dans le carnet "
                    "pour %s (shares engagées, attente de fill)",
                    signal.token_id[:16],
                )
                return False

        # ── Garde SELL: balance shares disponibles vs ordres ouverts ──────────
        # FIX: utiliser balance CLOB réelle (raw/1e6) pas qty DB (peut être désynchronisée).
        # Ex: DB dit held=20 mais CLOB balance='120000' = 0.12 shares → 400 garanti.
        if signal.side == "sell" and not self.config.bot.paper_trading:
            try:
                _pre = self._call_with_timeout(
                    lambda: self.pm_client.get_conditional_allowance(signal.token_id),
                    timeout=10.0,
                    label=f"pre-sell_allowance({signal.token_id[:16]})"
                )
                if not _pre:
                    _pre = {}
                clob_balance = float(_pre.get("balance", "0")) / 1e6  # FIXED: scaling 10^6
                qty_locked = self.db.get_live_sell_qty(signal.token_id)
                qty_available = max(0.0, clob_balance - qty_locked)
                logger.info(
                    "[Execute] SELL %s: clob_balance=%.4f locked=%.2f available=%.4f required=%.2f",
                    signal.token_id[:16], clob_balance, qty_locked, qty_available, signal.size,
                )
                _POLY_MIN_SIZE = 5.0  # minimum Polymarket: 5 shares
                if qty_available < _POLY_MIN_SIZE:
                    logger.warning(
                        "[Execute] SELL skippé %s: clob_available=%.4f < min=%.1f shares (400 évité)",
                        signal.token_id[:16], qty_available, _POLY_MIN_SIZE,
                    )
                    return False
                if signal.size < _POLY_MIN_SIZE:
                    # signal.size (DB qty) < 5 mais CLOB en a assez → désync → vendre 5 shares
                    adjusted = min(qty_available, _POLY_MIN_SIZE)
                    logger.warning(
                        "[Execute] SELL %s: signal.size=%.4f < min=%.1f → ajusté à %.1f (clob=%.2f)",
                        signal.token_id[:16], signal.size, _POLY_MIN_SIZE, adjusted, qty_available,
                    )
                    signal.size = adjusted
                if signal.size > qty_available:  # FIXED: strict, pas *0.99
                    logger.warning(
                        "[Execute] SELL skippé %s: qty=%.2f > clob_available=%.4f "
                        "(raw_balance=%s/1e6 → 400 évité)",
                        signal.token_id[:16], signal.size, qty_available, _pre.get("balance"),
                    )
                    return False
            except Exception as _be:
                logger.debug("[Execute] balance check pre-SELL erreur: %s", _be)

        # ── Allowance ERC-1155 synchrone avant SELL ────────────────────────
        # L'approbation au démarrage (update_balance_allowance) retourne une
        # réponse vide : l'API Polymarket propage l'autorisation de façon
        # asynchrone. Si le SELL arrive trop tôt, l'allowance n'est pas encore
        # active → erreur 400. Solution : rappeler ensure_conditional_allowance
        # juste avant chaque SELL de façon synchrone pour forcer la mise à jour.
        # Coût : 1-2 requêtes GET/GET supplémentaires par SELL (négligeable).
        #
        # Tokens neg-risk : ensure_conditional_allowance() retourne False quand
        # le token utilise le NegRisk Exchange (contrat différent, non gérable
        # automatiquement). Dans ce cas, on bloque le SELL pour éviter l'erreur
        # 400 et logguer clairement le problème.
        if signal.side == "sell" and not self.config.bot.paper_trading:
                try:
                    allowance_ok = self._call_with_timeout(
                        lambda: self.pm_client.ensure_conditional_allowance(signal.token_id),
                        timeout=12.0,
                        label=f"ensure_allowance({signal.token_id[:16]})"
                    )

                    # 2026 V7.0 SCALING: NegRisk permanent bypass (auto pour tous)
                    if not allowance_ok:
                        if self.pm_client.is_neg_risk_token(signal.token_id):
                            logger.info(f"[NEG-RISK BYPASS] Allowance forcée auto pour {signal.token_id[:16]}")
                            allowance_ok = True  # on force et on continue

                    if not allowance_ok:
                        logger.warning(
                            "[Execute] SELL bloqué [%s]: allowance ERC-1155 non confirmée "
                            "(token neg-risk — approuver manuellement via l'UI Polymarket)",
                            signal.token_id[:16],
                        )
                        self.db.add_log(
                            "WARNING", "trader",
                            f"SELL bloqué {signal.token_id[:16]}: neg-risk, allowance manuelle requise",
                        )
                        return False

                except Exception as _ae:
                    logger.debug("[Execute] ensure_allowance pre-SELL erreur: %s", _ae)

        verdict = self.risk.check(signal, current_balance, portfolio_value=portfolio_value)

        if not verdict.approved:
            logger.info(
                "Signal rejeté [%s %s @ %.4f]: %s",
                signal.side.upper(), signal.token_id[:16],
                signal.price or 0.0, verdict.reason,
            )
            if verdict.action in ("cancel_bids", "liquidate", "kill_switch"):
                self.db.add_log("WARNING", "risk", f"Rejeté [{verdict.action}]: {verdict.reason}")
            return False

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
            elif signal.order_type in ("limit", "sprint_maker"):
                resp = self._call_with_timeout(
                    lambda: self.pm_client.place_limit_order(
                        token_id=signal.token_id,
                        price=signal.price,
                        size=signal.size,
                        side=signal.side,
                    ),
                    timeout=15.0,
                    label=f"place_limit({signal.token_id[:16]})"
                )
            else:
                # V14.0: Smart Liquidity Check sur WS avant market order
                if hasattr(self.pm_client, "ws_client") and self.pm_client.ws_client.running:
                    ob = self.pm_client.ws_client.get_order_book(signal.token_id)
                    if ob:
                        levels = ob.get("asks" if signal.side == "buy" else "bids", [])
                        if levels:
                            best_p = float(levels[0]["price"])
                            accumulated_size = 0.0
                            accumulated_cost = 0.0
                            target_size = signal.size
                            for lvl in levels[:2]: # Check top 2 levels
                                p = float(lvl["price"])
                                s = float(lvl["size"])
                                take = min(s, target_size - accumulated_size)
                                accumulated_size += take
                                accumulated_cost += take * p
                                if accumulated_size >= target_size:
                                    break
                            
                            if accumulated_size > 0:
                                avg_fill = accumulated_cost / accumulated_size
                                slippage = abs(avg_fill - best_p) / best_p
                                if slippage > 0.015: # > 1.5%
                                    logger.warning("[SLIPPAGE_PROTECT] Cancel %s: Slippage %.2f%% > 1.5%% (Avg: %.4f, Best: %.4f)", signal.token_id[:8], slippage*100, avg_fill, best_p)
                                    self.db.add_log("WARNING", "risk", f"SLIPPAGE_PROTECT {slippage*100:.1f}%")
                                    if hasattr(self, 'db'):
                                        try:
                                            count = int(self.db.get_config("slippage_protect_events", "0") or "0")
                                            self.db.set_config("slippage_protect_events", str(count + 1))
                                        except Exception:
                                            pass
                                    return False
                                    
                resp = self._call_with_timeout(
                    lambda: self.pm_client.place_market_order(
                        token_id=signal.token_id,
                        amount=signal.size,
                        side=signal.side,
                    ),
                    timeout=15.0,
                    label=f"place_market({signal.token_id[:16]})"
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

                # Upgrade 3 — Sprint maker : enregistrer pour le cancel-replace watcher
                if signal.order_type == "sprint_maker" and order_id:
                    usdc_rsv = round(signal.size * (signal.price or 1.0), 4)
                    with self._sprint_pending_lock:
                        self._sprint_pending_makers[order_id] = {
                            "token_id":       signal.token_id,
                            "market_id":      signal.market_id,
                            "market_question": signal.market_question,
                            "ts_posted":      time.time(),
                            "local_db_id":    local_id,
                            "limit_price":    signal.price or 0.99,
                            "shares":         signal.size,
                            "usdc_amount":    usdc_rsv,
                            "requeue_count":  0,
                        }
                    logger.info(
                        "[SprintMaker] ✅ Limit maker enregistré @ %.3f (%s sh ≈ %.2f USDC) | order=%s",
                        signal.price or 0, signal.size, usdc_rsv, order_id[:12],
                    )
                    # 2026 V6
                    from bot.telegram import send_alert
                    send_alert(f"[SprintMaker] BUY {signal.size:.2f}sh @ {signal.price:.3f} | {order_id[:8]}")
                    return True   # balance réservée dans _on_price_tick_internal

                # 2026 V6
                from bot.telegram import send_alert
                send_alert(f"✅ {signal.side.upper()} {signal.size:.2f} @ {signal.price or 'market'} | {order_id[:8]} | PnL est. N/A")
                return

            # Mise à jour de l'inventaire après fill confirmé (matched ou paper filled)

            if status in ("filled", "matched"):
                # ── Calcul de la quantité et du prix réel de fill ─────────────
                # Pour les market orders : la réponse CLOB contient takingAmount
                # (shares reçus) et makingAmount (USDC dépensés).
                # signal.size = USDC pour market orders (convention V23).
                # signal.size = shares pour limit orders.
                if signal.order_type == "market":
                    taking = float(resp.get("takingAmount") or 0)
                    making = float(resp.get("makingAmount") or 0)
                    if taking > 0 and making > 0:
                        actual_size   = taking
                        actual_price  = making / taking
                    else:
                        actual_size   = signal.size
                        actual_price  = signal.price or 0.5
                else:
                    actual_size   = signal.size
                    actual_price  = signal.price or 0.5

                # Plafonner le SELL à la quantité réellement détenue (évite positions négatives)
                if signal.side == "sell":
                    qty_held = self.db.get_position(signal.token_id)
                    actual_size = min(actual_size, max(0.0, qty_held))
                    if actual_size <= 0:
                        logger.debug("SELL ignoré pour inventaire: qty_held=%.2f", qty_held)
                        self.db.update_order_status(local_id, "rejected", error="qty_held=0")
                        return

                qty_delta = actual_size if signal.side == "buy" else -actual_size
                self.db.update_position(
                    token_id=signal.token_id,
                    market_id=signal.market_id,
                    question=signal.market_question,
                    side="YES",
                    quantity_delta=qty_delta,
                    fill_price=actual_price,
                )
                logger.info(
                    "Position mise à jour: %s %+.2f shares @ %.4f",
                    signal.token_id[:16], qty_delta, actual_price,
                )
                pnl_str = "N/A"
                if signal.side == "sell":
                    try:
                        pts = self.db.get_all_positions()
                        pos = next((x for x in pts if x["token_id"] == signal.token_id), None)
                        if pos and pos.get("avg_price"):
                            pnl_val = ((signal.price or 0.5) - pos["avg_price"]) * actual_size
                            pnl_str = f"{pnl_val:+.2f}$"
                    except Exception:
                        pass
                
                # 2026 V6
                from bot.telegram import send_alert
                send_alert(f"✅ {signal.side.upper()} {actual_size:.2f} @ {signal.price or 'market'} | {order_id[:8]} | PnL est. {pnl_str}")
                
                # Paper trading : mise à jour du solde fictif (sur actual_size)
                if self.config.bot.paper_trading:
                    cost = actual_size * (signal.price or 0.5)
                    balance_delta = -cost if signal.side == "buy" else cost
                    current = self.db.get_latest_balance() or self.config.bot.paper_balance
                    self.db.record_balance(max(0.0, current + balance_delta))

                # ── Allowance ERC-1155 après fill BUY ──────────────────────────
                # Après un fill BUY, s'assurer que l'allowance ERC-1155 existe pour
                # pouvoir SELL ce token plus tard (sans erreur 400).
                # Appel asynchrone (daemon thread) pour ne pas bloquer le cycle.
                if signal.side == "buy" and not self.config.bot.paper_trading:
                    import threading
                    def _check_allowance():
                        try:
                            self.pm_client.ensure_conditional_allowance(signal.token_id)
                        except Exception:
                            pass
                    t = threading.Thread(target=_check_allowance, daemon=True)
                    t.start()

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
            return True  # Approuvé par risk, mais erreur d'exécution

        return True  # Signal approuvé et traité

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
        # 2026 TOP BOT UPGRADE WS — stop WebSocket
        try:
            self.pm_client.ws_client.stop()
            logger.info("[WS] WebSocket arrêté.")
        except Exception:
            pass
        try:
            self.pm_client.cancel_all_orders()
            logger.info("Tous les ordres ouverts annulés.")
            self.db.add_log("INFO", "trader", "Ordres annulés à l'arrêt")
        except Exception as e:
            logger.warning("Erreur annulation ordres: %s", e)
        logger.info("Bot arrêté proprement.")
        logger.info("=" * 60)

    # 2026 V7.0 SCALING: Auto-rebalance + 2026 V7.3 DASHBOARD ULTIME
    def _check_auto_rebalance(self, portfolio_value: float):
        """
        Vérifie toutes les 4h l'inventaire pour l'auto-rebalance.
        Vérifie à chaque cycle les demandes manuelles du Dashboard (V7.3).
        """
        import time
        now = time.time()
        
        # 2026 V7.3 DASHBOARD ULTIME: Check manual rebalance requests from UI
        positions = self.db.get_all_positions() if self.db else []
        if self.db:
            with self.db._cursor() as cur:
                cur.execute("SELECT key FROM bot_state WHERE key LIKE 'force_rebalance_%' AND value = 'true'")
                rows = cur.fetchall()
                for r in rows:
                    key = r["key"]
                    token_id = key.replace("force_rebalance_", "")
                    
                    for p in positions:
                        if p["token_id"] == token_id:
                            qty = float(p.get("quantity", 0))
                            if qty > 0.05:
                                sell_qty = round(qty * 0.5, 2)  # Sell half on manual rebalance
                                logger.info(f"[REBALANCE MANUAL] Dashboard trigger executé pour {token_id[:16]} ({sell_qty} shares)")
                                self.db.add_log("INFO", "trader", f"Rebalance partiel manuel sur {token_id[:16]}")
                                if not self.config.bot.paper_trading:
                                    try:
                                        self.pm_client.place_market_order(token_id, sell_qty, side="sell")
                                    except Exception as e:
                                        logger.error(f"[REBALANCE MANUAL] Erreur: {e}")
                    
                    # Consume the request intent
                    cur.execute("UPDATE bot_state SET value = 'false' WHERE key = ?", (key,))

        if not hasattr(self, "_last_rebalance_ts"):
            self._last_rebalance_ts = now
            return  # Première fois, on initialise juste le chronomètre
        
        # 4 heures
        if now - self._last_rebalance_ts < 4 * 3600:
            return
            
        self._last_rebalance_ts = now
        logger.info("[REBALANCE] Début du scan d'auto-rebalance...")
        
        positions = self.db.get_all_positions() if self.db else []
        for p in positions:
            qty = p.get("quantity", 0.0)
            avg_price = p.get("avg_price", 0.0)
            if qty < 0.01: 
                continue
            
            value = qty * avg_price
            skew = value / portfolio_value if portfolio_value > 0 else 0.0
            
            if skew > 0.8:
                target_qty = (0.5 * portfolio_value) / avg_price if avg_price > 0 else 0
                sell_qty = qty - target_qty
                
                logger.info("[REBALANCE] Skew extrême (%.2f > 0.80) détecté sur %s. Objectif SELL: %.2f shares", skew, p["token_id"][:16], sell_qty)
                self.db.add_log("WARNING", "trader", f"Auto-rebalance sur {p['token_id'][:16]} (skew={skew:.2f})")
                
                if not self.config.bot.paper_trading:
                    try:
                        self.pm_client.place_market_order(p["token_id"], sell_qty, side="sell")
                        logger.info("[REBALANCE] Ordre Market SELL exécuté pour %s !", p["token_id"][:16])
                    except Exception as e:
                        logger.error("[REBALANCE] Erreur lors du SELL: %s", e)
