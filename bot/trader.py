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

        # Purge des données anciennes au démarrage (nettoyage DB)
        self._run_db_purge()

        self._connect()

        # Vérifier et mettre à jour les allowances ERC-1155 pour tous les tokens
        # en inventaire au démarrage (évite les erreur 400 sur les SELL existants)
        if not self.config.bot.paper_trading:
            self._ensure_inventory_allowances()
            # Synchroniser les quantités DB avec le solde CLOB réel.
            # Corrige les phantoms (DB qty >> CLOB qty) qui gonflent la valorisation.
            self._sync_positions_from_clob()

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
            stop_loss_pct=self.config.bot.position_stop_loss_pct,
        )
        logger.info("Stratégie chargée: %s", type(self.strategy).__name__)
        self.db.add_log("INFO", "trader", f"Stratégie: {type(self.strategy).__name__}")

        # 2026 TOP BOT UPGRADE WS — start WebSocket for real-time order books
        if not self.config.bot.paper_trading:
            try:
                eligible = self.strategy.get_eligible_markets()
                ws_tokens = [m.yes_token_id for m in eligible] if eligible else []
                if ws_tokens:
                    self.pm_client.ws_client.start(
                        ws_tokens,
                        on_update_callback=self._on_ws_book_update
                    )
                    logger.info("[WS] WebSocket démarré pour %d token(s).", len(ws_tokens))
            except Exception as e:
                logger.warning("[WS] Impossible de démarrer le WebSocket: %s", e)

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

        # 3b. Valeur totale du portfolio (USDC + inventaire valorisé au marché)
        # → utilisée pour le High Water Mark et le circuit breaker.
        # Étape 3b-bis : rafraîchit les mids de marché pour les positions hors
        # univers OBI ce cycle (tokens non analysés par strategy.py).
        # Limité par cache TTL=60s pour éviter le rate-limit Polymarket.
        logger.info("[Cycle] Étape 3b: rafraîchissement mids inventaire")
        self._refresh_inventory_mids()  # NEW: met à jour current_mid en DB avant calcul

        # Calcul portfolio avec current_mid prioritaire (fallback avg_price si absent)
        logger.info("[Cycle] Étape 3b: calcul valeur portfolio")
        portfolio_value = self._compute_portfolio_value(balance_raw)
        self.risk.update_high_water_mark(portfolio_value)
        # Breakdown inventaire détaillé : qty, avg_price, current_mid, val_mid par position
        self._log_inventory_breakdown(balance_raw, portfolio_value)
        logger.info(
            "[Cycle] Étape 3b: portfolio=%.4f USDC (USDC=%.4f + inventaire=%.4f) | HWM=%.4f",
            portfolio_value, balance_raw, portfolio_value - balance_raw,
            self.db.get_high_water_mark(),
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

        # 5b. Sizing adaptatif : si cash très bas, élargir temporairement max_exposure_pct
        # pour permettre au moins 1 BUY ou 1 SELL sans blocage immédiat.
        # N'est PAS persisté en config — actif uniquement pour ce cycle.
        effective_max_expo = self._get_effective_max_exposure(balance)

        # 5c. Liquidation partielle auto si cash très bas et inventaire >> cash.
        # Déclenché ici (après cancel+replace) pour éviter les doubles SELL.
        if not self.config.bot.paper_trading:
            self._maybe_liquidate_partial(balance, portfolio_value)

        # 6. Stratégie OBI → signaux (balance passée pour sizing dynamique)
        #    Récupérer les marchés éligibles pour les partager avec CTF arb
        logger.info("[Cycle] Étape 6: analyse OBI → signaux (max_expo=%.0f%%)", effective_max_expo * 100)
        signals = self.strategy.analyze(balance=balance)
        logger.info("[Cycle] Étape 6: %d signal(s) généré(s)", len(signals))

        # 7. CTF Inverse Spread Arb (réutilise les marchés déjà chargés par la stratégie)
        logger.info("[Cycle] Étape 7: CTF arb check")
        eligible_markets = self.strategy.get_eligible_markets() if self.strategy else []
        self._check_ctf_arb(balance, eligible_markets)

        # 8. Exécution avec gestion de l'inventaire post-fill
        # residual_balance suit le solde consommé au fil des ordres du cycle :
        # chaque BUY approuvé déduit son coût pour que le check de réserve
        # du signal suivant reflète le solde réel restant (et non le solde initial).
        logger.info("[Cycle] Étape 8: exécution %d signal(s)", len(signals))
        cycle_executed = 0
        cycle_rejected = 0
        residual_balance = balance
        # 2026 TOP BOT UPGRADE BATCH — regroup limit orders for batching
        batch_orders = []
        for sig in signals:
            approved = self._execute_signal(sig, residual_balance, portfolio_value=portfolio_value, collect_batch=batch_orders)
            if approved:
                cycle_executed += 1
                # Déduire le coût estimé pour les BUY (SELL ne consomme pas de cash)
                if sig.side == "buy" and sig.price:
                    residual_balance = max(0.0, residual_balance - sig.size * sig.price)
                elif sig.side == "buy" and sig.order_type == "market":
                    residual_balance = max(0.0, residual_balance - sig.size)
            else:
                cycle_rejected += 1

        # 2026 TOP BOT UPGRADE BATCH — flush batch if >3 orders collected
        if len(batch_orders) > 3 and not self.config.bot.paper_trading:
            try:
                resps = self._call_with_timeout(
                    lambda: self.pm_client.place_orders_batch(batch_orders),
                    timeout=15.0,
                    label="place_orders_batch",
                )
                logger.info("[Batch] %d ordres envoyés en batch.", len(batch_orders))
            except Exception as be:
                logger.warning("[Batch] Erreur batch: %s — fallback individuel.", be)

        # 9. Snapshot de stratégie pour analytics
        logger.info("[Cycle] Étape 9: snapshot analytics")
        try:
            from bot.strategy import (OBI_BULLISH_THRESHOLD, OBI_SKEW_FACTOR,
                                       MIN_SPREAD, ORDER_SIZE_PCT)
            positions = self.db.get_all_positions()
            net_exposure = sum(p.get("quantity", 0) * (p.get("avg_price", 0) or 0)
                               for p in positions)
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
                signals_executed=cycle_executed,
                signals_rejected=cycle_rejected,
            )
        except Exception as se:
            logger.debug("[Analytics] Erreur snapshot: %s", se)

        # 10. Purge DB périodique (tous les _DB_PURGE_INTERVAL_CYCLES cycles)
        self._cycles_since_purge += 1
        if self._cycles_since_purge >= self._DB_PURGE_INTERVAL_CYCLES:
            self._run_db_purge()
            self._cycles_since_purge = 0

        logger.info("[Cycle] ── Cycle terminé. Prochain dans %ds. ──────────", OBI_POLL_INTERVAL)

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
        Valeur par défaut : config.max_exposure_pct (20%).
        """
        base_pct = getattr(self.config.bot, "max_exposure_pct", 0.20)
        if balance < 10.0:
            adaptive_pct = 0.35
            if adaptive_pct > base_pct:
                logger.info(
                    "[SizingAdaptatif] Cash bas (%.2f USDC) → max_expo élargi "
                    "%.0f%% → %.0f%% pour ce cycle.",
                    balance, base_pct * 100, adaptive_pct * 100,
                )
                # Patch temporaire sur le RiskManager pour ce cycle uniquement
                try:
                    object.__setattr__(self.risk.config, "max_exposure_pct", adaptive_pct)
                except (AttributeError, TypeError):
                    pass  # config frozen dataclass → on ignore si indisponible
                return adaptive_pct
        else:
            # Restaurer la valeur par défaut si elle avait été patchée
            if getattr(self.risk.config, "max_exposure_pct", base_pct) != base_pct:
                try:
                    object.__setattr__(self.risk.config, "max_exposure_pct", base_pct)
                except (AttributeError, TypeError):
                    pass
        return base_pct

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

        except Exception as e:
            logger.debug("[Portfolio] Erreur breakdown: %s", e)

    def _compute_portfolio_value(self, usdc_balance: float) -> float:
        """
        Calcule la valeur totale du portefeuille en USDC.

        Valorisation des shares par priorité (plus fiable → moins fiable) :
          1. current_mid DB [0.01, 0.99] — prix de marché récent
             (mis à jour par _refresh_inventory_mids() + strategy.py)
          2. avg_price DB [0.01, 0.99] — prix d'entrée historique (fallback)
             Utilisé quand current_mid est absent (ex: token 404 au 1er cycle)
             ou pour les marchés sans liquidité persistante.
          3. Position ignorée si avg_price aussi hors bornes (protège le HWM).

        Utilisé pour le High Water Mark et le circuit breaker.
        En cas d'erreur DB, retourne usdc_balance (mode dégradé safe).
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
                    # Ignore 404 (resolved/empty) from portfolio HWM
                    continue
                # ── 1. current_mid DB ──────────────────────────────────────────
                stored_mid = float(p.get("current_mid") or 0)
                if 0.01 <= stored_mid <= 0.99:
                    inventory_value += qty * stored_mid
                    continue
                # ── 2. avg_price DB [0.01, 0.99] ──────────────────────────────
                raw_price = float(p.get("avg_price") or 0)
                if 0.01 <= raw_price <= 0.99:
                    inventory_value += qty * raw_price
                    continue
                # ── 3. avg_price hors bornes → exclure du HWM ─────────────────
                skipped.append((p.get("token_id", "?")[:16], qty, raw_price))

            if skipped:
                logger.warning(
                    "[Portfolio] %d position(s) exclue(s) du HWM — mid et avg_price "
                    "hors bornes [0.01,0.99]: %s. Lancer sanitize_positions().",
                    len(skipped),
                    [(tid, f"qty={q:.2f}", f"avg={av:.4f}") for tid, q, av in skipped],
                )

            total = usdc_balance + inventory_value
            if inventory_value > 0:
                logger.debug(
                    "[Portfolio] USDC=%.4f + inventaire=%.4f → total=%.4f USDC "
                    "(%d position(s), %d exclue(s))",
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
        """Callback déclenché par le WebSocket à chaque mise à jour de carnet."""
        logger.debug("[WS] Book update reçu pour %s", token_id[:16])

    def _execute_signal(self, signal: Signal, current_balance: float,
                        portfolio_value: float = 0.0,
                        collect_batch: list = None) -> bool:
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
            elif signal.order_type == "limit":
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
