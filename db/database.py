"""
Module de base de données SQLite.
Gère le stockage des ordres, logs et état du bot.
Thread-safe grâce à check_same_thread=False et un design sans état partagé.
"""

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional


class Database:
    """Interface SQLite pour le bot Polymarket."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._local = threading.local()
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        """Une connexion par thread (Flask et bot tournent séparément)."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")  # Lectures concurrentes
            conn.execute("PRAGMA busy_timeout=5000")
            self._local.conn = conn
        return self._local.conn

    @contextmanager
    def _cursor(self):
        """Context manager pour les opérations DB."""
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _init_schema(self):
        """Crée les tables si elles n'existent pas."""
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS bot_state (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS orders (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT    NOT NULL,
                market_id   TEXT    NOT NULL,
                token_id    TEXT    NOT NULL,
                side        TEXT    NOT NULL,
                order_type  TEXT    NOT NULL,
                price       REAL,
                size        REAL    NOT NULL,
                amount_usdc REAL    NOT NULL,
                status      TEXT    NOT NULL DEFAULT 'pending',
                order_id    TEXT,
                error       TEXT
            );

            CREATE TABLE IF NOT EXISTS balance_history (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                balance   REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS logs (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT    NOT NULL,
                level     TEXT    NOT NULL,
                source    TEXT    NOT NULL,
                message   TEXT    NOT NULL
            );

            -- Valeurs initiales du bot_state
            INSERT OR IGNORE INTO bot_state (key, value) VALUES ('kill_switch', 'off');
            INSERT OR IGNORE INTO bot_state (key, value) VALUES ('daily_loss', '0.0');
            INSERT OR IGNORE INTO bot_state (key, value) VALUES ('daily_loss_reset', '');
            INSERT OR IGNORE INTO bot_state (key, value) VALUES ('high_water_mark', '0.0');

            -- Inventaire par token (mis à jour à chaque fill)
            CREATE TABLE IF NOT EXISTS positions (
                token_id    TEXT    PRIMARY KEY,
                market_id   TEXT    NOT NULL,
                question    TEXT,
                side        TEXT    NOT NULL DEFAULT 'YES',
                quantity    REAL    NOT NULL DEFAULT 0.0,
                avg_price   REAL,
                current_mid REAL,
                mid_updated_at TEXT,
                updated_at  TEXT    NOT NULL
            );

            -- Marchés en cooldown (news-breaker)
            CREATE TABLE IF NOT EXISTS cooldowns (
                token_id    TEXT    PRIMARY KEY,
                until_ts    REAL    NOT NULL,
                reason      TEXT
            );

            -- Round-trips BUY→SELL avec PnL et contexte marché
            CREATE TABLE IF NOT EXISTS trades (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                open_order_id   INTEGER NOT NULL,
                close_order_id  INTEGER,
                market_id       TEXT NOT NULL,
                token_id        TEXT NOT NULL,
                question        TEXT,
                open_price      REAL NOT NULL,
                open_size       REAL NOT NULL,
                open_timestamp  TEXT NOT NULL,
                close_price     REAL,
                close_size      REAL,
                close_timestamp TEXT,
                pnl_usdc        REAL,
                pnl_pct         REAL,
                hold_duration_s REAL,
                obi_at_open     REAL,
                regime_at_open  TEXT,
                spread_at_open  REAL,
                volume_24h_at_open REAL,
                mid_price_at_open  REAL,
                status          TEXT NOT NULL DEFAULT 'open'
            );
            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
            CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_id);
            CREATE INDEX IF NOT EXISTS idx_trades_open_ts ON trades(open_timestamp);

            -- Snapshots par cycle de stratégie (pour analyse des paramètres)
            CREATE TABLE IF NOT EXISTS strategy_snapshots (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp         TEXT NOT NULL,
                obi_threshold     REAL NOT NULL,
                skew_factor       REAL NOT NULL,
                min_spread        REAL NOT NULL,
                order_size_pct    REAL NOT NULL,
                balance_usdc      REAL NOT NULL,
                total_positions   INTEGER NOT NULL,
                net_exposure_usdc REAL NOT NULL,
                markets_scanned   INTEGER NOT NULL,
                signals_generated INTEGER NOT NULL,
                signals_executed  INTEGER NOT NULL,
                signals_rejected  INTEGER NOT NULL
            );

            -- Cache des calculs analytics (évite les requêtes lourdes)
            CREATE TABLE IF NOT EXISTS analytics_cache (
                key         TEXT PRIMARY KEY,
                value_json  TEXT NOT NULL,
                computed_at TEXT NOT NULL
            );
        """)
        conn.commit()
        # Migration : ajouter current_mid et mid_updated_at sur les DB existantes
        # SQLite ne supporte pas IF NOT EXISTS sur ALTER TABLE → on teste l'existence
        for col, typedef in [("current_mid", "REAL"), ("mid_updated_at", "TEXT")]:
            try:
                conn.execute(f"ALTER TABLE positions ADD COLUMN {col} {typedef}")
                conn.commit()
            except Exception:
                pass  # Colonne déjà existante → ignoré

    # ── Kill Switch ──────────────────────────────────────────────

    def get_kill_switch(self) -> bool:
        """Retourne True si le kill switch est activé."""
        with self._cursor() as cur:
            cur.execute("SELECT value FROM bot_state WHERE key = 'kill_switch'")
            row = cur.fetchone()
            return row["value"] == "on" if row else False

    def set_kill_switch(self, active: bool):
        """Active ou désactive le kill switch."""
        with self._cursor() as cur:
            cur.execute(
                "UPDATE bot_state SET value = ? WHERE key = 'kill_switch'",
                ("on" if active else "off",),
            )

    # 2026 V7.2 LIVE AGGRESSIVITY DASHBOARD CONTROL
    def get_aggressivity_level(self) -> str:
        with self._cursor() as cur:
            cur.execute("SELECT value FROM bot_state WHERE key = 'aggressivity_level'")
            row = cur.fetchone()
            return row["value"] if row else "Balanced"

    def set_aggressivity_level(self, level: str):
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO bot_state (key, value) VALUES ('aggressivity_level', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (level,)
            )

    # 2026 V7.3.8 OVERRIDE CONSTANTES DB — config générique via bot_state
    def get_config(self, key: str, default: float | None = None) -> float | None:
        """Lit une valeur de config depuis bot_state, retourne default si absente."""
        with self._cursor() as cur:
            cur.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
            row = cur.fetchone()
            if row and row["value"] is not None:
                try:
                    return float(row["value"])
                except (ValueError, TypeError):
                    return default
            return default

    def set_config(self, key: str, value):
        """Écrit une valeur de config dans bot_state (upsert)."""
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO bot_state (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value))
            )

    def set_config_dict(self, params: dict):
        """Écrit plusieurs clés de config en une seule transaction."""
        with self._cursor() as cur:
            for key, value in params.items():
                cur.execute(
                    "INSERT INTO bot_state (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (key, str(value))
                )

    # ── Ordres ───────────────────────────────────────────────────

    def record_order(
        self,
        market_id: str,
        token_id: str,
        side: str,
        order_type: str,
        price: Optional[float],
        size: float,
        amount_usdc: float,
        status: str = "pending",
        order_id: Optional[str] = None,
        error: Optional[str] = None,
    ) -> int:
        """Enregistre un ordre et retourne son ID local."""
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO orders
                   (timestamp, market_id, token_id, side, order_type,
                    price, size, amount_usdc, status, order_id, error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (now, market_id, token_id, side, order_type,
                 price, size, amount_usdc, status, order_id, error),
            )
            return cur.lastrowid

    def update_order_status(self, local_id: int, status: str, order_id: Optional[str] = None, error: Optional[str] = None):
        """Met à jour le statut d'un ordre par son ID local (DB)."""
        with self._cursor() as cur:
            cur.execute(
                """UPDATE orders
                   SET status = ?, order_id = COALESCE(?, order_id), error = COALESCE(?, error)
                   WHERE id = ?""",
                (status, order_id, error, local_id),
            )

    def update_order_status_by_clob_id(self, clob_id: str, status: str):
        """Met à jour le statut d'un ordre par son ID CLOB (order_id externe).
        Utilisé après cancel+replace pour marquer les ordres annulés comme 'cancelled'
        en DB, évitant que _has_live_sell() les retrouve et bloque de futurs SELL."""
        if not clob_id:
            return
        with self._cursor() as cur:
            cur.execute(
                "UPDATE orders SET status = ? WHERE order_id = ? AND status = 'live'",
                (status, clob_id),
            )

    def get_recent_orders(self, limit: int = 50) -> list[dict]:
        """Retourne les derniers ordres."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM orders ORDER BY id DESC LIMIT ?", (limit,)
            )
            return [dict(row) for row in cur.fetchall()]

    def get_live_sell_qty(self, token_id: str) -> float:
        """Retourne la somme des sizes SELL live pour ce token (shares lockées)."""
        with self._cursor() as cur:
            cur.execute(
                """SELECT COALESCE(SUM(size), 0) AS locked FROM orders
                   WHERE status = 'live' AND side = 'sell' AND token_id = ?""",
                (token_id,),
            )
            row = cur.fetchone()
            return float(row["locked"]) if row else 0.0

    def has_live_sell(self, token_id: str) -> bool:
        """Retourne True si un SELL limit est actuellement live en DB pour ce token.

        Contrairement à get_live_orders(), cette méthode ne filtre PAS par âge
        et ne marque pas d'ordres comme 'cancelled'. Elle est utilisée uniquement
        pour détecter les SELL de liquidation préservés entre cycles.

        Un SELL limit live peut rester dans le carnet CLOB pendant plusieurs
        minutes (ou heures) sans se filler — il ne doit pas être considéré stale.
        """
        with self._cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) AS cnt FROM orders
                   WHERE status = 'live' AND side = 'sell' AND token_id = ?""",
                (token_id,),
            )
            row = cur.fetchone()
            return (row["cnt"] if row else 0) > 0

    def get_live_orders(self, max_age_seconds: int = 60) -> list[dict]:
        """Retourne les ordres live récents (posés depuis < max_age_seconds).
        Les ordres live plus anciens sont considérés comme obsolètes et marqués cancelled."""
        now = datetime.now(timezone.utc)
        with self._cursor() as cur:
            # Récupérer tous les ordres live
            cur.execute("SELECT * FROM orders WHERE status = 'live' ORDER BY id DESC")
            all_live = [dict(row) for row in cur.fetchall()]

        recent = []
        stale_ids = []
        for order in all_live:
            try:
                ts = datetime.fromisoformat(order["timestamp"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age = (now - ts).total_seconds()
                if age <= max_age_seconds:
                    recent.append(order)
                else:
                    stale_ids.append(order["id"])
            except (ValueError, KeyError):
                stale_ids.append(order["id"])

        # Marquer les vieux ordres live comme cancelled (nettoyage)
        if stale_ids:
            with self._cursor() as cur:
                cur.executemany(
                    "UPDATE orders SET status = 'cancelled' WHERE id = ?",
                    [(sid,) for sid in stale_ids],
                )

        return recent

    # ── Solde ────────────────────────────────────────────────────

    def record_balance(self, balance: float):
        """Enregistre un snapshot du solde."""
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO balance_history (timestamp, balance) VALUES (?, ?)",
                (now, balance),
            )

    def get_latest_balance(self) -> Optional[float]:
        """Retourne le dernier solde enregistré."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT balance FROM balance_history ORDER BY id DESC LIMIT 1"
            )
            row = cur.fetchone()
            return row["balance"] if row else None

    def get_balance_history(self, limit: int = 100) -> list[dict]:
        """Retourne l'historique des soldes."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM balance_history ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]

    # ── Pertes journalières ──────────────────────────────────────

    def get_daily_loss(self) -> float:
        """Retourne la perte cumulée du jour."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._cursor() as cur:
            cur.execute("SELECT value FROM bot_state WHERE key = 'daily_loss_reset'")
            row = cur.fetchone()
            if row and row["value"] != today:
                # Nouveau jour : reset
                cur.execute(
                    "UPDATE bot_state SET value = '0.0' WHERE key = 'daily_loss'"
                )
                cur.execute(
                    "UPDATE bot_state SET value = ? WHERE key = 'daily_loss_reset'",
                    (today,),
                )
                return 0.0

            cur.execute("SELECT value FROM bot_state WHERE key = 'daily_loss'")
            row = cur.fetchone()
            return float(row["value"]) if row else 0.0

    def add_daily_loss(self, amount: float):
        """Ajoute un montant à la perte journalière."""
        current = self.get_daily_loss()
        with self._cursor() as cur:
            cur.execute(
                "UPDATE bot_state SET value = ? WHERE key = 'daily_loss'",
                (str(current + amount),),
            )

    # ── Logs ─────────────────────────────────────────────────────

    # ── Positions (inventaire) ───────────────────────────────────

    def update_position(self, token_id: str, market_id: str, question: str,
                        side: str, quantity_delta: float, fill_price: float):
        """Met à jour l'inventaire après un fill (quantité et prix moyen pondéré)."""
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute("SELECT quantity, avg_price FROM positions WHERE token_id = ?", (token_id,))
            row = cur.fetchone()
            if row:
                old_qty = row["quantity"]
                old_avg = row["avg_price"] or fill_price
                new_qty = old_qty + quantity_delta
                # Prix moyen pondéré uniquement si on accroît la position
                if quantity_delta > 0 and old_qty >= 0:
                    new_avg = (old_qty * old_avg + quantity_delta * fill_price) / new_qty if new_qty else fill_price
                else:
                    new_avg = old_avg
                cur.execute(
                    "UPDATE positions SET quantity=?, avg_price=?, updated_at=? WHERE token_id=?",
                    (new_qty, new_avg, now, token_id),
                )
            else:
                cur.execute(
                    "INSERT INTO positions (token_id, market_id, question, side, quantity, avg_price, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (token_id, market_id, question, side, quantity_delta, fill_price, now),
                )

    def get_position(self, token_id: str) -> float:
        """Retourne la quantité détenue pour un token (0.0 si aucune position)."""
        with self._cursor() as cur:
            cur.execute("SELECT quantity FROM positions WHERE token_id = ?", (token_id,))
            row = cur.fetchone()
            return row["quantity"] if row else 0.0

    def get_position_row(self, token_id: str) -> dict | None:
        """Retourne la ligne complète de position pour un token (None si absente).

        Inclut current_mid et mid_updated_at, utiles pour le RiskManager quand
        un token n'est pas dans les marchés éligibles ce cycle (pas de Signal).
        """
        with self._cursor() as cur:
            cur.execute("SELECT * FROM positions WHERE token_id = ?", (token_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_position_usdc(self, token_id: str, current_mid: float = 0.0) -> float:
        """Retourne l'exposition en USDC pour un token.

        Si current_mid > 0 : utilise quantity * current_mid (valeur de marché actuelle).
        Sinon : utilise quantity * avg_price (prix d'achat DB, fallback conservateur).

        Dans les deux cas, avg_price est borné à [0.01, 0.99] pour se protéger
        des corruptions DB (valeurs > 1.0 ou nulles). Si hors bornes, estimation
        à 0.50 (prix médian neutre).

        Retourne 0.0 si aucune position.
        """
        with self._cursor() as cur:
            cur.execute("SELECT quantity, avg_price FROM positions WHERE token_id = ?", (token_id,))
            row = cur.fetchone()
            if not row:
                return 0.0
            qty = row["quantity"]
            if qty == 0:
                return 0.0
            # Priorité : mid courant (valeur marché réelle)
            if current_mid > 0.0:
                return qty * current_mid
            # Fallback : avg_price avec garde-fou
            avg = row["avg_price"] or 0.0
            if avg < 0.01 or avg > 0.99:
                avg = 0.50  # Estimation conservatrice si corrompu
            return qty * avg

    def get_all_positions(self) -> list[dict]:
        """Retourne toutes les positions actives (quantity != 0)."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM positions WHERE quantity != 0.0")
            return [dict(row) for row in cur.fetchall()]

    def set_position_quantity(self, token_id: str, new_quantity: float):
        """Force la quantité d'une position à une valeur précise (sync CLOB→DB).

        Utilisé au démarrage pour corriger les désynchronisations entre la DB locale
        et le solde réel du CLOB (ex: fills externes, marchés résolus partiellement).
        Si new_quantity <= 0, la position est mise à 0 (mais la ligne reste).
        """
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                "UPDATE positions SET quantity = ?, updated_at = ? WHERE token_id = ?",
                (max(0.0, new_quantity), now, token_id),
            )

    def update_position_mid(self, token_id: str, current_mid: float):
        """Enregistre le mid de marché actuel pour un token en inventaire.

        Appelé à chaque cycle par la stratégie pour chaque marché analysé.
        Permet au RiskManager de valoriser l'exposition au prix de marché réel
        même si le token n'a pas généré de signal ce cycle.

        current_mid doit être dans [0.01, 0.99] — un mid hors bornes est ignoré.
        """
        if not (0.01 <= current_mid <= 0.99):
            return
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                """UPDATE positions
                   SET current_mid = ?, mid_updated_at = ?
                   WHERE token_id = ?""",
                (current_mid, now, token_id),
            )

    def sanitize_positions(self) -> dict:
        """Détecte et corrige les avg_price aberrants dans la table positions.

        Un share Polymarket vaut toujours entre 0.01 et 0.99 USDC.
        Un avg_price hors de [0.001, 1.0] indique une corruption de données
        (ex: amount_usdc total stocké à la place du prix unitaire, ou division
        par zéro aboutissant à un prix nul).

        Actions :
          - avg_price > 1.0 : probablement amount_usdc stocké au lieu du prix
            unitaire → on ne peut pas deviner le prix réel sans la quantité.
            On plafonne à 0.50 (prix médian neutre) et on loggue le problème.
          - avg_price <= 0.0 : prix nul/négatif → on remplace par 0.50.
          - quantity <= 0 avec la position existante : nettoyage des positions
            épuisées ou négatives (ne devraient pas exister, mais par sécurité).

        Retourne un dict résumant les corrections effectuées.
        """
        now = datetime.now(timezone.utc).isoformat()
        corrected_price = []
        removed_zero_qty = 0

        with self._cursor() as cur:
            cur.execute("SELECT token_id, quantity, avg_price FROM positions")
            rows = cur.fetchall()

        for row in rows:
            token_id = row["token_id"]
            qty = row["quantity"]
            avg = row["avg_price"] or 0.0

            # Positions à quantity nulle ou négative (orphelines)
            if qty <= 0.0:
                with self._cursor() as cur:
                    cur.execute(
                        "DELETE FROM positions WHERE token_id = ? AND quantity <= 0.0",
                        (token_id,),
                    )
                    if cur.rowcount > 0:
                        removed_zero_qty += 1
                continue

            # avg_price hors bornes valides
            if avg > 1.0 or avg <= 0.0:
                # Meilleure estimation conservative : 0.50 (prix médian)
                # L'utilisateur devra corriger manuellement si nécessaire
                corrected_avg = 0.50
                with self._cursor() as cur:
                    cur.execute(
                        "UPDATE positions SET avg_price = ?, updated_at = ? WHERE token_id = ?",
                        (corrected_avg, now, token_id),
                    )
                corrected_price.append({
                    "token_id": token_id[:20],
                    "qty": qty,
                    "old_avg": avg,
                    "new_avg": corrected_avg,
                })

        return {
            "corrected_avg_price": len(corrected_price),
            "removed_zero_qty": removed_zero_qty,
            "details": corrected_price,
        }

    # 2026 V7.5 POSITION MERGING
    def merge_positions(self) -> dict:
        """Fusionne les positions du même market_id et supprime les positions fantômes (qty ≤ 0.001).
        Retourne un résumé des opérations effectuées."""
        import logging
        _logger = logging.getLogger("database")
        merged_count = 0
        cleaned_count = 0
        details = []

        with self._cursor() as cur:
            # 1. Supprimer les positions fantômes (qty ≈ 0)
            cur.execute("SELECT token_id, quantity FROM positions WHERE abs(quantity) <= 0.001")
            ghosts = cur.fetchall()
            for g in ghosts:
                cur.execute("DELETE FROM positions WHERE token_id = ?", (g["token_id"],))
                cleaned_count += 1
                _logger.info("[PositionMerge] Cleaned ghost position: %s (qty=%.4f)", g["token_id"][:20], g["quantity"])

            # 2. Agréger par market_id — si le même market a YES et NO tokens, net them
            cur.execute("""
                SELECT market_id, COUNT(*) as cnt,
                       SUM(quantity) as total_qty,
                       SUM(quantity * avg_price) as total_cost
                FROM positions
                WHERE quantity > 0.001
                GROUP BY market_id
                HAVING cnt > 1
            """)
            groups = cur.fetchall()
            for g in groups:
                market_id = g["market_id"]
                total_qty = g["total_qty"]
                total_cost = g["total_cost"]
                net_avg = total_cost / total_qty if total_qty > 0 else 0.50

                # Get all tokens for this market
                cur.execute("SELECT token_id, quantity, avg_price, question, side FROM positions WHERE market_id = ? AND quantity > 0.001 ORDER BY quantity DESC", (market_id,))
                tokens = cur.fetchall()
                if len(tokens) <= 1:
                    continue

                # Keep the largest position, delete the rest, add their qty to it
                primary = tokens[0]
                for secondary in tokens[1:]:
                    # Merge into primary
                    cur.execute("DELETE FROM positions WHERE token_id = ?", (secondary["token_id"],))
                    merged_count += 1

                # Update primary with net totals
                now = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
                cur.execute(
                    "UPDATE positions SET quantity = ?, avg_price = ?, updated_at = ? WHERE token_id = ?",
                    (total_qty, net_avg, now, primary["token_id"])
                )
                details.append({
                    "market_id": market_id[:20],
                    "tokens_merged": len(tokens),
                    "net_qty": round(total_qty, 4),
                    "net_avg": round(net_avg, 4),
                })
                _logger.info("[PositionMerge] Market %s: %d positions → 1 (qty=%.2f, avg=%.4f)",
                            market_id[:20], len(tokens), total_qty, net_avg)

        return {"merged": merged_count, "cleaned": cleaned_count, "details": details}

    # 2026 V7.5 COPY-TRADING — table de suivi
    def _ensure_copy_trades_table(self):
        with self._cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS copy_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    source_wallet TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    price REAL NOT NULL,
                    status TEXT DEFAULT 'pending',
                    pnl_usdc REAL DEFAULT 0.0
                )
            """)

    def record_copy_trade(self, wallet: str, token_id: str, side: str, qty: float, price: float):
        self._ensure_copy_trades_table()
        now = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO copy_trades (timestamp, source_wallet, token_id, side, quantity, price, status) "
                "VALUES (?, ?, ?, ?, ?, ?, 'executed')",
                (now, wallet, token_id, side, qty, price)
            )

    def get_copy_trades(self, limit: int = 50) -> list:
        self._ensure_copy_trades_table()
        with self._cursor() as cur:
            cur.execute("SELECT * FROM copy_trades ORDER BY id DESC LIMIT ?", (limit,))
            return [dict(r) for r in cur.fetchall()]

    # ── Cooldowns (news-breaker) ─────────────────────────────────

    def set_cooldown(self, token_id: str, duration_seconds: float, reason: str = ""):
        """Place un token en cooldown jusqu'à now + duration_seconds."""
        import time
        until = time.time() + duration_seconds
        with self._cursor() as cur:
            cur.execute(
                "INSERT OR REPLACE INTO cooldowns (token_id, until_ts, reason) VALUES (?, ?, ?)",
                (token_id, until, reason),
            )

    def is_in_cooldown(self, token_id: str) -> bool:
        """Retourne True si le token est toujours en cooldown."""
        import time
        with self._cursor() as cur:
            cur.execute("SELECT until_ts FROM cooldowns WHERE token_id = ?", (token_id,))
            row = cur.fetchone()
            if not row:
                return False
            if time.time() >= row["until_ts"]:
                cur.execute("DELETE FROM cooldowns WHERE token_id = ?", (token_id,))
                return False
            return True

    def clear_cooldown(self, token_id: str):
        """Supprime manuellement le cooldown d'un token."""
        with self._cursor() as cur:
            cur.execute("DELETE FROM cooldowns WHERE token_id = ?", (token_id,))

    # ── High Water Mark (circuit breaker) ───────────────────────

    def get_high_water_mark(self) -> float:
        """Retourne le High Water Mark journalier du solde."""
        with self._cursor() as cur:
            cur.execute("SELECT value FROM bot_state WHERE key = 'high_water_mark'")
            row = cur.fetchone()
            return float(row["value"]) if row else 0.0

    def update_high_water_mark(self, balance: float):
        """Met à jour le HWM si le solde actuel est supérieur."""
        current_hwm = self.get_high_water_mark()
        if balance > current_hwm:
            with self._cursor() as cur:
                cur.execute(
                    "UPDATE bot_state SET value = ? WHERE key = 'high_water_mark'",
                    (str(balance),),
                )

    def reset_high_water_mark(self):
        """Remet le HWM à 0 (début de journée)."""
        with self._cursor() as cur:
            cur.execute("UPDATE bot_state SET value = '0.0' WHERE key = 'high_water_mark'")

    def add_log(self, level: str, source: str, message: str):
        """Ajoute une entrée de log en base."""
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO logs (timestamp, level, source, message) VALUES (?, ?, ?, ?)",
                (now, level, source, message),
            )

    def purge_old_data(self, days: int = 30) -> dict:
        """Nettoie les données anciennes pour éviter la croissance incontrôlée de la DB.

        Supprime :
          - logs           : entrées de plus de `days` jours
          - balance_history: snapshots de plus de `days` jours (1 par cycle × 10800 cycles/jour)
          - orders         : ordres terminaux (cancelled, error, rejected) de plus de `days` jours
          - strategy_snapshots : snapshots de plus de `days` jours (analytics rétrospectif)
          - trades closed  : trades fermés de plus de `days * 2` jours (rétention plus longue)
          - analytics_cache : invalidé à chaque purge (données stale)
          - cooldowns      : nettoyage des entrées expirées (toujours fait, sans limite de jours)

        Préservé indéfiniment :
          - positions       : inventaire actuel (ne jamais supprimer)
          - trades 'open'   : round-trips en cours (ne jamais supprimer)
          - bot_state       : kill_switch, HWM, daily_loss (configuration)
          - orders 'live'   : ordres actifs dans le carnet (ne pas toucher)

        Retourne un dict {table: rows_deleted} pour audit.

        Thread-safe : utilise le même _cursor() que toutes les autres méthodes.
        Non-bloquant pour la boucle principale : appelé une seule fois au démarrage
        du bot (pas de purge mid-cycle).
        """
        cutoff_dt = datetime.now(timezone.utc)
        # Calculer la date seuil ISO pour comparaison avec les timestamps TEXT
        # SQLite stocke les timestamps comme TEXT ISO8601 → comparaison lexicographique OK
        from datetime import timedelta
        cutoff_main = (cutoff_dt - timedelta(days=days)).isoformat()
        cutoff_trades = (cutoff_dt - timedelta(days=days * 2)).isoformat()

        deleted = {}

        with self._cursor() as cur:
            # 1. Logs système
            cur.execute(
                "DELETE FROM logs WHERE timestamp < ?",
                (cutoff_main,),
            )
            deleted["logs"] = cur.rowcount

            # 2. Historique des soldes
            cur.execute(
                "DELETE FROM balance_history WHERE timestamp < ?",
                (cutoff_main,),
            )
            deleted["balance_history"] = cur.rowcount

            # 3. Ordres terminaux anciens (cancelled, error, rejected)
            #    Les ordres 'live', 'pending', 'submitted', 'matched' sont préservés
            cur.execute(
                """DELETE FROM orders
                   WHERE timestamp < ?
                     AND status IN ('cancelled', 'error', 'rejected')""",
                (cutoff_main,),
            )
            deleted["orders"] = cur.rowcount

            # 4. Snapshots de stratégie (données analytics rétrospectives)
            cur.execute(
                "DELETE FROM strategy_snapshots WHERE timestamp < ?",
                (cutoff_main,),
            )
            deleted["strategy_snapshots"] = cur.rowcount

            # 5. Trades fermés (rétention 2× plus longue pour les analyses PnL)
            cur.execute(
                "DELETE FROM trades WHERE status = 'closed' AND close_timestamp < ?",
                (cutoff_trades,),
            )
            deleted["trades"] = cur.rowcount

            # 6. Cache analytics (invalider complètement — données potentiellement stale)
            cur.execute("DELETE FROM analytics_cache")
            deleted["analytics_cache"] = cur.rowcount

            # 7. Cooldowns expirés (indépendant de `days`, toujours nettoyés)
            import time as _time
            cur.execute(
                "DELETE FROM cooldowns WHERE until_ts < ?",
                (_time.time(),),
            )
            deleted["cooldowns"] = cur.rowcount

        total_deleted = sum(deleted.values())
        return {"rows_deleted": total_deleted, "by_table": deleted, "cutoff_days": days}

    def get_recent_logs(self, limit: int = 100, level: Optional[str] = None) -> list[dict]:
        """Retourne les derniers logs, filtrable par niveau."""
        with self._cursor() as cur:
            if level:
                cur.execute(
                    "SELECT * FROM logs WHERE level = ? ORDER BY id DESC LIMIT ?",
                    (level, limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM logs ORDER BY id DESC LIMIT ?", (limit,)
                )
            return [dict(row) for row in cur.fetchall()]

    # ── Trades (round-trips PnL) ──────────────────────────────────

    def open_trade(
        self,
        open_order_id: int,
        market_id: str,
        token_id: str,
        question: str,
        open_price: float,
        open_size: float,
        obi_at_open: float = None,
        regime_at_open: str = None,
        spread_at_open: float = None,
        volume_24h_at_open: float = None,
        mid_price_at_open: float = None,
    ) -> int:
        """Crée un trade ouvert après un fill BUY. Retourne l'ID du trade."""
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO trades
                   (open_order_id, market_id, token_id, question,
                    open_price, open_size, open_timestamp,
                    obi_at_open, regime_at_open, spread_at_open,
                    volume_24h_at_open, mid_price_at_open, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')""",
                (open_order_id, market_id, token_id, question,
                 open_price, open_size, now,
                 obi_at_open, regime_at_open, spread_at_open,
                 volume_24h_at_open, mid_price_at_open),
            )
            return cur.lastrowid

    def close_trade(
        self,
        token_id: str,
        close_order_id: int,
        close_price: float,
        close_size: float,
    ) -> Optional[int]:
        """Ferme le trade ouvert le plus ancien (FIFO) pour ce token.
        Calcule le PnL et la durée. Retourne l'ID du trade fermé ou None."""
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        with self._cursor() as cur:
            # FIFO : prendre le trade ouvert le plus ancien pour ce token
            cur.execute(
                """SELECT id, open_price, open_size, open_timestamp
                   FROM trades
                   WHERE token_id = ? AND status = 'open'
                   ORDER BY id ASC LIMIT 1""",
                (token_id,),
            )
            row = cur.fetchone()
            if not row:
                return None

            trade_id = row["id"]
            open_price = row["open_price"]
            open_size = row["open_size"]

            # Calcul du PnL
            actual_close_size = min(close_size, open_size)
            pnl_usdc = (close_price - open_price) * actual_close_size
            cost_basis = open_price * open_size
            pnl_pct = (pnl_usdc / cost_basis * 100) if cost_basis > 0 else 0.0

            # Durée du trade
            try:
                open_ts = datetime.fromisoformat(row["open_timestamp"])
                if open_ts.tzinfo is None:
                    open_ts = open_ts.replace(tzinfo=timezone.utc)
                hold_duration = (now - open_ts).total_seconds()
            except (ValueError, TypeError):
                hold_duration = 0.0

            cur.execute(
                """UPDATE trades SET
                       close_order_id = ?, close_price = ?, close_size = ?,
                       close_timestamp = ?, pnl_usdc = ?, pnl_pct = ?,
                       hold_duration_s = ?, status = 'closed'
                   WHERE id = ?""",
                (close_order_id, close_price, actual_close_size,
                 now_iso, pnl_usdc, pnl_pct, hold_duration, trade_id),
            )
            return trade_id

    def get_open_trades(self, token_id: str = None) -> list[dict]:
        """Retourne les trades ouverts (optionnel: filtrer par token)."""
        with self._cursor() as cur:
            if token_id:
                cur.execute(
                    "SELECT * FROM trades WHERE status = 'open' AND token_id = ? ORDER BY id ASC",
                    (token_id,),
                )
            else:
                cur.execute("SELECT * FROM trades WHERE status = 'open' ORDER BY id ASC")
            return [dict(row) for row in cur.fetchall()]

    def get_closed_trades(self, limit: int = 200, market_id: str = None) -> list[dict]:
        """Retourne les trades fermés, optionnel: filtrer par marché."""
        with self._cursor() as cur:
            if market_id:
                cur.execute(
                    "SELECT * FROM trades WHERE status = 'closed' AND market_id = ? ORDER BY id DESC LIMIT ?",
                    (market_id, limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM trades WHERE status = 'closed' ORDER BY id DESC LIMIT ?",
                    (limit,),
                )
            return [dict(row) for row in cur.fetchall()]

    def get_all_trades(self, limit: int = 500) -> list[dict]:
        """Retourne tous les trades (ouverts et fermés)."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))
            return [dict(row) for row in cur.fetchall()]

    # ── Strategy Snapshots ─────────────────────────────────────────

    def record_strategy_snapshot(
        self,
        obi_threshold: float,
        skew_factor: float,
        min_spread: float,
        order_size_pct: float,
        balance_usdc: float,
        total_positions: int,
        net_exposure_usdc: float,
        markets_scanned: int,
        signals_generated: int,
        signals_executed: int,
        signals_rejected: int,
    ):
        """Enregistre un snapshot de l'état du cycle pour analyse post-hoc."""
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO strategy_snapshots
                   (timestamp, obi_threshold, skew_factor, min_spread, order_size_pct,
                    balance_usdc, total_positions, net_exposure_usdc,
                    markets_scanned, signals_generated, signals_executed, signals_rejected)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (now, obi_threshold, skew_factor, min_spread, order_size_pct,
                 balance_usdc, total_positions, net_exposure_usdc,
                 markets_scanned, signals_generated, signals_executed, signals_rejected),
            )

    # ── Analytics Cache ────────────────────────────────────────────

    def get_analytics_cache(self, key: str, max_age_seconds: int = 300) -> Optional[dict]:
        """Retourne le cache si < max_age_seconds, sinon None."""
        with self._cursor() as cur:
            cur.execute("SELECT value_json, computed_at FROM analytics_cache WHERE key = ?", (key,))
            row = cur.fetchone()
            if not row:
                return None
            try:
                computed = datetime.fromisoformat(row["computed_at"])
                if computed.tzinfo is None:
                    computed = computed.replace(tzinfo=timezone.utc)
                age = (datetime.now(timezone.utc) - computed).total_seconds()
                if age > max_age_seconds:
                    return None
                return json.loads(row["value_json"])
            except (ValueError, TypeError, json.JSONDecodeError):
                return None

    def set_analytics_cache(self, key: str, value: dict):
        """Stocke un résultat d'analytics en cache."""
        now = datetime.now(timezone.utc).isoformat()
        with self._cursor() as cur:
            cur.execute(
                "INSERT OR REPLACE INTO analytics_cache (key, value_json, computed_at) VALUES (?, ?, ?)",
                (key, json.dumps(value, default=str), now),
            )

    # ── Agrégations Analytics ──────────────────────────────────────

    def get_pnl_summary(self) -> dict:
        """Retourne les métriques PnL globales des trades fermés."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                 AS total_trades,
                    COALESCE(SUM(pnl_usdc), 0)               AS total_pnl,
                    SUM(CASE WHEN pnl_usdc > 0 THEN 1 ELSE 0 END) AS win_count,
                    SUM(CASE WHEN pnl_usdc <= 0 THEN 1 ELSE 0 END) AS loss_count,
                    COALESCE(AVG(CASE WHEN pnl_usdc > 0 THEN pnl_usdc END), 0) AS avg_win,
                    COALESCE(AVG(CASE WHEN pnl_usdc <= 0 THEN pnl_usdc END), 0) AS avg_loss,
                    COALESCE(MAX(pnl_usdc), 0)               AS best_trade,
                    COALESCE(MIN(pnl_usdc), 0)               AS worst_trade,
                    COALESCE(AVG(hold_duration_s), 0)         AS avg_hold_duration,
                    COALESCE(SUM(CASE WHEN pnl_usdc > 0 THEN pnl_usdc ELSE 0 END), 0) AS gross_wins,
                    COALESCE(SUM(CASE WHEN pnl_usdc < 0 THEN ABS(pnl_usdc) ELSE 0 END), 0) AS gross_losses
                FROM trades WHERE status = 'closed'
            """)
            row = dict(cur.fetchone())
            total = row["total_trades"]
            row["win_rate"] = (row["win_count"] / total * 100) if total > 0 else 0.0
            row["profit_factor"] = (
                row["gross_wins"] / row["gross_losses"]
                if row["gross_losses"] > 0 else float("inf")
            )
            # Nombre de trades ouverts
            cur.execute("SELECT COUNT(*) AS cnt FROM trades WHERE status = 'open'")
            row["open_trades"] = cur.fetchone()["cnt"]
            return row

    def get_pnl_by_market(self, limit: int = 20) -> list[dict]:
        """PnL agrégé par marché, trié par PnL total DESC."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT
                    market_id, question,
                    COUNT(*) AS trades,
                    COALESCE(SUM(pnl_usdc), 0) AS total_pnl,
                    SUM(CASE WHEN pnl_usdc > 0 THEN 1 ELSE 0 END) AS wins,
                    ROUND(AVG(pnl_usdc), 4) AS avg_pnl,
                    ROUND(AVG(hold_duration_s), 0) AS avg_hold
                FROM trades WHERE status = 'closed'
                GROUP BY market_id
                ORDER BY total_pnl DESC
                LIMIT ?
            """, (limit,))
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r["win_rate"] = (r["wins"] / r["trades"] * 100) if r["trades"] > 0 else 0.0
            return rows

    def get_pnl_by_regime(self) -> list[dict]:
        """PnL agrégé par régime OBI (bullish/bearish/neutral)."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(regime_at_open, 'unknown') AS regime,
                    COUNT(*) AS trades,
                    COALESCE(SUM(pnl_usdc), 0) AS total_pnl,
                    SUM(CASE WHEN pnl_usdc > 0 THEN 1 ELSE 0 END) AS wins,
                    ROUND(AVG(pnl_usdc), 4) AS avg_pnl
                FROM trades WHERE status = 'closed'
                GROUP BY regime_at_open
            """)
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r["win_rate"] = (r["wins"] / r["trades"] * 100) if r["trades"] > 0 else 0.0
            return rows

    def get_balance_timeseries(self, limit: int = 500) -> list[dict]:
        """Historique des soldes pour la equity curve (ordre chronologique)."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT timestamp, balance FROM balance_history ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            rows = [dict(r) for r in cur.fetchall()]
            rows.reverse()  # Ordre chronologique
            return rows
