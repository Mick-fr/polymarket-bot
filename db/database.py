"""
Module de base de données SQLite.
Gère le stockage des ordres, logs et état du bot.
Thread-safe grâce à check_same_thread=False et un design sans état partagé.
"""

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
                updated_at  TEXT    NOT NULL
            );

            -- Marchés en cooldown (news-breaker)
            CREATE TABLE IF NOT EXISTS cooldowns (
                token_id    TEXT    PRIMARY KEY,
                until_ts    REAL    NOT NULL,
                reason      TEXT
            );
        """)
        conn.commit()

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
        """Met à jour le statut d'un ordre."""
        with self._cursor() as cur:
            cur.execute(
                """UPDATE orders
                   SET status = ?, order_id = COALESCE(?, order_id), error = COALESCE(?, error)
                   WHERE id = ?""",
                (status, order_id, error, local_id),
            )

    def get_recent_orders(self, limit: int = 50) -> list[dict]:
        """Retourne les derniers ordres."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM orders ORDER BY id DESC LIMIT ?", (limit,)
            )
            return [dict(row) for row in cur.fetchall()]

    def get_live_orders(self) -> list[dict]:
        """Retourne les ordres au statut 'live' (posés dans le carnet, pas encore matchés)."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM orders WHERE status = 'live' ORDER BY id DESC")
            return [dict(row) for row in cur.fetchall()]

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

    def get_position_usdc(self, token_id: str) -> float:
        """Retourne l'exposition en USDC = quantity * avg_price (signé, 0.0 si aucune position)."""
        with self._cursor() as cur:
            cur.execute("SELECT quantity, avg_price FROM positions WHERE token_id = ?", (token_id,))
            row = cur.fetchone()
            if not row:
                return 0.0
            return row["quantity"] * row["avg_price"]

    def get_all_positions(self) -> list[dict]:
        """Retourne toutes les positions actives (quantity != 0)."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM positions WHERE quantity != 0.0")
            return [dict(row) for row in cur.fetchall()]

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
