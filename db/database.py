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
