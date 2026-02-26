"""
Point d'entrée unique du projet.
Usage :
    python run.py bot        → lance le bot de trading
    python run.py dashboard  → lance le dashboard web
"""

import logging
import sys

from bot.config import load_config
from db.database import Database


def setup_logging(level: str):
    """Configure le logging global."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("bot", "dashboard"):
        print("Usage: python run.py [bot|dashboard]")
        sys.exit(1)

    mode = sys.argv[1]
    config = load_config()
    setup_logging(config.log_level)
    db = Database(config.db_path)

    if mode == "bot":
        from bot.trader import Trader

        trader = Trader(config, db)
        trader.start()

    elif mode == "dashboard":
        import subprocess

        bind = f"{config.dashboard.host}:{config.dashboard.port}"
        logger = logging.getLogger("dashboard")
        logger.info("Démarrage dashboard via Gunicorn sur %s", bind)

        cmd = [
            sys.executable, "-m", "gunicorn",
            "--bind", bind,
            "--workers", "2",
            "--timeout", "30",
            "--access-logfile", "-",
            "dashboard.wsgi:app",
        ]
        try:
            subprocess.run(cmd, check=True)
        except FileNotFoundError:
            # Fallback : gunicorn non installé (Windows dev) → Flask dev server
            logger.warning("Gunicorn non disponible, fallback Flask dev server")
            from dashboard.app import create_app

            app = create_app(config, db)
            app.run(
                host=config.dashboard.host,
                port=config.dashboard.port,
                debug=False,
            )


if __name__ == "__main__":
    main()
