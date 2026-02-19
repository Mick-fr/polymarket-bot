"""
Point d'entrée WSGI pour gunicorn.
Usage : gunicorn "dashboard.wsgi:app"
"""

import logging
import sys

from bot.config import load_config
from dashboard.app import create_app
from db.database import Database

# Configure le logging
config = load_config()
logging.basicConfig(
    level=getattr(logging, config.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

db = Database(config.db_path)
app = create_app(config, db)
