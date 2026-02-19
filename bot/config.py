"""
Configuration du bot Polymarket.
Charge toutes les variables depuis le fichier .env.
Aucune valeur sensible n'est codée en dur.
"""

import os
import sys
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Charge le .env depuis la racine du projet
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def _require_env(key: str) -> str:
    """Récupère une variable d'environnement obligatoire ou quitte."""
    value = os.getenv(key)
    if not value:
        print(f"FATAL: variable d'environnement manquante: {key}")
        sys.exit(1)
    return value


@dataclass(frozen=True)
class PolymarketConfig:
    """Paramètres de connexion à Polymarket."""

    host: str = "https://clob.polymarket.com"
    chain_id: int = 137  # Polygon mainnet
    private_key: str = ""
    funder_address: str = ""
    # 0 = EOA standard, 1 = email/Magic, 2 = browser proxy
    signature_type: int = 0


@dataclass(frozen=True)
class BotConfig:
    """Paramètres de fonctionnement du bot."""

    # Intervalle entre chaque cycle de la boucle principale (secondes)
    loop_interval: int = 60
    # Capital maximum autorisé par ordre (en USDC)
    max_order_size: float = 5.0
    # Perte maximale quotidienne avant arrêt automatique (en USDC)
    max_daily_loss: float = 10.0
    # Nombre de tentatives de reconnexion avant pause longue
    max_retries: int = 5
    # Pause entre les tentatives de reconnexion (secondes)
    retry_delay: int = 30


@dataclass(frozen=True)
class DashboardConfig:
    """Paramètres du dashboard web."""

    host: str = "0.0.0.0"
    port: int = 8080
    secret_key: str = ""
    password_hash: str = ""


@dataclass(frozen=True)
class AppConfig:
    """Configuration globale de l'application."""

    polymarket: PolymarketConfig
    bot: BotConfig
    dashboard: DashboardConfig
    db_path: str = ""
    log_level: str = "INFO"


def load_config() -> AppConfig:
    """Construit la configuration depuis les variables d'environnement."""
    data_dir = _PROJECT_ROOT / "data"
    data_dir.mkdir(exist_ok=True)

    return AppConfig(
        polymarket=PolymarketConfig(
            private_key=_require_env("POLYMARKET_PRIVATE_KEY"),
            funder_address=os.getenv("POLYMARKET_FUNDER_ADDRESS", ""),
            signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0")),
        ),
        bot=BotConfig(
            loop_interval=int(os.getenv("BOT_LOOP_INTERVAL", "60")),
            max_order_size=float(os.getenv("BOT_MAX_ORDER_SIZE", "5.0")),
            max_daily_loss=float(os.getenv("BOT_MAX_DAILY_LOSS", "10.0")),
            max_retries=int(os.getenv("BOT_MAX_RETRIES", "5")),
            retry_delay=int(os.getenv("BOT_RETRY_DELAY", "30")),
        ),
        dashboard=DashboardConfig(
            port=int(os.getenv("DASHBOARD_PORT", "8080")),
            secret_key=_require_env("DASHBOARD_SECRET_KEY"),
            password_hash=_require_env("DASHBOARD_PASSWORD_HASH"),
        ),
        db_path=str(data_dir / "bot.db"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
