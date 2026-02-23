"""
Configuration du bot Polymarket.
Charge toutes les variables depuis le fichier .env.
Aucune valeur sensible n'est codée en dur.
"""

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml
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
    # 10 USDC laisse de la marge pour le minimum 5 shares Polymarket
    max_order_size: float = 10.0
    # Perte maximale quotidienne avant arrêt automatique (en USDC)
    max_daily_loss: float = 10.0
    # Nombre de tentatives de reconnexion avant pause longue
    max_retries: int = 5
    # Pause entre les tentatives de reconnexion (secondes)
    retry_delay: int = 30
    # Paper trading : simule les ordres sans les envoyer à Polymarket
    paper_trading: bool = False
    paper_balance: float = 1000.0   # Solde fictif initial (USDC)
    # Exposition nette maximale par marché (fraction du solde disponible)
    # Ex : 0.20 = 20% du solde. Augmenter si le portefeuille est petit (< 100 USDC).
    max_exposure_pct: float = 0.20
    # Stop-loss par position : si la perte latente (avg_price - mid) / avg_price >= seuil,
    # le bot émet un SELL market pour couper immédiatement.
    # Ex : 0.25 = coupe si la position a perdu 25% de sa valeur.
    # 0.0 = désactivé.
    position_stop_loss_pct: float = 0.25

    # 2026 TOP BOT UPGRADE
    rebates_eligible: bool = True
    ai_edge_threshold: float = 0.07
    ai_weight: float = 0.6
    # 2026 FINAL TELEGRAM
    telegram_token: str = ""
    telegram_chat_id: str = ""
    
    # 2026 V6.2 FINAL
    telegram_enabled: bool = True

    # 2026 V6 SCALING
    as_enabled: bool = True
    as_risk_aversion: float = 0.25
    as_inventory_target: float = 0.0

    copy_trading_enabled: bool = False
    copy_top_n: int = 10

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


def _load_password_hash(data_dir: Path) -> str:
    """
    Charge le hash bcrypt depuis data/pw_hash.txt.
    Ce fichier évite les problèmes d'interpolation Docker Compose
    avec les caractères $ présents dans les hashes bcrypt.
    Fallback sur DASHBOARD_PASSWORD_HASH si le fichier n'existe pas.
    """
    pw_file = data_dir / "pw_hash.txt"
    if pw_file.exists():
        h = pw_file.read_text().strip()
        if h:
            return h
    # Fallback : variable d'environnement (peut être tronquée)
    h = os.getenv("DASHBOARD_PASSWORD_HASH", "")
    if not h:
        print("FATAL: DASHBOARD_PASSWORD_HASH manquant (ni data/pw_hash.txt ni variable env)")
        sys.exit(1)
    return h


def load_config() -> AppConfig:
    """Construit la configuration depuis les variables d'environnement et params.yaml."""
    data_dir = _PROJECT_ROOT / "data"
    data_dir.mkdir(exist_ok=True)
    
    # Charge le YAML (fallback si fichier absent)
    config_yaml_path = _PROJECT_ROOT / "config" / "params.yaml"
    yml = {}
    if config_yaml_path.exists():
        try:
            with open(config_yaml_path, "r", encoding="utf-8") as f:
                yml = yaml.safe_load(f) or {}
        except Exception as e:
            print(f"WARN: Impossible de charger {config_yaml_path}: {e}")

    bot_yml = yml.get("bot", {})
    tgram_yml = yml.get("telegram", {})

    return AppConfig(
        polymarket=PolymarketConfig(
            private_key=_require_env("POLYMARKET_PRIVATE_KEY"),
            funder_address=os.getenv("POLYMARKET_FUNDER_ADDRESS", ""),
            signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0")),
        ),
        bot=BotConfig(
            loop_interval=int(os.getenv("BOT_LOOP_INTERVAL") or bot_yml.get("loop_interval", 60)),
            max_order_size=float(os.getenv("BOT_MAX_ORDER_SIZE") or bot_yml.get("max_order_size", 5.0)),
            max_daily_loss=float(os.getenv("BOT_MAX_DAILY_LOSS") or bot_yml.get("max_daily_loss", 10.0)),
            max_retries=int(os.getenv("BOT_MAX_RETRIES", "5")),
            retry_delay=int(os.getenv("BOT_RETRY_DELAY", "30")),
            paper_trading=os.getenv("BOT_PAPER_TRADING", "false").lower() == "true",
            paper_balance=float(os.getenv("BOT_PAPER_BALANCE", "1000.0")),
            max_exposure_pct=float(os.getenv("BOT_MAX_EXPOSURE_PCT") or bot_yml.get("max_exposure_pct", 0.20)),
            position_stop_loss_pct=float(os.getenv("BOT_STOP_LOSS_PCT") or bot_yml.get("position_stop_loss_pct", 0.25)),
            rebates_eligible=os.getenv("BOT_REBATES_ELIGIBLE", "true").lower() == "true",
            ai_edge_threshold=float(os.getenv("BOT_AI_EDGE_THRESHOLD") or bot_yml.get("ai_edge_threshold", 0.07)),
            ai_weight=float(os.getenv("BOT_AI_WEIGHT") or bot_yml.get("ai_weight", 0.6)),
            telegram_token=os.getenv("TELEGRAM_TOKEN") or tgram_yml.get("token", ""),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID") or str(tgram_yml.get("chat_id", "")),
            
            # 2026 V6.2 FINAL
            telegram_enabled=str(os.getenv("TELEGRAM_ENABLED", "true")).lower() == "true",
            
            # 2026 V6 SCALING
            as_enabled=str(os.getenv("AS_ENABLED") or bot_yml.get("as_enabled", True)).lower() == "true",
            as_risk_aversion=float(os.getenv("AS_RISK_AVERSION") or bot_yml.get("as_risk_aversion", 0.25)),
            as_inventory_target=float(os.getenv("AS_INVENTORY_TARGET") or bot_yml.get("as_inventory_target", 0.0)),
            copy_trading_enabled=str(os.getenv("COPY_TRADING_ENABLED") or bot_yml.get("copy_trading_enabled", False)).lower() == "true",
            copy_top_n=int(os.getenv("COPY_TOP_N") or bot_yml.get("copy_top_n", 10)),
        ),
        dashboard=DashboardConfig(
            port=int(os.getenv("DASHBOARD_PORT", "8080")),
            secret_key=_require_env("DASHBOARD_SECRET_KEY"),
            password_hash=_load_password_hash(data_dir),
        ),
        db_path=str(data_dir / "bot.db"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
