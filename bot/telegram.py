import os
import re
import requests
import logging

logger = logging.getLogger(__name__)

# 2026 FINAL TELEGRAM NOTIFIER
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Telegram hard-limit : 4096 chars par message
_MAX_TEXT_LEN = 4096
# Format attendu : <bot_id>:<alphanumeric_token>
_TOKEN_RE = re.compile(r'^\d+:[A-Za-z0-9_-]{35,}$')


def _is_valid_token(token: str) -> bool:
    return bool(token and _TOKEN_RE.match(token))


def _is_valid_chat_id(chat_id: str) -> bool:
    try:
        int(chat_id)
        return True
    except (ValueError, TypeError):
        return False


def send_alert(text: str):
    """Envoie une alerte Telegram si configuré."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if not _is_valid_token(TELEGRAM_TOKEN):
        logger.warning("[Telegram] Token invalide — format attendu: <id>:<str>")
        return
    if not _is_valid_chat_id(TELEGRAM_CHAT_ID):
        logger.warning("[Telegram] CHAT_ID invalide — doit être un entier")
        return

    safe_text = str(text)[:_MAX_TEXT_LEN]
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_text},
            timeout=5,
        )
    except Exception as e:
        logger.debug("[Telegram] Échec d'envoi de l'alerte: %s", e)
