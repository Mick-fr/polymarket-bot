import os
import requests
import logging

logger = logging.getLogger(__name__)

# 2026 FINAL TELEGRAM NOTIFIER
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def send_alert(text: str):
    """Envoie une alerte Telegram si configuré."""
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try:
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=5)
        except Exception as e:
            logger.debug("[Telegram] Échec d'envoi de l'alerte: %s", e)
