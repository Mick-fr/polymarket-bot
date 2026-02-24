import os
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

# 2026 TOP BOT UPGRADE
def get_ai_fair_value(event_title: str, description: str = "") -> Optional[float]:
    """
    Interroge l'API Grok (x.ai) pour estimer la vraie probabilité YES de cet événement.
    Retourne la probabilité (0.00 à 1.00) ou None en cas d'erreur.
    """
    api_key = os.getenv("GROK_API_KEY")
    if not api_key:
        return None
    
    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    prompt = (
        "Estimate true probability YES for this Polymarket event. "
        "Answer ONLY with a number from 0.00 to 1.00.\n"
        f"Event: {event_title}\nDescription: {description}"
    )
    
    payload = {
        "model": "grok-2-latest",
        "messages": [
            {
                "role": "system",
                "content": "You are a quantitative analyst. Output ONLY a float number."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.0,
        "max_tokens": 10
    }
    
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=5.0)
        resp.raise_for_status()
        text_val = resp.json()["choices"][0]["message"]["content"].strip()
        prob = float(text_val)
        return max(0.0, min(1.0, prob))
    except (requests.RequestException, ValueError, KeyError) as e:
        logger.debug("[AI Edge] Grok fetch error pour '%s': %s", event_title[:20], e)
        return None

def get_ai_global_sentiment_bias() -> float:
    """
    V14.0: Interroge l'API Grok (x.ai) pour obtenir un biais directionnel global sur BTC.
    Retourne un multiplicateur entre 0.80 et 1.20 (1.00 = neutre).
    """
    api_key = os.getenv("GROK_API_KEY")
    if not api_key:
        return 1.0
        
    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    prompt = (
        "Analyze the current global sentiment for Bitcoin (latest news, macro). "
        "Output ONLY a single float multiplier between 0.80 and 1.20 where 1.00 is neutral, "
        "1.20 is extremely bullish, and 0.80 is extremely bearish. ONLY output numbers."
    )
    
    payload = {
        "model": "grok-2-latest",
        "messages": [
            {
                "role": "system",
                "content": "You are a quantitative analyst. Output ONLY a float number."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.0,
        "max_tokens": 5
    }
    
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=8.0)
        resp.raise_for_status()
        text_val = resp.json()["choices"][0]["message"]["content"].strip()
        bias = float(text_val)
        return max(0.80, min(1.20, bias))
    except (requests.RequestException, ValueError, KeyError) as e:
        logger.debug("[AI Edge] Erreur Grok sentiment bias: %s", e)
        return 1.0
