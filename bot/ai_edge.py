"""
Module AI Edge — Grok (x.ai) pour estimation de fair value et sentiment BTC.
Cache TTL intégré pour éviter le rate-limiting et les appels bloquants
dans le hot path du bot (cycle 4-8s).
"""

import os
import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

# ── Cache TTL ────────────────────────────────────────────────────────────────
# Fair value par événement : 5 min (les marchés 5min expirent, inutile de rafraîchir plus souvent)
_FAIR_VALUE_CACHE_TTL = 300
# Sentiment global BTC : 5 min (macro ne change pas en dessous de cette fréquence)
_SENTIMENT_CACHE_TTL = 300

_fair_value_cache: dict[str, tuple[float, float]] = {}  # {event_title: (prob, timestamp)}
_sentiment_cache: tuple[float, float] = (1.0, 0.0)      # (bias, timestamp)

# ── Constantes API ────────────────────────────────────────────────────────────
_GROK_URL = "https://api.x.ai/v1/chat/completions"
_GROK_MODEL = "grok-2-latest"
_SYSTEM_PROMPT = "You are a quantitative analyst. Output ONLY a float number."


def _grok_api_key() -> Optional[str]:
    return os.getenv("GROK_API_KEY") or None


def get_ai_fair_value(event_title: str, description: str = "") -> Optional[float]:
    """
    Interroge l'API Grok (x.ai) pour estimer la vraie probabilité YES de cet événement.
    Retourne la probabilité (0.00 à 1.00) ou None en cas d'erreur.
    Cache TTL de 5 minutes par event_title.
    """
    now = time.time()

    # ── Cache hit ─────────────────────────────────────────────────────────────
    cached = _fair_value_cache.get(event_title)
    if cached is not None and (now - cached[1]) < _FAIR_VALUE_CACHE_TTL:
        return cached[0]

    api_key = _grok_api_key()
    if not api_key:
        return None

    prompt = (
        "Estimate true probability YES for this Polymarket event. "
        "Answer ONLY with a number from 0.00 to 1.00.\n"
        f"Event: {event_title}\nDescription: {description}"
    )

    payload = {
        "model": _GROK_MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 10,
    }

    try:
        resp = requests.post(
            _GROK_URL,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            json=payload,
            timeout=4.0,
        )
        resp.raise_for_status()
        text_val = resp.json()["choices"][0]["message"]["content"].strip()
        prob = max(0.0, min(1.0, float(text_val)))
        _fair_value_cache[event_title] = (prob, now)
        return prob
    except (requests.RequestException, ValueError, KeyError) as e:
        logger.debug("[AI Edge] Grok fetch error pour '%s': %s", event_title[:20], e)
        # Retourne la valeur cachée (stale) si elle existe, sinon None
        if cached is not None:
            return cached[0]
        return None


def get_ai_global_sentiment_bias() -> float:
    """
    Interroge l'API Grok (x.ai) pour obtenir un biais directionnel global sur BTC.
    Retourne un multiplicateur entre 0.80 et 1.20 (1.00 = neutre).
    Cache TTL de 5 minutes.
    """
    global _sentiment_cache
    now = time.time()

    # ── Cache hit ─────────────────────────────────────────────────────────────
    if (now - _sentiment_cache[1]) < _SENTIMENT_CACHE_TTL:
        return _sentiment_cache[0]

    api_key = _grok_api_key()
    if not api_key:
        return 1.0

    prompt = (
        "Analyze the current global sentiment for Bitcoin (latest news, macro). "
        "Output ONLY a single float multiplier between 0.80 and 1.20 where 1.00 is neutral, "
        "1.20 is extremely bullish, and 0.80 is extremely bearish. ONLY output numbers."
    )

    payload = {
        "model": _GROK_MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 5,
    }

    try:
        resp = requests.post(
            _GROK_URL,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            json=payload,
            timeout=4.0,
        )
        resp.raise_for_status()
        text_val = resp.json()["choices"][0]["message"]["content"].strip()
        bias = max(0.80, min(1.20, float(text_val)))
        _sentiment_cache = (bias, now)
        return bias
    except (requests.RequestException, ValueError, KeyError) as e:
        logger.debug("[AI Edge] Erreur Grok sentiment bias: %s", e)
        # Retourne dernière valeur connue au lieu de 1.0 neutre
        return _sentiment_cache[0]
