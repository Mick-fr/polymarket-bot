"""
V10.0 — BinanceWSClient
Thread daemon asynchrone pour maintenir les prix spot/perp et funding rate BTC/ETH
en mémoire sans bloquer le cycle de 8s de trader.py.

Streams utilisés :
  - wss://stream.binance.com:9443/ws/btcusdt@bookTicker
  - wss://stream.binance.com:9443/ws/ethusdt@bookTicker

Le funding rate est pollé toutes les 120s via REST car il n'existe pas en WS pur.
"""

import json
import logging
import threading
import time
import collections
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import websocket  # websocket-client
except ImportError:
    websocket = None  # type: ignore
    logger.warning("[BinanceWS] websocket-client non installé — mode dégradé REST uniquement")

try:
    import requests as _req
except ImportError:
    _req = None  # type: ignore


class BinanceWSClient(threading.Thread):
    """
    Thread daemon : se connecte aux bookTicker Binance pour BTC et ETH,
    met à jour les best bid/ask en mémoire.  Polling REST pour le funding rate.
    """

    WS_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker"
    FUNDING_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
    FUNDING_POLL_SEC = 120  # polling interval pour funding rate

    def __init__(self, on_tick_callback: Optional[callable] = None):
        super().__init__(daemon=True, name="BinanceWS")
        self.on_tick_callback = on_tick_callback
        # Prix spot live
        self.btc_bid: float = 0.0
        self.btc_ask: float = 0.0
        self.eth_bid: float = 0.0
        self.eth_ask: float = 0.0
        # Funding rate (polled)
        self.btc_funding: float = 0.0
        self.eth_funding: float = 0.0
        # V10.5 Momentum & OBI
        self.btc_obi: float = 0.0
        self.eth_obi: float = 0.0
        self.btc_history = collections.deque(maxlen=90)
        self.eth_history = collections.deque(maxlen=90)
        # Metadata
        self._last_update_ts: float = 0.0
        self._connected = False
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    # ── Public API ──────────────────────────────────────────────────

    def get_mid(self, symbol: str) -> float:
        """Retourne le mid price live pour 'BTC' ou 'ETH'. 0.0 si pas dispo."""
        sym = symbol.upper()
        with self._lock:
            if "BTC" in sym:
                if self.btc_bid > 0 and self.btc_ask > 0:
                    return (self.btc_bid + self.btc_ask) / 2
            elif "ETH" in sym:
                if self.eth_bid > 0 and self.eth_ask > 0:
                    return (self.eth_bid + self.eth_ask) / 2
        return 0.0

    def get_funding(self, symbol: str) -> float:
        sym = symbol.upper()
        with self._lock:
            return self.btc_funding if "BTC" in sym else self.eth_funding

    def get_binance_obi(self, symbol: str) -> float:
        sym = symbol.upper()
        with self._lock:
            return self.btc_obi if "BTC" in sym else self.eth_obi

    def get_30s_momentum(self, symbol: str) -> float:
        sym = symbol.upper()
        with self._lock:
            history = self.btc_history if "BTC" in sym else self.eth_history
            if not history or len(history) < 2:
                return 0.0
            
            current_ts, current_mid = history[-1]
            target_ts = current_ts - 30.0
            
            oldest_mid = history[0][1]
            for ts, mid in history:
                if ts >= target_ts:
                    oldest_mid = mid
                    break
            
            if oldest_mid > 0:
                return (current_mid - oldest_mid) / oldest_mid * 100.0
            return 0.0

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def age_seconds(self) -> float:
        """Secondes depuis la dernière mise à jour de price."""
        if self._last_update_ts == 0:
            return float("inf")
        return time.time() - self._last_update_ts

    # ── Thread run ──────────────────────────────────────────────────

    def run(self):
        logger.info("[BinanceWS] Démarrage thread daemon")
        # Lancer le polling funding rate dans un sous-thread
        funding_thread = threading.Thread(target=self._poll_funding, daemon=True, name="BinanceFunding")
        funding_thread.start()
        # Boucle reconnexion
        while not self._stop_event.is_set():
            try:
                self._run_ws()
            except Exception as e:
                logger.warning("[BinanceWS] Erreur WS: %s — reconnexion dans 5s", e)
                self._connected = False
                time.sleep(5)

    def stop(self):
        self._stop_event.set()

    # ── WebSocket ────────────────────────────────────────────────────

    def _run_ws(self):
        if websocket is None:
            logger.warning("[BinanceWS] Pas de lib websocket — fallback REST")
            self._poll_rest_forever()
            return

        ws = websocket.WebSocketApp(
            self.WS_URL,
            on_message=self._on_message,
            on_open=self._on_open,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        ws.run_forever(ping_interval=30, ping_timeout=10)

    def _on_open(self, ws):
        self._connected = True
        logger.info("[BinanceWS] Connecté aux bookTicker BTC/ETH")

    def _on_message(self, ws, message):
        try:
            outer = json.loads(message)
            data = outer.get("data", outer)
            symbol = data.get("s", "")
            bid = float(data.get("b", 0))
            ask = float(data.get("a", 0))
            with self._lock:
                now = time.time()
                if symbol == "BTCUSDT":
                    self.btc_bid = bid
                    self.btc_ask = ask
                    bid_qty = float(data.get("B", 0))
                    ask_qty = float(data.get("A", 0))
                    self.btc_obi = (bid_qty - ask_qty) / (bid_qty + ask_qty + 1e-8)
                    mid = (bid + ask) / 2
                    if not self.btc_history or now - self.btc_history[-1][0] >= 1.0:
                        self.btc_history.append((now, mid))
                elif symbol == "ETHUSDT":
                    self.eth_bid = bid
                    self.eth_ask = ask
                    bid_qty = float(data.get("B", 0))
                    ask_qty = float(data.get("A", 0))
                    self.eth_obi = (bid_qty - ask_qty) / (bid_qty + ask_qty + 1e-8)
                    mid = (bid + ask) / 2
                    if not self.eth_history or now - self.eth_history[-1][0] >= 1.0:
                        self.eth_history.append((now, mid))
                self._last_update_ts = now
            
            # V13 Callback Asynchrone : Push Data Upstream Immediately
            if self.on_tick_callback and symbol in ["BTCUSDT", "ETHUSDT"]:
                mid_signal = self.btc_bid + self.btc_ask / 2 if symbol == "BTCUSDT" else self.eth_bid + self.eth_ask / 2
                threading.Thread(target=self.on_tick_callback, args=(symbol, mid_signal), daemon=True).start()

        except Exception:
            pass

    def _on_error(self, ws, error):
        logger.debug("[BinanceWS] Erreur: %s", error)

    def _on_close(self, ws, close_status_code, close_msg):
        self._connected = False
        logger.info("[BinanceWS] Connexion fermée (%s)", close_msg)

    # ── Fallback REST (si pas de lib websocket) ─────────────────────

    def _poll_rest_forever(self):
        """Fallback : polling REST toutes les 3s."""
        while not self._stop_event.is_set():
            try:
                if _req:
                    r = _req.get("https://api.binance.com/api/v3/ticker/bookTicker",
                                 params={"symbols": '["BTCUSDT","ETHUSDT"]'}, timeout=5)
                    for item in r.json():
                        sym = item["symbol"]
                        bid = float(item["bidPrice"])
                        ask = float(item["askPrice"])
                        with self._lock:
                            now = time.time()
                            bid_qty = float(item.get("bidQty", 0))
                            ask_qty = float(item.get("askQty", 0))
                            obi = (bid_qty - ask_qty) / (bid_qty + ask_qty + 1e-8)
                            mid = (bid + ask) / 2
                            
                            if sym == "BTCUSDT":
                                self.btc_bid, self.btc_ask = bid, ask
                                self.btc_obi = obi
                                if not self.btc_history or now - self.btc_history[-1][0] >= 1.0:
                                    self.btc_history.append((now, mid))
                            elif sym == "ETHUSDT":
                                self.eth_bid, self.eth_ask = bid, ask
                                self.eth_obi = obi
                                if not self.eth_history or now - self.eth_history[-1][0] >= 1.0:
                                    self.eth_history.append((now, mid))
                            self._last_update_ts = now

                        # V13 Callback Asynchrone (Fallback)
                        if self.on_tick_callback and sym in ["BTCUSDT", "ETHUSDT"]:
                            threading.Thread(target=self.on_tick_callback, args=(sym, mid), daemon=True).start()
                            
                    self._connected = True
            except Exception as e:
                logger.debug("[BinanceWS REST] Erreur polling: %s", e)
            time.sleep(3)

    # ── Funding Rate polling ────────────────────────────────────────

    def _poll_funding(self):
        """Poll le funding rate toutes les 120s."""
        while not self._stop_event.is_set():
            try:
                if _req:
                    r = _req.get(self.FUNDING_URL,
                                 params={"symbol": "BTCUSDT"}, timeout=5)
                    data = r.json()
                    with self._lock:
                        self.btc_funding = float(data.get("lastFundingRate", 0))

                    r2 = _req.get(self.FUNDING_URL,
                                  params={"symbol": "ETHUSDT"}, timeout=5)
                    data2 = r2.json()
                    with self._lock:
                        self.eth_funding = float(data2.get("lastFundingRate", 0))

                    logger.debug("[BinanceWS] Funding update: BTC=%.6f%% ETH=%.6f%%",
                                 self.btc_funding * 100, self.eth_funding * 100)
            except Exception as e:
                logger.debug("[BinanceWS] Erreur funding poll: %s", e)
            time.sleep(self.FUNDING_POLL_SEC)
