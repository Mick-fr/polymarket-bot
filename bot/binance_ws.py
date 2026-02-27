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
from queue import Queue, Full
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

    # Upgrade 4 — depth5 anti-spoofing + aggTrade CVD (BTC uniquement — focus sprint)
    WS_URL = (
        "wss://stream.binance.com:9443/stream?streams="
        "btcusdt@bookTicker/ethusdt@bookTicker"
        "/btcusdt@depth5@100ms"   # 5-niveau OBI anti-spoof
        "/btcusdt@aggTrade"       # CVD taker flow
    )
    FUNDING_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
    FUNDING_POLL_SEC = 120  # polling interval pour funding rate

    def __init__(self, on_tick_callback: Optional[callable] = None):
        super().__init__(daemon=True, name="BinanceWS")
        self.on_tick_callback = on_tick_callback
        # R6: Queue bornée remplace ThreadPoolExecutor unbounded
        # maxsize=16 → drop silencieux si le callback est trop lent,
        # on préfère être à jour que traiter des ticks obsolètes
        self._tick_queue: Queue = Queue(maxsize=16)
        # Prix spot live
        self.btc_bid: float = 0.0
        self.btc_ask: float = 0.0
        self.eth_bid: float = 0.0
        self.eth_ask: float = 0.0
        # V10.5 Momentum & OBI (L1 — conservé pour compatibilité)
        self.btc_obi: float = 0.0
        self.btc_obi_ema: float = 0.0
        self.eth_obi: float = 0.0
        self.eth_obi_ema: float = 0.0
        # Upgrade 4 — OBI depth5 (anti-spoofing) + CVD (taker flow)
        self.btc_obi_d5:     float = 0.0   # OBI 5-niveau [-1, +1]
        self.btc_obi_d5_ema: float = 0.0
        # deque (ts, delta_usdc): maxlen couvre ~5 min à 100 trades/s
        self.btc_cvd_history: collections.deque = collections.deque(maxlen=30_000)
        # Funding rate (polled)
        self.btc_funding: float = 0.0
        self.eth_funding: float = 0.0
        # Upgrade 2: étendu 90→400 pour couvrir 5min de 1s samples (XGBoost features)
        self.btc_history = collections.deque(maxlen=400)
        self.eth_history = collections.deque(maxlen=400)
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

    def get_obi_depth5(self, symbol: str) -> float:
        """OBI 5-niveaux anti-spoofing pour BTC.

        Utilise les données @depth5@100ms (EMA α=0.15).
        Retourne l'OBI L1 standard si depth5 pas encore reçu.
        """
        sym = symbol.upper()
        if "BTC" not in sym:
            return self.get_binance_obi(symbol)
        with self._lock:
            d5 = self.btc_obi_d5
        # Fallback L1 si depth5 indisponible (démarrage ou reconnexion)
        return d5 if d5 != 0.0 else self.get_binance_obi(symbol)

    def get_cvd(self, symbol: str, n_seconds: float = 60.0) -> float:
        """CVD normalisé (Cumulative Volume Delta taker) sur n_seconds.

        CVD = (buy_vol - sell_vol) / total_vol ∈ [-1, +1]
        Positif → pression acheteuse, négatif → vendeuse.
        Retourne 0.0 si historique insuffisant (<10 trades) ou symbole non BTC.
        """
        sym = symbol.upper()
        if "BTC" not in sym:
            return 0.0
        cutoff = time.time() - n_seconds
        # deque thread-safe pour lecture (pas de copie coûteuse)
        window = [(ts, d) for ts, d in self.btc_cvd_history if ts >= cutoff]
        if len(window) < 10:
            return 0.0
        buy_vol  = sum(d for _, d in window if d > 0)
        sell_vol = sum(-d for _, d in window if d < 0)
        total    = buy_vol + sell_vol
        if total < 1.0:
            return 0.0
        return (buy_vol - sell_vol) / total  # ∈ [-1, +1]

    def get_30s_momentum(self, symbol: str) -> float:
        sym = symbol.upper()
        with self._lock:
            history = self.btc_history if "BTC" in sym else self.eth_history
            if not history or len(history) < 2:
                return 0.0
            
            current_ts, current_mid = history[-1]
            target_ts = current_ts - 30.0
            
            # V15.2 Fallback: If history is less than 30s old, momentum is zero
            if history[0][0] > target_ts:
                return 0.0
                
            oldest_mid = history[0][1]
            for ts, mid in history:
                if ts >= target_ts:
                    oldest_mid = mid
                    break
            
            if oldest_mid > 0:
                return (current_mid - oldest_mid) / oldest_mid * 100.0
            return 0.0

    def get_ns_momentum(self, symbol: str, n_seconds: float) -> float:
        """Momentum % sur une fenêtre de n_seconds.

        Retourne (price_now - price_n_sec_ago) / price_n_sec_ago * 100.
        Retourne 0.0 si l'historique est insuffisant (< n_seconds).
        Utilisé par le modèle XGBoost : n=60, n=300.
        """
        sym = symbol.upper()
        with self._lock:
            history = self.btc_history if "BTC" in sym else self.eth_history
            if not history or len(history) < 2:
                return 0.0
            current_ts, current_mid = history[-1]
            target_ts = current_ts - n_seconds
            if history[0][0] > target_ts:
                return 0.0  # pas assez d'historique
            ref_mid = history[0][1]
            for ts, mid in history:
                if ts >= target_ts:
                    ref_mid = mid
                    break
            return (current_mid - ref_mid) / ref_mid * 100.0 if ref_mid > 0 else 0.0

    def get_ns_vol(self, symbol: str, n_seconds: float) -> float:
        """Volatilité réalisée annualisée sur une fenêtre de n_seconds.

        Std des log-returns 1s × sqrt(31_536_000) — borné [0.10, 3.0].
        Utilisé par le modèle XGBoost : n=60 (vol courte).
        """
        import math
        sym = symbol.upper()
        with self._lock:
            history = self.btc_history if "BTC" in sym else self.eth_history
            if not history or len(history) < 5:
                return 0.60
            current_ts = history[-1][0]
            cutoff_ts  = current_ts - n_seconds
            window = [(ts, mid) for ts, mid in history if ts >= cutoff_ts]
        if len(window) < 5:
            return 0.60
        log_rets = []
        for i in range(1, len(window)):
            p0, p1 = window[i-1][1], window[i][1]
            if p0 > 0 and p1 > 0:
                log_rets.append(math.log(p1 / p0))
        if not log_rets:
            return 0.60
        mean = sum(log_rets) / len(log_rets)
        var  = sum((r - mean) ** 2 for r in log_rets) / len(log_rets)
        ann  = math.sqrt(var) * math.sqrt(31_536_000)
        return max(0.10, min(3.0, ann))

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
        # R6: Worker unique pour les callbacks (backpressure via queue bornée)
        def _callback_worker():
            while not self._stop_event.is_set():
                try:
                    symbol, mid = self._tick_queue.get(timeout=1.0)
                except Exception:
                    continue
                try:
                    self.on_tick_callback(symbol, mid)
                except Exception as e:
                    logger.debug("[BinanceWS] callback error: %s", e)
        if self.on_tick_callback:
            threading.Thread(target=_callback_worker, daemon=True, name="BinanceCB").start()
        # Lancer le polling funding rate dans un sous-thread
        funding_thread = threading.Thread(target=self._poll_funding, daemon=True, name="BinanceFunding")
        funding_thread.start()
        # Boucle reconnexion
        while not self._stop_event.is_set():
            try:
                self._run_ws()
            except Exception as e:
                import traceback
                logger.warning("[BinanceWS] Erreur WS: %s — %s - reconnexion dans 5s", e, traceback.format_exc(limit=2))
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
        ws.run_forever(ping_interval=60, ping_timeout=30)

    def _on_open(self, ws):
        self._connected = True
        logger.info("[BinanceWS] ==================================")
        logger.info("[BinanceWS] Connecté aux bookTicker BTC/ETH (ping: 60s, timeout: 30s)")
        logger.info("[BinanceWS] ==================================")

    def _on_message(self, ws, message):
        try:
            outer = json.loads(message)
            stream = outer.get("stream", "")
            data   = outer.get("data", outer)

            # ── Upgrade 4a : OBI depth5 ──────────────────────────────────────
            if "depth5" in stream:
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                bid_vol = sum(float(q) for _, q in bids[:5])
                ask_vol = sum(float(q) for _, q in asks[:5])
                raw_d5 = (bid_vol - ask_vol) / (bid_vol + ask_vol + 1e-8)
                with self._lock:
                    prev = self.btc_obi_d5_ema
                    ema = 0.15 * raw_d5 + 0.85 * prev if prev != 0.0 else raw_d5
                    self.btc_obi_d5_ema = ema
                    self.btc_obi_d5 = ema
                return

            # ── Upgrade 4b : CVD aggTrade ─────────────────────────────────────
            if "aggTrade" in stream:
                qty   = float(data.get("q", 0))    # base qty (BTC)
                price = float(data.get("p", 0))    # price (USDC)
                is_buyer_maker = data.get("m", False)
                # buyer_maker=True → taker est vendeur → delta négatif
                delta = -(qty * price) if is_buyer_maker else (qty * price)
                self.btc_cvd_history.append((time.time(), delta))
                return

            # ── bookTicker (prix + L1 OBI EMA) ───────────────────────────────
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
                    raw_obi = (bid_qty - ask_qty) / (bid_qty + ask_qty + 1e-8)
                    self.btc_obi_ema = 0.2 * raw_obi + 0.8 * self.btc_obi_ema if self.btc_obi_ema else raw_obi
                    self.btc_obi = self.btc_obi_ema
                    mid = (bid + ask) / 2
                    if not self.btc_history or now - self.btc_history[-1][0] >= 1.0:
                        self.btc_history.append((now, mid))
                elif symbol == "ETHUSDT":
                    self.eth_bid = bid
                    self.eth_ask = ask
                    bid_qty = float(data.get("B", 0))
                    ask_qty = float(data.get("A", 0))
                    raw_obi = (bid_qty - ask_qty) / (bid_qty + ask_qty + 1e-8)
                    self.eth_obi_ema = 0.2 * raw_obi + 0.8 * self.eth_obi_ema if self.eth_obi_ema else raw_obi
                    self.eth_obi = self.eth_obi_ema
                    mid = (bid + ask) / 2
                    if not self.eth_history or now - self.eth_history[-1][0] >= 1.0:
                        self.eth_history.append((now, mid))
                self._last_update_ts = now

            # R6: Queue bornée — drop silencieux si en retard
            if self.on_tick_callback and symbol in ["BTCUSDT", "ETHUSDT"]:
                mid_signal = (self.btc_bid + self.btc_ask) / 2.0 if symbol == "BTCUSDT" else (self.eth_bid + self.eth_ask) / 2.0
                try:
                    self._tick_queue.put_nowait((symbol, mid_signal))
                except Full:
                    pass  # Drop : on préfère être à jour

        except Exception as e:
            logger.debug("[BinanceWS] payload json error : %s", e)

    def _on_error(self, ws, error):
        logger.warning("[BinanceWS] ! WebSocket Error Détectée ! : %s", error)

    def _on_close(self, ws, close_status_code, close_msg):
        self._connected = False
        logger.warning("[BinanceWS] -> Connexion fermée (Code: %s, Msg: %s)", close_status_code, close_msg)

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

                        # R6: Queue bornée (Fallback REST)
                        if self.on_tick_callback and sym in ["BTCUSDT", "ETHUSDT"]:
                            try:
                                self._tick_queue.put_nowait((sym, mid))
                            except Full:
                                pass
                            
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
