"""
Strategie OBI Market Making pour Polymarket.

Architecture :
  - MarketUniverse  : filtre les marches eligibles via Gamma API
  - OBICalculator   : calcule l'Order Book Imbalance sur 5 niveaux
  - OBIMarketMakingStrategy : genere les signaux bid/ask avec skewing proportionnel

Set B (Balanced) — Hyperparametres optimises 2026 :
  - OBI threshold : +/- 0.20 (plus reactif que 0.30)
  - OBI depth : 5 niveaux (lisse le bruit L1-L2 / anti-spoofing)
  - Skew : proportionnel continu (skew_ticks = obi * 3, arrondi)
  - Min spread : 0.02 USDC (2 ticks — rentable apres gas)
  - Expo max : 8% du solde par marche
  - Inv skew one-sided : 0.60 (reduit inventaire plus tot)
  - Cycle : 8s (trade-off gas vs latency)
  - Ordre size : 3% du solde (viable des 50$)
  - News-breaker : >7% en <5s (detecte plus tot)
  - Maturity-aware : taille /2 si <3 jours (volatilite terminale)
"""

import json
import logging
import time
import urllib.request
import requests
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from bot.polymarket_client import PolymarketClient
from bot.config import load_config
from bot.copy_trader import CopyTrader

logger = logging.getLogger("bot.strategy")

GAMMA_API_URL = "https://gamma-api.polymarket.com/markets"

# ─── Constantes OBI — Set B (Balanced) ──────────────────────────────────────

OBI_BULLISH_THRESHOLD  =  0.20   # OBI > +0.20 → pression acheteuse (ex: 0.30)
OBI_BEARISH_THRESHOLD  = -0.20   # OBI < -0.20 → pression vendeuse (ex: -0.30)
OBI_SKEW_FACTOR        =  3.0    # Multiplicateur OBI → ticks de skew (continu)
MIN_PRICE              =  0.20
MAX_PRICE              =  0.80
MIN_SPREAD             =  0.01   # 1 tick min — rentable apres gas (MODIFIÉ V11.7)
MIN_SPREAD_HIGH_VOL    =  0.01   # 1 tick accepte si volume > HIGH_VOL_THRESHOLD
HIGH_VOL_THRESHOLD     =  50_000.0  # Volume 24h au-dessus duquel spread 1 tick OK
MIN_VOLUME_24H         =  10_000.0
MAX_DAYS_TO_EXPIRY     =  14
TICK_SIZE              =  0.01   # 1 tick = 0.01 USDC
MATURITY_SHORT_DAYS    =  3.0    # Seuil maturite courte → sizing reduit
MATURITY_SHORT_FACTOR  =  0.5    # Facteur sizing pour maturite courte
NEWS_BREAKER_THRESHOLD =  0.07   # Move mid > 7% en < 5s → cooldown (ex: 0.10)
NEWS_BREAKER_WINDOW    =  5.0    # Fenetre temporelle news-breaker (secondes)
NEWS_BREAKER_COOLDOWN  =  600    # Duree cooldown news-breaker (10 min)
ORDER_SIZE_PCT         =  0.03   # 3% du solde par ordre (ex: 0.02)
MAX_NET_EXPOSURE_PCT   =  0.20   # 20% du solde = expo nette max par marche (défaut, surchargé via BOT_MAX_EXPOSURE_PCT)
INVENTORY_SKEW_THRESHOLD = 0.60  # Ratio >= 60% → one-sided (ex: 0.70)

# HOTFIX 2026-02-22 + 2026 TOP BOT — AI Edge params (module-level defaults)
import os as _os
AI_ENABLED          = _os.getenv("BOT_AI_ENABLED", "true").lower() == "true"
AI_EDGE_THRESHOLD   = float(_os.getenv("BOT_AI_EDGE_THRESHOLD", "0.07"))
AI_WEIGHT           = float(_os.getenv("BOT_AI_WEIGHT", "0.6"))
AI_COOLDOWN_SECONDS = int(_os.getenv("BOT_AI_COOLDOWN_SECONDS", "300"))


# ─── Dataclasses ─────────────────────────────────────────────────────────────

@dataclass
class Signal:
    """Un signal de trading (bid OU ask sur un token)."""
    token_id:        str
    market_id:       str
    market_question: str
    side:            str             # 'buy' ou 'sell'
    order_type:      str             # 'limit' ou 'market'
    price:           Optional[float] # None pour market orders
    size:            float           # shares (limit) ou USDC (market)
    confidence:      float           # 0.0 → 1.0
    reason:          str
    # Contexte marché au moment du signal (pour analytics post-hoc)
    obi_value:       float = 0.0
    obi_regime:      str   = "neutral"
    spread_at_signal: float = 0.0
    volume_24h:      float = 0.0
    mid_price:       float = 0.0
    # V38: p_true au moment du FIRE — utilisé par SprintMaker probe pour valider l'edge AMM
    p_true:          float = 0.5
    # V40: yes_token_id pour libérer le cooldown en cas d'ordre non envoyé (network failure)
    yes_token_id:    str   = ""


@dataclass
class EligibleMarket:
    """Marche passant tous les filtres Universe Selection."""
    market_id:      str
    question:       str
    yes_token_id:   str
    no_token_id:    str
    mid_price:      float
    best_bid:       float
    best_ask:       float
    spread:         float
    volume_24h:     float
    end_date_ts:    float   # UNIX timestamp
    days_to_expiry: float


@dataclass
class OBIResult:
    """Resultat du calcul OBI."""
    obi:      float          # [-1, +1]
    v_bid:    float          # Volume agrege bid
    v_ask:    float          # Volume agrege ask
    regime:   str            # 'bullish' | 'bearish' | 'neutral'
def fetch_sprint_targets():
    """Fetches the specific 5-Min BTC markets directly by slug."""
    current_ts = int(time.time())
    base_ts = current_ts - (current_ts % 300)
    slugs = [
        f"btc-updown-5m-{base_ts}",
        f"btc-updown-5m-{base_ts + 300}",
        f"btc-updown-5m-{base_ts + 600}",
        f"eth-updown-5m-{base_ts}",
        f"eth-updown-5m-{base_ts + 300}",
        f"eth-updown-5m-{base_ts + 600}"
    ]
    logger.debug("[V15.2 Sprints] Polling Sprint Targets: %s", slugs)
    sprint_markets = []
    for slug in slugs:
        try:
            url = f"https://gamma-api.polymarket.com/events?slug={slug}"
            res = requests.get(url, timeout=5)
            if res.status_code == 200:
                data = res.json()
                if data and len(data) > 0:
                    markets = data[0].get("markets", [])
                    # Ensure we flag these as explicitly targeted so they bypass filters later
                    for m in markets:
                        m['_is_direct_target'] = True
                    sprint_markets.extend(markets)
        except Exception as e:
            pass
    return sprint_markets

# ─── Universe Selection ──────────────────────────────────────────────────────

class MarketUniverse:
    """
    Filtre les marches Polymarket eligibles au market making.
    Source : API Gamma (enableOrderBook=true).
    """

    def __init__(self, max_days: int = MAX_DAYS_TO_EXPIRY):
        self._max_days = max_days
        self._cache: list[EligibleMarket] = []
        self._cache_ts: float = 0.0
        self._cache_ttl: float = 60.0   # MODIFIÉ V15: Restauration du cache (rate-limit protection)
        # Funnel counters — reset each refresh cycle, read by InfoEdgeOnlyStrategy
        self._f_raw      = 0
        self._f_price    = 0
        self._f_volume   = 0
        self._f_spread   = 0
        self._f_eligible = 0
        self._f_sprint   = 0

    def _funnel_reset(self):
        self._f_raw = 0; self._f_price = 0; self._f_volume = 0
        self._f_spread = 0; self._f_eligible = 0; self._f_sprint = 0

    def get_eligible_markets(self, force_refresh: bool = False) -> list[EligibleMarket]:
        """Retourne la liste des marches eligibles (avec cache 60s)."""
        now = time.time()
        if not force_refresh and (now - self._cache_ts) < self._cache_ttl and self._cache:
            return self._cache

        raw = self._fetch_gamma_markets()
        # 2026 V6.4 ULTRA-SURGICAL
        # self._detect_cross_arbitrage(raw)  # Removed caller to match new signature
        self._funnel_reset()
        self._f_raw = len(raw)

        eligible = []
        for m in raw:
            result = self._evaluate(m)
            if result is not None:
                eligible.append(result)
        self._f_eligible = len(eligible)

        logger.debug(
            "[Universe] %d marches bruts -> %d eligibles (filtres: prix, spread>=%.2f/%.2f, vol>=%.0f, mat<=%dj)",
            len(raw), len(eligible), MIN_SPREAD_HIGH_VOL, MIN_SPREAD, MIN_VOLUME_24H, MAX_DAYS_TO_EXPIRY,
        )
        self._cache = eligible
        self._cache_ts = now
        return eligible

    # 2026 V6.6 ULTRA-CHIRURGICAL
    def _detect_cross_arbitrage(self, event_id: str, bids: list, asks: list):
        # 2026 V6.5 FINAL MANUAL - SPAM ZÉRO
        if bids and max(b[0] for b in bids) >= 1.25:
            delta = max(b[0] for b in bids) - 1.0
            logger.warning(f"[ARB] {event_id} bids={delta:.1%}")
        if asks and min(a[0] for a in asks) <= 0.75:
            delta = 1.0 - min(a[0] for a in asks)
            logger.warning(f"[ARB] {event_id} asks={delta:.1%}")

    def _fetch_gamma_markets(self, limit: int = 500) -> list[dict]: # V11.13 : Limite à 500
        url = (
            f"{GAMMA_API_URL}?limit={limit}"
            f"&active=true&closed=false"
        )
        markets = []
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "polymarket-bot/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                markets = data if isinstance(data, list) else data.get("markets", [])
        except Exception as e:
            logger.error("[Universe] Erreur Gamma API: %s", e)
            
        # --- V11.14: INJECT DIRECT TARGETS ---
        sprint_targets = fetch_sprint_targets()
        if sprint_targets:
            markets.extend(sprint_targets)
        # ---------------------------------------

        return markets

    def _evaluate(self, m: dict) -> Optional[EligibleMarket]:
        """Applique tous les filtres sur un marche brut. Retourne None si rejete."""
        if m.get('_is_direct_target'):
            # Parse l'endDate réel pour avoir un countdown précis
            _end_str = (m.get("endDate") or m.get("expiration") or m.get("end_date_iso") or "")
            _days_left = 300 / 86400.0
            _end_ts    = time.time() + 300
            if _end_str:
                try:
                    _s = _end_str.replace("Z", "+00:00")
                    if "T" not in _s:
                        _s += "T00:00:00+00:00"
                    _end_dt = datetime.fromisoformat(_s)
                    if _end_dt.tzinfo is None:
                        _end_dt = _end_dt.replace(tzinfo=timezone.utc)
                    _days_left = (_end_dt - datetime.now(timezone.utc)).total_seconds() / 86400.0
                    _end_ts    = _end_dt.timestamp()
                except Exception:
                    pass
            return EligibleMarket(
                market_id=str(m.get("id") or m.get("conditionId") or ""),
                question=m.get("question", ""),
                yes_token_id=json.loads(m.get("clobTokenIds", "[]"))[0] if isinstance(m.get("clobTokenIds"), str) else (m.get("clobTokenIds", [])[0] if m.get("clobTokenIds") else ""),
                no_token_id=json.loads(m.get("clobTokenIds", "[]"))[1] if isinstance(m.get("clobTokenIds"), str) else (m.get("clobTokenIds", [])[1] if len(m.get("clobTokenIds", []))>1 else ""),
                mid_price=0.5,
                best_bid=0.0,
                best_ask=1.0,
                spread=1.0,
                volume_24h=0.0,
                end_date_ts=_end_ts,
                days_to_expiry=_days_left,
            )

        question = m.get("question", "")

        # ── Parse clobTokenIds ──
        clob_ids = m.get("clobTokenIds") or []
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except (json.JSONDecodeError, ValueError):
                clob_ids = []
        if len(clob_ids) < 2:
            return None   # Besoin de YES et NO

        yes_token = clob_ids[0]
        no_token  = clob_ids[1]

        # --- V11.8 BYPASS POUR LES SPRINTS ---
        # Si c'est un marché Bitcoin qui expire dans moins de 60 minutes, on force son passage.
        is_btc_sprint_candidate = False
        try:
            q_lower = question.lower()
            if "btc" in q_lower or "bitcoin" in q_lower or "eth" in q_lower or "ethereum" in q_lower:
                end_str = m.get("endDate") or m.get("expiration") or m.get("end_date_iso") or ""
                if end_str:
                    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    if end_dt.tzinfo is None:
                        end_dt = end_dt.replace(tzinfo=timezone.utc)
                    mins_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 60.0
                    if 0 < mins_left <= 60.0:
                        is_btc_sprint_candidate = True
        except Exception:
            pass
        # -------------------------------------

        # ── Prix (mid) ──
        try:
            best_bid = float(m.get("bestBid") or 0)
            best_ask = float(m.get("bestAsk") or 1)
        except (TypeError, ValueError):
            return None

        if (best_ask <= best_bid or best_ask == 0) and not is_btc_sprint_candidate:
            return None

        mid = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid

        if not (MIN_PRICE <= mid <= MAX_PRICE) and not is_btc_sprint_candidate:
            logger.debug("[Universe] '%s' rejete: mid=%.3f hors [%.2f, %.2f]",
                         question[:40], mid, MIN_PRICE, MAX_PRICE)
            return None
        self._f_price += 1

        # ── Volume 24h (evaluer avant le spread pour le seuil adaptatif) ──
        try:
            vol = float(m.get("volume24hr") or m.get("volume24hrClob") or 0)
        except (TypeError, ValueError):
            vol = 0.0

        if vol < MIN_VOLUME_24H and not is_btc_sprint_candidate:
            logger.debug("[Universe] '%s' rejete: volume24h=%.0f < %.0f",
                         question[:40], vol, MIN_VOLUME_24H)
            return None
        self._f_volume += 1

        # ── Spread adaptatif (volume-aware) ──
        # Marches tres liquides (>50k vol) : spread 1 tick OK
        # Marches standards : spread 2 ticks minimum
        min_spread = MIN_SPREAD_HIGH_VOL if vol >= HIGH_VOL_THRESHOLD else MIN_SPREAD
        if spread < min_spread and not is_btc_sprint_candidate:
            logger.debug("[Universe] '%s' rejete: spread=%.4f < %.2f (vol=%.0f)",
                         question[:40], spread, min_spread, vol)
            return None
        self._f_spread += 1

        # ── Maturite ──
        end_date_str = m.get("endDate") or m.get("expiration") or m.get("end_date_iso") or ""
        if not end_date_str:
            return None
        try:
            end_date_str = end_date_str.replace("Z", "+00:00")
            if "T" not in end_date_str:
                end_date_str += "T00:00:00+00:00"
            end_dt = datetime.fromisoformat(end_date_str)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            now_dt = datetime.now(timezone.utc)
            days_left = (end_dt - now_dt).total_seconds() / 86400.0
        except (ValueError, OverflowError):
            return None

        if not (0 < days_left <= self._max_days):
            logger.debug("[Universe] '%s' rejete: %.1f jours restants (max=%d)",
                         question[:40], days_left, self._max_days)
            return None

        market_id = str(m.get("id") or m.get("conditionId") or "")

        return EligibleMarket(
            market_id=market_id,
            question=question,
            yes_token_id=yes_token,
            no_token_id=no_token,
            mid_price=mid,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=spread,
            volume_24h=vol,
            end_date_ts=end_dt.timestamp(),
            days_to_expiry=days_left,
        )

# ─── V6 SCALING : Avellaneda-Stoikov Skew ────────────────────────────────────

class AvellanedaStoikovSkew:
    """
    Calcule le skew via Avellaneda-Stoikov (2008).
    Reservation price : r = mid - gamma * sigma^2 * T_remaining * q
    Optimal spread    : delta = gamma * sigma^2 * T_remaining + (2/gamma) * ln(1 + gamma/k)

    Inputs calibrés pour marchés Polymarket 5min BTC :
      - q : inventaire normalisé [-1, +1] = (expo_usdc / max_expo_usdc)
      - sigma : volatilité du mid Polymarket estimée sur fenêtre glissante
      - T_remaining : temps restant normalisé [0, 1] (fraction de la durée du marché)
      - gamma : risk aversion (config.as_risk_aversion, défaut 0.45)
    """
    def __init__(self, risk_aversion: float = 0.45):
        self.gamma = risk_aversion

    def calculate_skew_ticks(self, q: float, sigma: float, t_days: float) -> int:
        """Retourne le nombre de ticks de skew AS directionnel.

        Args:
            q: inventaire normalisé [-1, +1]. >0 = long, <0 = short.
            sigma: volatilité du mid (ex: 0.02 = 2% sur la fenêtre).
                   Si c'est la raw EMA vol, elle est clampée à [0.001, 0.10].
            t_days: temps restant en jours (pour marchés 5min → ~0.0035 jour).
        """
        # Clamp sigma pour éviter les valeurs aberrantes (0 ou NaN)
        sigma = max(0.001, min(0.10, sigma))
        # Clamp q pour éviter les skews extrêmes
        q = max(-1.0, min(1.0, q))
        # Temps restant minimal pour éviter div/0 comportement
        T = max(t_days, 0.001)

        # Reservation price delta (ticks)
        # rp_delta > 0 quand q < 0 (short → incentive BUY → bid monte)
        # rp_delta < 0 quand q > 0 (long → incentive SELL → ask baisse)
        rp_delta = -(q * self.gamma * (sigma ** 2) * T)
        return round(rp_delta / TICK_SIZE)

    def optimal_spread_ticks(self, sigma: float, t_days: float) -> int:
        """Spread optimal A-S (en ticks).

        Formule simplifiée : delta = gamma * sigma^2 * T + 2/gamma * ln(1 + gamma/k)
        Avec k estimé à 1.0 (intensité d'arrivée des ordres, calibré empiriquement).
        """
        import math
        sigma = max(0.001, min(0.10, sigma))
        T = max(t_days, 0.001)
        k = 1.0  # Paramètre d'intensité (à calibrer sur données réelles)
        spread = self.gamma * (sigma ** 2) * T + (2.0 / self.gamma) * math.log(1.0 + self.gamma / k)
        return max(1, round(spread / TICK_SIZE))


# ─── Calcul OBI ──────────────────────────────────────────────────────────────

class OBICalculator:
    """
    Order Book Imbalance sur les 5 meilleurs niveaux (Set B).
    OBI = (V_bid - V_ask) / (V_bid + V_ask)  ∈ [-1, +1]

    5 niveaux lissent le bruit L1-L2 (anti-spoofing : ordres de 10-20 shares
    en L1 qui biaisent artificiellement l'OBI).

    Fallback : si le carnet CLOB est vide (Polymarket AMM), on genere
    un OBI synthetique neutre a partir du bestBid/bestAsk Gamma.
    """

    LEVELS = 5   # Set B : 5 niveaux (ex: 3)

    @classmethod
    def compute(cls, order_book: dict,
                best_bid: float = 0.0,
                best_ask: float = 0.0) -> Optional[OBIResult]:
        """
        order_book : reponse brute de get_order_book()
        Attendu : {"bids": [{"price": "0.55", "size": "100"}, ...],
                   "asks": [{"price": "0.57", "size": "80"},  ...]}

        best_bid / best_ask : prix Gamma pour fallback si carnet vide.
        """
        try:
            # Accepte un dict OU un OrderBookSummary (py-clob-client dataclass)
            if isinstance(order_book, dict):
                bids = order_book.get("bids") or []
                asks = order_book.get("asks") or []
            else:
                bids = getattr(order_book, "bids", None) or []
                asks = getattr(order_book, "asks", None) or []

            # Normalise chaque niveau : OrderSummary(price, size) ou dict
            def _to_dict(e):
                if isinstance(e, dict):
                    return e
                p = getattr(e, "price", None)
                s = getattr(e, "size",  None)
                if p is None or s is None:
                    return None
                return {"price": str(p), "size": str(s)}

            bids = [d for e in bids if (d := _to_dict(e)) is not None]
            asks = [d for e in asks if (d := _to_dict(e)) is not None]

            # Trier : bids decroissant, asks croissant
            bids_sorted = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
            asks_sorted = sorted(asks, key=lambda x: float(x.get("price", 0)))

            v_bid = sum(float(b.get("size", 0)) for b in bids_sorted[:cls.LEVELS])
            v_ask = sum(float(a.get("size", 0)) for a in asks_sorted[:cls.LEVELS])
            total = v_bid + v_ask

            if total == 0:
                # ── Fallback : carnet AMM vide → OBI synthetique neutre ──
                if best_bid > 0 and best_ask > 0 and best_ask > best_bid:
                    logger.info(
                        "[OBI] Carnet CLOB vide -> fallback AMM (Gamma bid=%.4f ask=%.4f)",
                        best_bid, best_ask,
                    )
                    return OBIResult(obi=0.0, v_bid=0.0, v_ask=0.0, regime="neutral")
                return None

            obi = (v_bid - v_ask) / total

            if obi > OBI_BULLISH_THRESHOLD:
                regime = "bullish"
            elif obi < OBI_BEARISH_THRESHOLD:
                regime = "bearish"
            else:
                regime = "neutral"

            return OBIResult(obi=obi, v_bid=v_bid, v_ask=v_ask, regime=regime)

        except Exception as e:
            logger.info("[OBI] Erreur calcul: %s", e)
            return None


# ─── Interface de base ────────────────────────────────────────────────────────

class BaseStrategy(ABC):
    def __init__(self, client: PolymarketClient):
        self.client = client

    @abstractmethod
    def analyze(self, balance: float = 0.0) -> list[Signal]:
        ...


# ─── Strategie OBI Market Making — Set B (Balanced) ─────────────────────────

class OBIMarketMakingStrategy(BaseStrategy):
    """
    Market making avec skewing proportionnel base sur l'OBI.

    Set B (Balanced) — changements vs version initiale :
      - Skew proportionnel continu : skew_ticks = round(obi * 3)
        Au lieu de 3 branches rigides (neutre/bull/bear) avec 2 ticks fixes.
        OBI=0.10 → 0 tick, OBI=0.35 → 1 tick, OBI=0.70 → 2 ticks, OBI=0.95 → 3 ticks.
      - Maturity-aware sizing : taille /2 si days_to_expiry < 3 jours
      - Expo max 8% du solde par marche (5% avant)
      - Inv skew one-sided des 60% (70% avant)
      - Sizing 3% du solde par ordre (2% avant)
    """

    def __init__(self, client: PolymarketClient, db=None,
                 max_order_size_usdc: float = 5.0,
                 max_markets: int = 5,
                 paper_trading: bool = True,
                 max_exposure_pct: float = 0.20,
                 stop_loss_pct: float = 0.25):
        super().__init__(client)
        self.db = db
        self.max_order_size_usdc = max_order_size_usdc
        self.max_markets = max_markets
        self.paper_trading = paper_trading
        self.max_exposure_pct = max_exposure_pct   # configurable via BOT_MAX_EXPOSURE_PCT
        # Stop-loss par position : si la perte latente >= seuil, SELL market forcé.
        # 0.0 = désactivé.
        self.stop_loss_pct = stop_loss_pct
        # HOTFIX 2026-02-22 + 2026 TOP BOT — AI Edge params (stockrés dans l'instance, pas via client._config)
        self.ai_enabled = AI_ENABLED
        self.ai_edge_threshold = AI_EDGE_THRESHOLD
        self.ai_weight = AI_WEIGHT
        self.ai_cooldown_s = AI_COOLDOWN_SECONDS
        self._ai_probs: dict[str, tuple[float, float]] = {}  # token_id → (prob, timestamp)
        self._universe = MarketUniverse()
        self._obi_calc = OBICalculator()
        # Suivi prix precedents pour news-breaker {token_id: (price, timestamp)}
        self._price_history: dict[str, tuple[float, float]] = {}
        # Cooldown re-cotation : evite de re-coter le meme token trop rapidement
        # {token_id: last_quote_timestamp}
        self._last_quote_ts: dict[str, float] = {}
        self._quote_cooldown: float = 16.0  # secondes entre deux cotations du meme token (2 cycles)
        # EMA Volatility tracking
        self._volatility_ema: dict[str, float] = {}
        self._alpha_vol = 0.1
        # Tweak 1 : dernier mid par token pour cancel conditionnel
        # {token_id: last_quoted_mid}
        self._last_quoted_mid: dict[str, float] = {}

        # 2026 V6 SCALING : Load configuration
        self._config = load_config().bot
        self.as_skew_calc = AvellanedaStoikovSkew(risk_aversion=self._config.as_risk_aversion) if self._config.as_enabled else None
        self.copy_trader = CopyTrader(top_n=self._config.copy_top_n) if self._config.copy_trading_enabled else None

    # 2026 V8.0 OVERRIDE CONSTANTES DB — full preset mapping
    AGGRESSIVITY_PRESETS = {
        "MM Conservateur": {"order_size_pct": 0.008, "max_net_exposure_pct": 0.12, "inventory_skew_threshold": 0.25, "sizing_mult": 0.40, "max_order_usd": 6},
        "MM Balanced":     {"order_size_pct": 0.018, "max_net_exposure_pct": 0.22, "inventory_skew_threshold": 0.35, "sizing_mult": 0.50, "max_order_usd": 10},
        "MM Aggressif":    {"order_size_pct": 0.035, "max_net_exposure_pct": 0.35, "inventory_skew_threshold": 0.48, "sizing_mult": 1.45, "max_order_usd": 22},
        "MM Très Agressif":{"order_size_pct": 0.050, "max_net_exposure_pct": 0.50, "inventory_skew_threshold": 0.60, "sizing_mult": 2.00, "max_order_usd": 30},
    }

    def _apply_live_aggressivity(self):
        """Lit le niveau d'agressivité depuis la DB et applique TOUS les params correspondants."""
        if not self.db:
            return
            
        strategy_mode = self.db.get_config_str("strategy_mode", "MM Balanced")
        preset = self.AGGRESSIVITY_PRESETS.get(strategy_mode)
        
        # Par défaut, on désactive info_edge_only car il a pu être forcé manuellement (fallback)
        if strategy_mode == "Info Edge Only":
            self.db.set_config("info_edge_only", "true")
            # V10.3 FIX: initialiser les attributs pour éviter AttributeError sur le log suivant
            if not hasattr(self, "order_size_pct"):
                self.order_size_pct = ORDER_SIZE_PCT
            if not hasattr(self, "max_exposure_pct"):
                self.max_exposure_pct = 0.12
            if not hasattr(self, "inv_skew_threshold"):
                self.inv_skew_threshold = INVENTORY_SKEW_THRESHOLD
            if not hasattr(self, "sizing_mult"):
                self.sizing_mult = 1.0
        else:
            self.db.set_config("info_edge_only", "false")
            if preset:
                self.order_size_pct = preset["order_size_pct"]
                self.max_exposure_pct = preset["max_net_exposure_pct"]
                self.inv_skew_threshold = preset["inventory_skew_threshold"]
                self.sizing_mult = preset["sizing_mult"]
                self.max_order_size_usdc = preset["max_order_usd"]
                self.db.set_config_dict(preset)
            elif strategy_mode == "Custom":
                self.order_size_pct = self.db.get_config("order_size_pct", ORDER_SIZE_PCT)
                self.max_exposure_pct = self.db.get_config("max_net_exposure_pct", 0.20)
                self.inv_skew_threshold = self.db.get_config("inventory_skew_threshold", INVENTORY_SKEW_THRESHOLD)
                self.sizing_mult = self.db.get_config("sizing_mult", 1.0)
                self.max_order_size_usdc = self.db.get_config("max_order_usd", 15.0)
            else:
                # Fallback to module constants
                self.order_size_pct = ORDER_SIZE_PCT
                self.inv_skew_threshold = INVENTORY_SKEW_THRESHOLD
                self.sizing_mult = 1.0
        # 2026 V7.3.4/V7.3.8 — log on change
        if not hasattr(self, '_last_agg_level'):
            self._last_agg_level = ""
        if strategy_mode != self._last_agg_level:
            logger.info("[LIVE CONFIG] level=%s | order_size_pct=%.3f | max_expo=%.2f | skew=%.2f | sizing=%.2f | max_order=%.0f",
                        strategy_mode, self.order_size_pct, self.max_exposure_pct, self.inv_skew_threshold, self.sizing_mult, self.max_order_size_usdc)
            self._last_agg_level = strategy_mode

    # 2026 V7.3.4 — alias public pour trader.py
    def reload_sizing(self):
        """Recharge les paramètres de sizing depuis la DB (appelé par trader à chaque cycle)."""
        self._apply_live_aggressivity()

    def analyze(self, balance: float = 0.0) -> list[Signal]:
        self._apply_live_aggressivity()

        # V8.2 GARDE-FOU: si mode Info Edge Only, OBI ne génère AUCUN signal
        if self.db:
            strategy_mode = self.db.get_config_str("strategy_mode", "MM Balanced")
            if strategy_mode == "Info Edge Only":
                logger.info("[OBI] BLOQUÉ — mode Info Edge Only actif, aucun signal OBI")
                return []

        signals: list[Signal] = []

        # Taille par ordre : order_size_pct du solde, min 1 USDC, plafond max_order_size_usdc
        # 2026 V7.3.8 — uses instance var from DB instead of hardcoded ORDER_SIZE_PCT
        applied_order_size_pct = getattr(self, "order_size_pct", ORDER_SIZE_PCT)
        applied_sizing_mult = getattr(self, "sizing_mult", 1.0)
        order_size_usdc = max(1.0, min(balance * applied_order_size_pct * applied_sizing_mult, self.max_order_size_usdc))
        logger.info(
            "[OBI] Sizing: solde=%.2f USDC -> order_size=%.2f USDC (%.1f%% x %.2f, plafond=%.2f)",
            balance, order_size_usdc, applied_order_size_pct * 100, applied_sizing_mult, self.max_order_size_usdc,
        )

        # 2026 V7.8 SAFE MODE
        safe_mode = getattr(self.db, "get_config", lambda k, d: "false")("safe_mode", "false") == "true" if self.db else False
        applied_max_markets = 8 if safe_mode else self.max_markets

        markets = self._universe.get_eligible_markets()
        if not markets:
            logger.info("[OBI] Aucun marche eligible.")
            return signals

        traded = 0
        for market in markets:
            if traded >= applied_max_markets:
                break

            # ── Verification cooldown news-breaker ──
            if self.db and self.db.is_in_cooldown(market.yes_token_id):
                logger.info("[OBI] '%s' en cooldown, skip.", market.question[:40])
                continue

            # ── Cooldown re-cotation (30s) : evite le sur-trading ──
            last_quote = self._last_quote_ts.get(market.yes_token_id, 0.0)
            if (time.time() - last_quote) < self._quote_cooldown:
                logger.debug("[OBI] '%s' re-cotation trop rapide, skip.", market.question[:40])
                continue

            # ── V7.8 Safe Mode Filters ──
            if safe_mode:
                if market.days_to_expiry < 3.0:
                    logger.debug("[SAFE MODE] Ignoré '%s': maturité %.1fj < 3", market.question[:20], market.days_to_expiry)
                    continue
                if (market.spread / market.mid_price) > 0.04:
                    logger.debug("[SAFE MODE] Ignoré '%s': spread > 4%%", market.question[:20])
                    continue

            # ── Skip tokens inactifs (tous mécanismes mid ont échoué) ──────────
            # Évite 1 req get_order_book inutile par cycle pour les marchés
            # totalement sans liquidité (pas encore résolus, juste aucun trade).
            # Marqué par _refresh_inventory_mids() si le token est en inventaire.
            _inactive = getattr(self.client, "_inactive_tokens", set())
            if market.yes_token_id in _inactive:
                logger.debug(
                    "[OBI] '%s' marqué inactif (mid indisponible) → skip OBI.",
                    market.question[:40],
                )
                continue

            # ── Carnet d'ordres YES (avec timeout 12s pour éviter blocage TCP) ──
            try:
                ob = self._get_order_book_timeout(market.yes_token_id, timeout=12.0)
            except Exception as e:
                logger.info("[OBI] Impossible de recuperer le carnet pour %s: %s",
                            market.yes_token_id[:16], e)
                continue

            if ob is None:
                logger.info("[OBI] get_order_book a retourne None pour %s",
                            market.yes_token_id[:16])
                continue

            # Log du nombre de niveaux (OrderBookSummary ou dict)
            _bids_raw = (ob.get("bids") if isinstance(ob, dict) else getattr(ob, "bids", None)) or []
            _asks_raw = (ob.get("asks") if isinstance(ob, dict) else getattr(ob, "asks", None)) or []
            logger.info(
                "[OBI] Carnet '%s': %d bids, %d asks (L%d)",
                market.question[:40], len(_bids_raw), len(_asks_raw), OBICalculator.LEVELS,
            )

            # ── OBI (compute accepte dict ou OrderBookSummary, avec fallback Gamma) ──
            obi_result = OBICalculator.compute(
                ob,
                best_bid=market.best_bid,
                best_ask=market.best_ask,
            )
            if obi_result is None:
                logger.info("[OBI] OBI non calculable pour '%s' (carnet vide sans fallback Gamma)",
                            market.question[:40])
                continue

            # V7.8 Safe Mode OBI filter
            if safe_mode and abs(obi_result.obi) < 0.18:
                logger.debug("[SAFE MODE] Ignoré '%s': OBI %.2f < 0.18", market.question[:20], obi_result.obi)
                continue

            # ── News-Breaker : detection mouvement rapide (Set B: 7% au lieu de 10%) ──
            mid = market.mid_price
            if self._is_news_event(market.yes_token_id, mid):
                logger.warning(
                    "[OBI] NEWS-BREAKER: mid-price de '%s' a bouge > %.0f%% en < %.0fs -> cooldown %dmin",
                    market.question[:40],
                    NEWS_BREAKER_THRESHOLD * 100,
                    NEWS_BREAKER_WINDOW,
                    NEWS_BREAKER_COOLDOWN // 60,
                )
                if self.db:
                    self.db.set_cooldown(market.yes_token_id, NEWS_BREAKER_COOLDOWN, "news-breaker")
                    self.db.add_log("WARNING", "strategy",
                                   f"News-breaker: {market.question[:60]}")
                try:
                    self.client.cancel_all_orders()
                except Exception:
                    pass
                continue

            self._last_quote_ts[market.yes_token_id] = time.time()

            # Mise à jour de la volatilité temps réel
            current_vol = self._volatility_ema.get(market.yes_token_id, 0.0)
            if market.yes_token_id in self._price_history:
                prev_price = self._price_history[market.yes_token_id][0]
                ret = abs(mid - prev_price)
                current_vol = current_vol * (1 - self._alpha_vol) + ret * self._alpha_vol
            self._volatility_ema[market.yes_token_id] = current_vol
            self._update_price_history(market.yes_token_id, mid)

            # 2026 FINAL POLISH — AI Edge, force l'appel et log clair
            ai_prob = -1.0
            if self.ai_enabled:
                now_ts = time.time()
                cached = self._ai_probs.get(market.yes_token_id)
                if cached is None or (now_ts - cached[1]) > self.ai_cooldown_s:
                    try:
                        from bot.ai_edge import get_ai_fair_value
                        ai_val = get_ai_fair_value(market.question)
                        if ai_val is not None:
                            self._ai_probs[market.yes_token_id] = (ai_val, now_ts)
                            ai_prob = ai_val
                    except Exception as e_ai:
                        logger.debug("[AI EDGE] Erreur fetch API: %s", e_ai)
                elif cached:
                    ai_prob = cached[0]
                self._ai_last_market_title = market.question  # stocké pour le log
                
                # 2026 V6.5 ULTRA-CHIRURGICAL
                market_title = market.question[:40]
                delta = ai_prob - mid if ai_prob >= 0 else 0.0
                skew = delta * 4.0 * self.ai_weight
                logger.info(f"[AI EDGE] {market_title} | AI={ai_prob:.3f} mid={mid:.3f} delta={delta:+.1%} skew={skew:+.2f}")

            # ── Lecture de la position actuelle ──
            qty_held = self.db.get_position(market.yes_token_id) if self.db else 0.0

            # 2026 V7.0 SCALING: Copy Trading
            copy_direction = 0.0
            copy_sizing = 1.0
            if self._config.copy_trading_enabled and self.copy_trader:
                cinfo = self.copy_trader.get_market_direction(market.yes_token_id)
                if isinstance(cinfo, dict):
                    conf = cinfo.get("confidence", 0.0)
                    if conf > 0.7:
                        copy_direction = cinfo.get("direction", 0.0)
                        sizing_pct = cinfo.get("sizing", 0.15)
                        copy_sizing = 1.0 + sizing_pct
                        logger.info("[COPY TRADE] Goldsky top-10 validé: conf=%.2f dir=%+.1f sizing=+%.0f%% sur %s",
                                    conf, copy_direction, sizing_pct * 100, market.question[:30])

            # ── Calcul bid/ask avec skewing proportionnel (Set B) ──
            bid_price, ask_price = self._compute_quotes(
                mid=mid,
                spread=market.spread,
                obi=obi_result,
                volatility=current_vol,
                ai_prob=ai_prob,
                qty_held=qty_held,
                days_to_expiry=market.days_to_expiry,
                copy_direction=copy_direction
            )

            # Verifications de base
            if bid_price >= ask_price:
                logger.debug("[OBI] bid >= ask pour '%s', skip.", market.question[:40])
                continue
            if not (0.01 <= bid_price <= 0.99) or not (0.01 <= ask_price <= 0.99):
                continue

            # ── Tweak 3 : Maturity-aware sizing ──
            # Marches < 3 jours de maturite = plus volatils → taille /2
            maturity_factor = MATURITY_SHORT_FACTOR if market.days_to_expiry < MATURITY_SHORT_DAYS else 1.0
            
            # V7.8 Safe Mode BTC
            btc_factor = 0.5 if (safe_mode and "btc" in market.question.lower()) else 1.0

            effective_size = order_size_usdc * maturity_factor * copy_sizing * btc_factor
            if maturity_factor < 1.0 or btc_factor < 1.0:
                logger.info(
                    "[OBI] Sizing ajusté (maturité=%.1f, btc=%.1f) -> %.2f USDC",
                    maturity_factor, btc_factor, effective_size,
                )

            # Sizing base sur prix (nombre de shares pour depenser effective_size)
            # Polymarket exige un minimum de 5 shares par ordre
            POLY_MIN_SHARES = 5.0
            bid_size = max(POLY_MIN_SHARES, round(effective_size / bid_price, 2))
            ask_size = max(POLY_MIN_SHARES, round(effective_size / ask_price, 2))

            # Ratio d'inventaire : cohérent avec RiskManager (configurable via BOT_MAX_EXPOSURE_PCT)
            max_exposure = max(balance * self.max_exposure_pct, self.max_order_size_usdc)
            net_exposure_usdc = qty_held * mid
            inv_ratio = net_exposure_usdc / max_exposure if max_exposure > 0 else 0.0

            # ── Mise à jour du mid courant en DB ──
            # Permet au RiskManager de valoriser l'exposition au prix de marché réel
            # même si ce token ne génère pas de signal ce cycle (ex: filtre OBI).
            # Aussi utilisé par _compute_portfolio_value() pour le HWM/circuit breaker.
            if self.db and qty_held > 0:
                try:
                    self.db.update_position_mid(market.yes_token_id, mid)
                except Exception:
                    pass  # Non bloquant — dégradé silencieux

            # Decision : quelle(s) face(s) coter ce cycle
            #
            # PAPER TRADING : fill immediat → jamais BUY+SELL simultane.
            #   - Pas de position → BUY uniquement
            #   - Position existante → SELL uniquement
            #
            # LIVE TRADING : ordres limit dans le carnet, fill asynchrone.
            #   → BUY si inv_ratio < seuil cible (Set B, ex: 0.70)
            #   → SELL si position existante
            #   → Inventory skewing via RiskManager (ratio >= seuil → ask only)
            has_position = qty_held > 0.01
            skew_thresh = getattr(self, "inv_skew_threshold", INVENTORY_SKEW_THRESHOLD)
            if self.paper_trading:
                emit_sell = has_position
                emit_buy  = (not has_position) and (inv_ratio < skew_thresh)
            else:
                emit_buy  = inv_ratio < skew_thresh
                emit_sell = has_position

            logger.info(
                "[OBI] '%s' | mid=%.4f | OBI=%.3f (%s) | bid=%.4f ask=%.4f "
                "| qty=%.2f inv=%.2f mat=%.1fj -> buy=%s sell=%s",
                market.question[:45], mid, obi_result.obi, obi_result.regime,
                bid_price, ask_price, qty_held, inv_ratio, market.days_to_expiry,
                emit_buy, emit_sell,
            )

            # ── Enregistrer le mid pour cancel conditionnel (Tweak 1) ──
            self._last_quoted_mid[market.yes_token_id] = mid

            if emit_buy:
                signals.append(Signal(
                    token_id=market.yes_token_id,
                    market_id=market.market_id,
                    market_question=market.question,
                    side="buy",
                    order_type="limit",
                    price=bid_price,
                    size=bid_size,
                    confidence=min(abs(obi_result.obi) + 0.5, 1.0),
                    reason=f"OBI={obi_result.obi:.3f} regime={obi_result.regime}",
                    obi_value=obi_result.obi,
                    obi_regime=obi_result.regime,
                    spread_at_signal=market.spread,
                    volume_24h=market.volume_24h,
                    mid_price=mid,
                ))
            if emit_sell:
                # ── Stop-loss : SELL market si perte latente >= stop_loss_pct ──
                # Récupère avg_price depuis la DB pour comparer au mid actuel.
                # Si la position a perdu trop de valeur, on coupe immédiatement
                # avec un SELL market (FOK) au lieu d'attendre un SELL limit.
                sell_type = "limit"
                sell_price = ask_price
                sell_reason = f"OBI={obi_result.obi:.3f} regime={obi_result.regime}"

                if self.stop_loss_pct > 0 and self.db:
                    try:
                        pos_row = next(
                            (p for p in self.db.get_all_positions()
                             if p["token_id"] == market.yes_token_id),
                            None,
                        )
                        avg_p = float(pos_row["avg_price"]) if pos_row and pos_row.get("avg_price") else None
                        if avg_p and avg_p > 0:
                            loss_pct = (avg_p - mid) / avg_p
                            if loss_pct >= self.stop_loss_pct:
                                sell_type = "market"
                                sell_price = None   # Market order : pas de prix limite
                                sell_reason = (
                                    f"STOP-LOSS: perte latente {loss_pct*100:.1f}% "
                                    f"(avg={avg_p:.4f} mid={mid:.4f})"
                                )
                                logger.warning(
                                    "[STOP-LOSS] '%s' perte latente %.1f%% "
                                    "(avg=%.4f mid=%.4f) → SELL market forcé",
                                    market.question[:40], loss_pct * 100, avg_p, mid,
                                )
                                if self.db:
                                    self.db.add_log(
                                        "WARNING", "strategy",
                                        f"STOP-LOSS: {market.question[:60]} "
                                        f"perte={loss_pct*100:.1f}% avg={avg_p:.4f} mid={mid:.4f}",
                                    )
                    except Exception as sl_err:
                        logger.debug("[STOP-LOSS] Erreur calcul perte latente: %s", sl_err)

                signals.append(Signal(
                    token_id=market.yes_token_id,
                    market_id=market.market_id,
                    market_question=market.question,
                    side="sell",
                    order_type=sell_type,
                    price=sell_price,
                    size=min(ask_size, qty_held),  # ne jamais vendre plus que detenu
                    confidence=1.0 if sell_type == "market" else min(abs(obi_result.obi) + 0.5, 1.0),
                    reason=sell_reason,
                    obi_value=obi_result.obi,
                    obi_regime=obi_result.regime,
                    spread_at_signal=market.spread,
                    volume_24h=market.volume_24h,
                    mid_price=mid,
                ))
            traded += 1

        logger.info("[OBI] Analyse terminee: %d marches, %d signaux generes.",
                    traded, len(signals))
        return signals

    def _compute_quotes(self, mid: float, spread: float,
                        obi: OBIResult, volatility: float = 0.0,
                        ai_prob: float = -1.0,
                        qty_held: float = 0.0,
                        days_to_expiry: float = 14.0,
                        copy_direction: float = 0.0) -> tuple[float, float]:
        """
        Calcule bid et ask avec skew ASYMETRIQUE base sur l'OBI.
        Paramétrage dynamique du spread en fonction de la volatilité en temps réel (EMA).
        """
        # Élargissement dynamique du spread si volatilité élevée
        # multiplier = 1.0 (calme) à 2.0+ (très volatil, ex: 1 cent par cycle)
        vol_multiplier = 1.0 + (volatility * 100)
        dynamic_spread = spread * vol_multiplier
        half = dynamic_spread / 2.0

        # Skew proportionnel asymetrique OBI
        obi_skew = round(abs(obi.obi) * OBI_SKEW_FACTOR) * (1 if obi.obi >= 0 else -1)

        # 2026 V6 SCALING : Avellaneda-Stoikov
        as_skew = 0
        if getattr(self, "as_skew_calc", None) is not None:
            # Normaliser q en [-1, +1] : expo_usdc / max_expo_usdc
            max_expo = (self._config.max_exposure_pct * 100) or 20.0  # USDC max
            q_norm = (qty_held * mid - self._config.as_inventory_target) / max_expo if max_expo > 0 else 0.0
            q_norm = max(-1.0, min(1.0, q_norm))
            as_skew = self.as_skew_calc.calculate_skew_ticks(
                q=q_norm,
                sigma=volatility,
                t_days=days_to_expiry,
            )
            skew_final = round(obi_skew * 0.55 + as_skew * 0.45)
        else:
            skew_final = obi_skew

        # 2026 V6
        ai_skew_ticks = 0
        if ai_prob >= 0.0 and abs(ai_prob - mid) > self.ai_edge_threshold:
            delta = ai_prob - mid
            directional_skew = delta * 4.0 * self.ai_weight
            ai_skew_ticks = round(directional_skew / TICK_SIZE)
                        
        # 2026 V6 SCALING : Copy Trading
        copy_skew_ticks = 0
        if copy_direction != 0.0:
            copy_skew_ticks = round((copy_direction * 0.12) / TICK_SIZE)

        # OBI positif → ask monte, bid descend (protection BUY, exploitation ASK)
        # OBI negatif → bid monte, ask descend (opportunite BUY, protection SELL)
        bid = mid - half - skew_final * TICK_SIZE + ai_skew_ticks * TICK_SIZE + copy_skew_ticks * TICK_SIZE
        ask = mid + half + skew_final * TICK_SIZE + ai_skew_ticks * TICK_SIZE + copy_skew_ticks * TICK_SIZE

        # Arrondi au tick
        bid = round(round(bid / TICK_SIZE) * TICK_SIZE, 4)
        ask = round(round(ask / TICK_SIZE) * TICK_SIZE, 4)

        # Garantir l'ecart minimal (au moins 1 tick)
        if ask - bid < TICK_SIZE:
            ask = bid + TICK_SIZE

        return bid, ask

    def _is_news_event(self, token_id: str, current_mid: float) -> bool:
        """
        Retourne True si le mid-price a bouge de > NEWS_BREAKER_THRESHOLD
        en moins de NEWS_BREAKER_WINDOW secondes.
        Set B : 7% en 5s (ex: 10% en 5s).
        """
        now = time.time()
        if token_id in self._price_history:
            prev_price, prev_ts = self._price_history[token_id]
            if (now - prev_ts) < NEWS_BREAKER_WINDOW:
                if prev_price > 0 and abs(current_mid - prev_price) / prev_price > NEWS_BREAKER_THRESHOLD:
                    return True
        return False

    def _update_price_history(self, token_id: str, mid: float):
        """Enregistre le prix actuel pour la detection news-breaker."""
        self._price_history[token_id] = (mid, time.time())

    def get_last_quoted_mid(self, token_id: str) -> Optional[float]:
        """Retourne le dernier mid cote pour ce token (pour cancel conditionnel)."""
        return self._last_quoted_mid.get(token_id)

    def _get_order_book_timeout(self, token_id: str, timeout: float = 12.0):
        """
        Wrapper get_order_book avec timeout strict via Thread daemon.
        Évite que l'appel CLOB ne bloque le cycle entier si TCP hang.
        """
        import threading
        result = []
        exc_holder = []

        def _run():
            try:
                result.append(self.client.get_order_book(token_id))
            except Exception as e:
                exc_holder.append(e)

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=timeout)

        if t.is_alive():
            logger.warning("[OBI] get_order_book timeout (%.0fs) pour %s → skip marche",
                           timeout, token_id[:16])
            raise RuntimeError(f"get_order_book timeout ({timeout}s): {token_id[:16]}")

        if exc_holder:
            raise exc_holder[0]
        return result[0] if result else None

    def get_eligible_markets(self) -> list[EligibleMarket]:
        """Retourne les marches eligibles en cache (sans re-appeler Gamma)."""
        return self._universe._cache if self._universe._cache else []

# ─── V10.0 BTC/ETH PROFESSIONAL INFO EDGE ──────────────────────────────────────

class InfoEdgeStrategy(BaseStrategy):
    """
    Module "Info Edge" dédié BTC & ETH — V10.0 Professional.
    - Vrai pricing log-normal : P_true = N(d1) avec vol implicite
    - Edge Score = (P_true - P_poly) * 100
    - Décision dynamique : Edge > +20% → BUY YES | Edge < -20% → BUY NO
    - BinanceWSClient pour prix spot/perp live + funding rate
    - Volume Polymarket réel (volume_24h proxy)
    - Kelly-inspired tiered sizing : 1.0x / 1.8x / 2.8x
    - Maturity 5-40 min | Max 8% par trade | Expo 25% max
    """

    # V10.3 paramètres optimisés finaux pour 100$ capital
    ORDER_SIZE_PCT = 0.018
    MAX_EXPO_PCT   = 0.25
    MAX_ORDER_USDC = 12.0
    SIZING_MULT    = 1.0
    MIN_EDGE_SCORE = 4.5     # V18 / V16.0 — lowered from 12.5% to 4.5% for institutional realism
    MAX_TRADE_PCT  = 0.08
    MIN_MINUTES    = 0.0
    MAX_MINUTES    = 90.0    # sweet spot optimal
    MIN_VOLUME_5M  = 100     # V17.2: Relaxed volume for sprints
    IMPLIED_VOL    = 0.80

    def __init__(self, client: PolymarketClient, db=None, max_markets: int = 8,
                 max_order_size_usdc: float = 12.0, binance_ws=None):
        super().__init__(client)
        self.db = db
        self.max_markets = max_markets
        self.max_order_size_usdc = max_order_size_usdc
        self._universe = MarketUniverse()
        self._last_quote_ts = {}
        self._quote_cooldown = 16.0
        self.binance_ws = binance_ws  # BinanceWSClient instance
        # V19: Telemetry buffer to decouple DB writes from rapid-fire loop
        import threading
        self._telemetry_buffer: dict[str, any] = {}
        self._telemetry_lock = threading.Lock()

        # ── Upgrade 2 : chargement du modèle XGBoost sprint ─────────────────
        self._sprint_model        = None   # CalibratedClassifierCV ou None
        self._sprint_feature_cols = []
        self._load_sprint_model()

        # ── V25: REST CLOB poll pour p_poly frais sur sprints frais ──────────
        # WS book vide pendant ~30-60s après l'ouverture d'un sprint → stale 0.5.
        # Un thread daemon poll GET /book?token_id=X toutes les 5s et met à jour
        # ce cache. _calculate_edge_score l'utilise en priorité 2 (après WS).
        self._rest_clob_mid_cache: dict   = {}   # token_id → (mid, timestamp)
        self._rest_clob_poll_tokens: set  = set()
        self._rest_clob_poll_expiry: dict = {}   # token_id → end_date_ts  (V28)
        self._rest_clob_last_poll:   dict = {}   # token_id → last poll ts  (V28)
        self._rest_clob_running:    bool  = False
        self._start_rest_clob_poller()

    def _load_sprint_model(self) -> None:
        """Charge le modèle XGBoost depuis bot/models/sprint_xgb.pkl (si présent).

        Silencieux si le fichier n'existe pas — le bot utilise alors le modèle
        linéaire de secours (_compute_p_true_updown legacy).
        """
        import os, pickle
        model_path = os.path.join(os.path.dirname(__file__), "models", "sprint_xgb.pkl")
        if not os.path.exists(model_path):
            logger.info("[XGB] Modèle sprint absent (%s) — fallback linéaire actif.", model_path)
            return
        try:
            with open(model_path, "rb") as f:
                payload = pickle.load(f)
            self._sprint_model        = payload["model"]
            self._sprint_feature_cols = payload["features"]
            logger.info("[XGB] Modèle sprint chargé: %d features — %s",
                        len(self._sprint_feature_cols), self._sprint_feature_cols)
        except Exception as e:
            logger.warning("[XGB] Échec chargement modèle sprint: %s — fallback linéaire.", e)

    def _start_rest_clob_poller(self) -> None:
        """V28 (ex-V25): Thread daemon — poll REST CLOB avec intervalle adaptatif.

        Intervalle par token :
          - t < 120 s avant expiry → poll toutes les 1 s   (fenêtre critique)
          - t ≥ 120 s              → poll toutes les 5 s   (économie réseau)

        Le tick interne est 0.5 s pour réagir rapidement aux tokens qui passent
        en zone critique sans sur-solliciter le CLOB quand t est grand.
        Endpoint : GET https://clob.polymarket.com/book?token_id=<id>
        """
        import threading as _threading
        self._rest_clob_running = True

        def _loop():
            while self._rest_clob_running:
                try:
                    now = time.time()
                    tokens = list(self._rest_clob_poll_tokens)
                    for token_id in tokens:
                        # Déterminer l'intervalle souhaité selon le temps restant
                        exp_ts = self._rest_clob_poll_expiry.get(token_id, 0.0)
                        t_left = (exp_ts - now) if exp_ts > 0 else 9999.0

                        # Purge tokens expirés depuis > 30s (évite 404 en boucle)
                        if exp_ts > 0 and t_left < -30.0:
                            self._rest_clob_poll_tokens.discard(token_id)
                            self._rest_clob_poll_expiry.pop(token_id, None)
                            self._rest_clob_last_poll.pop(token_id, None)
                            self._rest_clob_mid_cache.pop(token_id, None)
                            continue

                        desired_interval = 1.0 if t_left < 120.0 else 5.0
                        last = self._rest_clob_last_poll.get(token_id, 0.0)
                        if now - last >= desired_interval:
                            self._poll_rest_clob_one(token_id)
                            self._rest_clob_last_poll[token_id] = now
                except Exception:
                    pass
                time.sleep(0.5)   # tick fin-grain — ne poll que si l'intervalle est écoulé

        t = _threading.Thread(target=_loop, daemon=True, name="RestClobPoller")
        t.start()
        logger.debug("[V28 RestPoller] Démarré — poll adaptatif 1s(t<120s)/5s par token")

    def _poll_rest_clob_one(self, token_id: str) -> None:
        """Fetche le mid REST CLOB pour un token et met à jour le cache."""
        try:
            url = f"https://clob.polymarket.com/book?token_id={token_id}"
            resp = requests.get(url, timeout=3.0)
            if resp.status_code == 404:
                logger.debug("[RestPoller] 404 pour token %s (marché expiré, sera purgé)",
                             token_id[:16])
                return
            if resp.status_code != 200:
                logger.warning("[RestPoller] HTTP %d pour token %s — CLOB inaccessible?",
                               resp.status_code, token_id[:16])
                return
            data = resp.json()
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            if not bids or not asks:
                logger.debug("[RestPoller] Book vide pour %s (bids=%d asks=%d)",
                             token_id[:16], len(bids), len(asks))
                return
            best_bid = float(bids[0]["price"])
            best_ask = float(asks[0]["price"])
            mid = (best_bid + best_ask) / 2.0
            if 0.01 <= mid <= 0.99:
                self._rest_clob_mid_cache[token_id] = (mid, time.time())
                logger.debug("[V25 RestPoller] %s mid=%.4f (bid=%.3f ask=%.3f)",
                             token_id[:16], mid, best_bid, best_ask)
            else:
                logger.debug("[RestPoller] mid hors range [%.3f] pour %s", mid, token_id[:16])
        except requests.exceptions.Timeout:
            logger.warning("[RestPoller] Timeout 3s pour token %s", token_id[:16])
        except Exception as e:
            logger.warning("[RestPoller] Erreur inattendue pour %s: %s", token_id[:16], e)

    def _check_market_streaks(self) -> bool:
        """V17.0 Anti-Streak: Check if the last 3 BTC sprint trades resolved as UP."""
        if not self.db: return False
        try:
            trades = self.db.get_closed_trades(limit=25)
            btc_sprints = [t for t in trades if "BTC" in t.get("question", "").upper() and "5" in t.get("question", "")]
            if len(btc_sprints) < 3:
                return False
            
            recent_3 = btc_sprints[:3]
            for t in recent_3:
                side = t.get("side", "")
                pnl = float(t.get("pnl_usdc", 0.0))
                is_up = (side == "buy" and pnl > 0) or (side == "sell" and pnl < 0)
                if not is_up:
                    return False
            return True
        except Exception as e:
            logger.debug("[Anti-Streak] Erreur: %s", e)
            return False

    def _is_btc_eth(self, q: str) -> bool:
        q_up = q.upper()
        return any(x in q_up for x in ["BITCOIN", "BTC", "ETHEREUM", "ETH"])

    def _get_asset_symbol(self, q: str) -> str:
        """Retourne 'BTC' ou 'ETH' selon la question du marché."""
        q_up = q.upper()
        if "BTC" in q_up or "BITCOIN" in q_up:
            return "BTC"
        return "ETH"

    def _is_updown_market(self, question: str) -> bool:
        """Détecte les marchés directionnels 'Up or Down' (pas de strike dollar fixe).
        Ex: 'Bitcoin Up or Down - February 26, 3:45PM-3:50PM ET' → True
        """
        q_up = question.upper()
        return "UP OR DOWN" in q_up or "UPDOWN" in q_up

    def _parse_strike_from_question(self, question: str) -> float:
        """Extrait le strike dollar réel pour les marchés 'Will X be above $Y?'.
        Ex: 'Will BTC be above $97,500 at 14:00 UTC?' → 97500.0
        Retourne 0.0 si non parsable.
        """
        import re
        m = re.search(r'\$([0-9][0-9,]*(?:\.[0-9]+)?)', question)
        if m:
            try:
                return float(m.group(1).replace(',', ''))
            except (ValueError, AttributeError):
                pass
        return 0.0

    def _compute_p_true_updown(self, sym: str, funding: float,
                               time_in_cycle_min: float = 2.0) -> tuple[float, float, float]:
        """Modèle directionnel pour marchés 'Bitcoin/ETH Up or Down'.

        V31 (Upgrade 5) : vector de features dict-based → backward-compatible avec
        l'ancien modèle 6-features ET le nouveau modèle 9-features.

        Features disponibles (sous-ensemble selon self._sprint_feature_cols) :
          mom_30s        : momentum 30s
          mom_60s        : momentum 60s
          mom_300s       : momentum 300s
          vol_60s        : vol réalisée 60s annualisée
          hour_sin/cos   : heure UTC cyclique
          time_in_cycle  : minutes écoulées dans la fenêtre 5-min (V31)
          taker_imb      : CVD 60s normalisé [-1,+1] (V31, proxy taker imbalance)
          vol_ratio      : vol_60s / vol_300s (régime vol, V31)

        Returns: (p_true_up, mom_30s, obi)
        """
        import datetime
        mom = 0.0
        obi = 0.0

        if self.binance_ws:
            ws_sym = f"{sym}USDT" if len(sym) <= 3 else sym
            mom = self.binance_ws.get_30s_momentum(ws_sym)
            # Upgrade 4 — OBI depth5 anti-spoofing + CVD taker flow (70/30)
            obi_d5 = self.binance_ws.get_obi_depth5(ws_sym)
            cvd_n  = self.binance_ws.get_cvd(ws_sym, 60.0)
            obi    = 0.70 * obi_d5 + 0.30 * cvd_n

        # ── V31 : inférence XGBoost dict-based (backward-compatible) ──────────
        if self._sprint_model is not None and self.binance_ws is not None:
            try:
                import math, numpy as _np
                ws_sym = f"{sym}USDT" if len(sym) <= 3 else sym
                mom_30s  = mom
                mom_60s  = self.binance_ws.get_ns_momentum(ws_sym, 60.0)
                mom_300s = self.binance_ws.get_ns_momentum(ws_sym, 300.0)
                vol_60s  = self.binance_ws.get_ns_vol(ws_sym, 60.0)
                now_utc  = datetime.datetime.now(datetime.timezone.utc)
                hour     = now_utc.hour + now_utc.minute / 60.0
                hour_sin = math.sin(2 * math.pi * hour / 24.0)
                hour_cos = math.cos(2 * math.pi * hour / 24.0)

                # V31: nouvelles features (ignorées si non dans self._sprint_feature_cols)
                taker_imb = float(self.binance_ws.get_cvd(ws_sym, 60.0))   # proxy buy imb
                vol_300s  = self.binance_ws.get_ns_vol(ws_sym, 300.0)
                vol_ratio = max(0.10, min(5.0, vol_60s / max(vol_300s, 1e-6)))
                # V32: exposé pour regime detection Kelly dans generate_signals
                self._last_vol_ratio = vol_ratio

                # Dict complet → extraction ordonnée par self._sprint_feature_cols
                feat_dict = {
                    "mom_30s":       mom_30s,
                    "mom_60s":       mom_60s,
                    "mom_300s":      mom_300s,
                    "vol_60s":       vol_60s,
                    "hour_sin":      hour_sin,
                    "hour_cos":      hour_cos,
                    "time_in_cycle": float(time_in_cycle_min),
                    "taker_imb":     taker_imb,
                    "vol_ratio":     vol_ratio,
                }
                X = _np.array(
                    [[feat_dict[c] for c in self._sprint_feature_cols]],
                    dtype=_np.float32,
                )

                p_up = float(self._sprint_model.predict_proba(X)[0, 1])
                p_up = max(0.05, min(0.95, p_up))

                logger.debug(
                    "[XGB] p_true=%.4f | mom30=%.4f mom300=%.4f vol60=%.3f "
                    "tic=%.1f timb=%.2f vratio=%.2f | feats=%s",
                    p_up, mom_30s, mom_300s, vol_60s,
                    time_in_cycle_min, taker_imb, vol_ratio,
                    self._sprint_feature_cols,
                )
                return p_up, mom, obi

            except Exception as e:
                logger.debug("[XGB] Inférence échouée: %s — fallback linéaire", e)

        # ── Fallback : modèle linéaire (legacy) ──────────────────────────────
        mom_contrib  = min(0.35, max(-0.35, mom  * 3.0))
        obi_contrib  = min(0.20, max(-0.20, obi  * 0.20))
        fund_contrib = min(0.05, max(-0.05, funding * 500.0))
        p_up = 0.50 + mom_contrib + obi_contrib + fund_contrib
        return min(0.95, max(0.05, p_up)), mom, obi

    def _compute_p_true(self, spot_price: float, strike_proxy: float,
                        t_minutes: float, vol: float) -> float:
        """
        Calcul de probabilité vraie via distribution log-normale simplifiée.
        Pour un événement binaire "prix > strike" dans T minutes :
          d1 = [ln(S/K) + 0.5*vol^2*T] / (vol*sqrt(T))
          P_true = N(d1)
        T en années (T_min / 525600).
        """
        import math
        try:
            T = max(t_minutes, 1.0) / 525_600.0  # minutes → années
            if strike_proxy <= 0 or spot_price <= 0:
                return 0.5
            d1 = (math.log(spot_price / strike_proxy) + 0.5 * vol**2 * T) / (vol * math.sqrt(T))
            # Approximation de N(d1) — CDF normale standard
            return self._norm_cdf(d1)
        except Exception:
            return 0.5

    @staticmethod
    def _norm_cdf(x: float) -> float:
        """Approximation rapide de la CDF normale standard (Abramowitz & Stegun)."""
        import math
        if x >= 0:
            t = 1.0 / (1.0 + 0.2316419 * x)
            d = 0.3989422804014327  # 1/sqrt(2*pi)
            p = d * math.exp(-x * x / 2.0) * t * (
                0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274)))
            )
            return 1.0 - p
        else:
            return 1.0 - BinanceWSClient._norm_cdf_static(-x) if False else 1.0 - InfoEdgeStrategy._norm_cdf(-x)

    def _get_dynamic_vol(self, symbol: str) -> float:
        """
        V14.0: Volatilité dynamique (Ecart-type des rendements 1-sec annualisé).
        """
        import math
        history = []
        if self.binance_ws:
            with self.binance_ws._lock:
                if symbol == "BTC" and self.binance_ws.btc_history:
                    history = list(self.binance_ws.btc_history)
                elif symbol == "ETH" and self.binance_ws.eth_history:
                    history = list(self.binance_ws.eth_history)
        
        if len(history) < 10 or (history[-1][0] - history[0][0]) < 30.0:
            return 0.60
            
        returns = []
        for i in range(1, len(history)):
            p0 = history[i-1][1]
            p1 = history[i][1]
            if p0 > 0 and p1 > 0:
                returns.append(math.log(p1 / p0))
                
        if not returns:
            return 0.60
            
        mean_ret = sum(returns) / len(returns)
        var = sum((r - mean_ret)**2 for r in returns) / len(returns)
        std_dev = math.sqrt(var)
        
        # Annualisation des données d'une seconde (31_536_000 sec/an)
        ann_vol = std_dev * math.sqrt(31_536_000)
        
        # Borner pour éviter les extrêmes explosifs dans d1
        return max(0.40, min(1.50, ann_vol))

    def _calculate_edge_score(self, market: EligibleMarket) -> tuple[float, float, float, float]:
        """
        Calcule le vrai Edge Score avec données Binance live.
        Returns: (edge_pct, p_true, p_poly, vol_estimate)
        """
        sym = self._get_asset_symbol(market.question)

        # ── Upgrade 1 : p_poly depuis le CLOB WebSocket (temps réel) ────────────
        # Fallback sur market.mid_price (Gamma API) si le WS n'a pas encore de book.
        # Priorité : WS yes_token_id → WS no_token_id (inversé) → DB NO token → Gamma API stale.
        p_poly = market.mid_price
        if hasattr(self.client, 'ws_client') and self.client.ws_client.running:
            _ws_mid = self.client.ws_client.get_midpoint(market.yes_token_id)
            if _ws_mid is not None and 0.01 <= _ws_mid <= 0.99:
                p_poly = _ws_mid
                logger.debug("[CLOB_WS] p_poly live %.4f (vs Gamma %.4f) pour %s",
                             _ws_mid, market.mid_price, market.yes_token_id[:16])

        # V25: REST CLOB cache — warm-up sprints frais (WS pas encore warm, TTL 30s)
        # Priorité : WS live → REST poll 5s → DB NO fallback → Gamma stale
        if abs(p_poly - 0.5) < 1e-6:
            _rest_entry = (self._rest_clob_mid_cache.get(market.yes_token_id)
                           or self._rest_clob_mid_cache.get(
                               getattr(market, "no_token_id", "")))
            if _rest_entry:
                _rest_mid_raw, _rest_ts = _rest_entry
                # Si c'est le cache NO token, inverser
                _no_tid = getattr(market, "no_token_id", "")
                _is_no_cache = (
                    _no_tid
                    and self._rest_clob_mid_cache.get(_no_tid) is _rest_entry
                    and not self._rest_clob_mid_cache.get(market.yes_token_id)
                )
                _rest_mid = (1.0 - _rest_mid_raw) if _is_no_cache else _rest_mid_raw
                if time.time() - _rest_ts <= 30.0 and 0.01 <= _rest_mid <= 0.99:
                    p_poly = _rest_mid
                    logger.debug("[V25 P_POLY] REST cache %.4f (%s) pour %s",
                                 p_poly,
                                 "NO↔" if _is_no_cache else "YES",
                                 market.yes_token_id[:16])

        # V24b: si p_poly est encore au défaut 0.5 (stale Gamma), essayer le DB
        # current_mid du token NO pour déduite le mid YES (1 - mid_NO).
        # Cas typique : marché sprint nouveau, WS book pas encore reçu, mais position NO
        # déjà suivie par _refresh_inventory_mids() avec un mid API frais.
        if abs(p_poly - 0.5) < 1e-6 and self.db:
            _no_tid = getattr(market, "no_token_id", "")
            if _no_tid:
                try:
                    _pos = self.db.get_position_row(_no_tid)
                    if _pos and _pos.get("current_mid"):
                        _no_mid = float(_pos["current_mid"])
                        if 0.01 <= _no_mid <= 0.99:
                            p_poly = 1.0 - _no_mid
                            logger.debug(
                                "[P_POLY] DB fallback NO-token %.4f → p_poly(YES)=%.4f %s",
                                _no_mid, p_poly, _no_tid[:16],
                            )
                except Exception:
                    pass

        # Prix spot Binance live
        spot = 0.0
        funding = 0.0
        if self.binance_ws:
            spot = self.binance_ws.get_mid(sym)
            funding = self.binance_ws.get_funding(sym)

        self._last_funding = funding  # Cache for DB push
        # V33: t réel depuis end_date_ts pour le pricing BS + time_in_cycle XGBoost
        _end_ts_edge = getattr(market, "end_date_ts", 0.0)
        if _end_ts_edge > 0:
            minutes_to_expiry = max(0.0, _end_ts_edge - time.time()) / 60.0
        else:
            minutes_to_expiry = market.days_to_expiry * 1440

        # V14.0 : Volatilité Dynamique (utilisée par les deux branches)
        base_vol = self._get_dynamic_vol(sym)
        self._last_iv = base_vol  # Cache for DB push
        vol = base_vol + abs(funding) * 50.0

        if self._is_updown_market(market.question):
            # ── Branche A : marché directionnel "Up or Down" ──────────────────
            # Modèle momentum + OBI — pas de strike fixe
            # V31: time_in_cycle = minutes écoulées depuis l'ouverture de la fenêtre
            _minutes_left    = minutes_to_expiry  # déjà calculé précisément ci-dessus
            _time_in_cycle   = max(0.0, min(4.0, 5.0 - _minutes_left))
            p_true, _mom, _obi = self._compute_p_true_updown(sym, funding, _time_in_cycle)

            # V35: BS hybrid — blend XGBoost (65%) + BS(strike implicite via mom_300s) (35%)
            # Ancre le XGBoost sur la géométrie Black-Scholes réelle du sprint.
            # strike_open = niveau spot au début du cycle (estimé via mom_300s).
            # Appliqué seulement si mom_300s ≥ 0.1% (déplacement non-trivial).
            _p_xgb = p_true
            if spot > 0 and _minutes_left > 0 and self.binance_ws and self.binance_ws.is_connected:
                try:
                    _ws_sym_ud = f"{sym}USDT" if len(sym) <= 3 else sym
                    _mom_300s  = self.binance_ws.get_ns_momentum(_ws_sym_ud, 300.0)
                    if abs(_mom_300s) >= 0.001:  # ≥ 0.1% — signal non trivial
                        _strike_open = spot / (1.0 + _mom_300s)
                        _p_bs = self._compute_p_true(spot, _strike_open, _minutes_left, vol)
                        p_true = max(0.05, min(0.95, 0.65 * _p_xgb + 0.35 * _p_bs))
                        logger.debug(
                            "[EDGE UpDown] BS hybrid: mom300=%+.3f%% K=%.2f "
                            "p_xgb=%.4f p_bs=%.4f → p_hybrid=%.4f",
                            _mom_300s * 100, _strike_open, _p_xgb, _p_bs, p_true
                        )
                except Exception:
                    pass  # BS hybrid optionnel — XGBoost seul en fallback

            # V37: Momentum Decay Filter — si mom_60s contredit mom_300s,
            # le momentum est en retournement → régresser p_true vers 0.5.
            # Prévient les entrées tardives sur momentum stale : XGBoost/BS ont
            # intégré l'élan des 5 dernières minutes mais BTC a déjà inversé.
            # Actif seulement si mom_300s ≥ 0.1% ET mom_60s ≥ 0.05% ET directions opposées.
            if spot > 0 and self.binance_ws and self.binance_ws.is_connected:
                try:
                    _wd_sym  = f"{sym}USDT" if len(sym) <= 3 else sym
                    _m60_ud  = self.binance_ws.get_ns_momentum(_wd_sym, 60.0)
                    _m300_ud = self.binance_ws.get_ns_momentum(_wd_sym, 300.0)
                    if (abs(_m300_ud) >= 0.001           # momentum long non trivial
                            and abs(_m60_ud) >= 0.0005   # momentum récent mesurable (≥ 0.05%)
                            and _m60_ud * _m300_ud < 0): # directions opposées → retournement
                        _p_before = p_true
                        # Régression vers 0.5 proportionnelle au ratio |mom_60s| / |mom_300s|
                        # Plus le retournement récent est fort → plus on régresse
                        _decay_r = min(1.0, abs(_m60_ud) / max(abs(_m300_ud), 1e-9))
                        _w_decay = 0.40 * _decay_r  # régression max 40% vers 0.5
                        p_true   = max(0.05, min(0.95, p_true * (1.0 - _w_decay) + 0.5 * _w_decay))
                        logger.info(
                            "[MOM DECAY] mom300=%+.3f%% mom60=%+.3f%% → inversion détectée "
                            "p_true %.4f → %.4f (decay=%.0f%%)",
                            _m300_ud * 100, _m60_ud * 100,
                            _p_before, p_true, _w_decay * 100,
                        )
                        # V37: Persist pour calibration (non-bloquant)
                        try:
                            if hasattr(self, 'db') and self.db:
                                self.db.log_decay_event(
                                    market_id=market.id,
                                    mom_300s=_m300_ud,
                                    mom_60s=_m60_ud,
                                    decay_r=_decay_r,
                                    w_decay=_w_decay,
                                    p_before=_p_before,
                                    p_after=p_true,
                                )
                        except Exception:
                            pass
                except Exception:
                    pass  # optionnel — ne bloque pas le signal

            logger.debug(
                "[EDGE UpDown] mom=%.4f%% obi=%.3f → p_true=%.4f p_poly=%.4f",
                _mom, _obi, p_true, p_poly
            )
        else:
            # ── Branche B : marché à niveau fixe "Will X be above $Y?" ────────
            strike = self._parse_strike_from_question(market.question)
            # V14.0 Funding Rate Alpha Drift
            if spot > 0 and strike > 0 and abs(funding) > 0.0001:
                strike = strike * (1.0 - (funding * 50.0))

            if spot > 0 and strike > 0:
                p_true = self._compute_p_true(spot, strike, minutes_to_expiry, vol)
                logger.debug(
                    "[EDGE Level] S=%.2f K=%.2f T_min=%.2f vol=%.3f → p_true=%.4f p_poly=%.4f",
                    spot, strike, minutes_to_expiry, vol, p_true, p_poly
                )
                # V16.0 OBI Drift (extrêmes seulement) — Upgrade 4: depth5
                obi_drift = 0.0
                if self.binance_ws and self.binance_ws.is_connected:
                    obi_drift = self.binance_ws.get_obi_depth5("BTCUSDT")
                if obi_drift > 0.90:
                    p_true = min(0.99, p_true + (p_true - 0.5) * 0.2)
                elif obi_drift < -0.90:
                    p_true = max(0.01, p_true + (p_true - 0.5) * 0.2)
            else:
                logger.warning(
                    "[EDGE Level] Strike non parsé (spot=%.2f strike=%.2f) pour: %s",
                    spot, strike, market.question[:60]
                )
                p_true = 0.5

        edge_pct = (p_true - p_poly) * 100.0

        # Volume estimé 5-min proxy via volume_24h Polymarket
        vol_est = getattr(market, 'volume_24h', 0) / 288.0  # 24h / 288 = 5min

        if hasattr(self, 'db') and self.db:
            try:
                self.db.set_config("live_dynamic_iv", round(base_vol, 4))
                self.db.set_config("live_funding_rate", round(funding, 6))
                self.db.set_config("live_sprint_edge", round(edge_pct, 2))
                self.db.set_config("live_sprint_ptrue", round(p_true * 100, 2))
                self.db.set_config("live_sprint_ppoly", round(p_poly * 100, 2))
            except Exception:
                pass

        return edge_pct, p_true, p_poly, vol_est

    def _decide_side(self, market: EligibleMarket, edge_pct: float, min_edge: float) -> str:
        """Décision dynamique : Edge > +min_edge → BUY YES | Edge < -min_edge → BUY NO."""
        if edge_pct >= min_edge:
            return "buy"   # BUY YES
        elif edge_pct <= -min_edge:
            return "sell"  # BUY NO (short YES)
        return ""  # pas de trade

    def analyze(self, balance: float = 0.0, target_market_ids: list[str] = None) -> list[Signal]:
        """V13.0: Event-Driven Sniper Architecture. Allows targeted rapid-fire."""
        try:
            return self._analyze_internal(balance, target_market_ids)
        except Exception as e:
            import traceback
            logger.error("[STRATEGY] 🚨 SILENT CRASH IN analyze 🚨 : %s\n%s", e, traceback.format_exc())
            return []

    def _analyze_internal(self, balance: float = 0.0, target_market_ids: list[str] = None) -> list[Signal]:
        signals: list[Signal] = []
        if not self._universe.get_eligible_markets():
            pass # We still want to do the global update even if no markets

        markets = self._universe.get_eligible_markets()
        
        # Récupération sécurisée des données Binance
        live_spot  = 0.0
        live_mom   = 0.0
        live_obi   = 0.0
        live_cvd10 = 0.0   # F3: CVD 10s — absorption (taker burst court terme)
        live_cvd30 = 0.0   # V26: CVD 30s — sizing modifier principal
        live_cvd60 = 0.0   # F3: CVD 60s — divergence (tendance taker long terme)
        if self.binance_ws and self.binance_ws.is_connected:
            try:
                live_spot  = self.binance_ws.get_mid("BTCUSDT")
                # Upgrade 4: OBI depth5 + CVD pour le dashboard
                live_obi   = (0.70 * self.binance_ws.get_obi_depth5("BTCUSDT")
                              + 0.30 * self.binance_ws.get_cvd("BTCUSDT", 60.0))
                live_mom   = self.binance_ws.get_30s_momentum("BTCUSDT")
                live_cvd10 = self.binance_ws.get_cvd("BTCUSDT", 10.0)
                live_cvd30 = self.binance_ws.get_cvd("BTCUSDT", 30.0)
                live_cvd60 = self.binance_ws.get_cvd("BTCUSDT", 60.0)
                # V19: Write to memory buffer instead of DB
                with self._telemetry_lock:
                    self._telemetry_buffer["live_btc_spot"] = round(live_spot, 2)
                    self._telemetry_buffer["live_btc_mom30s"] = round(live_mom, 4)
                    self._telemetry_buffer["live_btc_obi"] = round(live_obi, 3)
            except Exception as e:
                logger.debug("[Radar] Erreur update Binance globale: %s", e)

        if not markets:
            # --- V11.5 : Heartbeat pour le Live Scan Feed (when no markets) ---
            if self.db:
                now = time.time()
                if not hasattr(self, '_last_heartbeat_ts'):
                    self._last_heartbeat_ts = 0.0
                if now - self._last_heartbeat_ts > 60.0:
                    spot = self.binance_ws.get_mid("BTCUSDT") if self.binance_ws else 0.0
                    self.db.add_log("INFO", "sniper_feed", f"📡 Radar Actif | BTC Spot: {spot:.2f}$ | En recherche de cible 5-Min...")
                    self._last_heartbeat_ts = now
            return signals

        portfolio = balance
        if self.db:
            try:
                portfolio = float(self.db.get_config("last_portfolio_value", balance) or balance)
            except Exception:
                pass

        daily_edge_scores: list[float] = []
        max_edge_found = 0.0
        min_spread_found = 999.0
        sprint_markets_count = 0
        m30 = live_mom
        o_val = live_obi

        traded = 0
        for market in markets:
            # V13 Rapid-Fire Targeting: Skip immediately if not in target list
            if target_market_ids is not None and market.market_id not in target_market_ids:
                continue

            if traded >= self.max_markets:
                break

            # Extraction des IDs de jetons (souvent requis par le trader)
            token_yes = market.tokens[0] if hasattr(market, 'tokens') and len(market.tokens) > 0 else market.yes_token_id
            token_no = market.tokens[1] if hasattr(market, 'tokens') and len(market.tokens) > 1 else market.no_token_id

            if not self._is_btc_eth(market.question):
                continue

            # V33: t réel depuis end_date_ts (Unix ts) — précis à la seconde.
            # Gamma API days_to_expiry se rafraîchit ~toutes les 60s → stale jusqu'à
            # 60s → fenêtres sniper manquées (ex: t=283s Gamma vs t=236s réel).
            _end_ts_mkt = getattr(market, "end_date_ts", 0.0)
            if _end_ts_mkt > 0:
                _t_real_sec   = max(0.0, _end_ts_mkt - time.time())
                minutes_to_expiry = _t_real_sec / 60.0
            else:
                minutes_to_expiry = market.days_to_expiry * 1440
            q_lower = market.question.lower()
            is_btc = ("bitcoin" in q_lower or "btc" in q_lower)

            # Sprint = BTC + expire dans moins de 5.5 minutes (et pas déjà expiré)
            is_sprint = is_btc and (0 < minutes_to_expiry <= 5.5)

            # ── Upgrade 1 : abonnement dynamique CLOB WS pour les sprints ──────
            # Les sprint markets apparaissent en cours de session via fetch_sprint_targets().
            # Ils ne sont pas dans active_markets au démarrage → subscribe maintenant.
            if is_sprint and hasattr(self.client, 'ws_client'):
                self.client.ws_client.subscribe_tokens([token_yes, token_no])

            # V25/V28: enregistrer pour REST CLOB poll (p_poly frais dès t=0)
            if is_sprint and token_yes and token_no:
                self._rest_clob_poll_tokens.add(token_yes)
                self._rest_clob_poll_tokens.add(token_no)
                # V28: mémoriser l'expiry pour l'intervalle adaptatif 1s/5s
                _exp_ts = getattr(market, "end_date_ts", 0.0)
                if _exp_ts > 0:
                    self._rest_clob_poll_expiry[token_yes] = _exp_ts
                    self._rest_clob_poll_expiry[token_no]  = _exp_ts

            # ── V15.2 Real Edge Score computed for EVERY sprint tick ────
            edge_pct, p_true, p_poly, vol_5m = 0.0, 0.0, 0.0, 0.0
            try:
                edge_pct, p_true, p_poly, vol_5m = self._calculate_edge_score(market)
                if self.db:
                    bias = float(self.db.get_config("live_ai_sentiment_bias", 1.0) or 1.0)
                    edge_pct *= bias
            except Exception as e:
                logger.warning("[V10.0 EDGE] Erreur pricing: %s — skip", e)

            if is_sprint and self.db:
                # V23: Track max_edge AVANT les filtres spread/cooldown
                # (sinon max_edge_found reste 0 car les marchés sont toujours en cooldown)
                max_edge_found = max(max_edge_found, abs(edge_pct))
                logger.debug(
                    "[SPRINT_EDGE] %s | p_poly=%.3f p_true=%.3f edge=%+.2f%%",
                    market.question[:40], p_poly, p_true, edge_pct
                )
                # V19: Buffered DB writes — tout dans un seul bloc lock (atomic)
                with self._telemetry_lock:
                    self._telemetry_buffer["live_sprint_edge"] = round(edge_pct, 2)
                    self._telemetry_buffer["live_sprint_ptrue"] = round(p_true, 3)
                    self._telemetry_buffer["live_sprint_ppoly"] = round(p_poly, 3)
                    # V15.2: IV et funding dans le même lock pour éviter la race condition
                    if hasattr(self, '_last_iv'):
                        self._telemetry_buffer["live_dynamic_iv"] = round(self._last_iv, 4)
                    if hasattr(self, '_last_funding'):
                        self._telemetry_buffer["live_funding_rate"] = round(self._last_funding, 6)

                # V20 Checklist — reflète les 4 conditions du gate V20
                # Gate réel V20 : abs(edge) >= 3.0 AND abs(mom) >= 0.005 AND sign(edge)==sign(mom)
                import json
                tmom = 0.005
                tedge = 3.0

                mom_ok    = bool(abs(m30) >= tmom)
                edge_ok   = bool(abs(edge_pct) >= tedge)
                iv_ready  = bool(getattr(self, '_last_iv', 0) > 0)
                # direction_ok : edge et mom pointent dans le même sens
                direction_ok = bool((edge_pct > 0 and m30 > 0) or (edge_pct < 0 and m30 < 0))

                # OBI reste affiché à titre informatif (non bloquant dans gate V20)
                tobi = 0.05
                obi_ok = bool(abs(o_val) >= tobi)

                # V16.0 percentages
                mom_pct       = min(150.0, (abs(m30) / tmom) * 100) if tmom > 0 else 0
                obi_pct       = min(150.0, (abs(o_val) / tobi) * 100) if tobi > 0 else 0
                edge_pct_ratio = min(150.0, (abs(edge_pct) / tedge) * 100) if tedge > 0 else 0

                checklist = {
                    "mom_ok":       mom_ok,
                    "edge_ok":      edge_ok,
                    "direction_ok": direction_ok,
                    "iv_ready":     iv_ready,
                    "obi_ok":       obi_ok,  # informatif uniquement
                }
                # V19: Buffered DB writes
                self._telemetry_buffer["live_checklist"] = json.dumps(checklist)
                self._telemetry_buffer["live_percentages"] = json.dumps({
                    "mom_pct": round(mom_pct, 1),
                    "obi_pct": round(obi_pct, 1),
                    "edge_pct": round(edge_pct_ratio, 1)
                })
                
                # V16.0 Trigger Projection Gap Tracker: Spot Required for 6.5% edge
                spot_req = 0.0
                if live_spot > 0 and getattr(self, '_last_iv', 0) > 0:
                    target_p_true = min(0.99, market.mid_price + (tedge / 100.0))
                    if m30 < 0:
                        target_p_true = max(0.01, market.mid_price - (tedge / 100.0))
                        
                    fixed_strike = live_spot * (1.0 - (market.mid_price - 0.5) * 0.1)
                    if getattr(self, '_last_funding', 0) > 0.0001:
                        fixed_strike *= (1.0 - (getattr(self, '_last_funding', 0) * 50.0))
                        
                    minutes = market.days_to_expiry * 1440
                    iv_val = getattr(self, '_last_iv', 0.60)
                    
                    low, high = live_spot * 0.8, live_spot * 1.2
                    for _ in range(12):
                        mid_s = (low + high) / 2
                        pt = self._compute_p_true(mid_s, fixed_strike, minutes, iv_val)
                        if getattr(self, '_last_ai_bias', 1.0) != 1.0:
                             # Re-apply any known AI bias (crude approx here to match edge)
                             pass 
                        if pt < target_p_true:
                            low = mid_s
                        else:
                            high = mid_s
                    spot_req = (low + high) / 2
                
                # V19: Buffered DB write
                self._telemetry_buffer["live_trigger_projection"] = round(spot_req, 2)

                # V20 Near Miss : 3/4 conditions du gate V20
                # Conditions gate V20 : Mom, Edge, Direction, IV
                v20_conditions = {
                    "Mom":       mom_ok,
                    "Edge":      edge_ok,
                    "Direction": direction_ok,
                    "IV":        iv_ready,
                }
                met_count = sum(v20_conditions.values())

                if met_count == 3:
                    failed_cond = [k for k, v in v20_conditions.items() if not v][0]
                    now = time.time()
                    if not hasattr(self, '_last_near_miss_ts'):
                        self._last_near_miss_ts = 0.0

                    if now - self._last_near_miss_ts > 10.0:
                        from datetime import datetime, timezone
                        near_miss_data = {
                            "timestamp": datetime.now(timezone.utc).strftime("%H:%M:%S"),
                            "mom": round(m30, 4),
                            "obi": round(o_val, 2),
                            "iv": round(getattr(self, '_last_iv', 0), 3),
                            "edge": round(edge_pct, 2),
                            "missing_condition": failed_cond,
                            "synergy": bool(direction_ok),
                        }
                        self.db.record_near_miss(near_miss_data)
                        self._last_near_miss_ts = now

            if is_sprint:
                sprint_markets_count += 1
                min_minutes = 0.0  # V18 EMERGENCY: Abaissé à 0 pour le paradoxe de maturité
                min_edge = 4.5     # V18 EMERGENCY: Seuil assoupli à 4.5% pour capter le momentum
                min_vol = 0        # MODIFIÉ V11.7 : Un marché neuf n'a pas de volume initial
                max_trade = 0.06
            else:
                min_minutes = self.MIN_MINUTES
                min_edge = self.MIN_EDGE_SCORE
                min_vol = self.MIN_VOLUME_5M
                max_trade = self.MAX_TRADE_PCT

            if minutes_to_expiry < min_minutes or minutes_to_expiry > self.MAX_MINUTES:
                if not is_sprint:
                    logger.debug("[V10.3] %s ignoré (%.1fmin hors [%.1f,%.1f])", market.question[:20], minutes_to_expiry, min_minutes, self.MAX_MINUTES)
                continue

            # V23: Sprint markets exemptés du spread check (déjà force-inclus dans l'univers)
            if not is_sprint and market.mid_price > 0 and (market.spread / market.mid_price) > 0.06:
                logger.debug("[V10.3] %s ignoré (spread > 6%%)", market.question[:20])
                continue

            if (time.time() - self._last_quote_ts.get(market.yes_token_id, 0.0)) < self._quote_cooldown:
                continue

            # V36: Cooldown 45s post-reconnect WS Binance.
            # Après un reconnect, btc_history et les momentum (mom_300s notamment) sont
            # partiellement stale → les premières secondes post-reconnect donnent des edges
            # fictifs très élevés (ex: E=+40% immédiatement après reconnect = artefact).
            # Bloquer les signaux sprint pendant 45s pour laisser le buffer se remplir.
            if is_sprint and self.binance_ws:
                _reconnect_age = time.time() - getattr(self.binance_ws, "_last_reconnect_ts", 0.0)
                if _reconnect_age < 45.0:
                    if not hasattr(self, '_reconnect_skip_log_ts'):
                        self._reconnect_skip_log_ts = 0.0
                    if time.time() - self._reconnect_skip_log_ts > 10.0:
                        logger.info(
                            "[WS COOLDOWN] Reconnect il y a %.0fs — signaux bloqués (%.0f/45s) "
                            "pour stabiliser mom_300s",
                            _reconnect_age, 45.0 - _reconnect_age,
                        )
                        self._reconnect_skip_log_ts = time.time()
                    continue

            # --- Trading Gate Sprint (V22) : Standard + Sniper Override ---
            if is_sprint:

                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # PARAMÈTRES V22 — Tuner uniquement ici
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                conf = {
                    # ── Standard path ──────────────────────────────────────
                    "tedge_gate":           3.0,   # [2.0–4.0]   % edge min voie standard
                    "tmom":                 0.005, # [0.003–0.01] % mom_30s min pour confirmer
                    "tmom_dir_min":         0.001, # [0.001–0.003] % sous ce seuil → direction mom = bruit
                    "standard_max_time_sec": 180,  # [120–250]   entrée standard max 3 min avant ref price

                    # ── Sniper A : haute conviction + fenêtre serrée ───────
                    "sniper_edge_a":        8.0,   # [6.0–10.0]  % edge min
                    "sniper_obi_a":         0.65,  # [0.50–0.80] OBI abs min
                    "sniper_time_a_sec":    150,   # [90–200]    secondes restantes max

                    # ── Sniper B : ultra conviction + fenêtre large ────────
                    # Capture les setups edge ≥ 12% / OBI fort bloqués par mom faible
                    "sniper_edge_b":        12.0,  # [10.0–16.0] % edge min
                    "sniper_obi_b":         0.55,  # [0.40–0.70] OBI abs min  (0.50 → 0.55)
                    "sniper_time_b_sec":    240,   # [200–330]   secondes max  (300 → 240)

                    # ── Garde basse snipers : évite les entrées trop tardives ──
                    # En dessous de 60s, le risque d'expiration avant sortie est trop élevé
                    # (latence ordre ~500-1000ms + EXPIRY_EXIT à t<12s)
                    "sniper_min_time_sec":   60,   # [30–90]     secondes restantes min

                    # ── Sizing sniper ──────────────────────────────────────
                    "sniper_sizing_mult":   0.80,  # [0.75–0.90] ×0.80 = −20% vs standard

                    # ── Anti-spam sniper ───────────────────────────────────
                    "sniper_cooldown_sec":  300,   # [180–600]   1 sniper max par market_id

                    # ── Anti-Streak V17 ────────────────────────────────────
                    # _check_market_streaks() utilise anti_streak_window (hardcodé à 3 en DB)
                    "anti_streak_penalty":  0.5,   # [0.3–0.7]  facteur × sur sizing
                    "anti_streak_window":   3,     # [2–5]      trades consécutifs BTC UP

                    # ── F3 Vol Spike Detector ───────────────────────────────
                    # Round "chaud" si vol_ratio ≥ seuil OU |mom_60s| ≥ seuil.
                    # Round "calme" (les deux sous seuil) → skip le sprint :
                    #   signal momentum = bruit de microstructure, edge non fiable.
                    "vol_spike_ratio_min":  0.85,  # [0.70–1.10] vol_ratio plancher
                    "vol_spike_mom60_min":  0.0005,# [0.0003–0.001] |mom_60s| plancher (0.05%)

                    # ── Risk Manager (référence — enforcement via RiskManager) ──
                    # Ces valeurs sont lues par risk.py ; centralisées ici pour visibilité
                    "max_drawdown_pct":     10.0,  # [5–15]     % drawdown vs HWM → kill switch
                    "max_daily_loss_usdc":  10.0,  # [5–20]     perte jour max USDC
                    "max_net_exposure_pct": 25.0,  # [15–40]    % du solde expo nette max
                    "max_order_usdc":       12.0,  # [6–20]     plafond USDC par ordre
                }

                # V35: Overrides thresholds pour marchés UP/DOWN (sans strike $)
                # Ces marchés : p_poly = 0.500 fixe (AMM-only), exécution via probe AMM t=40-52s.
                # • edge bar plus haute  : XGBoost ±20% max → nécessite conviction forte
                # • OBI relaxé           : OBI Binance moins corrélé au résultat UP/DOWN
                # • t_min abaissé 60→30s : probe AMM est instantané, pas de limit-fill delay
                _no_strike_mkt_conf = ("$" not in market.question)
                if _no_strike_mkt_conf:
                    conf["sniper_edge_a"]       = 15.0   # 8% → 15%
                    conf["sniper_obi_a"]        = 0.50   # 0.65 → 0.50
                    conf["sniper_edge_b"]       = 20.0   # 12% → 20%
                    conf["sniper_obi_b"]        = 0.40   # 0.55 → 0.40
                    conf["sniper_min_time_sec"] = 30     # 60s → 30s (probe instant)

                # ── Variables locales ─────────────────────────────────────
                abs_edge_v22  = abs(edge_pct)
                abs_mom_v22   = abs(m30)
                abs_obi_v22   = abs(o_val)
                time_left_sec = minutes_to_expiry * 60.0  # min → secondes

                # V25: plancher d'entrée — pas d'entrée avec < 25s restantes.
                # Avec < 25s : spread large, p_poly figé à 0.99/0.01, book quasi vide.
                # On ne peut pas sortir profitablement → risque asymétrique total.
                if time_left_sec < 25.0:
                    logger.debug("[V25] Entrée rejetée: t=%.0fs < 25s min", time_left_sec)
                    continue

                # ── F3 Vol Spike Detector — skip rounds calmes ───────────────
                # Hot round : vol_ratio ≥ seuil OU |mom_60s| ≥ seuil (OR).
                # Calme     : les deux sous seuil → momentum = bruit → skip.
                # vol_ratio  = vol_60s / vol_300s (régime vol court vs long terme)
                # mom_60s    = retour normalisé sur 60s (Binance WS, zero-REST)
                if self.binance_ws:
                    _v60_vsd  = self.binance_ws.get_ns_vol("BTCUSDT", 60.0)
                    _v300_vsd = self.binance_ws.get_ns_vol("BTCUSDT", 300.0)
                    _vr_vsd   = max(0.10, min(5.0, _v60_vsd / max(_v300_vsd, 1e-6)))
                    _m60_vsd  = self.binance_ws.get_ns_momentum("BTCUSDT", 60.0)
                else:
                    _vr_vsd, _m60_vsd = 1.0, 0.0

                _hot_by_vol = _vr_vsd  >= conf["vol_spike_ratio_min"]
                _hot_by_mom = abs(_m60_vsd) >= conf["vol_spike_mom60_min"]

                if not (_hot_by_vol or _hot_by_mom):
                    if not hasattr(self, '_volskip_log_ts'):
                        self._volskip_log_ts: dict = {}
                    _now_vs = time.time()
                    if _now_vs - self._volskip_log_ts.get(market.market_id, 0.0) > 60.0:
                        logger.info(
                            "[VOL SKIP] Round calme — vr=%.2f(min=%.2f) m60=%+.4f%%(min=%.4f%%) "
                            "→ sprint %s ignoré",
                            _vr_vsd, conf["vol_spike_ratio_min"],
                            _m60_vsd * 100, conf["vol_spike_mom60_min"] * 100,
                            market.market_id[:8],
                        )
                        self._volskip_log_ts[market.market_id] = _now_vs
                    continue

                # ── Direction helpers ─────────────────────────────────────
                #
                # Bug V20 corrigé : dir_ok_mom n'a de sens que si mom dépasse
                # le seuil de bruit tmom_dir_min. En dessous, le signe est
                # aléatoire (microstructure noise). On l'invalide explicitement.
                #
                mom_is_meaningful = abs_mom_v22 >= conf["tmom_dir_min"]
                dir_ok_mom = mom_is_meaningful and (
                    (edge_pct > 0 and m30 > 0) or (edge_pct < 0 and m30 < 0)
                )

                # dir_ok_obi : OBI remplace mom pour la confirmation directionnelle
                # dans les voies sniper. OBI < 0.01 = bruit de microstructure.
                obi_is_meaningful = abs_obi_v22 >= 0.01
                dir_ok_obi = obi_is_meaningful and (
                    (edge_pct > 0 and o_val > 0) or (edge_pct < 0 and o_val < 0)
                )

                # F3: CVD avancé — multi-TF + divergence + absorption
                #
                # BASE (V29): alignement CVD_30s avec direction de l'edge
                #   confirmé  (|cvd30| ≥ 0.08, même dir) → ×1.00
                #   neutre    (|cvd30| < 0.08)            → ×0.85
                #   contre    (|cvd30| ≥ 0.08, dir opp.)  → ×0.70
                #
                # DIVERGENCE (CVD_60s vs mom_30s):
                #   prix monte + CVD_60s vend → vendeurs cachés = bearish div
                #   prix baisse + CVD_60s achète → acheteurs cachés = bullish div
                #   divergence confirme notre edge → ×1.10 | invalide → ×0.85
                #
                # ABSORPTION (CVD_10s fort + prix statique):
                #   |cvd10| > 0.25 ET |mom_30s| < 0.02% → liquidity wall
                #   absorption confirme edge → ×1.10 | invalide → ×0.85
                cvd30_val = live_cvd30

                # ── Base V29 ──────────────────────────────────────────────────
                _cvd_aligns = (
                    (edge_pct > 0 and cvd30_val > 0) or (edge_pct < 0 and cvd30_val < 0)
                )
                if abs(cvd30_val) >= 0.08 and _cvd_aligns:
                    _base_cvd = 1.00
                elif abs(cvd30_val) < 0.08:
                    _base_cvd = 0.85
                else:
                    _base_cvd = 0.70

                # ── Divergence: mom_30s direction vs CVD_60s direction ────────
                _dir_mom_f3  = 1 if m30 > 0.0002 else (-1 if m30 < -0.0002 else 0)
                _dir_cvd60   = 1 if live_cvd60 > 0.05 else (-1 if live_cvd60 < -0.05 else 0)
                _divergence  = (_dir_mom_f3 != 0 and _dir_cvd60 != 0
                                and _dir_mom_f3 != _dir_cvd60)
                # divergence confirme edge : prix monte + CVD vend → bearish → confirme BUY NO
                _div_confirms = _divergence and (
                    (edge_pct < 0 and _dir_mom_f3 > 0 and _dir_cvd60 < 0) or
                    (edge_pct > 0 and _dir_mom_f3 < 0 and _dir_cvd60 > 0)
                )
                if _div_confirms:
                    _base_cvd = min(1.15, _base_cvd * 1.10)   # divergence confirme → +10%
                elif _divergence:
                    _base_cvd *= 0.85                          # divergence invalide → −15%

                # ── Absorption: CVD_10s fort + prix statique ─────────────────
                # Grand flux taker en 10s mais prix immobile = mur de liquidité
                _has_absorption = abs(live_cvd10) > 0.25 and abs(m30) < 0.0002
                _abs_confirms = _has_absorption and (
                    (edge_pct < 0 and live_cvd10 > 0.25) or   # acheteurs absorbés → bearish
                    (edge_pct > 0 and live_cvd10 < -0.25)     # vendeurs absorbés  → bullish
                )
                if _abs_confirms:
                    _base_cvd = min(1.15, _base_cvd * 1.10)
                elif _has_absorption:
                    _base_cvd *= 0.85

                cvd_size_mult = round(max(0.50, min(1.15, _base_cvd)), 2)

                # Tag pour les logs EVAL/FIRE
                if _div_confirms:
                    _cvd_tag = "/div✓"
                elif _divergence:
                    _cvd_tag = "/div✗"
                elif _abs_confirms:
                    _cvd_tag = "/abs✓"
                elif _has_absorption:
                    _cvd_tag = "/abs✗"
                else:
                    _cvd_tag = ""

                # ── Évaluation des 3 paths (hiérarchique) ─────────────────

                # Path 1 — Standard : momentum + direction cohérente + fenêtre temporelle
                standard_pass = (
                    abs_edge_v22 >= conf["tedge_gate"]
                    and abs_mom_v22  >= conf["tmom"]
                    and dir_ok_mom
                    and time_left_sec <= conf["standard_max_time_sec"]
                )

                # Path 2 — Sniper A : haute conviction, fenêtre temporelle serrée
                # OBI confirme la direction à la place du momentum.
                # V29: CVD_30s n'est plus une gate — il module la taille via cvd_size_mult.
                sniper_a_pass = (
                    abs_edge_v22     >= conf["sniper_edge_a"]
                    and abs_obi_v22  >= conf["sniper_obi_a"]
                    and time_left_sec <= conf["sniper_time_a_sec"]
                    and time_left_sec >= conf["sniper_min_time_sec"]
                    and dir_ok_obi
                )

                # Path B — Sniper B : ultra conviction, fenêtre large.
                # Cible les setups edge ≥ 12% + OBI fort bloqués uniquement
                # par mom faible (ex: near-miss 08:01:45 edge=+13.75% OBI=+0.85).
                # V29: CVD_30s → sizing modifier uniquement.
                sniper_b_pass = (
                    abs_edge_v22     >= conf["sniper_edge_b"]
                    and abs_obi_v22  >= conf["sniper_obi_b"]
                    and time_left_sec <= conf["sniper_time_b_sec"]
                    and time_left_sec >= conf["sniper_min_time_sec"]
                    and dir_ok_obi
                )

                # ── Anti-spam Sniper ──────────────────────────────────────
                # Limite à 1 sniper par market_id sur toute sa durée de vie.
                # Lazy-init cohérent avec le reste de la classe.
                now_ts = time.time()
                if not hasattr(self, '_sniper_anti_spam'):
                    self._sniper_anti_spam: dict = {}
                # Purge des entrées expirées (évite la fuite mémoire sur longue session)
                self._sniper_anti_spam = {
                    k: v for k, v in self._sniper_anti_spam.items()
                    if now_ts - v < conf["sniper_cooldown_sec"]
                }
                sniper_already_fired = market.market_id in self._sniper_anti_spam

                # ── Décision finale (hiérarchique : Standard > Sniper B > Sniper A) ──
                side        = None
                fire_reason = None
                is_sniper   = False

                # V24: Standard bloqué si un sniper a déjà firé sur ce marché (300s)
                # Évite le pyramidage : sniper + standard = double exposition non désirée
                if standard_pass and not sniper_already_fired:
                    side        = "buy" if edge_pct > 0 else "sell"
                    fire_reason = "STANDARD_MOM_FLOW"
                    is_sniper   = False

                elif sniper_b_pass and not sniper_already_fired:
                    # Sniper B évalué avant A (conviction plus haute = prioritaire)
                    side        = "buy" if edge_pct > 0 else "sell"
                    fire_reason = "SNIPER_B_ULTRA_TRIGGERED"
                    is_sniper   = True
                    # Anti-spam posé APRÈS le guard p_poly stale (évite cooldown fantôme)

                elif sniper_a_pass and not sniper_already_fired:
                    side        = "buy" if edge_pct > 0 else "sell"
                    fire_reason = "SNIPER_A_HIGH_TRIGGERED"
                    is_sniper   = True
                    # Anti-spam posé APRÈS le guard p_poly stale (évite cooldown fantôme)

                # ── Guard p_poly stale : bloquer FIRE si p_poly = défaut 0.500 ──
                # Cause du bug 2026-02-28 : restart → REST CLOB pas encore warm →
                # p_poly = 0.500 (défaut Gamma API stale) → edge fictif → fallback
                # market exécuté au vrai prix CLOB (~0.97) → edge réel négatif.
                if side and abs(p_poly - 0.500) < 0.005:
                    # Fix Bug1: UP/DOWN markets CLOB toujours vide → skip sync rescue (évite timeout blocking)
                    # Pour marchés avec strike fixe seulement : UP/DOWN = CLOB vide par nature.
                    _no_strike_rescue = ("$" not in market.question)
                    if not _no_strike_rescue:
                        # Dernier recours : appel REST synchrone (≤0.4s réduit vs ancien 1.5s)
                        try:
                            _sync_url = f"https://clob.polymarket.com/book?token_id={market.yes_token_id}"
                            _sync_r = requests.get(_sync_url, timeout=0.4)
                            if _sync_r.status_code == 200:
                                _sd = _sync_r.json()
                                _sb, _sa = _sd.get("bids", []), _sd.get("asks", [])
                                if _sb and _sa:
                                    _smid = (float(_sb[0]["price"]) + float(_sa[0]["price"])) / 2.0
                                    if 0.01 <= _smid <= 0.99 and abs(_smid - 0.500) >= 0.005:
                                        p_poly = _smid
                                        self._rest_clob_mid_cache[market.yes_token_id] = (_smid, time.time())
                                        edge_pct = (p_true - p_poly) * 100.0
                                        logger.info("[FIRE BLOCKED] p_poly rescuée via sync REST: %.3f → edge recalc %+.2f%%",
                                                    p_poly, edge_pct)
                        except Exception:
                            pass

                    if abs(p_poly - 0.500) < 0.005:
                        # Relaxation pour marchés UP/DOWN (sans strike $) :
                        # Ces marchés démarrent naturellement à p_poly=0.500 et leur
                        # CLOB peut rester vide toute la durée (AMM-only).
                        # Après 120s d'ouverture, on accepte 0.500 comme prix AMM réel.
                        _no_strike_mkt = ("$" not in market.question)
                        _elapsed_s     = 300.0 - time_left_sec  # sprint = 300s
                        if _no_strike_mkt and _elapsed_s >= 120.0 and is_sniper:
                            # Seulement niveau sniper (E≥8%/12%) pour AMM-only :
                            # standard_pass (E~3%) trop faible quand p_poly incertain.
                            logger.info(
                                "[STALE RELAX] UP/DOWN CLOB vide depuis %.0fs — "
                                "p_poly=0.500 accepté (sniper %s, marché %s)",
                                _elapsed_s, fire_reason, market.market_id,
                            )
                            # side reste posé — trade autorisé
                        else:
                            # p_poly toujours stale après rescue → bloquer
                            if not hasattr(self, '_stale_blocked_ts'):
                                self._stale_blocked_ts: dict = {}
                            _now_sb = time.time()
                            if _now_sb - self._stale_blocked_ts.get(market.market_id, 0.0) > 30.0:
                                logger.warning(
                                    "[FIRE BLOCKED] p_poly stale (%.3f) sur %s — "
                                    "sync REST aussi vide, signal ignoré",
                                    p_poly, market.market_id,
                                )
                                self._stale_blocked_ts[market.market_id] = _now_sb
                            side        = None
                            fire_reason = None

                # ── PULSE + SNIPER_EVAL LOG (throttled 30s par marché) ────────
                # Un seul log par marché toutes les 30s pour éviter le spam.
                # FIRE et COOLDOWN sont toujours loggués immédiatement.
                now = time.time()
                if not hasattr(self, '_last_eval_log_ts'):
                    self._last_eval_log_ts: dict = {}

                will_fire = (standard_pass or sniper_a_pass or sniper_b_pass) and not sniper_already_fired and (side is not None)

                # Anti-spam sniper : posé ICI (après guard p_poly) pour éviter le cooldown fantôme.
                # Si side=None (FIRE BLOCKED), le cooldown n'est PAS posé → retry au prochain sprint.
                if will_fire and is_sniper:
                    self._sniper_anti_spam[market.market_id] = now_ts

                throttle_elapsed = now - self._last_eval_log_ts.get(market.market_id, 0.0) > 30.0

                if will_fire or throttle_elapsed:
                    def _sc(ok, label):
                        return f"{label}✓" if ok else f"{label}✗"
                    spot = self.binance_ws.get_mid("BTCUSDT") if self.binance_ws else 0.0
                    e_std = min(999, abs_edge_v22 / conf["tedge_gate"]      * 100)
                    e_sa  = min(999, abs_edge_v22 / conf["sniper_edge_a"]   * 100)
                    e_sb  = min(999, abs_edge_v22 / conf["sniper_edge_b"]   * 100)
                    m_std = min(999, abs_mom_v22  / conf["tmom"]            * 100)
                    o_sa  = min(999, abs_obi_v22  / conf["sniper_obi_a"]    * 100)
                    o_sb  = min(999, abs_obi_v22  / conf["sniper_obi_b"]    * 100)
                    _min_t = conf["sniper_min_time_sec"]
                    sa_detail = " ".join([
                        _sc(abs_edge_v22  >= conf["sniper_edge_a"],       f"E≥{conf['sniper_edge_a']:.0f}%"),
                        _sc(abs_obi_v22   >= conf["sniper_obi_a"],        f"OBI≥{conf['sniper_obi_a']}"),
                        _sc(time_left_sec <= conf["sniper_time_a_sec"],   f"t≤{conf['sniper_time_a_sec']}s(={time_left_sec:.0f}s)"),
                        _sc(time_left_sec >= _min_t,                      f"t≥{_min_t}s"),
                        _sc(dir_ok_obi,                                   "dirOBI"),
                        f"CVD30={cvd30_val:+.2f}(×{cvd_size_mult:.2f}{_cvd_tag})",
                    ])
                    sb_detail = " ".join([
                        _sc(abs_edge_v22  >= conf["sniper_edge_b"],       f"E≥{conf['sniper_edge_b']:.0f}%"),
                        _sc(abs_obi_v22   >= conf["sniper_obi_b"],        f"OBI≥{conf['sniper_obi_b']}"),
                        _sc(time_left_sec <= conf["sniper_time_b_sec"],   f"t≤{conf['sniper_time_b_sec']}s(={time_left_sec:.0f}s)"),
                        _sc(time_left_sec >= _min_t,                      f"t≥{_min_t}s"),
                        _sc(dir_ok_obi,                                   "dirOBI"),
                        f"CVD30={cvd30_val:+.2f}(×{cvd_size_mult:.2f}{_cvd_tag})",
                    ])
                    fire_tag  = " → 🔥FIRE" if will_fire else ""
                    spam_tag  = " ⛔COOLDOWN" if sniper_already_fired else ""
                    _vr_tag   = f"VR={_vr_vsd:.2f}({'🔥' if _hot_by_vol else '❄'})"
                    logger.info(
                        "[V29 EVAL] %-18s | Spot=%.0f$ t=%.0fs | "
                        "E=%+.2f%%(std:%3.0f%% sa:%3.0f%% sb:%3.0f%%) "
                        "M=%+.4f%%(%3.0f%%) OBI=%+.3f(sa:%3.0f%% sb:%3.0f%%) "
                        "CVD30=%+.2f(×%.2f) %s | "
                        "dir[M=%s O=%s] gate[Std=%s SA=%s SB=%s]%s%s | SA[%s] | SB[%s]",
                        market.question[:18], spot, time_left_sec,
                        edge_pct, e_std, e_sa, e_sb,
                        m30, m_std,
                        o_val, o_sa, o_sb, cvd30_val, cvd_size_mult, _vr_tag,
                        "✓" if dir_ok_mom    else "✗",
                        "✓" if dir_ok_obi    else "✗",
                        "✓" if standard_pass else "✗",
                        "✓" if sniper_a_pass else "✗",
                        "✓" if sniper_b_pass else "✗",
                        spam_tag, fire_tag,
                        sa_detail, sb_detail,
                    )
                    self._last_eval_log_ts[market.market_id] = now

                # ── FIRE ──────────────────────────────────────────────────
                if side:
                    max_edge_found = max(max_edge_found, abs_edge_v22)

                    # ── Sizing (calculé avant le log pour l'inclure) ───────
                    sizing_penalty  = 1.0
                    streak_applied  = False
                    sniper_applied  = False

                    # Anti-Streak V17
                    if side == "buy" and is_btc and self._check_market_streaks():
                        sizing_penalty *= conf["anti_streak_penalty"]
                        streak_applied  = True

                    # Sniper : pas de confirmation momentum → position réduite
                    if is_sniper:
                        sizing_penalty *= conf["sniper_sizing_mult"]
                        sniper_applied  = True

                    # V25: Kelly dynamique — proportionnel à l'edge
                    # edge = 1×tedge_gate (3%) → kelly_mult 0.5×
                    # edge = 2×tedge_gate (6%) → kelly_mult 1.0×
                    # edge ≥ 4×tedge_gate (12%) → kelly_mult 1.5× (cap)
                    kelly_mult = min(1.5, max(0.5, abs_edge_v22 / (conf["tedge_gate"] * 2.0)))

                    # V32: Regime detection — vol_ratio = vol_60s / vol_300s
                    # trending (>1.5) : momentum fort mais retournement probable → ×0.85
                    # calme    (<0.70) : peu de signal, edge potentiellement bruit  → ×0.75
                    # neutre           : pas d'ajustement → ×1.00
                    _vr = getattr(self, "_last_vol_ratio", 1.0)
                    if _vr > 1.50:
                        regime_mult  = 0.85
                        regime_label = f"regime_trend×0.85(vr={_vr:.2f})"
                    elif _vr < 0.70:
                        regime_mult  = 0.75
                        regime_label = f"regime_calm×0.75(vr={_vr:.2f})"
                    else:
                        regime_mult  = 1.00
                        regime_label = None

                    base_order = (balance * self.ORDER_SIZE_PCT * self.SIZING_MULT
                                  * sizing_penalty * kelly_mult * cvd_size_mult * regime_mult)
                    order_size = min(base_order * 2.8, portfolio * 0.06, conf["max_order_usdc"])
                    # V23: market orders → size = USDC (pas shares).
                    # place_market_order(amount=signal.size) attend des USDC.
                    # risk._compute_order_cost retourne signal.size pour market orders.
                    usdc_amount = round(order_size, 2)

                    # ── FIRE LOG ──────────────────────────────────────────────
                    penalty_detail = []
                    if streak_applied:
                        penalty_detail.append(f"streak×{conf['anti_streak_penalty']}")
                    if sniper_applied:
                        penalty_detail.append(f"sniper×{conf['sniper_sizing_mult']}")
                    penalty_detail.append(f"kelly×{kelly_mult:.2f}")
                    penalty_detail.append(f"cvd×{cvd_size_mult:.2f}")
                    if regime_label:
                        penalty_detail.append(regime_label)
                    penalty_str = ("×".join([""] + penalty_detail) if penalty_detail else "×1.0")
                    direction = "YES" if side == "buy" else "NO"
                    logger.info(
                        "🔥 [V22 FIRE] BUY %-3s | %-26s | "
                        "E=%+.2f%% M=%+.4f%% OBI=%+.3f | "
                        "p_poly=%.3f p_true=%.3f | "
                        "t=%.0fs | $%.2f USDC penalty=%s | mkt=%s",
                        direction, fire_reason,
                        edge_pct, m30, o_val,
                        p_poly, p_true,
                        time_left_sec,
                        usdc_amount, penalty_str,
                        market.market_id[:8],
                    )

                    # V23: BUY YES (bullish) ou BUY NO (bearish) — jamais SELL.
                    signal_token = token_yes if side == "buy" else token_no
                    opposite_token = token_no if side == "buy" else token_yes

                    # Guard anti-hedging : bloquer si position adverse déjà ouverte
                    # sur CE marché (UP+DOWN = argent perdu en spread garanti).
                    if self.db:
                        opp_qty = self.db.get_position(opposite_token)
                        if opp_qty and opp_qty > 0.01:
                            logger.warning(
                                "⛔ [V22 ANTI-HEDGE] BUY %s bloqué sur %s — position adverse %.2f %s déjà ouverte",
                                direction, market.market_id[:8],
                                opp_qty, "NO" if side == "buy" else "YES",
                            )
                            self._last_quote_ts[market.yes_token_id] = time.time()
                            continue

                    # ── Upgrade 1 (V32) : Limit maker — spread-aware + adverse-sel filter
                    #
                    # Source A — WS CLOB book (temps réel, O(1)) :
                    #   spread ≤ 2 ticks  → market direct (impossible de s'insérer)
                    #   spread ≥ 4 cents  → limit au mid exact (capture spread/2)
                    #   spread normal     → limit à ask − 1 tick (0.001)
                    #
                    # Source B — REST CLOB cache (5s lag) :
                    #   Post à mid + 0.5 tick si WS vide ET t > 45s ET cache frais.
                    #
                    # Filtre adverse selection (C) :
                    #   OBI > 0.75 → flux très informé/directionnel → market direct
                    #   Le gain de rebate maker ne compense pas la latence d'exécution.
                    _sm_limit_price = None
                    _sm_shares      = None

                    # ── C) Filtre adverse selection : OBI fort → market direct ───
                    _high_adverse = abs_obi_v22 > 0.75
                    if _high_adverse:
                        logger.debug(
                            "[SprintMaker] OBI fort (%.3f>0.75) → market direct "
                            "(adverse sel. + latence)",
                            abs_obi_v22,
                        )

                    # ── Source A : WS book (spread-aware) ───────────────────────
                    if not _high_adverse and hasattr(self.client, "ws_client") and self.client.ws_client.running:
                        _ob = self.client.ws_client.get_order_book(signal_token)
                        if _ob and _ob.get("asks"):
                            _best_ask = float(_ob["asks"][0]["price"])
                            _best_bid = float(_ob["bids"][0]["price"]) if _ob.get("bids") else None
                            _spread   = round(_best_ask - _best_bid, 4) if _best_bid else None
                            if _spread is not None and _spread <= 0.002:
                                # Locked/1-tick → impossible de s'insérer → FOK
                                logger.debug("[SprintMaker] Spread locked (%.3f) → FOK", _spread)
                            else:
                                if _spread is not None and _spread >= 0.040:
                                    # Spread large → mid exact → capture spread/2
                                    _lp = round((_best_ask + _best_bid) / 2, 3)
                                else:
                                    # Spread normal → ask − 1 tick
                                    _lp = round(max(0.01, _best_ask - 0.001), 3)
                                _sh = round(usdc_amount / _lp, 2)
                                if _sh >= 5.0:
                                    _sm_limit_price = _lp
                                    _sm_shares      = _sh
                                    logger.debug(
                                        "[SprintMaker] WS spread=%.3f → limit@%.3f (%s sh)",
                                        _spread if _spread else 0.0, _lp, _sh,
                                    )

                    # ── Source B : REST CLOB cache (fallback si WS vide) ────────
                    if not _high_adverse and _sm_limit_price is None and time_left_sec > 45.0:
                        _rc = self._rest_clob_mid_cache.get(signal_token)
                        if _rc and (time.time() - _rc[1]) < 10.0:
                            _rest_mid = _rc[0]
                            _lp = round(min(0.970, _rest_mid + 0.005), 3)
                            _sh = round(usdc_amount / _lp, 2)
                            if _sh >= 5.0:
                                _sm_limit_price = _lp
                                _sm_shares      = _sh
                                logger.debug(
                                    "[SprintMaker] REST cache %.3f → limit@%.3f (%s sh) t=%.0fs",
                                    _rest_mid, _lp, _sh, time_left_sec,
                                )

                    if _sm_limit_price is not None:
                        _order_type = "sprint_maker"
                        _price      = _sm_limit_price
                        _size       = _sm_shares
                    else:
                        # Locked/OBI-fort/no-data → FOK market direct
                        _order_type = "market"
                        _price      = 0.99
                        _size       = usdc_amount

                    # p_true du token signé (YES ou NO selon direction)
                    _signal_p_true = p_true if direction == "YES" else (1.0 - p_true)
                    signals.append(Signal(
                        token_id=signal_token,
                        market_id=market.market_id,
                        market_question=market.question,
                        yes_token_id=market.yes_token_id,  # V40: pour clear cooldown si ordre échoue
                        side="buy",  # Toujours BUY (YES ou NO selon direction)
                        order_type=_order_type,
                        price=_price,
                        size=_size,
                        confidence=0.99,
                        reason=(
                            f"V22 {fire_reason} {direction}: "
                            f"E={edge_pct:+.1f}% M={m30:+.4f}% O={o_val:+.3f} "
                            f"t={time_left_sec:.0f}s ${usdc_amount:.1f}"
                        ),
                        mid_price=0.50,
                        spread_at_signal=0.01,
                        p_true=_signal_p_true,
                    ))
                    # V24: Cooldown post-FIRE prolongé pour sprints (90s effectifs)
                    # Formule : _last_quote_ts = T + (90 - quote_cooldown)
                    # → check (T+now - last < 16) bloque jusqu'à T+now = last + 16 = T_fire + 90
                    # Évite pyramidage (3 fires en 90s sur même marché → 1 seul max)
                    self._last_quote_ts[market.yes_token_id] = time.time() + (90.0 - self._quote_cooldown)

                    if self.db:
                        spot_price = live_spot if "live_spot" in locals() else 0.0
                        sniper_tag = (
                            f" <span class='text-yellow-400 font-bold'>[{fire_reason}]</span>"
                            if is_sniper else ""
                        )
                        self.db.add_log(
                            "INFO", "sniper_feed",
                            f"{market.question[:22]}… | "
                            f"Spot:{spot_price:.0f}$ t:{time_left_sec:.0f}s | "
                            f"E:{edge_pct:+.1f}% ${order_size:.1f}USDC | "
                            f"<span class='text-green-400 font-bold'>{side.upper()}</span>{sniper_tag}"
                        )

                else:
                    # PASS LOG — uniquement si edge > 2% (évite le spam sur setups triviaux)
                    if abs_edge_v22 > 2.0 and self.db:
                        spot_price = live_spot if "live_spot" in locals() else 0.0

                        # Raison de blocage la plus précise possible
                        if abs_edge_v22 < conf["tedge_gate"]:
                            block = f"EDGE_LOW({abs_edge_v22:.1f}%)"
                        elif abs_mom_v22 < conf["tmom"] and abs_edge_v22 < conf["sniper_edge_a"]:
                            block = f"MOM_WEAK({abs_mom_v22:.4f}%)"
                        elif not dir_ok_mom and not dir_ok_obi:
                            e_dir = "UP" if edge_pct > 0 else "DN"
                            m_dir = "UP" if m30 > 0 else ("DN" if m30 < 0 else "FLAT")
                            o_dir = "UP" if o_val > 0 else ("DN" if o_val < 0 else "FLAT")
                            block = f"DIR_CONFLICT(E={e_dir},M={m_dir},O={o_dir})"
                        elif sniper_already_fired:
                            block = "SNIPER_COOLDOWN"
                        elif abs_edge_v22 >= conf["sniper_edge_a"] and not dir_ok_obi:
                            block = f"SNIPER_DIR_OBI_FAIL(obi={o_val:+.3f})"
                        elif abs_edge_v22 >= conf["sniper_edge_a"] and time_left_sec > conf["sniper_time_b_sec"]:
                            block = f"SNIPER_TOO_EARLY(t={time_left_sec:.0f}s)"
                        elif abs_edge_v22 >= conf["sniper_edge_a"] and abs_obi_v22 < conf["sniper_obi_b"]:
                            block = f"SNIPER_OBI_LOW({abs_obi_v22:.3f}<{conf['sniper_obi_b']})"
                        else:
                            block = f"MOM_WEAK({abs_mom_v22:.4f}%)"

                        self.db.add_log(
                            "INFO", "sniper_feed",
                            f"{market.question[:22]}… | "
                            f"Spot:{spot_price:.0f}$ t:{time_left_sec:.0f}s | "
                            f"E:{edge_pct:+.1f}% M:{m30:+.3f}% O:{o_val:+.3f} | "
                            f"<span class='text-slate-500'>PASS [{block}]</span>"
                        )

                # ── Bookkeeping (inchangé) ────────────────────────────────
                min_spread_found = min(min_spread_found, market.spread if market.spread > 0 else 0.01)
                daily_edge_scores.append(abs_edge_v22)
                max_edge_found = max(max_edge_found, abs_edge_v22)
                continue  # bypass complet du pipeline MM standard
            # ---------------------------------------------------------------

            abs_edge = abs(edge_pct)

            # Volume filter
            if vol_5m < min_vol:
                logger.debug("[V10.3/V10.5] %s ignoré (vol5m=%.0f$ < %.0f)", market.question[:20], vol_5m, min_vol)
                continue

            # Edge threshold
            if abs_edge < min_edge:
                logger.debug("[V10.3/V10.5] %s skip — |edge|=%.1f%% < %.1f%%", market.question[:20], abs_edge, min_edge)
                continue

            # Dynamic side decision
            side = self._decide_side(market, edge_pct, min_edge)
                
            if not side:
                continue

            dir_label = "BUY UP" if side == "buy" else "BUY DOWN"

            # On ignore le bloc legacy is_sprint ici si jamais ça atteint cette ligne (ça ne devrait pas)
            max_edge_found = max(max_edge_found, abs_edge)

            # ── Kelly-inspired tiered sizing ───────────────────────────
            if abs_edge >= 35.0:
                size_multiplier = 2.8
            elif abs_edge >= 25.0:
                size_multiplier = 1.8
            else:
                size_multiplier = 1.0

            base_order = balance * self.ORDER_SIZE_PCT * self.SIZING_MULT
            order_size = min(base_order * size_multiplier, portfolio * max_trade, self.MAX_ORDER_USDC)
            if order_size < 1.0:
                continue

            bid_price = min(market.mid_price + 0.005, 0.98)
            shares = max(5.0, order_size / bid_price)

            side_label = "BUY YES" if side == "buy" else "BUY NO"
            if min(market.days_to_expiry * 1440, 60.0) <= 5.5: pass  # Handled earlier
            else:
                logger.info("[V10.3] P_true=%.2f | P_poly=%.2f | Edge=%+.1f%% → %s | sizing=%.1fx | $%.2f | vol5m=$%.0f",
                            p_true, p_poly, edge_pct, side_label, size_multiplier, order_size, vol_5m)

            signals.append(Signal(
                token_id=market.yes_token_id,
                market_id=market.market_id,
                market_question=market.question,
                side=side,
                order_type="limit",
                price=round(bid_price, 3),
                size=round(shares, 2),
                confidence=min(0.99, 0.70 + abs_edge / 100),
                reason=f"V10Edge={edge_pct:+.1f}%_P={p_true:.2f}_x{size_multiplier}",
                mid_price=market.mid_price
            ))
            self._last_quote_ts[market.yes_token_id] = time.time()
            traded += 1

        avg_edge = sum(daily_edge_scores) / len(daily_edge_scores) if daily_edge_scores else 0.0
        
        # V14.1 Log Throttling
        now = time.time()
        if not hasattr(self, '_last_v106_log_ts'):
            self._last_v106_log_ts = 0.0
        if now - self._last_v106_log_ts > 5.0 or signals:
            ws_status = "WS✓" if (self.binance_ws and self.binance_ws.is_connected) else "WS✗"
            eligible_n = len(markets)
            spot_str = f"${live_spot:,.0f}" if live_spot > 0 else "?$"
            if sprint_markets_count > 0:
                logger.info(
                    "[RADAR] %s BTC %s Mom%+.3f%% OBI%+.3f | Sprint %d/%d | EdgeMax %.1f%% | %d signal(s)",
                    ws_status, spot_str, live_mom, live_obi,
                    sprint_markets_count, eligible_n, max_edge_found, len(signals)
                )
            else:
                logger.info(
                    "[RADAR] %s BTC %s Mom%+.3f%% OBI%+.3f | %d éligibles | Aucun sprint — en attente",
                    ws_status, spot_str, live_mom, live_obi, eligible_n
                )
            self._last_v106_log_ts = now
        if self.db:
            with self._telemetry_lock:
                self._telemetry_buffer["info_edge_avg_score"] = round(avg_edge, 2)
                self._telemetry_buffer["live_found_markets"] = sprint_markets_count
                self._telemetry_buffer["live_max_edge"] = round(max_edge_found, 1)
                
                # Nouveau : Sauvegarde du spread (si aucun marché, on met 0)
                final_spread = round(min_spread_found, 3) if min_spread_found != 999.0 else 0.0
                self._telemetry_buffer["live_min_spread"] = final_spread
                # Funnel transparence — pipeline de décision visible dans le dashboard
                u = self._universe
                self._telemetry_buffer["live_funnel_raw"]      = u._f_raw
                self._telemetry_buffer["live_funnel_price"]    = u._f_price
                self._telemetry_buffer["live_funnel_volume"]   = u._f_volume
                self._telemetry_buffer["live_funnel_spread"]   = u._f_spread
                self._telemetry_buffer["live_funnel_eligible"] = u._f_eligible
                self._telemetry_buffer["live_funnel_sprint"]   = sprint_markets_count
                self._telemetry_buffer["live_btc_ws_age"]      = round(self.binance_ws.age_seconds, 1) if self.binance_ws else 999.0
                self._telemetry_buffer["live_sprint_window_active"] = 1 if sprint_markets_count > 0 else 0
                self._telemetry_buffer["live_sprint_count"]    = sprint_markets_count

        # --- V22: Write default checklist/edge telemetry when no sprint markets ---
        if sprint_markets_count == 0 and self.db:
            import json as _json
            with self._telemetry_lock:
                iv_ready = bool(getattr(self, '_last_iv', 0) > 0)
                mom_ok   = bool(abs(m30) >= 0.005)
                obi_ok   = bool(abs(o_val) >= 0.05)
                self._telemetry_buffer["live_sprint_edge"]  = 0.0
                self._telemetry_buffer["live_sprint_ptrue"] = 0.0
                self._telemetry_buffer["live_sprint_ppoly"] = 0.0
                self._telemetry_buffer["live_checklist"] = _json.dumps({
                    "mom_ok": mom_ok, "edge_ok": False,
                    "direction_ok": False, "iv_ready": iv_ready, "obi_ok": obi_ok,
                })
                self._telemetry_buffer["live_percentages"] = _json.dumps({
                    "mom_pct": round(min(150.0, abs(m30) / 0.005 * 100), 1),
                    "obi_pct": round(min(150.0, abs(o_val) / 0.05 * 100), 1),
                    "edge_pct": 0.0,
                })

        # --- V11.5/V22: Heartbeat pour le Live Scan Feed ---
        if self.db:
            now = time.time()
            if not hasattr(self, '_last_heartbeat_ts') or now - self._last_heartbeat_ts > 10.0:
                spot = self.binance_ws.get_mid("BTCUSDT") if self.binance_ws else 0.0
                eligible_count = len(markets)
                if sprint_markets_count == 0:
                    self.db.add_log("INFO", "sniper_feed",
                        f"📡 Radar | BTC {spot:.0f}$ | Aucune fenêtre sprint ({eligible_count} mkts éligibles)")
                elif len(signals) == 0:
                    avg_e = sum(daily_edge_scores) / len(daily_edge_scores) if daily_edge_scores else 0.0
                    self.db.add_log("INFO", "sniper_feed",
                        f"⏱ Sprint Window | BTC {spot:.0f}$ | {sprint_markets_count} mkt(s) | Edge moy: {avg_e:+.1f}% | Gates en attente")
                self._last_heartbeat_ts = now

        return signals

    def flush_telemetry(self):
        """V19: Asynchronously flush buffered telemetry to DB to avoid GIL locking in on_price_tick"""
        if not self.db or not self._telemetry_buffer:
            return
            
        try:
            # Snapshot the buffer to avoid race conditions with tick updates
            with self._telemetry_lock:
                snapshot = dict(self._telemetry_buffer)
                self._telemetry_buffer.clear()
            
            for key, value in snapshot.items():
                self.db.set_config(key, value)
        except Exception as e:
            logger.debug("[Telemetry] Erreur lors du flush DB: %s", e)

    def info_edge_signals_only(self, balance: float = 0.0) -> list[Signal]:
        """V10.6 ENFORCED — 105$ capital optimized + 5-MIN BTC SCALPER (Mom>0.015%)."""
        logger.info("[V10.6] Info Edge Only optimisé 105$ | 5-MIN SCALPER (Mom>0.015%) | Edge 12.5%/8.0%")
        return self.analyze(balance=balance)


    def get_eligible_markets(self) -> list[EligibleMarket]:
        return self._universe._cache if self._universe._cache else []



# ─── DummyStrategy (conservee pour tests) ────────────────────────────────────

class DummyStrategy(BaseStrategy):
    """
    Strategie factice : scan et log les marches CLOB, ne passe aucun ordre.
    Utiliser OBIMarketMakingStrategy pour le trading reel.
    """

    def analyze(self) -> list[Signal]:
        universe = MarketUniverse()
        markets = universe.get_eligible_markets()

        if not markets:
            logger.info("[Dummy] Aucun marche eligible trouve.")
            return []

        logger.info("[Dummy] %d marches eligibles:", len(markets))
        for m in markets[:5]:
            logger.info(
                "[Dummy]  '%s' | mid=%.4f | spread=%.4f | vol24h=%.0f | %.1fd",
                m.question[:55], m.mid_price, m.spread, m.volume_24h, m.days_to_expiry,
            )
        return []
