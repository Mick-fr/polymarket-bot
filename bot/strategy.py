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

    regime:   str            # 'bullish' | 'bearish' | 'neutral'

    regime:   str            # 'bullish' | 'bearish' | 'neutral'

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
        self._cache_ttl: float = 0.0   # MODIFIÉ V11.12 : Cache détruit pour forcer le temps réel

    def get_eligible_markets(self, force_refresh: bool = False) -> list[EligibleMarket]:
        """Retourne la liste des marches eligibles (avec cache 60s)."""
        now = time.time()
        if not force_refresh and (now - self._cache_ts) < self._cache_ttl and self._cache:
            return self._cache

        raw = self._fetch_gamma_markets()
        # 2026 V6.4 ULTRA-SURGICAL
        # self._detect_cross_arbitrage(raw)  # Removed caller to match new signature
        
        eligible = []
        for m in raw:
            result = self._evaluate(m)
            if result is not None:
                eligible.append(result)

        logger.info(
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
            
        return markets

    def _evaluate(self, m: dict) -> Optional[EligibleMarket]:
        """Applique tous les filtres sur un marche brut. Retourne None si rejete."""
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
            if "btc" in q_lower or "bitcoin" in q_lower:
                end_str = m.get("endDate") or m.get("end_date_iso") or ""
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

        # ── Volume 24h (evaluer avant le spread pour le seuil adaptatif) ──
        try:
            vol = float(m.get("volume24hr") or m.get("volume24hrClob") or 0)
        except (TypeError, ValueError):
            vol = 0.0

        if vol < MIN_VOLUME_24H and not is_btc_sprint_candidate:
            logger.debug("[Universe] '%s' rejete: volume24h=%.0f < %.0f",
                         question[:40], vol, MIN_VOLUME_24H)
            return None

        # ── Spread adaptatif (volume-aware) ──
        # Marches tres liquides (>50k vol) : spread 1 tick OK
        # Marches standards : spread 2 ticks minimum
        min_spread = MIN_SPREAD_HIGH_VOL if vol >= HIGH_VOL_THRESHOLD else MIN_SPREAD
        if spread < min_spread and not is_btc_sprint_candidate:
            logger.debug("[Universe] '%s' rejete: spread=%.4f < %.2f (vol=%.0f)",
                         question[:40], spread, min_spread, vol)
            return None

        # ── Maturite ──
        end_date_str = m.get("endDate") or m.get("end_date_iso") or ""
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
    Calcule le skew additionnel via le modèle d'Avellaneda-Stoikov simplifié.
    Formule : reservation_price = mid - (q * gamma * sigma^2 * T)
    - q : inventaire directionnel (négatif si short)
    - gamma : as_risk_aversion
    - sigma : volatilité historique
    - T : temps restant (en fraction de jour ou année, ici jours pour la démo)
    """
    def __init__(self, risk_aversion: float = 0.25):
        self.gamma = risk_aversion

    def calculate_skew_ticks(self, q: float, sigma: float, t_days: float) -> int:
        """Retourne le nombre de ticks de skew AS directionnel."""
        # Réservation price
        # Plus q est grand (inventaire long fort), plus RP baisse (pour inciter au ask bas)
        rp_delta = - (q * self.gamma * (sigma ** 2) * max(t_days, 0.1))
        # Skew directionnel (positif = penche à l'achat, négatif = penche à la vente)
        directional_skew = rp_delta  # Si rp_delta négatif, on baisse les quotes
        return round(directional_skew / TICK_SIZE)


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
            as_skew = self.as_skew_calc.calculate_skew_ticks(
                q=qty_held - self._config.as_inventory_target,
                sigma=volatility,
                t_days=days_to_expiry
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
    MIN_EDGE_SCORE = 12.5    # V10.3 — compromis qualité/quantité pour 100$
    MAX_TRADE_PCT  = 0.08
    MIN_MINUTES    = 5.0
    MAX_MINUTES    = 90.0    # sweet spot optimal
    MIN_VOLUME_5M  = 520     # ~150k$ volume 24h
    IMPLIED_VOL    = 0.80

    def __init__(self, client: PolymarketClient, db=None, max_markets: int = 20,
                 max_order_size_usdc: float = 12.0, binance_ws=None):
        super().__init__(client)
        self.db = db
        self.max_markets = max_markets
        self.max_order_size_usdc = max_order_size_usdc
        self._universe = MarketUniverse()
        self._last_quote_ts = {}
        self._quote_cooldown = 16.0
        self.binance_ws = binance_ws  # BinanceWSClient instance

    def _is_btc_eth(self, q: str) -> bool:
        q_up = q.upper()
        return any(x in q_up for x in ["BITCOIN", "BTC", "ETHEREUM", "ETH"])

    def _get_asset_symbol(self, q: str) -> str:
        """Retourne 'BTC' ou 'ETH' selon la question du marché."""
        q_up = q.upper()
        if "BTC" in q_up or "BITCOIN" in q_up:
            return "BTC"
        return "ETH"

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

    def _calculate_edge_score(self, market: EligibleMarket) -> tuple[float, float, float, float]:
        """
        Calcule le vrai Edge Score avec données Binance live.
        Returns: (edge_pct, p_true, p_poly, vol_estimate)
        """
        sym = self._get_asset_symbol(market.question)
        p_poly = market.mid_price  # probabilité Polymarket

        # Prix spot Binance live
        spot = 0.0
        funding = 0.0
        if self.binance_ws:
            spot = self.binance_ws.get_mid(sym)
            funding = self.binance_ws.get_funding(sym)

        # Strike proxy : dérivé du mid Polymarket
        # Pour un marché "BTC > 100k ?", le strike ~ spot*(1 - (p_poly-0.5)*0.1)
        if spot > 0:
            strike = spot * (1.0 - (p_poly - 0.5) * 0.1)
        else:
            # Pas de prix Binance — fallback demi-conservateur
            strike = 0.0

        minutes_to_expiry = market.days_to_expiry * 1440

        # Vol ajustée (funding rate bonus)
        vol = self.IMPLIED_VOL + abs(funding) * 50.0  # funding fort = vol plus haute

        if spot > 0 and strike > 0:
            p_true = self._compute_p_true(spot, strike, minutes_to_expiry, vol)
        else:
            p_true = 0.5

        edge_pct = (p_true - p_poly) * 100.0

        # Volume estimé 5-min proxy via volume_24h Polymarket
        vol_est = getattr(market, 'volume_24h', 0) / 288.0  # 24h / 288 = 5min

        return edge_pct, p_true, p_poly, vol_est

    def _decide_side(self, market: EligibleMarket, edge_pct: float, min_edge: float) -> str:
        """Décision dynamique : Edge > +min_edge → BUY YES | Edge < -min_edge → BUY NO."""
        if edge_pct >= min_edge:
            return "buy"   # BUY YES
        elif edge_pct <= -min_edge:
            return "sell"  # BUY NO (short YES)
        return ""  # pas de trade

    def analyze(self, balance: float = 0.0) -> list[Signal]:
        """V10.0: Real pricing BTC/ETH 5-40min, P_true log-normal, Edge >= 20%, Kelly tiered."""
        signals: list[Signal] = []
        if not self._universe.get_eligible_markets():
            pass # We still want to do the global update even if no markets

        markets = self._universe.get_eligible_markets()
        
        # --- V11.5 : Mise à jour continue du Radar Binance ---
        if self.binance_ws and self.binance_ws.is_connected and self.db:
            try:
                global_obi = self.binance_ws.get_binance_obi("BTCUSDT")
                global_mom = self.binance_ws.get_30s_momentum("BTCUSDT")
                self.db.set_config("live_btc_mom30s", round(global_mom, 4))
                self.db.set_config("live_btc_obi", round(global_obi, 3))
            except Exception as e:
                logger.debug("[Radar] Erreur update Binance globale: %s", e)
        # -------------------------------------------------------

        if not markets:
            # --- V11.5 : Heartbeat pour le Live Scan Feed (when no markets) ---
            if self.db:
                spot = self.binance_ws.get_mid("BTCUSDT") if self.binance_ws else 0.0
                self.db.add_log("INFO", "sniper_feed", f"📡 Radar Actif | BTC Spot: {spot:.2f}$ | En recherche de cible 5-Min...")
            return signals

        portfolio = balance
        if self.db:
            try:
                portfolio = float(self.db.get_config("last_portfolio_value", balance) or balance)
            except Exception:
                pass

        daily_edge_scores: list[float] = []
        max_edge_found = 0.0
        sprint_markets_count = 0

        traded = 0
        for market in markets:
            if traded >= self.max_markets:
                break

            if not self._is_btc_eth(market.question):
                continue

            minutes_to_expiry = market.days_to_expiry * 1440
            q_lower = market.question.lower()
            is_btc = ("bitcoin" in q_lower or "btc" in q_lower)
            
            # Sprint = BTC + expire dans moins de 5.5 minutes
            is_sprint = is_btc and (minutes_to_expiry <= 5.5)
            
            if is_sprint:
                sprint_markets_count += 1
                min_minutes = 1.0  # VITAL: On trade jusqu'à la dernière minute ! Pas 2.2.
                min_edge = 7.0     # Seuil assoupli pour capter le momentum
                min_vol = 0        # MODIFIÉ V11.7 : Un marché neuf n'a pas de volume initial
                max_trade = 0.06
                logger.info("[5MIN BTC SPRINT] Détecté — reste %.1f min | Edge cible=%.1f%%", minutes_to_expiry, min_edge)
            else:
                min_minutes = self.MIN_MINUTES
                min_edge = self.MIN_EDGE_SCORE
                min_vol = self.MIN_VOLUME_5M
                max_trade = self.MAX_TRADE_PCT

            if minutes_to_expiry < min_minutes or minutes_to_expiry > self.MAX_MINUTES:
                if not is_sprint:
                    logger.debug("[V10.3] %s ignoré (%.1fmin hors [%.1f,%.1f])", market.question[:20], minutes_to_expiry, min_minutes, self.MAX_MINUTES)
                continue

            if market.mid_price > 0 and (market.spread / market.mid_price) > 0.06:
                logger.debug("[V10.3] %s ignoré (spread > 6%%)", market.question[:20])
                continue

            if (time.time() - self._last_quote_ts.get(market.yes_token_id, 0.0)) < self._quote_cooldown:
                continue

            # ── V10.0 Real Edge Score (P_true log-normal vs P_poly) ────
            try:
                edge_pct, p_true, p_poly, vol_5m = self._calculate_edge_score(market)
            except Exception as e:
                logger.warning("[V10.0 EDGE] Erreur pricing: %s — skip", e)
                continue

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

            # V10.8 Binance Verification for Sprint Markets
            if is_sprint and self.binance_ws and self.binance_ws.is_connected:
                obi = self.binance_ws.get_binance_obi("BTCUSDT")
                mom30s = self.binance_ws.get_30s_momentum("BTCUSDT")
                
                if self.db:
                    try:
                        self.db.set_config("live_btc_mom30s", round(mom30s, 4))
                        self.db.set_config("live_btc_obi", round(obi, 3))
                    except Exception as e:
                        logger.debug("[Dashboard] Erreur set sniper live data: %s", e)
                
                logger.info("[5MIN BTC DEBUG] %s | Reste %.1fmin | Edge=%+.1f%% | Mom30s=%+.3f%% | OBI=%+.3f", market.question[:35], minutes_to_expiry, edge_pct, mom30s, obi)

                if side == "buy":  # BUY YES (UP)
                    if mom30s <= 0.012 or obi <= 0.12:
                        logger.debug("[5MIN BTC] %s skip BUY YES — Mom30s=%.3f%% (req>0.012) | OBI=%.2f (req>0.12)", market.question[:20], mom30s, obi)
                        continue
                elif side == "sell":  # BUY NO (DOWN)
                    if mom30s >= -0.012 or obi >= -0.12:
                        logger.debug("[5MIN BTC] %s skip BUY NO  — Mom30s=%.3f%% (req<-0.012) | OBI=%.2f (req<-0.12)", market.question[:20], mom30s, obi)
                        continue
                        
                # Validation passed (log pushed below with full formatting)
                dir_label = "BUY UP" if side == "buy" else "BUY DOWN"

            if is_sprint and self.db:
                spot_price = self.binance_ws.get_mid("BTCUSDT") if self.binance_ws else 0.0
                action_text = side.upper() if side else "PASS"
                action_html = f"<span class='text-green-400 font-bold'>{action_text}</span>" if action_text != "PASS" else "<span class='text-slate-500'>PASS</span>"
                msg = f"{market.question[:25]}... | Spot: {spot_price:.2f}$ | Poly: {p_poly:.2f} | Edge: {edge_pct:+.1f}% | Dec: {action_html}"
                self.db.add_log("INFO", "sniper_feed", msg)

            daily_edge_scores.append(abs_edge)
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
            if is_sprint:
                # V10.6 Sprint explicit log format
                logger.info("[5MIN BTC] P_true=%.2f | OBI=%+.2f | Mom30s=%+.3f%% | Edge=%+.1f%% → %s | sizing=%.1fx | vol5m=$%.0f",
                            p_true, getattr(self.binance_ws, "btc_obi", 0.0), getattr(self.binance_ws, "_last_mom" if hasattr(self.binance_ws, "_last_mom") else "mom30s", mom30s if is_sprint else 0.0), edge_pct, dir_label, size_multiplier, vol_5m)
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
        logger.info("[V10.6] Info Edge Only optimisé | 5-MIN SCALPER ENABLED | %d signal(s) | avg_edge=%.1f%%", len(signals), avg_edge)
        if self.db:
            try:
                self.db.set_config("info_edge_avg_score", round(avg_edge, 2))
            except Exception:
                pass
            self.db.set_config("live_found_markets", sprint_markets_count)
            self.db.set_config("live_max_edge", round(max_edge_found, 1))

        # --- V11.5 : Heartbeat pour le Live Scan Feed ---
        if self.db and len(signals) == 0:
            # Compter si on a vu des marchés sprint ce cycle
            sprint_count = sum(1 for m in markets if ("btc" in m.question.lower() or "bitcoin" in m.question.lower()) and (m.days_to_expiry * 1440 <= 5.5))
            if sprint_count == 0:
                spot = self.binance_ws.get_mid("BTCUSDT") if self.binance_ws else 0.0
                self.db.add_log("INFO", "sniper_feed", f"📡 Radar Actif | BTC Spot: {spot:.2f}$ | En recherche de cible 5-Min...")

        return signals

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
