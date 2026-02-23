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
MIN_SPREAD             =  0.02   # 2 ticks min — rentable apres gas (ex: 0.01)
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
        self._cache_ttl: float = 60.0   # Refresh toutes les 60s

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

    def _fetch_gamma_markets(self, limit: int = 100) -> list[dict]:
        url = (
            f"{GAMMA_API_URL}?closed=false&enableOrderBook=true"
            f"&order=volume24hr&ascending=false&limit={limit}"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "polymarket-bot/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                return data if isinstance(data, list) else data.get("markets", [])
        except Exception as e:
            logger.error("[Universe] Erreur Gamma API: %s", e)
            return []

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

        # ── Prix (mid) ──
        try:
            best_bid = float(m.get("bestBid") or 0)
            best_ask = float(m.get("bestAsk") or 1)
        except (TypeError, ValueError):
            return None

        if best_ask <= best_bid or best_ask == 0:
            return None

        mid = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid

        if not (MIN_PRICE <= mid <= MAX_PRICE):
            logger.debug("[Universe] '%s' rejete: mid=%.3f hors [%.2f, %.2f]",
                         question[:40], mid, MIN_PRICE, MAX_PRICE)
            return None

        # ── Volume 24h (evaluer avant le spread pour le seuil adaptatif) ──
        try:
            vol = float(m.get("volume24hr") or m.get("volume24hrClob") or 0)
        except (TypeError, ValueError):
            vol = 0.0

        if vol < MIN_VOLUME_24H:
            logger.debug("[Universe] '%s' rejete: volume24h=%.0f < %.0f",
                         question[:40], vol, MIN_VOLUME_24H)
            return None

        # ── Spread adaptatif (volume-aware) ──
        # Marches tres liquides (>50k vol) : spread 1 tick OK
        # Marches standards : spread 2 ticks minimum
        min_spread = MIN_SPREAD_HIGH_VOL if vol >= HIGH_VOL_THRESHOLD else MIN_SPREAD
        if spread < min_spread:
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

    def analyze(self, balance: float = 0.0) -> list[Signal]:
        signals: list[Signal] = []

        # Taille par ordre : 3% du solde (Set B), min 1 USDC, plafond max_order_size_usdc
        order_size_usdc = max(1.0, min(balance * ORDER_SIZE_PCT, self.max_order_size_usdc))
        logger.info(
            "[OBI] Sizing: solde=%.2f USDC -> order_size=%.2f USDC (%.0f%%, plafond=%.2f)",
            balance, order_size_usdc, ORDER_SIZE_PCT * 100, self.max_order_size_usdc,
        )

        markets = self._universe.get_eligible_markets()
        if not markets:
            logger.info("[OBI] Aucun marche eligible.")
            return signals

        traded = 0
        for market in markets:
            if traded >= self.max_markets:
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
            effective_size = order_size_usdc * maturity_factor * copy_sizing
            if maturity_factor < 1.0:
                logger.info(
                    "[OBI] Maturite courte (%.1fj < %.0fj) -> sizing x%.1f = %.2f USDC",
                    market.days_to_expiry, MATURITY_SHORT_DAYS, maturity_factor, effective_size,
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
            #   → BUY si inv_ratio < 0.60 (Set B, ex: 0.70)
            #   → SELL si position existante
            #   → Inventory skewing via RiskManager (ratio >= 0.60 → ask only)
            has_position = qty_held > 0.01
            if self.paper_trading:
                emit_sell = has_position
                emit_buy  = (not has_position) and (inv_ratio < INVENTORY_SKEW_THRESHOLD)
            else:
                emit_buy  = inv_ratio < INVENTORY_SKEW_THRESHOLD
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
