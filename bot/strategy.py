"""
Stratégie OBI Market Making pour Polymarket.

Architecture :
  - MarketUniverse  : filtre les marchés éligibles via Gamma API
  - OBICalculator   : calcule l'Order Book Imbalance sur 3 niveaux
  - OBIMarketMakingStrategy : génère les signaux bid/ask avec skewing

Règles :
  - Prix entre 0.20 et 0.80 USDC
  - Spread > 0.01 USDC
  - Volume 24h > 10 000 USDC
  - Maturité < 14 jours
  - OBI = (V_bid - V_ask) / (V_bid + V_ask) sur L1+L2+L3
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

logger = logging.getLogger("bot.strategy")

GAMMA_API_URL = "https://gamma-api.polymarket.com/markets"

# ─── Constantes OBI ──────────────────────────────────────────────────────────

OBI_BULLISH_THRESHOLD  =  0.30   # OBI > +0.30 → pression acheteuse
OBI_BEARISH_THRESHOLD  = -0.30   # OBI < -0.30 → pression vendeuse
MIN_PRICE              =  0.20
MAX_PRICE              =  0.80
MIN_SPREAD             =  0.01
MIN_VOLUME_24H         =  10_000.0
MAX_DAYS_TO_EXPIRY     =  14
TICK_SIZE              =  0.01   # 1 tick = 0.01 USDC


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


@dataclass
class EligibleMarket:
    """Marché passant tous les filtres Universe Selection."""
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
    """Résultat du calcul OBI."""
    obi:      float          # [-1, +1]
    v_bid:    float          # Volume agrégé bid L1+L2+L3
    v_ask:    float          # Volume agrégé ask L1+L2+L3
    regime:   str            # 'bullish' | 'bearish' | 'neutral'


# ─── Universe Selection ───────────────────────────────────────────────────────

class MarketUniverse:
    """
    Filtre les marchés Polymarket éligibles au market making.
    Source : API Gamma (enableOrderBook=true).
    """

    def __init__(self, max_days: int = MAX_DAYS_TO_EXPIRY):
        self._max_days = max_days
        self._cache: list[EligibleMarket] = []
        self._cache_ts: float = 0.0
        self._cache_ttl: float = 60.0   # Refresh toutes les 60s

    def get_eligible_markets(self, force_refresh: bool = False) -> list[EligibleMarket]:
        """Retourne la liste des marchés éligibles (avec cache 60s)."""
        now = time.time()
        if not force_refresh and (now - self._cache_ts) < self._cache_ttl and self._cache:
            return self._cache

        raw = self._fetch_gamma_markets()
        eligible = []
        for m in raw:
            result = self._evaluate(m)
            if result is not None:
                eligible.append(result)

        logger.info(
            "[Universe] %d marchés bruts → %d éligibles (filtres: prix, spread, volume, maturité)",
            len(raw), len(eligible),
        )
        self._cache = eligible
        self._cache_ts = now
        return eligible

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
        """Applique tous les filtres sur un marché brut. Retourne None si rejeté."""
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
            logger.debug("[Universe] '%s' rejeté: mid=%.3f hors [%.2f, %.2f]",
                         question[:40], mid, MIN_PRICE, MAX_PRICE)
            return None

        # ── Spread ──
        if spread < MIN_SPREAD:
            logger.debug("[Universe] '%s' rejeté: spread=%.4f < %.2f",
                         question[:40], spread, MIN_SPREAD)
            return None

        # ── Volume 24h ──
        try:
            vol = float(m.get("volume24hr") or m.get("volume24hrClob") or 0)
        except (TypeError, ValueError):
            vol = 0.0

        if vol < MIN_VOLUME_24H:
            logger.debug("[Universe] '%s' rejeté: volume24h=%.0f < %.0f",
                         question[:40], vol, MIN_VOLUME_24H)
            return None

        # ── Maturité ──
        end_date_str = m.get("endDate") or m.get("end_date_iso") or ""
        if not end_date_str:
            return None
        try:
            # Format ISO : "2026-02-28T00:00:00Z" ou "2026-02-28"
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
            logger.debug("[Universe] '%s' rejeté: %.1f jours restants (max=%d)",
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


# ─── Calcul OBI ──────────────────────────────────────────────────────────────

class OBICalculator:
    """
    Order Book Imbalance sur les 3 meilleurs niveaux.
    OBI = (V_bid - V_ask) / (V_bid + V_ask)  ∈ [-1, +1]

    Fallback : si le carnet CLOB est vide (Polymarket AMM), on génère
    un OBI synthétique neutre à partir du bestBid/bestAsk Gamma.
    """

    LEVELS = 3

    @classmethod
    def compute(cls, order_book: dict,
                best_bid: float = 0.0,
                best_ask: float = 0.0) -> Optional[OBIResult]:
        """
        order_book : réponse brute de get_order_book()
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

            # Trier : bids décroissant, asks croissant
            bids_sorted = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
            asks_sorted = sorted(asks, key=lambda x: float(x.get("price", 0)))

            v_bid = sum(float(b.get("size", 0)) for b in bids_sorted[:cls.LEVELS])
            v_ask = sum(float(a.get("size", 0)) for a in asks_sorted[:cls.LEVELS])
            total = v_bid + v_ask

            if total == 0:
                # ── Fallback : carnet AMM vide → OBI synthétique neutre ──
                # On considère la liquidité symétrique (AMM) et on positionne
                # notre quote entre bestBid et bestAsk de Gamma.
                if best_bid > 0 and best_ask > 0 and best_ask > best_bid:
                    logger.info(
                        "[OBI] Carnet CLOB vide → fallback AMM (Gamma bid=%.4f ask=%.4f)",
                        best_bid, best_ask,
                    )
                    # OBI neutre : on assume liquidité symétrique AMM
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


# ─── Stratégie OBI Market Making ─────────────────────────────────────────────

class OBIMarketMakingStrategy(BaseStrategy):
    """
    Market making avec skewing basé sur l'OBI.

    Pour chaque marché éligible :
      1. Récupère le carnet d'ordres du token YES
      2. Calcule l'OBI sur 3 niveaux
      3. Applique le skewing :
         - Neutre  : bid @ mid-½spread, ask @ mid+½spread
         - Bullish : bid agressif (top of book), ask passif (+2 ticks)
         - Bearish : bid passif, ask agressif (top of book)
      4. Génère 2 signaux (bid + ask) si pas en cooldown
    """

    def __init__(self, client: PolymarketClient, db=None,
                 max_order_size_usdc: float = 5.0,
                 max_markets: int = 5):
        super().__init__(client)
        self.db = db
        self.max_order_size_usdc = max_order_size_usdc  # Plafond absolu par ordre
        self.max_markets = max_markets
        self._universe = MarketUniverse()
        self._obi_calc = OBICalculator()
        # Suivi prix précédents pour news-breaker {token_id: (price, timestamp)}
        self._price_history: dict[str, tuple[float, float]] = {}
        # Cooldown re-cotation : évite de re-coter le même token trop rapidement
        # {token_id: last_quote_timestamp}
        self._last_quote_ts: dict[str, float] = {}
        self._quote_cooldown: float = 30.0  # secondes entre deux cotations du même token

    def analyze(self, balance: float = 0.0) -> list[Signal]:
        signals: list[Signal] = []

        # Taille par ordre : 2% du solde, min 1 USDC, plafond max_order_size_usdc
        order_size_usdc = max(1.0, min(balance * 0.02, self.max_order_size_usdc))
        logger.info(
            "[OBI] Sizing: solde=%.2f USDC → order_size=%.2f USDC (2%%, plafond=%.2f)",
            balance, order_size_usdc, self.max_order_size_usdc,
        )

        markets = self._universe.get_eligible_markets()
        if not markets:
            logger.info("[OBI] Aucun marché éligible.")
            return signals

        traded = 0
        for market in markets:
            if traded >= self.max_markets:
                break

            # ── Vérification cooldown news-breaker ──
            if self.db and self.db.is_in_cooldown(market.yes_token_id):
                logger.info("[OBI] '%s' en cooldown, skip.", market.question[:40])
                continue

            # ── Cooldown re-cotation (30s) : évite le sur-trading ──
            last_quote = self._last_quote_ts.get(market.yes_token_id, 0.0)
            if (time.time() - last_quote) < self._quote_cooldown:
                logger.debug("[OBI] '%s' re-cotation trop rapide, skip.", market.question[:40])
                continue

            # ── Carnet d'ordres YES ──
            try:
                ob = self.client.get_order_book(market.yes_token_id)
            except Exception as e:
                logger.info("[OBI] Impossible de récupérer le carnet pour %s: %s",
                            market.yes_token_id[:16], e)
                continue

            if ob is None:
                logger.info("[OBI] get_order_book a retourné None pour %s",
                            market.yes_token_id[:16])
                continue

            # Log du nombre de niveaux (OrderBookSummary ou dict)
            _bids_raw = (ob.get("bids") if isinstance(ob, dict) else getattr(ob, "bids", None)) or []
            _asks_raw = (ob.get("asks") if isinstance(ob, dict) else getattr(ob, "asks", None)) or []
            logger.info(
                "[OBI] Carnet '%s': %d bids, %d asks",
                market.question[:40], len(_bids_raw), len(_asks_raw),
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

            # ── News-Breaker : détection mouvement rapide ──
            mid = market.mid_price
            if self._is_news_event(market.yes_token_id, mid):
                logger.warning(
                    "[OBI] NEWS-BREAKER: mid-price de '%s' a bougé > 0.10 en < 5s → cooldown 10min",
                    market.question[:40],
                )
                if self.db:
                    self.db.set_cooldown(market.yes_token_id, 600, "news-breaker")
                    self.db.add_log("WARNING", "strategy",
                                   f"News-breaker: {market.question[:60]}")
                # Annuler les ordres sur ce marché
                try:
                    self.client.cancel_all_orders()
                except Exception:
                    pass
                continue

            self._update_price_history(market.yes_token_id, mid)
            self._last_quote_ts[market.yes_token_id] = time.time()

            # ── Calcul bid/ask avec skewing ──
            bid_price, ask_price = self._compute_quotes(
                mid=mid,
                spread=market.spread,
                obi=obi_result,
            )

            # Vérifications de base
            if bid_price >= ask_price:
                logger.debug("[OBI] bid >= ask pour '%s', skip.", market.question[:40])
                continue
            if not (0.01 <= bid_price <= 0.99) or not (0.01 <= ask_price <= 0.99):
                continue

            # Sizing basé sur prix (nombre de shares pour dépenser order_size_usdc)
            bid_size = round(order_size_usdc / bid_price, 2)
            ask_size = round(order_size_usdc / ask_price, 2)

            # ── Lecture de la position actuelle pour éviter BUY+SELL simultané ──
            qty_held = self.db.get_position(market.yes_token_id) if self.db else 0.0
            # Ratio d'inventaire : cohérent avec RiskManager (5% du solde)
            max_exposure = max(balance * 0.05, self.max_order_size_usdc)
            net_exposure_usdc = qty_held * mid
            inv_ratio = net_exposure_usdc / max_exposure if max_exposure > 0 else 0.0

            # Décision : quelle(s) face(s) coter ce cycle
            # - Pas de position → BUY uniquement (rien à vendre)
            # - Position faible (< 70%) → BUY + SELL (market making normal)
            # - Position élevée (≥ 70%) → SELL uniquement (réduction d'inventaire)
            emit_buy  = inv_ratio < 0.70
            emit_sell = qty_held > 0.01   # besoin d'au moins 0.01 shares pour vendre

            logger.info(
                "[OBI] '%s' | mid=%.4f | OBI=%.3f (%s) | bid=%.4f ask=%.4f "
                "| qty_held=%.2f inv_ratio=%.2f → buy=%s sell=%s",
                market.question[:50], mid, obi_result.obi, obi_result.regime,
                bid_price, ask_price, qty_held, inv_ratio,
                emit_buy, emit_sell,
            )

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
                    reason=f"OBI={obi_result.obi:.3f} régime={obi_result.regime}",
                ))
            if emit_sell:
                signals.append(Signal(
                    token_id=market.yes_token_id,
                    market_id=market.market_id,
                    market_question=market.question,
                    side="sell",
                    order_type="limit",
                    price=ask_price,
                    size=min(ask_size, qty_held),  # ne jamais vendre plus que détenu
                    confidence=min(abs(obi_result.obi) + 0.5, 1.0),
                    reason=f"OBI={obi_result.obi:.3f} régime={obi_result.regime}",
                ))
            traded += 1

        logger.info("[OBI] Analyse terminée: %d marchés, %d signaux générés.",
                    traded, len(signals))
        return signals

    def _compute_quotes(self, mid: float, spread: float,
                        obi: OBIResult) -> tuple[float, float]:
        """
        Calcule bid et ask en fonction du régime OBI.
        Arrondi au tick (0.01).
        """
        half = spread / 2.0

        if obi.regime == "neutral":
            # Symétrique
            bid = mid - half
            ask = mid + half

        elif obi.regime == "bullish":
            # Bid agressif (top of book = mid), ask passif (+2 ticks)
            bid = mid
            ask = mid + 2 * TICK_SIZE

        else:  # bearish
            # Bid passif, ask agressif
            bid = mid - 2 * TICK_SIZE
            ask = mid

        # Arrondi au tick
        bid = round(round(bid / TICK_SIZE) * TICK_SIZE, 4)
        ask = round(round(ask / TICK_SIZE) * TICK_SIZE, 4)

        # Garantir l'écart minimal
        if ask - bid < TICK_SIZE:
            ask = bid + TICK_SIZE

        return bid, ask

    def _is_news_event(self, token_id: str, current_mid: float) -> bool:
        """
        Retourne True si le mid-price a bougé de > 0.10 en moins de 5 secondes.
        """
        now = time.time()
        if token_id in self._price_history:
            prev_price, prev_ts = self._price_history[token_id]
            if (now - prev_ts) < 5.0:
                if abs(current_mid - prev_price) > 0.10:
                    return True
        return False

    def _update_price_history(self, token_id: str, mid: float):
        """Enregistre le prix actuel pour la détection news-breaker."""
        self._price_history[token_id] = (mid, time.time())


# ─── DummyStrategy (conservée pour tests) ────────────────────────────────────

class DummyStrategy(BaseStrategy):
    """
    Stratégie factice : scan et log les marchés CLOB, ne passe aucun ordre.
    Utiliser OBIMarketMakingStrategy pour le trading réel.
    """

    def analyze(self) -> list[Signal]:
        universe = MarketUniverse()
        markets = universe.get_eligible_markets()

        if not markets:
            logger.info("[Dummy] Aucun marché éligible trouvé.")
            return []

        logger.info("[Dummy] %d marchés éligibles:", len(markets))
        for m in markets[:5]:
            logger.info(
                "[Dummy]  '%s' | mid=%.4f | spread=%.4f | vol24h=%.0f | %.1fd",
                m.question[:55], m.mid_price, m.spread, m.volume_24h, m.days_to_expiry,
            )
        return []
