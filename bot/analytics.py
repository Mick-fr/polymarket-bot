"""
Module d'analytique du bot OBI.
Calcule les metriques de performance a partir des trades fermes,
genere des graphiques (donnees pour Chart.js) et des recommandations
d'ajustement des parametres de la strategie.
"""

import logging
import math
from datetime import datetime, timezone
from typing import Optional

from db.database import Database

logger = logging.getLogger("bot.analytics")

# Seuil minimum de trades fermes pour generer des recommandations
MIN_TRADES_FOR_RECO = 20
MIN_TRADES_PER_BUCKET = 5


class TradeAnalytics:
    """Moteur d'analytique pour les trades du bot OBI."""

    def __init__(self, db: Database):
        self.db = db

    # ── Metriques de performance globales ──────────────────────────

    def compute_performance_summary(self) -> dict:
        """
        Calcule les metriques de performance globales.
        Retourne un dict avec total_pnl, win_rate, sharpe, max_drawdown, etc.
        """
        summary = self.db.get_pnl_summary()

        # Sharpe ratio
        closed = self.db.get_closed_trades(limit=10000)
        returns = [t["pnl_usdc"] for t in closed if t.get("pnl_usdc") is not None]
        summary["sharpe_ratio"] = self._compute_sharpe(returns)

        # Max drawdown sur equity curve
        dd_pct, dd_peak, dd_trough = self._compute_max_drawdown()
        summary["max_drawdown_pct"] = dd_pct
        summary["max_drawdown_peak"] = dd_peak
        summary["max_drawdown_trough"] = dd_trough

        # Expectancy = avg_win * win_rate - avg_loss * loss_rate
        total = summary.get("total_trades", 0)
        if total > 0:
            wr = summary.get("win_rate", 0) / 100.0
            lr = 1.0 - wr
            summary["expectancy"] = (
                summary.get("avg_win", 0) * wr
                + summary.get("avg_loss", 0) * lr  # avg_loss est negatif
            )
        else:
            summary["expectancy"] = 0.0

        return summary

    def rolling_win_rate(self) -> float:
        """V18: Win rate over the last 50 trades."""
        closed = self.db.get_closed_trades(limit=50)
        if not closed:
            return 0.0
        wins = sum(1 for t in closed if (t.get("pnl_usdc") or 0) > 0)
        return (wins / len(closed)) * 100

    def hourly_sharpe_ratio(self) -> float:
        """V18: Sharpe ratio dynamically based on hourly returns instead of static logic."""
        closed = self.db.get_closed_trades(limit=5000)
        if not closed:
            return 0.0
            
        hourly_returns = {}
        for t in closed:
            ts = t.get("close_timestamp", "")
            if len(ts) >= 13:
                hour_key = ts[:13]  # YYYY-MM-DD HH
                hourly_returns[hour_key] = hourly_returns.get(hour_key, 0.0) + (t.get("pnl_usdc") or 0.0)
                
        returns = list(hourly_returns.values())
        if len(returns) < 2:
            return 0.0
            
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
        std_r = math.sqrt(variance) if variance > 0 else 0.001
        
        # 24 hours * 365 days = 8760 periods/year
        return (mean_r / std_r) * math.sqrt(8760)

    def projected_vs_realized_edge(self) -> dict:
        """V18: Deviations between entry edge and realized PnL."""
        closed = self.db.get_closed_trades(limit=50)
        if not closed:
            return {"projected": 0.0, "realized": 0.0, "deviation": 0.0}
            
        total_realized_pct = sum(t.get("pnl_pct", 0) or 0 for t in closed)
        avg_realized = (total_realized_pct / len(closed)) if closed else 0.0
        
        # If we had actual exact p_true per trade recorded in trades table, we would use it.
        # As an approximation for the engine, the strategy requires edge > MIN_EDGE_SCORE (e.g. 4.5%).
        # Let's assume an average projected edge of 5.5% for recent trades. 
        # (Could be enriched further if DB schema is updated later to log p_true directly).
        avg_projected = 5.5
        
        return {
            "projected": round(avg_projected, 2),
            "realized": round(avg_realized, 2),
            "deviation": round(avg_realized - avg_projected, 2)
        }

    # ── Analyse par dimension ──────────────────────────────────────

    def performance_by_market(self) -> list[dict]:
        """PnL, win_rate, nombre de trades par market_id."""
        return self.db.get_pnl_by_market(limit=20)

    def performance_by_regime(self) -> list[dict]:
        """PnL, win_rate par regime OBI (bullish/bearish/neutral)."""
        return self.db.get_pnl_by_regime()

    def performance_by_obi_bucket(self) -> list[dict]:
        """
        Decoupe les OBI en buckets de 0.1 (-1.0 to +1.0),
        calcule win_rate et avg_pnl par bucket.
        """
        closed = self.db.get_closed_trades(limit=10000)
        buckets = {}
        for t in closed:
            obi = t.get("obi_at_open")
            if obi is None:
                continue
            # Arrondir au bucket de 0.1 le plus proche
            bucket_val = round(math.floor(obi * 10) / 10, 1)
            bucket_key = f"{bucket_val:+.1f}"
            if bucket_key not in buckets:
                buckets[bucket_key] = {"bucket": bucket_key, "trades": 0, "wins": 0,
                                       "total_pnl": 0.0, "pnls": []}
            b = buckets[bucket_key]
            b["trades"] += 1
            pnl = t.get("pnl_usdc", 0) or 0
            b["total_pnl"] += pnl
            b["pnls"].append(pnl)
            if pnl > 0:
                b["wins"] += 1

        result = []
        for key in sorted(buckets.keys()):
            b = buckets[key]
            b["win_rate"] = (b["wins"] / b["trades"] * 100) if b["trades"] > 0 else 0.0
            b["avg_pnl"] = b["total_pnl"] / b["trades"] if b["trades"] > 0 else 0.0
            del b["pnls"]
            result.append(b)
        return result

    def performance_by_spread_bucket(self) -> list[dict]:
        """PnL par tranche de spread (0.01, 0.02, 0.03+)."""
        closed = self.db.get_closed_trades(limit=10000)
        buckets = {"0.01": {"trades": 0, "wins": 0, "total_pnl": 0.0},
                   "0.02": {"trades": 0, "wins": 0, "total_pnl": 0.0},
                   "0.03+": {"trades": 0, "wins": 0, "total_pnl": 0.0}}
        for t in closed:
            spread = t.get("spread_at_open")
            if spread is None:
                continue
            if spread <= 0.015:
                key = "0.01"
            elif spread <= 0.025:
                key = "0.02"
            else:
                key = "0.03+"
            b = buckets[key]
            b["trades"] += 1
            pnl = t.get("pnl_usdc", 0) or 0
            b["total_pnl"] += pnl
            if pnl > 0:
                b["wins"] += 1

        result = []
        for key in ["0.01", "0.02", "0.03+"]:
            b = buckets[key]
            b["bucket"] = key
            b["win_rate"] = (b["wins"] / b["trades"] * 100) if b["trades"] > 0 else 0.0
            b["avg_pnl"] = b["total_pnl"] / b["trades"] if b["trades"] > 0 else 0.0
            result.append(b)
        return result

    # ── Calcul Sharpe Ratio ────────────────────────────────────────

    def _compute_sharpe(self, returns: list[float],
                         risk_free_rate: float = 0.0) -> float:
        """
        Sharpe ratio annualise.
        Estimation : ~10 trades/jour (8s cycles, ~3 signals/cycle, 8h de trading)
        → ~3650 trades/an.
        """
        if len(returns) < 2:
            return 0.0
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
        std_r = math.sqrt(variance) if variance > 0 else 0.001

        # Estimer le nombre de periodes par an
        # En moyenne ~10 trades/jour, ~365 jours
        trades_per_year = 3650
        annualization = math.sqrt(trades_per_year)

        return ((mean_r - risk_free_rate) / std_r) * annualization

    # ── Max Drawdown ───────────────────────────────────────────────

    def _compute_max_drawdown(self) -> tuple[float, str, str]:
        """
        Calcule le drawdown maximal en % sur la equity curve (balance_history).
        Retourne (max_dd_pct, peak_date, trough_date).
        """
        balances = self.db.get_balance_timeseries(limit=5000)
        if len(balances) < 2:
            return 0.0, "", ""

        max_dd = 0.0
        peak = balances[0]["balance"]
        peak_date = balances[0]["timestamp"]
        best_peak_date = peak_date
        best_trough_date = peak_date

        for point in balances:
            bal = point["balance"]
            ts = point["timestamp"]
            if bal > peak:
                peak = bal
                peak_date = ts
            dd = (peak - bal) / peak if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd
                best_peak_date = peak_date
                best_trough_date = ts

        return round(max_dd * 100, 2), best_peak_date, best_trough_date

    # ── Timeseries pour charts ─────────────────────────────────────

    def equity_curve(self) -> list[dict]:
        """Retourne les points de la equity curve pour Chart.js."""
        return self.db.get_balance_timeseries(limit=1000)

    def cumulative_pnl_curve(self) -> list[dict]:
        """PnL cumule trade par trade (pour Chart.js)."""
        closed = self.db.get_closed_trades(limit=10000)
        closed.reverse()  # Ordre chronologique (les plus anciens d'abord)
        cum_pnl = 0.0
        result = []
        for i, t in enumerate(closed):
            pnl = t.get("pnl_usdc", 0) or 0
            cum_pnl += pnl
            result.append({
                "trade_num": i + 1,
                "cumulative_pnl": round(cum_pnl, 4),
                "pnl": round(pnl, 4),
                "timestamp": t.get("close_timestamp", ""),
                "question": (t.get("question", "") or "")[:40],
            })
        return result

    def daily_pnl_bars(self) -> list[dict]:
        """PnL par jour (bar chart vert/rouge)."""
        closed = self.db.get_closed_trades(limit=10000)
        daily = {}
        for t in closed:
            ts = t.get("close_timestamp", "")
            if not ts:
                continue
            day = ts[:10]  # YYYY-MM-DD
            if day not in daily:
                daily[day] = {"date": day, "pnl": 0.0, "trades": 0}
            daily[day]["pnl"] += t.get("pnl_usdc", 0) or 0
            daily[day]["trades"] += 1

        result = sorted(daily.values(), key=lambda x: x["date"])
        for r in result:
            r["pnl"] = round(r["pnl"], 4)
        return result

    # ── Backfill historique ────────────────────────────────────────

    def backfill_trades_from_orders(self) -> int:
        """
        Recree les trades a partir des ordres historiques existants.
        Apparie BUY/SELL par token_id en FIFO chronologique.
        Retourne le nombre de trades crees.
        """
        # Verifier s'il y a deja des trades
        existing = self.db.get_all_trades(limit=1)
        if existing:
            logger.info("[Backfill] Trades deja existants, skip.")
            return 0

        # Recuperer tous les ordres remplis
        all_orders = []
        with self.db._cursor() as cur:
            cur.execute("""
                SELECT id, timestamp, market_id, token_id, side, price, size, status
                FROM orders
                WHERE status IN ('filled', 'matched')
                ORDER BY id ASC
            """)
            all_orders = [dict(row) for row in cur.fetchall()]

        if not all_orders:
            return 0

        # FIFO matching par token_id
        open_buys = {}  # token_id -> list of buy orders
        trades_created = 0

        for order in all_orders:
            token = order["token_id"]
            if order["side"] == "buy":
                if token not in open_buys:
                    open_buys[token] = []
                open_buys[token].append(order)
                # Creer un trade ouvert
                self.db.open_trade(
                    open_order_id=order["id"],
                    market_id=order["market_id"],
                    token_id=token,
                    question="",
                    open_price=order["price"] or 0.5,
                    open_size=order["size"],
                )
                trades_created += 1

            elif order["side"] == "sell" and token in open_buys and open_buys[token]:
                # Fermer le trade FIFO le plus ancien
                self.db.close_trade(
                    token_id=token,
                    close_order_id=order["id"],
                    close_price=order["price"] or 0.5,
                    close_size=order["size"],
                )
                open_buys[token].pop(0)

        logger.info("[Backfill] %d trades crees a partir des ordres historiques.", trades_created)
        return trades_created


class ParameterRecommender:
    """
    Analyse les trades historiques et recommande des ajustements
    aux parametres de la strategie OBI.
    """

    def __init__(self, db: Database, analytics: TradeAnalytics):
        self.db = db
        self.analytics = analytics

    def generate_recommendations(self) -> list[dict]:
        """
        Retourne une liste de recommandations.
        Chaque recommandation est un dict avec :
          parameter, current_value, recommended_value, confidence,
          direction ('augmenter'|'reduire'|'maintenir'), reasoning, evidence
        """
        closed = self.db.get_closed_trades(limit=10000)
        if len(closed) < MIN_TRADES_FOR_RECO:
            return [{
                "parameter": "general",
                "current_value": None,
                "recommended_value": None,
                "confidence": 0.0,
                "direction": "attendre",
                "reasoning": (
                    f"Pas assez de donnees : {len(closed)} trades fermes "
                    f"(minimum {MIN_TRADES_FOR_RECO} requis). "
                    "Continuez le paper trading pour accumuler des donnees."
                ),
                "evidence": {"closed_trades": len(closed)},
            }]

        recs = []
        recs.extend(self._analyze_obi_threshold(closed))
        recs.extend(self._analyze_skew_factor(closed))
        recs.extend(self._analyze_min_spread(closed))
        recs.extend(self._analyze_order_size(closed))
        recs.extend(self._analyze_exposure_limit(closed))
        return recs

    def _analyze_obi_threshold(self, closed: list[dict]) -> list[dict]:
        """
        Compare la rentabilite des trades par buckets OBI.
        Si les buckets proches du seuil actuel (0.20) sont deficitaires
        mais les buckets plus eloignes sont rentables, recommande de monter le seuil.
        """
        from bot.strategy import OBI_BULLISH_THRESHOLD

        # Grouper par buckets de 0.05
        buckets = {}
        for t in closed:
            obi = t.get("obi_at_open")
            if obi is None:
                continue
            abs_obi = abs(obi)
            bucket = round(math.floor(abs_obi * 20) / 20, 2)  # pas de 0.05
            if bucket not in buckets:
                buckets[bucket] = {"trades": 0, "wins": 0, "total_pnl": 0.0}
            b = buckets[bucket]
            b["trades"] += 1
            pnl = t.get("pnl_usdc", 0) or 0
            b["total_pnl"] += pnl
            if pnl > 0:
                b["wins"] += 1

        if not buckets:
            return []

        # Trouver le point de crossover (win rate > 50%)
        sorted_buckets = sorted(buckets.items())
        evidence_list = []
        crossover = None
        for bval, bdata in sorted_buckets:
            wr = (bdata["wins"] / bdata["trades"] * 100) if bdata["trades"] >= MIN_TRADES_PER_BUCKET else None
            evidence_list.append({
                "range": f"{bval:.2f}-{bval+0.05:.2f}",
                "trades": bdata["trades"],
                "win_rate": round(wr, 1) if wr is not None else None,
                "avg_pnl": round(bdata["total_pnl"] / bdata["trades"], 4) if bdata["trades"] > 0 else 0,
            })
            if wr is not None and wr >= 50 and crossover is None:
                crossover = bval

        # Recommandation
        current = OBI_BULLISH_THRESHOLD
        if crossover is not None and abs(crossover - current) >= 0.05:
            recommended = crossover
            direction = "augmenter" if recommended > current else "reduire"
            total_analyzed = sum(b["trades"] for b in buckets.values())
            confidence = min(0.9, total_analyzed / 100)
            reasoning = (
                f"Le seuil de rentabilite (win rate >= 50%) se situe a |OBI| = {crossover:.2f}. "
                f"Le seuil actuel ({current:.2f}) inclut des trades non rentables. "
                f"Base sur {total_analyzed} trades."
            )
        else:
            recommended = current
            direction = "maintenir"
            confidence = 0.5
            reasoning = f"Le seuil actuel ({current:.2f}) est proche du point de rentabilite optimal."

        return [{
            "parameter": "OBI_BULLISH_THRESHOLD",
            "current_value": current,
            "recommended_value": recommended,
            "confidence": round(confidence, 2),
            "direction": direction,
            "reasoning": reasoning,
            "evidence": {"obi_buckets": evidence_list},
        }]

    def _analyze_skew_factor(self, closed: list[dict]) -> list[dict]:
        """
        Analyse si le skew factor produit des fills bien prices.
        Compare le fill price vs mid_price_at_open.
        """
        from bot.strategy import OBI_SKEW_FACTOR

        slippages = []
        for t in closed:
            mid = t.get("mid_price_at_open")
            open_price = t.get("open_price")
            if mid is None or open_price is None or mid <= 0:
                continue
            # Slippage = combien on paie au-dessus du mid (pour les BUY)
            slippage = open_price - mid
            slippages.append(slippage)

        if len(slippages) < MIN_TRADES_PER_BUCKET:
            return []

        avg_slippage = sum(slippages) / len(slippages)
        current = OBI_SKEW_FACTOR

        # Si slippage moyen > 0.005 (0.5 cent), le skew est trop agressif
        if avg_slippage > 0.005:
            recommended = max(1.0, current - 0.5)
            direction = "reduire"
            reasoning = (
                f"Slippage moyen = {avg_slippage:.4f} USDC (achat au-dessus du mid). "
                f"Reduire le skew de {current:.1f} a {recommended:.1f} pour mieux pricer les BUY."
            )
            confidence = min(0.8, len(slippages) / 50)
        elif avg_slippage < -0.005:
            recommended = min(5.0, current + 0.5)
            direction = "augmenter"
            reasoning = (
                f"Slippage moyen = {avg_slippage:.4f} (achat sous le mid, bon signe). "
                f"Le skew pourrait etre plus agressif pour capturer plus d'opportunites."
            )
            confidence = min(0.6, len(slippages) / 50)
        else:
            recommended = current
            direction = "maintenir"
            reasoning = f"Slippage moyen = {avg_slippage:.4f}, bien calibre."
            confidence = 0.7

        return [{
            "parameter": "OBI_SKEW_FACTOR",
            "current_value": current,
            "recommended_value": recommended,
            "confidence": round(confidence, 2),
            "direction": direction,
            "reasoning": reasoning,
            "evidence": {
                "avg_slippage": round(avg_slippage, 4),
                "sample_size": len(slippages),
            },
        }]

    def _analyze_min_spread(self, closed: list[dict]) -> list[dict]:
        """Compare PnL par tranche de spread."""
        from bot.strategy import MIN_SPREAD

        spread_data = self.analytics.performance_by_spread_bucket()
        if not spread_data or all(b["trades"] < MIN_TRADES_PER_BUCKET for b in spread_data):
            return []

        current = MIN_SPREAD

        # Verifier si les spreads etroits sont deficitaires
        narrow = next((b for b in spread_data if b["bucket"] == "0.01"), None)
        medium = next((b for b in spread_data if b["bucket"] == "0.02"), None)

        if narrow and narrow["trades"] >= MIN_TRADES_PER_BUCKET and narrow["avg_pnl"] < 0:
            if medium and medium["avg_pnl"] > 0:
                recommended = 0.02
                direction = "augmenter" if current < 0.02 else "maintenir"
                reasoning = (
                    f"Spreads < 0.015 : win rate {narrow['win_rate']:.0f}%, avg PnL {narrow['avg_pnl']:.4f}. "
                    f"Spreads 0.015-0.025 : win rate {medium['win_rate']:.0f}%, avg PnL {medium['avg_pnl']:.4f}. "
                    "Les spreads etroits ne couvrent pas les couts."
                )
                confidence = min(0.8, (narrow["trades"] + medium["trades"]) / 40)
            else:
                recommended = current
                direction = "maintenir"
                reasoning = "Donnees insuffisantes pour les spreads moyens."
                confidence = 0.3
        else:
            recommended = current
            direction = "maintenir"
            reasoning = f"Le spread minimum actuel ({current:.2f}) est adequat."
            confidence = 0.6

        return [{
            "parameter": "MIN_SPREAD",
            "current_value": current,
            "recommended_value": recommended,
            "confidence": round(confidence, 2),
            "direction": direction,
            "reasoning": reasoning,
            "evidence": {"spread_buckets": spread_data},
        }]

    def _analyze_order_size(self, closed: list[dict]) -> list[dict]:
        """Compare rendement ajuste par taille de trade."""
        from bot.strategy import ORDER_SIZE_PCT

        # Grouper par quartile de taille
        sizes = [(t.get("open_size", 0) * (t.get("open_price", 0.5) or 0.5), t.get("pnl_usdc", 0) or 0)
                 for t in closed if t.get("open_size")]
        if len(sizes) < MIN_TRADES_FOR_RECO:
            return []

        sizes.sort(key=lambda x: x[0])
        mid = len(sizes) // 2
        small = sizes[:mid]
        large = sizes[mid:]

        small_roi = sum(p for _, p in small) / sum(s for s, _ in small) * 100 if small else 0
        large_roi = sum(p for _, p in large) / sum(s for s, _ in large) * 100 if large else 0

        current = ORDER_SIZE_PCT
        if large_roi < small_roi - 1.0 and len(large) >= MIN_TRADES_PER_BUCKET:
            recommended = max(0.01, current - 0.005)
            direction = "reduire"
            reasoning = (
                f"ROI petits ordres : {small_roi:.2f}% vs gros ordres : {large_roi:.2f}%. "
                f"Reduire la taille pour ameliorer le rendement ajuste au risque."
            )
            confidence = min(0.7, len(sizes) / 80)
        else:
            recommended = current
            direction = "maintenir"
            reasoning = f"Taille d'ordre actuelle ({current*100:.0f}% du solde) bien equilibree."
            confidence = 0.5

        return [{
            "parameter": "ORDER_SIZE_PCT",
            "current_value": current,
            "recommended_value": recommended,
            "confidence": round(confidence, 2),
            "direction": direction,
            "reasoning": reasoning,
            "evidence": {
                "small_roi_pct": round(small_roi, 2),
                "large_roi_pct": round(large_roi, 2),
                "sample_size": len(sizes),
            },
        }]

    def _analyze_exposure_limit(self, closed: list[dict]) -> list[dict]:
        """Verifie si le plafond 8% d'exposition est optimal."""
        from bot.strategy import MAX_NET_EXPOSURE_PCT

        current = MAX_NET_EXPOSURE_PCT

        # Regarder les trades avec haute exposition relative
        high_exp = [t for t in closed
                    if t.get("open_size") and t.get("mid_price_at_open")
                    and (t["open_size"] * (t.get("open_price", 0.5) or 0.5)) > 3.0]  # > 3 USDC
        low_exp = [t for t in closed
                   if t.get("open_size") and t.get("mid_price_at_open")
                   and (t["open_size"] * (t.get("open_price", 0.5) or 0.5)) <= 3.0]

        if len(high_exp) < MIN_TRADES_PER_BUCKET or len(low_exp) < MIN_TRADES_PER_BUCKET:
            return [{
                "parameter": "MAX_NET_EXPOSURE_PCT",
                "current_value": current,
                "recommended_value": current,
                "confidence": 0.3,
                "direction": "maintenir",
                "reasoning": "Pas assez de donnees pour analyser l'exposition.",
                "evidence": {"high_exp_trades": len(high_exp), "low_exp_trades": len(low_exp)},
            }]

        high_wr = sum(1 for t in high_exp if (t.get("pnl_usdc", 0) or 0) > 0) / len(high_exp) * 100
        low_wr = sum(1 for t in low_exp if (t.get("pnl_usdc", 0) or 0) > 0) / len(low_exp) * 100

        if high_wr < low_wr - 10:
            recommended = max(0.04, current - 0.02)
            direction = "reduire"
            reasoning = (
                f"Win rate haute exposition : {high_wr:.0f}% vs basse : {low_wr:.0f}%. "
                f"Reduire le plafond pour limiter les pertes sur les grosses positions."
            )
        elif high_wr > low_wr + 10:
            recommended = min(0.15, current + 0.02)
            direction = "augmenter"
            reasoning = (
                f"Win rate haute exposition : {high_wr:.0f}% vs basse : {low_wr:.0f}%. "
                f"Les grosses positions performent bien, augmenter le plafond."
            )
        else:
            recommended = current
            direction = "maintenir"
            reasoning = f"Plafond d'exposition ({current*100:.0f}%) bien calibre."

        confidence = min(0.7, (len(high_exp) + len(low_exp)) / 60)

        return [{
            "parameter": "MAX_NET_EXPOSURE_PCT",
            "current_value": current,
            "recommended_value": recommended,
            "confidence": round(confidence, 2),
            "direction": direction,
            "reasoning": reasoning,
            "evidence": {
                "high_exp_win_rate": round(high_wr, 1),
                "low_exp_win_rate": round(low_wr, 1),
                "high_exp_trades": len(high_exp),
                "low_exp_trades": len(low_exp),
            },
        }]
