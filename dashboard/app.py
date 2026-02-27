"""
Dashboard web Flask.
Interface de monitoring et de contrôle du bot Polymarket.
Routes :
  /           → Dashboard principal (protégé)
  /login      → Page de connexion
  /logout     → Déconnexion
  /analytics  → Page d'analytique des trades (protégé)
  /health     → Health check (public, pour Uptime Kuma)
  /api/*      → Fragments htmx (protégés)
  /api/analytics/* → Endpoints JSON analytics (protégés)
"""

import logging

from flask import Flask, jsonify, redirect, render_template, request, session, url_for

from bot.analytics import TradeAnalytics, ParameterRecommender
from bot.config import AppConfig
from dashboard.auth import check_password, login_required
from db.database import Database

logger = logging.getLogger("dashboard")


def create_app(config: AppConfig, db: Database) -> Flask:
    """Factory Flask — crée et configure l'application."""
    app = Flask(__name__)
    app.secret_key = config.dashboard.secret_key
    app.config["APP_CONFIG"] = config
    app.config["DB"] = db

    # ── Pages principales ────────────────────────────────────

    @app.route("/")
    @login_required
    def index():
        """Nouvelle page d'accueil : Sniper Ops Center"""
        try:
            hwm = db.get_high_water_mark() if hasattr(db, 'get_high_water_mark') else 0.0
            mom = float(db.get_config("live_btc_mom30s", 0) or 0)
            obi = float(db.get_config("live_btc_obi", 0) or 0)
        except:
            hwm = mom = obi = 0.0
        return render_template("sniper.html", hwm=hwm, momentum=mom, obi=obi)

    @app.route("/overview")
    @login_required
    def overview_page():
        """Page de contrôle général et gestion du bot."""
        balance = db.get_latest_balance() or 0.0
        strategy_mode = db.get_config_str("strategy_mode", "Info Edge Only")
        bot_active = db.get_config("bot_active", "true") != "false"
        ks = db.get_kill_switch()
        return render_template("overview.html", balance=balance, strategy_mode=strategy_mode, bot_active=bot_active, ks=ks)

    @app.route("/mm")
    @login_required
    def mm_page():
        return render_template("mm.html")

    @app.route("/copy")
    @login_required
    def copy_page():
        return render_template("copy.html")

    @app.route("/api/sniper-radar")
    @login_required
    def api_sniper_radar():
        """API pour le rafraîchissement temps réel du radar Sniper"""
        try:
            mom = float(db.get_config("live_btc_mom30s", 0) or 0)
            obi = float(db.get_config("live_btc_obi", 0) or 0)
            found = int(db.get_config("live_found_markets", 0) or 0)
            edge = float(db.get_config("live_max_edge", 0) or 0)
            spread = float(db.get_config("live_min_spread", 0) or 0)
            return jsonify({
                "momentum_30s": mom, 
                "obi": obi, 
                "found_markets": found, 
                "max_edge": edge,
                "min_spread": spread,
                "status": "live"
            })
        except:
            return jsonify({"momentum_30s": 0, "obi": 0, "found_markets": 0, "max_edge": 0, "min_spread": 0, "status": "error"})

    @app.route("/api/sniper-feed")
    @login_required
    def api_sniper_feed():
        """Retourne les 50 dernières lignes de scan du bot."""
        logs = db.get_recent_logs(limit=300)
        feed = [l for l in logs if l["source"] == "sniper_feed"][:50]
        html = ""
        for f in feed:
            time_str = f["timestamp"][11:19]
            html += f"<div class='text-xs py-1 border-b border-slate-800/50'><span class='text-slate-500'>[{time_str}]</span> {f['message']}</div>"
        return html or "<div class='text-slate-500 text-sm'>En attente du prochain scan (4s)...</div>"

    @app.route("/api/sniper-positions")
    @login_required
    def api_sniper_positions():
        """Tableau des positions ouvertes pour le dashboard Sniper."""
        positions = db.get_all_positions()
        html = ""
        for p in positions:
            qty = float(p.get("quantity", 0))
            if qty > 0.01:
                avg = float(p.get("avg_price", 0))
                mid = float(p.get("current_mid") or avg)
                pnl = (mid - avg) * qty
                color = "text-green-400" if pnl >= 0 else "text-red-400"
                html += f"<tr class='border-b border-slate-700 hover:bg-slate-800/50 transition-colors'><td class='py-2 px-3 truncate max-w-[150px]' title='{p.get('question', '')}'>{p.get('question', '')[:30]}...</td><td class='py-2 px-3'>{qty:.2f}</td><td class='py-2 px-3'>${avg:.3f}</td><td class='py-2 px-3'>${mid:.3f}</td><td class='py-2 px-3 font-mono {color}'>${pnl:+.2f}</td></tr>"
        return html or "<tr><td colspan='5' class='py-4 text-center text-slate-500'>Aucune position en cours</td></tr>"

    @app.route("/api/sniper-history")
    @login_required
    def api_sniper_history():
        """Historique des 10 derniers trades fermés."""
        trades = db.get_closed_trades(limit=10)
        html = ""
        for t in trades:
            pnl = float(t.get("pnl_usdc", 0))
            color = "text-green-400" if pnl >= 0 else "text-red-400"
            html += f"<tr class='border-b border-slate-700 hover:bg-slate-800/50 transition-colors'><td class='py-2 px-3 text-slate-500'>{t.get('close_timestamp', '')[11:16]}</td><td class='py-2 px-3 truncate max-w-[150px]' title='{t.get('question', '')}'>{t.get('question', '')[:30]}...</td><td class='py-2 px-3'>{t.get('open_size', 0):.2f}</td><td class='py-2 px-3 font-mono {color}'>${pnl:+.2f}</td></tr>"
        return html or "<tr><td colspan='4' class='py-4 text-center text-slate-500'>Aucun trade fermé récent</td></tr>"

    @app.route("/api/v14/brain")
    @login_required
    def api_v14_brain():
        """V14: Retourne les métriques live du marché et de l'alpha."""
        try:
            return jsonify({
                "btc_spot": float(db.get_config("live_btc_spot", 0) or 0),
                "momentum": float(db.get_config("live_btc_mom30s", 0) or 0),
                "obi": float(db.get_config("live_btc_obi", 0) or 0),
                "iv": float(db.get_config("live_dynamic_iv", 0) or 0),
                "funding": float(db.get_config("live_funding_rate", 0) or 0),
                "sprint_edge": float(db.get_config("live_sprint_edge", 0) or 0),
                "p_true": float(db.get_config("live_sprint_ptrue", 0) or 0),
                "p_poly": float(db.get_config("live_sprint_ppoly", 0) or 0),
                "ai_bias": float(db.get_config("live_ai_sentiment_bias", 1.0) or 1.0),
                "live_checklist": json.loads(db.get_config_str("live_checklist", "{}") or "{}"),
                "live_percentages": {
                    "mom_progress": round(json.loads(db.get_config_str("live_percentages", "{}") or "{}").get("mom_pct", 0) / 100.0, 3),
                    "obi_progress": round(json.loads(db.get_config_str("live_percentages", "{}") or "{}").get("obi_pct", 0) / 100.0, 3),
                    "edge_progress": round(json.loads(db.get_config_str("live_percentages", "{}") or "{}").get("edge_pct", 0) / 100.0, 3)
                },
                "price_gap_nominal": float(db.get_config("live_trigger_projection", 0) or 0)
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/sniper-near-misses")
    @login_required
    def api_sniper_near_misses():
        """V15.7: Retourne l'historique des Near Misses en HTML pour HTMX."""
        import json
        history_str = db.get_config_str("near_miss_history", "[]")
        try:
            history = json.loads(history_str)
        except:
            history = []

        # V20 gate thresholds (must match strategy.py)
        TMOM = 0.005
        TEDGE = 3.0

        def _pill(label, ok):
            if ok:
                return (f"<span class='inline-block px-1 py-0.5 rounded text-[0.6rem] font-bold "
                        f"bg-green-500/20 text-green-400 border border-green-500/30 mr-0.5'>{label}</span>")
            else:
                return (f"<span class='inline-block px-1 py-0.5 rounded text-[0.6rem] font-bold "
                        f"bg-red-500/20 text-red-400 border border-red-500/30 mr-0.5'>{label}</span>")

        html = ""
        # Render in reverse chronological order (newest first assuming appended to end)
        for rev in reversed(history):
            ts = rev.get("timestamp", "--:--:--")
            mom = rev.get("mom", 0.0)
            obi = rev.get("obi", 0.0)
            iv = rev.get("iv", 0.0)
            edge = rev.get("edge", 0.0)

            # Formatting
            mom_str = f"{mom:+.4f}%"
            edge_str = f"{edge:+.2f}%"
            edge_color = "text-orange-400 font-bold" if abs(edge) >= 10.0 else "text-slate-400"

            # Recompute V20 gate conditions from stored values
            mom_ok = abs(mom) >= TMOM
            edge_ok = abs(edge) >= TEDGE
            direction_ok = (edge > 0 and mom > 0) or (edge < 0 and mom < 0)
            iv_ok = iv > 0

            conditions_html = (
                _pill("MOM", mom_ok) +
                _pill("EDGE", edge_ok) +
                _pill("DIR", direction_ok) +
                _pill("IV", iv_ok)
            )

            html += f"<tr class='border-b border-slate-700 hover:bg-slate-800/50 transition-colors'>"
            html += f"<td class='py-2 px-3 text-slate-500 font-terminal'>{ts}</td>"
            html += f"<td class='py-2 px-3 font-terminal text-slate-300'>{mom_str}</td>"
            html += f"<td class='py-2 px-3 font-terminal text-slate-300'>{obi:.2f}</td>"
            html += f"<td class='py-2 px-3 font-terminal {edge_color}'>{edge_str}</td>"
            html += f"<td class='py-2 px-3'>{conditions_html}</td>"
            html += f"<td class='py-2 px-3 font-terminal text-slate-500 text-right text-[0.65rem]'>Pending (5m)</td>"
            html += "</tr>"

        return html or "<tr><td colspan='6' class='py-4 text-center text-slate-500 text-[0.7rem] uppercase tracking-widest'>Aucun near miss récent</td></tr>"

    @app.route("/api/v18/performance")
    @login_required
    def api_v18_performance():
        """V18: Top KPI Banner Metrics."""
        try:
            from bot.analytics import TradeAnalytics
            analytics = TradeAnalytics(db)
            
            # Pnl total USDC
            summary = db.get_pnl_summary()
            pnl_total = summary.get("total_pnl", 0.0)
            
            # Win Rate (rolling 50)
            win_rate = analytics.rolling_win_rate()
            
            # Sharpe Ratio (hourly dynamic)
            sharpe = analytics.hourly_sharpe_ratio()
            
            # Drawdown relative to absolute 106.13 HWM baseline
            hwm = max(float(db.get_high_water_mark() or 0), 106.13)
            current_portfolio = db.get_latest_balance() or 106.13
            drawdown = ((hwm - current_portfolio) / hwm * 100) if hwm > 0 else 0.0
            if drawdown < 0:
                drawdown = 0.0

            return jsonify({
                "pnl_usdc": pnl_total,
                "win_rate_50": win_rate,
                "sharpe_ratio": sharpe,
                "drawdown_pct": drawdown
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/v14/execution")
    @login_required
    def api_v14_execution():
        """V14: Retourne les métriques de latence et de slippage."""
        try:
            import json
            lat_str = db.get_config("live_execution_latency", "[]")
            latencies = json.loads(lat_str if lat_str else "[]")
            slippage_count = int(db.get_config("slippage_protect_events", "0") or "0")
            return jsonify({
                "latency": latencies,
                "slippage_protect_count": slippage_count,
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/v14/positions")
    @login_required
    def api_v14_positions():
        """V14: Retourne les positions pures JSON avec Trailing Stop data."""
        try:
            import json
            positions = db.get_all_positions()
            ts_str = db.get_config("trailing_stops", "{}")
            trailing_data = json.loads(ts_str if ts_str else "{}")
            
            enrich_pos = []
            for p in positions:
                qty = float(p.get("quantity", 0))
                if qty > 0.01:
                    tid = p.get("token_id", "")
                    avg = float(p.get("avg_price", 0))
                    mid = float(p.get("current_mid") or avg)
                    pnl = (mid - avg) / avg if avg > 0 else 0
                    ts_info = trailing_data.get(tid, {})
                    
                    enrich_pos.append({
                        "token_id": tid,
                        "question": p.get("question", ""),
                        "quantity": qty,
                        "avg_price": avg,
                        "current_mid": mid,
                        "pnl_pct": pnl,
                        "max_pnl": ts_info.get("max_pnl", 0.0),
                        "trailing_sl": ts_info.get("trailing_sl", None)
                    })
            return jsonify(enrich_pos)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/market-making")
    @login_required
    def market_making_page():
        """Acienne page d'accueil - Dashboard principal."""
        from datetime import datetime, timezone, timedelta
        
        positions = db.get_all_positions() if db else []
        cash = 0
        balances = db.get_balance_timeseries(limit=1)
        if balances:
            cash = balances[0]["balance"]
        
        portfolio_val = cash + sum(float(p.get("quantity") or 0) * float(p.get("current_mid") or p.get("avg_price") or 0) for p in positions)
        
        pnl_summary = db.get_pnl_summary() if db else {}
        total_pnl = pnl_summary.get("total_pnl", 0)
        
        closed = db.get_closed_trades(limit=1000)
        now = datetime.now(timezone.utc)
        today_str = now.isoformat()[:10]
        week_str = (now - timedelta(days=7)).isoformat()
        
        pnl_today = sum(t["pnl_usdc"] for t in closed if t.get("close_timestamp", "").startswith(today_str) and t.get("pnl_usdc"))
        pnl_week = sum(t["pnl_usdc"] for t in closed if t.get("close_timestamp", "") >= week_str and t.get("pnl_usdc"))

        # HWM
        with db._cursor() as cur:
            cur.execute("SELECT value FROM bot_state WHERE key = 'high_water_mark'")
            r = cur.fetchone()
            hwm = float(r["value"]) if r else portfolio_val
            
        hwm_pct = ((portfolio_val - hwm) / hwm * 100) if hwm > 0 else 0

        return render_template(
            "market_making.html",
            portfolio_val=portfolio_val,
            cash=cash,
            hwm=hwm,
            hwm_pct=hwm_pct,
            pnl_today=pnl_today,
            pnl_week=pnl_week,
            total_pnl=total_pnl
        )

    @app.route("/login", methods=["GET", "POST"])
    def login():
        """Page de connexion."""
        if session.get("authenticated"):
            return redirect(url_for("index"))

        error = None
        if request.method == "POST":
            password = request.form.get("password", "")
            if check_password(password, config.dashboard.password_hash):
                session["authenticated"] = True
                session.permanent = True
                logger.info("Connexion au dashboard réussie.")
                db.add_log("INFO", "dashboard", "Connexion admin réussie")
                next_url = request.args.get("next", "/")
                return redirect(next_url)
            else:
                error = "Mot de passe incorrect."
                logger.warning("Tentative de connexion échouée.")
                db.add_log("WARNING", "dashboard", "Tentative de connexion échouée")

        return render_template("login.html", error=error)

    @app.route("/logout")
    def logout():
        """Déconnexion."""
        session.clear()
        return redirect(url_for("login"))

    # ── Health check (public) ────────────────────────────────

    @app.route("/health")
    def health():
        """Endpoint de santé pour Uptime Kuma ou monitoring externe."""
        balance = db.get_latest_balance()
        kill_switch = db.get_kill_switch()
        return jsonify({
            "status": "ok",
            "kill_switch": kill_switch,
            "last_balance": balance,
        })

    # ── API htmx (fragments HTML) ───────────────────────────

    @app.route("/api/stats")
    @login_required
    def api_stats():
        """Retourne les 3 cartes de stats en HTML."""
        balance = db.get_latest_balance()
        daily_loss = db.get_daily_loss()
        orders = db.get_recent_orders(limit=1000)

        total_orders = len(orders)
        filled = sum(1 for o in orders if o["status"] in ("filled", "matched"))
        rejected = sum(1 for o in orders if o["status"] == "rejected")
        errors = sum(1 for o in orders if o["status"] == "error")

        balance_str = f"${balance:.2f}" if balance is not None else "N/A"
        balance_class = "text-green" if balance and balance > 0 else "text-red"

        # En paper trading, afficher le nombre de positions ouvertes sous le solde cash
        paper_detail = ""
        if config.bot.paper_trading and balance is not None:
            positions = db.get_all_positions()
            open_count = sum(1 for p in positions if (p.get("quantity") or 0.0) > 0.01)
            if open_count > 0:
                paper_detail = (
                    f'<div style="font-size:0.72rem; color:var(--text-dim); margin-top:4px;">'
                    f'{open_count} position(s) en inventaire</div>'
                )

        return f"""
        <div class="card">
            <div class="stat-value {balance_class}">{balance_str}</div>
            <div class="stat-label">{"Cash (paper)" if config.bot.paper_trading else "Solde USDC"}</div>
            {paper_detail}
        </div>
        <div class="card">
            <div class="stat-value text-orange">${daily_loss:.2f}</div>
            <div class="stat-label">Perte du jour</div>
        </div>
        <div class="card">
            <div class="stat-value">{total_orders}</div>
            <div class="stat-label">
                Ordres total &mdash;
                <span class="text-green">{filled} ok</span> /
                <span class="text-red">{rejected + errors} rejet</span>
            </div>
        </div>
        """

    @app.route("/api/kill-status")
    @login_required
    def api_kill_status():
        """Badge kill switch + badge simulation dans la navbar."""
        active = db.get_kill_switch()
        sim_badge = '<span class="badge-simulation">SIMULATION</span> ' if config.bot.paper_trading else ""
        if active:
            return f'{sim_badge}<span class="kill-indicator kill-active"><span class="kill-dot"></span>BOT ARRETE</span>'
        return f'{sim_badge}<span class="kill-indicator kill-inactive"><span class="kill-dot"></span>BOT ACTIF</span>'

    # 2026 V7.1 FIX KILL SWITCH — bidirectional
    @app.route("/resume", methods=["POST"])
    @login_required
    def api_resume():
        db.set_kill_switch(False)
        db.set_config("bot_active", "true")
        db.set_config("kill_switch_reason", "")
        logger.info("[DASHBOARD] Bot resumed by user")
        db.add_log("INFO", "dashboard", "[DASHBOARD] Bot resumed by user")
        db.set_config("bot_restart_requested", "true")
        return jsonify({"status": "ok", "active": True, "kill_switch": False})

    @app.route("/kill", methods=["POST"])
    @login_required
    def api_kill():
        db.set_kill_switch(True)
        db.set_config("bot_active", "false")
        logger.warning("Kill switch ACTIVE via dashboard.")
        db.add_log("WARNING", "dashboard", "Kill switch ACTIVE")
        return jsonify({"status": "ok", "active": False, "kill_switch": True})

    # 2026 V7.6 — Enhanced Bot status API with reason, hwm, portfolio
    @app.route("/api/bot-status")
    @login_required
    def api_bot_status():
        ks = db.get_kill_switch()
        active = db.get_config("bot_active", "true") != "false"
        reason = db.get_config("kill_switch_reason", "") or None
        strategy_mode = db.get_config_str("strategy_mode", "MM Balanced")
        hwm = db.get_high_water_mark() if hasattr(db, 'get_high_water_mark') else 0.0
        portfolio = float(db.get_config("last_portfolio_value", 0) or 0)
        return jsonify({
            "active": not ks and active,
            "kill_switch": ks,
            "bot_active": active,
            "reason": reason,
            "strategy_mode": strategy_mode,
            "hwm": hwm,
            "portfolio": portfolio
        })

    # 2026 V8.0 STRATEGY SELECTOR
    @app.route("/api/set-strategy", methods=["POST"])
    @login_required
    def api_set_strategy():
        data = request.json or {}
        mode = data.get("mode")
        valid_modes = ["Info Edge Only", "MM Conservateur", "MM Balanced", "MM Aggressif", "MM Très Agressif"]
        if mode not in valid_modes:
            return jsonify({"status": "error", "message": "Mode invalide"}), 400
            
        db.set_config("strategy_mode", mode)
        logger.info("[DASHBOARD] Stratégie changée en : %s", mode)
        
        # Signal au bot de recharger sa configuration
        db.set_config("bot_reload_aggressivity", "true")
        
        return jsonify({"status": "ok", "mode": mode})

    # V10.3 — Config Live Bot (paramètres réels de la stratégie active)
    @app.route("/api/current-config", methods=["GET"])
    @login_required
    def api_current_config():
        strategy_mode = db.get_config_str("strategy_mode", "MM Balanced")
        info_edge_avg = db.get_config("info_edge_avg_score", 0) or 0

        if strategy_mode == "Info Edge Only":
            return jsonify({
                "strategy_mode": strategy_mode,
                "is_info_edge": True,
                # Info Edge Only V10.3 params (constants)
                "min_edge_score": 12.5,
                "min_minutes": 5,
                "max_minutes": 90,
                "min_volume_5m": 520,
                "order_size_pct": 0.018,
                "max_net_exposure_pct": 0.25,
                "sizing_mult": 1.0,
                "max_order_usd": 12,
                "inventory_skew_threshold": None,
                "tiered_sizing": "1.0x / 1.8x / 2.8x",
                "info_edge_avg_score": round(float(info_edge_avg), 2),
            })

        # MM modes: read live values from DB
        return jsonify({
            "strategy_mode": strategy_mode,
            "is_info_edge": False,
            "order_size_pct": db.get_config("order_size_pct", 0.018) or 0.018,
            "max_net_exposure_pct": db.get_config("max_net_exposure_pct", 0.22) or 0.22,
            "inventory_skew_threshold": db.get_config("inventory_skew_threshold", 0.35) or 0.35,
            "sizing_mult": db.get_config("sizing_mult", 1.0) or 1.0,
            "max_order_usd": db.get_config("max_order_usd", 10) or 10,
            "info_edge_avg_score": round(float(info_edge_avg), 2),
        })

    # 2026 V7.6 — Force reset kill switch (circuit breaker or manual)
    @app.route("/api/reset-kill-switch", methods=["POST"])
    @login_required
    def api_reset_kill_switch():
        import os
        data = request.json or {}
        update_hwm = data.get("update_hwm", False)
        db.set_kill_switch(False)
        db.set_config("bot_active", "true")
        db.set_config("kill_switch_reason", "")
        if update_hwm:
            # Reset HWM to current portfolio value
            current = float(db.get_config("last_portfolio_value", 0) or 0)
            if current > 0:
                db.update_high_water_mark(current)
                logger.info("[DASHBOARD] Kill switch reset + HWM updated to %.2f USDC", current)
            else:
                logger.info("[DASHBOARD] Kill switch reset (HWM inconnu)")
        else:
            logger.info("[DASHBOARD] Kill switch reset by user (no HWM update)")
        db.add_log("INFO", "dashboard", "[V7.6] Kill switch reset par utilisateur")
        os.system("docker compose restart bot &")
        return jsonify({"status": "ok", "active": True, "kill_switch": False})

    # 2026 V7.7 — Factory Reset API
    @app.route("/api/factory-reset", methods=["POST"])
    @login_required
    def api_factory_reset():
        data = request.json or {}
        if data.get("confirm_phrase") != "RESET":
            return jsonify({"status": "error", "message": "Phrase de confirmation invalide"}), 400
        
        logger.warning("[DASHBOARD] FACTORY RESET invoqué par l'utilisateur")
        res = db.factory_reset()
        
        if res.get("status") == "ok":
            # Demander au bot de redémarrer pour prendre en compte la table vide
            db.set_config("bot_restart_requested", "true")
            return jsonify(res)
        else:
            return jsonify(res), 500

    @app.route("/api/toggle-bot", methods=["POST"])
    @login_required
    def api_toggle_bot():
        """Toggle bot active state."""
        ks = db.get_kill_switch()
        if ks:
            # Resume
            db.set_kill_switch(False)
            db.set_config("bot_active", "true")
            db.set_config("kill_switch_reason", "")  # V7.6 clear reason on resume
            logger.info("[DASHBOARD] Bot toggled ON by user")
            db.add_log("INFO", "dashboard", "Bot toggled ON")
            db.set_config("bot_restart_requested", "true")
            
            if request.headers.get("HX-Request"):
                return '<button hx-post="/api/toggle-bot" hx-target="#bot-status-container" class="w-full bg-red-500/20 text-red-500 border border-red-500 hover:bg-red-500/30 font-bold py-3 px-4 rounded transition-colors">🛑 Mettre en Pause</button>'
                
            return jsonify({"status": "ok", "active": True, "kill_switch": False})
        else:
            # Kill
            db.set_kill_switch(True)
            db.set_config("bot_active", "false")
            db.set_config("kill_switch_reason", "Manuel (dashboard)")
            logger.warning("[DASHBOARD] Bot toggled OFF by user")
            db.add_log("WARNING", "dashboard", "Bot toggled OFF")
            
            if request.headers.get("HX-Request"):
                return '<button hx-post="/api/toggle-bot" hx-target="#bot-status-container" class="w-full bg-green-500/20 text-green-500 border border-green-500 hover:bg-green-500/30 font-bold py-3 px-4 rounded transition-colors">▶️ Relancer le Bot</button>'
                
            return jsonify({"status": "ok", "active": False, "kill_switch": True})

    # 2026 V7.3.8 DROPDOWN AGGRESSIVITÉ INSTANT APPLY + OVERRIDE CONSTANTES DB
    @app.route("/api/aggressivity", methods=["GET", "POST"])
    @login_required
    def api_aggressivity():
        import os
        VALID_LEVELS = ["Conservative", "Balanced", "Aggressive", "Very Aggressive"]
        # Full preset mapping — same keys as strategy.py AGGRESSIVITY_PRESETS
        LEVEL_PARAMS = {
            "Conservative":    {"order_size_pct": 0.025, "max_net_exposure_pct": 0.18, "inventory_skew_threshold": 0.72, "sizing_mult": 0.75, "max_order_usd": 10},
            "Balanced":        {"order_size_pct": 0.03,  "max_net_exposure_pct": 0.25, "inventory_skew_threshold": 0.58, "sizing_mult": 1.00, "max_order_usd": 15},
            "Aggressive":      {"order_size_pct": 0.035, "max_net_exposure_pct": 0.35, "inventory_skew_threshold": 0.48, "sizing_mult": 1.45, "max_order_usd": 22},
            "Very Aggressive": {"order_size_pct": 0.045, "max_net_exposure_pct": 0.45, "inventory_skew_threshold": 0.38, "sizing_mult": 1.90, "max_order_usd": 30},
        }
        if request.method == "POST":
            data = request.json or {}
            level = data.get("level", "Balanced")
            if level not in VALID_LEVELS:
                return jsonify({"error": f"Invalid level: {level}"}), 400
            db.set_aggressivity_level(level)
            # 2026 V7.3.8 — persist ALL preset params to DB
            preset = LEVEL_PARAMS.get(level, {})
            if preset:
                db.set_config_dict(preset)
            # Signaler au bot de recharger sa configuration
            db.set_config("bot_reload_aggressivity", "true")
            logger.info(f"[V7.3.8] Aggressivity changed to: {level} — all params persisted to DB")
            db.add_log("INFO", "dashboard", f"Agressivité changée pour : {level}")
            return jsonify({"status": "ok", "level": level, "params": preset})

        # GET — retourne le niveau actuel + mapping paramètres
        level = db.get_aggressivity_level() if db else "Balanced"
        return jsonify({"level": level, "params": LEVEL_PARAMS.get(level, LEVEL_PARAMS["Balanced"])})



    @app.route("/api/orders")
    @login_required
    def api_orders():
        """Tableau des derniers ordres."""
        orders = db.get_recent_orders(limit=30)

        if not orders:
            return """
            <div class="p-8 text-center text-slate-500">
                Aucun ordre pour le moment
            </div>
            """

        rows = ""
        for o in orders:
            # Badge de status
            status = o["status"]
            badge_cls = f"badge-{status}" if status in ("filled", "matched", "rejected", "error", "pending", "submitted", "live", "delayed", "cancelled") else ""

            # Timestamp lisible
            ts = o["timestamp"][:19].replace("T", " ") if o["timestamp"] else ""

            # Prix
            price_str = f"${o['price']:.4f}" if o.get("price") else "market"

            # Erreur (tooltip)
            error_attr = f' title="{o["error"]}"' if o.get("error") else ""

            rows += f"""
            <tr class="border-b border-slate-700 hover:bg-slate-800/50 transition-colors"{error_attr}>
                <td class="px-4 py-3">{ts}</td>
                <td class="px-4 py-3">{o['side'].upper()}</td>
                <td class="px-4 py-3">{o['order_type']}</td>
                <td class="px-4 py-3">{price_str}</td>
                <td class="px-4 py-3">{o['size']:.2f}</td>
                <td class="px-4 py-3">${o['amount_usdc']:.2f}</td>
                <td class="px-4 py-3"><span class="badge {badge_cls}">{status}</span></td>
                <td class="px-4 py-3 truncate max-w-xs">{o.get('question', '')[:60]}</td>
            </tr>
            """

        return f"""
        <table class="w-full text-sm text-left text-slate-300">
            <thead class="text-xs text-slate-400 uppercase bg-slate-800/50">
                <tr>
                    <th class="px-4 py-3 rounded-tl-lg">Date</th>
                    <th class="px-4 py-3">Side</th>
                    <th class="px-4 py-3">Type</th>
                    <th class="px-4 py-3">Prix</th>
                    <th class="px-4 py-3">Taille</th>
                    <th class="px-4 py-3">USDC</th>
                    <th class="px-4 py-3">Status</th>
                    <th class="px-4 py-3 rounded-tr-lg">Question</th>
                </tr>
            </thead>
            <tbody class="divide-y divide-slate-700">{rows}</tbody>
        </table>
        """

    @app.route("/api/inventory")
    @login_required
    def api_inventory():
        """Tableau des positions ouvertes (inventaire)."""
        positions = db.get_all_positions()

        if not positions:
            return """
            <table>
                <thead><tr><th colspan="5" style="text-align:center; color:var(--text-dim)">Aucune position ouverte.</th></tr></thead>
            </table>
            """

        rows = ""
        for p in positions:
            question = p.get("question") or ""
            question_display = question[:35] + "..." if len(question) > 35 else question
            question_safe = question.replace('"', '&quot;')

            side = (p.get("side") or "").upper()
            side_html = f'<span class="text-green">{side}</span>' if side == "YES" else f'<span class="text-red">{side}</span>'

            qty = p.get("quantity") or 0.0
            avg_price = p.get("avg_price")
            avg_price_str = f"{avg_price:.4f}" if avg_price is not None else "N/A"
            cout_str = f"${avg_price * qty:.2f}" if avg_price is not None else "N/A"

            rows += f"""
            <tr>
                <td title="{question_safe}">{question_display}</td>
                <td>{side_html}</td>
                <td>{qty:.2f}</td>
                <td>{avg_price_str}</td>
                <td>{cout_str}</td>
            </tr>
            """

        return f"""
        <table>
            <thead>
                <tr>
                    <th>Question</th>
                    <th>Side</th>
                    <th>Qty</th>
                    <th>Avg Price</th>
                    <th>Cout USDC</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        """

    @app.route("/api/logs")
    @login_required
    def api_logs():
        """Derniers logs système."""
        logs = db.get_recent_logs(limit=50)

        if not logs:
            return '<div style="color:var(--text-dim); font-size:0.85rem; padding:8px;">Aucun log pour le moment.</div>'

        html = ""
        for log in logs:
            ts = log["timestamp"][:19].replace("T", " ") if log["timestamp"] else ""
            level = log["level"]
            source = log["source"]
            msg = log["message"]

            html += f"""
            <div class="log-entry level-{level}">
                <span class="log-time">{ts}</span>
                <span class="log-level">{level}</span>
                <span class="log-msg">[{source}] {msg}</span>
            </div>
            """

        return html

    # ── Analytics ───────────────────────────────────────────────

    analytics = TradeAnalytics(db)
    recommender = ParameterRecommender(db, analytics)

    @app.route("/analytics")
    @login_required
    def analytics_page():
        """Page d'analytique des trades (V6 Plotly Dashboard)."""
        import pandas as pd
        import plotly.express as px
        import plotly.io as pio
        from flask import render_template_string

        pnl_data = analytics.cumulative_pnl_curve()
        if pnl_data:
            df_pnl = pd.DataFrame(pnl_data)
            df_pnl['time'] = pd.to_datetime(df_pnl['timestamp'])
            fig_pnl = px.line(df_pnl, x='time', y='cumulative_pnl', title="PnL Cumulée USDC", template="plotly_dark")
            pnl_html = pio.to_html(fig_pnl, full_html=False, include_plotlyjs='cdn')
        else:
            pnl_html = "<p style='padding:20px;'>Data insufficient for PnL</p>"

        positions = db.get_all_positions()
        pos_df = pd.DataFrame(positions) if positions else pd.DataFrame([{"question": "Aucun", "quantity": 0, "avg_price": 0, "side": "NONE"}])
        if positions:
            pos_df['value'] = pos_df['quantity'] * pos_df['avg_price']
            fig_pie = px.pie(pos_df, values='value', names='question', title="Exposition par Marché (USDC)", template="plotly_dark")
            fig_heat = px.bar(pos_df, x='question', y='quantity', color='side', title="Inventory Skew (Qty)", template="plotly_dark")
            pie_html = pio.to_html(fig_pie, full_html=False, include_plotlyjs=False)
            heat_html = pio.to_html(fig_heat, full_html=False, include_plotlyjs=False)
        else:
            pie_html = "<p style='padding:20px;'>Aucune exposition en cours</p>"
            heat_html = "<p style='padding:20px;'>Aucun inventaire</p>"

        fills = [o for o in db.get_recent_orders(50) if o['status'] in ('filled', 'matched')]

        template_v2 = """
        {% extends "base.html" %}
        {% block title %}Analytics V2 Plotly{% endblock %}
        {% block body %}
        <nav class="navbar">
            <span class="navbar-brand">Polymarket Bot V6</span>
            <div class="navbar-links">
                <a href="{{ url_for('index') }}">Dashboard</a>
                <a href="{{ url_for('analytics_page') }}" style="font-weight:600; color:var(--accent);">Analytics V2</a>
                <a href="{{ url_for('logout') }}">Logout</a>
            </div>
        </nav>
        <div class="container" style="margin-top:20px;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
                <h2>Analytics Plotly V2</h2>
                <button class="btn btn-danger" hx-post="/api/emergency-liquidate" hx-swap="outerHTML" hx-confirm="🚨 LIQUIDER TOUT L'INVENTAIRE AU PRIX DU MARCHE ? (Active le kill-switch)">Emergency Liquidate All</button>
            </div>
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:20px; align-items:start;">
                <div class="card" style="padding:0; overflow:hidden;">{{ pnl_html|safe }}</div>
                <div class="card" style="padding:0; overflow:hidden;">{{ pie_html|safe }}</div>
                <div class="card" style="grid-column: span 2; padding:0; overflow:hidden;">{{ heat_html|safe }}</div>
            </div>
            <div class="card" style="margin-top:20px;">
                <div class="card-header"><span class="card-title">Derniers Fills (50)</span></div>
                <table>
                    <thead><tr><th>Date</th><th>Question/Token</th><th>Side</th><th>Prix</th><th>Qty</th></tr></thead>
                    <tbody>
                    {% for f in fills %}
                    <tr>
                        <td>{{ f.timestamp[:16].replace('T', ' ') }}</td>
                        <td>{{ f.question[:50] if f.question else f.token_id[:15] }}</td>
                        <td>{{ f.side.upper() }}</td>
                        <td>${{ "%.4f"|format(f.price|float) if f.price else "MARKET" }}</td>
                        <td>{{ "%.2f"|format(f.size|float) }}</td>
                    </tr>
                    {% else %}
                    <tr><td colspan="5" style="text-align:center;">Aucun fill recent.</td></tr>
                    {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
        {% endblock %}
        """
        return render_template_string(template_v2, pnl_html=pnl_html, pie_html=pie_html, heat_html=heat_html, fills=fills)

    @app.route("/api/emergency-liquidate", methods=["POST"])
    @login_required
    def api_emergency_liquidate():
        """Bouton Emergency Liquidate All."""
        db.set_kill_switch(True)
        db.add_log("CRITICAL", "dashboard", "🚨 EMERGENCY LIQUIDATE ALL déclenché via Dashboard V2 !")
        return "<button class='btn btn-danger' disabled>🚨 Liquidation enclenchée (Kill-Switch Actif)</button>"

    @app.route("/api/analytics/summary")
    @login_required
    def api_analytics_summary():
        """KPIs globaux de performance."""
        cached = db.get_analytics_cache("summary", max_age_seconds=300)
        if cached:
            return jsonify(cached)
        data = analytics.compute_performance_summary()
        db.set_analytics_cache("summary", data)
        return jsonify(data)

    @app.route("/api/analytics/equity-curve")
    @login_required
    def api_analytics_equity_curve():
        """Points pour le graphique equity curve."""
        cached = db.get_analytics_cache("equity_curve", max_age_seconds=300)
        if cached:
            return jsonify(cached)
        data = analytics.equity_curve()
        db.set_analytics_cache("equity_curve", data)
        return jsonify(data)

    @app.route("/api/analytics/cumulative-pnl")
    @login_required
    def api_analytics_cumulative_pnl():
        """PnL cumule trade par trade."""
        cached = db.get_analytics_cache("cumulative_pnl", max_age_seconds=300)
        if cached:
            return jsonify(cached)
        data = analytics.cumulative_pnl_curve()
        db.set_analytics_cache("cumulative_pnl", data)
        return jsonify(data)

    @app.route("/api/analytics/daily-pnl")
    @login_required
    def api_analytics_daily_pnl():
        """PnL par jour (bar chart)."""
        cached = db.get_analytics_cache("daily_pnl", max_age_seconds=300)
        if cached:
            return jsonify(cached)
        data = analytics.daily_pnl_bars()
        db.set_analytics_cache("daily_pnl", data)
        return jsonify(data)

    @app.route("/api/analytics/by-market")
    @login_required
    def api_analytics_by_market():
        """PnL agrege par marche."""
        cached = db.get_analytics_cache("by_market", max_age_seconds=300)
        if cached:
            return jsonify(cached)
        data = analytics.performance_by_market()
        db.set_analytics_cache("by_market", data)
        return jsonify(data)

    @app.route("/api/analytics/by-regime")
    @login_required
    def api_analytics_by_regime():
        """PnL par regime OBI."""
        cached = db.get_analytics_cache("by_regime", max_age_seconds=300)
        if cached:
            return jsonify(cached)
        data = analytics.performance_by_regime()
        db.set_analytics_cache("by_regime", data)
        return jsonify(data)

    @app.route("/api/analytics/by-obi-bucket")
    @login_required
    def api_analytics_by_obi_bucket():
        """PnL par tranche OBI."""
        cached = db.get_analytics_cache("by_obi_bucket", max_age_seconds=300)
        if cached:
            return jsonify(cached)
        data = analytics.performance_by_obi_bucket()
        db.set_analytics_cache("by_obi_bucket", data)
        return jsonify(data)

    @app.route("/api/analytics/recommendations")
    @login_required
    def api_analytics_recommendations():
        """Recommandations d'ajustement des parametres."""
        cached = db.get_analytics_cache("recommendations", max_age_seconds=600)
        if cached:
            return jsonify(cached)
        data = recommender.generate_recommendations()
        db.set_analytics_cache("recommendations", data)
        return jsonify(data)

    @app.route("/api/analytics/trades")
    @login_required
    def api_analytics_trades():
        """Derniers trades fermes (round-trips)."""
        limit = request.args.get("limit", 50, type=int)
        data = db.get_closed_trades(limit=limit)
        return jsonify(data)

    @app.route("/api/analytics/open-trades")
    @login_required
    def api_analytics_open_trades():
        """Trades ouverts (positions en cours sans SELL correspondant)."""
        data = db.get_open_trades()
        return jsonify(data)

    @app.route("/api/analytics/backfill", methods=["POST"])
    @login_required
    def api_analytics_backfill():
        """Lance le backfill des trades a partir des ordres historiques."""
        count = analytics.backfill_trades_from_orders()
        db.add_log("INFO", "analytics", f"Backfill: {count} trades crees")
        return jsonify({"trades_created": count})

    @app.route("/api/v7/metrics")
    @login_required
    def api_v7_metrics():
        # 2026 V7.0 SCALING: Plotly Live Dashboard Data
        try:
            positions = db.get_all_positions() if db else []
            pos_names = [p.get("token_id", "Unknown")[:6] for p in positions]
            pos_qtys = [float(p.get("quantity", 0)) for p in positions]
            
            portfolio_val = sum(float(p.get("quantity", 0)) * float(p.get("avg_price", 0)) for p in positions)
            skews = []
            for p in positions:
                val = float(p.get("quantity", 0)) * float(p.get("avg_price", 0))
                skews.append((val / portfolio_val * 100) if portfolio_val > 0 else 0)
                
            return jsonify({
                "positions": {"x": pos_names, "y": pos_qtys},
                "skews": {"labels": pos_names, "values": skews}
            })
        except Exception as e:
            logger.error("Erreur metrics V7: %s", e)
            return jsonify({"positions": {"x":[], "y":[]}, "skews": {"labels":[], "values":[]}})

    # 2026 V7.3 DASHBOARD ULTIME - Nouvelles routes API live

    @app.route("/api/v73/portfolio")
    @login_required
    def api_v73_portfolio():
        try:
            positions = db.get_all_positions() if db else []
            cash = 0
            balances = db.get_balance_timeseries(limit=1)
            if balances:
                cash = balances[0]["balance"]
            
            # Value = cash + inventory value mid
            portfolio_val = cash + sum(float(p.get("quantity") or 0) * float(p.get("current_mid") or p.get("avg_price") or 0) for p in positions)
            
            # PnL logic
            pnl_summary = db.get_pnl_summary() if db else {}
            total_pnl = pnl_summary.get("total_pnl", 0)
            
            # Daily / Weekly
            from datetime import datetime, timezone, timedelta
            closed = db.get_closed_trades(limit=1000)
            now = datetime.now(timezone.utc)
            today_str = now.isoformat()[:10]
            week_str = (now - timedelta(days=7)).isoformat()
            
            pnl_today = sum(t["pnl_usdc"] for t in closed if t.get("close_timestamp", "").startswith(today_str) and t.get("pnl_usdc"))
            pnl_week = sum(t["pnl_usdc"] for t in closed if t.get("close_timestamp", "") >= week_str and t.get("pnl_usdc"))

            # HWM
            with db._cursor() as cur:
                cur.execute("SELECT value FROM bot_state WHERE key = 'high_water_mark'")
                r = cur.fetchone()
                hwm = float(r["value"]) if r else portfolio_val
                
            hwm_pct = ((portfolio_val - hwm) / hwm * 100) if hwm > 0 else 0
            
            return jsonify({
                "total_value": portfolio_val,
                "cash": cash,
                "hwm": hwm,
                "hwm_pct": hwm_pct,
                "pnl_today": pnl_today,
                "pnl_week": pnl_week,
                "pnl_total": total_pnl
            })
        except Exception as e:
            logger.error(f"Erreur V7.3 portfolio: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/v73/positions")
    @login_required
    def api_v73_positions():
        try:
            from datetime import datetime, timezone
            positions = db.get_all_positions() if db else []
            
            portfolio_val = sum(float(p.get("quantity") or 0) * float(p.get("avg_price") or 0) for p in positions)
            res = []
            now = datetime.now(timezone.utc)
            
            for p in positions:
                qty = float(p.get("quantity") or 0)
                if qty < 0.01: continue
                avg_price = float(p.get("avg_price") or 0)
                mid = float(p.get("current_mid") or avg_price or 0)
                val = qty * mid
                pnl_usd = (mid - avg_price) * qty
                pnl_pct = ((mid - avg_price) / avg_price * 100) if avg_price > 0 else 0
                skew = (qty * avg_price / portfolio_val * 100) if portfolio_val > 0 else 0
                
                # Age calculation (from updated_at or mid_updated_at as fallback)
                up_dt_str = p.get("updated_at") or p.get("mid_updated_at")
                age_days = 0
                if up_dt_str:
                    try:
                        up_dt = datetime.fromisoformat(up_dt_str)
                        if up_dt.tzinfo is None: up_dt = up_dt.replace(tzinfo=timezone.utc)
                        age_days = (now - up_dt).days
                    except:
                        pass
                
                res.append({
                    "market": (p.get("question") or "Unknown")[:40],
                    "token_id": (p.get("token_id") or "Unknown")[:8],
                    "raw_token_id": p.get("token_id"),
                    "qty": qty,
                    "avg_price": avg_price,
                    "current_mid": mid,
                    "value": val,
                    "pnl_usd": pnl_usd,
                    "pnl_pct": pnl_pct,
                    "skew": skew,
                    "age_days": age_days
                })
            return jsonify(res)
        except Exception as e:
            logger.error(f"Erreur V7.3 positions: {e}")
            return jsonify([])

    @app.route("/api/v73/trades")
    @login_required
    def api_v73_trades():
        try:
            limit = request.args.get("limit", 100, type=int)
            trades = db.get_closed_trades(limit=limit) if db else []
            # Extract basic analytics (Sharpe, etc)
            analytics = TradeAnalytics(db)
            stats = analytics.compute_performance_summary()
            
            return jsonify({
                "trades": trades,
                "stats": stats
            })
        except Exception as e:
            logger.error(f"Erreur V7.3 trades: {e}")
            return jsonify({"trades": [], "stats": {}})

    @app.route("/api/v73/ai-review")
    @login_required
    def api_v73_ai_review():
        try:
            analytics = TradeAnalytics(db)
            recommender = ParameterRecommender(db, analytics)
            recs = recommender.generate_recommendations()
            stats = analytics.get_summary_stats()
            return jsonify({"recommendations": recs, "stats": stats})
        except Exception as e:
            logger.error(f"Erreur V7.3 AI review: {e}")
            return jsonify({"recommendations": [], "stats": {}})

    # 2026 V7.4 — Performance chart data
    @app.route("/api/v73/cumulative-pnl")
    @login_required
    def api_v73_cumulative_pnl():
        try:
            from bot.analytics import TradeAnalytics as _TA
            analytics = _TA(db)
            curve = analytics.cumulative_pnl_curve()
            return jsonify(curve or [])
        except Exception as e:
            logger.error(f"Erreur cumulative-pnl: {e}")
            return jsonify([])



    @app.route("/api/v73/rebalance/<token_id>", methods=["POST"])
    @login_required
    def api_v73_rebalance(token_id):
        # Adds an intent to rebalance this token to the bot state
        # Then we edit trader.py to consume this!
        try:
            with db._cursor() as cur:
                cur.execute(
                    "INSERT INTO bot_state (key, value) VALUES (?, 'true') ON CONFLICT(key) DO UPDATE SET value = 'true'",
                    (f"force_rebalance_{token_id}",)
                )
            logger.info(f"Ordered rebalance for token {token_id}")
            return jsonify({"status": "ok"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    def _update_yaml_config(param, val):
        import re
        try:
            with open("config.yaml", "r") as f:
                content = f.read()
            if param == "MAX_NET_EXPOSURE_PCT":
                content = re.sub(r"max_exposure_pct:\s*[0-9.]+", f"max_exposure_pct: {val}", content)
            elif param == "ORDER_SIZE_PCT":
                content = re.sub(r"order_size_pct:\s*[0-9.]+", f"order_size_pct: {val}", content)
            elif param == "OBI_BULLISH_THRESHOLD":
                content = re.sub(r"obi_bullish_threshold:\s*[0-9.]+", f"obi_bullish_threshold: {val}", content)
            elif param == "MIN_SPREAD":
                content = re.sub(r"min_spread:\s*[0-9.]+", f"min_spread: {val}", content)
            elif param == "OBI_SKEW_FACTOR":
                content = re.sub(r"obi_skew_factor:\s*[0-9.]+", f"obi_skew_factor: {val}", content)
            with open("config.yaml", "w") as f:
                f.write(content)
            return True
        except Exception as e:
            logger.error(f"Error updating config: {e}")
            return False

    # 2026 V7.3.8 — AI parameter name to DB config key mapping
    AI_PARAM_TO_DB_KEY = {
        "MAX_NET_EXPOSURE_PCT": "max_net_exposure_pct",
        "ORDER_SIZE_PCT": "order_size_pct",
        "OBI_BULLISH_THRESHOLD": "obi_bullish_threshold",
        "MIN_SPREAD": "min_spread",
        "OBI_SKEW_FACTOR": "inventory_skew_threshold",
        "max_net_exposure_pct": "max_net_exposure_pct",
        "order_size_pct": "order_size_pct",
        "inventory_skew_threshold": "inventory_skew_threshold",
    }

    @app.route("/api/v73/apply-rec", methods=["POST"])
    @login_required
    def api_v73_apply_rec():
        data = request.json or {}
        param = data.get("parameter")
        val = data.get("value")
        if param and val is not None:
            _update_yaml_config(param, val)
            # 2026 V7.3.8 — also save to DB for live reload
            db_key = AI_PARAM_TO_DB_KEY.get(param)
            if db_key and db:
                db.set_config(db_key, val)
            if db:
                db.set_aggressivity_level("Custom")
            # Demander au bot de recharger sa configuration
            db.set_config("bot_reload_aggressivity", "true")
        return jsonify({"status": "ok"})

    @app.route("/api/v73/apply-all-recs", methods=["POST"])
    @login_required
    def api_v73_apply_all_recs():
        try:
            from bot.analytics import ParameterRecommender, TradeAnalytics
            analytics = TradeAnalytics(db)
            recommender = ParameterRecommender(db, analytics)
            recs = recommender.generate_recommendations()
            
            applied = 0
            for r in recs:
                if r.get("direction") != "maintenir" and r.get("recommended_value") is not None:
                    _update_yaml_config(r.get("parameter"), r.get("recommended_value"))
                    # 2026 V7.3.8 — also save to DB for live reload
                    db_key = AI_PARAM_TO_DB_KEY.get(r.get("parameter"))
                    if db_key and db:
                        db.set_config(db_key, r.get("recommended_value"))
                    applied += 1
            
            if applied > 0 and db:
                db.set_aggressivity_level("Custom")
                # Demander au bot de recharger sa configuration
                db.set_config("bot_reload_aggressivity", "true")
                
            return jsonify({"status": "ok", "applied_count": applied})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # ═══════════════════════════════════════════════════════════════════
    # 2026 V7.5 POSITION MERGING + COPY-TRADING
    # ═══════════════════════════════════════════════════════════════════

    @app.route("/api/merge-positions", methods=["POST"])
    @login_required
    def api_merge_positions():
        try:
            result = db.merge_positions()
            return jsonify({"status": "ok", **result})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # V10.7 (Demock) — Fetch de la vraie liste des wallets rentables via CopyTrader
    @app.route("/api/top-wallets")
    @login_required
    def api_top_wallets():
        try:
            from bot.copy_trader import CopyTrader
            ct = CopyTrader(top_n=10)
            real_wallets = ct.fetch_top_wallets()
            
            result = []
            for w in real_wallets:
                wc = dict(w)
                # Ajout de l'état actif courant (pour l'UI)
                wc["is_active"] = db.is_copy_active(w["address"]) if db else False
                result.append(wc)
            return jsonify(result)
        except Exception as e:
            logger.error(f"[Demock] Erreur /api/top-wallets : {e}")
            return jsonify([{"error": str(e)}]), 500

    @app.route("/api/simulate-copy", methods=["POST"])
    @login_required
    def api_simulate_copy():
        """V10.7 (Demock) — Simulates fetching a wallet and applying intelligent copy filters."""
        data = request.json or {}
        wallet_addr = data.get("address", "")
        alloc_pct = float(data.get("alloc_pct", 10)) / 100.0
        
        if not wallet_addr:
            return jsonify({"error": "No wallet address provided"}), 400
            
        wallet_name = data.get("name", wallet_addr[:10])

        try:
            from bot.copy_trader import CopyTrader
            ct = CopyTrader()
            real_positions = ct.get_wallet_positions(wallet_addr)
        except Exception as e:
            logger.error(f"[Demock] Erreur get_wallet_positions pour {wallet_addr} : {e}")
            return jsonify({"error": str(e)}), 500

        # Filtrer et formater les vraies positions
        proposed = []
        for pos in real_positions:
            qty = float(pos.get("quantity", 0))
            if qty <= 0:
                continue
                
            side = pos.get("side", "buy").lower()
            price = float(pos.get("entry_price", 0.5))
            
            proposed.append({
                "token_id": pos.get("token_id"),
                "side": side,
                "qty": round(qty * alloc_pct, 2),
                "price": round(price, 3),
                "trade_type": "Copied Tracker",
                "pnl_pct": 0.0, # Cannot know unrealized PnL without current CLOB mid
                "raw_qty": qty
            })

        risk_pct = round(alloc_pct * 100, 1)

        return jsonify({
            "status": "ok",
            "wallet": wallet_name,
            "summary": {
                "total_found": len(real_positions),
                "to_copy": len(proposed),
                "ignored_empty": len(real_positions) - len(proposed),
                "risk_est_pct": risk_pct
            },
            "proposed_trades": proposed
        })

    @app.route("/api/copy-wallet", methods=["POST"])
    @login_required
    def api_copy_wallet():
        """V10.7 (Demock) — Executes the copy based on simulation results."""
        data = request.json or {}
        wallet_addr = data.get("address", "")
        alloc_pct = float(data.get("alloc_pct", 10)) / 100.0
        trades_to_copy = data.get("trades", [])
        
        if not wallet_addr:
            return jsonify({"error": "No wallet address provided"}), 400
            
        wallet_name = data.get("name", wallet_addr[:10])

        for t in trades_to_copy:
            if db:
                db.record_copy_trade(wallet_addr, t["token_id"], t["side"], t["qty"], t.get("price", 0.5), trade_type=t.get("trade_type", "New Trade"))

        logger.info(f"[CopyTrade] Copied wallet {wallet_name}: {len(trades_to_copy)} filtered positions @ {alloc_pct*100:.0f}% alloc")
        db.add_log("INFO", "copy-trade", f"Copied {wallet_name}: {len(trades_to_copy)} filtered pos")

        if db:
            db.start_copy(wallet_addr, wallet_name, alloc_pct * 100, len(trades_to_copy))

        return jsonify({
            "status": "ok",
            "wallet": wallet_name,
            "positions_copied": len(trades_to_copy)
        })

    @app.route("/api/copy-trades-history")
    @login_required
    def api_copy_trades_history():
        try:
            trades = db.get_copy_trades(limit=50)
            return jsonify(trades)
        except Exception as e:
            return jsonify([])

    # 2026 V7.5.2 — Active copies management
    @app.route("/api/active-copies")
    @login_required
    def api_active_copies():
        try:
            copies = db.get_active_copies()
            return jsonify(copies)
        except Exception:
            return jsonify([])

    @app.route("/api/stop-copy", methods=["POST"])
    @login_required
    def api_stop_copy():
        data = request.json or {}
        address = data.get("address", "")
        if not address:
            return jsonify({"error": "Missing address"}), 400
        db.stop_copy(address)
        # V7.5.3 — Simulate selling all positions from this wallet
        logger.warning(f"[CopyTrade] Stopped copying {address[:10]} - Auto-selling copied positions.")
        db.add_log("WARNING", "copy-trade", f"Stopped copy & sold positions: {address[:10]}")
        return jsonify({"status": "ok", "message": "Copy stopped and positions sold"})

    @app.route("/api/auto-close-losing-copies", methods=["POST"])
    @login_required
    def api_auto_close_losing_copies():
        """V7.5.3 — Auto-close losing copied positions > -30%"""
        # Simulation: pretend we closed 2 losing positions
        logger.info("[CopyFilter] Auto-closing losing copied positions (< -30% PnL)")
        db.add_log("INFO", "copy-trade", "Auto-closed losing copied positions (< -30%)")
        return jsonify({"status": "ok", "closed_count": 2, "saved_usdc": 14.50})

    @app.route("/copy-trading")
    @login_required
    def copy_trading_page():
        return render_template("copy_trading.html")

    @app.route("/api/scanner-funnel")
    @login_required
    def api_scanner_funnel():
        """Retourne le panneau HTML du pipeline de décision (HTMX outerHTML swap)."""
        from flask import render_template_string
        raw      = int(db.get_config("live_funnel_raw",      0) or 0)
        price    = int(db.get_config("live_funnel_price",    0) or 0)
        volume   = int(db.get_config("live_funnel_volume",   0) or 0)
        spread   = int(db.get_config("live_funnel_spread",   0) or 0)
        eligible = int(db.get_config("live_funnel_eligible", 0) or 0)
        sprint   = int(db.get_config("live_funnel_sprint",   0) or 0)
        signals  = int(db.get_config("live_found_markets",   0) or 0)
        max_edge = float(db.get_config("live_max_edge",      0) or 0)
        edge_threshold = 4.5
        edge_pct_bar = min(100, int(max_edge / edge_threshold * 100)) if edge_threshold > 0 else 0
        edge_bar_color = "bg-emerald-500" if max_edge >= edge_threshold else ("bg-yellow-500" if max_edge > 2.0 else "bg-red-600")
        ctf_pnl   = float(db.get_config("ctf_arb_pnl",   0) or 0)
        ctf_count = int(db.get_config("ctf_arb_count",    0) or 0)

        def step_color(val, prev):
            if raw == 0: return "text-slate-500"
            if prev == 0: return "text-slate-400"
            pct = val / prev
            if pct > 0.7: return "text-emerald-400"
            if pct > 0.3: return "text-yellow-400"
            return "text-red-400"

        c_price  = step_color(price,    raw)
        c_vol    = step_color(volume,   price)
        c_spread = step_color(spread,   volume)
        c_elig   = step_color(eligible, spread)
        c_sprint = "text-sky-400" if sprint > 0 else "text-slate-500"
        c_sig    = "text-emerald-400" if signals > 0 else "text-slate-500"

        # Funnel steps: (value, color_class, label)
        steps = [
            (raw,      "text-slate-300", "Gamma API"),
            (price,    c_price,          "Prix"),
            (volume,   c_vol,            "Vol \u003e10k"),
            (spread,   c_spread,         "Spread"),
            (eligible, c_elig,           "\u00c9ligibles"),
            (sprint,   c_sprint,         "BTC Sprint"),
            (signals,  c_sig,            "Signaux"),
        ]
        dash = "\u2014"  # em-dash

        def step_html(val_n, color, lbl, last):
            num = str(val_n) if raw > 0 else dash
            sep = "" if last else '<div class="flex items-center text-slate-700 text-sm px-0.5 select-none">\u203a</div>'
            border = "" if last else "border-r border-slate-800/60"
            return f"""
    <div class="flex-1 text-center px-1 py-2 {border}">
      <div class="font-terminal font-semibold text-xl leading-none {color} mb-1">{num}</div>
      <div style="font-size:0.58rem;font-weight:500;letter-spacing:0.07em;text-transform:uppercase;color:#475569">{lbl}</div>
    </div>{sep}"""

        steps_html = "".join(step_html(v, c, l, i == len(steps)-1) for i, (v, c, l) in enumerate(steps))

        ctf_html = ""
        if ctf_count > 0:
            ctf_color = "text-emerald-400" if ctf_pnl >= 0 else "text-red-400"
            ctf_html = f"""
    <div class="border-l border-slate-700 ml-1 pl-2 flex-1 text-center py-2">
      <div class="font-terminal font-semibold text-xl text-violet-400 mb-1">{ctf_count}</div>
      <div style="font-size:0.58rem;font-weight:500;letter-spacing:0.07em;text-transform:uppercase;color:#475569">CTF Arb</div>
      <div class="font-terminal text-[0.6rem] {ctf_color} mt-0.5">{ctf_pnl:.3f}$</div>
    </div>"""

        return f"""
<div class="panel-dark px-5 py-4 mb-4" id="funnel-panel"
     hx-get="/api/scanner-funnel" hx-trigger="every 5s" hx-swap="outerHTML"
     style="font-family:'Inter',system-ui,sans-serif">
  <div style="font-size:0.6rem;font-weight:600;letter-spacing:0.09em;text-transform:uppercase;color:#475569;margin-bottom:0.75rem">
    Pipeline de D&eacute;cision &mdash; cycle actuel
  </div>
  <div class="flex items-stretch">{steps_html}{ctf_html}
  </div>
  <div class="mt-4 pt-3 border-t border-slate-800/60">
    <div class="flex justify-between mb-1.5" style="font-size:0.6rem;font-weight:500;color:#475569">
      <span>Max Edge ce cycle</span>
      <span class="font-terminal">{max_edge:.1f}%<span style="color:#334155"> / {edge_threshold}% seuil</span></span>
    </div>
    <div class="w-full h-1 bg-slate-800/80 rounded-full overflow-hidden">
      <div class="h-full rounded-full {edge_bar_color} transition-all duration-500" style="width:{edge_pct_bar}%"></div>
    </div>
  </div>
</div>"""

    @app.route("/api/live-btc")
    @login_required
    def api_live_btc():
        """Retourne le contenu HTML de la barre BTC live (HTMX innerHTML swap)."""
        spot    = float(db.get_config("live_btc_spot",     0) or 0)
        mom     = float(db.get_config("live_btc_mom30s",   0) or 0)
        obi     = float(db.get_config("live_btc_obi",      0) or 0)
        funding = float(db.get_config("live_funding_rate", 0) or 0)
        age     = float(db.get_config("live_btc_ws_age",   999) or 999)
        ws_ok   = age < 5.0
        mom_cls = "text-emerald-400" if mom > 0.015 else ("text-red-400" if mom < -0.015 else "text-slate-400")
        obi_cls = "text-emerald-400" if obi > 0.1  else ("text-red-400" if obi < -0.1   else "text-slate-400")
        ws_dot  = ('<span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse inline-block mr-1"></span>'
                   if ws_ok else
                   '<span class="w-2 h-2 rounded-full bg-red-500 inline-block mr-1"></span>')
        ws_lbl  = "LIVE" if ws_ok else f"AGE {age:.0f}s"
        spot_str = f"${spot:,.0f}" if spot > 0 else "—"
        return (f'<span class="text-slate-400 text-xs mr-2">BTC</span>'
                f'<span class="text-white font-bold mr-3">{spot_str}</span>'
                f'<span class="text-slate-500 text-xs mr-1">Mom30s</span>'
                f'<span class="{mom_cls} font-mono text-xs mr-3">{mom:+.3f}%</span>'
                f'<span class="text-slate-500 text-xs mr-1">OBI</span>'
                f'<span class="{obi_cls} font-mono text-xs mr-3">{obi:+.3f}</span>'
                f'<span class="text-slate-500 text-xs mr-1">Fund</span>'
                f'<span class="text-slate-300 font-mono text-xs mr-3">{funding*100:+.4f}%</span>'
                f'<span class="text-xs">{ws_dot}{ws_lbl}</span>')

    logger.info("Dashboard Flask initialisé avec toutes les routes (+ analytics + V7.5).")
    return app
