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
        """Dashboard principal."""
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
            "index.html",
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
        import os
        db.set_kill_switch(False)
        logger.info("[DASHBOARD] Bot resumed by user")
        db.add_log("INFO", "dashboard", "[DASHBOARD] Bot resumed by user")
        os.system("docker compose restart bot &")
        return jsonify({"status": "ok"})

    @app.route("/kill", methods=["POST"])
    @login_required
    def api_kill():
        db.set_kill_switch(True)
        logger.warning("Kill switch ACTIVE via dashboard.")
        db.add_log("WARNING", "dashboard", "Kill switch ACTIVE")
        return jsonify({"status": "ok"})

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
            # Write reload flag so bot picks it up instantly next cycle
            try:
                with open("/tmp/reload_aggressivity.flag", "w") as f:
                    f.write(level)
            except Exception:
                pass  # Non-critical on Windows dev
            logger.info(f"[V7.3.8] Aggressivity changed to: {level} — all params persisted to DB")
            db.add_log("INFO", "dashboard", f"Agressivité changée pour : {level}")
            return jsonify({"status": "ok", "level": level, "params": preset})

        # GET — retourne le niveau actuel + mapping paramètres
        level = db.get_aggressivity_level() if db else "Balanced"
        return jsonify({"level": level, "params": LEVEL_PARAMS.get(level, LEVEL_PARAMS["Balanced"])})

    # 2026 V7.3.10 DASHBOARD LIVE VALUES SYNC
    @app.route("/api/current-config", methods=["GET"])
    @login_required
    def api_current_config():
        """Retourne toutes les valeurs de config live depuis la DB."""
        level = db.get_aggressivity_level() if db else "Balanced"
        return jsonify({
            "aggressivity_level":      level,
            "order_size_pct":          db.get_config("order_size_pct",          0.03)  if db else 0.03,
            "max_net_exposure_pct":    db.get_config("max_net_exposure_pct",    0.20)  if db else 0.20,
            "inventory_skew_threshold":db.get_config("inventory_skew_threshold",0.60)  if db else 0.60,
            "sizing_mult":             db.get_config("sizing_mult",              1.00)  if db else 1.00,
            "max_order_usd":           db.get_config("max_order_usd",           15.0)  if db else 15.0,
        })

    @app.route("/api/orders")
    @login_required
    def api_orders():
        """Tableau des derniers ordres."""
        orders = db.get_recent_orders(limit=30)

        if not orders:
            return """
            <table>
                <thead><tr><th colspan="7" style="text-align:center; color:var(--text-dim)">Aucun ordre pour le moment</th></tr></thead>
            </table>
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
            <tr{error_attr}>
                <td>{ts}</td>
                <td>{o['side'].upper()}</td>
                <td>{o['order_type']}</td>
                <td>{price_str}</td>
                <td>{o['size']:.2f}</td>
                <td>${o['amount_usdc']:.2f}</td>
                <td><span class="badge {badge_cls}">{status}</span></td>
            </tr>
            """

        return f"""
        <table>
            <thead>
                <tr>
                    <th>Date</th>
                    <th>Side</th>
                    <th>Type</th>
                    <th>Prix</th>
                    <th>Taille</th>
                    <th>USDC</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
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
            # Write reload flag for instant pickup
            try:
                with open("/tmp/reload_aggressivity.flag", "w") as f:
                    f.write("Custom")
            except Exception:
                pass
        import os
        os.system("docker compose restart bot &")
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
                # Write reload flag for instant pickup
                try:
                    with open("/tmp/reload_aggressivity.flag", "w") as f:
                        f.write("Custom")
                except Exception:
                    pass
                import os
                os.system("docker compose restart bot &")
                
            return jsonify({"status": "ok", "applied_count": applied})
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    logger.info("Dashboard Flask initialisé avec toutes les routes (+ analytics).")
    return app
