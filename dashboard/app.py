"""
Dashboard web Flask.
Interface de monitoring et de contrôle du bot Polymarket.
Routes :
  /           → Dashboard principal (protégé)
  /login      → Page de connexion
  /logout     → Déconnexion
  /health     → Health check (public, pour Uptime Kuma)
  /api/*      → Fragments htmx (protégés)
"""

import logging

from flask import Flask, jsonify, redirect, render_template, request, session, url_for

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
        return render_template("index.html")

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
        filled = sum(1 for o in orders if o["status"] == "filled")
        rejected = sum(1 for o in orders if o["status"] == "rejected")
        errors = sum(1 for o in orders if o["status"] == "error")

        balance_str = f"${balance:.2f}" if balance is not None else "N/A"
        balance_class = "text-green" if balance and balance > 0 else "text-red"

        return f"""
        <div class="card">
            <div class="stat-value {balance_class}">{balance_str}</div>
            <div class="stat-label">Solde USDC</div>
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

    @app.route("/api/kill-switch-ui")
    @login_required
    def api_kill_switch_ui():
        """Bouton kill switch."""
        active = db.get_kill_switch()
        if active:
            return """
            <div style="display:flex; align-items:center; gap:16px;">
                <span style="font-size:0.95rem;">Le bot est actuellement <strong class="text-red">en pause</strong>.</span>
                <button class="btn btn-success"
                        hx-post="/api/kill-switch"
                        hx-vals='{"action": "off"}'
                        hx-target="#kill-switch-area"
                        hx-swap="innerHTML">
                    Relancer le bot
                </button>
            </div>
            """
        return """
        <div style="display:flex; align-items:center; gap:16px;">
            <span style="font-size:0.95rem;">Le bot est actuellement <strong class="text-green">actif</strong>.</span>
            <button class="btn btn-danger"
                    hx-post="/api/kill-switch"
                    hx-vals='{"action": "on"}'
                    hx-target="#kill-switch-area"
                    hx-swap="innerHTML"
                    hx-confirm="Confirmer l'arret d'urgence du bot ?">
                ARRET D'URGENCE
            </button>
        </div>
        """

    @app.route("/api/kill-switch", methods=["POST"])
    @login_required
    def api_kill_switch_toggle():
        """Active ou désactive le kill switch."""
        action = request.form.get("action") or (request.json or {}).get("action")
        if action == "on":
            db.set_kill_switch(True)
            logger.warning("Kill switch ACTIVE via dashboard.")
            db.add_log("WARNING", "dashboard", "Kill switch ACTIVE")
        elif action == "off":
            db.set_kill_switch(False)
            logger.info("Kill switch DESACTIVE via dashboard.")
            db.add_log("INFO", "dashboard", "Kill switch DESACTIVE")

        # Retourne le nouveau bouton via htmx
        return api_kill_switch_ui()

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
            badge_cls = f"badge-{status}" if status in ("filled", "rejected", "error", "pending", "submitted") else ""

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

    logger.info("Dashboard Flask initialisé avec toutes les routes.")
    return app
