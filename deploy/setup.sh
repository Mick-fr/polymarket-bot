#!/bin/bash
# ══════════════════════════════════════════════════════════
# POLYMARKET BOT — Script de setup initial sur le VPS
# Execute une seule fois apres le premier clone du projet.
# Usage : bash deploy/setup.sh
# ══════════════════════════════════════════════════════════

set -e  # Arrete le script si une commande echoue

echo "=================================================="
echo "  POLYMARKET BOT — Setup VPS"
echo "=================================================="

# ── 1. Verifier que Docker est installe ──────────────────
if ! command -v docker &> /dev/null; then
    echo "ERREUR: Docker n'est pas installe."
    echo "Sur Hostinger: hPanel > VPS > OS & Panel > Choisir le template Docker"
    exit 1
fi

if ! command -v docker compose &> /dev/null; then
    echo "ERREUR: Docker Compose n'est pas disponible."
    exit 1
fi

echo "[OK] Docker et Docker Compose detectes."

# ── 2. Verifier que le .env existe ───────────────────────
if [ ! -f .env ]; then
    echo ""
    echo "ERREUR: fichier .env introuvable."
    echo ""
    echo "Options :"
    echo "  1. Genere-le sur ta machine locale :"
    echo "     python deploy/generate_env.py"
    echo "     puis transfère le .env sur le VPS via SCP"
    echo ""
    echo "  2. Copie le template et remplis-le manuellement :"
    echo "     cp .env.example .env"
    echo "     nano .env"
    echo ""
    exit 1
fi

echo "[OK] Fichier .env trouve."

# ── 3. Creer le repertoire de donnees ────────────────────
mkdir -p data
echo "[OK] Repertoire data/ cree."

# ── 4. Construire les images Docker ──────────────────────
echo ""
echo "Construction des images Docker..."
docker compose build --no-cache

echo "[OK] Images construites."

# ── 5. Premier lancement ────────────────────────────────
echo ""
echo "Demarrage des conteneurs..."
docker compose up -d

echo ""
echo "=================================================="
echo "  DEPLOIEMENT REUSSI !"
echo "=================================================="
echo ""
echo "  Bot     : tourne en arriere-plan"
echo "  Dashboard: http://<IP_VPS>:$(grep DASHBOARD_PORT .env | cut -d= -f2 || echo 8080)"
echo ""
echo "  Commandes utiles :"
echo "    docker compose logs -f bot        # Logs du bot en temps reel"
echo "    docker compose logs -f dashboard  # Logs du dashboard"
echo "    docker compose ps                 # Etat des conteneurs"
echo "    docker compose restart bot        # Redemarrer le bot"
echo "    docker compose down               # Tout arreter"
echo "    docker compose up -d              # Tout relancer"
echo ""
