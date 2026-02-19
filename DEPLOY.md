# Deploiement — Polymarket Bot sur VPS Hostinger

Guide pas-a-pas pour deployer le bot et le dashboard sur un VPS Hostinger avec Docker.

---

## Prerequis

- Un VPS Hostinger (KVM 1 minimum : 1 vCPU, 4 Go RAM, 50 Go SSD)
- Un wallet Polygon avec du USDC (minimum 50$)
- Python 3.9+ installe en local (pour generer le .env)
- Git installe en local

---

## Etape 1 — Preparer le wallet Polymarket

Avant de deployer le bot, ton wallet doit etre configure sur Polymarket :

1. **Creer/importer un wallet** sur Polygon (MetaMask, ou wallet dedie au bot)
2. **Deposer du USDC** sur le wallet via Polygon
3. **Se connecter a Polymarket** au moins une fois via le site web pour activer le wallet
4. **Approuver les allowances** (si wallet EOA) :
   - USDC : `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`
   - Conditional Tokens : `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`

   Cela se fait automatiquement lors du premier trade sur le site Polymarket.

5. **Exporter la cle privee** du wallet (tu en auras besoin pour le .env)

> **SECURITE** : Utilise un wallet dedie au bot, pas ton wallet principal.

---

## Etape 2 — Generer le fichier .env (en local)

Sur ta machine locale, clone le projet et genere la config :

```bash
git clone <ton-repo> polymarket-bot
cd polymarket-bot

# Installe bcrypt localement (necessaire pour le script)
pip install bcrypt

# Lance le generateur interactif
python deploy/generate_env.py
```

Le script te demandera :
- Ta cle privee Polygon
- Ton adresse funder (optionnel pour wallet EOA)
- Un mot de passe pour le dashboard

Il generera automatiquement le `DASHBOARD_SECRET_KEY` et le hash bcrypt du mot de passe.

> **Resultat** : un fichier `.env` a la racine du projet. Ne le commit JAMAIS.

---

## Etape 3 — Configurer le VPS Hostinger

### 3a. Installer le template Docker

1. Connecte-toi a **hPanel** (panel.hostinger.com)
2. Va dans **VPS > Manage**
3. Dans le menu gauche : **OS & Panel > Operating System**
4. Cherche **"Docker"** et selectionne le template **Ubuntu 24.04 avec Docker**
5. Clique **Change OS** et attends ~5 minutes

### 3b. Securiser le VPS

Connecte-toi en SSH (via le terminal hPanel ou depuis ta machine) :

```bash
ssh root@<IP_VPS>
```

**Configurer le firewall (via hPanel — recommande) :**

1. hPanel > VPS > **Security > Firewall**
2. Clique **Add Firewall**
3. Ajoute ces regles :

| Action | Protocol | Port   | Source        |
|--------|----------|--------|---------------|
| Accept | TCP      | 22     | Ton IP seule* |
| Accept | TCP      | 8080   | Anywhere      |
| Drop   | All      | All    | Anywhere      |

> *Optionnel mais recommande : restreindre SSH a ton IP fixe.

**Alternative — UFW en ligne de commande :**

```bash
# IMPORTANT : autoriser SSH AVANT d'activer le firewall
sudo ufw allow ssh
sudo ufw allow 8080/tcp
sudo ufw enable

# Optionnel : limiter les tentatives SSH (anti brute-force)
sudo ufw limit ssh
```

### 3c. Creer un utilisateur dedie (optionnel mais recommande)

```bash
# Creer l'utilisateur
adduser botuser

# L'ajouter au groupe docker
usermod -aG docker botuser

# Se connecter avec ce compte
su - botuser
```

---

## Etape 4 — Deployer le projet

### Methode A — Via Git (recommandee)

```bash
# Sur le VPS
cd /home/botuser  # ou /root si pas de botuser
git clone <ton-repo> polymarket-bot
cd polymarket-bot
```

Transfère le .env depuis ta machine locale :

```bash
# Depuis ta machine locale
scp .env root@<IP_VPS>:/home/botuser/polymarket-bot/.env
# ou si tu utilises root :
scp .env root@<IP_VPS>:/root/polymarket-bot/.env
```

### Methode B — Via le Docker Manager hPanel

1. hPanel > VPS > **Docker Manager**
2. Clique **Create project**
3. Choisis **Compose from URL** et colle l'URL de ton repo Git
4. Ajoute les variables d'environnement une par une dans l'interface
5. Lance le deploiement

> Note : la methode A est plus flexible pour gerer le .env.

### Lancer le setup

```bash
cd polymarket-bot
bash deploy/setup.sh
```

Le script va :
1. Verifier que Docker est installe
2. Verifier que le .env existe
3. Construire les images Docker
4. Lancer les conteneurs

---

## Etape 5 — Verifier que tout fonctionne

```bash
# Etat des conteneurs
docker compose ps

# Resultat attendu :
# polymarket-bot        running
# polymarket-dashboard  running

# Logs du bot en temps reel
docker compose logs -f bot

# Tester le dashboard
curl http://localhost:8080/health
# Resultat attendu : {"kill_switch":false,"last_balance":null,"status":"ok"}
```

Ouvre ton navigateur : `http://<IP_VPS>:8080`
- Tu devrais voir la page de login
- Connecte-toi avec le mot de passe choisi a l'etape 2

---

## Commandes de gestion quotidienne

```bash
cd /home/botuser/polymarket-bot  # ou /root/polymarket-bot

# ── Voir l'etat ──────────────────────────────────────────
docker compose ps                    # Etat des conteneurs
docker compose logs -f bot           # Logs bot (temps reel)
docker compose logs -f dashboard     # Logs dashboard
docker compose logs --tail 100 bot   # 100 derniers logs du bot

# ── Controle ─────────────────────────────────────────────
docker compose restart bot           # Redemarrer le bot
docker compose restart dashboard     # Redemarrer le dashboard
docker compose down                  # Tout arreter
docker compose up -d                 # Tout relancer

# ── Mise a jour du code ──────────────────────────────────
git pull                             # Recuperer les changements
docker compose build --no-cache      # Reconstruire les images
docker compose up -d                 # Relancer avec le nouveau code

# ── Backup de la base de donnees ─────────────────────────
docker compose cp bot:/app/data/bot.db ./backup_bot.db

# ── Voir les ressources utilisees ────────────────────────
docker stats --no-stream
```

---

## Monitoring externe (optionnel)

### Uptime Kuma (deploiement 1-clic depuis le catalogue Hostinger)

1. hPanel > VPS > **Docker Manager > Catalogue**
2. Cherche **Uptime Kuma** et deploie-le (port 3001 par defaut)
3. Accede a `http://<IP_VPS>:3001`
4. Ajoute un moniteur HTTP :
   - URL : `http://polymarket-dashboard:8080/health` (reseau Docker interne)
   - Ou : `http://localhost:8080/health`
   - Intervalle : 60 secondes
5. Configure les alertes (Telegram, email, Discord...)

> Si tu deploies Uptime Kuma, ouvre le port 3001 dans le firewall.

---

## Securite — Recapitulatif

| Element                 | Mesure                                              |
|-------------------------|-----------------------------------------------------|
| Cle privee              | Dans `.env`, jamais dans le code ni git              |
| Mot de passe dashboard  | Hashe en bcrypt, seul le hash est stocke             |
| Acces SSH               | Restreint par IP si possible + rate limiting         |
| Ports ouverts           | Uniquement 22 (SSH) et 8080 (dashboard)              |
| Wallet                  | Dedie au bot, capital limite (50$)                   |
| Dashboard               | Protege par login, sessions Flask signees            |
| Docker                  | `restart: unless-stopped` + logs limites a 30 Mo     |
| Kill switch             | Accessible depuis le dashboard, arret immediat       |

---

## Depannage

### Le bot ne demarre pas
```bash
docker compose logs bot
# Chercher "FATAL: variable d'environnement manquante"
# → Verifier le .env
```

### Le dashboard affiche une erreur 500
```bash
docker compose logs dashboard
# Verifier DASHBOARD_SECRET_KEY et DASHBOARD_PASSWORD_HASH dans .env
```

### Impossible de se connecter au dashboard
```bash
# Verifier que le port est ouvert
curl http://localhost:8080/health

# Si OK en local mais pas depuis l'exterieur → probleme firewall
sudo ufw status
# ou verifier les regles dans hPanel > Security > Firewall
```

### Le bot perd la connexion a Polymarket
Le bot gere automatiquement les reconnexions (jusqu'a 5 tentatives).
Verifier les logs :
```bash
docker compose logs --tail 50 bot | grep -i "connexion\|erreur\|reconnex"
```

### Reconstruire apres une mise a jour
```bash
git pull
docker compose down
docker compose build --no-cache
docker compose up -d
```
