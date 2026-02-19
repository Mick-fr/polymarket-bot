#!/usr/bin/env python3
"""
Script de generation du fichier .env.
Execute-le en local AVANT de deployer sur le VPS.

Usage :
    python deploy/generate_env.py

Il te demandera interactivement :
  - Ta cle privee Polymarket
  - Ton adresse funder (optionnel)
  - Le mot de passe du dashboard

Et generera automatiquement :
  - DASHBOARD_SECRET_KEY (aleatoire)
  - DASHBOARD_PASSWORD_HASH (bcrypt)
"""

import secrets
import sys
from pathlib import Path


def main():
    print("=" * 55)
    print("  POLYMARKET BOT — Generation du fichier .env")
    print("=" * 55)
    print()

    # --- Verifier que bcrypt est installe ---
    try:
        import bcrypt
    except ImportError:
        print("ERREUR: bcrypt n'est pas installe.")
        print("Installe-le avec: pip install bcrypt")
        sys.exit(1)

    # --- Cle privee ---
    print("[1/4] Cle privee du wallet Polygon")
    print("      (SANS le prefixe 0x)")
    private_key = input("      > ").strip()
    if not private_key:
        print("ERREUR: la cle privee est obligatoire.")
        sys.exit(1)
    # Retire le prefixe 0x si present
    if private_key.startswith("0x"):
        private_key = private_key[2:]
    print()

    # --- Funder address ---
    print("[2/4] Adresse funder (wallet qui detient les fonds)")
    print("      Laisser vide si tu utilises un wallet EOA standard")
    funder = input("      > ").strip()
    print()

    # --- Signature type ---
    print("[3/4] Type de signature")
    print("      0 = EOA standard (MetaMask, hardware wallet)")
    print("      1 = Email/Magic wallet")
    print("      2 = Browser proxy wallet")
    sig_type = input("      > [0]: ").strip() or "0"
    print()

    # --- Mot de passe dashboard ---
    print("[4/4] Mot de passe pour le dashboard web")
    password = input("      > ").strip()
    if not password:
        print("ERREUR: le mot de passe est obligatoire.")
        sys.exit(1)
    print()

    # --- Generation automatique ---
    secret_key = secrets.token_hex(32)
    password_hash = bcrypt.hashpw(
        password.encode("utf-8"), bcrypt.gensalt()
    ).decode("utf-8")

    # --- Ecriture du fichier ---
    env_content = f"""# ══════════════════════════════════════════════════════════
# POLYMARKET BOT — Configuration (genere automatiquement)
# ══════════════════════════════════════════════════════════

# ── Polymarket ───────────────────────────────────────────
POLYMARKET_PRIVATE_KEY={private_key}
POLYMARKET_FUNDER_ADDRESS={funder}
POLYMARKET_SIGNATURE_TYPE={sig_type}

# ── Bot ──────────────────────────────────────────────────
BOT_LOOP_INTERVAL=60
BOT_MAX_ORDER_SIZE=5.0
BOT_MAX_DAILY_LOSS=10.0
BOT_MAX_RETRIES=5
BOT_RETRY_DELAY=30

# ── Dashboard ────────────────────────────────────────────
DASHBOARD_PORT=8080
DASHBOARD_SECRET_KEY={secret_key}
DASHBOARD_PASSWORD_HASH={password_hash}

# ── General ──────────────────────────────────────────────
LOG_LEVEL=INFO
"""

    env_path = Path(__file__).resolve().parent.parent / ".env"
    env_path.write_text(env_content, encoding="utf-8")

    print("=" * 55)
    print(f"  .env genere avec succes : {env_path}")
    print("=" * 55)
    print()
    print("  IMPORTANT:")
    print("  - Ne commit JAMAIS ce fichier dans git")
    print("  - Transfère-le sur ton VPS via SCP ou copie manuelle")
    print("  - Le mot de passe dashboard est hashe en bcrypt")
    print()


if __name__ == "__main__":
    main()
