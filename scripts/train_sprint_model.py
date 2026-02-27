#!/usr/bin/env python3
"""
Upgrade 2 — Training XGBoost model for BTC sprint market direction prediction.

Source de données : Binance 1-min klines BTCUSDT (6 mois, ~260k candles).
Pas besoin de données Polymarket : le marché "Bitcoin Up or Down 5 min"
résout exactement sur la clôture vs l'ouverture du créneau 5 min BTC.

Features calculées à T+2min dans la fenêtre (= entrée standard du bot à t≈180s) :
  - mom_30s   : momentum 30s (approximé par le return de la 1ère kline)
  - mom_60s   : momentum 60s (return sur 2 klines)
  - mom_300s  : momentum 5min (return sur 5 klines — fenêtre entière)
  - vol_60s   : volatilité annualisée sur 2 klines de 1 min
  - hour_sin  : encodage cyclique de l'heure UTC (sin)
  - hour_cos  : encodage cyclique de l'heure UTC (cos)

Target : int(window_close > window_open) — direction du créneau 5 min BTC.

Usage :
    pip install xgboost scikit-learn pandas requests
    python scripts/train_sprint_model.py

Output :
    bot/models/sprint_xgb.pkl   — modèle calibré prêt pour l'inférence
    bot/models/sprint_features.json — liste ordonnée des features
"""

import json
import math
import os
import pickle
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# ── Chemins ────────────────────────────────────────────────────────────────────
REPO_ROOT    = Path(__file__).resolve().parent.parent
MODEL_DIR    = REPO_ROOT / "bot" / "models"
MODEL_PATH   = MODEL_DIR / "sprint_xgb.pkl"
FEATURE_PATH = MODEL_DIR / "sprint_features.json"
CACHE_PATH   = MODEL_DIR / "_klines_cache.pkl"   # évite de re-télécharger

FEATURE_COLS = ["mom_30s", "mom_60s", "mom_300s", "vol_60s", "hour_sin", "hour_cos"]

# ── Paramètres ────────────────────────────────────────────────────────────────
MONTHS_HISTORY = 6
SYMBOL         = "BTCUSDT"


# ─────────────────────────────────────────────────────────────────────────────
# 1. Fetch Binance 1-min klines
# ─────────────────────────────────────────────────────────────────────────────

def fetch_binance_1m(symbol: str = SYMBOL, months: int = MONTHS_HISTORY) -> pd.DataFrame:
    """Télécharge les klines 1-min Binance pour les N derniers mois.

    Utilise un cache local (CACHE_PATH) pour éviter de tout re-télécharger.
    Le cache est invalidé s'il a plus de 24h.
    """
    if CACHE_PATH.exists():
        age_h = (time.time() - CACHE_PATH.stat().st_mtime) / 3600
        if age_h < 24:
            print(f"[Cache] Chargement klines depuis cache ({age_h:.1f}h)…")
            with open(CACHE_PATH, "rb") as f:
                return pickle.load(f)

    url     = "https://api.binance.com/api/v3/klines"
    end_ms  = int(time.time() * 1000)
    start_ms = end_ms - months * 30 * 24 * 60 * 60 * 1000

    all_rows = []
    current  = start_ms
    limit    = 1000
    print(f"[Fetch] Téléchargement klines 1m {symbol} ({months} mois)…")

    while current < end_ms:
        resp = requests.get(url, params={
            "symbol":    symbol,
            "interval":  "1m",
            "startTime": current,
            "endTime":   end_ms,
            "limit":     limit,
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        all_rows.extend(data)
        current = data[-1][0] + 60_000
        if len(data) < limit:
            break
        time.sleep(0.05)  # rate-limit friendly

    print(f"[Fetch] {len(all_rows):,} klines téléchargées.")

    df = pd.DataFrame(all_rows, columns=[
        "ts_open", "open", "high", "low", "close", "volume",
        "ts_close", "quote_vol", "n_trades",
        "taker_buy_base", "taker_buy_quote", "_ignore",
    ])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    df["ts_open"] = pd.to_datetime(df["ts_open"], unit="ms", utc=True)
    df = df.set_index("ts_open").sort_index()
    df = df[~df.index.duplicated(keep="first")]

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, "wb") as f:
        pickle.dump(df, f)
    print(f"[Cache] Sauvegardé → {CACHE_PATH}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2. Feature engineering
# ─────────────────────────────────────────────────────────────────────────────

def build_features(df_1m: pd.DataFrame) -> pd.DataFrame:
    """Construit le dataset (features + target) à partir des klines 1-min.

    Pour chaque créneau 5-min, on simule l'entrée à T+2min (minute 2 de la fenêtre)
    et on calcule les features disponibles à ce moment.

    Convention :
        - window_open_price = open de la kline minute 0 du créneau
        - entry_kline = kline minute 2 (index i=2 dans le créneau de 5)
        - window_close_price = close de la kline minute 4
        - target = int(window_close_price > window_open_price)
    """
    df_1m = df_1m.reset_index()
    close = df_1m["close"].values
    open_ = df_1m["open"].values
    ts    = df_1m["ts_open"].values

    n  = len(df_1m)
    rows = []

    # Besoin d'au moins 30 klines d'historique + 2 klines futures
    for i in range(30, n - 2):
        dt = pd.Timestamp(ts[i])
        # Seulement les klines à minute ≡ 2 (mod 5) dans le créneau
        if dt.minute % 5 != 2:
            continue

        window_open_idx  = i - 2   # kline minute 0 du créneau
        window_close_idx = i + 2   # kline minute 4 du créneau

        if window_open_idx < 30 or window_close_idx >= n:
            continue

        window_open_price  = open_[window_open_idx]
        window_close_price = close[window_close_idx]
        target = int(window_close_price > window_open_price)

        # ── Features ──────────────────────────────────────────────────────────
        # mom_30s  ≈ return de la kline juste avant l'entrée (1 min)
        c_now  = close[i]
        c_1m   = close[i - 1]   # 1 min ago
        c_5m   = close[i - 5]   # 5 min ago
        c_30m  = close[i - 30]  # 30 min ago

        mom_30s  = (c_now - c_1m)  / c_1m  * 100.0 if c_1m  > 0 else 0.0
        mom_60s  = (c_now - c_1m)  / c_1m  * 100.0  # même resolution pour 1-min klines
        mom_300s = (c_now - c_5m)  / c_5m  * 100.0 if c_5m  > 0 else 0.0

        # vol_60s : std des log-returns sur les 2 dernières klines (approximation)
        log_rets = []
        for j in range(max(1, i - 1), i + 1):
            if close[j-1] > 0 and close[j] > 0:
                log_rets.append(math.log(close[j] / close[j-1]))
        vol_60s = float(np.std(log_rets)) * math.sqrt(525_600) if log_rets else 0.60
        vol_60s = max(0.10, min(3.0, vol_60s))

        # Encodage cyclique de l'heure UTC
        hour = dt.hour + dt.minute / 60.0
        hour_sin = math.sin(2 * math.pi * hour / 24.0)
        hour_cos = math.cos(2 * math.pi * hour / 24.0)

        rows.append({
            "mom_30s":  mom_30s,
            "mom_60s":  mom_60s,
            "mom_300s": mom_300s,
            "vol_60s":  vol_60s,
            "hour_sin": hour_sin,
            "hour_cos": hour_cos,
            "target":   target,
        })

    df_feat = pd.DataFrame(rows)
    print(f"[Features] {len(df_feat):,} exemples générés. "
          f"Taux UP={df_feat['target'].mean():.3f} (base=0.50).")
    return df_feat


# ─────────────────────────────────────────────────────────────────────────────
# 3. Training XGBoost avec TimeSeriesSplit + calibration Platt
# ─────────────────────────────────────────────────────────────────────────────

def train_model(df_feat: pd.DataFrame):
    """Entraîne et calibre un XGBClassifier avec validation temporelle.

    Retourne le modèle calibré CalibratedClassifierCV(XGBClassifier, method='sigmoid').
    La calibration Platt garantit que predict_proba(X)[:,1] est une vraie probabilité
    utilisable directement comme p_true dans la stratégie.
    """
    from xgboost import XGBClassifier
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
    from sklearn.model_selection import TimeSeriesSplit

    X = df_feat[FEATURE_COLS].values.astype(np.float32)
    y = df_feat["target"].values.astype(np.int32)

    print(f"\n[Train] {len(X):,} exemples | {len(FEATURE_COLS)} features")
    print(f"[Train] Features: {FEATURE_COLS}")

    xgb_params = dict(
        n_estimators    = 300,
        max_depth       = 4,
        learning_rate   = 0.03,
        subsample       = 0.8,
        colsample_bytree= 0.8,
        min_child_weight= 20,   # régularisation pour éviter overfit sur 5-min noise
        gamma           = 0.1,
        eval_metric     = "logloss",
        random_state    = 42,
        n_jobs          = -1,
    )

    tscv = TimeSeriesSplit(n_splits=5)
    fold_metrics = []

    print("\n[CV] TimeSeriesSplit (5 folds) :")
    for fold, (tr_idx, te_idx) in enumerate(tscv.split(X)):
        X_tr, X_te = X[tr_idx], X[te_idx]
        y_tr, y_te = y[tr_idx], y[te_idx]
        m = XGBClassifier(**xgb_params)
        m.fit(X_tr, y_tr)
        proba = m.predict_proba(X_te)[:, 1]
        bs  = brier_score_loss(y_te, proba)
        ll  = log_loss(y_te, proba)
        auc = roc_auc_score(y_te, proba)
        fold_metrics.append({"brier": bs, "logloss": ll, "auc": auc})
        print(f"  Fold {fold+1}: Brier={bs:.4f}  LogLoss={ll:.4f}  AUC={auc:.4f}")

    mean_bs  = np.mean([m["brier"]   for m in fold_metrics])
    mean_auc = np.mean([m["auc"]     for m in fold_metrics])
    print(f"\n  Moyenne: Brier={mean_bs:.4f} (baseline=0.25)  AUC={mean_auc:.4f} (baseline=0.50)")

    if mean_auc < 0.51:
        print("\n⚠️  AUC < 0.51 : le modèle n'est pas significativement meilleur que random.")
        print("   Vérifier les features ou augmenter l'historique.")

    # Entraînement final sur tout le dataset + calibration Platt
    print("\n[Train] Entraînement final + calibration Platt (cv=3)…")
    base = XGBClassifier(**xgb_params)
    calibrated = CalibratedClassifierCV(base, cv=3, method="sigmoid")
    calibrated.fit(X, y)

    # Feature importance (via le modèle non-calibré entraîné sur tout)
    base_full = XGBClassifier(**xgb_params)
    base_full.fit(X, y)
    importances = base_full.feature_importances_
    print("\n[Importance] Feature importances :")
    for feat, imp in sorted(zip(FEATURE_COLS, importances), key=lambda x: -x[1]):
        bar = "█" * int(imp * 50)
        print(f"  {feat:<12} {imp:.4f}  {bar}")

    return calibrated


# ─────────────────────────────────────────────────────────────────────────────
# 4. Sauvegarde
# ─────────────────────────────────────────────────────────────────────────────

def save_model(model, feature_cols: list):
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"model": model, "features": feature_cols}, f)
    with open(FEATURE_PATH, "w") as f:
        json.dump(feature_cols, f, indent=2)
    print(f"\n✅  Modèle sauvegardé → {MODEL_PATH}")
    print(f"✅  Features  sauvegardées → {FEATURE_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Quick sanity-check
# ─────────────────────────────────────────────────────────────────────────────

def sanity_check(model):
    """Vérifie que le modèle produit des probabilités sensées sur des exemples synthétiques."""
    test_cases = [
        # mom_30s, mom_60s, mom_300s, vol_60s, hour_sin, hour_cos
        [ 0.10,  0.15,  0.30, 0.60,  0.0,  1.0],   # fort momentum haussier → P(UP) élevé
        [-0.10, -0.15, -0.30, 0.60,  0.0,  1.0],   # fort momentum baissier → P(UP) faible
        [ 0.00,  0.00,  0.00, 0.60,  0.0,  1.0],   # neutre → P(UP) ≈ 0.50
    ]
    X_test = np.array(test_cases, dtype=np.float32)
    probas  = model.predict_proba(X_test)[:, 1]
    labels  = ["UP fort", "DOWN fort", "Neutre  "]
    print("\n[Sanity] Vérification sur cas synthétiques :")
    for label, p in zip(labels, probas):
        bar = "▓" * int(p * 40)
        print(f"  {label}: P(UP)={p:.3f}  {bar}")

    ok = probas[0] > 0.52 and probas[1] < 0.48
    if ok:
        print("  ✅  Sanity check passé (UP > 0.52, DOWN < 0.48)")
    else:
        print("  ⚠️  Sanity check échoué — vérifier calibration ou features")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Upgrade 2 — Sprint XGBoost Model Training")
    print("=" * 60)

    df_klines  = fetch_binance_1m()
    df_feat    = build_features(df_klines)
    model      = train_model(df_feat)
    sanity_check(model)
    save_model(model, FEATURE_COLS)

    print("\nProchaine étape : redémarrer le bot (docker-compose restart bot)")
    print("Le modèle sera chargé automatiquement depuis bot/models/sprint_xgb.pkl")
