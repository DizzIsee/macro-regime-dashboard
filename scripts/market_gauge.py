"""
market_gauge.py — SMA positioning & risk bias calculator.

Fetches 400 days of daily close history for SPX, QQQ, IWM, BTC via yfinance.
Calculates SMA 10/20/50/200, consecutive-days counts, composite scores,
and a portfolio-weighted bias score. Writes data/market_gauge.json.

Run standalone:  python3 scripts/market_gauge.py
Called from:     scripts/fetch_data.py (Phase 10)
                 scripts/update_market_prices.py (intraday + weekend BTC)
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import yfinance as yf
    import pandas as pd
except ImportError as e:
    print(f"ERROR: missing dependency — {e}")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)
OUTPUT = DATA / "market_gauge.json"

# ── asset definitions ─────────────────────────────────────────────────────────
ASSETS = [
    {"ticker": "SPX",   "yf_ticker": "^GSPC",   "type": "equity", "weight": 0.35},
    {"ticker": "QQQ",   "yf_ticker": "QQQ",      "type": "equity", "weight": 0.25},
    {"ticker": "IWM",   "yf_ticker": "IWM",      "type": "equity", "weight": 0.20},
    {"ticker": "BTC",   "yf_ticker": "BTC-USD",  "type": "crypto", "weight": 0.20},
]

BIAS_LABELS = [
    (85, "FULL BULL"),
    (70, "RISK ON"),
    (50, "MIXED"),
    (30, "RISK OFF"),
    (0,  "BEAR MARKET"),
]

INTERPRETATION = {
    "FULL BULL":   "All major assets in full bull alignment — size up aggressively on A+ setups",
    "RISK ON":     "Risk-on environment — run full playbook with normal sizing",
    "MIXED":       "Mixed signals — reduce size, skip B setups, favor stronger assets",
    "RISK OFF":    "Risk-off — minimal sizing, cash is a position",
    "BEAR MARKET": "Bear market conditions — no longs, bear rallies only",
}


# ── helpers ───────────────────────────────────────────────────────────────────

def bias_label(score):
    for threshold, label in BIAS_LABELS:
        if score >= threshold:
            return label
    return "BEAR MARKET"


def consecutive_days(series, sma_series):
    """
    Count consecutive days the close has been on the same side of an SMA.
    Returns a positive integer always — direction is captured in 'above'.
    """
    count = 0
    above = series.iloc[-1] > sma_series.iloc[-1]
    for i in range(len(series) - 1, -1, -1):
        if pd.isna(series.iloc[i]) or pd.isna(sma_series.iloc[i]):
            break
        if (series.iloc[i] > sma_series.iloc[i]) == above:
            count += 1
        else:
            break
    return count


def vix_score(vix):
    if vix < 15:   return 100
    if vix < 20:   return 75
    if vix < 25:   return 50
    if vix < 30:   return 25
    return 0


def distance_score(dist_200):
    """Normalize -20%..+20% → 0..100 for BTC."""
    return min(max((dist_200 + 20) / 40 * 100, 0), 100)


# ── VIX fetch (once, reused for all equities) ─────────────────────────────────

def fetch_vix():
    try:
        t = yf.Ticker("^VIX")
        info = t.fast_info
        price = getattr(info, "last_price", None)
        if price is not None:
            v = round(float(price), 2)
            print(f"  VIX: {v}")
            return v
    except Exception as e:
        print(f"  WARNING: VIX fetch failed — {e}")
    return 20.0   # neutral fallback


# ── per-asset calculation ─────────────────────────────────────────────────────

def compute_asset(asset, vix_val):
    ticker   = asset["ticker"]
    yf_tick  = asset["yf_ticker"]
    is_crypto = asset["type"] == "crypto"

    print(f"\n  [{ticker}] downloading 400d history from yfinance ({yf_tick})...")
    try:
        df = yf.download(yf_tick, period="400d", interval="1d",
                         auto_adjust=True, progress=False)
    except Exception as e:
        print(f"  [{ticker}] ERROR: download failed — {e}")
        return None

    if df is None or df.empty:
        print(f"  [{ticker}] WARNING: empty data returned — skipping")
        return None

    # Flatten MultiIndex columns if present (yfinance sometimes returns them)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    if "Close" not in df.columns:
        print(f"  [{ticker}] WARNING: no Close column — skipping")
        return None

    close = df["Close"].dropna()
    if len(close) < 200:
        print(f"  [{ticker}] WARNING: only {len(close)} rows, need ≥200 — skipping")
        return None

    # SMAs
    sma10  = close.rolling(10).mean()
    sma20  = close.rolling(20).mean()
    sma50  = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()

    cur_close = float(close.iloc[-1])
    cur_10    = float(sma10.iloc[-1])
    cur_20    = float(sma20.iloc[-1])
    cur_50    = float(sma50.iloc[-1])
    cur_200   = float(sma200.iloc[-1])

    above_10  = cur_close > cur_10
    above_20  = cur_close > cur_20
    above_50  = cur_close > cur_50
    above_200 = cur_close > cur_200

    dist_10  = round(((cur_close - cur_10)  / cur_10)  * 100, 2)
    dist_20  = round(((cur_close - cur_20)  / cur_20)  * 100, 2)
    dist_50  = round(((cur_close - cur_50)  / cur_50)  * 100, 2)
    dist_200 = round(((cur_close - cur_200) / cur_200) * 100, 2)

    days_10  = consecutive_days(close, sma10)
    days_20  = consecutive_days(close, sma20)
    days_50  = consecutive_days(close, sma50)
    days_200 = consecutive_days(close, sma200)

    # Composite score
    sma_score_val = ((int(above_10) + int(above_20) +
                      int(above_50) + int(above_200)) / 4) * 100

    align = 0
    if cur_10  > cur_20:  align += 1
    if cur_20  > cur_50:  align += 1
    if cur_50  > cur_200: align += 1
    if cur_close > cur_10: align += 1
    alignment_score_val = (align / 4) * 100

    if is_crypto:
        third_score = distance_score(dist_200)
        third_label = "distance_score"
    else:
        third_score = vix_score(vix_val)
        third_label = "vix_score"

    composite = round(
        sma_score_val * 0.55 +
        alignment_score_val * 0.25 +
        third_score * 0.20
    )
    composite = max(0, min(100, composite))

    # as_of date
    last_date = close.index[-1]
    if hasattr(last_date, "strftime"):
        date_str = last_date.strftime("%b %-d, %Y")
    else:
        date_str = str(last_date)[:10]

    if is_crypto:
        as_of = date_str
    else:
        as_of = date_str + " close"

    print(f"  [{ticker}] close={cur_close:.2f}  sma10={cur_10:.2f}  sma20={cur_20:.2f}"
          f"  sma50={cur_50:.2f}  sma200={cur_200:.2f}")
    print(f"  [{ticker}] above=({int(above_10)},{int(above_20)},{int(above_50)},{int(above_200)})"
          f"  days=({days_10},{days_20},{days_50},{days_200})"
          f"  align={align}/4  {third_label}={third_score:.1f}"
          f"  composite={composite}")

    return {
        "ticker":          ticker,
        "yf_ticker":       yf_tick,
        "close":           round(cur_close, 2),
        "as_of":           as_of,
        "composite_score": composite,
        "bias":            bias_label(composite),
        "alignment":       align,
        "vix":             vix_val if not is_crypto else None,
        "sma": {
            "sma10":  {"value": round(cur_10, 2),  "above": above_10,  "distance_pct": dist_10,  "days_count": days_10},
            "sma20":  {"value": round(cur_20, 2),  "above": above_20,  "distance_pct": dist_20,  "days_count": days_20},
            "sma50":  {"value": round(cur_50, 2),  "above": above_50,  "distance_pct": dist_50,  "days_count": days_50},
            "sma200": {"value": round(cur_200, 2), "above": above_200, "distance_pct": dist_200, "days_count": days_200},
        },
    }


# ── main ──────────────────────────────────────────────────────────────────────

def run_market_gauge(btc_only=False):
    """
    Compute and write market_gauge.json.
    btc_only=True: only recalculate BTC (weekend cron job).
    """
    now_utc = datetime.now(timezone.utc)
    ts = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    print("=== Market Trend Gauge ===")
    print(f"  UTC: {ts}  btc_only={btc_only}")

    # Load existing data if btc_only so equity assets are preserved
    existing_assets = []
    existing_meta   = {}
    if btc_only and OUTPUT.exists():
        with open(OUTPUT) as f:
            prev = json.load(f)
        existing_assets = [a for a in prev.get("assets", [])
                           if a["ticker"] != "BTC"]
        existing_meta   = prev.get("meta", {})

    vix_val = None if btc_only else fetch_vix()
    if vix_val is None and not btc_only:
        vix_val = 20.0  # neutral fallback

    assets_to_run = [a for a in ASSETS if (not btc_only or a["ticker"] == "BTC")]
    results = []

    for asset in assets_to_run:
        # Pass a dummy VIX for BTC (unused in its score calculation)
        v = vix_val if asset["type"] == "equity" else 20.0
        result = compute_asset(asset, v)
        if result is not None:
            results.append(result)

    if btc_only:
        # Merge: keep existing equities, replace BTC
        all_results = existing_assets + results
    else:
        all_results = results

    if not all_results:
        print("WARNING: no asset data computed — not writing output")
        return

    # Portfolio score (weighted average of valid assets)
    weights = {a["ticker"]: a["weight"] for a in ASSETS}
    total_w = 0.0
    weighted_sum = 0.0
    for r in all_results:
        w = weights.get(r["ticker"], 0)
        weighted_sum += r["composite_score"] * w
        total_w += w
    portfolio_score = round(weighted_sum / total_w) if total_w > 0 else 0
    portfolio_bias  = bias_label(portfolio_score)

    output = {
        "meta": {
            "last_updated":    ts,
            "portfolio_bias":  portfolio_bias,
            "portfolio_score": portfolio_score,
            "interpretation":  INTERPRETATION[portfolio_bias],
        },
        "assets": all_results,
    }

    with open(OUTPUT, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n=== PORTFOLIO: {portfolio_bias} ({portfolio_score}/100) ===")
    print(f"  Written to {OUTPUT}")
    return output


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--btc-only", action="store_true",
                        help="Only refresh BTC (weekend cron)")
    args = parser.parse_args()
    result = run_market_gauge(btc_only=args.btc_only)
    if result:
        print("\n--- Full JSON output ---")
        print(json.dumps(result, indent=2))
