"""
update_market_prices.py — lightweight intraday market data refresh.

Fetches ONLY Yahoo Finance quotes and ZQ Fed Funds Futures.
Updates:
  data/macro_data.json  — market_anchors block + meta.last_updated
  data/fomc_probs.json  — fully recomputed from fresh futures prices

Does NOT touch FRED economic indicators, chart_data.json,
t5yie_data.json, or the AI narrative (synthesis.json).

Run standalone:  python3 scripts/update_market_prices.py
Called from:     GitHub Actions intraday-market-update job (21:00 UTC Mon-Fri)
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
from scripts.yahoo_client import get_quote
from scripts.fomc_probabilities import compute_probabilities

ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"
MACRO_JSON = DATA / "macro_data.json"
FOMC_JSON  = DATA / "fomc_probs.json"


def get_10y_yield():
    """
    Fetch 10Y Treasury yield (^TNX) and daily bps change.
    ^TNX last_price is in percentage points (e.g. 4.35 = 4.35%).
    """
    try:
        t = yf.Ticker("^TNX")
        info = t.fast_info
        price = getattr(info, "last_price", None)
        prev  = getattr(info, "previous_close", None)
        if price is None:
            return None
        delta_bps = round((price - prev) * 100) if prev is not None else None
        delta_dir = "up" if (delta_bps or 0) >= 0 else "down"
        return {
            "value":     round(float(price), 2),
            "delta_bps": delta_bps,
            "delta_dir": delta_dir,
        }
    except Exception as e:
        print(f"  [yahoo] WARNING: ^TNX failed — {e}")
        return None


def main():
    now_utc = datetime.now(timezone.utc)
    ts = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    print("=== Intraday Market Price Update ===")
    print(f"  UTC: {ts}")

    # ── Load existing macro_data.json ────────────────────────────────
    if not MACRO_JSON.exists():
        print("ERROR: macro_data.json not found — run fetch_data.py first")
        sys.exit(1)

    with open(MACRO_JSON) as f:
        macro = json.load(f)

    # ── Yahoo Finance quotes ─────────────────────────────────────────
    print("\n--- Yahoo Finance quotes ---")
    spx       = get_quote("^GSPC")
    vix       = get_quote("^VIX")
    dxy       = get_quote("DX-Y.NYB")
    wti       = get_quote("CL=F")
    yield_10y = get_10y_yield()

    for name, q in [("SPX", spx), ("VIX", vix), ("DXY", dxy),
                    ("WTI", wti), ("10Y Yield", yield_10y)]:
        print(f"  {name}: {q if q else 'unavailable'}")

    # ── Patch market_anchors (only fields we fetched; keep others) ───
    ma = macro.setdefault("market_anchors", {})

    if spx:
        ma["spx"] = {
            "value":     round(spx["price"]),
            "delta_pct": spx["delta_pct"],
            "delta_dir": spx["delta_dir"],
        }
    if yield_10y:
        ma["yield_10y"] = yield_10y
    if dxy:
        ma["dxy"] = {
            "value":     dxy["price"],
            "delta_pct": dxy["delta_pct"],
            "delta_dir": dxy["delta_dir"],
        }
    if wti:
        ma["wti"] = {
            "value":     wti["price"],
            "delta_pct": wti["delta_pct"],
            "delta_dir": wti["delta_dir"],
        }
    if vix:
        ma["vix"] = {
            "value":     vix["price"],
            "delta_pct": vix["delta_pct"],
            "delta_dir": vix["delta_dir"],
        }

    macro.setdefault("meta", {})["last_updated"] = ts

    with open(MACRO_JSON, "w") as f:
        json.dump(macro, f, indent=2)
    print(f"\n  [write] macro_data.json — market_anchors updated")

    # ── FOMC probabilities ───────────────────────────────────────────
    print("\n--- FOMC probabilities (ZQ futures) ---")
    current_rate_low  = 3.50
    current_rate_high = 3.75
    if FOMC_JSON.exists():
        with open(FOMC_JSON) as f:
            prev_fomc = json.load(f)
        current_rate_low  = prev_fomc.get("meta", {}).get("current_rate_low",  3.50)
        current_rate_high = prev_fomc.get("meta", {}).get("current_rate_high", 3.75)

    meetings = compute_probabilities(current_rate_low, current_rate_high)

    fomc_data = {
        "meta": {
            "last_updated":      ts,
            "current_rate_low":  current_rate_low,
            "current_rate_high": current_rate_high,
        },
        "meetings": meetings,
    }
    with open(FOMC_JSON, "w") as f:
        json.dump(fomc_data, f, indent=2)
    print(f"  [write] fomc_probs.json updated")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
