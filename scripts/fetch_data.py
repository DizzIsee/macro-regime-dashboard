"""
Main data pipeline — fetches all live data and writes:
  data/macro_data.json
  data/chart_data.json
  data/t5yie_data.json
  data/fomc_probs.json

Run: python scripts/fetch_data.py
Requires FRED_API_KEY in .env or environment.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Allow running from repo root or scripts/
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.fred_client import get_series, latest, yoy_pct_change, mom_change
from scripts.yahoo_client import get_quote, get_futures_price
from scripts.liquidity_score import (
    compute_us_liquidity_score,
    compute_global_liquidity_score,
    composite_score,
    percentile_rank,
)
from scripts.signal_scorer import compute_signals
from scripts.fomc_probabilities import compute_probabilities

# ── paths ──────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

STATIC_OVERRIDES = DATA / "static_overrides.json"
CHART_DATA = DATA / "chart_data.json"

# ── helpers ────────────────────────────────────────────────────────────────────
def fmt_date(iso_date: str) -> str:
    """'2026-01-01' → 'Jan 2026'"""
    try:
        return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%b %Y")
    except Exception:
        return iso_date


def tag_inflation(value: float, estimate: float, tolerance: float = 0.1) -> str:
    if abs(value - estimate) <= tolerance:
        return "inline"
    return "hot" if value > estimate else "cool"


def tag_growth(value: float, estimate: float) -> str:
    return "beat" if value >= estimate else "miss"


def tag_labor(value: float, estimate: float, tolerance: float = 0.1) -> str:
    """Unemployment: higher = worse = soft."""
    if abs(value - estimate) <= tolerance:
        return "inline"
    return "soft" if value > estimate else "inline"


def write_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  [write] {path.name} ({path.stat().st_size // 1024} KB)")


def load_static_overrides() -> dict:
    if STATIC_OVERRIDES.exists():
        with open(STATIC_OVERRIDES) as f:
            return json.load(f)
    return {}


# ── main ───────────────────────────────────────────────────────────────────────
def main():
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        print("ERROR: FRED_API_KEY not set. Add it to .env or environment.")
        sys.exit(1)

    now_utc = datetime.now(timezone.utc)
    static = load_static_overrides()

    print("\n=== Phase 1: Fetching FRED series ===")

    # ── Inflation ──────────────────────────────────────────────────────────────
    print("Fetching CPI...")
    cpi = yoy_pct_change("CPIAUCSL", api_key)
    print(f"  CPI YoY: {cpi}")

    print("Fetching Core CPI...")
    core_cpi = yoy_pct_change("CPILFESL", api_key)
    print(f"  Core CPI YoY: {core_cpi}")

    print("Fetching PPI...")
    ppi = yoy_pct_change("PPIACO", api_key)
    print(f"  PPI YoY: {ppi}")

    print("Fetching PCE...")
    pce = yoy_pct_change("PCEPI", api_key)
    print(f"  PCE YoY: {pce}")

    print("Fetching Core PCE...")
    core_pce = yoy_pct_change("PCEPILFE", api_key)
    print(f"  Core PCE YoY: {core_pce}")

    # ── Labor ──────────────────────────────────────────────────────────────────
    print("Fetching Unemployment...")
    unemp = latest("UNRATE", api_key)
    print(f"  UNRATE: {unemp}")

    print("Fetching Initial Claims...")
    claims = latest("ICSA", api_key)
    if claims:
        claims["value"] = round(claims["value"] / 1000, 1)  # convert to thousands
    print(f"  ICSA (K): {claims}")

    print("Fetching NFP...")
    nfp = mom_change("PAYEMS", api_key)
    print(f"  NFP MoM (K): {nfp}")

    # ── Sentiment ──────────────────────────────────────────────────────────────
    print("Fetching UMich Sentiment...")
    umich = latest("UMCSENT", api_key)
    print(f"  UMCSENT: {umich}")

    # ── Rates / Financial Conditions ───────────────────────────────────────────
    print("Fetching 10Y Treasury...")
    y10 = latest("DGS10", api_key)
    print(f"  DGS10: {y10}")

    print("Fetching Fed Funds Rate...")
    fed_funds = latest("FEDFUNDS", api_key)
    print(f"  FEDFUNDS: {fed_funds}")

    print("Fetching NFCI...")
    nfci = latest("NFCI", api_key)
    print(f"  NFCI: {nfci}")

    print("Fetching ANFCI...")
    anfci = latest("ANFCI", api_key)
    print(f"  ANFCI: {anfci}")

    print("Fetching HY OAS...")
    hy_oas = latest("BAMLH0A0HYM2", api_key)
    print(f"  HY OAS: {hy_oas}")

    print("Fetching IG BBB OAS...")
    ig_oas = latest("BAMLC0A4CBBB", api_key)
    print(f"  IG OAS: {ig_oas}")

    # ── Liquidity ──────────────────────────────────────────────────────────────
    print("Fetching WALCL (Fed balance sheet, weekly)...")
    # WALCL is in millions USD; fetch from 2015 to present
    walcl_series_raw = get_series("WALCL", api_key, observation_start="2015-01-01",
                                   limit=1000, sort_order="asc")
    # convert millions → billions
    walcl_series = [x["value"] / 1000 for x in walcl_series_raw]
    walcl_latest = walcl_series[-1] if walcl_series else None
    print(f"  WALCL latest (B): {walcl_latest} ({len(walcl_series)} obs)")

    print("Fetching TGA (WTREGEN, weekly)...")
    # WTREGEN is in millions USD; same date range
    tga_series_raw = get_series("WTREGEN", api_key, observation_start="2015-01-01",
                                 limit=1000, sort_order="asc")
    # convert millions → billions
    tga_series = [x["value"] / 1000 for x in tga_series_raw]
    tga_latest = tga_series[-1] if tga_series else None
    print(f"  TGA latest (B): {tga_latest} ({len(tga_series)} obs)")

    print("Fetching RRP (RRPONTSYD, daily)...")
    rrp_daily_raw = get_series("RRPONTSYD", api_key, observation_start="2015-01-01",
                                limit=3000, sort_order="asc")
    # RRPONTSYD is in billions — keep as is
    rrp_daily = [x["value"] for x in rrp_daily_raw]
    rrp_latest = rrp_daily[-1] if rrp_daily else None
    print(f"  RRP latest (B): {rrp_latest}")

    print("Fetching Excess Reserves (WRESBAL)...")
    reserves = latest("WRESBAL", api_key)
    print(f"  WRESBAL: {reserves}")

    print("Fetching ECB Assets (ECBASSETSW)...")
    ecb_series_raw = get_series("ECBASSETSW", api_key, observation_start="2015-01-01",
                                 limit=1000, sort_order="asc")
    print(f"  ECB series length: {len(ecb_series_raw)}")

    print("Fetching BOJ Assets (JPNASSETS)...")
    # JPNASSETS is in "hundred millions of yen" (億円), not billions
    boj_series_raw = get_series("JPNASSETS", api_key, observation_start="2015-01-01",
                                 limit=1000, sort_order="asc")
    print(f"  BOJ series length: {len(boj_series_raw)}")

    # ── Inflation Expectations ─────────────────────────────────────────────────
    print("Fetching T5YIE (5Y breakeven, daily)...")
    t5yie_series = get_series("T5YIE", api_key,
                               observation_start=(now_utc - timedelta(days=5*365)).strftime("%Y-%m-%d"),
                               limit=2000, sort_order="asc")
    t5yie_latest = t5yie_series[-1] if t5yie_series else None
    print(f"  T5YIE: {t5yie_latest}, {len(t5yie_series)} obs")

    print("Fetching T10YIE (10Y breakeven)...")
    t10yie = latest("T10YIE", api_key)
    print(f"  T10YIE: {t10yie}")

    print("Fetching Cleveland Fed 1Y Inflation Expectation...")
    exp1y = latest("EXPINF1YR", api_key)
    print(f"  EXPINF1YR: {exp1y}")

    print("Fetching Cleveland Fed 2Y Inflation Expectation...")
    exp2y = latest("EXPINF2YR", api_key)
    print(f"  EXPINF2YR: {exp2y}")

    print("\n=== Phase 2: Fetching Yahoo Finance ===")

    print("Fetching SPX...")
    spx = get_quote("^GSPC")
    print(f"  SPX: {spx}")

    print("Fetching VIX...")
    vix = get_quote("^VIX")
    print(f"  VIX: {vix}")

    print("Fetching DXY...")
    dxy = get_quote("DX-Y.NYB")
    print(f"  DXY: {dxy}")

    print("Fetching WTI...")
    wti = get_quote("CL=F")
    print(f"  WTI: {wti}")

    print("Fetching EURUSD (for ECB conversion)...")
    eurusd = get_futures_price("EURUSD=X")
    print(f"  EURUSD: {eurusd}")

    print("Fetching USDJPY (for BOJ conversion)...")
    usdjpy = get_futures_price("JPY=X")
    print(f"  USDJPY: {usdjpy}")

    print("\n=== Phase 3: Computing liquidity scores ===")

    # Align WALCL, TGA, RRP to weekly (use WALCL dates as anchor, downsample RRP to weekly)
    # For simplicity: use the min length of walcl/tga (both weekly), and downsample RRP
    n_weeks = min(len(walcl_series), len(tga_series))

    # RRP is daily — sample every 5th point to approximate weekly
    rrp_weekly = rrp_daily[::5] if len(rrp_daily) >= 5 else rrp_daily
    n = min(n_weeks, len(rrp_weekly))

    walcl_aligned = walcl_series[-n:]
    tga_aligned = tga_series[-n:]
    rrp_aligned = rrp_weekly[-n:]

    us_liq = compute_us_liquidity_score(walcl_aligned, tga_aligned, rrp_aligned)
    print(f"  US liquidity score: {us_liq['score']}, net_liq: {us_liq['net_liq']} B")

    # ECB: millions EUR → USD billions
    if ecb_series_raw and eurusd:
        ecb_usd_series = [x["value"] / 1000 * eurusd for x in ecb_series_raw]
    else:
        ecb_usd_series = []
    ecb_latest_usd = ecb_usd_series[-1] if ecb_usd_series else None
    print(f"  ECB latest (USD B): {round(ecb_latest_usd, 1) if ecb_latest_usd else None}")

    # BOJ: JPNASSETS in "hundred millions JPY" (億円) → USD billions
    # formula: value * 100M yen / usdjpy / 1B = value / (usdjpy * 10)
    if boj_series_raw and usdjpy:
        boj_usd_series = [x["value"] / (usdjpy * 10) for x in boj_series_raw]
    else:
        boj_usd_series = []
    boj_latest_usd = boj_usd_series[-1] if boj_usd_series else None
    print(f"  BOJ latest (USD B): {round(boj_latest_usd, 1) if boj_latest_usd else None}")

    # PBOC static in trillions → convert to billions
    pboc_static_b = static.get("pboc", {}).get("value_t", 7.2) * 1000

    # Align fed/ecb/boj to same length
    ng = min(len(walcl_series), len(ecb_usd_series) if ecb_usd_series else 0,
             len(boj_usd_series) if boj_usd_series else 0)
    if ng > 0:
        global_liq = compute_global_liquidity_score(
            walcl_series[-ng:], ecb_usd_series[-ng:], boj_usd_series[-ng:], pboc_static_b
        )
    else:
        global_liq = {"score": 0.5, "total": 0.0}
    print(f"  Global liquidity score: {global_liq['score']}, total: {global_liq['total']} T")

    comp_score = composite_score(us_liq["score"], global_liq["score"])
    comp_pctile = round(comp_score * 100)
    print(f"  Composite score: {comp_score} ({comp_pctile}th percentile)")

    # Net US liquidity in trillions
    net_us_liq_t = round(us_liq["net_liq"] / 1000, 3)
    walcl_t = round(walcl_latest / 1000, 3) if walcl_latest else None
    tga_b = round(tga_latest, 1) if tga_latest else None
    rrp_b = round(rrp_latest, 1) if rrp_latest else None
    # WRESBAL is in millions USD → convert to billions
    reserves_b = round(reserves["value"] / 1000, 1) if reserves else None
    ecb_t = round(ecb_latest_usd / 1000, 3) if ecb_latest_usd else None
    boj_t = round(boj_latest_usd / 1000, 3) if boj_latest_usd else None
    pboc_t = static.get("pboc", {}).get("value_t", 7.2)
    g4_total_t = round((walcl_latest or 0) / 1000 + (ecb_latest_usd or 0) / 1000 +
                       (boj_latest_usd or 0) / 1000 + pboc_t, 3)

    print("\n=== Phase 4: Building signal distribution ===")

    # Estimates are semi-static — use last known consensus values
    # These will be updated when macro_data.json is wired to UI; here we hardcode
    # recent consensus estimates as fallback (they change monthly).
    signal_inputs = {
        # Hawk rubric
        "ppi_yoy":       ppi["value"] if ppi else None,
        "ppi_est":       2.6,
        "core_pce_yoy":  core_pce["value"] if core_pce else None,
        "core_pce_est":  2.8,
        "fed_action":    "hold",  # TODO: derive from fed_funds delta when FOMC action is known
        # Dove rubric
        "unemp_rate":    unemp["value"] if unemp else None,
        "unemp_est":     4.3,
        "nfp":           nfp["value"] if nfp else None,
        "nfp_est":       100,
        # Tight rubric
        "yield_10y":     y10["value"] if y10 else None,
        "wti":           wti["price"] if wti else None,
        "vix":           vix["price"] if vix else None,
        # Neutral informational (not in hawk/dove/tight rubric)
        "cpi_yoy":       cpi["value"] if cpi else None,
        "cpi_est":       2.4,
        "core_cpi_yoy":  core_cpi["value"] if core_cpi else None,
        "core_cpi_est":  2.5,
        "pce_yoy":       pce["value"] if pce else None,
        "pce_est":       2.8,
        "dxy":           dxy["price"] if dxy else None,
    }
    signals = compute_signals(signal_inputs)
    print(f"  Signals: hawk={signals['hawk']['score']}, dove={signals['dove']['score']}, "
          f"tight={signals['tight']['score']}, neutral={signals['neutral']['score']}")

    print("\n=== Phase 5: FOMC probabilities ===")

    # Determine current Fed Funds target range from FEDFUNDS series
    ff_val = fed_funds["value"] if fed_funds else 3.375
    # Approximate the range bounds (FEDFUNDS reports the effective rate, not the target range)
    # Use 0.25 step convention: lower bound = floor(ff / 0.25) * 0.25
    ff_low = round((ff_val // 0.25) * 0.25, 2)
    ff_high = round(ff_low + 0.25, 2)
    print(f"  Current Fed Funds: {ff_val}% → range {ff_low}–{ff_high}%")

    fomc_meetings = compute_probabilities(ff_low, ff_high)

    fomc_data = {
        "meta": {
            "last_updated": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "current_rate_low": ff_low,
            "current_rate_high": ff_high,
        },
        "meetings": fomc_meetings,
    }
    write_json(DATA / "fomc_probs.json", fomc_data)

    print("\n=== Phase 6: Building macro_data.json ===")

    # Fed Funds display string
    fed_funds_str = f"{ff_low:.2f}–{ff_high:.2f}%"

    macro_data = {
        "meta": {
            "last_updated": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "next_update": (now_utc + timedelta(days=1)).replace(hour=13, minute=0, second=0,
                                                                   microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "inflation_labor": {
            "cpi_yoy": {
                "value": cpi["value"] if cpi else None,
                "estimate": 2.4,
                "tag": tag_inflation(cpi["value"], 2.4) if cpi else "inline",
                "date": fmt_date(cpi["date"]) if cpi else None,
            },
            "core_cpi_yoy": {
                "value": core_cpi["value"] if core_cpi else None,
                "estimate": 2.5,
                "tag": tag_inflation(core_cpi["value"], 2.5) if core_cpi else "inline",
                "date": fmt_date(core_cpi["date"]) if core_cpi else None,
            },
            "ppi_yoy": {
                "value": ppi["value"] if ppi else None,
                "estimate": 2.6,
                "tag": tag_inflation(ppi["value"], 2.6) if ppi else "inline",
                "date": fmt_date(ppi["date"]) if ppi else None,
            },
            "pce_yoy": {
                "value": pce["value"] if pce else None,
                "estimate": 2.8,
                "tag": tag_inflation(pce["value"], 2.8) if pce else "inline",
                "date": fmt_date(pce["date"]) if pce else None,
            },
            "core_pce_yoy": {
                "value": core_pce["value"] if core_pce else None,
                "estimate": 2.8,
                "tag": tag_inflation(core_pce["value"], 2.8) if core_pce else "inline",
                "date": fmt_date(core_pce["date"]) if core_pce else None,
            },
            "unemployment": {
                "value": unemp["value"] if unemp else None,
                "estimate": 4.3,
                "tag": tag_labor(unemp["value"], 4.3) if unemp else "inline",
                "date": fmt_date(unemp["date"]) if unemp else None,
            },
            "initial_claims": {
                "value": claims["value"] if claims else None,
                "estimate": 210,
                "tag": "inline",
                "date": "Wkly",
            },
        },
        "growth_sentiment": {
            "nfp_mom": {
                "value": nfp["value"] if nfp else None,
                "estimate": 100,
                "tag": tag_growth(nfp["value"], 100) if nfp else "inline",
                "date": fmt_date(nfp["date"]) if nfp else None,
            },
            "umich_sent": {
                "value": umich["value"] if umich else None,
                "estimate": 55.5,
                "tag": tag_growth(umich["value"], 55.5) if umich else "inline",
                "date": fmt_date(umich["date"]) if umich else None,
            },
            "ism_mfg": {
                "value": static.get("ism_mfg", {}).get("value"),
                "estimate": static.get("ism_mfg", {}).get("estimate"),
                "tag": static.get("ism_mfg", {}).get("tag", "inline"),
                "date": static.get("ism_mfg", {}).get("date"),
                "static": True,
            },
        },
        "market_anchors": {
            "spx": {
                "value": round(spx["price"]) if spx else None,
                "delta_pct": spx["delta_pct"] if spx else None,
                "delta_dir": spx["delta_dir"] if spx else "up",
            },
            "yield_10y": {
                "value": y10["value"] if y10 else None,
                "delta_bps": None,  # would need intraday delta; omit for now
                "delta_dir": "down",
            },
            "dxy": {
                "value": dxy["price"] if dxy else None,
                "delta_pct": dxy["delta_pct"] if dxy else None,
                "delta_dir": dxy["delta_dir"] if dxy else "down",
            },
            "wti": {
                "value": wti["price"] if wti else None,
                "delta_pct": wti["delta_pct"] if wti else None,
                "delta_dir": wti["delta_dir"] if wti else "down",
            },
            "vix": {
                "value": vix["price"] if vix else None,
                "delta_pct": vix["delta_pct"] if vix else None,
                "delta_dir": vix["delta_dir"] if vix else "down",
            },
            "fed_funds": {
                "value": fed_funds_str,
                "action": "HOLD",
                "meeting_count": 2,
            },
        },
        "signal_distribution": signals,
        "liquidity": {
            "us_score": us_liq["score"],
            "global_score": global_liq["score"],
            "composite": comp_score,
            "composite_pctile": comp_pctile,
            "net_us_liq_t": net_us_liq_t,
            "walcl_t": walcl_t,
            "tga_b": tga_b,
            "rrp_b": rrp_b,
            "reserves_b": reserves_b,
            "nfci": nfci["value"] if nfci else None,
            "anfci": anfci["value"] if anfci else None,
            "hy_oas": hy_oas["value"] if hy_oas else None,
            "ig_oas": ig_oas["value"] if ig_oas else None,
            "ecb_t": ecb_t,
            "boj_t": boj_t,
            "pboc_t": pboc_t,
            "g4_total_t": g4_total_t,
        },
        "inflation_expectations": {
            "exp_1y": {
                "value": exp1y["value"] if exp1y else None,
                "source": "Cleveland Fed",
                "date": fmt_date(exp1y["date"]) if exp1y else None,
            },
            "exp_2y": {
                "value": exp2y["value"] if exp2y else None,
                "source": "Cleveland Fed",
                "date": fmt_date(exp2y["date"]) if exp2y else None,
            },
            "t5yie": {
                "value": t5yie_latest["value"] if t5yie_latest else None,
                "source": "FRED T5YIE",
                "date": t5yie_latest["date"] if t5yie_latest else None,
            },
            "t10yie": {
                "value": t10yie["value"] if t10yie else None,
                "source": "FRED T10YIE",
                "date": fmt_date(t10yie["date"]) if t10yie else None,
            },
            "core_pce_yoy": {
                "value": core_pce["value"] if core_pce else None,
                "source": "BEA",
                "date": fmt_date(core_pce["date"]) if core_pce else None,
            },
            "sticky_cpi": {
                "value": static.get("sticky_cpi", {}).get("value"),
                "source": "Atlanta Fed",
                "date": static.get("sticky_cpi", {}).get("last_updated"),
                "static": True,
            },
        },
    }
    write_json(DATA / "macro_data.json", macro_data)

    print("\n=== Phase 7: Writing t5yie_data.json ===")
    t5yie_output = [{"date": obs["date"], "value": obs["value"]} for obs in t5yie_series]
    write_json(DATA / "t5yie_data.json", t5yie_output)
    print(f"  T5YIE: {len(t5yie_output)} observations")

    print("\n=== Phase 8: Extending chart_data.json ===")
    update_chart_data(api_key, walcl_series_raw, tga_series_raw, rrp_daily_raw,
                      ecb_series_raw, boj_series_raw, eurusd, usdjpy, pboc_static_b)

    print("\n=== Phase 9: Generating AI narrative ===")
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).parent))
        from generate_narrative import generate_narrative
        generate_narrative()
    except Exception as e:
        print(f"  WARNING: narrative generation failed — {e}")
        print("  Continuing without synthesis.json update.")

    print("\n=== Phase 10: Market Trend Gauge ===")
    try:
        from market_gauge import run_market_gauge
        run_market_gauge()
    except Exception as e:
        print(f"  WARNING: market gauge failed — {e}")

    print("\n=== ALL DONE ===")
    print(f"  last_updated: {now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}")


# ── chart_data.json updater ────────────────────────────────────────────────────
def update_chart_data(api_key, walcl_raw, tga_raw, rrp_raw,
                      ecb_raw, boj_raw, eurusd, usdjpy, pboc_static_b):
    """
    Load existing chart_data.json, determine last date, fetch new FRED data,
    compute scores for new weeks, and append.
    """
    # Load existing
    existing = []
    if CHART_DATA.exists():
        with open(CHART_DATA) as f:
            existing = json.load(f)
    last_date = existing[-1]["date"] if existing else "2016-01-01"
    print(f"  Existing chart data last date: {last_date}")

    # Build full weekly series from FRED data we already fetched
    # Align WALCL (weekly) and TGA (weekly) by date
    walcl_dict = {obs["date"]: obs["value"] / 1000 for obs in walcl_raw}   # billions
    tga_dict   = {obs["date"]: obs["value"] / 1000 for obs in tga_raw}     # billions

    # RRP is daily — build a dict keyed by date
    rrp_dict = {obs["date"]: obs["value"] for obs in rrp_raw}

    # ECB / BOJ conversion
    ecb_dict = {}
    if ecb_raw and eurusd:
        ecb_dict = {obs["date"]: obs["value"] / 1000 * eurusd for obs in ecb_raw}
    boj_dict = {}
    if boj_raw and usdjpy:
        # JPNASSETS in hundred-millions JPY → USD billions
        boj_dict = {obs["date"]: obs["value"] / (usdjpy * 10) for obs in boj_raw}

    # Fetch SPX historical from FRED (weekly close)
    print("  Fetching SP500 from FRED for chart extension...")
    spx_raw = get_series("SP500", api_key,
                          observation_start=(datetime.strptime(last_date, "%Y-%m-%d")
                                             + timedelta(days=1)).strftime("%Y-%m-%d"),
                          limit=200, sort_order="asc")
    spx_dict = {obs["date"]: obs["value"] for obs in spx_raw}
    print(f"  SP500 new observations: {len(spx_raw)}")

    # Compute full history for normalization
    all_walcl  = [v for _, v in sorted(walcl_dict.items())]
    all_tga    = [v for _, v in sorted(tga_dict.items())]
    all_rrp    = []
    all_ecb    = [v for _, v in sorted(ecb_dict.items())]
    all_boj    = [v for _, v in sorted(boj_dict.items())]

    # For each WALCL date, pick nearest RRP value
    walcl_dates = sorted(walcl_dict.keys())
    rrp_dates   = sorted(rrp_dict.keys())

    def nearest_rrp(date_str):
        if not rrp_dict:
            return 0.0
        # find closest date
        idx = 0
        for i, d in enumerate(rrp_dates):
            if d <= date_str:
                idx = i
        return rrp_dict[rrp_dates[idx]]

    # Build full US net liquidity series for percentile normalization
    full_net_us = [walcl_dict[d] - tga_dict.get(d, 0) - nearest_rrp(d) for d in walcl_dates
                   if d in tga_dict]

    # Build full global series
    ng_dates = sorted(set(ecb_dict.keys()) & set(boj_dict.keys()) & set(walcl_dict.keys()))
    full_global = [walcl_dict[d] + ecb_dict[d] + boj_dict[d] + pboc_static_b
                   for d in ng_dates]

    # Find new weeks to add
    existing_dates = {row["date"] for row in existing}
    new_rows = []

    for date_str in walcl_dates:
        if date_str <= last_date:
            continue
        if date_str in existing_dates:
            continue
        if date_str not in tga_dict:
            continue

        net_us = walcl_dict[date_str] - tga_dict[date_str] - nearest_rrp(date_str)
        us_score = percentile_rank(net_us, full_net_us) - 0.5

        global_score = 0.0
        if date_str in ecb_dict and date_str in boj_dict:
            g_val = walcl_dict[date_str] + ecb_dict[date_str] + boj_dict[date_str] + pboc_static_b
            global_score = percentile_rank(g_val, full_global) - 0.5

        comp = round((us_score + global_score) / 2, 3)
        us_score = round(us_score, 3)
        global_score = round(global_score, 3)

        spx_val = spx_dict.get(date_str)
        # Try to find closest SPX date within ±3 days
        if spx_val is None:
            for delta in range(1, 4):
                for sign in (-1, 1):
                    alt = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=delta * sign)
                           ).strftime("%Y-%m-%d")
                    if alt in spx_dict:
                        spx_val = spx_dict[alt]
                        break
                if spx_val is not None:
                    break

        if spx_val is None:
            continue  # skip weeks with no SPX data

        new_rows.append({
            "date": date_str,
            "us_score": us_score,
            "global_score": global_score,
            "spx": round(spx_val, 2),
            "composite": comp,
        })

    if new_rows:
        print(f"  Appending {len(new_rows)} new weekly rows to chart_data.json")
        existing.extend(new_rows)
    else:
        print("  chart_data.json is already up to date")

    write_json(CHART_DATA, existing)


if __name__ == "__main__":
    main()
