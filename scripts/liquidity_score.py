"""
Liquidity score calculation.

US Score  = percentile_rank(WALCL - WTREGEN - RRPONTSYD) over a rolling 10Y window
Global    = weighted average of Fed + ECB + BOJ + PBOC balance sheets (USD), normalized similarly
Composite = average(US, Global)
"""

import statistics


def percentile_rank(value: float, history: list[float]) -> float:
    """Return the fraction of historical values <= current value (0.0–1.0)."""
    if not history:
        return 0.5
    count_below = sum(1 for v in history if v <= value)
    return round(count_below / len(history), 4)


def compute_us_liquidity_score(walcl_series: list[float], tga_series: list[float],
                                rrp_series: list[float]) -> dict:
    """
    walcl_series, tga_series, rrp_series — parallel lists of weekly observations,
    all in the same unit (billions USD), newest last.

    Returns {"score": float (0-1), "net_liq": float (latest value)}.
    """
    n = min(len(walcl_series), len(tga_series), len(rrp_series))
    if n == 0:
        return {"score": 0.5, "net_liq": 0.0}

    net_series = [walcl_series[i] - tga_series[i] - rrp_series[i] for i in range(n)]
    current = net_series[-1]
    score = percentile_rank(current, net_series)
    return {"score": score, "net_liq": round(current, 3)}


def compute_global_liquidity_score(fed_series: list[float], ecb_series: list[float],
                                    boj_series: list[float], pboc_static: float) -> dict:
    """
    All series in USD billions, newest last.
    PBOC is static (no live API) — passed as scalar.

    Returns {"score": float (0-1), "total": float}.
    """
    n = min(len(fed_series), len(ecb_series), len(boj_series))
    if n == 0:
        return {"score": 0.5, "total": 0.0}

    # Combine with equal weighting (PBOC static, added as constant shift)
    combined = [fed_series[i] + ecb_series[i] + boj_series[i] + pboc_static for i in range(n)]
    current = combined[-1]
    score = percentile_rank(current, combined)
    return {"score": score, "total": round(current / 1000, 3)}  # return in trillions


def composite_score(us_score: float, global_score: float) -> float:
    return round((us_score + global_score) / 2, 4)
