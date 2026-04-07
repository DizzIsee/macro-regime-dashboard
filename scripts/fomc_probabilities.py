"""
Fed Funds Futures → FOMC cut/hold probability calculator.
Methodology mirrors CME FedWatch.

Implied Rate = 100 - Futures Price
Probability of 25bp cut = (Current Rate - Implied Rate) / 0.25
Clamped to [0, 1].
"""

from scripts.yahoo_client import get_futures_price

# ZQ contract tickers and the FOMC meeting they cover
CONTRACTS = [
    {"ticker": "ZQK26.CBT", "label": "APR 29", "date": "Apr 29"},
    {"ticker": "ZQM26.CBT", "label": "JUN 17", "date": "Jun 17"},
    {"ticker": "ZQN26.CBT", "label": "JUL 29", "date": "Jul 29"},
    {"ticker": "ZQU26.CBT", "label": "SEP 16", "date": "Sep 16"},
    {"ticker": "ZQV26.CBT", "label": "OCT 28", "date": "Oct 28"},
    {"ticker": "ZQZ26.CBT", "label": "DEC 9",  "date": "Dec 9"},
]


def compute_probabilities(current_rate_low: float, current_rate_high: float) -> list[dict]:
    """
    current_rate_low / high: the current Fed Funds target range bounds (e.g. 3.50, 3.75).
    Returns list of meeting dicts matching fomc_probs.json schema.
    """
    current_mid = (current_rate_low + current_rate_high) / 2
    results = []

    for contract in CONTRACTS:
        price = get_futures_price(contract["ticker"])

        if price is None:
            print(f"  [fomc] WARNING: {contract['ticker']} returned no data — skipping")
            results.append({
                "date": contract["date"],
                "label": contract["label"],
                "hold_prob": None,
                "cut25_prob": None,
                "cut50_prob": None,
                "error": "data unavailable",
            })
            continue

        implied_rate = 100 - price

        # Probability of at least one 25bp cut
        cut25_raw = (current_mid - implied_rate) / 0.25
        cut25_prob = max(0.0, min(1.0, cut25_raw))

        # Probability of at least one 50bp cut
        cut50_raw = (current_mid - implied_rate - 0.25) / 0.25
        cut50_prob = max(0.0, min(1.0, cut50_raw))

        hold_prob = 1.0 - cut25_prob

        print(f"  [fomc] {contract['ticker']}: price={price:.4f}, "
              f"implied={implied_rate:.4f}%, "
              f"hold={hold_prob*100:.1f}%, cut25={cut25_prob*100:.1f}%, cut50={cut50_prob*100:.1f}%")

        results.append({
            "date": contract["date"],
            "label": contract["label"],
            "hold_prob": round(hold_prob * 100, 1),
            "cut25_prob": round(cut25_prob * 100, 1),
            "cut50_prob": round(cut50_prob * 100, 1),
        })

    return results
