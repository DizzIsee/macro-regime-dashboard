"""
Fed Funds Futures → FOMC cut/hold probability calculator.
Methodology mirrors CME FedWatch exactly.

Two-part approach:
  1. Meeting-date weighting: each monthly futures contract covers the full month,
     but the FOMC meeting lands partway through. The price reflects a day-weighted
     blend of the pre-meeting rate and the post-meeting rate.

     rate_after = (implied_avg - (days_before/N) * rate_before) * N / days_after

     Where days_before = meeting_day - 1  (days at old rate)
           days_after  = N - (meeting_day - 1)  (days at new rate, inclusive of meeting day)
           N           = total days in contract month

  2. Chaining: rate_before[n] = E[rate after meeting n-1], not current_mid.
     Without this, back-month contracts (Jul, Sep, Oct, Dec) are mis-read because
     the market prices them against the expected post-prior-meeting rate, not today's rate.

     E[rate after meeting n] = P(0 cuts total) * current_rate
                              + P(1 cut total) * (current_rate - 0.25)
                              + P(2 cuts total) * (current_rate - 0.50) + ...

  3. Probability tree: builds a distribution of cumulative cuts after each meeting.
     Displayed values are cumulative probabilities (probability of N total cuts by
     this meeting date), matching what CME FedWatch shows.

Contract → meeting mapping:
  ZQK26 (May 2026)  → Apr 29 FOMC  [Apr 29 is BEFORE May 1, so all 31 May days post-meeting]
  ZQM26 (Jun 2026)  → Jun 17 FOMC  [16 days before / 14 days after in June]
  ZQN26 (Jul 2026)  → Jul 29 FOMC  [28 days before / 3 days after in July ← biggest error source]
  ZQU26 (Sep 2026)  → Sep 16 FOMC  [15 days before / 15 days after in September]
  ZQV26 (Oct 2026)  → Oct 28 FOMC  [27 days before / 4 days after in October]
  ZQZ26 (Dec 2026)  → Dec 9 FOMC   [8 days before / 23 days after in December]
"""

import calendar

from scripts.yahoo_client import get_futures_price

# meeting_day: the FOMC announcement day within the contract month.
# None means the meeting falls BEFORE the contract month starts (Apr 29 → May contract):
# in that case all days in the month reflect the post-meeting rate.
CONTRACTS = [
    {
        "ticker": "ZQK26.CBT",
        "label": "APR 29", "date": "Apr 29",
        "contract_year": 2026, "contract_month": 5,
        "meeting_day": None,   # Apr 29 meeting → entire May is post-meeting
    },
    {
        "ticker": "ZQM26.CBT",
        "label": "JUN 17", "date": "Jun 17",
        "contract_year": 2026, "contract_month": 6,
        "meeting_day": 17,     # Jun 1-16 old rate, Jun 17-30 new rate
    },
    {
        "ticker": "ZQN26.CBT",
        "label": "JUL 29", "date": "Jul 29",
        "contract_year": 2026, "contract_month": 7,
        "meeting_day": 29,     # Jul 1-28 old rate, Jul 29-31 new rate (3 days only!)
    },
    {
        "ticker": "ZQU26.CBT",
        "label": "SEP 16", "date": "Sep 16",
        "contract_year": 2026, "contract_month": 9,
        "meeting_day": 16,     # Sep 1-15 old rate, Sep 16-30 new rate
    },
    {
        "ticker": "ZQV26.CBT",
        "label": "OCT 28", "date": "Oct 28",
        "contract_year": 2026, "contract_month": 10,
        "meeting_day": 28,     # Oct 1-27 old rate, Oct 28-31 new rate (4 days only!)
    },
    {
        "ticker": "ZQZ26.CBT",
        "label": "DEC 9", "date": "Dec 9",
        "contract_year": 2026, "contract_month": 12,
        "meeting_day": 9,      # Dec 1-8 old rate, Dec 9-31 new rate
    },
]


def compute_probabilities(current_rate_low, current_rate_high):
    """
    current_rate_low / high: current Fed Funds target range (e.g. 3.50, 3.75).
    Returns list of meeting dicts matching fomc_probs.json schema.

    Displayed probabilities are CUMULATIVE at each meeting date:
      hold_prob   = P(rate still at current level — 0 cuts so far)
      cut25_prob  = P(exactly 1 total 25bp cut so far)
      cut50_prob  = P(2 or more total 25bp cuts so far)
    """
    current_mid = (current_rate_low + current_rate_high) / 2

    # Fetch all futures prices upfront
    prices = {}
    for c in CONTRACTS:
        prices[c["ticker"]] = get_futures_price(c["ticker"])

    results = []

    # Probability distribution: {n_cuts: probability}
    # Starts at 0 cuts with certainty.
    dist = {0: 1.0}

    # E[rate] going into the next meeting — initialized to current rate.
    expected_rate_before = current_mid

    for contract in CONTRACTS:
        price = prices[contract["ticker"]]

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

        implied_avg = 100.0 - price
        total_days = calendar.monthrange(
            contract["contract_year"], contract["contract_month"]
        )[1]
        meeting_day = contract.get("meeting_day")

        # ── Step 1: meeting-date-weighted rate extraction ──────────────────
        if meeting_day is None:
            # Meeting is in the prior month; entire contract month is post-meeting.
            days_before = 0
            days_after = total_days
            rate_after = implied_avg
        else:
            days_before = meeting_day - 1          # days at old rate
            days_after = total_days - days_before  # days at new rate (meeting day onwards)

            if days_after <= 0:
                # Degenerate — shouldn't happen for real FOMC calendars
                rate_after = expected_rate_before
            else:
                # implied_avg = (db/N)*rate_before + (da/N)*rate_after
                rate_after = (
                    implied_avg - (days_before / total_days) * expected_rate_before
                ) * total_days / days_after

        # Clamp: rate_after can't be negative or absurdly high (data noise guard)
        rate_after = max(0.0, min(current_mid + 0.25, rate_after))

        # ── Step 2: conditional cut probability at THIS meeting ────────────
        # E[rate_after] = E[rate_before] - p_cut * 0.25
        p_cut = (expected_rate_before - rate_after) / 0.25
        p_cut = max(0.0, min(1.0, p_cut))
        p_hold = 1.0 - p_cut

        print(
            f"  [fomc] {contract['ticker']}: price={price:.4f}, "
            f"days={days_before}/{days_after}/{total_days}, "
            f"impl_avg={implied_avg:.4f}%, "
            f"E[before]={expected_rate_before:.4f}%, "
            f"E[after]={rate_after:.4f}%, "
            f"p_cut_this_meeting={p_cut*100:.1f}%"
        )

        # ── Step 3: update cumulative probability distribution ─────────────
        new_dist = {}
        for n_cuts, prob in dist.items():
            new_dist[n_cuts] = new_dist.get(n_cuts, 0.0) + prob * p_hold
            new_dist[n_cuts + 1] = new_dist.get(n_cuts + 1, 0.0) + prob * p_cut
        dist = new_dist

        # E[rate] going into the NEXT meeting
        expected_rate_before = sum(
            prob * (current_mid - n * 0.25)
            for n, prob in dist.items()
        )

        # ── Cumulative display probabilities ───────────────────────────────
        hold_prob   = dist.get(0, 0.0)
        cut25_prob  = dist.get(1, 0.0)
        cut50_prob  = sum(v for k, v in dist.items() if k >= 2)

        print(
            f"          cumulative → hold={hold_prob*100:.1f}%, "
            f"cut25={cut25_prob*100:.1f}%, cut50+={cut50_prob*100:.1f}%  "
            f"(E[rate next]={expected_rate_before:.4f}%)"
        )

        results.append({
            "date": contract["date"],
            "label": contract["label"],
            "hold_prob":  round(hold_prob  * 100, 1),
            "cut25_prob": round(cut25_prob * 100, 1),
            "cut50_prob": round(cut50_prob * 100, 1),
        })

    return results
