"""Yahoo Finance wrapper — fetches live quotes and futures prices via yfinance."""

import yfinance as yf


def get_quote(ticker):
    """
    Fetch latest price and 1-day % change for a ticker.
    Returns {"price": float, "delta_pct": float, "delta_dir": "up"|"down"} or None.
    """
    try:
        t = yf.Ticker(ticker)
        info = t.fast_info
        price = getattr(info, "last_price", None)
        prev_close = getattr(info, "previous_close", None)

        if price is None or prev_close is None or prev_close == 0:
            return None

        delta_pct = round(((price - prev_close) / prev_close) * 100, 2)
        return {
            "price": round(float(price), 4),
            "delta_pct": abs(delta_pct),
            "delta_dir": "up" if delta_pct >= 0 else "down",
        }
    except Exception as e:
        print("  [yahoo] WARNING: {} failed — {}".format(ticker, e))
        return None


def get_futures_price(ticker):
    """
    Return the last closing price for a futures contract (e.g. ZQK26.CBT).
    Returns None if the ticker is stale, delisted, or unavailable.
    """
    try:
        t = yf.Ticker(ticker)
        info = t.fast_info
        price = getattr(info, "last_price", None)
        if price is None:
            return None
        return round(float(price), 4)
    except Exception as e:
        print("  [yahoo] WARNING: {} failed — {}".format(ticker, e))
        return None
