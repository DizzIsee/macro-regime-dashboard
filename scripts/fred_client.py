"""FRED API wrapper — fetches series data from the St. Louis Fed."""

import requests
from datetime import datetime

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"


def get_series(series_id, api_key, observation_start=None,
               observation_end=None, limit=100, sort_order="desc"):
    """
    Fetch observations for a FRED series. Returns list of {"date": ..., "value": ...} dicts,
    newest first by default. Values of "." (missing) are filtered out.
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": sort_order,
        "limit": limit,
    }
    if observation_start:
        params["observation_start"] = observation_start
    if observation_end:
        params["observation_end"] = observation_end

    resp = requests.get(FRED_BASE, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    observations = [
        {"date": obs["date"], "value": float(obs["value"])}
        for obs in data.get("observations", [])
        if obs["value"] != "."
    ]
    return observations


def latest(series_id, api_key):
    """Return the most recent non-null observation for a series."""
    obs = get_series(series_id, api_key, limit=10, sort_order="desc")
    return obs[0] if obs else None


def yoy_pct_change(series_id, api_key):
    """
    Compute YoY % change for the most recent available month.
    Returns {"value": float, "date": str} or None.
    """
    obs = get_series(series_id, api_key, limit=14, sort_order="desc")
    if len(obs) < 13:
        return None

    current = obs[0]
    year_ago = obs[12]

    if year_ago["value"] == 0:
        return None

    change = ((current["value"] - year_ago["value"]) / abs(year_ago["value"])) * 100
    return {"value": round(change, 2), "date": current["date"]}


def mom_change(series_id, api_key):
    """
    Compute MoM absolute change for the most recent available month.
    Returns {"value": float, "date": str} or None.
    """
    obs = get_series(series_id, api_key, limit=3, sort_order="desc")
    if len(obs) < 2:
        return None
    change = obs[0]["value"] - obs[1]["value"]
    return {"value": round(change, 1), "date": obs[0]["date"]}
