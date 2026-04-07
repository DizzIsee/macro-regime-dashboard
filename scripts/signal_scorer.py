"""
Signal distribution scorer — hawk / dove / tight / neutral.
All thresholds and rules match the CLAUDE.md spec.
"""


def compute_signals(data: dict) -> dict:
    """
    data keys expected (all floats unless noted):
      ppi_yoy, ppi_est
      core_pce_yoy, core_pce_est
      fed_action (str: "hold" | "cut" | "hike")
      unemp_rate, unemp_est
      nfp, nfp_est
      yield_10y
      wti
      vix
    """
    hawk_items = []
    dove_items = []
    tight_items = []

    # HAWKISH
    if data.get("ppi_yoy") is not None and data.get("ppi_est") is not None:
        if data["ppi_yoy"] > data["ppi_est"]:
            hawk_items.append(f"PPI {data['ppi_yoy']:.1f}% > Est {data['ppi_est']:.1f}%")

    if data.get("core_pce_yoy") is not None and data.get("core_pce_est") is not None:
        if data["core_pce_yoy"] > data["core_pce_est"]:
            hawk_items.append(f"Core PCE {data['core_pce_yoy']:.1f}% > Est {data['core_pce_est']:.1f}%")

    if data.get("fed_action") == "hold":
        hawk_items.append("Fed on hold")

    # DOVISH
    if data.get("unemp_rate") is not None and data.get("unemp_est") is not None:
        if data["unemp_rate"] > data["unemp_est"]:
            dove_items.append(f"Unemployment {data['unemp_rate']:.1f}% > Est {data['unemp_est']:.1f}%")

    if data.get("nfp") is not None:
        nfp_est = data.get("nfp_est", 0)
        if data["nfp"] < 0 or data["nfp"] < nfp_est:
            dove_items.append(f"NFP {int(data['nfp']):+d}K < Est {int(nfp_est):+d}K")

    # TIGHTENING
    if data.get("yield_10y") is not None and data["yield_10y"] > 4.0:
        tight_items.append(f"10Y {data['yield_10y']:.2f}% > 4.0%")

    if data.get("wti") is not None and data["wti"] > 80:
        tight_items.append(f"WTI ${data['wti']:.2f} > $80")

    if data.get("vix") is not None and data["vix"] > 20:
        tight_items.append(f"VIX {data['vix']:.2f} > 20")

    hawk = len(hawk_items)
    dove = len(dove_items)
    tight = len(tight_items)
    neutral = 15 - hawk - dove - tight

    return {
        "hawk":    {"score": hawk,    "items": hawk_items},
        "dove":    {"score": dove,    "items": dove_items},
        "tight":   {"score": tight,   "items": tight_items},
        "neutral": {"score": neutral, "items": []},
    }
