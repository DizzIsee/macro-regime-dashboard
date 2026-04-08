"""
Signal distribution scorer — hawk / dove / tight / neutral.
All thresholds and rules match the CLAUDE.md spec.

Neutral items are generated as the inverse of the hawk/dove/tight conditions:
metrics that were evaluated but did not trigger any directional signal.
This ensures the breakdown card always has data-driven content for all 4 columns.
"""


def compute_signals(data):
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
      -- optional, used for neutral items only --
      cpi_yoy, cpi_est
      core_cpi_yoy, core_cpi_est
      pce_yoy, pce_est
      dxy
    """
    hawk_items    = []
    dove_items    = []
    tight_items   = []
    neutral_items = []

    # ── HAWKISH ───────────────────────────────────────────────────────────────
    if data.get("ppi_yoy") is not None and data.get("ppi_est") is not None:
        if data["ppi_yoy"] > data["ppi_est"]:
            hawk_items.append(
                "PPI {:.1f}% > Est {:.1f}%".format(data["ppi_yoy"], data["ppi_est"])
            )
        else:
            neutral_items.append(
                "PPI {:.1f}% \u2264 Est {:.1f}% (in-line)".format(
                    data["ppi_yoy"], data["ppi_est"]
                )
            )

    if data.get("core_pce_yoy") is not None and data.get("core_pce_est") is not None:
        if data["core_pce_yoy"] > data["core_pce_est"]:
            hawk_items.append(
                "Core PCE {:.1f}% > Est {:.1f}%".format(
                    data["core_pce_yoy"], data["core_pce_est"]
                )
            )
        else:
            neutral_items.append(
                "Core PCE {:.1f}% \u2264 Est {:.1f}% (in-line)".format(
                    data["core_pce_yoy"], data["core_pce_est"]
                )
            )

    if data.get("fed_action") == "hold":
        hawk_items.append("Fed on hold")
    elif data.get("fed_action") == "cut":
        dove_items.append("Fed cut rates")
    elif data.get("fed_action") == "hike":
        hawk_items.append("Fed hiked rates")

    # ── DOVISH ────────────────────────────────────────────────────────────────
    if data.get("unemp_rate") is not None and data.get("unemp_est") is not None:
        if data["unemp_rate"] > data["unemp_est"]:
            dove_items.append(
                "Unemployment {:.1f}% > Est {:.1f}%".format(
                    data["unemp_rate"], data["unemp_est"]
                )
            )
        else:
            neutral_items.append(
                "Unemployment {:.1f}% \u2264 Est {:.1f}% (in-line)".format(
                    data["unemp_rate"], data["unemp_est"]
                )
            )

    if data.get("nfp") is not None:
        nfp_est = data.get("nfp_est", 0)
        if data["nfp"] < 0 or data["nfp"] < nfp_est:
            dove_items.append(
                "NFP {:+d}K < Est {:+d}K".format(int(data["nfp"]), int(nfp_est))
            )
        else:
            neutral_items.append(
                "NFP {:+d}K beat Est {:+d}K".format(int(data["nfp"]), int(nfp_est))
            )

    # ── TIGHTENING ────────────────────────────────────────────────────────────
    if data.get("yield_10y") is not None:
        if data["yield_10y"] > 4.0:
            tight_items.append("10Y {:.2f}% > 4.0%".format(data["yield_10y"]))
        else:
            neutral_items.append("10Y {:.2f}% \u2264 4.0%".format(data["yield_10y"]))

    if data.get("wti") is not None:
        if data["wti"] > 80:
            tight_items.append("WTI ${:.2f} > $80".format(data["wti"]))
        else:
            neutral_items.append("WTI ${:.2f} \u2264 $80".format(data["wti"]))

    if data.get("vix") is not None:
        if data["vix"] > 20:
            tight_items.append("VIX {:.2f} > 20".format(data["vix"]))
        else:
            neutral_items.append("VIX {:.2f} \u2264 20".format(data["vix"]))

    # ── NEUTRAL (informational — metrics not in hawk/dove/tight rubric) ───────
    if data.get("cpi_yoy") is not None and data.get("cpi_est") is not None:
        diff = abs(data["cpi_yoy"] - data["cpi_est"])
        label = "in-line" if diff <= 0.1 else ("hot" if data["cpi_yoy"] > data["cpi_est"] else "cool")
        neutral_items.append("CPI {:.1f}% ({})".format(data["cpi_yoy"], label))

    if data.get("core_cpi_yoy") is not None and data.get("core_cpi_est") is not None:
        diff = abs(data["core_cpi_yoy"] - data["core_cpi_est"])
        label = "in-line" if diff <= 0.1 else ("hot" if data["core_cpi_yoy"] > data["core_cpi_est"] else "cool")
        neutral_items.append("Core CPI {:.1f}% ({})".format(data["core_cpi_yoy"], label))

    if data.get("pce_yoy") is not None and data.get("pce_est") is not None:
        diff = abs(data["pce_yoy"] - data["pce_est"])
        label = "in-line" if diff <= 0.1 else ("hot" if data["pce_yoy"] > data["pce_est"] else "cool")
        neutral_items.append("PCE {:.1f}% ({})".format(data["pce_yoy"], label))

    if data.get("dxy") is not None:
        if data["dxy"] < 103:
            neutral_items.append("DXY {:.1f} < 103 (not elevated)".format(data["dxy"]))
        else:
            neutral_items.append("DXY {:.1f} \u2265 103 (elevated)".format(data["dxy"]))

    # ── Totals ────────────────────────────────────────────────────────────────
    hawk    = len(hawk_items)
    dove    = len(dove_items)
    tight   = len(tight_items)
    neutral = 15 - hawk - dove - tight

    return {
        "hawk":    {"score": hawk,    "items": hawk_items},
        "dove":    {"score": dove,    "items": dove_items},
        "tight":   {"score": tight,   "items": tight_items},
        "neutral": {"score": neutral, "items": neutral_items},
    }
