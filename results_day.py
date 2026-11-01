"""
results_day.py — turn one results-day screen into stored figures and events.

On the day a company reports, four things change at once: the quarter's
reported figures, (often) the full-year guidance, (sometimes) the dividend,
and (sometimes) a buyback. The Research tab's results-day screen takes all of
them in one place; this plans what that screen saves, without Streamlit, so
the unit handling and the "did anything actually change" rules are testable.

Units on the way in are the panel's: ¥bn for money, ¥ per share for EPS / DPS,
millions for share counts. Everything stored is in yen / shares.
"""

import capital_events as CE
import fundamentals as F

PERIODS = ("1Q", "2Q", "3Q", "FY")
LINES = ("net_sales", "operating_profit", "ordinary_profit", "net_profit", "eps")
GUIDE_LINES = ("net_sales", "operating_profit", "ordinary_profit", "net_profit", "eps", "dps")


def next_fy(fy: str) -> str:
    try:
        return f"FY{int(fy[2:]) + 1}"
    except (TypeError, ValueError):
        return ""


def reported_basis(period: str, single_quarter: bool) -> str:
    """Where a reported figure is stored. Year to date goes under the
    collector's own bases (so a typed one and a collected one are the same
    thing); a single quarter under actual_qN."""
    n = PERIODS.index(period) + 1
    if single_quarter:
        return f"actual_q{n}"
    return "actual" if n == 4 else f"ytd_{n}q"


def guidance_fy(fy: str, period: str) -> str:
    """The year the guidance given on results day is for: the same year after
    a quarter, the next one after a full-year result."""
    return next_fy(fy) if period == "FY" else fy


def _yen(metric, v):
    return float(v) if metric in ("eps", "dps") else float(v) * 1e9


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _same(a, b):
    return a is not None and b is not None and abs(a - b) <= max(abs(a), abs(b), 1.0) * 1e-9


def plan(code: str, fy: str, period: str, single_quarter: bool, reported: dict,
         guidance: dict, current: dict, date: str, source: str = "",
         buyback: dict = None, existing_events: list = (), shares_out=None,
         treasury=None) -> tuple:
    """(entries, events, summary) for one results-day save.

    reported / guidance: {metric: value in panel units}, blanks omitted.
    current: the panel's merged map {(metric, fy, basis): {"value": ...}} —
    used to skip figures that already hold the typed value, and to read the
    dividend going in.
    buyback: {"amount_bn", "shares_m", "until", "note"} or None.
    """
    src = F.typed_source(source)
    entries, events, summary = {}, [], []

    def put(metric, fy_, basis, value):
        cur = _num((current.get((metric, fy_, basis)) or {}).get("value"))
        if _same(cur, value):
            return False
        entries[F.manual_key(metric, fy_, basis)] = {
            "value": value, "unit": "jpy" if metric in ("eps", "dps") else "jpy_abs",
            "source": src, "as_of": date}
        return True

    basis = reported_basis(period, single_quarter)
    for m in LINES:
        v = _num(reported.get(m))
        if v is not None and put(m, fy, basis, _yen(m, v)):
            summary.append(f"{m} {period} reported")

    gfy = guidance_fy(fy, period)
    dps_before = _num((current.get(("dps", gfy, "company")) or {}).get("value"))
    for m in GUIDE_LINES:
        v = _num(guidance.get(m))
        if v is not None and put(m, gfy, "company", _yen(m, v)):
            summary.append(f"{m} {gfy} guidance")

    new_dps = _num(guidance.get("dps"))
    if new_dps is not None and dps_before is not None and not _same(new_dps, dps_before):
        ev = {"date": date, "type": "Dividend change", "fy": gfy,
              "dps_old": dps_before, "dps_new": new_dps,
              "note": f"DPS {dps_before:g} → {new_dps:g} with {period} results",
              "source": src, "logged_at": date}
        ev["id"] = CE.new_id(code, date, ev["type"], list(existing_events) + events)
        events.append(ev)

    if buyback and (_num(buyback.get("amount_bn")) or _num(buyback.get("shares_m"))):
        shares = _num(buyback.get("shares_m"))
        ev = {"date": date, "type": "Buyback announced",
              "amount_jpy": (_num(buyback.get("amount_bn")) or 0) * 1e9 or None,
              "shares": shares * 1e6 if shares else None,
              "pct_shares": CE.pct_of_shares(shares * 1e6 if shares else None,
                                             shares_out, treasury),
              "until": buyback.get("until") or "", "note": buyback.get("note") or "",
              "tdnet_title": buyback.get("tdnet_title") or "",
              "source": src, "logged_at": date}
        ev["id"] = CE.new_id(code, date, ev["type"], list(existing_events) + events)
        events.append(ev)
    return entries, events, summary
