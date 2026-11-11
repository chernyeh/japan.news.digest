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


GOING_IN = ("street", "guide", "h1")


def going_in_basis(column: str, period: str) -> str:
    """Where a corrected "going in" figure is stored: the street's estimate
    for the period (a quarter's own estimate, or the full-year consensus at a
    full-year result), the full-year guidance, or the 1H guidance."""
    if column == "street":
        return "consensus" if period == "FY" else f"consensus_q{PERIODS.index(period) + 1}"
    return "company" if column == "guide" else "company_h1"


def _day_before(date: str) -> str:
    from datetime import date as _d, timedelta
    return (_d.fromisoformat(date) - timedelta(days=1)).isoformat()


def plan_going_in(fy: str, period: str, typed: dict, shown: dict, guidance: dict,
                  date: str, source: str = "") -> tuple:
    """(entries, log_rows) for the "going in" columns of the results-day grid —
    the street estimate and the company's guidance as they stood before the
    result — where the reader typed over what was shown.

    typed / shown: {metric: {"street"|"guide"|"h1": value in panel units}}.
    Dated the day before `date`, so every look-back reads them as what was
    expected going in rather than as something said on the day.

    Full-year guidance is the one clash: at a quarterly result the same cell
    may also get new guidance in section 2. The new number is then the current
    one, and the corrected going-in figure goes to the history log only."""
    src = F.typed_source(source)
    before = _day_before(date)
    entries, log_rows = {}, []
    for m, cols in (typed or {}).items():
        for col in GOING_IN:
            v = _num((cols or {}).get(col))
            was = _num(((shown or {}).get(m) or {}).get(col))
            if v is None or _same(v, was):
                continue
            basis = going_in_basis(col, period)
            row = {"value": _yen(m, v), "unit": "jpy" if m in ("eps", "dps") else "jpy_abs",
                   "source": src, "as_of": before}
            clash = (col == "guide" and period != "FY"
                     and _num((guidance or {}).get(m)) is not None)
            if clash:
                log_rows.append({"date": before, "metric": m, "fy": fy, "basis": basis,
                                 "value": row["value"], "source": src, "origin": "typed",
                                 "logged_at": date})
            else:
                entries[F.manual_key(m, fy, basis)] = row
    return entries, log_rows


# How close new guidance has to be to the street's full-year number to count
# as "in line". A judgement call, kept in one place: within ±2% either way.
INLINE_BAND = 0.02


def vs_consensus(guidance, consensus, band: float = INLINE_BAND):
    """("above" | "in line" | "below", signed change) of a guidance figure
    against the street's full-year consensus, or None when either is missing.
    The change is measured off the consensus, so +4% means guidance sits 4%
    above what the street had."""
    g, c = _num(guidance), _num(consensus)
    if g is None or c is None or c == 0:
        return None
    chg = (g - c) / abs(c)
    if abs(chg) <= band:
        return "in line", chg
    return ("above" if chg > 0 else "below"), chg


def plan_fy_consensus(gfy: str, typed: dict, shown: dict, date: str,
                      source: str = "") -> dict:
    """Entries for the street's full-year consensus typed over in the guidance
    grid — the year the guidance is for, stored under "consensus" and dated the
    day before the result, like every other going-in figure.

    typed / shown: {metric: value in panel units}."""
    src = F.typed_source(source)
    before = _day_before(date)
    entries = {}
    for m, v in (typed or {}).items():
        v, was = _num(v), _num((shown or {}).get(m))
        if v is None or _same(v, was):
            continue
        entries[F.manual_key(m, gfy, "consensus")] = {
            "value": _yen(m, v), "unit": "jpy" if m in ("eps", "dps") else "jpy_abs",
            "source": src, "as_of": before}
    return entries
