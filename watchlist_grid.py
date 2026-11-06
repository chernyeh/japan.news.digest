"""
watchlist_grid.py — the arithmetic behind the Watchlist comparison grid.

The grid puts every watched company on one row: valuation, earnings momentum,
price against TOPIX, liquidity and your own thesis figures. Everything here is
plain Python over dicts so it can be tested without Streamlit; app.py loads the
data and draws the table.

Two questions every number in the grid has to answer are "compared to what?"
and "is this one good or bad?". So this module also produces:

- medians, for the list itself and for each sector across the whole collected
  universe (the reference rows under each sector group), and
- a rank shade per cell: the best third of the list on a measure is tinted
  green and the worst third red, using the direction that measure is read in
  (a low P/E is good, a high yield is good).
"""

from datetime import date, timedelta

import splits as S

# (label, key, decimals/format, direction) — direction +1 means higher is
# better, -1 lower is better, 0 means do not rank-shade.
GROUPS = [
    ("Valuation (FY1)", [
        ("P/E street", "pe_st", "x1", -1),
        ("P/E co.", "pe_co", "x1", -1),
        ("PEG", "peg", "x2", -1),
        ("EV/EBITDA", "ev_ebitda", "x1", -1),
        ("P/B", "pb", "x2", -1),
        ("Yield", "yield", "pct1", +1),
        ("Net cash / mkt cap", "net_cash_pct", "pct0", +1),
    ]),
    ("Earnings momentum", [
        ("OP vs street", "op_vs_street", "spct1", +1),
        ("OP guidance", "guide_move", "spct1", +1),
        ("Street FY since", "street_move", "spct1", +1),
        ("Stock since result", "reaction", "spp1", 0),
    ]),
    ("Price vs TOPIX", [
        ("3M", "vs3m", "spp1", 0),
        ("6M", "vs6m", "spp1", 0),
        ("12M", "vs12m", "spp1", 0),
    ]),
    ("Liquidity", [
        ("ADV 20d ¥m", "adv_m", "n0", 0),
        ("Days to exit ¥100m", "days_exit", "n1", 0),
    ]),
    ("Your thesis", [
        ("Conviction", "conviction", "n0", 0),
        ("Munger ¥", "munger_price", "yen", 0),
        ("Price vs Munger", "vs_munger", "spct0", 0),
        ("Bear ¥", "bear_price", "yen", 0),
        ("Exit ¥", "exit_price", "yen", 0),
        ("Upside to exit", "to_exit", "spct0", 0),
        ("Reward : risk", "rr", "rr", 0),
        ("Action", "action", "text", 0),
        ("Review", "review_date", "text", 0),
    ]),
    ("Decision flags", [
        ("Flags", "flags_n", "flags", 0),
    ]),
]

# Multiples where a zero or negative figure means a loss or negative book, not
# a bargain: left out of the ranking and of every median.
POSITIVE_ONLY = {"pe_st", "pe_co", "peg", "ev_ebitda", "pb"}

# What the per-sector universe reference row carries — the measures that have
# a meaningful sector norm.
SECTOR_MEDIAN_KEYS = ["mcap_bn", "pe_st", "pe_co", "peg", "ev_ebitda", "pb",
                      "yield", "net_cash_pct"]

PERIODS = {"3M": (91, 14), "6M": (181, 21), "12M": (365, 28)}
TOPIX_CODES = ("1306", "1308")


def all_columns():
    return [c for _, cols in GROUPS for c in cols]


# ── Statistics ──────────────────────────────────────────────────────────────

def _usable(key: str, value) -> bool:
    if value is None or isinstance(value, (str, bool)):
        return False
    try:
        if value != value or value in (float("inf"), float("-inf")):   # NaN, ∞
            return False
    except TypeError:
        return False
    return not (key in POSITIVE_ONLY and value <= 0)


def median(values: list):
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    mid = len(vals) // 2
    return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2


def medians(rows: list, keys: list) -> dict:
    """{key: median} over `rows`, skipping blanks and unusable multiples; also
    "_n", the number of rows."""
    out = {k: median([r.get(k) for r in rows if _usable(k, r.get(k))]) for k in keys}
    out["_n"] = len(rows)
    return out


def sector_medians(universe_rows: list, keys: list = None) -> dict:
    """{sector: medians} across the whole collected universe."""
    keys = keys or SECTOR_MEDIAN_KEYS
    by = {}
    for r in universe_rows or []:
        by.setdefault(r.get("sector") or "Unclassified", []).append(r)
    return {s: medians(rs, keys) for s, rs in by.items()}


def rank_shades(rows: list, key: str, direction: int) -> list:
    """Per row: "good", "bad" or "" — the best and worst third of the rows on
    `key`. Needs at least three usable figures; with fewer, nothing is shaded,
    because a rank among two is not information."""
    if not direction:
        return [""] * len(rows)
    vals = [(i, r.get(key)) for i, r in enumerate(rows) if _usable(key, r.get(key))]
    out = [""] * len(rows)
    if len(vals) < 3:
        return out
    ordered = sorted(vals, key=lambda t: t[1] * direction, reverse=True)
    third = max(1, len(ordered) // 3)
    for i, _ in ordered[:third]:
        out[i] = "good"
    for i, _ in ordered[-third:]:
        out[i] = "bad"
    return out


# ── Thesis arithmetic ───────────────────────────────────────────────────────

def thesis_metrics(price, thesis: dict) -> dict:
    """Where the price sits against your own levels.

    vs_munger: price / Munger price - 1. Zero or below means the stock is at or
    under the price you said would be an extraordinary margin of safety.
    to_exit: exit price / price - 1, the upside left to your fully-valued
    level; negative means it has already passed it."""
    thesis = thesis or {}
    out = {k: thesis.get(k) for k in ("conviction", "munger_price", "bear_price",
                                      "exit_price", "action", "action_since",
                                      "review_date", "note")}
    m, x = thesis.get("munger_price"), thesis.get("exit_price")
    out["vs_munger"] = (price / m - 1) if (price and m) else None
    out["to_exit"] = (x / price - 1) if (price and x) else None
    out["rr"] = reward_to_risk(price, thesis.get("bear_price"), x)
    return out


def reward_to_risk(price, bear, exit_price):
    """(exit - price) / (price - bear): yen of upside to your exit for each yen
    of downside to your bear-case value. None without all three, or when the
    price is already at or past the exit (no reward left to weigh). A price at
    or under the bear value has no measured downside: float("inf")."""
    if not (price and bear and exit_price) or exit_price <= price:
        return None
    if price <= bear:
        return float("inf")
    return (exit_price - price) / (price - bear)


# ── Relative performance from the weekly price history ──────────────────────
# data/jquants_3m_perf.csv carries a 3M figure for only ~125 of the ~980
# collected companies (the daily price sweep does not reach every company every
# day, so most lack an archived base price). data/price_history.csv has a weekly
# close for nearly all of them, which is enough for a 3M and 6M figure.

def _as_date(v):
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except (TypeError, ValueError):
        return None


def close_near(series: list, target: date, tol_days: int):
    """(date, close) of the point in `series` nearest `target`, within
    `tol_days`, preferring the earlier point on a tie; None if none is close
    enough."""
    best = None
    for d, c in series or []:
        d = _as_date(d)
        if d is None or not c:
            continue
        gap = abs((d - target).days)
        if gap > tol_days:
            continue
        if best is None or gap < best[0] or (gap == best[0] and d < best[1]):
            best = (gap, d, float(c))
    return (best[1], best[2]) if best else None


def _latest(series: list):
    pts = [(_as_date(d), c) for d, c in series or [] if _as_date(d) and c]
    return max(pts) if pts else None


def rel_perf_from_history(series: list, topix_series: list, events: list = None,
                          latest_price=None, latest_date=None) -> dict:
    """{"vs3m", "vs6m", "vs12m"} — the stock's return relative to TOPIX, in
    percentage points of geometric excess ((1+r)/(1+r_topix) - 1), as
    compute_3m_perf.py measures it.

    `series` and `topix_series` are [(iso date, close)] weekly. Closes are as
    recorded, so a split in the window is taken out with the factors in
    data/split_events.csv; a window that spans an unresolved move is left
    blank rather than published as a crash."""
    out = {"vs3m": None, "vs6m": None, "vs12m": None}
    end = _latest(series)
    if latest_price and latest_date:
        end = (_as_date(latest_date), float(latest_price))
    tend = _latest(topix_series)
    if not end or not tend or abs((end[0] - tend[0]).days) > 7:
        return out
    events = events or []
    for label, (days, tol) in PERIODS.items():
        target = end[0] - timedelta(days=days)
        base = close_near(series, target, tol)
        tbase = close_near(topix_series, target, tol)
        if not base or not tbase:
            continue
        if S.unresolved_between(events, base[0], end[0]):
            continue
        b = base[1] / S.cumulative_factor(events, base[0], end[0])
        r = end[1] / b - 1
        rt = tend[1] / tbase[1] - 1
        out["vs" + label.lower()] = round(((1 + r) / (1 + rt) - 1) * 100, 1)
    return out


def topix_series(history: dict) -> list:
    """The TOPIX proxy's weekly series: the first of the TOPIX ETFs present."""
    for c in TOPIX_CODES:
        if history.get(c):
            return history[c]
    return []


# ── Formatting ──────────────────────────────────────────────────────────────

def fmt(value, kind: str) -> str:
    """Cell text for a value; "" for a blank (the caller draws the dash)."""
    if value is None or value == "":
        return ""
    if kind == "text":
        return str(value)
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value)
    if v != v:
        return ""
    if kind == "x1":
        return f"{v:.1f}x"
    if kind == "x2":
        return f"{v:.2f}x"
    if kind == "pct0":
        return f"{v * 100:.0f}%"
    if kind == "pct1":
        return f"{v * 100:.1f}%"
    if kind == "spct0":
        return f"{v * 100:+.0f}%"
    if kind == "spct1":
        return f"{v * 100:+.1f}%"
    if kind == "spp1":
        return f"{v:+.1f}%"
    if kind == "n0":
        return f"{v:,.0f}"
    if kind == "n1":
        return f"{v:,.1f}"
    if kind == "yen":
        return f"¥{v:,.0f}" if v >= 100 else f"¥{v:,.1f}"
    if kind == "rr":
        return "∞" if v == float("inf") else f"{v:.1f}:1"
    return str(value)


def sort_rows(rows: list, key: str, descending: bool = False) -> list:
    """Rows sorted on `key`, blanks always last whichever way the sort runs."""
    def usable(r):
        v = r.get(key)
        return v is not None and v == v and v != ""
    have = [r for r in rows if usable(r)]
    blank = [r for r in rows if not usable(r)]
    try:
        have.sort(key=lambda r: r[key], reverse=descending)
    except TypeError:
        have.sort(key=lambda r: str(r[key]), reverse=descending)
    return have + blank


# ── Price reaction to a result ──────────────────────────────────────────────

REACTION_MIN_DAYS = 5
MAX_REACTION_DAYS = 14


def reaction_after(series: list, topix_series: list, result_date, events: list = None,
                   min_days: int = REACTION_MIN_DAYS):
    """(relative %, end date, settled) — how the stock moved against TOPIX
    after a result, or None if there is nothing to measure yet.

    From the last weekly close on or before the results day (TSE results come
    out at or after the close, so that close is the pre-result price) to the
    first weekly close at least `min_days` after it. Until that close exists
    the latest close is used and `settled` is False: the reaction so far."""
    d0 = _as_date(result_date)
    if not d0 or not series or not topix_series:
        return None
    pts = sorted((_as_date(d), float(c)) for d, c in series if _as_date(d) and c)
    tpts = sorted((_as_date(d), float(c)) for d, c in topix_series if _as_date(d) and c)
    before = [p for p in pts if p[0] <= d0]
    if not before:
        return None
    base = before[-1]
    # The weekly history has holes (the daily sweep did not reach every company
    # every day before the universe was queued first). A base or end close too
    # far from the result measures drift, not a reaction, so none is given.
    if (d0 - base[0]).days > 7:
        return None
    after = [p for p in pts if p[0] >= d0 + timedelta(days=min_days)]
    settled = bool(after)
    end = after[0] if after else (pts[-1] if pts[-1][0] > d0 else None)
    if not end or (end[0] - d0).days > MAX_REACTION_DAYS:
        return None
    tb = [p for p in tpts if p[0] <= base[0]]
    te = [p for p in tpts if p[0] <= end[0]]
    if not tb or not te or (base[0] - tb[-1][0]).days > 7 or (end[0] - te[-1][0]).days > 7:
        return None
    events = events or []
    if S.unresolved_between(events, base[0], end[0]):
        return None
    b = base[1] / S.cumulative_factor(events, base[0], end[0])
    r = end[1] / b - 1
    rt = te[-1][1] / tb[-1][1] - 1
    return round(((1 + r) / (1 + rt) - 1) * 100, 1), end[0].isoformat(), settled


# ── Decision flags ──────────────────────────────────────────────────────────
# Each flag is something that asks for a decision, not a fact to admire. The
# wording leans on the behaviours the flag exists to catch: sitting on an
# opportunity that already passed your own hurdle (the procrastination trap),
# treating a gain as house money, and reading a beat as good news when the
# market did not (your rule 10).

DEFAULTS = {"watch_days": 60, "near_exit": 0.10, "rr_hurdle": 3.0,
            "result_window_days": 90}

SEVERITY_ORDER = {"act": 0, "warn": 1, "info": 2}


def _days_since(iso, today: date):
    d = _as_date(iso)
    return (today - d).days if d else None


def _rr_txt(rr):
    return "∞" if rr == float("inf") else f"{rr:.1f}:1"


def beat(row: dict) -> bool:
    """A result the market should have liked: OP beat the street, or (with no
    street figure to beat) the company raised its OP guidance."""
    v = row.get("op_vs_street")
    if v is not None:
        return v > 0
    return (row.get("guide_move") or 0) > 0


def decision_flags(row: dict, today: date = None, settings: dict = None) -> list:
    """[{"kind", "severity", "short", "text"}] for one grid row."""
    today = today or date.today()
    cfg = {**DEFAULTS, **(settings or {})}
    out = []
    price, action = row.get("price"), (row.get("action") or "")
    rr = row.get("rr")

    vm = row.get("vs_munger")
    if vm is not None and vm <= 0:
        holding_back = action in ("", "WATCH")
        out.append({
            "kind": "below_munger", "severity": "act", "short": "≤ Munger",
            "text": (f"At {vm * 100:+.0f}% to your Munger price of ¥{row['munger_price']:,.0f}: "
                     "the price you said would be an extraordinary margin of safety. "
                     + ("Still on WATCH — if the thesis holds, this is the price you said you would buy at."
                        if holding_back else f"Action is {action}."))})

    te = row.get("to_exit")
    if te is not None and action not in ("TRIM", "SELL"):
        if te <= 0:
            out.append({
                "kind": "past_exit", "severity": "act", "short": "Past exit",
                "text": (f"Price is {-te * 100:.0f}% above your exit of ¥{row['exit_price']:,.0f}. "
                         "Trim, or raise the exit with the reason written down — not because "
                         "it has gone up (house money).")})
        elif te <= cfg["near_exit"]:
            out.append({
                "kind": "near_exit", "severity": "warn", "short": "Near exit",
                "text": (f"Within {te * 100:.0f}% of your exit of ¥{row['exit_price']:,.0f}. "
                         "Decide now whether to trim at the exit, while it is a plan rather "
                         "than a reaction.")})

    if action == "WATCH" and rr is not None and rr >= cfg["rr_hurdle"]:
        since = row.get("action_since") or row.get("thesis_updated") or row.get("added_at")
        days = _days_since(since, today)
        if days is not None and days >= cfg["watch_days"]:
            out.append({
                "kind": "stuck_watch", "severity": "act", "short": "Stuck on WATCH",
                "text": (f"Reward-to-risk {_rr_txt(rr)} clears your {cfg['rr_hurdle']:.0f}:1 hurdle "
                         f"and it has been on WATCH for {days} days. Name the single fact you "
                         "are waiting for — if there is none, this is fortification, not diligence.")})

    rd = _days_since(row.get("review_date"), today)
    if rd is not None and rd >= 0:
        out.append({
            "kind": "review_due", "severity": "warn", "short": "Review due",
            "text": ("Review date is today." if rd == 0 else
                     f"Review date passed {rd} day{'s' if rd != 1 else ''} ago "
                     f"({row['review_date']}).")})

    rx = row.get("reaction")
    rdays = _days_since(row.get("result_date"), today)
    if (rx is not None and rx <= 0 and beat(row)
            and rdays is not None and rdays <= cfg["result_window_days"]):
        how = (f"beat the street by {row['op_vs_street'] * 100:+.1f}% on OP"
               if row.get("op_vs_street") is not None
               else f"raised OP guidance {row['guide_move'] * 100:+.1f}%")
        out.append({
            "kind": "beat_fell", "severity": "warn", "short": "Beat, stock fell",
            "text": (f"{row.get('result_label', 'Latest result')}: {how}, but the stock is "
                     f"{rx:+.1f}% vs TOPIX since"
                     + ("" if row.get("reaction_settled") else " (so far)")
                     + ". Good results not rewarded: already assumed, or the sector is "
                       "over-owned (rule 10).")})
    return sorted(out, key=lambda f: SEVERITY_ORDER[f["severity"]])


def peer_flags(rows: list, today: date = None, settings: dict = None) -> dict:
    """{code: [flag]} — rule 10 applied to peers: when a watched company in the
    same sector beat and its stock still fell, that is a read on the sector
    for every other watched name in it."""
    today = today or date.today()
    cfg = {**DEFAULTS, **(settings or {})}
    def recent(r):
        d = _days_since(r.get("result_date"), today)
        return d is not None and d <= cfg["result_window_days"]
    fell = [r for r in rows
            if r.get("reaction") is not None and r["reaction"] <= 0 and beat(r) and recent(r)]
    out = {}
    for r in rows:
        peers = [p for p in fell if p["code"] != r["code"]
                 and p.get("sector") and p.get("sector") == r.get("sector")]
        if peers:
            names = ", ".join(f"{p.get('name') or p['code']} ({p['reaction']:+.1f}%)" for p in peers)
            out[r["code"]] = [{
                "kind": "peer_beat_fell", "severity": "info", "short": "Peer beat, fell",
                "text": (f"Sector peer beat and fell: {names}. A red flag for the sector — "
                         "good results may already be priced in.")}]
    return out
