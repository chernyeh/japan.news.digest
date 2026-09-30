"""splits.py — find stock splits in the daily price archive and undo them.

The archive (data/archive/prices_YYYY-MM-DD.csv) keeps each day's *raw* close.
Nothing in it is adjusted for corporate actions, so a company that splits 5:1
inside a lookback window shows an 80% "fall" against its pre-split close. On
29 Sep 2026 -- the ex-date for 1 October splits -- that put eight names at
-50% to -92% vs TOPIX over 3M, and they sat at the top of the Screener.

Telling a split from a crash
----------------------------
The TSE caps how far a price can move in one session: the daily price limit
(制限値幅), a fixed yen amount per price band -- ¥1,500 either way for a stock
priced ¥7,000-¥9,999, for instance (JPX, "Daily Price Limits"). So between two
closes a known number of trading days apart there is a range the price could
have reached by trading at all. A close outside that range did not get there
by trading; it is a split or consolidation (or, rarely, a bad data point).

The test is deliberately loose -- each session's limit is doubled -- so a
price at a band edge cannot trip it. The one real exception is JPX's limit
expansion: after two sessions stuck at the limit, the next session's limit
is widened to four times normal (制限値幅の拡大), which is how a tender-offer
target goes 501 -> 601 -> 1,001 in two days. Expansion needs the step before
to have been a limit move in the same direction, so that is what unlocks the
wider range here. What the looseness costs is sensitivity to small splits:
a 1:1.2 split moves the price less than one session's limit and is not
caught. Those leave an error of a fifth or less, where the ones caught here
were errors of 50-93%.

What is done with a move that trading cannot explain
----------------------------------------------------
- Down, and within 10% of a common factor: a split. Applied.
- Up, within 10% of a common consolidation factor, and backed by a 株式併合
  notice in the TDnet index: a consolidation. Applied. Without the notice an
  up-gap is far more likely a rally the snapshots caught only part of, so
  it is not applied.
- One impossible move followed later by one in the opposite direction that
  undoes it (to within 1.5x): a bad quote -- 1326 was archived in dollars for
  a few days. Both applied at their observed ratios, which cancel.
- Anything else: unresolved. Not applied, and a return whose window spans one
  should not be published (see unresolved_between).

Weekdays stand in for trading days. That over-counts across Japanese
holidays, which only widens the range, so it errs toward "no split".

The factor
----------
The observed ratio carries that day's genuine move on top of the split (a
5:1 split on a -2% day reads 5.10). When the ratio is within 10% of a common
split factor it is snapped to that factor, which keeps the day's own move in
the return; otherwise the observed ratio is used as is. The factors are far
enough apart that the 10% windows never overlap, so a snap cannot land on a
neighbouring factor unless the ex-date itself moved more than 10%.

"""

import csv
import math
import os
from datetime import date, timedelta

EVENTS_PATH = "data/split_events.csv"
EVENT_COLUMNS = ["Code", "ExDate", "Factor", "Status", "PrevDate", "PrevClose",
                 "Close", "Snapped", "TdnetNotice"]

# JPX daily price limits: (price band upper bound, limit in yen). A base price
# below the bound takes that limit. Above the table, 15% is used.
PRICE_LIMITS = [
    (100, 30), (200, 50), (500, 80), (700, 100), (1_000, 150),
    (1_500, 300), (2_000, 400), (3_000, 500), (5_000, 700), (7_000, 1_000),
    (10_000, 1_500), (15_000, 3_000), (20_000, 4_000), (30_000, 5_000),
    (50_000, 7_000), (70_000, 10_000), (100_000, 15_000), (150_000, 30_000),
    (200_000, 40_000), (300_000, 50_000), (500_000, 70_000),
    (700_000, 100_000), (1_000_000, 150_000),
]
LIMIT_MARGIN = 2.0
# JPX's expanded limit is 4x normal; the half on top absorbs band edges.
EXPANDED_MARGIN = 4.5
# A step counts as a limit move -- the precondition for expansion -- when it
# covered this share of its sessions' combined normal limit.
LIMIT_MOVE_SHARE = 0.8
# How far apart an out-and-back pair may land and still be read as a glitch.
GLITCH_BAND = 1.5

# Forward split factors seen on the TSE. Consolidations (株式併合) are their
# reciprocals. Kept sparse on purpose: adjacent factors are more than 20%
# apart, so the SNAP_TOL windows below never overlap.
COMMON_FACTORS = [1.5, 2, 3, 4, 5, 10, 15, 20, 25, 50, 100]
SNAP_TOL = 0.10


def price_limit(price: float) -> float:
    """The one-session limit, in yen, for a stock whose base price is `price`."""
    for bound, limit in PRICE_LIMITS:
        if price < bound:
            return float(limit)
    return price * 0.15


def trading_days_between(d0: date, d1: date) -> int:
    """Weekdays in (d0, d1], at least 1. Holidays are counted, which widens the
    reachable range and so errs toward "no split"."""
    n = sum(1 for k in range(1, (d1 - d0).days + 1)
            if (d0 + timedelta(days=k)).weekday() < 5)
    return max(n, 1)


def reachable_range(prev_close: float, days: int, margin: float = LIMIT_MARGIN,
                    up_margin: float = None, down_margin: float = None) -> tuple:
    """(low, high) a close could reach from `prev_close` by trading over `days`
    sessions, each allowed `margin` times its normal limit. `up_margin` /
    `down_margin` override it for one side (an expanded limit is one-sided)."""
    up = margin if up_margin is None else up_margin
    down = margin if down_margin is None else down_margin
    lo = hi = float(prev_close)
    for _ in range(days):
        lo = max(lo - down * price_limit(lo), 0.0)
        hi = hi + up * price_limit(hi)
    return lo, hi


def _limit_move_direction(c0: float, c1: float, days: int) -> int:
    """+1 / -1 if the step c0 -> c1 was a limit move up / down, else 0."""
    room = sum(price_limit(c0) for _ in range(days))
    if room <= 0:
        return 0
    if (c1 - c0) >= LIMIT_MOVE_SHARE * room:
        return 1
    if (c0 - c1) >= LIMIT_MOVE_SHARE * room:
        return -1
    return 0


def snap_factor(ratio: float, tol: float = SNAP_TOL) -> tuple:
    """(factor, snapped). The nearest common factor -- or its reciprocal for a
    consolidation -- if `ratio` is within `tol` of it, else `ratio` itself."""
    # 1.5 is kept for splits (1:1.5 is a real TSE ratio) but not for
    # consolidations: a +50% day is a rally, and no one consolidates 3-for-2.
    candidates = (COMMON_FACTORS if ratio >= 1
                  else [1 / f for f in COMMON_FACTORS if f >= 2])
    best = min(candidates, key=lambda f: abs(math.log(ratio / f)))
    if abs(ratio / best - 1) <= tol:
        return float(best), True
    return float(ratio), False


def detect_events(points: list, notices: list = None) -> list:
    """Moves in one company's close series that trading cannot explain.

    points:  [(date, close), ...] in any order; non-positive closes ignored.
    notices: dates of this company's 株式分割 / 株式併合 notices on TDnet.
    Returns [{"ex_date", "factor", "prev_date", "prev_close", "close",
    "snapped", "status"}, ...] oldest first. `factor` is old shares -> new
    shares: 5.0 for a 5:1 split, 0.1 for a 10:1 consolidation. ex_date is the
    first snapshot on the new basis. status is "split", "consolidation",
    "glitch" (applied, cancels its partner) or "unresolved" (not applied)."""
    notices = notices or []
    pts = sorted((d, float(c)) for d, c in points if c and float(c) > 0)
    events, prev_dir = [], 0
    for (d0, c0), (d1, c1) in zip(pts, pts[1:]):
        if d1 <= d0:
            continue
        days = trading_days_between(d0, d1)
        lo, hi = reachable_range(
            c0, days,
            up_margin=EXPANDED_MARGIN if prev_dir > 0 else LIMIT_MARGIN,
            down_margin=EXPANDED_MARGIN if prev_dir < 0 else LIMIT_MARGIN)
        if lo <= c1 <= hi:
            prev_dir = _limit_move_direction(c0, c1, days)
            continue
        prev_dir = 0   # whatever this was, it was not a limit move
        factor, snapped = snap_factor(c0 / c1)
        events.append({"ex_date": d1, "factor": factor, "prev_date": d0,
                       "prev_close": c0, "close": c1, "snapped": snapped,
                       "status": "unresolved"})

    # Out-and-back pairs first: a bad quote and its correction.
    k = 0
    while k < len(events) - 1:
        a, b = events[k], events[k + 1]
        ra, rb = a["prev_close"] / a["close"], b["prev_close"] / b["close"]
        if (ra > 1) != (rb > 1) and 1 / GLITCH_BAND <= ra * rb <= GLITCH_BAND:
            for e, r in ((a, ra), (b, rb)):
                e.update(factor=r, snapped=False, status="glitch")
            k += 2
        else:
            k += 1

    for e in events:
        if e["status"] != "unresolved" or not e["snapped"]:
            continue
        if e["factor"] > 1:
            e["status"] = "split"
        elif notice_before(notices, e["ex_date"]):
            e["status"] = "consolidation"
    return events


APPLIED = ("split", "consolidation", "glitch")


def cumulative_factor(events: list, after: date, upto: date) -> float:
    """Product of the factors of events with after < ex_date <= upto: what a
    close taken on `after` must be divided by to be comparable with one taken
    on `upto`."""
    f = 1.0
    for e in events:
        if after < e["ex_date"] <= upto and e.get("status", "split") in APPLIED:
            f *= e["factor"]
    return f


def unresolved_between(events: list, after: date, upto: date) -> bool:
    """True if a move trading cannot explain, and that was not resolved into a
    split, consolidation or glitch, falls in (after, upto]. A return measured
    across it is not one to publish."""
    return any(after < e["ex_date"] <= upto and e.get("status") == "unresolved"
               for e in events)


def adjust_closes(closes: dict, events: list) -> dict:
    """{date: close} restated on the basis of the latest date in the series,
    so consecutive closes give true returns across a split."""
    if not closes or not events:
        return dict(closes)
    last = max(closes)
    return {d: c / cumulative_factor(events, d, last) for d, c in closes.items()}


# ── TDnet cross-check ────────────────────────────────────────────────────────
# Not used to decide anything -- the price-limit test stands on its own, and
# the TDnet index only reaches back a couple of months. Recorded next to each
# event so a reader can see which ones the company itself announced.

def tdnet_split_notices(path: str = "data/tdnet_filings.csv") -> dict:
    """{code: [date, ...]} of 株式分割 / 株式併合 notices in the TDnet index."""
    out = {}
    if not os.path.exists(path):
        return out
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            title = row.get("Title") or ""
            if "株式分割" not in title and "株式併合" not in title:
                continue
            try:
                d = date.fromisoformat((row.get("PubDateTime") or "")[:10])
            except ValueError:
                continue
            out.setdefault((row.get("Code") or "").strip(), []).append(d)
    return out


def notice_before(notices: list, ex_date: date, window_days: int = 180):
    """Latest notice date within `window_days` before `ex_date`, or None."""
    hits = [d for d in notices if ex_date - timedelta(days=window_days) <= d <= ex_date]
    return max(hits) if hits else None


# ── Persistence ──────────────────────────────────────────────────────────────

def write_events(events_by_code: dict, path: str = EVENTS_PATH, notices: dict = None):
    notices = notices or {}
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(EVENT_COLUMNS)
        for code in sorted(events_by_code):
            for e in events_by_code[code]:
                n = notice_before(notices.get(code, []), e["ex_date"])
                w.writerow([code, e["ex_date"].isoformat(), round(e["factor"], 9),
                            e.get("status", ""), e["prev_date"].isoformat(),
                            e["prev_close"], e["close"],
                            "yes" if e["snapped"] else "no",
                            n.isoformat() if n else ""])


def parse_events(rows) -> dict:
    """{code: [event, ...]} from split_events.csv rows (dicts)."""
    out = {}
    for row in rows:
        try:
            e = {"ex_date": date.fromisoformat(row["ExDate"]),
                 "factor": float(row["Factor"]),
                 "status": (row.get("Status") or "split").strip()}
        except (KeyError, ValueError, TypeError):
            continue
        if e["factor"] > 0:
            out.setdefault((row.get("Code") or "").strip(), []).append(e)
    return out


def load_events(path: str = EVENTS_PATH) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, newline="", encoding="utf-8") as fh:
        return parse_events(csv.DictReader(fh))
