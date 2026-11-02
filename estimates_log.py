"""
estimates_log.py — every number the panel has ever shown, with the date it
was true.

Every other store in this app keeps the *current* value: consensus.csv is
replaced by each collector run, consensus_manual.json holds one typed value
per cell, guidance_history.csv keeps only a figure's first and latest value.
That is enough to draw today's table and not enough to answer the questions a
result is judged by afterwards — was guidance raised, did the quarter beat
what the street expected *before* it was reported, was the dividend hiked.

So this is an append-only log, one CSV per company at
data/estimates_log/<code>.csv. A row is written only when a value changes, and
nothing is ever rewritten: the current value of a figure is its newest row,
and every earlier row is what it was before.

Row fields:
  date       when the figure became true — the disclosure date for a filing,
             the day it was observed or typed for everything else
  metric/fy/basis  the same keys as consensus.csv
  value      yen, or yen per share for EPS / DPS, or a share count
  source     where it came from (jquants, yfinance, typed:Koyfin, screenshot:…)
  origin     how it got here: filing / collect / typed / backfill
  logged_at  the day the row was written

Writers: collect_consensus.py (filings and every collector run, including the
post-results refresh), fundamentals.save_manual_overrides (typed and
screenshot values), backfill_estimates_log.py (git history, once).
"""

import csv
import io
import os

LOG_DIR = os.path.join("data", "estimates_log")
COLUMNS = ["date", "metric", "fy", "basis", "value", "source", "origin", "logged_at"]

# Within one day, which row is the later word. A filing is the event itself; a
# collector run that day saw its result; a typed value is the reader overriding
# both, so it sorts last and is what "current" resolves to.
_ORIGIN_ORDER = {"backfill": 0, "filing": 1, "collect": 2, "screenshot": 3, "typed": 4}
_UA = "japan-news-digest"


def path_for(code: str, log_dir: str = LOG_DIR) -> str:
    return os.path.join(log_dir, f"{code}.csv")


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None          # NaN is not a value


def _same(a, b) -> bool:
    if a is None or b is None:
        return a is b
    return abs(a - b) <= max(abs(a), abs(b), 1.0) * 1e-9


def origin_of(source: str) -> str:
    source = source or ""
    if source.startswith("screenshot"):
        return "screenshot"
    if source.startswith("typed") or source == "manual":
        return "typed"
    return "collect"


def compact(rows: list) -> list:
    """Sorted by key then date, with every row that repeats the value before
    it dropped. Idempotent, so merging a log with itself, or with rows it
    already holds, changes nothing."""
    clean = []
    for r in rows or ():
        v = _num(r.get("value"))
        if v is None or not r.get("date") or not r.get("metric") or not r.get("basis"):
            continue
        clean.append({**{c: r.get(c, "") for c in COLUMNS}, "value": v})
    clean.sort(key=lambda r: (r["metric"], r["fy"], r["basis"], r["date"],
                              _ORIGIN_ORDER.get(r["origin"], 9)))
    out, prev_key, prev_val = [], None, None
    for r in clean:
        key = (r["metric"], r["fy"], r["basis"])
        if key == prev_key and _same(r["value"], prev_val):
            continue
        out.append(r)
        prev_key, prev_val = key, r["value"]
    return out


def merge(existing: list, new: list) -> list:
    return compact(list(existing or ()) + list(new or ()))


# ── Local files (the collector and the backfill) ───────────────────────────

def read_local(code: str, log_dir: str = LOG_DIR) -> list:
    p = path_for(code, log_dir)
    if not os.path.exists(p):
        return []
    with open(p, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def to_csv(rows: list) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COLUMNS, lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow({c: (repr(r[c]) if c == "value" and isinstance(r[c], float)
                        else r.get(c, "")) for c in COLUMNS})
    return buf.getvalue()


def write_local(code: str, rows: list, log_dir: str = LOG_DIR) -> int:
    """Merge `rows` into the company's log on disk. Returns how many rows the
    log grew by."""
    old = compact(read_local(code, log_dir))
    merged = merge(old, rows)
    if len(merged) == len(old):
        return 0
    os.makedirs(log_dir, exist_ok=True)
    with open(path_for(code, log_dir), "w", encoding="utf-8", newline="") as fh:
        fh.write(to_csv(merged))
    return len(merged) - len(old)


# Reported figures. consensus.csv stamps all of a company's filed figures with
# its *latest* disclosure date, so a year-old 1Q read from it would be logged
# as reported last week. They enter the log only from the filings themselves
# (collect_consensus.filing_log_rows), each with its own date.
REPORTED_BASES = {"actual", "ytd_1q", "ytd_2q", "ytd_3q"}


def rows_from_consensus(rows: list, logged_at: str, origin: str = "collect") -> list:
    """consensus.csv rows (one company or many) as log rows, reported figures
    excepted (see REPORTED_BASES). A figure's own as_of is its date where it
    has one — a guidance filing's disclosure date, a Yahoo collection date — so
    a backfilled row lands on the day it was true."""
    out = []
    for r in rows or ():
        if r.get("basis") in REPORTED_BASES:
            continue
        out.append({"date": (r.get("as_of") or logged_at)[:10], "metric": r.get("metric", ""),
                    "fy": r.get("fy", ""), "basis": r.get("basis", ""),
                    "value": r.get("value"), "source": r.get("source", ""),
                    "origin": origin, "logged_at": logged_at})
    return out


def rows_from_manual(entries: dict, logged_at: str) -> list:
    """Typed / screenshot override entries ({"metric|fy|basis": {...}})."""
    out = []
    for flat, e in (entries or {}).items():
        parts = flat.split("|")
        if len(parts) != 3:
            continue
        src = e.get("source", "typed")
        out.append({"date": (e.get("as_of") or logged_at)[:10], "metric": parts[0],
                    "fy": parts[1], "basis": parts[2], "value": e.get("value"),
                    "source": src, "origin": origin_of(src), "logged_at": logged_at})
    return out


# ── GitHub (the app) ───────────────────────────────────────────────────────

def load_from_github(repo: str, code: str, token: str = None) -> list:
    import gh_read
    status, text = gh_read.raw_get(repo, path_for(code).replace(os.sep, "/"), token)
    if status != 200 or not text:
        return []
    return compact(list(csv.DictReader(io.StringIO(text))))


def append_via_github(repo: str, token: str, code: str, rows: list) -> tuple:
    """Merge `rows` into data/estimates_log/<code>.csv on main. (ok, message).
    Retries once on a 409, where another save moved the file under us."""
    import base64
    import requests
    if not token:
        return False, "no GITHUB_TOKEN — not logged"
    if not rows:
        return True, "nothing to log"
    path = path_for(code).replace(os.sep, "/")
    api = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {"User-Agent": _UA, "Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}
    for attempt in range(2):
        r = requests.get(api, headers=headers, timeout=15)
        if r.status_code == 404:
            sha, old = None, []
        elif r.status_code == 200:
            payload = r.json()
            sha = payload.get("sha")
            try:
                text = base64.b64decode(payload.get("content", "")).decode("utf-8")
            except Exception:
                text = ""
            old = list(csv.DictReader(io.StringIO(text))) if text else []
        else:
            return False, f"log read error: HTTP {r.status_code}"
        merged = merge(old, rows)
        if sha and len(merged) == len(compact(old)):
            return True, "already logged"
        body = {"message": f"chore: estimates log for {code} [skip ci]",
                "content": base64.b64encode(to_csv(merged).encode("utf-8")).decode("ascii"),
                "branch": "main"}
        if sha:
            body["sha"] = sha
        put = requests.put(api, headers=headers, json=body, timeout=15)
        if put.status_code in (200, 201):
            return True, "logged"
        if put.status_code == 409 and attempt == 0:
            continue
        return False, f"log write error: HTTP {put.status_code}"
    return False, "log write failed after retry"


# ── Reading the log back ───────────────────────────────────────────────────

def series(rows: list, metric: str, fy: str, basis: str) -> list:
    """[(date, value, source)] for one figure, oldest first, changes only."""
    return [(r["date"], _num(r["value"]), r.get("source", ""))
            for r in compact(rows)
            if r["metric"] == metric and r["fy"] == fy and r["basis"] == basis]


def value_before(rows: list, metric: str, fy: str, basis: str, date: str):
    """The figure as it stood strictly before `date` — what the market had in
    hand going into an announcement made that day. None if nothing was logged
    before then."""
    best = None
    for d, v, _ in series(rows, metric, fy, basis):
        if d < date:
            best = v
        else:
            break
    return best


def value_on(rows: list, metric: str, fy: str, basis: str, date: str):
    """The figure at the end of `date` — including anything filed that day."""
    best = None
    for d, v, _ in series(rows, metric, fy, basis):
        if d <= date:
            best = v
        else:
            break
    return best


_PERIOD_BASES = (("1Q", "ytd_1q"), ("2Q", "ytd_2q"), ("3Q", "ytd_3q"), ("FY", "actual"))


def results_dates(rows: list) -> dict:
    """{(fy, "1Q"|"2Q"|"3Q"|"FY"): date} — when each period was first
    reported, read from the earliest logged actual for it. A filing row gives
    the real disclosure date; a figure first seen by a collector run gives the
    day it was seen, which can be a few days late."""
    out = {}
    for r in compact(rows):
        for label, basis in _PERIOD_BASES:
            if r["basis"] == basis and r["metric"] in ("net_sales", "operating_profit",
                                                       "ordinary_profit", "net_profit"):
                k = (r["fy"], label)
                if k not in out or r["date"] < out[k]:
                    out[k] = r["date"]
    return out


def _chg(new, old):
    if new is None or old is None or old <= 0:
        return None
    return new / old - 1


def results_record(rows: list, metrics=("net_sales", "operating_profit", "net_profit")) -> list:
    """One entry per reported period, newest first: what was reported, what
    the street and the company expected going in, and what the company did to
    its full-year guidance that day.

    For a quarter the comparison is the single quarter against a typed street
    estimate for it, where one was logged before the date; for the full year,
    the actual against the full-year street consensus and the company's own
    guidance as they stood the day before."""
    dates = results_dates(rows)
    out = []
    for (fy, label), date in sorted(dates.items(), key=lambda kv: kv[1], reverse=True):
        n = {"1Q": 1, "2Q": 2, "3Q": 3, "FY": 4}[label]
        entry = {"fy": fy, "period": label, "date": date, "lines": {}}
        for m in metrics:
            basis = "actual" if n == 4 else f"ytd_{n}q"
            ytd = value_on(rows, m, fy, basis, date)
            prev_ytd = (None if n == 1 else
                        value_on(rows, m, fy, f"ytd_{n - 1}q", date))
            q = ytd if n == 1 else (None if (ytd is None or prev_ytd is None) else ytd - prev_ytd)
            # A quarter's street estimate is never revised once the quarter is
            # reported, so one typed on the results day itself (read off a
            # terminal that afternoon) is still the pre-results number. The
            # full-year consensus below does move on the day, so it stays strict.
            street_q = (value_before(rows, m, fy, f"consensus_q{n}", date)
                        or value_on(rows, m, fy, f"consensus_q{n}", date))
            line = {"ytd": ytd, "quarter": q, "street_q": street_q,
                    "vs_street_q": _chg(q, street_q)}
            if n == 4:
                line["street_fy"] = value_before(rows, m, fy, "consensus", date)
                line["guide_fy"] = value_before(rows, m, fy, "company", date)
                line["vs_street_fy"] = _chg(ytd, line["street_fy"])
                line["vs_guide_fy"] = _chg(ytd, line["guide_fy"])
            else:
                g = value_before(rows, m, fy, "company", date)
                line["guide_fy"] = g
                line["progress"] = (ytd / g) if (ytd is not None and g) else None
            # What the company did to the year's guidance on the day. On a
            # full-year result that is the *next* year's first guidance, which
            # has no "before" — so it is reported as new, not as a revision.
            gfy = fy if n < 4 else _next_fy(fy)
            before = value_before(rows, m, gfy, "company", date)
            after = value_on(rows, m, gfy, "company", date)
            line["guide_before"], line["guide_after"] = before, after
            line["guide_move"] = (_chg(after, before)
                                  if (before is not None and after is not None
                                      and not _eq(after, before)) else None)
            entry["lines"][m] = line
        out.append(entry)
    return out


def _eq(a, b):
    return _same(a, b)


def _next_fy(fy: str) -> str:
    try:
        return f"FY{int(fy[2:]) + 1}"
    except (TypeError, ValueError):
        return ""


def revisions(rows: list, metric: str, basis: str = "company") -> dict:
    """{fy: [(date, value, change_vs_previous)]} — every step a figure took,
    for the timeline. The first step has no change."""
    by_fy = {}
    for r in compact(rows):
        if r["metric"] == metric and r["basis"] == basis:
            by_fy.setdefault(r["fy"], []).append((r["date"], _num(r["value"])))
    out = {}
    for fy, pts in by_fy.items():
        steps, prev = [], None
        for d, v in pts:
            steps.append((d, v, _chg(v, prev) if prev is not None else None))
            prev = v
        out[fy] = steps
    return out


def dividend_changes(rows: list) -> list:
    """[(date, fy, old, new, change)] for every move in the company's DPS
    guidance, newest first — the hikes and cuts."""
    out = []
    for fy, steps in revisions(rows, "dps", "company").items():
        for i in range(1, len(steps)):
            d, v, c = steps[i]
            out.append((d, fy, steps[i - 1][1], v, c))
    return sorted(out, key=lambda t: t[0], reverse=True)


def summarize_latest(rows: list) -> dict:
    """The newest reported period, condensed for the watchlist scorecard."""
    rec = results_record(rows)
    if not rec:
        return {}
    e = rec[0]
    op = e["lines"].get("operating_profit") or {}
    sales = e["lines"].get("net_sales") or {}
    street_move = None
    if e["period"] != "FY":
        # How the street's full-year number has moved since the result.
        before = value_before(rows, "operating_profit", e["fy"], "consensus", e["date"])
        now = (series(rows, "operating_profit", e["fy"], "consensus") or [(None, None, None)])[-1][1]
        street_move = _chg(now, before) if (before is not None and now is not None
                                            and not _eq(now, before)) else None
    divs = [d for d in dividend_changes(rows) if d[0] >= e["date"]]
    return {"fy": e["fy"], "period": e["period"], "date": e["date"],
            "op_quarter": op.get("quarter"), "op_vs_street": (op.get("vs_street_q")
                                                              if e["period"] != "FY"
                                                              else op.get("vs_street_fy")),
            "sales_vs_street": (sales.get("vs_street_q") if e["period"] != "FY"
                                else sales.get("vs_street_fy")),
            "op_vs_guide": op.get("vs_guide_fy"), "op_progress": op.get("progress"),
            "guide_move": op.get("guide_move"), "street_move": street_move,
            "dps_move": divs[0][4] if divs else None}
