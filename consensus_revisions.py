"""
consensus_revisions.py — has the street moved its numbers, and how did the
stock take it?

Reads the estimates log (data/estimates_log/<code>.csv), which already holds
every consensus figure with the date it was seen, and turns it into one
Decisions-waiting flag per watched company:

- after a result (within `cons_post_days`): consensus now against consensus
  as it stood at the close of the results day — the street resetting on the
  print;
- otherwise, the last `cons_roll_days` (30 by default) — drift between
  results. When the next result is close, that is expectations being built
  into the price, so the flag says the results date and how far the stock has
  run against TOPIX over the same stretch.

Two things the log needs care with, or the flags would be noise:

1. Sources are compared only with themselves. Yahoo (collected) and what you
   type from Koyfin / screenshots sit in the same series in the log; a switch
   from one to the other is not a revision. Typed figures are preferred
   where they show a move, Yahoo otherwise.
2. A date on which one source logged two different values for the same
   figure (the 27 Aug 2026 backfill mislabelled some fiscal years) is
   ambiguous, and is skipped rather than read as a 60% revision.

Everything is plain Python over dicts, so it is tested without Streamlit.
"""

from datetime import date, timedelta

import estimates_log as EL

DEFAULTS = {
    "cons_move": 0.03,       # |move| that raises a flag (warn)
    "cons_big": 0.07,        # |move| that makes it "act"
    "cons_show": 0.01,       # smallest move listed as context
    "cons_post_days": 45,    # a result this recent anchors the window
    "cons_roll_days": 30,    # otherwise, the trailing window
    "cons_next_days": 45,    # a result this close is mentioned as "due"
}

TRIGGER_METRICS = ("operating_profit", "eps")
LOG_METRICS = ("operating_profit", "eps", "net_sales", "net_profit")
METRIC_LABEL = {"operating_profit": "OP", "eps": "EPS", "net_sales": "Sales",
                "net_profit": "NP", "ordinary_profit": "RP"}

# How long a cleared rolling-window flag stays cleared (a post-result one is
# tied to its result date, so it never comes back for the same result).
ROLLING_CLEAR_DAYS = 30


def _as_date(v):
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except (TypeError, ValueError):
        return None


def family(source: str) -> str:
    return "yfinance" if (source or "") == "yfinance" else "typed"


def source_label(source: str) -> str:
    s = source or ""
    if s == "yfinance":
        return "Yahoo"
    if "Koyfin" in s:
        return "Koyfin"
    if s.startswith("screenshot"):
        return "screenshot"
    if "Company data" in s:
        return "co. data"
    return "typed"


def fy_short(fy: str) -> str:
    """FY2027 -> FY27."""
    fy = str(fy or "")
    return f"FY{fy[-2:]}" if fy.startswith("FY") and fy[2:].isdigit() else fy


def short_date(iso) -> str:
    d = _as_date(iso)
    return f"{d.day} {d:%b}" if d else ""


def clean_series(rows: list, metric: str, fy: str, fam: str) -> list:
    """[(iso date, value, source)] for one consensus figure from one source
    family, oldest first: one point per date, ambiguous dates dropped,
    repeats dropped."""
    by_date = {}
    for r in rows or ():
        if (r.get("basis") != "consensus" or r.get("metric") != metric
                or r.get("fy") != fy or family(r.get("source")) != fam):
            continue
        v = EL._num(r.get("value"))
        d = str(r.get("date") or "")[:10]
        if v is None or not d:
            continue
        by_date.setdefault(d, []).append((v, r.get("source", "")))
    out = []
    for d in sorted(by_date):
        vals = by_date[d]
        first = vals[0][0]
        if any(not EL._same(v, first) for v, _ in vals[1:]):
            continue                                  # ambiguous: skip the date
        if out and EL._same(out[-1][1], first):
            continue
        out.append((d, first, vals[-1][1]))
    return out


def move(series: list, anchor: str):
    """(old, new, new_date, source) — the last value dated on or before
    `anchor` against the latest value dated after it. None if either side is
    missing or nothing changed."""
    old = [p for p in series if p[0] <= anchor]
    new = [p for p in series if p[0] > anchor]
    if not old or not new:
        return None
    o, n = old[-1], new[-1]
    if o[1] is None or n[1] is None or o[1] <= 0 or EL._same(o[1], n[1]):
        return None
    return o[1], n[1], n[0], n[2]


def moves(rows: list, fys: list, anchor: str, metrics=TRIGGER_METRICS) -> list:
    """[{metric, fy, old, new, chg, date, source}] — one per metric and year
    that moved since `anchor`, typed figures preferred over Yahoo's."""
    out = []
    for m in metrics:
        for fy in fys:
            for fam in ("typed", "yfinance"):
                mv = move(clean_series(rows, m, fy, fam), anchor)
                if mv:
                    o, n, d, src = mv
                    out.append({"metric": m, "fy": fy, "old": o, "new": n,
                                "chg": n / o - 1, "date": d, "source": source_label(src)})
                    break
    return out


def window(row: dict, today: date, cfg: dict) -> tuple:
    """("post", result date) after a recent result, else ("roll", start)."""
    rd = _as_date(row.get("result_date"))
    if rd and 0 <= (today - rd).days <= cfg["cons_post_days"]:
        return "post", rd.isoformat()
    return "roll", (today - timedelta(days=cfg["cons_roll_days"])).isoformat()


def fy_pair(row: dict, rows: list) -> list:
    """FY1 and FY2 — the grid's FY1 where known, else the two latest years
    with consensus in the log."""
    fy1 = row.get("fy1") or ""
    if fy1.startswith("FY") and fy1[2:].isdigit():
        return [fy1, f"FY{int(fy1[2:]) + 1}"]
    fys = sorted({r.get("fy") for r in rows or () if r.get("basis") == "consensus"
                  and r.get("metric") in TRIGGER_METRICS and r.get("fy")})
    return fys[-2:]


def _money(metric: str, v: float) -> str:
    if metric in ("eps", "dps"):
        return f"¥{v:,.1f}"
    return f"¥{v / 1e9:,.1f}bn"


def _items_text(items: list) -> str:
    """"OP FY27 −9.5%, FY28 +3.3% (Koyfin) · EPS FY27 −7.6% (Yahoo)"."""
    groups, order = {}, []
    for it in items:
        k = (it["metric"], it["source"])
        if k not in groups:
            groups[k] = []
            order.append(k)
        groups[k].append(f"{fy_short(it['fy'])} {it['chg'] * 100:+.1f}%")
    return " · ".join(f"{METRIC_LABEL.get(m, m)} {', '.join(groups[(m, s)])} ({s})"
                      for m, s in order)


def _read(direction: str, rel, mode: str, due: bool) -> str:
    """The one-phrase reading of revision against price."""
    if rel is None:
        return ""
    if direction == "down":
        if rel <= -2:
            return "mkt agrees"
        if rel >= 2:
            return "stock up on a cut → looking through / short cover?"
        return "stock flat on a cut → partly priced in?"
    if rel <= 0:
        return "upgrade not rewarded → priced in / over-owned? (rule 10)"
    if rel >= 2:
        return "bar ↑ into results" if due else "upgrade being priced in"
    return "muted reaction"


def revision_flag(row: dict, rows: list, today: date = None, settings: dict = None,
                  rel_since=None):
    """The consensus-revision flag for one watched company, or None.

    `rel_since(iso date)` gives the stock's move against TOPIX from that date
    to now (watchlist_grid.rel_since, bound to the company's price history)."""
    today = today or date.today()
    cfg = {**DEFAULTS, **(settings or {})}
    mode, anchor = window(row, today, cfg)
    fys = fy_pair(row, rows)
    if not fys:
        return None
    items = [i for i in moves(rows, fys, anchor) if abs(i["chg"]) >= cfg["cons_show"]]
    trig = [i for i in items if abs(i["chg"]) >= cfg["cons_move"]]
    if not trig:
        return None

    def rank(i):
        return (i["fy"] != fys[0], i["metric"] != "operating_profit", -abs(i["chg"]))
    primary = sorted(items, key=rank)[0]
    ups = any(i["chg"] > 0 for i in trig)
    downs = any(i["chg"] < 0 for i in trig)
    direction = "up" if primary["chg"] > 0 else "down"
    short = "Cons. mixed" if (ups and downs) else ("Cons. raised" if ups else "Cons. cut")
    big = max(abs(i["chg"]) for i in trig) >= cfg["cons_big"]
    severity = "act" if big else "warn"

    head = (f"Since {row.get('result_period') or 'result'} ({short_date(anchor)})"
            if mode == "post" else f"{cfg['cons_roll_days']}d")
    prim = (f"{METRIC_LABEL.get(primary['metric'])} {fy_short(primary['fy'])} "
            f"{_money(primary['metric'], primary['old'])}→"
            f"{_money(primary['metric'], primary['new']).lstrip('¥')}")
    parts = [f"{head}: cons. {_items_text(items)}", prim]

    rel = rel_since(anchor) if rel_since else None
    if rel is not None:
        parts.append(f"stock {rel:+.1f}% vs TOPIX")
    nr = _as_date(row.get("next_result"))
    due = bool(nr and 0 <= (nr - today).days <= cfg["cons_next_days"])
    if due:
        parts.append(f"next result {short_date(nr)} ({(nr - today).days}d)")
    mixed_fy = ({i["fy"] for i in trig if i["chg"] > 0}
                and {i["fy"] for i in trig if i["chg"] < 0}
                and not ({i["fy"] for i in trig if i["chg"] > 0}
                         & {i["fy"] for i in trig if i["chg"] < 0}))
    read = ""
    if mixed_fy:
        near_down = any(i["chg"] < 0 and i["fy"] == fys[0] for i in trig)
        read = "near yr cut, outer yr raised" if near_down else "near yr raised, outer yr cut"
    r2 = _read(direction, rel, mode, due)
    read = " · ".join(x for x in (read, r2) if x)
    if read:
        parts.append(read)

    flag = {"kind": "cons_rev", "severity": severity, "short": short,
            "key": f"{anchor if mode == 'post' else 'roll'}|{short}|{severity}",
            "text": " · ".join(parts), "items": items, "mode": mode, "anchor": anchor,
            "rel": rel}
    if mode == "roll":
        flag["clear_days"] = ROLLING_CLEAR_DAYS
    return flag


def change_log(rows: list, metrics=LOG_METRICS, since: str = "", min_chg: float = 0.0) -> list:
    """Every consensus change in the log, per source, newest first:
    [{date, metric, fy, old, new, chg, source}]. This is the permanent record
    the flags are drawn from."""
    out = []
    fys = sorted({r.get("fy") for r in rows or () if r.get("basis") == "consensus"})
    for m in metrics:
        for fy in fys:
            for fam in ("typed", "yfinance"):
                s = clean_series(rows, m, fy, fam)
                for (d0, v0, _), (d1, v1, src) in zip(s, s[1:]):
                    if d1 < since or not v0 or v0 <= 0:
                        continue
                    c = v1 / v0 - 1
                    if abs(c) < min_chg:
                        continue
                    out.append({"date": d1, "metric": m, "fy": fy, "old": v0, "new": v1,
                                "chg": c, "source": source_label(src)})
    return sorted(out, key=lambda e: (e["date"], e["metric"], e["fy"]), reverse=True)
