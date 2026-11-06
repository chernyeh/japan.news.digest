"""
one_offs.py — items that make a quarter look better or worse than the
business underneath it, per company, in data/one_offs.json.

Three ways one gets onto the page:
  * logged — typed, or read from a TDnet notice by notice_reader — with an
    amount and the line of the P&L it lands on;
  * suggested — a TDnet notice whose title announces an extraordinary gain or
    loss, an impairment or an asset sale, not yet logged;
  * flagged — a quarter whose own numbers look unusual against the company's
    history (net profit far from its usual share of ordinary profit; an
    operating margin far from the same quarter in prior years). A flag is a
    reason to look, not a finding.

Logged items feed the "ex one-offs" line in the forecast table: the reported
figure with the item taken out, so YoY and vs-street read on what recurs.

Where a one-off lands decides which lines it moves (Japanese GAAP layout):
  operating      operating profit, ordinary profit, net profit
  non_operating  ordinary profit, net profit
  extraordinary  net profit only (特別損益 sit below ordinary profit)
Net profit moves by the after-tax amount where one is given, else by the
pre-tax amount less a 30% tax charge — an assumption, said so on screen.
"""

import capital_events as CE
import fundamentals as F

ONE_OFFS_PATH = "data/one_offs.json"
LINES = ("operating", "non_operating", "extraordinary")
# Where an item lands decides which lines it is taken out of: an operating
# item comes out of OP and everything below it; a below-OP item leaves OP
# alone and adjusts only the lines under it.
LINE_LABEL = {"operating": "In OP (operating)",
              "non_operating": "Below OP — non-operating",
              "extraordinary": "Below OP — extraordinary"}
ASSUMED_TAX = 0.30

_AFFECTS = {"operating": ("operating_profit", "ordinary_profit", "net_profit"),
            "non_operating": ("ordinary_profit", "net_profit"),
            "extraordinary": ("net_profit",)}

# Titles that announce a one-off. Order matters only for the label.
_TITLE_KEYS = (("特別利益", "Extraordinary gain"), ("特別損失", "Extraordinary loss"),
               ("減損", "Impairment"), ("投資有価証券売却益", "Gain on securities"),
               ("固定資産売却益", "Gain on asset sale"), ("引当金", "Provision"),
               ("和解", "Settlement"), ("訴訟", "Litigation"))


def load_from_github(repo: str, token: str = None) -> dict:
    return CE.load_from_github(repo, token, ONE_OFFS_PATH)


def save_via_github(repo, token, code, upsert=(), remove_ids=()):
    return CE.save_via_github(repo, token, code, upsert, remove_ids,
                              path=ONE_OFFS_PATH, what="one-offs")


def np_impact(entry: dict) -> float:
    """The item's effect on net profit: the after-tax amount where given."""
    v = entry.get("np_amount_jpy")
    if v not in (None, ""):
        return float(v)
    return float(entry.get("amount_jpy") or 0) * (1 - ASSUMED_TAX)


def impact(entries: list, metric: str, fy: str, periods) -> float:
    """Total effect of logged items on `metric` over `periods` (e.g. ("2Q",)
    for a quarter, ("1Q", "2Q") for the first half)."""
    tot = 0.0
    for e in entries or ():
        if e.get("fy") != fy or e.get("period") not in periods:
            continue
        if metric not in _AFFECTS.get(e.get("line"), ()):
            continue
        tot += np_impact(e) if metric == "net_profit" else float(e.get("amount_jpy") or 0)
    return tot


def has_items(entries: list, fy: str, period: str) -> bool:
    return any(e.get("fy") == fy and e.get("period") == period for e in entries or ())


def tdnet_candidates(tdnet_rows: list, code: str, logged: list, results: dict = None) -> list:
    """TDnet notices announcing a one-off that no logged item cites, newest
    first. Where the company's results dates are known, each is placed in the
    period whose results come first on or after the notice — the result it
    will show up in."""
    cited = {e.get("tdnet_title") for e in logged or () if e.get("tdnet_title")}
    upcoming = sorted(((d, k) for k, d in (results or {}).items()), key=lambda t: t[0])
    out = []
    for r in tdnet_rows or ():
        if r.get("Code") and str(r.get("Code"))[:4] != str(code)[:4]:
            continue
        title = r.get("Title", "")
        label = next((lbl for k, lbl in _TITLE_KEYS if k in title), "")
        if not label or title in cited:
            continue
        date = (r.get("PubDateTime") or "")[:10]
        where = next((k for d, k in upcoming if d >= date), None)
        out.append({"date": date, "kind": label, "title": title,
                    "title_en": r.get("TitleEN", ""),
                    "url": r.get("EngUrl") or r.get("DocUrl", ""),
                    "fy": where[0] if where else "", "period": where[1] if where else ""})
    return sorted(out, key=lambda c: c["date"], reverse=True)


def numeric_flags(get, years: list, margin_gap: float = 0.05, np_gap: float = 0.15,
                  recent: int = 6, limit: int = 4) -> list:
    """Quarters whose numbers look unusual against the company's own history.

    1. Net profit's share of ordinary profit (of operating profit for an IFRS
       filer with no ordinary line) more than `np_gap` away from its median
       across the quarters on record — the signature of something below the
       line: an extraordinary item, a tax one-off, a minority swing.
    2. A quarter's operating margin more than `margin_gap` away from the same
       quarter's average in the other years — a one-off inside OP, or a real
       change worth knowing about."""
    qs = []
    for fy in years:
        bd = {m: F.quarter_breakdown(get, m, fy)
              for m in ("net_sales", "operating_profit", "ordinary_profit", "net_profit")}
        for n in (1, 2, 3, 4):
            p = f"q{n}"
            vals = {m: (bd[m].get(p) or (None, ""))[:2] for m in bd}
            # Reported quarters only: skip one with any forecast in it, and
            # one with nothing in it at all.
            if (all(v[0] is None for v in vals.values())
                    or any(v[1] not in ("actual", "derived") for v in vals.values()
                           if v[0] is not None)):
                continue
            qs.append((fy, n, {m: v[0] for m, v in vals.items()}))

    flags = []
    base = "ordinary_profit" if any(v["ordinary_profit"] for _, _, v in qs) else "operating_profit"
    ratios = [(fy, n, v["net_profit"] / v[base]) for fy, n, v in qs
              if v.get(base) and v[base] > 0 and v.get("net_profit") is not None]
    if len(ratios) >= 4:
        med = sorted(r for _, _, r in ratios)[len(ratios) // 2]
        # The band scales with the company's normal: a 15-point swing is a
        # signal where net profit is usually 65% of ordinary profit, and noise
        # where a big non-operating line makes it 120% of operating profit.
        band = max(np_gap, abs(med) * 0.35)
        for fy, n, r in ratios:
            if abs(r - med) > band:
                flags.append({"fy": fy, "period": f"{n}Q", "kind": "below the line",
                              "title": f"{n}Q {fy}: net profit {r * 100:.0f}% of "
                                       f"{'ordinary' if base == 'ordinary_profit' else 'operating'} profit",
                              "detail": f"Usually about {med * 100:.0f}%. Something between "
                                        f"{'ordinary' if base == 'ordinary_profit' else 'operating'} "
                                        "profit and net profit — an extraordinary item, a tax "
                                        "one-off, minorities — moved this quarter."})
    margins = {}
    for fy, n, v in qs:
        if v.get("net_sales") and v["net_sales"] > 0 and v.get("operating_profit") is not None:
            margins.setdefault(n, []).append((fy, v["operating_profit"] / v["net_sales"]))
    for n, pts in margins.items():
        if len(pts) < 2:
            continue
        for fy, m in pts:
            others = [x for y, x in pts if y != fy]
            avg = sum(others) / len(others)
            if abs(m - avg) > margin_gap:
                flags.append({"fy": fy, "period": f"{n}Q", "kind": "margin",
                              "title": f"{n}Q {fy}: OP margin {m * 100:.1f}%",
                              "detail": f"Against {avg * 100:.1f}% in {n}Q of the other "
                                        f"year(s) on record — a one-off inside operating "
                                        "profit, or a real change in the business."})
    # The most recent quarters only: a flag is a reason to look at a result
    # that still matters, not an audit of the whole history.
    keep = sorted({(fy, n) for fy, n, _ in qs}, reverse=True)[:recent]
    keep = {(fy, f"{n}Q") for fy, n in keep}
    flags = [f for f in flags if (f["fy"], f["period"]) in keep]
    return sorted(flags, key=lambda f: (f["fy"], f["period"]), reverse=True)[:limit]


def items_for(entries: list, metric: str, fy: str, periods) -> list:
    """The logged items that move `metric` in `fy` over `periods`."""
    return [e for e in entries or ()
            if e.get("fy") == fy and e.get("period") in periods
            and metric in _AFFECTS.get(e.get("line"), ())]


def describe(entry: dict, metric: str = "operating_profit") -> str:
    """One line for the hover bubble: period, what it is, its effect on this
    line, where it sits, and the note."""
    amt = np_impact(entry) if metric == "net_profit" else float(entry.get("amount_jpy") or 0)
    where = LINE_LABEL.get(entry.get("line"), "")
    # The note is the description now; older entries may carry an item name.
    what = " — ".join(x for x in (entry.get("item"), entry.get("note")) if x) or "One-off"
    head = f"{entry.get('period', '')} · {what}: "
    if metric == "net_profit":
        if entry.get("np_amount_jpy") in (None, ""):
            # Show the arithmetic: the net-profit adjustment is this item's own
            # pre-tax amount, taxed — not anything read off the Adj. OP line.
            pre = float(entry.get("amount_jpy") or 0)
            return (head + f"{pre / 1e9:+,.1f}bn pre-tax × (1 − {ASSUMED_TAX:.0%}) = "
                    f"{amt / 1e9:+,.1f}bn, after an assumed {ASSUMED_TAX:.0%} tax ({where})")
        return head + f"{amt / 1e9:+,.1f}bn after tax, as entered ({where})"
    return head + f"{amt / 1e9:+,.1f}bn ({where})"


def bubble_text(entries: list, metric: str, fy: str, periods, total: float) -> str:
    """The hover text on the "One-off adj." figure: what the figure is, how it
    was worked out, then one line per item. `total` is the effect on `metric`
    (as impact() returns it); the line shows it with the sign flipped, because
    the adjustment takes the item back out."""
    line = {"operating_profit": "operating profit", "ordinary_profit": "ordinary profit",
            "net_profit": "net profit"}.get(metric, metric)
    out = [f"{-total / 1e9:+,.1f}bn added back to reported {line} to strip out one-offs."]
    if metric == "net_profit":
        out.append("Not taken from the Adj. OP line: each item's own after-tax effect "
                   f"— the after-tax amount where one was entered, else the pre-tax "
                   f"amount less an assumed {ASSUMED_TAX:.0%} tax. Minority interests "
                   "are not stripped out.")
    out += [describe(e, metric) for e in items_for(entries, metric, fy, periods)]
    return "\n".join(out)
