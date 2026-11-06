"""
financials.py — balance-sheet and cash-flow history for the Research tab:
cash, debt, shareholders' equity, operating cash flow, capex and what is
derived from them (net cash vs equity and market cap, free cash flow, ROE).

Sources, per field, in order of preference:

  1. Typed by the user (data/consensus_manual.json, basis "fin") — always wins,
     because a typed figure was read off the filing itself.
  2. J-Quants /fins/summary — the filed tanshin. Carries net assets, total
     assets, the equity ratio, cash & equivalents, and operating / investing
     cash flow. It carries NO capex, NO interest-bearing debt and NO D&A.
  3. Yahoo Finance annual statements — fills capex, debt and D&A, and any
     cash-flow year J-Quants does not have. A SECONDARY source: Yahoo's
     "Total Debt" usually includes lease liabilities, and its fiscal years are
     labelled by the statement date, which is mapped to the same FYyyyy label
     J-Quants uses (the year the fiscal year ends in).

One row per company x fiscal year x period, where period is "FY" (the full
year) or "H1" (the first half). Japanese issuers are only required to publish
a cash-flow statement at the half and the full year, so there is no Q1 or Q3
cash flow to collect — an H1 row is the newest cash flow a company has filed
for most of the year, and it is six months, not twelve: it is never mixed into
an annual free-cash-flow figure.

Shareholders' equity (自己資本) is net assets less non-controlling interests
and subscription rights. J-Quants does not file it directly; it is the filed
equity ratio x total assets, which is exactly how the tanshin defines the
ratio. ROE is computed on it, not on net assets.
"""

import csv
import io
import os

import fundamentals as F

FIN_HISTORY_PATH = "data/financials_history.csv"

FIELDS = ("sh_equity", "net_assets", "total_assets", "cash", "debt",
          "cfo", "cfi", "capex", "dep_amort")
FIN_COLUMNS = ["code", "fy", "period", "period_end"] + list(FIELDS) + ["sources", "as_of"]

# What the panel and the typed-values grid call each field.
LABELS = {
    "cash": "Cash & equivalents",
    "debt": "Interest-bearing debt",
    "sh_equity": "Shareholders' equity",
    "net_assets": "Net assets",
    "total_assets": "Total assets",
    "cfo": "Operating cash flow",
    "cfi": "Investing cash flow",
    "capex": "Capex",
    "dep_amort": "D&A",
}
# The fields a user may type. Capex is the one no filed feed carries; the rest
# are here so a wrong Yahoo figure (debt with leases, say) can be corrected.
TYPEABLE = ("cash", "debt", "sh_equity", "cfo", "capex", "dep_amort")
# Flows over a period vs balances at a date. The latest balance sheet takes
# balances only — a flow needs a period.
FLOWS = ("cfo", "cfi", "capex", "dep_amort")

# Manual store basis: "<field>|FY2026|fin" for a full year, "<field>|FY2027|fin_h1"
# for a first half, "<field>|latest|fin" for the latest balance sheet.
MANUAL_BASIS = {"FY": "fin", "H1": "fin_h1"}
LATEST = "latest"


def fy_label(end: str) -> str:
    end = str(end or "")
    return f"FY{end[:4]}" if len(end) >= 4 and end[:4].isdigit() else ""


# ── J-Quants ─────────────────────────────────────────────────────────────

def _sh_equity(net_assets, total_assets, eqar):
    ratio = F.equity_to_assets(eqar, net_assets, total_assets)
    if ratio is None or not total_assets:
        return None
    return ratio * total_assets


def jq_rows(records: list, years: int = 5) -> list:
    """Rows from the filed tanshins: one per full year (FY filings) and one per
    first half (2Q filings), newest `years` fiscal years of each.

    Read newest-first across every filing for the same year and period, first
    non-empty value per field — an amended tanshin often restates only part of
    the original, the same reason the guidance reader does this."""
    out = {}
    for period, wanted in (("FY", ("FY",)), ("H1", ("2Q",))):
        recs = [r for r in records
                if F.jq_pick_str(r, "period_type")[0].upper() in wanted]
        ends = []
        for r in reversed(recs):
            e = F.jq_pick_str(r, "fy_end")[0][:10]
            if e and e not in ends:
                ends.append(e)
        for end in ends[:years]:
            same = [r for r in reversed(recs) if F.jq_pick_str(r, "fy_end")[0][:10] == end]

            def newest(concept):
                for rec in same:
                    v, k = F.jq_pick(rec, concept)
                    if v is not None:
                        return v, k
                return None, ""

            row, src = {}, {}
            for field, concept in (("net_assets", "equity"), ("total_assets", "total_assets"),
                                   ("cash", "cash"), ("cfo", "cfo"), ("cfi", "cfi"),
                                   ("debt", "debt"), ("dep_amort", "dep_amort")):
                v, k = newest(concept)
                if v is not None:
                    row[field], src[field] = v, f"jquants:{k}"
            eqar, k = newest("equity_to_assets")
            sh = _sh_equity(row.get("net_assets"), row.get("total_assets"), eqar)
            if sh is not None:
                row["sh_equity"], src["sh_equity"] = sh, f"jquants:EqAR×TA"
            if not row:
                continue
            row.update({"fy": fy_label(end), "period": period,
                        "period_end": (end if period == "FY" else ""),
                        "_src": src,
                        "as_of": F.jq_pick_str(same[0], "disc_date")[0][:10]})
            out[(row["fy"], period)] = row
    return list(out.values())


# ── Yahoo ────────────────────────────────────────────────────────────────

# Yahoo statement row -> our field, with the sign convention to undo. Capex is
# reported negative (a cash outflow); it is stored positive, as a spend.
_YH_CF = (("cfo", "Operating Cash Flow", 1), ("capex", "Capital Expenditure", -1),
          ("cfi", "Investing Cash Flow", 1),
          ("dep_amort", "Depreciation And Amortization", 1))
_YH_BS = (("debt", "Total Debt", 1), ("cash", "Cash And Cash Equivalents", 1),
          ("sh_equity", "Stockholders Equity", 1), ("total_assets", "Total Assets", 1))


def yahoo_rows(cashflow, balance_sheet) -> list:
    """FY rows from Yahoo's annual cash-flow and balance-sheet frames (index =
    line item, columns = statement dates). Either may be None or empty."""
    out = {}
    for frame, spec in ((cashflow, _YH_CF), (balance_sheet, _YH_BS)):
        if frame is None or getattr(frame, "empty", True):
            continue
        for col in frame.columns:
            label = fy_label(str(col)[:10])
            if not label:
                continue
            row = out.setdefault(label, {"fy": label, "period": "FY",
                                         "period_end": str(col)[:10], "_src": {}})
            for field, line, sign in spec:
                if line not in frame.index:
                    continue
                v = F.to_num(frame.loc[line, col])
                if v is None:
                    continue
                row[field] = v * sign
                row["_src"][field] = f"yfinance:{line}"
    return [r for r in out.values() if any(f in r for f in FIELDS)]


def merge(code: str, jq: list, yh: list, as_of: str) -> list:
    """J-Quants wins per field (filed); Yahoo fills what it lacks. Capex, debt
    and D&A are therefore Yahoo's in practice — J-Quants files none of them."""
    rows = {}
    for src_rows in (jq, yh):
        for r in src_rows:
            key = (r["fy"], r["period"])
            cur = rows.setdefault(key, {"code": code, "fy": r["fy"], "period": r["period"],
                                        "period_end": r.get("period_end", ""),
                                        "_src": {}, "as_of": r.get("as_of") or as_of})
            if not cur.get("period_end") and r.get("period_end"):
                cur["period_end"] = r["period_end"]
            for f in FIELDS:
                if cur.get(f) is None and r.get(f) is not None:
                    cur[f] = r[f]
                    cur["_src"][f] = r["_src"].get(f, "")
    out = []
    for r in rows.values():
        r["sources"] = ";".join(f"{k}={v}" for k, v in sorted(r.pop("_src").items()))
        out.append(r)
    return sorted(out, key=lambda r: (r["fy"], r["period"]))


def merge_store(existing: list, fresh: list) -> list:
    """Fresh rows replace every stored row for the companies they cover;
    companies not collected this run keep theirs."""
    codes = {r["code"] for r in fresh}
    keep = [r for r in existing if r.get("code") not in codes]
    return sorted(keep + list(fresh), key=lambda r: (r["code"], r["fy"], r["period"]))


def write_local(rows: list, path: str = FIN_HISTORY_PATH):
    existing = F.read_rows(path, FIN_COLUMNS)
    F.write_rows(path, FIN_COLUMNS, merge_store(existing, rows))


# ── Loading ──────────────────────────────────────────────────────────────

def parse_rows(rows) -> dict:
    """{code: {(fy, period): row}} with numbers parsed."""
    out = {}
    for r in rows:
        code = str(r.get("code", "")).strip()
        if not code or not r.get("fy"):
            continue
        row = {k: (F.to_num(r.get(k)) if k in FIELDS else r.get(k, "")) for k in FIN_COLUMNS}
        out.setdefault(code, {})[(r["fy"], r.get("period") or "FY")] = row
    return out


def load_from_github(repo: str, token: str = None) -> dict:
    import gh_read
    return parse_rows(gh_read.raw_csv(repo, FIN_HISTORY_PATH, token))


def load_local(path: str = FIN_HISTORY_PATH) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, newline="", encoding="utf-8") as fh:
        return parse_rows(csv.DictReader(fh))


# ── Typed overrides ──────────────────────────────────────────────────────

def manual_key(field: str, fy: str, period: str = "FY") -> str:
    if fy == LATEST:
        return f"{field}|{LATEST}|fin"
    return f"{field}|{fy}|{MANUAL_BASIS[period]}"


def apply_manual(hist: dict, latest: dict, manual_for_code: dict) -> tuple:
    """(hist, latest, typed) with typed values laid over the collected ones.

    `hist` is {(fy, period): row}; `latest` the balance-sheet snapshot
    {"cash", "debt", "sh_equity", ...}. `typed` is the set of
    (fy_or_latest, period, field) that came from the user, so the panel can
    mark them the way it marks a typed forecast."""
    hist = {k: dict(v) for k, v in (hist or {}).items()}
    latest = dict(latest or {})
    typed = set()
    rev = {v: k for k, v in MANUAL_BASIS.items()}
    for key, entry in (manual_for_code or {}).items():
        parts = key.split("|")
        if len(parts) != 3:
            continue
        field, fy, basis = parts
        if field not in FIELDS:
            continue
        val = F.to_num((entry or {}).get("value"))
        if val is None:
            continue
        if fy == LATEST and basis == "fin":
            latest[field] = val
            typed.add((LATEST, "", field))
        elif basis in rev:
            period = rev[basis]
            row = hist.setdefault((fy, period), {"fy": fy, "period": period})
            row[field] = val
            typed.add((fy, period, field))
    return hist, latest, typed


# ── Derived figures ──────────────────────────────────────────────────────

def latest_balance(fundrow: dict) -> dict:
    """The latest balance sheet from the fundamentals snapshot: cash, debt and
    shareholders' equity (equity ratio x total assets where filed, else net
    assets), with the filing it is from."""
    fr = fundrow or {}
    sh = _sh_equity(fr.get("equity"), fr.get("total_assets"), fr.get("equity_to_assets"))
    return {"cash": fr.get("cash"), "debt": fr.get("debt"),
            "sh_equity": sh if sh is not None else fr.get("equity"),
            "sh_equity_basis": "shareholders'" if sh is not None else "net assets",
            "as_of": fr.get("as_of", ""), "doc_type": fr.get("doc_type", "")}


def _div(a, b):
    if a is None or b in (None, 0):
        return None
    return a / b


def fcf(row: dict):
    """Operating cash flow less capex. None without both — CFO plus investing
    cash flow is NOT used as a stand-in: for a Japanese company parking cash in
    time deposits and securities, investing outflows are mostly not capex."""
    if not row or row.get("cfo") is None or row.get("capex") is None:
        return None
    return row["cfo"] - abs(row["capex"])


def summary(hist: dict, latest: dict, mcap, net_profit_by_fy: dict,
            sales_by_fy: dict = None, years: int = 4) -> dict:
    """Everything the panel shows, as numbers (yen / fractions).

    - net cash = cash - debt on the latest balance sheet; against shareholders'
      equity and against market cap.
    - per full year: CFO, capex, FCF, FCF margin, CFO / net profit (cash
      conversion), ROE on average shareholders' equity.
    - FCF yield on the latest full year that has both CFO and capex.
    """
    hist = hist or {}
    sales_by_fy = sales_by_fy or {}
    cash, debt = latest.get("cash"), latest.get("debt")
    nc = None if (cash is None and debt is None) else (cash or 0) - (debt or 0)
    out = {"cash": cash, "debt": debt, "net_cash": nc,
           "sh_equity": latest.get("sh_equity"),
           "nc_to_equity": _div(nc, latest.get("sh_equity")),
           "nc_to_mcap": _div(nc, mcap), "mcap": mcap, "years": []}
    fys = sorted({fy for (fy, p) in hist if p == "FY"})
    for i, fy in enumerate(fys):
        r = hist.get((fy, "FY")) or {}
        prev = hist.get((fys[i - 1], "FY")) if i else None
        eq, eq0 = r.get("sh_equity"), (prev or {}).get("sh_equity")
        avg_eq = (eq + eq0) / 2 if (eq is not None and eq0 is not None) else None
        np_ = (net_profit_by_fy or {}).get(fy)
        f = fcf(r)
        out["years"].append({
            "fy": fy, "cash": r.get("cash"), "debt": r.get("debt"),
            "net_cash": (None if (r.get("cash") is None and r.get("debt") is None)
                         else (r.get("cash") or 0) - (r.get("debt") or 0)),
            "sh_equity": eq, "cfo": r.get("cfo"), "capex": r.get("capex"),
            "dep_amort": r.get("dep_amort"), "fcf": f,
            "fcf_margin": _div(f, sales_by_fy.get(fy)),
            "cash_conv": _div(r.get("cfo"), np_) if (np_ and np_ > 0) else None,
            "capex_to_da": _div(r.get("capex"), r.get("dep_amort")),
            "roe": _div(np_, avg_eq) if avg_eq and avg_eq > 0 else None,
            "roe_basis": "average" if avg_eq else "",
        })
    out["years"] = out["years"][-years:]
    last = next((y for y in reversed(out["years"]) if y["fcf"] is not None), None)
    out["fcf_latest"] = last
    out["fcf_yield"] = _div(last["fcf"], mcap) if last else None
    # The newest first half, if it is newer than the last full year: its
    # balance sheet at the half-year end and its six months of cash flow.
    h1 = sorted((fy for (fy, p) in hist if p == "H1"), reverse=True)
    if h1 and (not fys or h1[0] > fys[-1]):
        r = hist[(h1[0], "H1")]
        cash, debt = r.get("cash"), r.get("debt")
        nc = None if (cash is None and debt is None) else (cash or 0) - (debt or 0)
        out["h1"] = {"fy": h1[0], "cash": cash, "debt": debt, "net_cash": nc,
                     "sh_equity": r.get("sh_equity"),
                     "nc_to_equity": _div(nc, r.get("sh_equity")),
                     "cfo": r.get("cfo"), "capex": r.get("capex"),
                     "dep_amort": r.get("dep_amort"), "fcf": fcf(r),
                     "capex_to_da": _div(r.get("capex"), r.get("dep_amort"))}
    return out


# ── The typed-values grid ────────────────────────────────────────────────
# Columns are periods: "Latest" (the newest balance sheet), "FY2026" (a full
# year), "1H FY2027" (a first half). Values are typed in ¥bn.

def period_key(col: str) -> tuple:
    """Grid column -> (fy, period) for manual_key; ("latest", "") for Latest."""
    col = str(col or "").strip()
    if col.lower() == LATEST:
        return LATEST, ""
    if col.upper().startswith("1H "):
        return col[3:].strip(), "H1"
    return col, "FY"


def period_col(fy: str, period: str) -> str:
    if fy == LATEST:
        return "Latest"
    return f"1H {fy}" if period == "H1" else fy


def plan_grid(before: dict, after: dict, manual_now: dict, as_of: str) -> tuple:
    """(entries, removed, ignored) from a grid edit. `before` / `after` are
    {(field, column): value_in_bn_or_None}.

    A changed cell becomes a typed entry. A cell cleared where a typed value
    existed removes the override, so the collected number shows through again
    — clearing never stores a blank. An unchanged cell writes nothing.

    Every period column takes every field: a first half is a balance sheet at
    the half-year end as well as six months of cash flow. The one cell that
    cannot be stored is a cash-flow figure under Latest — a flow needs a
    period — and it is returned in `ignored` as (field, column) so the screen
    can say so rather than drop it without a word."""
    entries, removed, ignored = {}, [], []
    for (field, col), new in after.items():
        old = before.get((field, col))
        fy, period = period_key(col)
        new = F.to_num(new)
        if fy == LATEST and field in FLOWS:
            if new is not None and (old is None or abs(new - old) >= 5e-4):
                ignored.append((field, col))
            continue
        key = manual_key(field, fy, period or "FY")
        if new is None:
            if old is not None and key in (manual_now or {}):
                removed.append(key)
            continue
        if old is not None and abs(new - old) < 5e-4:
            continue
        entries[key] = {"value": new * 1e9, "unit": "jpy_abs", "source": "typed",
                        "as_of": as_of}
    return entries, removed, ignored


def describe_key(key: str) -> str:
    """"capex|FY2027|fin_h1" -> "Capex 1H FY2027", for a save message."""
    parts = str(key).split("|")
    if len(parts) != 3:
        return key
    field, fy, basis = parts
    col = "Latest" if fy == LATEST else period_col(fy, "H1" if basis == "fin_h1" else "FY")
    return f"{LABELS.get(field, field)} {col}"
