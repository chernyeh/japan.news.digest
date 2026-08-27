"""
fundamentals.py — company guidance, street consensus, and price-based
valuation multiples for the Research tab's forecast panel.

Three sources, in order of preference:

  1. J-Quants /fins/summary — the company's own guidance (the 会社予想 that
     Japanese equities actually trade off) plus the balance-sheet items it
     carries. This is filed data, so it is preferred wherever it exists.
  2. Yahoo Finance via yfinance — analyst consensus, and the balance-sheet
     items J-Quants does not carry (interest-bearing debt, cash, D&A).
  3. The user, by hand or by screenshot (see data/consensus_manual.json) —
     always wins over both, because they are looking at a terminal we cannot.

Everything is normalised to **yen** internally; formatting happens at the
render edge only, so the spreadsheet exports carry raw numbers.

Field names for 1 are resolved through _JQ_ALIASES rather than hardcoded.
The V2 API abbreviates its columns (Eq, TA, BPS) and the repo has no captured
response to check against, so the collector probes the aliases and reports
which key it actually matched — see `collect_consensus.py --probe`. A guessed
schema that silently yields None everywhere is the failure mode this avoids.
"""

import csv
import json
import math
import os

CONSENSUS_PATH = "data/consensus.csv"
FUNDAMENTALS_PATH = "data/fundamentals.csv"
MANUAL_PATH = "data/consensus_manual.json"
UNIVERSE_PATH = "data/jpxnikkei400.csv"
RUN_MANIFEST_PATH = "data/consensus_run.json"

_UA = "japan-news-digest-fundamentals"

# Long-format fact table: one row per company x metric x fiscal year x basis.
# Long rather than wide so it pivots directly in a spreadsheet and survives new
# metrics being added without a schema migration.
CONSENSUS_COLUMNS = ["code", "name", "metric", "fy", "basis",
                     "value", "unit", "source", "as_of"]

# Point-in-time snapshot, one row per company — the inputs P/B and EV/EBITDA
# need, which are not per-fiscal-year forecasts.
FUNDAMENTALS_COLUMNS = ["code", "name", "shares", "bps", "equity", "total_assets",
                        "debt", "cash", "ebitda", "dep_amort", "op_actual",
                        "fy_end", "fy_end_next", "ebitda_basis", "sources", "as_of"]

METRICS = ("net_sales", "operating_profit", "net_profit", "eps", "dps")

_MONTH_ABBR = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def fy_end_month(fy_end: str):
    """Month a fiscal year closes in, from a filed "2027-03-31". None when the
    date is missing or malformed — never a default, because a guessed March on
    a December-close company is worse than showing nothing."""
    try:
        month = int(str(fy_end)[5:7])
    except (TypeError, ValueError):
        return None
    return month if 1 <= month <= 12 else None


def fy_end_note(fy_end: str) -> str:
    """"2027-03-31" -> "yr to Mar 2027". Japanese fiscal years close in March
    for most issuers but in December, February or elsewhere for a meaningful
    minority, and "FY2027" alone does not say which — so every forecast column
    carries the month it actually runs to."""
    month = fy_end_month(fy_end)
    if not month:
        return ""
    return f"yr to {_MONTH_ABBR[month]} {str(fy_end)[:4]}"

# Per-share metrics are in yen each; the rest are absolute yen.
_PER_SHARE = {"eps", "dps"}


# ── J-Quants field resolution ────────────────────────────────────────────
# Each concept maps to the candidate keys seen across V1 (long names) and V2
# (abbreviated). First key present with a non-empty value wins.
_JQ_ALIASES = {
    # Actuals
    "net_sales":        ("Sales", "NetSales"),
    "operating_profit": ("OP", "OperatingProfit"),
    "net_profit":       ("NP", "Profit", "NetProfit"),
    "eps":              ("EPS", "EarningsPerShare"),
    "dps":              ("DivAnn", "ResultDividendPerShareAnnual"),
    # Current-year company forecast
    "f_net_sales":        ("FSales", "ForecastNetSales", "FNCSales"),
    "f_operating_profit": ("FOP", "ForecastOperatingProfit", "FNCOP"),
    "f_net_profit":       ("FNP", "ForecastProfit", "FNCNP"),
    "f_eps":              ("FEPS", "ForecastEarningsPerShare", "FNCEPS"),
    "f_dps":              ("FDivAnn", "FDivTotalAnn", "FDivFY",
                           "ForecastDividendPerShareAnnual"),
    # Next-year company forecast (only filed alongside full-year results)
    "nx_net_sales":        ("NxFSales", "NextYearForecastNetSales", "NxFNCSales"),
    "nx_operating_profit": ("NxFOP", "NextYearForecastOperatingProfit", "NxFNCOP"),
    # NxFNp, not NxFNP — the V2 response really does use that casing.
    "nx_net_profit":       ("NxFNP", "NxFNp", "NextYearForecastProfit", "NxFNCNP"),
    "nx_eps":              ("NxFEPS", "NextYearForecastEarningsPerShare", "NxFNCEPS"),
    "nx_dps":              ("NxFDivAnn", "NxFDivTotalAnn", "NxFDivFY",
                           "NextYearForecastDividendPerShareAnnual"),
    # Balance sheet
    "equity":       ("Eq", "Equity", "NetAssets", "TotalNetAssets"),
    "total_assets": ("TA", "TotalAssets"),
    "bps":          ("BPS", "BookValuePerShare"),
    "cash":         ("CashEq", "Cash", "CashAndEquivalents", "CashAndCashEquivalents"),
    # Confirmed absent from the V2 /fins/summary response — it carries no
    # interest-bearing debt and no depreciation line, so enterprise value and
    # EBITDA keep coming from Yahoo. Kept here so a future field is picked up.
    "debt":         ("IBD", "InterestBearingDebt", "Borrowings", "TotalDebt"),
    "dep_amort":    ("Dep", "Depreciation", "DepreciationAndAmortization"),
    # Period metadata
    "period_type":  ("CurPerType", "TypeOfCurrentPeriod"),
    "fy_end":       ("CurFYEn", "CurrentFiscalYearEndDate"),
    # Filed rather than derived. Japanese fiscal years do not all close in
    # March — a December or February close is common enough that adding twelve
    # months to CurFYEn would quietly mislabel a whole column of forecasts.
    "nx_fy_end":    ("NxtFYEn", "NextFiscalYearEndDate"),
    "disc_date":    ("DiscDate", "DisclosedDate"),
}


def to_num(val):
    """J-Quants sends numbers as strings and absences as "" — and a NaN from
    yfinance is not falsy, so it has to be caught explicitly or it propagates
    silently through every multiple downstream."""
    if val is None or val == "":
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


# Concepts whose value is a date or a code, not a number. These must not go
# through to_num — a fiscal year end of "2027-03-31" coerces to None, which
# would leave every forecast unlabelled and silently unusable.
TEXT_CONCEPTS = frozenset({"period_type", "fy_end", "nx_fy_end", "disc_date"})


def jq_pick(record: dict, concept: str):
    """(number, matched_key) for a numeric `concept`, or (None, "")."""
    for key in _JQ_ALIASES.get(concept, ()):
        if key in record:
            num = to_num(record[key])
            if num is not None:
                return num, key
    return None, ""


def jq_pick_str(record: dict, concept: str):
    """(text, matched_key) for a date/code `concept`, or ("", "")."""
    for key in _JQ_ALIASES.get(concept, ()):
        val = record.get(key)
        if val not in (None, ""):
            return str(val), key
    return "", ""


def jq_get(record: dict, concept: str):
    """Whichever picker suits the concept, so callers need not track which."""
    return (jq_pick_str(record, concept) if concept in TEXT_CONCEPTS
            else jq_pick(record, concept))


def jq_field_report(record: dict) -> dict:
    """{concept: matched_key_or_""} — what the probe mode prints, so the real
    response schema is discoverable from a workflow log."""
    return {concept: jq_get(record, concept)[1] for concept in _JQ_ALIASES}


# ── Valuation ────────────────────────────────────────────────────────────

def _ratio(num, den):
    """Guard every multiple against a zero, negative or missing denominator.
    A loss-making forecast gives a negative P/E, which is not a number worth
    showing — the panel renders those as "n.m." rather than a minus sign the
    eye reads as cheap."""
    if num is None or den is None or den == 0:
        return None
    out = num / den
    return out if out > 0 else None


def enterprise_value(market_cap, debt, cash):
    """Market cap + net debt, all in yen. None if market cap is unknown;
    a missing debt or cash leg is treated as zero only when the other is
    present, since a company with neither reported simply has no net debt
    figure worth pretending to."""
    if market_cap is None:
        return None
    if debt is None and cash is None:
        return None
    return market_cap + (debt or 0) - (cash or 0)


def compute_valuations(price, shares, market_cap, fund: dict, forecasts: dict) -> dict:
    """Multiples at `price`, from a fundamentals row and a
    {(metric, fy, basis): value} forecast map. Every entry may be None; the
    caller renders those as "—" rather than dropping the row, because a blank
    cell where a peer has a number is itself informative.

    `market_cap` in yen. `forecasts` values in yen (absolute) or yen-per-share.
    """
    out = {}
    bps = fund.get("bps")
    if bps is None and fund.get("equity") is not None and shares:
        bps = fund["equity"] / shares

    out["pb"] = _ratio(price, bps)
    out["bps"] = bps

    ev = enterprise_value(market_cap, fund.get("debt"), fund.get("cash"))
    out["ev"] = ev
    out["net_debt"] = (None if (fund.get("debt") is None and fund.get("cash") is None)
                       else (fund.get("debt") or 0) - (fund.get("cash") or 0))
    out["ev_ebitda"] = _ratio(ev, fund.get("ebitda"))

    for fy_key in ("fy1", "fy2"):
        for basis in ("company", "consensus"):
            eps = forecasts.get(("eps", fy_key, basis))
            sales = forecasts.get(("net_sales", fy_key, basis))
            dps = forecasts.get(("dps", fy_key, basis))
            out[f"pe_{fy_key}_{basis}"] = _ratio(price, eps)
            out[f"ps_{fy_key}_{basis}"] = _ratio(market_cap, sales)
            out[f"yield_{fy_key}_{basis}"] = (
                None if (dps is None or not price) else dps / price)

    # Growth and PEG lean on consensus for FY2, since company guidance for a
    # second year barely exists (see the module docstring on NxF*).
    e1 = forecasts.get(("eps", "fy1", "consensus")) or forecasts.get(("eps", "fy1", "company"))
    e2 = forecasts.get(("eps", "fy2", "consensus")) or forecasts.get(("eps", "fy2", "company"))
    growth = None
    if e1 and e2 and e1 > 0:
        growth = (e2 - e1) / e1
    out["eps_growth"] = growth
    pe_for_peg = out.get("pe_fy1_consensus") or out.get("pe_fy1_company")
    out["peg"] = (pe_for_peg / (growth * 100)
                  if (pe_for_peg and growth and growth > 0) else None)
    return out


def guidance_gap(company, consensus, threshold: float = 0.05):
    """Signed fractional gap of consensus vs company guidance, or None when
    either side is missing or the gap is inside `threshold`. Only a gap worth
    looking at earns a chip in the table."""
    if company in (None, 0) or consensus is None:
        return None
    gap = (consensus - company) / abs(company)
    return gap if abs(gap) >= threshold else None


# ── CSV / JSON persistence ───────────────────────────────────────────────

def read_universe(path: str = UNIVERSE_PATH) -> list:
    """[{"Code","MarketDiv","Name","AppliedOn"}, ...] — empty if not built yet."""
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def read_rows(path: str, columns: list) -> list:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return [{c: r.get(c, "") for c in columns} for r in csv.DictReader(fh)]


def write_rows(path: str, columns: list, rows: list):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=columns)
        w.writeheader()
        for row in rows:
            w.writerow({c: row.get(c, "") for c in columns})


def consensus_key(row: dict) -> tuple:
    return (row.get("code", ""), row.get("metric", ""),
            row.get("fy", ""), row.get("basis", ""))


def merge_consensus(existing: list, fresh: list) -> list:
    """A fresh run replaces everything it covers, per (code, basis), and leaves
    the rest alone. A run that fails halfway therefore degrades to a partial
    refresh rather than deleting the companies it never reached.

    Replacing by (code, basis) rather than by the full row key is what stops a
    run that got the fiscal labels wrong from leaving its rows behind forever.
    When the J-Quants key was rejected, the labels fell back to Yahoo's own
    year end and slipped a year, so consensus went in under FY2027/FY2028; the
    corrected run then wrote FY2026/FY2027 and the FY2028 rows survived as
    duplicates of FY2027, a whole phantom year of estimates. Guidance and
    consensus are scoped separately so a Yahoo-only refresh cannot wipe filed
    company numbers, and vice versa."""
    refreshed = {(r.get("code", ""), r.get("basis", "")) for r in fresh}
    merged = {consensus_key(r): r for r in existing
              if (r.get("code", ""), r.get("basis", "")) not in refreshed}
    for row in fresh:
        merged[consensus_key(row)] = row
    return sorted(merged.values(),
                  key=lambda r: (r.get("code", ""), r.get("fy", ""),
                                 r.get("metric", ""), r.get("basis", "")))


# ── GitHub-backed loaders (same pattern as jquants.load_*_from_github) ────

def _raw_csv(repo: str, path: str, token: str = None):
    import requests
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    url = f"https://raw.githubusercontent.com/{repo}/main/{path}"
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code != 200:
        if r.status_code != 404:
            print(f"{path} fetch error: {r.status_code}")
        return []
    return list(csv.DictReader(r.text.splitlines()))


def load_consensus_from_github(repo: str, token: str = None) -> dict:
    """{code: {(metric, fy, basis): {"value","unit","source","as_of"}}}"""
    out = {}
    for row in _raw_csv(repo, CONSENSUS_PATH, token):
        val = to_num(row.get("value"))
        if val is None:
            continue
        out.setdefault(row.get("code", ""), {})[
            (row.get("metric", ""), row.get("fy", ""), row.get("basis", ""))
        ] = {"value": val, "unit": row.get("unit", ""),
             "source": row.get("source", ""), "as_of": row.get("as_of", "")}
    return out


def load_fundamentals_from_github(repo: str, token: str = None) -> dict:
    """{code: {field: float_or_str}} for the balance-sheet snapshot."""
    numeric = {"shares", "bps", "equity", "total_assets", "debt",
               "cash", "ebitda", "dep_amort", "op_actual"}
    # fy_end / fy_end_next stay strings: they are dates, and to_num would
    # turn "2027-03-31" into None.
    out = {}
    for row in _raw_csv(repo, FUNDAMENTALS_PATH, token):
        code = row.get("code", "")
        if not code:
            continue
        out[code] = {k: (to_num(v) if k in numeric else v) for k, v in row.items()}
    return out


def load_universe_from_github(repo: str, token: str = None) -> dict:
    """{code: {"name", "market_div"}} for the JPX-Nikkei 400."""
    return {r["Code"]: {"name": r.get("Name", ""), "market_div": r.get("MarketDiv", "")}
            for r in _raw_csv(repo, UNIVERSE_PATH, token) if r.get("Code")}


def load_manual_from_github(repo: str, token: str = None) -> dict:
    """{code: {"<metric>|<fy>|<basis>": {"value","unit","source","as_of"}}}"""
    import requests
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    url = f"https://raw.githubusercontent.com/{repo}/main/{MANUAL_PATH}"
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except ValueError:
        return {}


def load_run_manifest_from_github(repo: str, token: str = None) -> dict:
    """What the last collector run reached. Lets the panel say *why* a column
    is empty instead of showing dashes that could mean three different things."""
    import requests
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    url = f"https://raw.githubusercontent.com/{repo}/main/{RUN_MANIFEST_PATH}"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        return r.json() if r.status_code == 200 else {}
    except Exception:
        return {}


def apply_manual_overrides(auto: dict, manual_for_code: dict) -> dict:
    """Overlay a company's manual entries onto its auto-collected map. Manual
    always wins — the whole point is that the user is reading a terminal this
    app cannot."""
    merged = dict(auto)
    for flat_key, entry in (manual_for_code or {}).items():
        parts = flat_key.split("|")
        if len(parts) != 3:
            continue
        val = to_num(entry.get("value"))
        if val is None:
            continue
        merged[tuple(parts)] = {"value": val, "unit": entry.get("unit", ""),
                                "source": entry.get("source", "manual"),
                                "as_of": entry.get("as_of", "")}
    return merged


# ── Manual overrides (GitHub-backed, same pattern as research_links.py) ──

def manual_key(metric: str, fy: str, basis: str) -> str:
    """Flat "metric|fy|basis" so the store stays plain JSON — tuple keys do not
    survive a round trip."""
    return f"{metric}|{fy}|{basis}"


def save_manual_overrides(repo: str, token: str, sec_code: str, entries: dict) -> tuple:
    """Merge `entries` into sec_code's block of data/consensus_manual.json,
    committed straight to main (matching how the scheduled data jobs already
    commit data/ updates). Returns (ok, message). Retries once on a 409, where
    the sha moved under us because another session wrote first."""
    import base64
    import requests

    if not token:
        return False, "No GITHUB_TOKEN configured — overrides kept for this session only."

    api_url = f"https://api.github.com/repos/{repo}/contents/{MANUAL_PATH}"
    headers = {"User-Agent": _UA, "Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}

    for attempt in range(2):
        r = requests.get(api_url, headers=headers, timeout=15)
        if r.status_code == 404:
            sha, data = None, {}
        elif r.status_code == 200:
            payload = r.json()
            sha = payload.get("sha")
            try:
                data = json.loads(base64.b64decode(payload.get("content", "")).decode("utf-8"))
            except Exception:
                data = {}
        else:
            return False, f"GitHub read error: HTTP {r.status_code}"

        data.setdefault(sec_code, {}).update(entries)
        body = {
            "message": f"chore: consensus overrides for {sec_code} [skip ci]",
            "content": base64.b64encode(
                json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
            ).decode("ascii"),
            "branch": "main",
        }
        if sha:
            body["sha"] = sha
        put = requests.put(api_url, headers=headers, json=body, timeout=15)
        if put.status_code in (200, 201):
            return True, f"Saved {len(entries)} override(s)."
        if put.status_code == 409 and attempt == 0:
            continue
        return False, f"GitHub write error: HTTP {put.status_code} — {put.text[:160]}"

    return False, "GitHub write failed after retry (concurrent edit)."


# ── Spreadsheet export ───────────────────────────────────────────────────

EXPORT_COLUMNS = ["code", "name", "metric", "fy", "basis", "value",
                  "unit", "source", "as_of"]


def export_rows(code: str, name: str, forecast_map: dict) -> list:
    """Long-format rows for one company, values raw so the receiving sheet can
    compute on them. Never build these from the rendered strings — "¥1,150.0B"
    is display, not data."""
    rows = []
    for (metric, fy, basis), entry in sorted(forecast_map.items()):
        rows.append({
            "code": code, "name": name, "metric": metric, "fy": fy, "basis": basis,
            "value": entry.get("value"),
            "unit": entry.get("unit") or ("jpy" if metric in _PER_SHARE else "jpy_abs"),
            "source": entry.get("source", ""), "as_of": entry.get("as_of", ""),
        })
    return rows


def to_tsv(rows: list, columns: list = None) -> str:
    """Tab-separated, for pasting straight into Google Sheets or Excel — a
    paste splits on tabs with no import dialog. Rendered inside st.code(),
    which supplies its own copy button."""
    cols = columns or EXPORT_COLUMNS
    out = ["\t".join(cols)]
    for row in rows:
        out.append("\t".join("" if row.get(c) is None else str(row.get(c, "")) for c in cols))
    return "\n".join(out)


def to_csv_bytes(rows: list, columns: list = None) -> bytes:
    """UTF-8 **with BOM** — without it Excel mis-decodes the Japanese company
    names on open, which is the single most common complaint about CSV exports
    of Japanese data."""
    import io
    cols = columns or EXPORT_COLUMNS
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for row in rows:
        w.writerow({c: ("" if row.get(c) is None else row.get(c)) for c in cols})
    return buf.getvalue().encode("utf-8-sig")


def to_xlsx_bytes(rows: list, columns: list = None, sheet_name: str = "Consensus") -> bytes:
    """Same data as an .xlsx, via openpyxl (already a dependency for the JPX
    earnings-calendar parser). Numbers are written as numbers, not text, so
    the sheet can sum and chart them without a re-type."""
    import io
    from openpyxl import Workbook

    cols = columns or EXPORT_COLUMNS
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]
    ws.append(cols)
    for row in rows:
        ws.append([row.get(c) for c in cols])
    for i, col in enumerate(cols, start=1):
        width = max([len(col)] + [len(str(r.get(col, ""))) for r in rows[:200]]) + 2
        ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = min(width, 42)
    ws.freeze_panes = "A2"
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
