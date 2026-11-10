"""
export_feed.py — the Digest's published feed, read by Gloomberg.

Writes data/export/digest_feed.json: per company, the facts the Digest has
already collected that a portfolio tool wants beside its own numbers --

  next_results    next announcement date from the JPX results calendar
  last_results    the most recent one the calendar still lists
  estimates       full-year actuals, company guidance and street consensus,
                  with the reader's typed overrides (consensus_manual.json)
                  applied, exactly as the Research tab's forecast table has them.
                  Shape: {fy: {metric: {basis: [value, source, as_of]}}}; the
                  unit is yen per share for eps and dps, yen for the rest
  filings         TDnet disclosures from the last FILING_DAYS days
  capital_events  buybacks, dividend changes and the like, as logged
  liquidity       average daily value traded and days to exit
  perf_vs_topix   3M / 6M / 12M relative performance

This repository is public, so the feed is public too, and it carries public
market data only. Nothing about anyone's holdings or watchlist goes in here,
in either direction: Gloomberg reads this file, it never writes to this repo.
Anything added to the feed has to pass that test.

The file is rewritten only when its content changes (generated_at aside), so
a run that finds nothing new makes no commit. Each company sits on one line,
so a day's diff shows which companies moved.

Run: python export_feed.py [--data-dir data] [--out data/export/digest_feed.json]
"""

import argparse
import csv
import glob
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

SCHEMA_VERSION = 1
FILING_DAYS = 30          # filings older than this are left out
MAX_FILINGS = 8           # per company, newest first
EVENT_YEARS = 3           # capital events older than this are left out
METRICS = ("net_sales", "operating_profit", "ordinary_profit", "net_profit", "eps", "dps")
BASES = ("actual", "company", "consensus")
JST = timezone(timedelta(hours=9))


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None   # NaN -> None


def _read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _code(v):
    """TSE codes are four characters (7203, and since 2024 also e.g. 285A)."""
    c = str(v or "").strip().upper()
    if c.endswith(".0"):
        c = c[:-2]
    return c if len(c) == 4 and c[:3].isdigit() and c.isalnum() else None


def _http_url(v):
    v = str(v or "").strip()
    return v if v.startswith("https://") or v.startswith("http://") else None


# ── Sections ─────────────────────────────────────────────────────────────────

def load_names(data_dir):
    out = {}
    for r in _read_csv(os.path.join(data_dir, "metadata.csv")):
        c = _code(r.get("Code"))
        if c:
            out[c] = {"name": (r.get("Name") or "").strip(),
                      "sector": (r.get("Sector") or "").strip()}
    return out


def load_results_dates(data_dir, today_iso):
    """{code: {"next": entry, "last": entry}} from the JPX calendar files."""
    import jquants
    entries = []
    for path in sorted(glob.glob(os.path.join(data_dir, "jpx_earnings", "*.xlsx"))):
        with open(path, "rb") as f:
            entries.extend(jquants.parse_jpx_earnings_excel(f.read(), source_label=os.path.basename(path)))
    out = {}
    for e in entries:
        c, d = _code(e.get("code")), e.get("announcement_date") or ""
        if not c or len(d) != 10:
            continue
        slot = out.setdefault(c, {})
        rec = {"date": d, "period": e.get("period_type") or "",
               "fy_end": e.get("fiscal_year_end") or ""}
        if d >= today_iso:
            if "next" not in slot or d < slot["next"]["date"]:
                slot["next"] = rec
        elif "last" not in slot or d > slot["last"]["date"]:
            slot["last"] = rec
    return out


def load_estimates(data_dir):
    """{code: {fy: {metric: {basis: [value, source, as_of]}}}}

    The same merge the Research tab draws: collected rows, then the typed
    overrides on top (typed always wins). Full years only, from the latest
    year with an actual onwards, so the feed carries what is live and not the
    decade of history behind it."""
    import fundamentals as fund
    auto = {}
    for r in _read_csv(os.path.join(data_dir, "consensus.csv")):
        c, val = _code(r.get("code")), _num(r.get("value"))
        if not c or val is None:
            continue
        auto.setdefault(c, {})[(r.get("metric", ""), r.get("fy", ""), r.get("basis", ""))] = {
            "value": val, "unit": r.get("unit", ""), "source": r.get("source", ""),
            "as_of": r.get("as_of", "")}
    manual_path = os.path.join(data_dir, "consensus_manual.json")
    manual = {}
    if os.path.exists(manual_path):
        with open(manual_path, encoding="utf-8") as f:
            manual = json.load(f) or {}
    out = {}
    for c in set(auto) | {k for k in manual if _code(k)}:
        merged = fund.apply_manual_overrides(auto.get(c, {}), manual.get(c, {}))
        rows = [(m, fy, b, e) for (m, fy, b), e in merged.items()
                if m in METRICS and b in BASES and fy.startswith("FY") and e.get("value") is not None]
        if not rows:
            continue
        actual_fys = sorted(fy for m, fy, b, _ in rows if b == "actual")
        floor = actual_fys[-1] if actual_fys else ""
        nested = {}
        for m, fy, b, e in rows:
            if fy >= floor:
                nested.setdefault(fy, {}).setdefault(m, {})[b] = [
                    round(e["value"], 4), e.get("source") or "", e.get("as_of") or ""]
        out[c] = nested
    return out


def load_filings(data_dir, today):
    cutoff = (today - timedelta(days=FILING_DAYS)).strftime("%Y-%m-%d")
    out = {}
    for r in _read_csv(os.path.join(data_dir, "tdnet_filings.csv")):
        c, when = _code(r.get("Code")), (r.get("PubDateTime") or "").strip()
        if not c or when[:10] < cutoff:
            continue
        title = (r.get("Title") or "").strip()
        title_en = (r.get("TitleEN") or "").strip()
        rec = {"at": when, "title": title, "url": _http_url(r.get("DocUrl"))}
        # TitleEN is sometimes just the Japanese title again.
        if title_en and title_en != title:
            rec["title_en"] = title_en
        if _http_url(r.get("EngUrl")):
            rec["url_en"] = _http_url(r.get("EngUrl"))
        out.setdefault(c, []).append({k: v for k, v in rec.items() if v})
    for c in out:
        out[c] = sorted(out[c], key=lambda x: x["at"], reverse=True)[:MAX_FILINGS]
    return out


def load_capital_events(data_dir, today):
    path = os.path.join(data_dir, "capital_events.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f) or {}
    cutoff = (today - timedelta(days=366 * EVENT_YEARS)).strftime("%Y-%m-%d")
    keep = ("date", "type", "fy", "amount_bn", "shares_m", "dps_old", "dps_new", "until", "note", "source")
    out = {}
    for code, events in data.items():
        c = _code(code)
        if not c or not isinstance(events, list):
            continue
        rows = [{k: e.get(k) for k in keep if e.get(k) not in (None, "")}
                for e in events if isinstance(e, dict) and (e.get("date") or "") >= cutoff]
        if rows:
            out[c] = sorted(rows, key=lambda e: e.get("date", ""), reverse=True)
    return out


def load_liquidity(data_dir):
    out = {}
    for r in _read_csv(os.path.join(data_dir, "liquidity.csv")):
        c = _code(r.get("code"))
        if not c:
            continue
        out[c] = {k: v for k, v in {
            "close": _num(r.get("close")), "mcap_bn": _num(r.get("mcap_b")),
            "adv_jpy_20d": _num(r.get("adv_jpy_20")), "adv_jpy_60d": _num(r.get("adv_jpy_60")),
            "days_to_exit_100m": _num(r.get("days_to_exit_100m")),
            "tier": (r.get("tier") or "").strip() or None, "as_of": (r.get("as_of") or "").strip() or None,
        }.items() if v is not None}
    return out


def load_perf(data_dir):
    out = {}
    for r in _read_csv(os.path.join(data_dir, "jquants_3m_perf.csv")):
        c = _code(r.get("Code"))
        if not c:
            continue
        p = {k: v for k, v in {
            "vs_topix_3m": _num(r.get("VsTopix3M")), "vs_topix_6m": _num(r.get("VsTopix6M")),
            "vs_topix_12m": _num(r.get("VsTopix12M")),
            "as_of": (r.get("ComputedDate") or "").strip() or None,
        }.items() if v is not None}
        if len(p) > 1:
            out[c] = p
    return out


# ── Feed ─────────────────────────────────────────────────────────────────────

def build_feed(data_dir="data", now=None):
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(JST)
    today_iso = today.strftime("%Y-%m-%d")
    names = load_names(data_dir)
    sections = {
        "results": load_results_dates(data_dir, today_iso),
        "estimates": load_estimates(data_dir),
        "filings": load_filings(data_dir, today),
        "capital_events": load_capital_events(data_dir, today),
        "liquidity": load_liquidity(data_dir),
        "perf_vs_topix": load_perf(data_dir),
    }
    # A code with nothing but a liquidity row is an ETF, REIT or fund the
    # Digest does not cover; leave those out rather than ship 800 near-empty
    # entries.
    codes = set(names)
    for key, s in sections.items():
        if key != "liquidity":
            codes |= set(s)
    companies = {}
    for c in sorted(codes):
        entry = dict(names.get(c, {}))
        res = sections["results"].get(c, {})
        if res.get("next"):
            entry["next_results"] = res["next"]
        if res.get("last"):
            entry["last_results"] = res["last"]
        for key in ("estimates", "filings", "capital_events", "liquidity", "perf_vs_topix"):
            if sections[key].get(c):
                entry[key] = sections[key][c]
        companies[c] = entry
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "as_of_jst": today_iso,
        "source": "https://github.com/chernyeh/japan.news.digest",
        "notes": ("Public market data collected by the Japan Investment Digest. estimates: "
                  "{fy: {metric: {basis: [value, source, as_of]}}}, values raw -- yen per "
                  "share for eps and dps, yen for the rest."),
        "companies": companies,
    }


def _content_hash(feed: dict) -> str:
    body = {k: v for k, v in feed.items() if k != "generated_at"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def render(feed: dict) -> str:
    """Valid JSON with one company per line, for diffs a person can read."""
    head = {k: v for k, v in feed.items() if k != "companies"}
    lines = ["{"]
    for k, v in head.items():
        lines.append(f"  {json.dumps(k)}: {json.dumps(v, ensure_ascii=False)},")
    lines.append('  "companies": {')
    items = list(feed["companies"].items())
    for i, (c, e) in enumerate(items):
        sep = "," if i < len(items) - 1 else ""
        lines.append(f"    {json.dumps(c)}: {json.dumps(e, ensure_ascii=False, sort_keys=True)}{sep}")
    lines.append("  }")
    lines.append("}")
    return "\n".join(lines) + "\n"


def write_if_changed(feed: dict, out_path: str) -> bool:
    if os.path.exists(out_path):
        try:
            with open(out_path, encoding="utf-8") as f:
                if _content_hash(json.load(f)) == _content_hash(feed):
                    return False
        except (ValueError, OSError):
            pass
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(render(feed))
    return True


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--out", default=os.path.join("data", "export", "digest_feed.json"))
    a = ap.parse_args()
    feed = build_feed(a.data_dir)
    changed = write_if_changed(feed, a.out)
    n = len(feed["companies"])
    print(f"{'Wrote' if changed else 'Unchanged:'} {a.out} — {n} companies")


if __name__ == "__main__":
    main()
