"""
collect_consensus.py — builds data/consensus.csv and data/fundamentals.csv for
the Research tab's forecast panel, over the JPX-Nikkei 400.

Run modes (the scheduled one is .github/workflows/weekly_consensus.yml):

    python collect_consensus.py                    # full run over the universe
    python collect_consensus.py --limit 10         # smoke test
    python collect_consensus.py --only 6504,7203   # named companies
    python collect_consensus.py --probe 6504       # print J-Quants' real schema
    python collect_consensus.py --dry-run          # fetch, report, write nothing

Wants JQUANTS_API_KEY in the environment for company guidance. Without it the
run still produces consensus and Yahoo balance-sheet data, and says so — half
a panel beats no panel.

**The --probe mode matters.** J-Quants V2 abbreviates its response columns and
we have no captured response in the repo to check names against, so
fundamentals.py resolves each concept through an alias list. Probe prints which
key actually matched for one company; if a concept comes back unmatched, add
the real key to _JQ_ALIASES rather than letting it silently stay None.

A full run also prints a coverage report — how many of the 400 yielded FY1/FY2
consensus, a book value, and debt/cash. That is the number to look at before
trusting the panel: Yahoo's estimate coverage of Japanese mid-caps is not
guaranteed, and the report says exactly where the holes are.
"""

import argparse
import os
import sys
import time
from datetime import date, datetime, timezone

import fundamentals as F

_JQ_BASE = "https://api.jquants.com/v2"
_TIMEOUT = 20


def diagnose_key(api_key: str) -> list:
    """Describe what is wrong with a rejected key without ever printing it.

    Two rounds were lost to a 403 whose only message was "invalid or expired",
    which covers several very different mistakes. This separates them:

      - a value pasted with a trailing newline (GitHub stores secrets verbatim)
      - a V1 ID token pasted where a V2 API key belongs; those are JWTs and
        expire after 24 hours, which matches "expired" exactly
      - an opaque key that is simply wrong or revoked

    A JWT's payload is base64, not encrypted, so its expiry can be read locally.
    Only the expiry and the shape are reported — never the value."""
    import base64
    import json as _json

    notes = []
    if not api_key:
        return ["The secret is empty."]
    if api_key != api_key.strip():
        notes.append("The value has leading or trailing whitespace — GitHub stores "
                     "secrets verbatim, so a stray newline from copy-paste ends up "
                     "in the header. Re-paste without it. (Collector calls now "
                     "strip it, so this alone should no longer cause a 403.)")
    notes.append(f"Key shape: {len(api_key.strip())} characters, "
                 f"{'dot-separated (JWT-like)' if api_key.strip().count('.') == 2 else 'opaque'}.")

    parts = api_key.strip().split(".")
    if len(parts) == 3:
        notes.append("This looks like a **V1 ID token**, not a V2 API key. V2 wants the "
                     "key from the J-Quants dashboard; ID tokens are the deprecated "
                     "V1 flow and last only 24 hours.")
        try:
            pad = parts[1] + "=" * (-len(parts[1]) % 4)
            claims = _json.loads(base64.urlsafe_b64decode(pad))
            exp = claims.get("exp")
            if exp:
                when = datetime.fromtimestamp(exp, tz=timezone.utc)
                state = "EXPIRED" if when < datetime.now(timezone.utc) else "still valid"
                notes.append(f"Token expiry: {when.isoformat()} ({state}).")
        except Exception:
            notes.append("Could not read an expiry from it.")
    return notes


def probe_auth(api_key: str, code: str) -> list:
    """Try each auth style against the API and report the status of each, so a
    wrong *scheme* is distinguishable from a wrong *key*."""
    import requests
    results = []
    attempts = (
        ("v2 + x-api-key (expected)", f"{_JQ_BASE}/fins/summary", {"x-api-key": api_key}),
        ("v2 + Bearer",               f"{_JQ_BASE}/fins/summary",
         {"Authorization": f"Bearer {api_key}"}),
        ("v1 + Bearer (deprecated)",  "https://api.jquants.com/v1/fins/statements",
         {"Authorization": f"Bearer {api_key}"}),
    )
    for label, url, headers in attempts:
        try:
            r = requests.get(url, headers=headers, params={"code": code}, timeout=_TIMEOUT)
            body = (r.text or "")[:110].replace("\n", " ")
            results.append(f"  {label:28} HTTP {r.status_code}  {body}")
        except Exception as exc:
            results.append(f"  {label:28} request failed: {exc}")
    return results


class JQuantsAuthError(RuntimeError):
    """The key was rejected. Distinct from a per-company failure because it
    will fail identically for every remaining company — the first live run
    spent twelve minutes collecting 400 copies of "The incoming api key is
    invalid or expired" and reported them as routine retryable failures."""


# ── J-Quants ─────────────────────────────────────────────────────────────

def fetch_jq_summary(api_key: str, code: str) -> list:
    """Raw /fins/summary records for one company, oldest first."""
    import requests
    out, page_key = [], None
    while True:
        params = {"code": code}
        if page_key:
            params["pagination_key"] = page_key
        r = requests.get(f"{_JQ_BASE}/fins/summary",
                         headers={"x-api-key": api_key}, params=params, timeout=_TIMEOUT)
        if r.status_code in (401, 403):
            raise JQuantsAuthError(f"HTTP {r.status_code}: {r.text[:160]}")
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:120]}")
        payload = r.json()
        batch = payload.get("data", [])
        out.extend(batch)
        page_key = payload.get("pagination_key")
        if not page_key or not batch:
            break
    out.sort(key=lambda r: F.jq_pick_str(r, "disc_date")[0])
    return out


def fy_label(fy_end: str) -> str:
    """"2027-03-31" -> "FY2027". Japanese fiscal years are named for the year
    they end in, which is what every broker note and the company's own
    presentation uses."""
    if not fy_end or len(str(fy_end)) < 4:
        return ""
    return f"FY{str(fy_end)[:4]}"


def next_fy_label(label: str) -> str:
    if not label.startswith("FY"):
        return ""
    try:
        return f"FY{int(label[2:]) + 1}"
    except ValueError:
        return ""


def company_guidance(records: list) -> tuple:
    """({(metric, fy_label, "company"): value}, fundamentals_dict, as_of, fy1_label).

    Reads only the newest record — guidance is restated in full at every
    quarter, so older ones are superseded rather than additive."""
    if not records:
        return {}, {}, "", ""
    latest = records[-1]
    as_of = F.jq_pick_str(latest, "disc_date")[0][:10]
    fy1 = fy_label(F.jq_pick_str(latest, "fy_end")[0])
    fy2 = next_fy_label(fy1)

    # Read newest-first across every filing for the *same* fiscal year, taking
    # the first non-empty value per concept, rather than only the newest record.
    # J-Quants filing types carry different subsets — a dividend revision
    # restates only the dividend fields and leaves the rest blank — so reading
    # one record wholesale silently discards guidance that is present a filing
    # or two back. Restricting to the same CurFYEn is what stops it reaching
    # back far enough to pick up last year's forecast and label it as this
    # year's.
    same_fy = [r for r in reversed(records)
               if fy_label(F.jq_pick_str(r, "fy_end")[0]) == fy1] or [latest]

    def newest(concept):
        for rec in same_fy:
            val, key = F.jq_pick(rec, concept)
            if val is not None:
                return val, key
        return None, ""

    out = {}
    for metric in F.METRICS:
        cur, _ = newest(f"f_{metric}")
        if cur is not None and fy1:
            out[(metric, fy1, "company")] = cur
        nxt, _ = newest(f"nx_{metric}")
        # Next-year guidance is only filed alongside full-year results, so this
        # is populated for a few months a year and blank the rest. That is the
        # disclosure calendar, not a gap in the fetch.
        if nxt is not None and fy2:
            out[(metric, fy2, "company")] = nxt

    fund = {}
    # The dates the two forecast years actually close on. NxtFYEn is filed
    # alongside next-year guidance rather than computed, so a company changing
    # its year end is reported, not smoothed over.
    for concept, field in (("fy_end", "fy_end"), ("nx_fy_end", "fy_end_next")):
        val = F.jq_pick_str(latest, concept)[0]
        if val:
            fund[field] = val[:10]
    for concept in ("equity", "total_assets", "bps", "cash", "debt", "dep_amort"):
        val, key = newest(concept)
        if val is not None:
            fund[concept] = val
            fund.setdefault("_sources", {})[concept] = f"jquants:{key}"
    op, _ = newest("operating_profit")
    if op is not None:
        fund["op_actual"] = op
    return out, fund, as_of, fy1


# ── Yahoo ────────────────────────────────────────────────────────────────

def _yahoo_symbol(code: str) -> str:
    """TSE codes map to "<code>.T". Alphanumeric codes (417A) take the same
    form — they are four characters like any other, and stripping non-digits
    would silently mangle them."""
    return f"{code}.T"


def _frame_value(frame, row_name: str, col):
    try:
        if frame is None or getattr(frame, "empty", True):
            return None
        if row_name not in frame.index:
            return None
        return F.to_num(frame.loc[row_name, col])
    except Exception:
        return None


def fetch_yahoo(code: str, fy1: str, fy2: str) -> tuple:
    """({(metric, fy, "consensus"): value}, fundamentals_dict, as_of).

    Yahoo indexes estimates as relative periods ("0y", "+1y") with no fiscal
    year attached, so they are labelled with the company's own FY labels taken
    from J-Quants. If those are unknown the estimates are dropped rather than
    guessed — mislabelling Yahoo's +1y as the company's FY1 would silently
    compare two different years in the same table row."""
    import yfinance as yf

    t = yf.Ticker(_yahoo_symbol(code))
    as_of = date.today().isoformat()
    cons, fund = {}, {}

    try:
        info = t.info or {}
    except Exception:
        info = {}

    # Without a J-Quants key there is no filed fiscal-year end to label the
    # estimates with, so fall back to Yahoo's own. "0y" is the fiscal year
    # currently running, which is the one ending at nextFiscalYearEnd.
    if not fy1:
        epoch = F.to_num(info.get("nextFiscalYearEnd"))
        if epoch:
            try:
                year = datetime.fromtimestamp(epoch, tz=timezone.utc).year
            except (OverflowError, OSError, ValueError):
                year = 0
            # Yahoo sometimes carries a stale or zeroed fiscal-year end, which
            # labelled live estimates "FY2016". A "next" year-end that has
            # already passed is not a next year-end.
            if date.today().year <= year <= date.today().year + 2:
                fy1 = f"FY{year}"
                fy2 = next_fy_label(fy1)

    slot = {"0y": fy1, "+1y": fy2}
    if fy1 or fy2:
        for frame_name, metric, scale in (("earnings_estimate", "eps", 1.0),
                                          ("revenue_estimate", "net_sales", 1.0)):
            try:
                frame = getattr(t, frame_name)
            except Exception:
                continue
            if frame is None or getattr(frame, "empty", True):
                continue
            for period, label in slot.items():
                if not label or period not in frame.index:
                    continue
                val = F.to_num(frame.loc[period].get("avg"))
                if val is not None:
                    cons[(metric, label, "consensus")] = val * scale
                n = F.to_num(frame.loc[period].get("numberOfAnalysts"))
                if n is not None:
                    fund.setdefault("_analysts", {})[label] = int(n)

    for concept, keys in (("bps", ("bookValue",)),
                          ("debt", ("totalDebt",)),
                          ("cash", ("totalCash",)),
                          ("ebitda", ("ebitda",)),
                          ("shares", ("sharesOutstanding",))):
        for k in keys:
            val = F.to_num(info.get(k))
            if val is not None:
                fund[concept] = val
                fund.setdefault("_sources", {})[concept] = f"yfinance:{k}"
                break

    # .info's bookValue is per-share; the balance sheet gives the absolute
    # figures, which are the ones EV needs and which .info sometimes omits.
    try:
        bs = t.quarterly_balance_sheet
        col = bs.columns[0] if bs is not None and not bs.empty else None
    except Exception:
        bs, col = None, None
    if col is not None:
        for concept, row in (("debt", "Total Debt"),
                             ("cash", "Cash And Cash Equivalents"),
                             ("equity", "Stockholders Equity"),
                             ("total_assets", "Total Assets")):
            if concept in fund:
                continue
            val = _frame_value(bs, row, col)
            if val is not None:
                fund[concept] = val
                fund.setdefault("_sources", {})[concept] = f"yfinance:bs:{row}"
    return cons, fund, as_of


# ── Assembly ─────────────────────────────────────────────────────────────

def build_rows(code: str, name: str, guide: dict, cons: dict,
               jq_as_of: str, y_as_of: str) -> list:
    rows = []
    for (metric, fy, basis), value in list(guide.items()) + list(cons.items()):
        rows.append({
            "code": code, "name": name, "metric": metric, "fy": fy, "basis": basis,
            "value": value,
            "unit": "jpy" if metric in ("eps", "dps") else "jpy_abs",
            "source": "jquants" if basis == "company" else "yfinance",
            "as_of": jq_as_of if basis == "company" else y_as_of,
        })
    return rows


def merge_fundamentals(code: str, name: str, jq: dict, yh: dict, as_of: str) -> dict:
    """J-Quants wins per field — it is the filed number. Yahoo fills the legs
    J-Quants does not carry."""
    out = {"code": code, "name": name, "as_of": as_of}
    for field in ("fy_end", "fy_end_next"):
        if jq.get(field):
            out[field] = jq[field]
    srcs = {}
    for concept in ("shares", "bps", "equity", "total_assets", "debt",
                    "cash", "ebitda", "dep_amort", "op_actual"):
        for src in (jq, yh):
            if concept in src and src[concept] is not None:
                out[concept] = src[concept]
                got = (src.get("_sources") or {}).get(concept)
                if got:
                    srcs[concept] = got
                break

    # EBITDA is not a Japanese reporting line. Prefer building it from filed
    # operating profit plus depreciation; fall back to Yahoo's own figure,
    # and record which, because the two are not the same construction.
    if out.get("op_actual") is not None and out.get("dep_amort") is not None:
        out["ebitda"] = out["op_actual"] + out["dep_amort"]
        out["ebitda_basis"] = "op+dep (jquants)"
    elif out.get("ebitda") is not None:
        out["ebitda_basis"] = "yfinance"
    else:
        out["ebitda_basis"] = ""
    out["sources"] = ";".join(f"{k}={v}" for k, v in sorted(srcs.items()))
    return out


def coverage_report(cons_rows: list, fund_rows: list, universe_n: int) -> str:
    codes = {r["code"] for r in cons_rows}
    def n_with(basis, fy_index):
        seen = {}
        for r in cons_rows:
            if r["basis"] == basis and r["metric"] == "eps":
                seen.setdefault(r["code"], set()).add(r["fy"])
        return sum(1 for fys in seen.values() if len(fys) > fy_index)
    have = lambda field: sum(1 for r in fund_rows if r.get(field) not in (None, ""))
    return "\n".join([
        "",
        "── Coverage " + "─" * 48,
        f"  universe                     {universe_n}",
        f"  any data                     {len(codes)}",
        f"  company EPS guidance (FY1)   {n_with('company', 0)}",
        f"  company EPS guidance (FY2)   {n_with('company', 1)}",
        f"  consensus EPS (FY1)          {n_with('consensus', 0)}",
        f"  consensus EPS (FY1+FY2)      {n_with('consensus', 1)}",
        f"  book value (P/B)             {have('bps')}",
        f"  debt (EV)                    {have('debt')}",
        f"  cash (EV)                    {have('cash')}",
        f"  EBITDA (EV/EBITDA)           {have('ebitda')}",
        f"  fiscal year end filed        {have('fy_end')}",
        "",
        "  fiscal year ends " + (_fy_end_spread(fund_rows) or "(none filed)"),
        "─" * 60,
    ])


def _fy_end_spread(fund_rows: list) -> str:
    """"Mar 371 · Dec 18 · Feb 6" — how the universe's year ends are spread.

    Worth printing: the panel labels a column FY2027, and what that column
    covers depends entirely on this. A month appearing here that the app has
    not been told about is a column of forecasts silently off by a quarter."""
    counts = {}
    for row in fund_rows:
        month = F.fy_end_month(row.get("fy_end"))
        if month:
            counts[month] = counts.get(month, 0) + 1
    return " · ".join(f"{F._MONTH_ABBR[m]} {n}"
                      for m, n in sorted(counts.items(), key=lambda kv: -kv[1]))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--universe", default=F.UNIVERSE_PATH)
    ap.add_argument("--out-consensus", default=F.CONSENSUS_PATH)
    ap.add_argument("--out-fundamentals", default=F.FUNDAMENTALS_PATH)
    ap.add_argument("--out-manifest", default=F.RUN_MANIFEST_PATH,
                    help="where the run manifest is written; overridable so a "
                         "test run cannot write into the repo's data directory")
    ap.add_argument("--limit", type=int, default=0, help="first N companies only")
    ap.add_argument("--only", default="", help="comma-separated codes")
    ap.add_argument("--probe", default="", help="print J-Quants' matched field names for one code and exit")
    ap.add_argument("--dry-run", action="store_true", help="fetch and report, write nothing")
    ap.add_argument("--sleep", type=float, default=0.6, help="delay between companies")
    args = ap.parse_args()

    # GitHub stores secrets verbatim, so a newline picked up while copying
    # travels into the auth header and reads as an invalid key.
    api_key = os.environ.get("JQUANTS_API_KEY", "").strip()

    if args.probe:
        if not api_key:
            print("ERROR: --probe needs JQUANTS_API_KEY.", file=sys.stderr)
            return 1
        try:
            records = fetch_jq_summary(api_key, args.probe)
        except JQuantsAuthError as exc:
            print(f"::error title=J-Quants key rejected::{exc}")
            print("\nThe key was rejected. What that usually means:\n")
            for note in diagnose_key(os.environ.get("JQUANTS_API_KEY", "")):
                print(f"  - {note}")
            print("\nSame request, each auth style:")
            for line in probe_auth(api_key, args.probe):
                print(line)
            print("\nGet a V2 API key from the dashboard at https://jpx-jquants.com "
                  "(not EDINET — different service), and set it as the JQUANTS_API_KEY "
                  "repo secret under Settings -> Secrets and variables -> Actions.")
            return 1
        if not records:
            print(f"No /fins/summary records for {args.probe}.")
            return 1
        latest = records[-1]
        print(f"{len(records)} record(s); newest has {len(latest)} keys.\n")
        # Key names alone were not enough: the first probe showed every
        # current-year forecast unmatched, which reads as "wrong alias" but can
        # equally mean "right alias, empty on this record". Print the values.
        print("Newest record identity:")
        for k in ("Code", "DocType", "CurPerType", "CurFYSt", "CurFYEn",
                  "NxtFYSt", "NxtFYEn", "DiscDate"):
            if k in latest:
                print(f"  {k:12} {latest[k]!r}")
        print("\nForecast fields on the newest record (blank = present but empty):")
        for k in sorted(k for k in latest if k.startswith(("F", "Nx"))):
            print(f"  {k:18} {str(latest[k])[:28]!r}")
        print("\nSame fields across all records, newest first "
              "(which filing actually carries the guidance):")
        for rec in reversed(records):
            filled = [k for k in ("FSales", "FOP", "FNP", "FEPS") if str(rec.get(k, "")).strip()]
            print(f"  {F.jq_pick_str(rec, 'disc_date')[0][:10]}  "
                  f"{str(rec.get('CurPerType', '?')):4} {str(rec.get('DocType', ''))[:34]:34} "
                  f"FY-forecast fields set: {filled or 'none'}")

        # Dividends and interim guidance are filed under their own families of
        # keys — per quarter, per half, consolidated and non-consolidated — and
        # an issuer that files only the quarterly split has no annual total for
        # an "annual DPS" alias to match. Print the family whole.
        for title, needles in (("Dividend", ("div", "haitou")),
                               ("Interim / quarterly", ("2q", "1q", "3q", "half",
                                                        "interim", "cumulative"))):
            hits = sorted(k for k in latest
                          if any(n in k.lower() for n in needles))
            print(f"\n{title} fields on the newest record:")
            for k in hits:
                print(f"  {k:22} {str(latest[k])[:28]!r}")
            if not hits:
                print("  (none)")

        print("\nMatched concept -> key:")
        for concept, key in sorted(F.jq_field_report(latest).items()):
            print(f"  {concept:22} {key or '·· NO MATCH — add the real key to _JQ_ALIASES'}")
        unmatched = set(latest) - {k for c in F._JQ_ALIASES.values() for k in c}
        if unmatched:
            print(f"\nResponse keys not in any alias list:\n  {sorted(unmatched)}")
        return 0

    universe = F.read_universe(args.universe)
    if not universe:
        print(f"ERROR: {args.universe} is missing or empty. "
              f"Run build_jpx400.py first.", file=sys.stderr)
        return 1
    if args.only:
        wanted = {c.strip() for c in args.only.split(",") if c.strip()}
        universe = [u for u in universe if u["Code"] in wanted]
    if args.limit:
        universe = universe[:args.limit]

    if not api_key:
        # ::error:: renders as a red annotation on the workflow summary page.
        # The previous plain warning scrolled past in a 400-line log, and the
        # committed CSVs gave no hint why every company column was empty —
        # a missing key and a total API outage looked identical afterwards.
        print("::error title=JQUANTS_API_KEY missing::Company guidance will be "
              "absent from every company. Add JQUANTS_API_KEY under Settings -> "
              "Secrets and variables -> Actions, then re-run. Consensus and "
              "Yahoo balance-sheet data are still collected.")

    cons_rows, fund_rows, failures = [], [], []
    auth_failed = False
    for i, row in enumerate(universe, 1):
        code, name = row["Code"], row.get("Name", "")
        guide, jq_fund, jq_as_of, fy1 = {}, {}, "", ""
        if api_key:
            try:
                guide, jq_fund, jq_as_of, fy1 = company_guidance(fetch_jq_summary(api_key, code))
            except JQuantsAuthError as exc:
                # Stop asking. Every remaining company would fail the same way,
                # and burying one configuration error under 400 identical
                # retryable-looking lines is how the first run hid it.
                print(f"::error title=J-Quants key rejected::{exc} — company guidance will be "
                      f"absent for all {len(universe)} companies. Refresh JQUANTS_API_KEY under "
                      f"Settings -> Secrets and variables -> Actions and re-run. Consensus and "
                      f"Yahoo balance-sheet data are unaffected.")
                auth_failed = True
                api_key = ""
                failures.append((code, f"jquants auth: {exc}"))
            except Exception as exc:
                failures.append((code, f"jquants: {exc}"))
        fy2 = next_fy_label(fy1)
        try:
            cons, y_fund, y_as_of = fetch_yahoo(code, fy1, fy2)
        except Exception as exc:
            failures.append((code, f"yahoo: {exc}"))
            cons, y_fund, y_as_of = {}, {}, date.today().isoformat()

        cons_rows.extend(build_rows(code, name, guide, cons, jq_as_of, y_as_of))
        fund_rows.append(merge_fundamentals(code, name, jq_fund, y_fund,
                                            jq_as_of or y_as_of))
        print(f"  [{i}/{len(universe)}] {code} {name[:36]:36} "
              f"guide={len(guide):2} cons={len(cons):2} fy1={fy1 or '?'}")
        if i < len(universe):
            time.sleep(args.sleep)

    print(coverage_report(cons_rows, fund_rows, len(universe)))

    # A manifest of what this run could and could not reach. Without it the
    # committed CSVs cannot distinguish "the key was missing" from "J-Quants
    # was down" from "these companies genuinely have not guided" — all three
    # render as the same empty column, and the panel had no way to say which.
    manifest = {
        "ran_at": date.today().isoformat(),
        "universe": len(universe),
        "attempted": len(universe),
        "jquants_key_present": bool(os.environ.get("JQUANTS_API_KEY", "")),
        "jquants_key_rejected": auth_failed,
        "guidance_rows": sum(1 for r in cons_rows if r["basis"] == "company"),
        "consensus_rows": sum(1 for r in cons_rows if r["basis"] == "consensus"),
        "companies_with_guidance": len({r["code"] for r in cons_rows if r["basis"] == "company"}),
        "companies_with_consensus": len({r["code"] for r in cons_rows if r["basis"] == "consensus"}),
        "jquants_failures": sum(1 for _, e in failures if e.startswith("jquants")),
        "yahoo_failures": sum(1 for _, e in failures if e.startswith("yahoo")),
        "sample_errors": [f"{c}: {e[:120]}" for c, e in failures[:5]],
    }
    if failures:
        print(f"\n{len(failures)} failure(s) — skipped, will retry next run:")
        for code, err in failures[:15]:
            print(f"  {code}: {err}")
        if len(failures) > 15:
            print(f"  … and {len(failures) - 15} more")

    if args.dry_run:
        print("\n--dry-run: nothing written.")
        return 0

    merged = F.merge_consensus(F.read_rows(args.out_consensus, F.CONSENSUS_COLUMNS), cons_rows)
    F.write_rows(args.out_consensus, F.CONSENSUS_COLUMNS, merged)

    by_code = {r["code"]: r for r in F.read_rows(args.out_fundamentals, F.FUNDAMENTALS_COLUMNS)}
    for r in fund_rows:
        by_code[r["code"]] = r
    F.write_rows(args.out_fundamentals, F.FUNDAMENTALS_COLUMNS,
                 sorted(by_code.values(), key=lambda r: r["code"]))

    import json
    os.makedirs(os.path.dirname(args.out_manifest) or ".", exist_ok=True)
    with open(args.out_manifest, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2, sort_keys=True)

    print(f"\nWrote {len(merged)} consensus rows -> {args.out_consensus}")
    print(f"Wrote {len(by_code)} fundamentals rows -> {args.out_fundamentals}")
    print(f"Wrote run manifest -> {args.out_manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
