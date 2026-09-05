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


def forecast_horizon(latest: dict) -> tuple:
    """(fy_label, fy_end_date, key_prefix) for the fiscal year the company's
    live guidance actually covers, plus the field family it is filed under.

    This is not the same as the year the filing reports on, and conflating the
    two put a whole column out by a year. A Japanese full-year tanshin reports
    the year that has just closed — CurFYEn is that closed year — and carries
    guidance for the year now starting, under the NextYearForecast (NxF*)
    family. A quarterly tanshin reports the year in progress and carries
    guidance for that same year, under Forecast (F*).

    Reading CurFYEn as the guidance year therefore labelled Fuji Electric's
    live forecast FY2026 when it covers the year to March 2027, and Yahoo's
    "current year" estimate — which is that same year to March 2027 — was
    filed against the label one year earlier. The company and consensus halves
    of a row were describing different years."""
    period = F.jq_pick_str(latest, "period_type")[0].upper()
    cur_end = F.jq_pick_str(latest, "fy_end")[0][:10]
    nxt_end = F.jq_pick_str(latest, "nx_fy_end")[0][:10]
    if period.startswith("FY"):
        end = nxt_end or _plus_one_year(cur_end)
        return fy_label(end), end, "nx_"
    return fy_label(cur_end), cur_end, "f_"


def _plus_one_year(iso: str) -> str:
    """Fallback only, for a full-year filing that carries no NxtFYEn. Keeps the
    month and day, so a February or December close is not quietly moved."""
    if not iso or len(iso) < 10:
        return ""
    try:
        return f"{int(iso[:4]) + 1}{iso[4:]}"
    except ValueError:
        return ""


# How many completed fiscal years of actuals to keep. Three, because a progress
# rate is only readable against the same company's own history: 24% of guidance
# at Q1 is not "behind" for a company that earns its profit in the second half,
# and the only way to know which is to have last year's Q1 to compare it to.
HISTORY_YEARS = 3
# Collected per fiscal year alongside F.METRICS, but kept separate from it:
# these are counts, not money, so they must not pick up the ¥bn scaling or the
# per-share formatting that every consumer of F.METRICS applies.
SHARE_METRICS = ("shares", "treasury")

# The quarterly period types a tanshin is filed under. 4Q is deliberately absent
# — a 4Q filing is the full year, and it is picked up as an actual.
_YTD_PERIODS = ("1Q", "2Q", "3Q")


def _newest_reader(records: list):
    """A (value, matched_key) reader over `records`, newest first.

    Factored out because the same across-filings read is wanted in four places:
    a full-year tanshin and the amended one filed a week later carry different
    subsets, so reading one record wholesale silently drops fields."""
    def newest(concept):
        for rec in records:
            val, key = F.jq_pick(rec, concept)
            if val is not None:
                return val, key
        return None, ""
    return newest


def _fy_ends_newest_first(records: list, limit: int) -> list:
    """The distinct fiscal-year ends in `records`, newest first, capped."""
    ends = []
    for rec in reversed(records):
        end = F.jq_pick_str(rec, "fy_end")[0][:10]
        if end and end not in ends:
            ends.append(end)
    return ends[:limit]


def company_actuals(records: list, years: int = HISTORY_YEARS) -> tuple:
    """({(metric, fy_label, "actual"): value}, newest_fy_label, as_of, nc) for
    the last `years` *completed* fiscal years.

    Taken only from full-year filings. A quarterly tanshin also carries
    Sales/OP/NP, but those are the year to date — three months of it, in a Q1 —
    and putting them in a column headed by a fiscal year would read as a full
    year's trading. Those are collected separately by company_ytd(), which
    labels them as the year-to-date figures they are.

    More than one year is kept because a prior year's full-year actual is the
    denominator of that year's progress rate; without it the seasonality
    comparison has nothing to divide by."""
    fy_records = [r for r in records
                  if F.jq_pick_str(r, "period_type")[0].upper().startswith("FY")]
    if not fy_records:
        return {}, "", "", set()
    ends = _fy_ends_newest_first(fy_records, years)
    if not ends or not fy_label(ends[0]):
        return {}, "", "", set()

    out, nc = {}, set()
    for end in ends:
        label = fy_label(end)
        if not label:
            continue
        newest = _newest_reader([r for r in reversed(fy_records)
                                 if F.jq_pick_str(r, "fy_end")[0][:10] == end])
        for metric in F.METRICS:
            if metric == "dps":
                val, key = F.dps_annual(newest, ""), ""
            else:
                val, key = newest(metric)
            if val is not None:
                out[(metric, label, "actual")] = val
                if key in F.NONCONSOLIDATED_KEYS:
                    nc.add((metric, label, "actual"))
        # Share count and treasury stock per year, which is the only way to see
        # whether a buyback is retiring shares or just parking them. Both
        # aliases were already in _JQ_ALIASES and neither was ever read -- the
        # same gap `cfo` had. They ride in the existing long format rather than
        # a new file, so nothing new is loaded at run time.
        for metric in SHARE_METRICS:
            val, key = newest(metric)
            if val is not None:
                out[(metric, label, "actual")] = val
                if key in F.NONCONSOLIDATED_KEYS:
                    nc.add((metric, label, "actual"))

    latest = fy_records[-1]
    return out, fy_label(ends[0]), F.jq_pick_str(latest, "disc_date")[0][:10], nc


def company_ytd(records: list, years: int = HISTORY_YEARS) -> tuple:
    """({(metric, fy_label, "ytd_1q"|"ytd_2q"|"ytd_3q"): value}, nc).

    The cumulative year-to-date figures every quarterly tanshin carries and the
    collector previously threw away — they were filtered out one line into
    company_actuals and never looked at again, even though they arrive in the
    same /fins/summary response and cost nothing extra to fetch.

    They are what makes 進捗率 computable: year to date over the full-year
    guidance, which is the first number the market quotes off a Japanese
    quarterly result.

    Note the fiscal-year labelling differs from a full-year filing. On a
    quarterly tanshin CurFYEn is the year *in progress*, so its label is the
    year the figures belong to; on a full-year tanshin CurFYEn is the year just
    closed. Conflating the two is what put a whole column out by a year in
    forecast_horizon(), and the same trap applies here."""
    q_records = [r for r in records
                 if F.jq_pick_str(r, "period_type")[0].upper() in _YTD_PERIODS]
    if not q_records:
        return {}, set()

    out, nc = {}, set()
    for end in _fy_ends_newest_first(q_records, years):
        label = fy_label(end)
        if not label:
            continue
        for period in _YTD_PERIODS:
            same = [r for r in reversed(q_records)
                    if F.jq_pick_str(r, "fy_end")[0][:10] == end
                    and F.jq_pick_str(r, "period_type")[0].upper() == period]
            if not same:
                continue
            newest = _newest_reader(same)
            basis = f"ytd_{period.lower()}"
            for metric in F.METRICS:
                # No year-to-date dividend: an annual DPS is a rate for the
                # year, not a flow that accumulates quarter by quarter, which
                # is the same reason there is no implied-2H dividend.
                if metric == "dps":
                    continue
                val, key = newest(metric)
                if val is not None:
                    out[(metric, label, basis)] = val
                    if key in F.NONCONSOLIDATED_KEYS:
                        nc.add((metric, label, basis))
    return out, nc


def company_guidance(records: list) -> tuple:
    """({(metric, fy_label, "company"): value}, fundamentals_dict, as_of, fy1_label).

    Companies guide one year at a time, so there is one forecast column here,
    aligned to forecast_horizon() above. The second column the panel shows is
    the street's, not the company's."""
    if not records:
        return {}, {}, "", ""
    latest = records[-1]
    as_of = F.jq_pick_str(latest, "disc_date")[0][:10]
    cur_end = F.jq_pick_str(latest, "fy_end")[0][:10]
    fy1, fy1_end, prefix = forecast_horizon(latest)

    # Read newest-first across every filing for the *same* reported fiscal year,
    # taking the first non-empty value per concept, rather than only the newest
    # record. J-Quants filing types carry different subsets — a dividend
    # revision restates only the dividend fields and leaves the rest blank — so
    # reading one record wholesale silently discards guidance that is present a
    # filing or two back. Restricting to that one CurFYEn is what stops it
    # reaching back far enough to pick up a superseded year's forecast.
    same_fy = [r for r in reversed(records)
               if F.jq_pick_str(r, "fy_end")[0][:10] == cur_end] or [latest]

    def newest(concept):
        for rec in same_fy:
            val, key = F.jq_pick(rec, concept)
            if val is not None:
                return val, key
        return None, ""

    # A full-year tanshin whose next-year family is empty is a company that has
    # not guided yet — 未定 is a normal thing to file. Its Forecast (F*) fields,
    # if any, describe the year that has just closed, so showing them as a
    # forecast would present last year's superseded number as this year's plan.
    # fy1 still stands: it is the year now running, which is what the street's
    # "current year" estimate covers whether the company has guided or not.
    if prefix == "nx_" and not any(newest(f"nx_{m}")[0] is not None for m in F.METRICS):
        fy1_has_guidance = False
    else:
        fy1_has_guidance = True

    out, nc = {}, set()

    def keep(metric, fy, basis, val, key):
        if val is None:
            return
        out[(metric, fy, basis)] = val
        if key in F.NONCONSOLIDATED_KEYS:
            nc.add((metric, fy, basis))

    if fy1 and fy1_has_guidance:
        for metric in F.METRICS:
            if metric == "dps":
                # No annual total is filed for a next year, only the four
                # instalments — see fundamentals.dps_annual.
                val, key = F.dps_annual(newest, prefix), ""
            else:
                val, key = newest(f"{prefix}{metric}")
            keep(metric, fy1, "company", val, key)

        # Interim guidance, where the company files it. Many do: the first-half
        # forecast is its own row in the tanshin, and it is the number a
        # mid-year result gets judged against.
        interim_prefix = "nx2q_" if prefix == "nx_" else "f2q_"
        for metric in F.INTERIM_METRICS:
            val, key = newest(f"{interim_prefix}{metric}")
            keep(metric, fy1, "company_h1", val, key)

    # A quarterly filing occasionally carries next-year fields too. Where it
    # does, that is a genuine second company year and worth keeping.
    if prefix == "f_" and fy1:
        fy2 = next_fy_label(fy1)
        for metric in F.METRICS:
            if metric == "dps":
                val, key = F.dps_annual(newest, "nx_"), ""
            else:
                val, key = newest(f"nx_{metric}")
            if fy2:
                keep(metric, fy2, "company", val, key)

    fund = {}
    # The date the guidance year closes, filed rather than derived, so a
    # December or February close — or a company changing its year end — is
    # reported rather than smoothed over.
    if fy1_end:
        fund["fy_end"] = fy1_end
    nxt = F.jq_pick_str(latest, "nx_fy_end")[0][:10]
    if prefix == "nx_":
        # The next-year end is already the guidance year; the one after it is
        # not filed anywhere, so the panel derives it only for a column the
        # street (or a screenshot) fills.
        fund["fy_end_next"] = _plus_one_year(fy1_end)
    elif nxt:
        fund["fy_end_next"] = nxt

    for concept in ("equity", "total_assets", "bps", "cash", "debt", "dep_amort",
                    "shares", "cfo", "equity_to_assets"):
        val, key = newest(concept)
        if val is not None:
            fund[concept] = val
            fund.setdefault("_sources", {})[concept] = f"jquants:{key}"
    op, _ = newest("operating_profit")
    if op is not None:
        fund["op_actual"] = op

    # Which accounting standard the newest filing is drawn up under. Taken from
    # `latest` rather than across filings: it is the *current* basis that says
    # how to label the income statement, and a company mid-IFRS-transition is
    # precisely the case where an older filing would give the wrong answer.
    doc_type = F.jq_pick_str(latest, "doc_type")[0]
    if doc_type:
        fund["doc_type"] = doc_type
    return out, fund, as_of, fy1, nc


def guidance_history(records: list, code: str, name: str,
                     years: int = HISTORY_YEARS) -> list:
    """Rows for data/guidance_history.csv: how each guidance figure has moved
    since it was first filed.

    Built from the same /fins/summary response the rest of the collection uses,
    so it costs no extra call. Japanese issuers have a reputation for guiding
    conservatively and revising up through the year; whether a particular
    management actually does that is a fact sitting in its own filing history,
    and this is what puts a number on it.

    A *summary* rather than every filed value — first, latest and a count. The
    full sequence stays re-derivable from the API, which returns a company's
    whole history, so storing it would multiply the file for nothing.

    Guidance for a fiscal year arrives under two different key families: the
    NxF* set on the full-year tanshin that first announces it, and the F* set on
    every quarterly filing afterwards. Both describe the same year, so both are
    read into the same series — reading only one would show a year's guidance as
    never having been revised."""
    series = {}
    for rec in records:
        as_of = F.jq_pick_str(rec, "disc_date")[0][:10]
        if not as_of:
            continue
        period = F.jq_pick_str(rec, "period_type")[0].upper()
        cur_end = F.jq_pick_str(rec, "fy_end")[0][:10]
        nxt_end = F.jq_pick_str(rec, "nx_fy_end")[0][:10]
        # (the fiscal year this filing guides, the key prefix it guides under)
        horizons = []
        if period.startswith("FY"):
            horizons.append((fy_label(nxt_end or _plus_one_year(cur_end)), "nx_"))
        else:
            horizons.append((fy_label(cur_end), "f_"))
            if nxt_end:
                horizons.append((fy_label(nxt_end), "nx_"))
        for fy, prefix in horizons:
            if not fy:
                continue
            for metric in F.METRICS:
                if metric == "dps":
                    val = F.dps_annual(_newest_reader([rec]), prefix)
                else:
                    val, _ = F.jq_pick(rec, f"{prefix}{metric}")
                if val is None:
                    continue
                series.setdefault((fy, metric), []).append((as_of, val))

    keep = sorted({fy for fy, _ in series}, reverse=True)[:years]
    rows = []
    for (fy, metric), points in sorted(series.items()):
        if fy not in keep:
            continue
        points.sort(key=lambda p: p[0])
        first_at, first_val = points[0]
        last_at, last_val = points[-1]
        # A revision is a *changed* value, not another filing repeating the same
        # number — most quarterly filings restate unchanged guidance verbatim.
        revisions = sum(1 for i in range(1, len(points))
                        if points[i][1] != points[i - 1][1])
        rows.append({"code": code, "name": name, "fy": fy, "metric": metric,
                     "first_value": first_val, "first_as_of": first_at,
                     "latest_value": last_val, "latest_as_of": last_at,
                     "revisions": revisions})
    return rows


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
               jq_as_of: str, y_as_of: str, nonconsolidated=()) -> list:
    """Long-format rows for consensus.csv.

    A value that came from a parent-only J-Quants key is sourced
    "jquants:nonconsolidated" rather than plain "jquants", so the panel can
    mark it. Everything else keeps the source string it always had, which is
    what stops this from being a schema change."""
    nc = set(nonconsolidated or ())
    rows = []
    for (metric, fy, basis), value in list(guide.items()) + list(cons.items()):
        filed = (basis.startswith("company") or basis == "actual"
                 or basis.startswith("ytd_"))
        source = "yfinance"
        if filed:
            source = ("jquants:nonconsolidated" if (metric, fy, basis) in nc
                      else "jquants")
        rows.append({
            "code": code, "name": name, "metric": metric, "fy": fy, "basis": basis,
            "value": value,
            # Share counts are counts. Tagging them jpy_abs would be a lie the
            # next consumer of this file has no way to catch.
            "unit": ("count" if metric in SHARE_METRICS
                     else "jpy" if metric in ("eps", "dps") else "jpy_abs"),
            "source": source,
            "as_of": jq_as_of if filed else y_as_of,
        })
    return rows


def merge_fundamentals(code: str, name: str, jq: dict, yh: dict, as_of: str) -> dict:
    """J-Quants wins per field — it is the filed number. Yahoo fills the legs
    J-Quants does not carry."""
    out = {"code": code, "name": name, "as_of": as_of}
    for field in ("fy_end", "fy_end_next"):
        if jq.get(field):
            out[field] = jq[field]
    if jq.get("doc_type"):
        out["doc_type"] = jq["doc_type"]
    srcs = {}
    for concept in ("shares", "bps", "equity", "total_assets", "debt",
                    "cash", "ebitda", "dep_amort", "op_actual",
                    "equity_to_assets", "cfo"):
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
    n_metric = lambda metric, basis: len({r["code"] for r in cons_rows
                                          if r["metric"] == metric and r["basis"] == basis})
    return "\n".join([
        "",
        "── Coverage " + "─" * 48,
        f"  universe                     {universe_n}",
        f"  any data                     {len(codes)}",
        f"  company EPS guidance (FY1)   {n_with('company', 0)}",
        f"  company EPS guidance (FY2)   {n_with('company', 1)}",
        f"  consensus EPS (FY1)          {n_with('consensus', 0)}",
        f"  consensus EPS (FY1+FY2)      {n_with('consensus', 1)}",
        f"  company DPS guidance         {n_metric('dps', 'company')}",
        f"  company ordinary profit      {n_metric('ordinary_profit', 'company')}",
        f"  company interim (H1)         {n_metric('net_sales', 'company_h1')}",
        f"  prior-year actuals           {n_metric('net_sales', 'actual')}",
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
        # The equity ratio's own scale is not documented, and 5.2 and 0.052 are
        # both plausible encodings of the same 5.2%, so the probe prints the
        # raw value beside the ratio computed from the balance sheet. That is
        # what fundamentals.equity_to_assets checks at collection time; seeing
        # both here is how you confirm it is checking the right thing.
        _eqar, _eqar_key = F.jq_pick(latest, "equity_to_assets")
        _eq, _ = F.jq_pick(latest, "equity")
        _ta, _ = F.jq_pick(latest, "total_assets")
        print(f"\nEquity ratio (ACCOUNTING — not Basel CET1 or a solvency margin):")
        print(f"  filed      {_eqar!r} (key {_eqar_key or 'NO MATCH'})")
        print(f"  equity/TA  {(_eq / _ta) if (_eq and _ta) else None}")
        print(f"  resolved   {F.equity_to_assets(_eqar, _eq, _ta)}")
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

    cons_rows, fund_rows, hist_all, failures = [], [], [], []
    auth_failed = False
    for i, row in enumerate(universe, 1):
        code, name = row["Code"], row.get("Name", "")
        guide, actual, ytd, jq_fund, jq_as_of, fy1 = {}, {}, {}, {}, "", ""
        hist_rows = []
        nonconsolidated = set()
        if api_key:
            try:
                _records = fetch_jq_summary(api_key, code)
                guide, jq_fund, jq_as_of, fy1, _nc_g = company_guidance(_records)
                actual, _fy0, _, _nc_a = company_actuals(_records)
                ytd, _nc_y = company_ytd(_records)
                hist_rows = guidance_history(_records, code, name)
                nonconsolidated = _nc_g | _nc_a | _nc_y
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

        cons_rows.extend(build_rows(code, name, {**actual, **ytd, **guide}, cons,
                                    jq_as_of, y_as_of, nonconsolidated))
        fund_rows.append(merge_fundamentals(code, name, jq_fund, y_fund,
                                            jq_as_of or y_as_of))
        hist_all.extend(hist_rows)
        print(f"  [{i}/{len(universe)}] {code} {name[:36]:36} "
              f"guide={len(guide):2} ytd={len(ytd):2} cons={len(cons):2} "
              f"fy1={fy1 or '?'}")
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

    # Guidance history is keyed on (code, fy, metric) and rebuilt from the
    # company's whole filing history each run, so a fresh row supersedes the
    # stored one; companies not collected this run keep theirs.
    _hist = {(r["code"], r["fy"], r["metric"]): r
             for r in F.read_rows(F.GUIDANCE_HISTORY_PATH, F.GUIDANCE_HISTORY_COLUMNS)}
    for r in hist_all:
        _hist[(r["code"], r["fy"], r["metric"])] = r
    F.write_rows(F.GUIDANCE_HISTORY_PATH, F.GUIDANCE_HISTORY_COLUMNS,
                 sorted(_hist.values(),
                        key=lambda r: (r["code"], r["fy"], r["metric"])))

    import json
    os.makedirs(os.path.dirname(args.out_manifest) or ".", exist_ok=True)
    with open(args.out_manifest, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2, sort_keys=True)

    print(f"\nWrote {len(merged)} consensus rows -> {args.out_consensus}")
    print(f"Wrote {len(by_code)} fundamentals rows -> {args.out_fundamentals}")
    print(f"Wrote {len(_hist)} guidance-history rows -> {F.GUIDANCE_HISTORY_PATH}")
    print(f"Wrote run manifest -> {args.out_manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
