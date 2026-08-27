"""
build_universe.py — write data/universe.csv, the list of companies the
Research tab's forecast panel collects data for.

    python build_universe.py                 # TOPIX 1000 (the default)
    python build_universe.py --scope topix500
    python build_universe.py --dump          # print the raw schema and stop

The TOPIX 1000 is not published as a downloadable list, but it does not need
to be: JPX classifies every listed company into a scale category, and the
categories *define* the indices —

    TOPIX Core30 + Large70                    = TOPIX 100
    ... + Mid400                              = TOPIX 500
    ... + Small 1                             = TOPIX 1000
    ... + Small 2                             = TOPIX (all)

so the constituent list is a filter over J-Quants' listed-issue master
(/v2/equities/master), which this app already has a key for. That is the
exchange's own classification rather than a third party's copy of it, it needs
no scraping, and re-running this picks up the annual review automatically.

Deliberately a separate, hand-run script like build_jpx400.py rather than part
of the weekly collector: the universe changes once a year, and a bad fetch
that silently halved it would quietly halve the app's coverage. It refuses to
write a short list for the same reason.
"""

import argparse
import csv
import os
import sys

import fundamentals as F

_JQ_BASE = "https://api.jquants.com/v2"
_TIMEOUT = 30

# V2 abbreviates its response columns and V1 spells them out; take whichever
# is present. Same approach as fundamentals._JQ_ALIASES, and for the same
# reason — guessing one name and getting an empty column is the failure mode.
_FIELDS = {
    "code":   ("Code", "LocalCode"),
    "name":   ("CoNmEn", "CompanyNameEnglish", "CompanyNameEng", "CoNm", "CompanyName"),
    "name_jp": ("CoNm", "CompanyName"),
    "scale":  ("ScaleCat", "ScaleCategory"),
    "market": ("MktCdNm", "MarketCodeName", "MktCd", "MarketCode"),
    "sector": ("Sec17CdNm", "Sector17CodeName", "Sec33CdNm", "Sector33CodeName"),
}

# Matched as substrings, case-insensitively, because the category strings carry
# a "TOPIX " prefix in some responses and not others, and JPX writes the Small
# tiers with and without the space.
_SCOPES = {
    "topix100":  ("core30", "large70"),
    "topix500":  ("core30", "large70", "mid400"),
    "topix1000": ("core30", "large70", "mid400", "small 1", "small1"),
    "topix":     ("core30", "large70", "mid400", "small 1", "small1",
                  "small 2", "small2"),
}

# Any of these appearing in a value marks the column as the scale category.
_SCALE_MARKERS = ("core30", "large70", "mid400", "small 1", "small1",
                  "small 2", "small2")
_FLOOR = {"topix100": 90, "topix500": 450, "topix1000": 900, "topix": 1500}

COLUMNS = ["Code", "MarketDiv", "Name", "ScaleCategory", "Sector"]


# Last resort when neither the V1 nor the guessed V2 name is present: match the
# key by shape. (wanted, banned) — a key qualifies if it contains any wanted
# fragment and no banned one, so "MktCdNm" cannot be mistaken for a company
# name and "Sec17CdNm" cannot be mistaken for a market.
_FUZZY = {
    "name":     ((("nmen", "nameen"),), ("mkt", "sec", "div", "scale")),
    "name_jp":  ((("nm", "name"),), ("mkt", "sec", "div", "scale", "en")),
    "market":   ((("mkt", "market"),), ("code",)),
    "sector":   ((("sec17", "sector17", "sec33", "sector33", "sec", "sector"),), ()),
}


def _fuzzy_key(record: dict, field: str) -> str:
    groups, banned = _FUZZY.get(field, ((), ()))
    for wanted in groups:
        for key in record:
            low = key.lower()
            if any(w in low for w in wanted) and not any(b in low for b in banned):
                if record.get(key) not in (None, ""):
                    return key
    return ""


def _pick(record: dict, field: str, override: str = "") -> str:
    for key in ((override,) if override else ()) + _FIELDS[field]:
        val = record.get(key)
        if val not in (None, ""):
            return str(val).strip()
    key = _fuzzy_key(record, field)
    return str(record[key]).strip() if key else ""


def detect_scale_key(records: list) -> str:
    """The response column holding the TOPIX scale category, found by its
    *values* rather than its name.

    V2 abbreviates its column names and this app cannot read the spec that
    documents them, so a guessed name is one more thing to get wrong. The
    values need no guessing: "TOPIX Core30" says what it is. Returns "" when
    nothing matches, and the alias list takes over."""
    counts = {}
    for rec in records[:500]:
        for key, val in rec.items():
            if isinstance(val, str) and any(m in val.lower() for m in _SCALE_MARKERS):
                counts[key] = counts.get(key, 0) + 1
    return max(counts, key=counts.get) if counts else ""


def describe_records(records: list, limit: int = 12) -> str:
    """The response's own shape, for a failure message. A run that cannot find
    the scale category should hand back enough to fix it in one go rather than
    sending anyone to read a spec page — so: the keys, and the distinct values
    of every column narrow enough to be a classification."""
    lines = [f"{len(records)} records; keys: {sorted(records[0])}"]
    for key in sorted(records[0]):
        vals = {str(r.get(key, "")) for r in records[:500] if r.get(key) not in (None, "")}
        if 0 < len(vals) <= limit:
            lines.append(f"  {key}: {sorted(vals)[:limit]}")
    return "\n".join(lines)


# V2 renamed V1's /listed/info to /equities/master, alongside the
# /equities/earnings-calendar and /fins/summary paths this app already calls.
# The old name is kept as a fallback rather than removed: it costs one request
# on a path that no longer exists, and it makes the failure legible if the
# naming moves again.
_ENDPOINTS = ("/equities/master", "/listed/info", "/equities/listed")

# J-Quants answers an unrouted path with 403 and this message, not 404. Read as
# an entitlement problem it looks like a key that needs rotating; read as what
# it is, it means try the next path.
_NO_SUCH_ENDPOINT = "does not exist"

# The array's key varies with the endpoint; fall back to whichever key holds a
# list of objects rather than adding a fourth guess every time.
def _records_in(payload: dict) -> list:
    for key in ("info", "data", "listed"):
        val = payload.get(key)
        if isinstance(val, list):
            return val
    for val in payload.values():
        if isinstance(val, list) and val and isinstance(val[0], dict):
            return val
    return []


def fetch_listed(api_key: str) -> list:
    """Every listed company J-Quants knows about, one page at a time."""
    import requests
    tried = []
    for path in _ENDPOINTS:
        out, page_key, ok = [], None, True
        while True:
            params = {"pagination_key": page_key} if page_key else {}
            r = requests.get(f"{_JQ_BASE}{path}", headers={"x-api-key": api_key},
                             params=params, timeout=_TIMEOUT)
            if r.status_code in (401, 403) and _NO_SUCH_ENDPOINT not in r.text:
                # A genuine rejection. The same key works for /fins/summary in
                # the collector, so this is about the endpoint's entitlement,
                # not the key — say so rather than sending anyone to rotate it.
                raise SystemExit(
                    f"::warning title=Universe refresh skipped::J-Quants returned "
                    f"HTTP {r.status_code} for {path}. If the collector's own calls "
                    f"work, this endpoint is not included in the plan. "
                    f"{r.text[:120]}")
            if r.status_code != 200:
                tried.append(f"{path} -> HTTP {r.status_code} {r.text[:80]}")
                ok = False
                break
            payload = r.json()
            batch = _records_in(payload)
            out.extend(batch)
            page_key = payload.get("pagination_key")
            if not page_key or not batch:
                break
        if ok and out:
            if path != _ENDPOINTS[0]:
                print(f"note: the listed-company master answered at {path}")
            return out
        if ok:
            tried.append(f"{path} -> 200 but no records in the response")

    raise SystemExit(
        "::warning title=Universe refresh failed::No listed-company endpoint "
        "answered, so the universe was left as it is and the collection falls "
        "back to it. Tried:\n  " + "\n  ".join(tried))


def in_scope(scale: str, scope: str) -> bool:
    low = (scale or "").lower()
    return any(k in low for k in _SCOPES[scope])


def build(records: list, scope: str, scale_key: str = "") -> list:
    rows, seen = [], set()
    scale_key = scale_key or detect_scale_key(records)
    for rec in records:
        code = _pick(rec, "code")
        # J-Quants pads codes to five characters for some feeds ("65040");
        # every other file in this repo keys on the four-character TSE code.
        if len(code) == 5 and code.endswith("0"):
            code = code[:4]
        scale = _pick(rec, "scale", scale_key)
        if not code or code in seen or not in_scope(scale, scope):
            continue
        seen.add(code)
        rows.append({
            "Code": code,
            "MarketDiv": _pick(rec, "market"),
            "Name": _pick(rec, "name") or _pick(rec, "name_jp"),
            "ScaleCategory": scale,
            "Sector": _pick(rec, "sector"),
        })
    rows.sort(key=lambda r: r["Code"])
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="topix1000", choices=sorted(_SCOPES))
    ap.add_argument("-o", "--out", default=F.UNIVERSE_PATH)
    ap.add_argument("--expect", type=int, default=0,
                    help="minimum constituent count to accept (0 uses the scope's own floor)")
    ap.add_argument("--dump", action="store_true",
                    help="print the response schema and the scale-category spread, then stop")
    args = ap.parse_args()

    api_key = (os.environ.get("JQUANTS_API_KEY") or "").strip()
    if not api_key:
        print("ERROR: JQUANTS_API_KEY is not set.", file=sys.stderr)
        return 1

    records = fetch_listed(api_key)
    if not records:
        print("ERROR: the listed-company master returned nothing.", file=sys.stderr)
        return 1

    if args.dump:
        print(f"{len(records)} listed companies; newest record has "
              f"{len(records[-1])} keys.\n")
        print("Keys:", sorted(records[-1]))
        print("\nResolved fields on one record:")
        scale_key = detect_scale_key(records)
        for field in _FIELDS:
            print(f"  {field:9} {_pick(records[-1], field, scale_key)!r}")
        print(f"\nScale category column, found by its values: {scale_key or '(none)'}")
        spread = {}
        for rec in records:
            key = _pick(rec, "scale", scale_key) or "(blank)"
            spread[key] = spread.get(key, 0) + 1
        print("\nScale categories:")
        for cat, n in sorted(spread.items(), key=lambda kv: -kv[1]):
            print(f"  {cat:28} {n}")
        return 0

    rows = build(records, args.scope)
    floor = args.expect or _FLOOR[args.scope]
    if len(rows) < floor:
        # Refuse to write a half-fetched index. A short list would silently
        # shrink the app's coverage rather than fail visibly, and the file it
        # would overwrite is the only record of what the universe was.
        print(f"::warning title=Universe left unchanged::{args.scope} resolved to "
              f"{len(rows)} constituents, expected at least {floor}, so the file "
              f"was left as it is. The response's own shape follows — the scale "
              f"category is whichever column lists the TOPIX tiers.",
              file=sys.stderr)
        print(describe_records(records), file=sys.stderr)
        return 1

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)

    cats = {}
    for r in rows:
        cats[r["ScaleCategory"]] = cats.get(r["ScaleCategory"], 0) + 1
    print(f"Wrote {len(rows)} {args.scope} constituents to {args.out}")
    for cat, n in sorted(cats.items(), key=lambda kv: -kv[1]):
        print(f"  {cat:28} {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
