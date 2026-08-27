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

so the constituent list is a filter over J-Quants' /listed/info, which this
app already has a key for. That is the exchange's own classification rather
than a third party's copy of it, it needs no scraping, and re-running this
picks up the annual review automatically.

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
# a "TOPIX " prefix in some responses and not others.
_SCOPES = {
    "topix100":  ("core30", "large70"),
    "topix500":  ("core30", "large70", "mid400"),
    "topix1000": ("core30", "large70", "mid400", "small 1"),
    "topix":     ("core30", "large70", "mid400", "small 1", "small 2"),
}
_FLOOR = {"topix100": 90, "topix500": 450, "topix1000": 900, "topix": 1500}

COLUMNS = ["Code", "MarketDiv", "Name", "ScaleCategory", "Sector"]


def _pick(record: dict, field: str) -> str:
    for key in _FIELDS[field]:
        val = record.get(key)
        if val not in (None, ""):
            return str(val).strip()
    return ""


def fetch_listed(api_key: str) -> list:
    """Every listed company J-Quants knows about, one page at a time."""
    import requests
    out, page_key = [], None
    while True:
        params = {"pagination_key": page_key} if page_key else {}
        r = requests.get(f"{_JQ_BASE}/listed/info",
                         headers={"x-api-key": api_key}, params=params, timeout=_TIMEOUT)
        if r.status_code in (401, 403):
            raise SystemExit(
                f"J-Quants rejected the key (HTTP {r.status_code}): {r.text[:160]}\n"
                "Set JQUANTS_API_KEY to a V2 API key from https://jpx-jquants.com.")
        if r.status_code != 200:
            raise SystemExit(f"J-Quants HTTP {r.status_code}: {r.text[:160]}")
        payload = r.json()
        batch = payload.get("info") or payload.get("data") or []
        out.extend(batch)
        page_key = payload.get("pagination_key")
        if not page_key or not batch:
            break
    return out


def in_scope(scale: str, scope: str) -> bool:
    low = (scale or "").lower()
    return any(k in low for k in _SCOPES[scope])


def build(records: list, scope: str) -> list:
    rows, seen = [], set()
    for rec in records:
        code = _pick(rec, "code")
        # J-Quants pads codes to five characters for some feeds ("65040");
        # every other file in this repo keys on the four-character TSE code.
        if len(code) == 5 and code.endswith("0"):
            code = code[:4]
        scale = _pick(rec, "scale")
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
        print("ERROR: /listed/info returned nothing.", file=sys.stderr)
        return 1

    if args.dump:
        print(f"{len(records)} listed companies; newest record has "
              f"{len(records[-1])} keys.\n")
        print("Keys:", sorted(records[-1]))
        print("\nResolved fields on one record:")
        for field in _FIELDS:
            print(f"  {field:9} {_pick(records[-1], field)!r}")
        spread = {}
        for rec in records:
            spread[_pick(rec, "scale") or "(blank)"] = \
                spread.get(_pick(rec, "scale") or "(blank)", 0) + 1
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
        print(f"ERROR: {args.scope} resolved to {len(rows)} constituents, expected at "
              f"least {floor}. The scale-category names have probably changed; run "
              f"--dump and check before committing.", file=sys.stderr)
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
