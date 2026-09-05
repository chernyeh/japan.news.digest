#!/usr/bin/env python3
"""Pick the watchlist companies that filed results in the latest TDnet batch.

data/consensus.csv is rebuilt once a week, on Saturdays. In results season that
is up to six days stale, which is exactly the window in which a progress rate
against guidance is worth looking at. This reads the TDnet index that
collect_tdnet.py has just refreshed and prints the handful of codes worth
re-collecting, for `collect_consensus.py --only`.

Prints a comma-separated list on stdout (empty if there is nothing to do) and a
human-readable account on stderr, so the workflow log says why it did or did
not run.

    python select_refresh_codes.py                 # today's watchlist filers
    python select_refresh_codes.py --date 2026-08-06 --cap 5
"""
import argparse
import csv
import sys

import watchlist

# Matches the full-year tanshin (2026年3月期 決算短信) and every quarterly one
# (第１四半期決算短信). The quarterly filings are the point: they carry the
# cumulative year-to-date figures that the progress rate is computed from.
RESULTS_MARKER = "決算短信"
DEFAULT_CAP = 20
TDNET_PATH = "data/tdnet_filings.csv"


def read_filings(path: str = TDNET_PATH) -> list:
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))
    except FileNotFoundError:
        return []


def newest_date(rows: list) -> str:
    """The most recent filing date in the index, as YYYY-MM-DD."""
    return max((str(r.get("PubDateTime", ""))[:10] for r in rows), default="")


def select_refresh_codes(rows: list, watched, cap: int = DEFAULT_CAP,
                         on_date: str = "") -> list:
    """Watchlist codes that filed a 決算短信 on `on_date`, newest date by default.

    The date filter is not cosmetic. collect_tdnet.py fetches a three-day
    trailing window and merges it into a rolling index, so a filing stays in
    the file for three runs; without this the same companies would be
    re-collected three evenings running.
    """
    if not rows:
        return []
    watched = {str(c) for c in (watched or [])}
    if not watched:
        return []
    day = on_date or newest_date(rows)
    out = []
    for row in rows:
        code = str(row.get("Code", "")).strip()
        if code not in watched or code in out:
            continue
        if str(row.get("PubDateTime", ""))[:10] != day:
            continue
        if RESULTS_MARKER not in str(row.get("Title", "")):
            continue
        out.append(code)
    # Japanese results season clusters hard — early February, May, August and
    # November. The cap keeps a two-minute job from becoming an hour on the
    # heaviest days; whatever it drops is picked up by Saturday's full run.
    return out[:cap]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--filings", default=TDNET_PATH)
    ap.add_argument("--date", default="", help="YYYY-MM-DD; default is the newest in the index")
    ap.add_argument("--cap", type=int, default=DEFAULT_CAP)
    args = ap.parse_args()

    rows = read_filings(args.filings)
    watched = watchlist.load_codes_from_disk()
    if not rows:
        print(f"No filings index at {args.filings}.", file=sys.stderr)
    if not watched:
        print("Watchlist is empty — nothing to refresh. Star a company in the "
              "app and it will be picked up the next time it reports.",
              file=sys.stderr)

    day = args.date or newest_date(rows)
    codes = select_refresh_codes(rows, watched, args.cap, args.date)
    print(f"{len(watched)} watched, filings dated {day or '?'}, "
          f"{len(codes)} to refresh: {', '.join(codes) or 'none'}", file=sys.stderr)
    print(",".join(codes))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
