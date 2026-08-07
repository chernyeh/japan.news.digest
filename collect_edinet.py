"""
collect_edinet.py — builds/updates the rolling EDINET filing index
(data/edinet_filings.csv) used by the Research tab.

Run modes (both used by .github/workflows/*_edinet.yml):
  python collect_edinet.py --days 3     # daily incremental (trailing window, self-healing)
  python collect_edinet.py --days 730   # one-time ~2-year backfill

Requires EDINET_API_KEY in the environment (free registration at
https://disclosure2.edinet-fsa.go.jp/ — see edinet.py's module docstring).
"""

import argparse
import os
import sys
import time
from datetime import date, timedelta

from edinet import fetch_edinet_day, prune_and_dedupe, write_filings_csv, read_filings_csv

DEFAULT_OUT = os.path.join("data", "edinet_filings.csv")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3,
                         help="How many days back (from today) to (re)fetch.")
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--sleep", type=float, default=0.4,
                         help="Delay between per-day requests (be polite to EDINET's API).")
    args = parser.parse_args()

    api_key = os.environ.get("EDINET_API_KEY", "")
    if not api_key:
        print("ERROR: EDINET_API_KEY not set in environment.", file=sys.stderr)
        sys.exit(1)

    existing = read_filings_csv(args.out)
    by_doc_id = {row["DocID"]: row for row in existing if row.get("DocID")}
    print(f"Existing index: {len(by_doc_id)} filings")

    today = date.today()
    dates = [today - timedelta(days=i) for i in range(args.days)]
    fetched_days, failed_days, new_rows = 0, 0, 0

    for i, d in enumerate(dates):
        try:
            day_rows = fetch_edinet_day(d, api_key)
            fetched_days += 1
            for row in day_rows:
                if row["DocID"] not in by_doc_id:
                    new_rows += 1
                by_doc_id[row["DocID"]] = row
            print(f"  {d}: {len(day_rows)} relevant filings")
        except Exception as exc:
            failed_days += 1
            print(f"  {d}: FAILED ({exc}) — skipping, will retry on next scheduled run")
        if i < len(dates) - 1:
            time.sleep(args.sleep)

    merged = prune_and_dedupe(list(by_doc_id.values()))
    write_filings_csv(args.out, merged)
    print(f"Done: {fetched_days} days fetched, {failed_days} failed, "
          f"{new_rows} new filings, {len(merged)} total in index -> {args.out}")


if __name__ == "__main__":
    main()
