"""
compute_price_history.py — one closing price per company per week, kept for
good, in data/price_history.csv.

The daily job already writes a snapshot of every close to data/archive/ (one
file per trading day, pruned at 760 days). That is the right shape for a
3-month return and the wrong one for a chart: the app would have to fetch
hundreds of files to draw one line. This condenses it to the last close of
each ISO week, per company, and merges into the existing file so weeks the
archive has since pruned are never lost.

Closes are as recorded, not split-adjusted: a forward P/E divides that day's
price by the EPS guided at that time, and both sit on the share count of the
day, so adjusting one without the other would be the error.

    python compute_price_history.py
"""

import csv
import os
from datetime import date

ARCHIVE_DIR = os.path.join("data", "archive")
OUT_PATH = os.path.join("data", "price_history.csv")
COLUMNS = ["code", "week", "date", "close"]


def week_key(iso: str) -> str:
    y, w, _ = date.fromisoformat(iso).isocalendar()
    return f"{y}-W{w:02d}"


def condense(daily_rows) -> dict:
    """{(code, week): (date, close)} keeping each week's last trading day."""
    out = {}
    for code, day, close in daily_rows:
        try:
            close = float(close)
        except (TypeError, ValueError):
            continue
        if not code or not day or close <= 0:
            continue
        k = (code, week_key(day))
        if k not in out or day > out[k][0]:
            out[k] = (day, close)
    return out


def _archive_rows(archive_dir: str):
    for fname in sorted(os.listdir(archive_dir)):
        if not (fname.startswith("prices_") and fname.endswith(".csv")):
            continue
        with open(os.path.join(archive_dir, fname), newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                yield r.get("Code", ""), (r.get("Date") or fname[7:-4])[:10], r.get("Close")


def main(archive_dir: str = ARCHIVE_DIR, out_path: str = OUT_PATH) -> int:
    existing = {}
    if os.path.exists(out_path):
        with open(out_path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                existing[(r["code"], r["week"])] = (r["date"], float(r["close"]))
    fresh = condense(_archive_rows(archive_dir)) if os.path.isdir(archive_dir) else {}
    merged = dict(existing)
    for k, v in fresh.items():
        if k not in merged or v[0] >= merged[k][0]:
            merged[k] = v
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(COLUMNS)
        for (code, week), (day, close) in sorted(merged.items()):
            w.writerow([code, week, day, f"{close:.2f}"])
    print(f"Price history: {len(merged)} weekly closes "
          f"({len(merged) - len(existing)} new) -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
