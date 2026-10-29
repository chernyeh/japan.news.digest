"""
collect_tdnet.py — builds/updates the rolling TDnet filing index
(data/tdnet_filings.csv) used by the Research tab.

Run modes (both used by .github/workflows/*_tdnet.yml):
  python collect_tdnet.py --days 3     # daily incremental (trailing window, self-healing)
  python collect_tdnet.py --days 730   # one-time ~2-year backfill

No API key needed — mirrors the Reg Filings tab's keyless Yanoshin + TSE
English-search sources, just walked across a much longer history.
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import requests

from tdnet import (fetch_tdnet_range, fetch_english_lookup, prune_and_dedupe,
                    fetch_tdnet_official_day, _normalize_code, _title_key,
                    attach_english, CSV_EN_FIELDS,
                    write_filings_csv, read_filings_csv, row_key)
from translate import translate_ja_to_en

DEFAULT_OUT = os.path.join("data", "tdnet_filings.csv")
OFFICIAL_LIST_DAYS = 30  # how far back TDnet's own daily list pages go
CHUNK_DAYS = 3  # matches the window already proven to work for the live Reg Filings tab


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3,
                         help="How many days back (from today) to (re)fetch.")
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--sleep", type=float, default=0.5,
                         help="Delay between per-chunk requests (be polite to Yanoshin).")
    args = parser.parse_args()

    existing = read_filings_csv(args.out)
    by_key = {row_key(row): row for row in existing}
    print(f"Existing index: {len(by_key)} filings")

    today = date.today()
    range_start = today - timedelta(days=args.days)
    d_from_full = range_start.strftime("%Y%m%d")
    d_to_full = today.strftime("%Y%m%d")

    print(f"Building English disclosure lookup for {d_from_full}-{d_to_full}...")
    session = requests.Session()
    en_lookup = fetch_english_lookup(d_from_full, d_to_full, session=session)
    print(f"English lookup: {len(en_lookup)} entries")

    # Walk in small (CHUNK_DAYS) windows — Yanoshin's JP endpoint isn't
    # documented to paginate, so this reuses the exact window size already
    # proven to work for the live Reg Filings tab, repeated across history.
    chunks = []
    cursor = today
    while cursor >= range_start:
        chunk_from = max(cursor - timedelta(days=CHUNK_DAYS - 1), range_start)
        chunks.append((chunk_from, cursor))
        cursor = chunk_from - timedelta(days=1)

    fetched_chunks, failed_chunks, new_rows = 0, 0, 0
    fresh = []   # every row fetched this run; English versions are paired once all are in
    for i, (c_from, c_to) in enumerate(chunks):
        try:
            rows = fetch_tdnet_range(c_from.strftime("%Y%m%d"), c_to.strftime("%Y%m%d"),
                                      session=session)
            fetched_chunks += 1
            fresh.extend(rows)
            for row in rows:
                key = row_key(row)
                if key not in by_key:
                    new_rows += 1
                by_key[key] = row
            print(f"  {c_from}..{c_to}: {len(rows)} filings")
        except Exception as exc:
            failed_chunks += 1
            print(f"  {c_from}..{c_to}: FAILED ({exc}) — skipping, will retry on next scheduled run")
        if i < len(chunks) - 1:
            time.sleep(args.sleep)

    # Yanoshin has run up to a day behind TDnet, so the recent days are also
    # read from TDnet's own list (which keeps about a month). A filing both
    # sources carry is matched on code + time + title, since the two can
    # spell its PDF link differently.
    seen = {(r.get("Code"), r.get("PubDateTime"), _title_key(r.get("Title"))) for r in by_key.values()}
    official_new = 0
    for n in range(min(args.days, OFFICIAL_LIST_DAYS) + 1):
        day = today - timedelta(days=n)
        for o in fetch_tdnet_official_day(day.strftime("%Y%m%d"), session=session):
            code = _normalize_code(o["code"])
            if not code or (code, o["pub_date"], _title_key(o["title"])) in seen:
                continue
            row = {"Code": code, "Name": o["name"], "NameEN": "", "Title": o["title"],
                   "TitleEN": "", "PubDateTime": o["pub_date"],
                   "DocUrl": o["doc_url"], "EngUrl": ""}
            seen.add((code, o["pub_date"], _title_key(o["title"])))
            by_key[row_key(row)] = row
            fresh.append(row)
            official_new += 1
    print(f"TDnet's own list added {official_new} filing(s) Yanoshin did not have")
    new_rows += official_new

    # Pair each English version with the filing it translates, across both
    # sources at once (see tdnet.attach_english).
    left = attach_english(fresh, en_lookup, CSV_EN_FIELDS)
    print(f"English versions: {sum(len(v) for v in en_lookup.values()) - len(left)} paired, "
          f"{len(left)} left unpaired")

    merged = prune_and_dedupe(list(by_key.values()))

    # Translate only rows with no English title match — covers both new rows
    # and any pre-existing rows from before this field existed. Self-limiting
    # after the first run.
    to_translate = [row for row in merged if row.get("Title") and not row.get("TitleEN")]
    if to_translate:
        print(f"Translating {len(to_translate)} filing title(s)...")
        with ThreadPoolExecutor(max_workers=10) as ex:
            list(ex.map(lambda row: row.__setitem__(
                "TitleEN", translate_ja_to_en(row["Title"])), to_translate))

    write_filings_csv(args.out, merged)
    print(f"Done: {fetched_chunks} chunks fetched, {failed_chunks} failed, "
          f"{new_rows} new filings, {len(to_translate)} translated, "
          f"{len(merged)} total in index -> {args.out}")


if __name__ == "__main__":
    main()
