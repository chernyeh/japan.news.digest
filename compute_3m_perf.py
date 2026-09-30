#!/usr/bin/env python3
"""3M / 6M / 12M relative performance vs TOPIX, split-adjusted.

    python compute_3m_perf.py        # -> data/jquants_3m_perf.csv, data/split_events.csv
    python compute_3m_perf.py --out /tmp/perf.csv --events-out /tmp/splits.csv --no-yf

Each return compares today's close (data/jquants_prices_latest.csv) with the
close in the archive snapshot nearest the lookback date (data/archive/). Both
are raw closes, so a split between them read as a crash: on 29 Sep 2026 eight
of the Screener's worst performers were 1 October splits, Tokio Marine's 15:1
among them at -92.5%. splits.py finds those events in the archive itself;
each base close is divided by the splits after it before the return is taken.
A return whose window spans a move that could not be explained either way is
left blank rather than published.

This used to be written out inline, as a heredoc, by both daily_prices.yml and
weekly_3m_perf.yml -- and the two copies had drifted (different tolerances,
different fallbacks). Both workflows now run this file.

The yfinance fallback only runs for a period with no archive snapshot close
enough to its lookback date; its prices are already split-adjusted by Yahoo
(auto_adjust=True), so they are not adjusted again here.
"""
import argparse
import csv
import os
import sys
import time
from datetime import date, timedelta

import splits as S

ARCHIVE_DIR = "data/archive"
LATEST_PATH = "data/jquants_prices_latest.csv"
OUT_PATH = "data/jquants_3m_perf.csv"
# (days ago, tolerance in days) for each period
PERIODS = {"3M": (91, 14), "6M": (181, 21), "12M": (365, 28)}
TOPIX_CODES = ["1308", "1306"]   # TOPIX ETFs; 1308 preferred
OUT_COLUMNS = ["Code", "VsTopix3M", "VsTopix6M", "VsTopix12M",
               "TopixReturn3M", "TopixReturn6M", "TopixReturn12M", "ComputedDate"]


def norm_code(raw) -> str:
    return str(raw or "").strip().zfill(4)[:4]


def geo_relative(r_stock, r_bench):
    """Relative return, compounded: (1+r)/(1+b) - 1, in %."""
    if r_stock is None or r_bench is None:
        return None
    return round(((1 + r_stock / 100) / (1 + r_bench / 100) - 1) * 100, 1)


def read_closes(path: str) -> dict:
    """{code: close} from one price CSV; zero and blank closes dropped."""
    out = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            try:
                p = float(row.get("Close") or 0)
            except (TypeError, ValueError):
                continue
            if p > 0:
                out[norm_code(row.get("Code"))] = p
    return out


def latest_date(path: str, fallback: date) -> date:
    """The trading date the latest-prices file carries, else `fallback`."""
    best = None
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            try:
                d = date.fromisoformat((row.get("Date") or "")[:10])
            except ValueError:
                continue
            best = d if best is None or d > best else best
    return best or fallback


def list_archives(archive_dir: str = ARCHIVE_DIR) -> dict:
    """{snapshot date: filename}"""
    out = {}
    if not os.path.isdir(archive_dir):
        return out
    for fname in os.listdir(archive_dir):
        if fname.startswith("prices_") and fname.endswith(".csv"):
            try:
                out[date.fromisoformat(fname[7:-4])] = fname
            except ValueError:
                pass
    return out


def find_best_archive(archives: dict, target: date, tolerance: int):
    """(date, filename) of the snapshot nearest `target`, if within tolerance."""
    if not archives:
        return None, None
    d = min(archives, key=lambda x: abs((x - target).days))
    if abs((d - target).days) <= tolerance:
        return d, archives[d]
    return None, None


def load_series(archives: dict, since: date, codes: set, archive_dir: str = ARCHIVE_DIR) -> dict:
    """{code: [(date, close), ...]} for `codes`, from every snapshot on or after `since`."""
    series = {}
    for d in sorted(x for x in archives if x >= since):
        for code, p in read_closes(os.path.join(archive_dir, archives[d])).items():
            if code in codes:
                series.setdefault(code, []).append((d, p))
    return series


def adjusted_return(code: str, p_now: float, p_then: float, base_date: date,
                    today: date, events: dict, adjust: bool = True):
    """% return from base_date to today, across any splits in between. None if
    the window spans a move that could not be resolved."""
    if not p_then or p_then <= 0 or not p_now or p_now <= 0:
        return None
    if adjust:
        evs = events.get(code, [])
        if S.unresolved_between(evs, base_date, today):
            return None
        p_then = p_then / S.cumulative_factor(evs, base_date, today)
    return (p_now / p_then - 1) * 100


def compute_results(today: date, today_prices: dict, bases: dict,
                    topix_returns: dict, events: dict) -> tuple:
    """({code: {period: vs-TOPIX %}}, stats).

    bases: {period: (base_date, {code: close}, adjust)} -- `adjust` is False
    for prices that arrived already split-adjusted (the yfinance fallback)."""
    results, stats = {}, {"adjusted": 0, "withheld": 0}
    for code, p_now in today_prices.items():
        row = {}
        for period in PERIODS:
            if period not in topix_returns or period not in bases:
                continue
            base_date, prices, adjust = bases[period]
            p_then = prices.get(code)
            if not p_then:
                continue
            r = adjusted_return(code, p_now, p_then, base_date, today, events, adjust)
            if r is None:
                stats["withheld"] += 1
                continue
            if adjust and S.cumulative_factor(events.get(code, []), base_date, today) != 1.0:
                stats["adjusted"] += 1
            rel = geo_relative(r, topix_returns[period])
            if rel is not None:
                row[period] = rel
        if row:
            results[code] = row
    return results, stats


def topix_from_bases(today: date, today_prices: dict, bases: dict, events: dict) -> dict:
    out = {}
    for period, (base_date, prices, adjust) in bases.items():
        for tc in TOPIX_CODES:
            r = adjusted_return(tc, today_prices.get(tc), prices.get(tc),
                                base_date, today, events, adjust)
            if r is not None and abs(r) <= 80:
                out[period] = round(r, 2)
                print(f"  TOPIX {period} from archive ({tc}): {out[period]}%")
                break
    return out


# ── Fallbacks for a period with no usable archive snapshot ───────────────────

def yf_topix_returns(periods: list) -> dict:
    out = {}
    try:
        import yfinance as yf
    except ImportError:
        return out
    for ticker in ("1308.T", "1306.T"):
        try:
            closes = yf.Ticker(ticker).history(period="2y", auto_adjust=True)["Close"].dropna()
        except Exception as e:
            print(f"TOPIX {ticker}: {e}")
            continue
        if len(closes) < 30:
            continue
        for period in periods:
            if period in out:
                continue
            p_then = float(closes.iloc[max(0, len(closes) - PERIODS[period][0])])
            if p_then > 0:
                cand = round((float(closes.iloc[-1]) / p_then - 1) * 100, 2)
                if abs(cand) <= 80:
                    out[period] = cand
                    print(f"  TOPIX {period} via {ticker}: {cand}%")
        break
    return out


def stooq_topix_returns(periods: list) -> dict:
    """Stooq's TOPIX index series -- more reliable than Yahoo from CI."""
    out = {}
    try:
        import requests
        end = date.today()
        start = end - timedelta(days=400)
        r = requests.get(f"https://stooq.com/q/d/l/?s=^tpx&d1={start:%Y%m%d}"
                         f"&d2={end:%Y%m%d}&i=d", timeout=12)
        rows = []
        for line in r.text.strip().splitlines()[1:]:
            parts = line.split(",")
            if len(parts) >= 5:
                try:
                    rows.append((parts[0], float(parts[4])))
                except ValueError:
                    pass
        rows.sort()
        if len(rows) >= 30:
            for period in periods:
                p_then = rows[max(0, len(rows) - PERIODS[period][0])][1]
                if p_then > 0:
                    cand = round((rows[-1][1] / p_then - 1) * 100, 2)
                    if abs(cand) <= 80:
                        out[period] = cand
                        print(f"  TOPIX {period} via stooq: {cand}%")
    except Exception as e:
        print(f"Stooq TOPIX fallback: {e}")
    return out


def yf_base_prices(codes: list, periods: list) -> dict:
    """{period: {code: split-adjusted close ~period ago}} via yfinance."""
    out = {p: {} for p in periods}
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:
        return out
    tickers = [c + ".T" for c in codes]
    batch_size = 50
    total = (len(tickers) + batch_size - 1) // batch_size
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        n = i // batch_size + 1
        if n % 10 == 0:
            print(f"  Batch {n}/{total}")
        for attempt in range(3):
            try:
                raw = yf.download(batch, period="13mo", auto_adjust=True,
                                  progress=False, group_by="ticker", threads=False)
                if raw.empty:
                    break
                for t in batch:
                    try:
                        closes = (raw[t]["Close"] if isinstance(raw.columns, pd.MultiIndex)
                                  else raw["Close"]).dropna()
                    except Exception:
                        continue
                    if len(closes) >= 20:
                        for period in periods:
                            idx = max(0, len(closes) - PERIODS[period][0])
                            out[period][t[:-2]] = float(closes.iloc[idx])
                break
            except Exception as e:
                print(f"Batch {n} attempt {attempt + 1} error: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        time.sleep(2)
    return out


# ── Main ─────────────────────────────────────────────────────────────────────

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--latest", default=LATEST_PATH)
    ap.add_argument("--archive-dir", default=ARCHIVE_DIR)
    ap.add_argument("--out", default=OUT_PATH)
    ap.add_argument("--events-out", default=S.EVENTS_PATH)
    ap.add_argument("--no-yf", action="store_true",
                    help="skip the network fallbacks (for a dry run)")
    args = ap.parse_args(argv)

    if not os.path.exists(args.latest):
        print(f"ERROR: {args.latest} not found")
        return 1
    today_prices = read_closes(args.latest)
    today = latest_date(args.latest, date.today())
    print(f"Today prices: {len(today_prices)} stocks, as of {today}")

    archives = list_archives(args.archive_dir)
    bases = {}
    for period, (days, tol) in PERIODS.items():
        d, fname = find_best_archive(archives, today - timedelta(days=days), tol)
        if d:
            prices = read_closes(os.path.join(args.archive_dir, fname))
            bases[period] = (d, prices, True)
            print(f"{period}: archive {fname} ({abs((d - (today - timedelta(days=days))).days)}d "
                  f"from target, {len(prices)} stocks)")
        else:
            print(f"{period}: no archive within {tol}d -- network fallback")

    # Splits: every move the archive cannot explain by trading, across the
    # longest window any period reaches back to.
    since = min([b[0] for b in bases.values()] + [today - timedelta(days=400)])
    codes = set(today_prices) | set(TOPIX_CODES)
    series = load_series(archives, since, codes, args.archive_dir)
    for code, p in today_prices.items():
        series.setdefault(code, []).append((today, p))
    notices = S.tdnet_split_notices()
    events = {}
    for code, pts in series.items():
        evs = S.detect_events(pts, notices.get(code, []))
        if evs:
            events[code] = evs
    by_status = {}
    for evs in events.values():
        for e in evs:
            by_status[e["status"]] = by_status.get(e["status"], 0) + 1
    print(f"Split scan: {len(series)} series since {since}: {by_status or 'nothing found'}")

    topix_returns = topix_from_bases(today, today_prices, bases, events)
    missing = [p for p in PERIODS if p not in bases or p not in topix_returns]
    if missing and not args.no_yf:
        print(f"Network fallback for: {missing}")
        need_topix = [p for p in missing if p not in topix_returns]
        topix_returns.update(yf_topix_returns(need_topix))
        topix_returns.update({k: v for k, v in stooq_topix_returns(
            [p for p in need_topix if p not in topix_returns]).items()})
        need_prices = [p for p in missing if p not in bases]
        if need_prices:
            fetched = yf_base_prices(sorted(today_prices), need_prices)
            for p in need_prices:
                # Already split-adjusted by Yahoo: no archive adjustment.
                bases[p] = (today - timedelta(days=PERIODS[p][0]), fetched.get(p, {}), False)
                print(f"  yfinance {p}: {len(fetched.get(p, {}))} stocks")
    print(f"TOPIX returns: {topix_returns}")

    results, stats = compute_results(today, today_prices, bases, topix_returns, events)
    print(f"Computed: {len(results)} stocks -- {stats['adjusted']} stock-period(s) "
          f"split-adjusted, {stats['withheld']} withheld across an unresolved move")

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    tr = {p: (round(topix_returns[p], 2) if p in topix_returns else "") for p in PERIODS}
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(OUT_COLUMNS)
        for code, perfs in sorted(results.items()):
            w.writerow([code, perfs.get("3M", ""), perfs.get("6M", ""), perfs.get("12M", ""),
                        tr["3M"], tr["6M"], tr["12M"], today.isoformat()])
    print(f"Written {args.out}: {len(results)} entries")

    S.write_events(events, args.events_out, notices)
    print(f"Written {args.events_out}: {sum(len(v) for v in events.values())} event(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
