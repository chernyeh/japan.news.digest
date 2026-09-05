#!/usr/bin/env python3
"""Turn the daily price archive into a per-company liquidity read.

Position sizing needs to know how long it would take to get out. Nothing in this
repo could answer that: the archive held Code, Date, Close and MarketCapB, and
volume -- which yfinance returns in the same frame -- was thrown away. The daily
price job now keeps it, and this reduces the archive to one row per company so
the app never has to load a time series.

    python compute_liquidity.py                 # -> data/liquidity.csv
    python compute_liquidity.py --participation 0.15 --probe 7203

An important limit, carried through into the output rather than buried here: the
archive is one snapshot per workflow run, not one per trading session. Over the
last year there are ~187 files for ~245 sessions, so a "20-day" average is
twenty *observations*. Every window below is therefore counted in observations
and reported with its own span in calendar days, so a thin or gappy sample is
visible instead of implied.
"""
import argparse
import csv
import glob
import os
import statistics
import sys
from datetime import date

ARCHIVE_GLOB = "data/archive/prices_*.csv"
OUT_PATH = "data/liquidity.csv"
COLUMNS = ["code", "close", "mcap_b", "adv_jpy_20", "adv_jpy_60", "adv_shares_20",
           "obs_20", "obs_60", "span_days_20", "participation", "days_to_exit_100m",
           "tier", "first_obs", "last_obs", "as_of"]

# What fraction of a day's traded value you assume you can be, without moving
# the price. 20% is a common desk convention and is deliberately a parameter:
# it is an assumption, not a measurement, and the output names it so a reader
# can disagree with it.
DEFAULT_PARTICIPATION = 0.20
YARDSTICK_JPY = 100_000_000  # the position the days-to-exit column prices


def read_archive(pattern: str = ARCHIVE_GLOB) -> dict:
    """{code: [(date, close, volume, turnover_jpy, mcap_b), ...]} newest last.

    Files written before volume was collected simply contribute no turnover;
    they are not an error and are not dropped, because their closes still
    date-stamp the series."""
    series = {}
    for path in sorted(glob.glob(pattern)):
        stamp = os.path.basename(path)[7:-4]
        try:
            with open(path, newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    code = str(row.get("Code", "")).strip()
                    if not code:
                        continue
                    def _num(key):
                        try:
                            return float(row.get(key) or "")
                        except ValueError:
                            return None
                    close, vol = _num("Close"), _num("Volume")
                    turn = _num("TurnoverJPY")
                    if turn is None and close and vol:
                        turn = close * vol
                    series.setdefault(code, []).append(
                        (stamp, close, vol, turn, _num("MarketCapB")))
        except OSError as exc:
            print(f"  skipped {path}: {exc}", file=sys.stderr)
    return series


def _median(values):
    vals = [v for v in values if v]
    return statistics.median(vals) if vals else None


def _span_days(stamps) -> int:
    """Calendar days the window actually covers, which is the honest denominator
    when the observations are not one per session."""
    try:
        days = sorted(date.fromisoformat(s) for s in stamps)
        return (days[-1] - days[0]).days
    except Exception:
        return 0


def tier_for(adv_jpy) -> str:
    """A coarse label, not a score. The thresholds are round numbers chosen to
    separate 'size it how you like' from 'this constrains the position', and
    they are stated so a reader can substitute their own."""
    if not adv_jpy:
        return "unknown"
    if adv_jpy >= 5_000_000_000:
        return "deep"          # >= Y5bn/day
    if adv_jpy >= 1_000_000_000:
        return "adequate"      # Y1-5bn/day
    if adv_jpy >= 200_000_000:
        return "thin"          # Y200m-1bn/day
    return "very thin"         # < Y200m/day


def liquidity_row(code: str, obs: list, participation: float = DEFAULT_PARTICIPATION) -> dict:
    """One company's read, or {} where there is nothing usable to report."""
    if not obs:
        return {}
    obs = sorted(obs)
    w20, w60 = obs[-20:], obs[-60:]
    adv20 = _median(t for _, _, _, t, _mc in w20)
    adv60 = _median(t for _, _, _, t, _mc in w60)
    shares20 = _median(v for _, _, v, _t, _mc in w20)
    close = next((c for _, c, _, _t, _mc in reversed(obs) if c), None)
    mcap = next((m for _, _, _, _t, m in reversed(obs) if m), None)
    # Days to liquidate the yardstick position at the stated participation rate.
    # Deliberately not capped: a number like 340 days is the finding.
    dte = (YARDSTICK_JPY / (adv20 * participation)) if adv20 else None
    return {
        "code": code,
        "close": round(close, 2) if close else "",
        "mcap_b": round(mcap, 1) if mcap else "",
        "adv_jpy_20": round(adv20) if adv20 else "",
        "adv_jpy_60": round(adv60) if adv60 else "",
        "adv_shares_20": round(shares20) if shares20 else "",
        "obs_20": sum(1 for _, _, _, t, _mc in w20 if t),
        "obs_60": sum(1 for _, _, _, t, _mc in w60 if t),
        "span_days_20": _span_days([d for d, _, _, t, _mc in w20 if t]),
        "participation": participation,
        # Two decimals below a day: rounding 0.04 to "0.0 days" reads
        # as instant, which is a different claim from "an afternoon".
        "days_to_exit_100m": ("" if not dte else
                              round(dte, 2) if dte < 1 else round(dte, 1)),
        "tier": tier_for(adv20),
        "first_obs": obs[0][0],
        "last_obs": obs[-1][0],
        "as_of": date.today().isoformat(),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--archive", default=ARCHIVE_GLOB)
    ap.add_argument("--out", default=OUT_PATH)
    ap.add_argument("--participation", type=float, default=DEFAULT_PARTICIPATION,
                    help="assumed share of a day's traded value (default 0.20)")
    ap.add_argument("--probe", default="", help="print one company's window and exit")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    series = read_archive(args.archive)
    if not series:
        print(f"No archive files matched {args.archive}.", file=sys.stderr)
        return 1
    print(f"{len(series)} companies across {len(glob.glob(args.archive))} archive file(s)")

    if args.probe:
        obs = sorted(series.get(args.probe, []))
        if not obs:
            print(f"No archive rows for {args.probe}.")
            return 1
        print(f"\n{args.probe}: {len(obs)} observation(s), "
              f"{obs[0][0]} -> {obs[-1][0]}")
        print("  last 10 (date, close, volume, turnover):")
        for row in obs[-10:]:
            print(f"    {row[0]}  close={row[1]}  vol={row[2]}  turn={row[3]}")
        print(f"\n  {liquidity_row(args.probe, obs, args.participation)}")
        return 0

    rows = [r for r in (liquidity_row(c, o, args.participation)
                        for c, o in sorted(series.items())) if r]
    with_turnover = sum(1 for r in rows if r["adv_jpy_20"])
    print(f"{len(rows)} row(s); {with_turnover} with a turnover figure "
          f"({round(with_turnover / max(len(rows), 1) * 100, 1)}%)")
    if with_turnover:
        from collections import Counter
        for tier, n in Counter(r["tier"] for r in rows if r["adv_jpy_20"]).most_common():
            print(f"   {tier:11} {n}")
    else:
        print("   No turnover anywhere yet: every archive file predates volume "
              "collection. This fills in from the next daily price run.")

    if args.dry_run:
        print("\n--dry-run: nothing written.")
        return 0
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    print(f"\nWrote {len(rows)} rows -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
