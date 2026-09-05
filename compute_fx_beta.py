#!/usr/bin/env python3
"""How each company's price has actually behaved when the yen moved.

A company's disclosed FX sensitivity is a translation figure: it says what a one
yen move does to reported operating profit under the company's own assumptions,
ignores hedging that is usually months forward, and is not linear across large
moves. What it does not say is how the *share price* responds -- which is what a
position is exposed to, and which the market has already been pricing.

This measures it: an ordinary least-squares regression of each company's returns
on USD/JPY's, over the paired observations in the price archive. Reduced to one
row per company, so the app never loads a series.

    python compute_fx_beta.py                # -> data/fx_beta.csv
    python compute_fx_beta.py --probe 7203

The sign convention matters and is carried into the output. The pair is quoted
as yen per dollar, so a *rising* USD/JPY is a *weaker* yen -- the direction an
exporter benefits from. A positive beta therefore means the stock rises as the
yen weakens.
"""
import argparse
import csv
import os
import sys

import compute_liquidity as CL
import fx_history as FXH

OUT_PATH = "data/fx_beta.csv"
COLUMNS = ["code", "fx_beta", "r2", "obs", "first_obs", "last_obs", "weak", "as_of"]


def beta_rows(series: dict, fx: dict, min_obs: int = FXH.MIN_OBS) -> list:
    """One row per company that has enough overlap with the FX history."""
    from datetime import date
    fx_levels = {d: r.get("USDJPY") for d, r in fx.items() if r.get("USDJPY")}
    today = date.today().isoformat()
    rows = []
    for code, obs in sorted(series.items()):
        closes = {d: c for d, c, _v, _t, _m in obs if c}
        res = FXH.fx_beta(closes, fx_levels, min_obs)
        if res.get("insufficient"):
            continue
        rows.append({
            "code": code,
            "fx_beta": round(res["beta"], 3),
            "r2": round(res["r2"], 3),
            "obs": res["obs"],
            "first_obs": res["first"],
            "last_obs": res["last"],
            # Carried rather than filtered on: a weak fit is itself a finding --
            # it says the yen is not what drives this name, which is worth
            # knowing about a company everyone calls an FX play.
            "weak": "yes" if res["weak"] else "no",
            "as_of": today,
        })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--archive", default=CL.ARCHIVE_GLOB)
    ap.add_argument("--fx", default=FXH.FX_HISTORY_PATH)
    ap.add_argument("--out", default=OUT_PATH)
    ap.add_argument("--min-obs", type=int, default=FXH.MIN_OBS)
    ap.add_argument("--probe", default="")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    fx = FXH.load_fx_history(args.fx)
    if not fx:
        print(f"No FX history at {args.fx} yet. The daily price job starts "
              f"writing it from its next run; this needs roughly "
              f"{args.min_obs} paired observations before it can say anything.",
              file=sys.stderr)
        return 0
    series = CL.read_archive(args.archive)
    print(f"{len(series)} companies, {len(fx)} FX day(s)")

    if args.probe:
        obs = series.get(args.probe, [])
        closes = {d: c for d, c, _v, _t, _m in obs if c}
        fxl = {d: r.get("USDJPY") for d, r in fx.items() if r.get("USDJPY")}
        print(f"\n{args.probe}: {len(closes)} close(s), {len(fxl)} FX level(s), "
              f"{len(set(closes) & set(fxl))} shared date(s)")
        print(f"  {FXH.fx_beta(closes, fxl, args.min_obs)}")
        return 0

    rows = beta_rows(series, fx, args.min_obs)
    print(f"{len(rows)} company/ies with a usable regression "
          f"(min {args.min_obs} paired observations)")
    if rows:
        weak = sum(1 for r in rows if r["weak"] == "yes")
        print(f"   {weak} of them with r2 below 0.05 — the yen explains little "
              f"of their variance")
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
