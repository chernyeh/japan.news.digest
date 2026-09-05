#!/usr/bin/env python3
"""The yen's daily history, and what a company's price does when it moves.

market_data.py fetches USD/JPY live for the Markets tab and discards it, so no
history existed and the question "how does this name actually behave when the
yen moves" could not be asked at all -- only the sensitivity a company chooses
to disclose, which is usually translation-only and ignores hedging.

Two halves, both pure so they can be tested without a network:

  * merge_fx_row / load_fx_history -- the archive, keyed on the date so a re-run
    on the same day corrects its row rather than appending a duplicate.
  * fx_beta -- an ordinary least-squares slope of the company's returns on the
    yen's, plus the r-squared and the sample size, because a beta without them
    is not a finding.

The archive is written by the daily price workflow. A day not archived cannot be
recovered later, which is why it starts collecting well before there is enough
of it to regress.
"""
import csv
import os

FX_HISTORY_PATH = "data/fx_history.csv"
FX_PAIRS = ("USDJPY", "EURJPY", "CNYJPY")
FX_COLUMNS = ["Date"] + list(FX_PAIRS)

# Below this many paired observations a slope is arithmetic rather than
# evidence. Two quarters of daily data would be ~120; 30 is the floor at which
# it is worth showing at all, and the sample size travels with the answer so a
# reader can apply their own bar.
MIN_OBS = 30


def load_fx_history(path: str = FX_HISTORY_PATH) -> dict:
    """{date: {pair: rate}} from the archive, or {} if it is not there yet."""
    out = {}
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                day = (row.get("Date") or "").strip()
                if not day:
                    continue
                rates = {}
                for pair in FX_PAIRS:
                    try:
                        rates[pair] = float(row.get(pair) or "")
                    except ValueError:
                        pass
                if rates:
                    out[day] = rates
    except FileNotFoundError:
        return {}
    return out


def merge_fx_row(existing: dict, day: str, rates: dict) -> dict:
    """The archive with `day` set to `rates`. Same-day re-runs correct rather
    than duplicate, and a pair that failed to fetch leaves the stored value
    alone instead of blanking it."""
    merged = dict(existing or {})
    prior = dict(merged.get(day) or {})
    for pair in FX_PAIRS:
        val = rates.get(pair)
        if val:
            prior[pair] = val
    if prior:
        merged[day] = prior
    return merged


def write_fx_history(history: dict, path: str = FX_HISTORY_PATH):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FX_COLUMNS)
        w.writeheader()
        for day in sorted(history):
            row = {"Date": day}
            row.update({p: history[day].get(p, "") for p in FX_PAIRS})
            w.writerow(row)


def _returns(series: dict) -> dict:
    """{date: pct change on the previous *available* observation}.

    "Previous available" rather than "previous day" is deliberate: the price
    archive is one snapshot per workflow run, so consecutive observations can be
    several days apart, and pretending otherwise would understate the moves."""
    days = sorted(d for d, v in series.items() if v)
    out = {}
    for prev, cur in zip(days, days[1:]):
        a, b = series[prev], series[cur]
        if a:
            out[cur] = (b - a) / a
    return out


def fx_beta(stock: dict, fx: dict, min_obs: int = MIN_OBS) -> dict:
    """How far the stock moves for a 1% move in the yen, measured rather than
    disclosed.

    `stock` and `fx` are {date: level}. Returns {} where there is not enough
    overlap to say anything. A positive beta means the stock rises as the pair
    rises -- and since the pairs are quoted as yen per unit of foreign currency,
    a rising pair is a *weaker* yen, which is the direction an exporter benefits
    from. That sign convention is stated here because getting it backwards
    inverts every conclusion drawn from it.
    """
    sr, fr = _returns(stock), _returns(fx)
    days = sorted(set(sr) & set(fr))
    n = len(days)
    if n < min_obs:
        return {"obs": n, "insufficient": True, "min_obs": min_obs}
    xs = [fr[d] for d in days]
    ys = [sr[d] for d in days]
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    # Not just "sxx == 0". If the yen barely moved across the window, sxx is
    # tiny but non-zero and the slope it produces is floating-point noise
    # divided by floating-point noise -- a confident-looking number with nothing
    # behind it. A daily standard deviation below 0.01% is no variation at all;
    # USD/JPY's is nearer 0.5%.
    if (sxx / n) ** 0.5 < 1e-4:
        return {"obs": n, "insufficient": True, "min_obs": min_obs,
                "reason": "the yen barely moved over this window"}
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    beta = sxy / sxx
    syy = sum((y - my) ** 2 for y in ys)
    r2 = (sxy ** 2) / (sxx * syy) if syy else 0.0
    return {
        "beta": beta,
        "r2": r2,
        "obs": n,
        "first": days[0],
        "last": days[-1],
        "insufficient": False,
        # An r-squared this low means the yen explains almost none of the
        # variance, and a beta drawn from it should not be quoted as though the
        # relationship were reliable.
        "weak": r2 < 0.05,
    }
