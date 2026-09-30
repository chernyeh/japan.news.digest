"""Tests for split detection and split-adjusted performance — `python test_splits.py`.

No framework, no network, no dependence on the data files the workflows rewrite
every day: every series here is synthetic. The shapes are the real ones,
though — the 1 October 2026 splits, a tender-offer target riding JPX's
expanded limit up, and the few days 1326 was archived in dollars.
"""

import csv
import os
import sys
import tempfile
from datetime import date, timedelta

import compute_3m_perf as P
import compute_fx_beta as FB
import splits as S


def _weekdays(start: date, n: int) -> list:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


# ── The price-limit test ─────────────────────────────────────────────────────

def test_price_limit_bands():
    assert S.price_limit(99) == 30
    assert S.price_limit(100) == 50          # a band's bound belongs to the next band
    assert S.price_limit(7_992) == 1_500
    assert S.price_limit(56_520) == 10_000


def test_ordinary_moves_are_not_events():
    days = _weekdays(date(2026, 6, 1), 60)
    closes = [3000 * (1 + 0.01 * ((i * 7) % 11 - 5)) for i in range(60)]
    assert S.detect_events(list(zip(days, closes))) == []


def test_a_crash_down_the_limit_is_not_a_split():
    # A genuine collapse: ¥2,000 -> limit down (-¥500) -> limit down (-¥400),
    # then the expanded limit lets it fall ¥700 in one session. 1,100 / 400 is
    # 2.75, which would snap to a 3:1 "split" -- and hide the crash -- if the
    # expanded limit were not allowed for after two limit moves.
    days = _weekdays(date(2026, 3, 2), 4)
    closes = [2000, 1500, 1100, 400]
    assert S.detect_events(list(zip(days, closes))) == []


def test_five_for_one_split_is_found_and_snapped():
    days = _weekdays(date(2026, 9, 21), 7)
    closes = [56_000, 56_300, 56_100, 56_520, 11_505, 11_600, 11_815]
    evs = S.detect_events(list(zip(days, closes)))
    assert len(evs) == 1, evs
    e = evs[0]
    assert e["status"] == "split" and e["factor"] == 5.0 and e["snapped"]
    assert e["ex_date"] == days[4]


def test_fifteen_for_one_split():
    # Tokio Marine, 28 -> 29 Sep 2026.
    evs = S.detect_events([(date(2026, 9, 28), 7_992), (date(2026, 9, 29), 523.2)])
    assert [(e["factor"], e["status"]) for e in evs] == [(15.0, "split")]


def test_tender_offer_rally_on_expanded_limit_is_not_a_consolidation():
    # Okamoto Glass, Jan 2026: +¥100 (the limit), then +¥400 (4x, expanded).
    days = _weekdays(date(2026, 1, 13), 4)
    closes = [501, 601, 1001, 950]
    assert S.detect_events(list(zip(days, closes))) == []


def test_up_gap_is_a_consolidation_only_with_a_tdnet_notice():
    days = [date(2026, 5, 1), date(2026, 5, 7)]
    pts = list(zip(days, [300, 3010]))       # 10:1 consolidation shape
    bare = S.detect_events(pts)
    assert [e["status"] for e in bare] == ["unresolved"]
    backed = S.detect_events(pts, notices=[date(2026, 3, 20)])
    assert [(e["status"], round(e["factor"], 3)) for e in backed] == [("consolidation", 0.1)]


def test_out_and_back_bad_quote_cancels():
    # 1326 archived in dollars for three sessions.
    pts = [(date(2026, 1, 9), 60_630), (date(2026, 1, 13), 425.05),
           (date(2026, 1, 14), 423.5), (date(2026, 1, 19), 61_000)]
    evs = S.detect_events(pts)
    assert [e["status"] for e in evs] == ["glitch", "glitch"]
    f = S.cumulative_factor(evs, date(2026, 1, 1), date(2026, 1, 31))
    assert abs(f - 60_630 / 61_000 * 423.5 / 425.05) < 1e-9


def test_unresolved_move_is_reported_in_its_window_only():
    evs = [{"ex_date": date(2026, 7, 9), "factor": 0.6, "status": "unresolved"}]
    assert S.unresolved_between(evs, date(2026, 7, 1), date(2026, 9, 30))
    assert not S.unresolved_between(evs, date(2026, 7, 9), date(2026, 9, 30))
    assert S.cumulative_factor(evs, date(2026, 7, 1), date(2026, 9, 30)) == 1.0


def test_events_round_trip_through_csv():
    evs = {"8766": S.detect_events([(date(2026, 9, 28), 7_992), (date(2026, 9, 29), 523.2)])}
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "split_events.csv")
        S.write_events(evs, path)
        back = S.load_events(path)
    assert back["8766"][0]["factor"] == 15.0
    assert back["8766"][0]["status"] == "split"
    assert back["8766"][0]["ex_date"] == date(2026, 9, 29)


# ── Split-adjusted returns ───────────────────────────────────────────────────

def test_split_mid_window_gives_the_unsplit_return():
    base, today = date(2026, 7, 1), date(2026, 9, 30)
    events = {"9999": S.detect_events([(date(2026, 9, 28), 1_000), (date(2026, 9, 29), 205)])}
    split = P.adjusted_return("9999", 210, 1_000, base, today, events)
    unsplit = P.adjusted_return("0000", 1_050, 1_000, base, today, {})
    assert abs(split - unsplit) < 1e-9 and abs(split - 5.0) < 1e-9


def test_split_before_the_base_date_is_not_applied_again():
    events = {"9999": [{"ex_date": date(2026, 6, 1), "factor": 5.0, "status": "split"}]}
    r = P.adjusted_return("9999", 210, 200, date(2026, 7, 1), date(2026, 9, 30), events)
    assert abs(r - 5.0) < 1e-9


def test_already_adjusted_prices_are_left_alone():
    events = {"9999": [{"ex_date": date(2026, 9, 29), "factor": 5.0, "status": "split"}]}
    r = P.adjusted_return("9999", 210, 200, date(2026, 7, 1), date(2026, 9, 30),
                          events, adjust=False)
    assert abs(r - 5.0) < 1e-9


def test_return_across_an_unresolved_move_is_withheld():
    events = {"9999": [{"ex_date": date(2026, 8, 3), "factor": 0.6, "status": "unresolved"}]}
    assert P.adjusted_return("9999", 750, 460, date(2026, 7, 1), date(2026, 9, 30), events) is None


def test_whole_run_on_a_synthetic_archive():
    """main() end to end: a 5:1 split between the 3M base and today must come
    out as the stock's real relative return, not -80%."""
    today = date(2026, 9, 30)
    base = today - timedelta(days=91)
    with tempfile.TemporaryDirectory() as tmp:
        arch = os.path.join(tmp, "archive")
        os.makedirs(arch)

        def write(path, d, rows):
            with open(path, "w", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                w.writerow(["Code", "Date", "Close"])
                for code, close in rows:
                    w.writerow([code, d.isoformat(), close])

        write(os.path.join(arch, f"prices_{base}.csv"), base,
              [("1308", 3000), ("9999", 1000), ("8888", 2000)])
        write(os.path.join(arch, "prices_2026-09-28.csv"), date(2026, 9, 28),
              [("1308", 3090), ("9999", 1080), ("8888", 2050)])
        write(os.path.join(arch, "prices_2026-09-29.csv"), date(2026, 9, 29),
              [("1308", 3090), ("9999", 216), ("8888", 2050)])
        latest = os.path.join(tmp, "latest.csv")
        write(latest, today, [("1308", 3090), ("9999", 220.5), ("8888", 2060)])

        out = os.path.join(tmp, "perf.csv")
        events_out = os.path.join(tmp, "events.csv")
        assert P.main(["--latest", latest, "--archive-dir", arch, "--out", out,
                       "--events-out", events_out, "--no-yf"]) == 0
        rows = {r["Code"]: r for r in csv.DictReader(open(out, encoding="utf-8"))}
        events = list(csv.DictReader(open(events_out, encoding="utf-8")))

    # TOPIX +3%. 9999: 1000 -> 220.5 x 5 = 1102.5, +10.25% -> +7.0% vs TOPIX.
    assert rows["9999"]["VsTopix3M"] == "7.0", rows["9999"]
    # 8888, no split: 2000 -> 2060, +3% -> 0.0% vs TOPIX.
    assert rows["8888"]["VsTopix3M"] == "0.0", rows["8888"]
    assert [(e["Code"], e["Factor"], e["Status"]) for e in events] == [("9999", "5.0", "split")]


def test_fx_beta_series_is_restated_across_a_split():
    closes = {"2026-09-25": 7_950.0, "2026-09-28": 7_992.0,
              "2026-09-29": 523.2, "2026-09-30": 537.5}
    adj = FB.split_adjusted(closes)
    assert abs(adj["2026-09-28"] - 7_992 / 15) < 1e-9
    assert adj["2026-09-30"] == 537.5
    # The ex-date return is now the day's real move, not -93%.
    assert abs(adj["2026-09-29"] / adj["2026-09-28"] - 1) < 0.02


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  ok   {name}")
        except AssertionError as exc:
            failed += 1
            print(f"  FAIL {name}: {exc or '(assertion)'}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
