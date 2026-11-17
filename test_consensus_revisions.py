"""Tests for consensus_revisions.py and flag_dismissals.py — `pytest test_consensus_revisions.py` (no network)."""

from datetime import date

import consensus_revisions as CR
import flag_dismissals as FD
import select_refresh_codes as SR
import watchlist_grid as G

TODAY = date(2026, 11, 17)


def _r(d, m, fy, v, src="yfinance"):
    return {"date": d, "metric": m, "fy": fy, "basis": "consensus", "value": v, "source": src}


def test_post_result_cut_is_measured_from_the_results_day_close():
    rows = [_r("2026-10-20", "eps", "FY2026", 100.0),
            _r("2026-11-13", "eps", "FY2026", 99.0),       # results day itself: still "before"
            _r("2026-11-16", "eps", "FY2026", 90.0),
            _r("2026-10-20", "eps", "FY2027", 100.0),
            _r("2026-11-16", "eps", "FY2027", 104.0)]
    row = {"fy1": "FY2026", "result_date": "2026-11-13", "result_period": "3Q FY26"}
    f = CR.revision_flag(row, rows, TODAY, rel_since=lambda d: -5.0)
    assert f["short"] == "Cons. mixed" and f["severity"] == "act"      # −9.1% ≥ 7%
    assert f["text"].startswith("Since 3Q FY26 (13 Nov): cons. EPS FY26 -9.1%, FY27 +4.0% (Yahoo)")
    assert "stock -5.0% vs TOPIX" in f["text"] and "near yr cut, outer yr raised" in f["text"]
    assert f["key"] == "2026-11-13|Cons. mixed|act" and "clear_days" not in f


def test_rolling_upgrade_into_results_reads_as_bar_raised():
    rows = [_r("2026-10-01", "operating_profit", "FY2027", 10e9, "typed:Koyfin"),
            _r("2026-11-10", "operating_profit", "FY2027", 10.5e9, "typed:Koyfin")]
    row = {"fy1": "FY2027", "result_date": "2026-08-01", "next_result": "2026-11-27"}
    f = CR.revision_flag(row, rows, TODAY, rel_since=lambda d: 6.0)
    assert f["short"] == "Cons. raised" and f["severity"] == "warn"
    assert "30d: cons. OP FY27 +5.0% (Koyfin)" in f["text"]
    assert "¥10.0bn→10.5bn" in f["text"] and "next result 27 Nov (10d)" in f["text"]
    assert "bar ↑ into results" in f["text"] and f["clear_days"] == CR.ROLLING_CLEAR_DAYS


def test_upgrade_not_rewarded_is_rule_10():
    rows = [_r("2026-10-01", "eps", "FY2027", 100.0), _r("2026-11-10", "eps", "FY2027", 105.0)]
    f = CR.revision_flag({"fy1": "FY2027"}, rows, TODAY, rel_since=lambda d: -1.0)
    assert "upgrade not rewarded" in f["text"]


def test_small_moves_and_source_switches_do_not_flag():
    small = [_r("2026-10-01", "eps", "FY2027", 100.0), _r("2026-11-10", "eps", "FY2027", 102.0)]
    assert CR.revision_flag({"fy1": "FY2027"}, small, TODAY) is None
    # Yahoo says 100, then you type a Koyfin 110: two sources, not a revision.
    switch = [_r("2026-10-01", "eps", "FY2027", 100.0),
              _r("2026-11-10", "eps", "FY2027", 110.0, "typed:Koyfin")]
    assert CR.revision_flag({"fy1": "FY2027"}, switch, TODAY) is None


def test_ambiguous_backfill_date_is_skipped():
    # The 27 Aug backfill logged two values for one figure on one day.
    rows = [_r("2026-08-27", "eps", "FY2027", 73.3), _r("2026-08-27", "eps", "FY2027", 120.6),
            _r("2026-09-19", "eps", "FY2027", 71.8)]
    assert CR.clean_series(rows, "eps", "FY2027", "yfinance") == [("2026-09-19", 71.8, "yfinance")]


def test_change_log_lists_every_move_newest_first():
    rows = [_r("2026-09-01", "eps", "FY2027", 100.0), _r("2026-10-01", "eps", "FY2027", 95.0),
            _r("2026-11-01", "eps", "FY2027", 96.0)]
    log = CR.change_log(rows)
    assert [(e["date"], round(e["chg"], 4)) for e in log] == [("2026-11-01", 0.0105),
                                                              ("2026-10-01", -0.05)]
    assert len(CR.change_log(rows, min_chg=0.02)) == 1


def test_dismissal_hides_until_trigger_changes_and_is_never_deleted():
    flag = {"kind": "cons_rev", "key": "roll|Cons. cut|warn", "short": "Cons. cut",
            "severity": "warn", "text": "30d: …", "clear_days": 30}
    e = FD.make_entry("6526", "Socionext", flag, TODAY, " not thesis-relevant ")
    assert e["until"] == "2026-12-17" and e["note"] == "not thesis-relevant"
    data = FD.apply({}, "6526", [e])
    hidden = FD.hidden_ids(data, TODAY)
    shown, cleared = FD.split("6526", [flag, {**flag, "key": "roll|Cons. cut|act"}], hidden)
    assert len(shown) == 1 and shown[0]["key"].endswith("act") and len(cleared) == 1
    assert not FD.hidden_ids(data, date(2026, 12, 18))                 # lapsed
    data = FD.apply(data, "6526", [FD.restored_entry(e, TODAY)])
    assert not FD.hidden_ids(data, TODAY) and len(FD.log(data)) == 1   # restored, still logged
    # Cleared again later: a second log entry, not an overwrite.
    data = FD.apply(data, "6526", [FD.make_entry("6526", "Socionext", flag, date(2026, 11, 20))])
    assert len(FD.log(data)) == 2


def test_rel_since_measures_against_topix_from_a_date():
    st = [("2026-10-16", 100.0), ("2026-11-13", 110.0)]
    tp = [("2026-10-16", 1000.0), ("2026-11-13", 1050.0)]
    assert G.rel_since(st, tp, "2026-10-17") == round((1.10 / 1.05 - 1) * 100, 1)
    assert G.rel_since(st, tp, "2026-09-01") is None                   # no close near the start


def test_refresh_all_watched_puts_filers_first():
    assert SR.with_all_watched(["6501"], ["9983", "6501", "2802"]) == ["6501", "2802", "9983"]
    assert len(SR.with_all_watched([], [str(i) for i in range(1000, 1100)])) == SR.WATCHED_CAP


def test_short_flag_text_style():
    r = {"code": "1", "price": 1000, "op_vs_street": 2.379, "reaction": -12.3,
         "reaction_settled": True, "result_date": "2026-11-06", "result_period": "2Q FY27"}
    f = [x for x in G.decision_flags(r, TODAY) if x["kind"] == "beat_fell"][0]
    assert f["text"] == ("2Q FY27 (6 Nov): beat cons. +237.9% on OP, but stock -12.3% vs TOPIX"
                         " · priced in / over-owned? (rule 10)")
