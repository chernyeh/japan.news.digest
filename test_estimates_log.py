"""Tests for estimates_log — `python test_estimates_log.py`.

The log is append-only and everything the look-back views say is read off it,
so the two things that matter are checked here: that merging never loses or
duplicates a step, and that "what the street expected going in" is read from
before the announcement, not after it.
"""

import os
import sys
import tempfile

import estimates_log as L


def row(date, metric, fy, basis, value, source="jquants", origin="filing"):
    return {"date": date, "metric": metric, "fy": fy, "basis": basis, "value": value,
            "source": source, "origin": origin, "logged_at": date}


def test_compact_drops_repeats_and_keeps_changes():
    rows = [row("2026-05-12", "operating_profit", "FY2027", "company", 2800e9),
            row("2026-08-05", "operating_profit", "FY2027", "company", 2800e9),
            row("2026-11-05", "operating_profit", "FY2027", "company", 3000e9)]
    got = L.compact(rows)
    assert [r["date"] for r in got] == ["2026-05-12", "2026-11-05"]


def test_merge_is_idempotent():
    rows = [row("2026-05-12", "eps", "FY2027", "company", 250.0)]
    once = L.merge([], rows)
    assert L.merge(once, rows) == once
    assert L.merge(once, once) == once


def test_a_typed_value_is_the_latest_word_on_its_day():
    rows = [row("2026-10-01", "eps", "FY2027", "consensus", 300.0, "typed:Koyfin", "typed"),
            row("2026-10-01", "eps", "FY2027", "consensus", 295.0, "yfinance", "collect")]
    assert L.series(rows, "eps", "FY2027", "consensus")[-1][1] == 300.0


def test_value_before_excludes_the_announcement_day():
    rows = [row("2026-07-01", "operating_profit", "FY2027", "consensus", 2900e9, "yfinance", "collect"),
            row("2026-08-05", "operating_profit", "FY2027", "consensus", 3100e9, "yfinance", "collect")]
    assert L.value_before(rows, "operating_profit", "FY2027", "consensus", "2026-08-05") == 2900e9
    assert L.value_on(rows, "operating_profit", "FY2027", "consensus", "2026-08-05") == 3100e9
    assert L.value_before(rows, "operating_profit", "FY2027", "consensus", "2026-06-01") is None


QUARTER = [
    row("2026-05-12", "operating_profit", "FY2027", "company", 3000e9),
    row("2026-07-20", "operating_profit", "FY2027", "consensus_q1", 1050e9, "typed:Koyfin", "typed"),
    row("2026-08-05", "operating_profit", "FY2027", "ytd_1q", 1170e9),
    row("2026-08-05", "operating_profit", "FY2027", "company", 3200e9),   # raised on the day
    row("2026-10-25", "operating_profit", "FY2027", "consensus_q2", 900e9, "typed:Koyfin", "typed"),
    row("2026-11-05", "operating_profit", "FY2027", "ytd_2q", 2050e9),
]


def test_results_record_scores_a_quarter_against_the_street_going_in():
    rec = L.results_record(QUARTER, metrics=("operating_profit",))
    assert [(e["fy"], e["period"]) for e in rec] == [("FY2027", "2Q"), ("FY2027", "1Q")]
    q2 = rec[0]["lines"]["operating_profit"]
    assert abs(q2["quarter"] - 880e9) < 1
    assert abs(q2["vs_street_q"] - (880 / 900 - 1)) < 1e-9
    q1 = rec[1]["lines"]["operating_profit"]
    assert abs(q1["vs_street_q"] - (1170 / 1050 - 1)) < 1e-9
    # Guidance raised from 3,000 to 3,200 on 1Q results day.
    assert abs(q1["guide_move"] - (3200 / 3000 - 1)) < 1e-9
    assert abs(q1["progress"] - 1170 / 3000) < 1e-9   # against guidance going in


def test_full_year_scored_against_frozen_consensus_and_guidance():
    rows = [row("2026-02-05", "operating_profit", "FY2026", "company", 3800e9),
            row("2026-04-30", "operating_profit", "FY2026", "consensus", 3700e9, "yfinance", "collect"),
            row("2026-05-12", "operating_profit", "FY2026", "actual", 3766e9),
            row("2026-05-12", "operating_profit", "FY2026", "consensus", 3766e9, "yfinance", "collect"),
            row("2026-05-12", "operating_profit", "FY2027", "company", 3000e9)]
    e = L.results_record(rows, metrics=("operating_profit",))[0]
    line = e["lines"]["operating_profit"]
    assert e["period"] == "FY"
    assert abs(line["vs_street_fy"] - (3766 / 3700 - 1)) < 1e-9
    assert abs(line["vs_guide_fy"] - (3766 / 3800 - 1)) < 1e-9
    assert line["guide_move"] is None          # next year's first guidance is new, not a revision
    assert line["guide_after"] == 3000e9


def test_dividend_changes_lists_hikes_newest_first():
    rows = [row("2026-05-12", "dps", "FY2027", "company", 90.0),
            row("2026-11-05", "dps", "FY2027", "company", 100.0)]
    (d, fy, old, new, chg), = L.dividend_changes(rows)
    assert (d, fy, old, new) == ("2026-11-05", "FY2027", 90.0, 100.0)
    assert abs(chg - 1 / 9) < 1e-9


def test_local_write_only_grows_on_change():
    with tempfile.TemporaryDirectory() as d:
        rows = [row("2026-05-12", "eps", "FY2027", "company", 250.0)]
        assert L.write_local("9999", rows, d) == 1
        assert L.write_local("9999", rows, d) == 0
        assert L.write_local("9999", [row("2026-08-05", "eps", "FY2027", "company", 260.0)], d) == 1
        assert len(L.read_local("9999", d)) == 2


def test_rows_from_manual_keeps_the_source_and_origin():
    got = L.rows_from_manual({"eps|FY2027|consensus_q2": {"value": 80.0, "source": "typed:QUICK",
                                                          "as_of": "2026-10-20"}}, "2026-10-20")
    assert got[0]["basis"] == "consensus_q2" and got[0]["origin"] == "typed"
    assert got[0]["source"] == "typed:QUICK"


def test_summarize_latest_reports_the_newest_period():
    s = L.summarize_latest(QUARTER)
    assert (s["fy"], s["period"]) == ("FY2027", "2Q")
    assert abs(s["op_vs_street"] - (880 / 900 - 1)) < 1e-9



# ── Collector: every filing step, dated by its own disclosure ────────────

import collect_consensus as C


def _jq(per, fy_end, disc, nxt="", **kw):
    d = {"Code": "7203", "CurPerType": per, "CurFYEn": fy_end, "DiscDate": disc,
         "NxtFYEn": nxt, "DocType": f"{per}FinancialStatements_Consolidated_JP"}
    d.update({k: str(v) for k, v in kw.items()})
    return d


_FILINGS = [
    _jq("FY", "2026-03-31", "2026-05-13", nxt="2027-03-31", OP=3_766_200, Sales=50_685_000,
        NxFOP=3_000_000),
    _jq("1Q", "2027-03-31", "2026-08-05", OP=1_170_000, FOP=3_000_000),
    _jq("2Q", "2027-03-31", "2026-11-06", OP=2_050_000, FOP=3_400_000),
]


def test_filing_rows_keep_every_guidance_step_with_its_date():
    rows = L.compact(C.filing_log_rows(_FILINGS, "2026-11-07"))
    steps = L.series(rows, "operating_profit", "FY2027", "company")
    assert [(d, v) for d, v, _ in steps] == [("2026-05-13", 3_000_000.0),
                                            ("2026-11-06", 3_400_000.0)]


def test_filing_rows_date_each_reported_period():
    rows = L.compact(C.filing_log_rows(_FILINGS, "2026-11-07"))
    dates = L.results_dates(rows)
    assert dates[("FY2027", "1Q")] == "2026-08-05"
    assert dates[("FY2027", "2Q")] == "2026-11-06"
    assert dates[("FY2026", "FY")] == "2026-05-13"


# ── Capital events ───────────────────────────────────────────────────────

import capital_events as CE


def test_titles_classify_into_events():
    assert CE.classify_title("自己株式の取得に係る事項の決定に関するお知らせ") == "Buyback announced"
    assert CE.classify_title("自己株式の取得結果及び取得終了に関するお知らせ") == "Buyback completed"
    assert CE.classify_title("自己株式立会外買付取引(ToSTNeT-3)による自己株式の取得結果"
                             "及び取得終了並びに自己株式の消却に関するお知らせ") == "Share cancellation"
    assert CE.classify_title("配当予想の修正（増配）に関するお知らせ") == "Dividend change"
    assert CE.classify_title("決算短信") == ""


def test_pct_of_shares_excludes_treasury():
    assert abs(CE.pct_of_shares(10e6, 1_010e6, 10e6) - 0.01) < 1e-12
    assert CE.pct_of_shares(10e6, None) is None


def test_suggestions_skip_logged_notices():
    tdnet = [{"Code": "72030", "Title": "自己株式の取得に係る事項の決定に関するお知らせ",
              "PubDateTime": "2026-11-05 15:00", "DocUrl": "u1"},
             {"Code": "72030", "Title": "決算短信", "PubDateTime": "2026-11-05 15:00"},
             {"Code": "6758", "Title": "自己株式の取得に係る事項の決定に関するお知らせ",
              "PubDateTime": "2026-11-05 15:00"}]
    got = CE.suggestions(tdnet, "7203", [])
    assert [(g["date"], g["type"]) for g in got] == [("2026-11-05", "Buyback announced")]
    assert CE.suggestions(tdnet, "7203", [{"date": "2026-11-05", "type": "Buyback announced"}]) == []


def test_apply_upserts_by_id_and_removes():
    d = CE.apply({}, "7203", [{"id": "a", "date": "2026-11-05"}, {"id": "b", "date": "2026-08-01"}])
    assert [e["id"] for e in d["7203"]] == ["a", "b"]
    d = CE.apply(d, "7203", [{"id": "b", "date": "2026-08-01", "note": "x"}], ["a"])
    assert d["7203"] == [{"id": "b", "date": "2026-08-01", "note": "x"}]
    assert "7203" not in CE.apply(d, "7203", remove_ids=["b"])


def test_share_count_trend_reads_filing_rows():
    rows = [row("2026-05-13", "shares", "FY2026", "actual", 15_794e6),
            row("2026-05-13", "treasury", "FY2026", "actual", 2_700e6),
            row("2026-08-05", "shares", "FY2027", "ytd_1q", 15_794e6),
            row("2026-08-05", "treasury", "FY2027", "ytd_1q", 2_762e6)]
    assert CE.share_count_trend(rows)[-1] == ("2026-08-05", 15_794e6, 2_762e6)


# ── Results-day planner ──────────────────────────────────────────────────

import results_day as RD

_CUR = {("operating_profit", "FY2027", "company"): {"value": 3000e9},
        ("dps", "FY2027", "company"): {"value": 100.0},
        ("operating_profit", "FY2027", "ytd_1q"): {"value": 1170e9}}


def test_results_day_saves_ytd_guidance_and_a_dividend_hike_in_one_go():
    entries, events, _ = RD.plan(
        "7203", "FY2027", "2Q", False,
        reported={"operating_profit": 2050, "net_sales": 24500},
        guidance={"operating_profit": 3400, "dps": 110, "net_sales": None},
        current=_CUR, date="2026-11-05", source="Company data")
    assert entries["operating_profit|FY2027|ytd_2q"]["value"] == 2050e9
    assert entries["net_sales|FY2027|ytd_2q"]["source"] == "typed:Company data"
    assert entries["operating_profit|FY2027|company"]["value"] == 3400e9
    assert entries["dps|FY2027|company"]["value"] == 110.0
    (ev,) = events
    assert ev["type"] == "Dividend change" and (ev["dps_old"], ev["dps_new"]) == (100.0, 110.0)


def test_results_day_skips_figures_already_held():
    entries, events, _ = RD.plan("7203", "FY2027", "1Q", False,
                                 reported={"operating_profit": 1170},
                                 guidance={"operating_profit": 3000, "dps": 100},
                                 current=_CUR, date="2026-08-05")
    assert entries == {} and events == []


def test_single_quarter_and_full_year_bases():
    assert RD.reported_basis("2Q", True) == "actual_q2"
    assert RD.reported_basis("FY", False) == "actual"
    assert RD.guidance_fy("FY2026", "FY") == "FY2027"
    assert RD.guidance_fy("FY2027", "2Q") == "FY2027"


def test_results_day_buyback_event_carries_its_size():
    _, events, _ = RD.plan("7203", "FY2027", "2Q", False, {}, {}, _CUR, "2026-11-05",
                           buyback={"amount_bn": 500, "shares_m": 150, "until": "2027-03-31"},
                           shares_out=15_794e6, treasury=2_794e6)
    (ev,) = events
    assert ev["type"] == "Buyback announced" and ev["amount_jpy"] == 500e9
    assert abs(ev["pct_shares"] - 150e6 / 13_000e6) < 1e-12
    assert ev["id"] == "7203-2026-11-05-buyback-announced"

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
