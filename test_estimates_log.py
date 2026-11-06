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
    # The monthly progress report is the commonest buyback notice: not a completion.
    assert CE.classify_title("自己株式の取得状況に関するお知らせ") == "Buyback progress"
    assert CE.classify_title("自己株式の取得状況（途中経過）に関するお知らせ") == "Buyback progress"
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


# ── Forward reads ────────────────────────────────────────────────────────

import forward as FW


def test_buyback_running_past_next_results_says_no_new_one_likely():
    ev = [{"date": "2026-05-08", "type": "Buyback announced", "amount_jpy": 1000e9,
           "until": "2027-03-31"},
          {"date": "2026-10-05", "type": "Buyback progress", "cum_amount_jpy": 600e9}]
    r = FW.buyback_status(ev, "2026-11-02", "2026-11-05")
    assert r["state"] == "running" and abs(r["pct_done"] - 0.6) < 1e-9
    assert r["runs_past"] and "unlikely" in r["detail"]


def test_buyback_finished_leaves_room_for_a_new_one():
    ev = [{"date": "2025-05-08", "type": "Buyback announced", "amount_jpy": 500e9,
           "until": "2026-03-31"}]
    r = FW.buyback_status(ev, "2026-11-02", "2026-11-05")
    assert r["state"] == "finished" and "possible" in r["detail"]
    assert FW.buyback_status([], "2026-11-02") is None


def _hist(ytd1_now, new_guide):
    """Two prior years where 1Q is 25% of the year, and a 1Q that beats."""
    rows = []
    for fy, q1, full, d in (("FY2025", 250e9, 1000e9, "2024-08-05"),
                            ("FY2026", 275e9, 1100e9, "2025-08-05")):
        rows += [row(d, "operating_profit", fy, "ytd_1q", q1),
                 row(f"{int(d[:4]) + 1}-05-10", "operating_profit", fy, "actual", full)]
    rows += [row("2026-05-10", "operating_profit", "FY2027", "company", 1200e9),
             row("2026-08-05", "operating_profit", "FY2027", "ytd_1q", ytd1_now),
             row("2026-08-05", "operating_profit", "FY2027", "company", new_guide)]
    return rows


def test_raise_that_only_banks_the_beat_is_flagged():
    # Plan for 1Q = 1,200 x 25% = 300; actual 400 beats by 100; raise 100.
    r = FW.raise_quality(_hist(400e9, 1300e9), "FY2027")
    assert abs(r["beat"] - 100e9) < 1 and abs(r["raise"] - 100e9) < 1
    assert r["title"].startswith("Raise only banks the beat") and r["tone"] == "warn"
    # Rest of year 900 vs last year's 825: +9% against 1Q's +45%.
    assert r["rest_yoy"] < r["ytd_yoy"]


def test_raise_beyond_the_beat_is_good():
    r = FW.raise_quality(_hist(320e9, 1400e9), "FY2027")
    assert r["title"].startswith("Raised beyond the beat")


def test_beat_without_a_raise_is_a_cut_to_the_rest_of_the_year():
    r = FW.raise_quality(_hist(400e9, 1200e9), "FY2027")
    assert r["title"] == "Beat, guidance not raised" and r["tone"] == "warn"


def test_guidance_stance_uses_the_first_line_both_sides_have():
    r = FW.guidance_stance({"operating_profit": 3400e9, "eps": 270.0},
                           {"eps": 290.0}, {"raised": 4, "cut": 1, "years": 5, "thin": False})
    assert r["metric"] == "eps" and r["tone"] == "good" and "above" in r["title"]
    assert FW.guidance_stance({}, {}) is None


# ── One-offs ─────────────────────────────────────────────────────────────

import one_offs as OO
import notice_reader as NR


def test_one_off_lands_on_the_right_lines():
    e = [{"fy": "FY2027", "period": "2Q", "line": "extraordinary", "amount_jpy": 100e9},
         {"fy": "FY2027", "period": "2Q", "line": "operating", "amount_jpy": -20e9,
          "np_amount_jpy": -14e9}]
    assert OO.impact(e, "operating_profit", "FY2027", ("2Q",)) == -20e9
    assert OO.impact(e, "ordinary_profit", "FY2027", ("2Q",)) == -20e9
    # 100 x (1 - 30%) assumed tax, plus the stated -14 after tax
    assert abs(OO.impact(e, "net_profit", "FY2027", ("2Q",)) - 56e9) < 1
    assert OO.impact(e, "operating_profit", "FY2027", ("1Q",)) == 0


def test_tdnet_one_off_notices_are_placed_in_the_result_they_hit():
    rows = [{"Code": "7203", "Title": "特別利益の計上に関するお知らせ",
             "PubDateTime": "2026-10-28 15:00", "DocUrl": "u"},
            {"Code": "7203", "Title": "決算短信", "PubDateTime": "2026-11-05 15:00"}]
    got = OO.tdnet_candidates(rows, "7203", [], {("FY2027", "2Q"): "2026-11-05",
                                                 ("FY2027", "1Q"): "2026-08-04"})
    assert [(g["kind"], g["fy"], g["period"]) for g in got] == [("Extraordinary gain", "FY2027", "2Q")]
    assert OO.tdnet_candidates(rows, "7203", [{"tdnet_title": "特別利益の計上に関するお知らせ"}]) == []


def test_numeric_flags_catch_a_below_the_line_quarter():
    m = {}
    for fy in ("FY2025", "FY2026"):
        for n in (1, 2, 3):
            m[("ordinary_profit", fy, f"ytd_{n}q")] = 100.0 * n
            m[("net_profit", fy, f"ytd_{n}q")] = 70.0 * n
            m[("net_sales", fy, f"ytd_{n}q")] = 1000.0 * n
            m[("operating_profit", fy, f"ytd_{n}q")] = 100.0 * n
    # FY2026 2Q: net profit 150 on ordinary 100 — a gain below the line.
    m[("net_profit", "FY2026", "ytd_2q")] = 70.0 + 150.0
    m[("net_profit", "FY2026", "ytd_3q")] = 70.0 + 150.0 + 70.0
    flags = OO.numeric_flags(lambda a, b, c: m.get((a, b, c)), ["FY2025", "FY2026"])
    assert any(f["fy"] == "FY2026" and f["period"] == "2Q" and f["kind"] == "below the line"
               for f in flags)


def test_notice_reading_becomes_a_progress_event():
    r = NR.normalise({"kind": "buyback_progress",
                      "buyback": {"cumulative_amount_jpy": 6e11, "cumulative_shares": "1.2e8",
                                  "as_of": "2026-10-31", "authorised_shares": None,
                                  "authorised_amount_jpy": None, "period_start": "",
                                  "period_end": ""},
                      "one_offs": [], "notes": ""})
    ev = NR.to_event(r, "7203", "2026-11-02", "自己株式の取得状況に関するお知らせ", [])
    assert ev["type"] == "Buyback progress" and ev["cum_amount_jpy"] == 6e11
    assert ev["cum_shares"] == 1.2e8 and ev["as_of"] == "2026-10-31"
    assert NR.to_event(NR.normalise({"kind": "one_offs"}), "7203", "d", "t", []) is None


def test_notice_one_offs_are_normalised():
    r = NR.normalise({"kind": "one_offs", "buyback": {}, "notes": "",
                      "one_offs": [{"item": "Gain on sale of securities", "amount_jpy": 5e9,
                                    "line": "weird", "period": "2Q FY3/27", "consolidated": True},
                                   {"item": "no amount", "amount_jpy": None}]})
    assert len(r["one_offs"]) == 1 and r["one_offs"][0]["line"] == "extraordinary"


def test_read_notice_calls_the_model_with_the_pdf_and_parses_the_reply():
    import json as _json
    import types as _types
    seen = {}

    class _Msgs:
        def create(self, **kw):
            seen.update(kw)
            reply = {"kind": "buyback_authorisation", "notes": "", "one_offs": [],
                     "buyback": {"authorised_shares": 2e8, "authorised_amount_jpy": 1e12,
                                 "period_start": "2026-05-09", "period_end": "2027-03-31",
                                 "cumulative_shares": None, "cumulative_amount_jpy": None,
                                 "as_of": ""}}
            return _types.SimpleNamespace(stop_reason="end_turn", content=[
                _types.SimpleNamespace(type="text", text=_json.dumps(reply))])

    fake = _types.ModuleType("anthropic")
    fake.Anthropic = lambda api_key=None: _types.SimpleNamespace(messages=_Msgs())
    fake.BadRequestError = type("BadRequestError", (Exception,), {})
    real = sys.modules.get("anthropic")
    sys.modules["anthropic"] = fake
    try:
        r = NR.read_notice(b"%PDF-1.7 fake", "key", "7203", "取得に係る事項の決定")
    finally:
        if real is not None:
            sys.modules["anthropic"] = real
        else:
            del sys.modules["anthropic"]
    assert r["kind"] == "buyback_authorisation"
    assert r["buyback"]["authorised_amount_jpy"] == 1e12 and r["buyback"]["period_end"] == "2027-03-31"
    assert seen["messages"][0]["content"][0]["type"] == "document"


# ── Weekly price history ─────────────────────────────────────────────────

import compute_price_history as PH


def test_weekly_condense_keeps_the_last_trading_day():
    got = PH.condense([("7203", "2026-10-26", "3000"), ("7203", "2026-10-30", "3100"),
                       ("7203", "2026-10-28", "2900"), ("7203", "2026-11-02", "3050"),
                       ("7203", "2026-11-03", "bad")])
    assert got[("7203", "2026-W44")] == ("2026-10-30", 3100.0)
    assert got[("7203", "2026-W45")] == ("2026-11-02", 3050.0)


def test_pe_history_rolls_to_the_next_year_once_the_full_year_is_reported():
    rows = [row("2025-05-08", "eps", "FY2026", "company", 300.0),
            row("2026-05-08", "eps", "FY2027", "company", 250.0),
            row("2026-05-08", "net_sales", "FY2026", "actual", 1.0)]
    got = FW.pe_history([("2026-04-24", 3000.0), ("2026-05-15", 3000.0)], rows)
    assert got == [("2026-04-24", 10.0, "FY2026"), ("2026-05-15", 12.0, "FY2027")]


def test_one_off_bubble_text_names_the_item_and_the_note():
    e = [{"fy": "FY2027", "period": "2Q", "line": "operating", "item": "Restructuring",
          "note": "Closure of the Shiga plant", "amount_jpy": -15e9}]
    assert OO.items_for(e, "operating_profit", "FY2027", ("2Q",)) == e
    assert OO.items_for(e, "operating_profit", "FY2027", ("1Q",)) == []
    txt = OO.describe(e[0])
    assert "Restructuring" in txt and "-15.0bn" in txt and "Shiga" in txt
    assert "assumed 30% tax" in OO.describe(e[0], "net_profit")


def test_a_quarter_estimate_typed_on_results_day_still_counts():
    rows = [row("2026-08-04", "operating_profit", "FY2027", "ytd_1q", 49.1e9),
            row("2026-11-02", "operating_profit", "FY2027", "consensus_q2", 37.45e9,
                "typed:Koyfin", "typed"),
            row("2026-11-02", "operating_profit", "FY2027", "ytd_2q", 65.6e9,
                "typed:Company data", "typed")]
    q2 = L.results_record(rows, metrics=("operating_profit",))[0]["lines"]["operating_profit"]
    assert abs(q2["street_q"] - 37.45e9) < 1 and q2["vs_street_q"] < -0.5


def test_going_in_corrections_are_dated_the_day_before():
    entries, log = RD.plan_going_in(
        "FY2027", "2Q", typed={"operating_profit": {"street": 40.0, "guide": 160.0, "h1": 70.0}},
        shown={"operating_profit": {"street": 37.45, "guide": 160.0, "h1": None}},
        guidance={}, date="2026-11-02", source="Koyfin")
    assert set(entries) == {"operating_profit|FY2027|consensus_q2",
                            "operating_profit|FY2027|company_h1"}
    assert entries["operating_profit|FY2027|consensus_q2"]["as_of"] == "2026-11-01"
    assert entries["operating_profit|FY2027|consensus_q2"]["value"] == 40e9
    assert log == []


def test_going_in_guidance_with_new_guidance_goes_to_the_log_only():
    entries, log = RD.plan_going_in(
        "FY2027", "2Q", typed={"operating_profit": {"guide": 150.0}},
        shown={"operating_profit": {"guide": 160.0}},
        guidance={"operating_profit": 170.0}, date="2026-11-02")
    assert entries == {}
    assert log[0]["basis"] == "company" and log[0]["date"] == "2026-11-01"
    assert RD.going_in_basis("street", "FY") == "consensus"


def test_last_reported_quarter_falls_back_to_last_year():
    m = {("operating_profit", "FY2026", "ytd_3q"): 70.0, ("operating_profit", "FY2026", "ytd_2q"): 40.0,
         ("operating_profit", "FY2026", "ytd_1q"): 18.0, ("operating_profit", "FY2026", "actual"): 118.0,
         ("operating_profit", "FY2027", "ytd_1q"): 49.0, ("operating_profit", "FY2027", "ytd_2q"): 65.6}
    g = lambda a, b, c: m.get((a, b, c))
    fy, n, v = FW.last_reported_quarter(g, "FY2027")
    assert (fy, n) == ("FY2027", 2) and abs(v - 16.6) < 1e-9
    assert FW.last_reported_quarter(g, "FY2028")[:2] == ("FY2027", 2)


def test_street_first_half_is_reported_1q_plus_street_2q():
    import fundamentals as F
    m = {("operating_profit", "FY2027", "ytd_1q"): 49.1, ("operating_profit", "FY2027", "consensus_q2"): 37.45}
    assert abs(F.quarter_street(lambda a, b, c: m.get((a, b, c)), "operating_profit",
                                "FY2027", "ytd_2q") - 86.55) < 1e-9

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
