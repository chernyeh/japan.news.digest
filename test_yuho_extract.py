"""Tests for yuho_extract and holders_extract: the EDINET CSV parse, the
section pick, the HTML flattening, the normalising of model output and the
derived readings. The fixtures mirror EDINET's own CSV layout (UTF-16,
tab-separated, nine columns) so the parse is exercised the way it runs."""

import io
import zipfile

import yuho_extract as YX
import holders_extract as HX

HEADER = "要素ID\t項目名\tコンテキストID\t相対年度\t連結・個別\t期間・時点\tユニットID\t単位\t値"


def _csv(rows: list) -> str:
    return HEADER + "\n" + "\n".join("\t".join(r) for r in rows) + "\n"


def _zip(files: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, text in files.items():
            z.writestr(name, text.encode("utf-16"))
    return buf.getvalue()


OFFICERS_HTML = (
    "<table><tr><th>役職名</th><th>氏名</th><th>生年月日</th><th>略歴</th><th>所有株式数（千株）</th></tr>"
    "<tr><td>代表取締役社長</td><td>服部 真二</td><td>1953年5月1日</td>"
    "<td>1984年4月 当社入社<br/>2010年6月 取締役就任</td><td>1,200</td></tr>"
    "<tr><td>社外取締役</td><td>山田 花子</td><td>1960年1月2日</td>"
    "<td>1983年4月 株式会社三菱銀行入行</td><td>0</td></tr></table>"
)

ROWS = [
    ("jpcrp_cor:NumberOfEmployees", "従業員数", "CurrentYearInstant", "当期", "連結", "時点", "pure", "人", "12,000"),
    ("jpcrp_cor:NumberOfEmployees", "従業員数", "Prior1YearInstant", "前期", "連結", "時点", "pure", "人", "11,500"),
    ("jpcrp_cor:NumberOfEmployees", "従業員数", "CurrentYearInstant_NonConsolidatedMember", "当期", "個別", "時点", "pure", "人", "2,100"),
    ("jpcrp_cor:AverageAnnualSalaryInformationAboutReportingCompanyInformationAboutEmployees",
     "平均年間給与", "CurrentYearInstant_NonConsolidatedMember", "当期", "個別", "時点", "JPY", "円", "7,650,000"),
    ("jpcrp_cor:AverageAgeYearsInformationAboutReportingCompanyInformationAboutEmployees",
     "平均年齢", "CurrentYearInstant_NonConsolidatedMember", "当期", "個別", "時点", "pure", "歳", "43.2"),
    ("jpcrp_cor:NumberOfEmployees", "従業員数", "CurrentYearInstant_WatchesSegmentMember", "当期", "連結", "時点", "pure", "人", "4,000"),
    ("jpcrp_cor:ShareholdingRatioForeignCorporationsEtc", "外国法人等 所有株式数の割合", "CurrentYearInstant", "当期", "個別", "時点", "pure", "%", "22.4"),
    ("jpcrp_cor:NumberOfIssuesSpecifiedInvestmentShares", "特定投資株式 銘柄数", "CurrentYearInstant", "当期", "個別", "時点", "pure", "銘柄", "31"),
    ("jpcrp_cor:TotalAmountOnBalanceSheetSpecifiedInvestmentShares", "特定投資株式 貸借対照表計上額の合計額", "CurrentYearInstant", "当期", "個別", "時点", "JPY", "百万円", "45,210"),
    ("jpcrp_cor:InformationAboutOfficersTextBlock", "役員の状況 [テキストブロック]", "FilingDateInstant", "", "", "", "", "", OFFICERS_HTML),
    ("jpcrp_cor:MajorShareholdersTextBlock", "大株主の状況 [テキストブロック]", "FilingDateInstant", "", "", "", "", "", "<p>holders</p>"),
    ("jpcrp_cor:SomethingElseTextBlock", "設備の新設、除却等の計画 [テキストブロック]", "FilingDateInstant", "", "", "", "", "", "<p>capex plan</p>"),
    ("jpcrp_cor:SomethingElseTextBlock", "設備の新設、除却等の計画 [テキストブロック]", "Prior1YearInstant", "前期", "", "", "", "", "<p>old plan</p>"),
]


def test_parse_and_pick_facts_from_an_edinet_style_csv_zip():
    data = _zip({"XBRL_TO_CSV/jpcrp030000-asr-001_E02280-000_2026-03-31_01_2026-06-24.csv": _csv(ROWS),
                 "XBRL_TO_CSV/jpaud-aar-cn-001.csv": _csv([ROWS[0]])})
    prepared = YX.prepare(data)
    f = prepared["facts"]
    assert f["employees"]["consolidated"]["value"] == 12000
    assert f["employees"]["parent"]["value"] == 2100
    assert f["avg_salary"]["parent"]["value"] == 7650000
    assert f["avg_age"]["parent"]["value"] == 43.2
    assert f["foreign_pct"]["parent"]["value"] == 22.4
    assert f["cross_issues"]["parent"]["value"] == 31
    assert f["cross_book_value"]["parent"]["unit"] == "百万円"
    # The segment row and the prior year are not the company total.
    assert prepared["n_rows"] == len(ROWS)
    assert prepared["estimate"]["usd"] > 0


def test_text_blocks_are_picked_by_id_or_label_current_year_only():
    rows = YX.parse_csv_text(_csv(ROWS))
    b = YX.text_blocks(rows)
    assert set(b) == {"officers", "major_shareholders", "capex_plan"}
    assert b["capex_plan"] == "<p>capex plan</p>"


def test_html_flattening_keeps_table_rows_and_cells():
    t = YX.html_to_text(OFFICERS_HTML)
    lines = t.split("\n")
    assert lines[0].startswith("役職名 | 氏名")
    assert "代表取締役社長 | 服部 真二 | 1953年5月1日 | 1984年4月 当社入社" in lines[1]
    assert "2010年6月 取締役就任 | 1,200" in lines[1]


def test_officers_normalise_and_board_stats_and_bias_read():
    raw = {"as_of": "2026年6月24日現在", "officers": [
        {"name": "服部 真二", "title": "代表取締役社長", "role_kind": "representative_director",
         "outside": False, "age": 73, "birth_year": 1953, "joined_company_year": 1984,
         "director_since_year": 2010, "is_ceo": True, "is_chair": False, "is_cfo": False,
         "career_summary": "Joined 1984; sales; overseas.", "background_tags": ["sales_marketing", "overseas", "bogus"],
         "shares_held": 1200000, "term_end": ""},
        {"name": "山田 花子", "title": "社外取締役", "role_kind": "outside_director", "outside": True,
         "age": 66, "birth_year": 1960, "joined_company_year": 0, "director_since_year": 2020,
         "is_ceo": False, "is_chair": False, "is_cfo": False, "career_summary": "Ex-bank.",
         "background_tags": ["ex_bank"], "shares_held": 0, "term_end": ""},
        {"name": "鈴木 一郎", "title": "執行役員", "role_kind": "executive_officer", "outside": False,
         "age": 55, "birth_year": 0, "joined_company_year": 1995, "director_since_year": 0,
         "is_ceo": False, "is_chair": False, "is_cfo": True, "career_summary": "", "background_tags": [],
         "shares_held": 10, "term_end": ""},
        {"name": "", "title": "x"},
    ], "notes": "n"}
    sec = YX.normalise("officers", raw)
    assert len(sec["officers"]) == 3
    assert sec["officers"][0]["background_tags"] == ["sales_marketing", "overseas"]
    bs = YX.board_stats(sec["officers"], 2026)
    assert bs["size"] == 2 and bs["outside"] == 1 and bs["ex_bank"] == 1 and bs["lifers"] == 1
    entry = {"fy": "FY2026", "sections": {"officers": sec}}
    read = YX.bias_read(entry, price=5000)
    assert "CEO 服部 真二" in read and "42 years at the company" in read
    assert "¥6,000m" in read and "top-line and expansion" in read
    assert "board of 2, 1 outside (50%)" in read


def test_cross_holdings_and_capex_reads():
    entry = {"fy": "FY2026", "facts": {}, "sections": {
        "cross_holdings": YX.normalise("cross_holdings", {
            "policy_summary": "p", "total_issues": 31, "total_book_value_jpy": 45.21e9,
            "issues": [{"name": "A", "shares": 1, "book_value_jpy": 1e9, "purpose": "trade",
                        "purpose_kind": "business_relationship", "holds_our_shares": True, "change": "unchanged"},
                       {"name": "B", "shares": 1, "book_value_jpy": 3e9, "purpose": "x",
                        "purpose_kind": "quantified_effect_stated", "holds_our_shares": False, "change": "decreased"}],
            "reduction_stated": True, "notes": ""}),
        "capex": YX.normalise("capex", {
            "actual_total_jpy": 0, "actual_by_segment": [{"segment": "Watches", "amount_jpy": 8e9, "what": "lines"}],
            "plan": [{"site": "Morioka", "location": "Iwate", "segment": "Watches", "description": "new line",
                      "planned_total_jpy": 12e9, "paid_to_date_jpy": 2e9, "funding": "自己資金",
                      "start": "2026年4月", "finish": "2027年9月", "capacity_note": ""}],
            "plan_total_jpy": 0, "plan_paid_jpy": 0, "funding_summary": "own funds", "notes": ""}),
    }}
    ch = YX.cross_holdings_read(entry, market_cap=400e9, equity=200e9)
    assert ch["issues"] == 31 and abs(ch["pct_mcap"] - 0.113) < 0.001 and ch["mutual"] == 1
    assert ch["vague_purpose"] == 1 and ch["reduction_stated"]
    # Largest first after normalising.
    assert entry["sections"]["cross_holdings"]["issues"][0]["name"] == "B"
    cx = YX.capex_read(entry, dep_amort=5e9)
    assert cx["actual_jpy"] == 8e9 and cx["plan_total_jpy"] == 12e9 and cx["remaining_jpy"] == 10e9
    assert abs(cx["remaining_vs_actual"] - 1.25) < 1e-9 and cx["remaining_vs_da"] == 2.0


def test_cross_holdings_falls_back_to_the_tagged_total():
    entry = {"fy": "FY2026", "sections": {},
             "facts": {"cross_book_value": {"parent": {"value": 45210, "unit": "百万円"}},
                       "cross_issues": {"parent": {"value": 31, "unit": ""}}}}
    ch = YX.cross_holdings_read(entry, market_cap=400e9)
    assert ch["total_jpy"] == 45.21e9 and ch["issues"] == 31


def test_yuho_filings_and_fy_label():
    idx = [{"SecCode": "8050", "DocTypeCode": "120", "PeriodEnd": "2026-03-31", "SubmitDateTime": "2026-06-24 11:09", "DocID": "A"},
           {"SecCode": "8050", "DocTypeCode": "120", "PeriodEnd": "2025-03-31", "SubmitDateTime": "2025-06-26 12:20", "DocID": "B"},
           {"SecCode": "8050", "DocTypeCode": "130", "PeriodEnd": "2026-03-31", "SubmitDateTime": "2026-07-01", "DocID": "C"},
           {"SecCode": "8035", "DocTypeCode": "120", "PeriodEnd": "2026-03-31", "SubmitDateTime": "2026-06-20", "DocID": "D"}]
    ys = YX.yuho_filings(idx, "8050")
    assert [y["DocID"] for y in ys] == ["A", "B"]
    assert YX.fy_label("2026-03-31") == "FY2026"


def test_yoy_series_and_build_entry():
    e1 = YX.build_entry({"PeriodEnd": "2025-03-31", "SubmitDateTime": "2025-06-26 12:20", "DocID": "B"},
                        {"facts": {"avg_salary": {"parent": {"value": 7.4e6, "unit": "円"}}}, "blocks": {}},
                        {}, {}, {"input": 0, "output": 0}, "2026-11-12")
    e2 = YX.build_entry({"PeriodEnd": "2026-03-31", "SubmitDateTime": "2026-06-24 11:09", "DocID": "A"},
                        {"facts": {"avg_salary": {"parent": {"value": 7.65e6, "unit": "円"}}}, "blocks": {"officers": "x"}},
                        {}, {}, {"input": 10, "output": 5}, "2026-11-12")
    assert e2["id"] == "yuho_FY2026" and e2["blocks_found"] == ["officers"]
    assert YX.yoy([e2, e1], "avg_salary") == [("FY2025", 7.4e6), ("FY2026", 7.65e6)]


# ── holders_extract ──────────────────────────────────────────────────────

IDX = [
    {"SecCode": "8050", "DocTypeCode": "350", "FilerName": "株式会社三菱ＵＦＪフィナンシャル・グループ",
     "DocDescription": "変更報告書（特例対象株券等）", "SubmitDateTime": "2026-10-15 09:00", "DocID": "L1"},
    {"SecCode": "8050", "DocTypeCode": "350", "FilerName": "エフィッシモ キャピタル マネージメント ピーティーイー エルティーディー",
     "DocDescription": "大量保有報告書", "SubmitDateTime": "2026-09-01 15:00", "DocID": "L2"},
    {"SecCode": "8050", "DocTypeCode": "350", "FilerName": "株式会社三菱ＵＦＪフィナンシャル・グループ",
     "DocDescription": "大量保有報告書（特例対象株券等）", "SubmitDateTime": "2025-04-01 09:00", "DocID": "L0"},
    {"SecCode": "8050", "DocTypeCode": "180", "FilerName": "セイコーグループ株式会社", "DocDescription": "臨時報告書",
     "SubmitDateTime": "2026-06-30 16:51", "DocID": "X1"},
    {"SecCode": "8050", "DocTypeCode": "180", "FilerName": "セイコーグループ株式会社", "DocDescription": "臨時報告書",
     "SubmitDateTime": "2026-02-10 16:51", "DocID": "X0"},
    {"SecCode": "7203", "DocTypeCode": "350", "FilerName": "x", "DocDescription": "大量保有報告書",
     "SubmitDateTime": "2026-09-01", "DocID": "Z"},
]


def test_large_holding_rows_read_the_index_and_flag_activists():
    rows = HX.large_holding_rows(IDX, "8050")
    assert [r["doc_id"] for r in rows] == ["L1", "L2", "L0"]
    assert rows[0]["special"] and rows[0]["kind"] == "change" and not rows[0]["activist"]
    assert rows[1]["activist"] == "Effissimo" and not rows[1]["special"] and rows[1]["kind"] == "new"
    s = HX.holders_summary(rows)
    assert s[0]["holder"].startswith("株式会社三菱") and s[0]["filings"] == 2 and s[0]["first"] == "2025-04-01"


def test_large_holding_numbers_from_tagged_values():
    rows = [{"element": "jplvh_cor:NameOfFiler", "label": "氏名又は名称", "value": "Effissimo"},
            {"element": "jplvh_cor:HoldingRatioOfShareCertificatesEtc", "label": "株券等保有割合", "value": "6.12"},
            {"element": "jplvh_cor:HoldingRatioOfShareCertificatesEtcPerLastReport", "label": "直前の報告書に記載された株券等保有割合", "value": "5.03"},
            {"element": "jplvh_cor:PurposeOfHolding", "label": "保有目的", "value": "純投資及び状況に応じて重要提案行為等を行うこと"}]
    p = HX.parse_large_holding(rows)
    assert p["pct"] == 6.12 and p["prev_pct"] == 5.03 and p["holder"] == "Effissimo"
    assert HX.purpose_read(p["purpose"]) == "activist (重要提案行為)"


def test_agm_candidates_sit_in_the_window_after_year_end():
    c = HX.agm_candidates(IDX, "8050", "2026-03-31")
    assert [r["DocID"] for r in c] == ["X1"]


def test_vote_text_needs_a_voting_table():
    assert HX.vote_text([{"value": "<p>議決権行使の結果 賛成 100</p>"}]).startswith("議決権行使の結果")
    assert HX.vote_text([{"value": "<p>代表取締役の異動</p>"}]) == ""


def test_agm_normalise_and_read():
    raw = {"agm_date": "2026-06-26", "resolutions": [
        {"number": "第1号議案", "title": "Dividend", "kind": "dividend", "person_name": "",
         "proposed_by_shareholder": False, "votes_for": 900, "votes_against": 100, "votes_abstain": 0,
         "approval_pct": 0, "passed": True},
        {"number": "第2号議案 1", "title": "Elect director", "kind": "director_election", "person_name": "服部 真二",
         "proposed_by_shareholder": False, "votes_for": 700, "votes_against": 300, "votes_abstain": 0,
         "approval_pct": 70.0, "passed": True},
        {"number": "第2号議案 2", "title": "Elect director", "kind": "director_election", "person_name": "山田 花子",
         "proposed_by_shareholder": False, "votes_for": 950, "votes_against": 50, "votes_abstain": 0,
         "approval_pct": 95.0, "passed": True},
        {"number": "第3号議案", "title": "Buyback", "kind": "shareholder_proposal", "person_name": "",
         "proposed_by_shareholder": True, "votes_for": 300, "votes_against": 700, "votes_abstain": 0,
         "approval_pct": 30.0, "passed": False},
    ], "notes": ""}
    agm = HX.normalise_agm(raw)
    assert agm["resolutions"][0]["approval_pct"] == 90.0
    officers = [{"name": "服部真二", "is_ceo": True}]
    prior = {"resolutions": [{"person_name": "服部 真二", "approval_pct": 82.0, "kind": "director_election"}]}
    r = HX.agm_read(agm, officers, prior)
    assert r["ceo"] == ("服部 真二", 70.0) and r["ceo_prior"] == 82.0
    assert r["low"] == [("服部 真二", 70.0)] and r["weakest"][0] == "服部 真二"
    assert r["shareholder_proposals"] == [("Buyback", 30.0, False)]
    assert r["drops"] == [("服部 真二", 82.0, 70.0)] and r["median"] == 95.0


def test_prior_year_lookup_matches_full_name_then_unique_surname():
    prior = {"resolutions": [
        {"person_name": "服部 真二", "approval_pct": 84.2},
        {"person_name": "佐藤 一郎", "approval_pct": 90.0},
        {"person_name": "佐藤 花子", "approval_pct": 70.0},
        {"person_name": "Director A", "approval_pct": 60.0},
        {"person_name": "Director B", "approval_pct": 61.0}]}
    lk = HX.prior_lookup(prior)
    assert HX.prior_pct(lk, "服部真二") == 84.2          # spacing differs, full match
    assert HX.prior_pct(lk, "服部 真一") == 84.2          # unique surname
    assert HX.prior_pct(lk, "佐藤 次郎") is None          # ambiguous surname
    assert HX.prior_pct(lk, "佐藤 花子") == 70.0          # exact wins over ambiguity
    assert HX.prior_pct(lk, "Director B") == 61.0 and HX.prior_pct(lk, "Director C") is None
