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


PAY_HTML = ("<table><tr><th>役員区分</th><th>報酬等の総額</th><th>対象となる役員の員数</th></tr>"
            "<tr><td>取締役</td><td>612</td><td>6</td></tr></table>")
CROSS_HTML = ("<p>特定投資株式</p><table><tr><th>銘柄</th><th>保有目的</th><th>貸借対照表計上額</th>"
              "<th>当社の株式の保有の有無</th></tr><tr><td>Bank X</td><td>取引関係</td><td>9,100</td><td>有</td></tr></table>")
PLAN_HTML = ("<table><tr><th>会社名</th><th>投資予定金額</th><th>既支払額</th><th>資金調達方法</th><th>着手</th><th>完了</th></tr>"
             "<tr><td>Morioka</td><td>9,500</td><td>1,200</td><td>自己資金</td><td>2026.4</td><td>2027.9</td></tr></table>")
OWNER_HTML = ("<p>所有者別状況</p><table><tr><th>政府及び地方公共団体</th><th>金融機関</th><th>外国法人等</th></tr>"
              "<tr><td>0</td><td>31.2</td><td>22.4</td></tr></table>")


def test_sections_are_found_by_content_when_tags_and_labels_say_nothing():
    """The first real filing matched none of the documented tag names: the
    text itself has to identify a section."""
    parent = "<div>" + OFFICERS_HTML + PAY_HTML + CROSS_HTML + "</div>"
    rows = [
        {"element": "jpcrp_cor:CorporateGovernanceTextBlock", "label": "コーポレート・ガバナンスの状況等",
         "context": "FilingDateInstant", "value": parent},
        {"element": "jpcrp_cor:SomeOddNameTextBlock", "label": "(2)", "context": "FilingDateInstant",
         "value": OFFICERS_HTML},
        {"element": "jpcrp_cor:AnotherOddNameTextBlock", "label": "", "context": "FilingDateInstant",
         "value": PAY_HTML},
        {"element": "jpcrp_cor:ThirdOddNameTextBlock", "label": "", "context": "FilingDateInstant",
         "value": CROSS_HTML},
        {"element": "jpcrp_cor:FourthTextBlock", "label": "", "context": "FilingDateInstant", "value": PLAN_HTML},
        {"element": "jpcrp_cor:FifthTextBlock", "label": "", "context": "FilingDateInstant", "value": OWNER_HTML},
        {"element": "jpcrp_cor:FifthTextBlock", "label": "", "context": "Prior1YearInstant", "value": OWNER_HTML},
        {"element": "jpcrp_cor:PolicyHeadingTextBlock", "label": "株式の保有状況", "context": "FilingDateInstant",
         "value": "<p>保有方針</p>"},
    ]
    b = YX.text_blocks(rows)
    # Each section is its own sub-block, not the parent that holds all three,
    # and the content-rich cross-holdings table beats the heading that merely
    # carries the label.
    assert b["officers"] == OFFICERS_HTML and b["exec_pay"] == PAY_HTML
    assert b["cross_holdings"] == CROSS_HTML and b["capex_plan"] == PLAN_HTML
    assert b["share_status"] == OWNER_HTML
    cat = YX.catalog(rows)
    assert cat[0]["element"] == "CorporateGovernanceTextBlock" and cat[0]["chars"] > cat[1]["chars"]
    assert {c["kind"] for c in cat} >= {"officers", "exec_pay", "cross_holdings", "capex_plan", "share_status"}
    assert all(not k.startswith("_") for k in YX.prepare(_zip({"a.csv": _csv(ROWS)}))["catalog"][0])


def test_consolidated_is_read_from_the_context_id_first():
    assert YX._consolidated({"context": "CurrentYearInstant", "cons": "個別"}) is False
    assert YX._consolidated({"context": "CurrentYearInstant_NonConsolidatedMember", "cons": "連結"}) is False
    assert YX._consolidated({"context": "CurrentYearInstant", "cons": ""}) is True
    e = {"facts": {}, "sections": {"share_status": YX.normalise("share_status", {
        "as_of": "", "foreign_pct": 22.4, "financial_institutions_pct": 31.2, "individuals_pct": 20.0,
        "treasury_shares": 10, "shares_issued": 1000, "shareholders_count": 28400, "notes": ""})}}
    assert YX.foreign_pct(e) == 22.4 and YX.shareholders_count(e) == 28400


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
    assert "Board of 2, 1 outside (50%)" in read


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


# ── the board, parsed without a model ───────────────────────────────────

BOARD_ONE_ROW = """
<p>2026年6月24日現在</p>
<table>
<tr><th>役職名</th><th>氏名</th><th>生年月日</th><th>略歴</th><th>任期</th><th>所有株式数（千株）</th></tr>
<tr><td>代表取締役会長 CEO</td><td>服部　真二</td><td>昭和28年5月1日</td>
<td>1984年4月 当社入社<br/>1995年6月 当社海外営業部長<br/>2003年6月 当社取締役<br/>2012年6月 当社代表取締役社長（現任）</td>
<td>(注)3</td><td>1,240</td></tr>
<tr><td>取締役 専務執行役員 CFO</td><td>鈴木 一郎</td><td>1965年2月3日</td>
<td><table><tr><td>1988年4月</td><td>株式会社三菱銀行入行</td></tr><tr><td>2005年4月</td><td>当社入社 経理部長</td></tr>
<tr><td>2018年6月</td><td>当社取締役（現任）</td></tr></table></td><td>(注)3</td><td>12</td></tr>
<tr><td>取締役</td><td>山田 花子</td><td>平成元年1月2日</td>
<td>1990年4月 三菱商事株式会社入社<br/>2015年6月 同社執行役員<br/>2020年6月 当社取締役（現任）</td><td>(注)3</td><td>－</td></tr>
<tr><td>常勤監査役</td><td>田中 太郎</td><td>1962年7月8日</td><td>1986年4月 当社入社<br/>2019年6月 当社監査役（現任）</td><td>(注)4</td><td>3</td></tr>
</table>
<p>（注）1. 取締役山田花子は、社外取締役であります。</p>
"""

BOARD_ROWSPAN = """
<table>
<tr><th>役名</th><th>職名</th><th>氏名</th><th>生年月日</th><th>略歴</th><th>任期</th><th>所有株式数（株）</th></tr>
<tr><td rowspan="3">取締役社長</td><td rowspan="3">－</td><td rowspan="3">佐藤 二郎</td><td rowspan="3">1970年3月4日</td>
<td>1993年4月 当社入社</td><td rowspan="3">(注)2</td><td rowspan="3">52,000</td></tr>
<tr><td>2016年6月 当社取締役</td></tr>
<tr><td>2022年6月 当社取締役社長（現任）</td></tr>
<tr><td rowspan="2">社外取締役</td><td rowspan="2">－</td><td rowspan="2">高橋 三郎</td><td rowspan="2">1958年9月10日</td>
<td>1981年4月 経済産業省入省</td><td rowspan="2">(注)2</td><td rowspan="2">0</td></tr>
<tr><td>2021年6月 当社取締役（現任）</td></tr>
</table>
"""


def test_board_parsed_from_a_one_row_per_officer_table():
    b = YX.parse_officers(BOARD_ONE_ROW, filed="2026-06-24")
    assert b["as_of"] == "2026年6月24日現在"
    offs = {o["name"]: o for o in b["officers"]}
    assert set(offs) == {"服部 真二", "鈴木 一郎", "山田 花子", "田中 太郎"}
    h = offs["服部 真二"]
    assert h["birth_year"] == 1953 and h["age"] == 73          # 昭和28 → 1953; birthday passed
    assert h["joined_company_year"] == 1984 and h["director_since_year"] == 2003
    assert h["is_ceo"] and h["is_chair"] and h["role_kind"] == "representative_director"
    assert h["shares_held"] == 1_240_000 and "overseas" in h["background_tags"]
    s = offs["鈴木 一郎"]                                        # nested career table, ex-bank
    assert s["joined_company_year"] == 2005 and s["director_since_year"] == 2018
    assert s["is_cfo"] and "ex_bank" in s["background_tags"] and "finance_accounting" in s["background_tags"]
    assert s["shares_held"] == 12_000
    y = offs["山田 花子"]                                        # outside by the note; 平成元 → 1989
    assert y["outside"] and y["role_kind"] == "outside_director" and y["birth_year"] == 1989
    assert y["joined_company_year"] == 0 and y["director_since_year"] == 2020
    assert "ex_trading_house" in y["background_tags"] and "other_company_executive" in y["background_tags"]
    assert y["shares_held"] == 0
    t = offs["田中 太郎"]
    assert t["role_kind"] == "corporate_auditor" and not t["is_ceo"]
    assert YX.board_stats(b["officers"], 2026)["size"] == 3   # the auditor is not the board


def test_board_parsed_from_a_rowspan_table():
    b = YX.parse_officers(BOARD_ROWSPAN, filed="2026-06-24")
    offs = {o["name"]: o for o in b["officers"]}
    assert set(offs) == {"佐藤 二郎", "高橋 三郎"}
    s = offs["佐藤 二郎"]
    assert s["joined_company_year"] == 1993 and s["director_since_year"] == 2016
    assert s["is_ceo"] and s["shares_held"] == 52_000 and s["age"] == 56
    t = offs["高橋 三郎"]
    assert t["outside"] and "ex_government" in t["background_tags"] and t["director_since_year"] == 2021
    e = {"fy": "FY2026", "sections": {"officers": YX.strip_private(b)}}
    assert "_career" not in e["sections"]["officers"]["officers"][0]
    assert "CEO 佐藤 二郎" in YX.bias_read(e)


def test_no_table_gives_an_empty_board_not_an_error():
    assert YX.parse_officers("<p>nothing</p>")["officers"] == []
    assert YX.parse_officers("")["notes"]


def test_estimate_and_read_jobs_follow_the_model_and_skip_unwanted_sections():
    blocks = {"officers": "x" * 5000, "major_shareholders": "y" * 5000, "share_status": "z" * 3000,
              "cross_holdings": "<p>" + "c" * 4000 + "</p>", "capex_plan": "<p>p</p>", "exec_pay": "<p>e</p>"}
    est = YX.estimate_cost(blocks, "claude-haiku-5-5")
    assert est["jobs"] == ["cross_holdings", "exec_pay", "capex"]
    assert est["input_tokens"] < 6000 and est["usd"] < 0.01
    assert YX.estimate_cost(blocks, "claude-opus-5-5")["usd"] > 10 * est["usd"]
    assert abs(YX.cost_usd({"input": 1_000_000, "output": 0}, "claude-haiku-5-5") - 0.10) < 1e-9


def test_policy_bullets_use_the_list_or_split_the_summary():
    assert YX.policy_bullets({"policy_points": ["a", "", "b"], "policy_summary": "x. y"}) == ["a", "b"]
    assert YX.policy_bullets({"policy_summary": "Holds partners' shares. Reviewed yearly; board approves."}) == [
        "Holds partners' shares.", "Reviewed yearly;", "board approves."]
    assert YX.policy_bullets({}) == []


def test_officers_go_to_the_model_only_when_the_table_did_not_parse():
    blocks = {"officers": "<p>" + "o" * 3000 + "</p>", "exec_pay": "<p>e</p>"}
    assert YX.estimate_cost(blocks, "claude-haiku-5-5")["jobs"] == ["exec_pay"]
    est = YX.estimate_cost(blocks, "claude-haiku-5-5", include_officers=True)
    assert est["jobs"] == ["officers", "exec_pay"] and est["usd"] < 0.01
    sec = YX.normalise("officers", {"officers": [{"name": "辻 朋邦", "name_en": "Tomokuni Tsuji",
                                                   "title": "代表取締役社長", "role_kind": "representative_director",
                                                   "outside": False, "age": 37, "is_ceo": True}]})
    assert sec["officers"][0]["name_en"] == "Tomokuni Tsuji"
    cross = YX.normalise("cross_holdings", {"issues": [{"name": "㈱バンダイナムコホールディングス",
                                                        "name_en": "Bandai Namco Holdings", "book_value_jpy": 1.7e9}],
                                            "policy_points": ["p1", "p2"]})
    assert cross["issues"][0]["name_en"] == "Bandai Namco Holdings" and cross["policy_points"] == ["p1", "p2"]
    agm = HX.normalise_agm({"resolutions": [{"number": "1", "title": "t", "kind": "director_election",
                                             "person_name": "辻 朋邦", "person_name_en": "Tomokuni Tsuji",
                                             "approval_pct": 89.4, "passed": True}]})
    assert agm["resolutions"][0]["person_name_en"] == "Tomokuni Tsuji"


def test_ages_come_from_birth_dates_and_board_and_management_split():
    sec = YX.normalise("officers", {"officers": [
        {"name": "辻 朋邦", "title": "代表取締役社長", "role_kind": "representative_director", "outside": False,
         "age": 0, "birth_year": 1988, "birth_month": 8, "is_ceo": True, "joined_company_year": 2014,
         "director_since_year": 2016, "shares_held": 2488000, "background_tags": ["sales_marketing"]},
        {"name": "笹本 裕", "title": "取締役", "role_kind": "outside_director", "outside": True,
         "age": 0, "birth_year": 1964, "birth_month": 0},
        {"name": "猪山 雄央", "title": "補欠の監査等委員である取締役", "role_kind": "audit_committee_member",
         "outside": False, "birth_year": 1975},
        {"name": "森田 英樹", "title": "執行役員", "role_kind": "executive_officer", "outside": False}],
        "executive_officers": [{"name": "辻 友子", "name_en": "Tomoko Tsuji", "title": "常務執行役員",
                                "title_en": "Managing Executive Officer"},
                               {"name": "森田 英樹", "title": "執行役員"}],
        "notes": "long text"})
    assert sec["notes"] == ""
    o = sec["officers"]
    assert YX.age_of(o[0], "2026-06-30") == (37, True)          # birthday in August, not yet
    assert YX.age_of(o[1], "2026-06-30") == (62, False)         # year only: approximate
    assert YX.age_of({"birth_year": 0}, "2026-06-30") is None
    board, mgmt, subs = YX.board_and_management(sec)
    assert [b["name"] for b in board] == ["辻 朋邦", "笹本 裕"]
    assert [m["name"] for m in mgmt] == ["森田 英樹", "辻 友子"]   # no duplicate
    assert [x["name"] for x in subs] == ["猪山 雄央"]
    pts = YX.bias_points({"fy": "FY2026", "date": "2026-06-30", "sections": {"officers": sec}}, price=1000)
    assert pts[0] == "CEO 辻 朋邦, 37: sales"
    assert "12 years at the company; director since 2016" in pts
    assert "Holds 2,488,000 shares (¥2,488m)" in pts
    assert any(p.startswith("Board of 2, 1 outside (50%)") for p in pts)
