"""Tests for pairing English versions with Japanese filings — `python test_attach_english.py`.

The cases are from the Reg Filings tab on 29 Oct 2026, where every filing a
company made at 17:00 showed the same English title and PDF.
"""

import sys

from tdnet import attach_english, dedupe_filings


def _jp(code, title, t="2026-10-29 17:00"):
    return dict(code=code, name="社", title=title, title_en="", name_en="",
                pub_date=t, doc_url=f"https://x/{code}-{len(title)}.pdf", eng_url="")


def _en(title, url, name="Tokyo Electric Power Company Holdings,Incorporated"):
    return {"name_en": name, "title_en": title, "eng_url": url}


def test_tepco_three_filings_each_get_their_own_english_or_none():
    rows = [_jp("95010", "2027年３月期　第２四半期（中間期）決算短信〔日本基準〕(連結)"),
            _jp("95010", "2026年度第2四半期決算説明資料"),
            _jp("95010", "特別損失の計上に関するお知らせ")]
    lookup = {("95010", "2026-10-29 17:00"): [
        _en("[Summary]Consolidated Financial Results for the Six Months Ended September 30,2026"
            "(Under Japanese GAAP)", "https://x/en-tanshin.pdf"),
        _en("Notice of Recording of Extraordinary Loss", "https://x/en-loss.pdf"),
    ]}
    left = attach_english(rows, lookup)
    assert rows[0]["eng_url"] == "https://x/en-tanshin.pdf", rows[0]
    assert rows[1]["eng_url"] == "", rows[1]          # no English presentation was filed
    assert rows[2]["eng_url"] == "https://x/en-loss.pdf", rows[2]
    assert left == [], left
    assert all(r["name_en"].startswith("Tokyo Electric") for r in rows)


def test_dividend_english_goes_to_the_dividend_filing_only():
    rows = [_jp("71480", "2026年9月期　決算短信〔日本基準〕（連結）", "2026-10-29 16:30"),
            _jp("71480", "2026年9月期 決算説明資料", "2026-10-29 16:30"),
            _jp("71480", "剰余金の配当に関するお知らせ", "2026-10-29 16:30"),
            _jp("71480", "自己株式取得に係る事項の決定に関するお知らせ", "2026-10-29 16:30")]
    lookup = {("71480", "2026-10-29 16:30"): [
        _en("Notice Regarding Dividend Payment", "https://x/en-div.pdf", "Financial Partners Group")]}
    attach_english(rows, lookup)
    assert [r["eng_url"] for r in rows] == ["", "", "https://x/en-div.pdf", ""], rows


def test_an_english_version_that_fits_no_filing_is_not_guessed():
    rows = [_jp("71480", "2026年9月期　決算短信"), _jp("71480", "自己株式取得に関するお知らせ")]
    lookup = {("71480", "2026-10-29 17:00"): [_en("Notice Regarding Dividend Payment", "https://x/d.pdf")]}
    left = attach_english(rows, lookup)
    assert not any(r["eng_url"] for r in rows), rows
    assert len(left) == 1, left
    # ...and the leftover is then dropped, not shown as a duplicate row:
    left_rows = [dict(code=c, name="", title=e["title_en"], title_en=e["title_en"], name_en="",
                      pub_date=t, doc_url="", eng_url=e["eng_url"], en_only=True) for c, t, e in left]
    assert len(dedupe_filings(rows + left_rows)) == 2


def test_a_lone_filing_and_a_lone_english_version_pair_whatever_the_wording():
    rows = [_jp("41890", "代表取締役の異動に関するお知らせ", "2026-10-29 16:30")]
    lookup = {("41890", "2026-10-29 16:30"): [_en("Announcement Concerning Changes", "https://x/k.pdf")]}
    attach_english(rows, lookup)
    assert rows[0]["eng_url"] == "https://x/k.pdf", rows[0]


def test_english_with_no_japanese_filing_is_returned_to_show():
    left = attach_english([], {("40630", "2026-10-29 15:00"): [_en("Financial Results", "https://x/s.pdf")]})
    assert len(left) == 1 and left[0][0] == "40630", left


def test_csv_field_names_work_for_the_scheduled_job():
    from tdnet import CSV_EN_FIELDS
    rows = [{"Code": "9501", "Title": "決算短信", "PubDateTime": "2026-10-29 17:00",
             "TitleEN": "", "NameEN": "", "EngUrl": ""}]
    attach_english(rows, {("9501", "2026-10-29 17:00"): [_en("Financial Results", "https://x/a.pdf")]},
                   CSV_EN_FIELDS)
    assert rows[0]["EngUrl"] == "https://x/a.pdf", rows[0]


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
