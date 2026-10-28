"""Tests for merging Reg Filings rows from three sources — `python test_dedupe_filings.py`.

Each case is a way the same filing reaches the tab twice: TDnet's own list
and Yanoshin's copy spell titles and PDF links differently, and the TSE
English list posts its version minutes after the Japanese one.
"""

import sys

from tdnet import dedupe_filings


def _jp(code="40630", t="2026-10-27 15:00", title="2027年3月期 第2四半期（中間期）決算短信〔日本基準〕（連結）",
        doc="https://www.release.tdnet.info/inbs/140120261027500001.pdf", **kw):
    return dict(code=code, name="信越化学工業", title=title, title_en="", name_en="",
                pub_date=t, doc_url=doc, eng_url="", **kw)


def _en(code="40630", t="2026-10-27 15:10", title="Consolidated Financial Results for Q2",
        url="https://www.release.tdnet.info/inbs/140120261027500999.pdf"):
    return dict(code=code, name="Shin-Etsu Chemical", title=title, title_en=title,
                name_en="Shin-Etsu Chemical", pub_date=t, doc_url="", eng_url=url, en_only=True)


def test_same_pdf_in_both_japanese_sources_is_one_row():
    yanoshin = _jp(title="決算短信 （連結）")
    official = _jp(title="決算短信（連結）")
    assert len(dedupe_filings([yanoshin, official])) == 1


def test_titles_differing_only_in_spaces_and_width_are_one_row():
    """Yanoshin's own link form, so only the title can match them."""
    a = _jp(title="２０２７年３月期　決算短信", doc="https://webapi.yanoshin.jp/webapi/tdnet/x")
    b = _jp(title="2027年3月期 決算短信", doc="https://www.release.tdnet.info/inbs/1.pdf")
    assert len(dedupe_filings([a, b])) == 1


def test_english_version_posted_later_attaches_instead_of_duplicating():
    out = dedupe_filings([_jp(), _en()])
    assert len(out) == 1, out
    assert out[0]["eng_url"].endswith("500999.pdf"), out[0]


def test_english_row_for_a_company_with_several_filings_is_dropped_not_guessed():
    a = _jp(title="決算短信", doc="https://x/1.pdf")
    b = _jp(title="配当予想の修正", doc="https://x/2.pdf", t="2026-10-27 15:00")
    out = dedupe_filings([a, b, _en()])
    assert len(out) == 2 and not any(f["eng_url"] for f in out), out


def test_english_only_filing_is_kept_when_no_japanese_source_has_it():
    out = dedupe_filings([_jp(code="72030"), _en()])
    assert len(out) == 2 and out[1]["eng_url"], out


def test_the_same_english_row_twice_is_one_row():
    assert len(dedupe_filings([_en(), _en()])) == 1


def test_different_filings_from_one_company_are_both_kept():
    a = _jp(title="決算短信", doc="https://x/1.pdf")
    b = _jp(title="自己株式の取得に関するお知らせ", doc="https://x/2.pdf")
    assert len(dedupe_filings([a, b])) == 2


def test_an_english_match_on_the_duplicate_is_kept():
    first = _jp()
    second = _jp(); second["eng_url"] = "https://x/en.pdf"; second["title_en"] = "Q2 results"
    out = dedupe_filings([first, second])
    assert len(out) == 1 and out[0]["eng_url"] == "https://x/en.pdf", out


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
