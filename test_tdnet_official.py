"""Tests for reading TDnet's own daily list — `python test_tdnet_official.py`.

No network: requests is stubbed with pages in TDnet's main-list-table layout.
The page reader was written without access to the live site, so what matters
most is that anything it cannot read comes back as [] — the Reg Filings tab
then falls back to Yanoshin instead of losing the feed.
"""

import sys
import types

import tdnet


def _row(time, code, name, title, pdf):
    return (f'<tr><td class="oddnew-L kjTime" noWrap>{time}</td>'
            f'<td class="oddnew-M kjCode" noWrap>{code}</td>'
            f'<td class="oddnew-M kjName" noWrap>{name}</td>'
            f'<td class="oddnew-M kjTitle" align="left">'
            f'<a href="{pdf}" target="_blank">{title}</a></td>'
            f'<td class="oddnew-M kjXbrl"></td><td class="oddnew-M kjPlace">東</td>'
            f'<td class="oddnew-R kjHistroy"></td></tr>')


def _page(rows):
    return ('<html><body><table id="main-list-table">' + "".join(rows)
            + "</table></body></html>")


class _Resp:
    def __init__(self, status_code, text=""):
        self.status_code, self.text = status_code, text
        self.content = text.encode("utf-8")


def _install(pages):
    """pages: {page_number: html}; anything else is a 404."""
    seen = []
    mod = types.ModuleType("requests")

    def get(url, headers=None, timeout=None):
        seen.append(url)
        n = int(url.split("I_list_")[1][:3])
        return _Resp(200, pages[n]) if n in pages else _Resp(404)

    mod.get = get
    sys.modules["requests"] = mod
    return seen


def test_reads_a_day_and_builds_full_pdf_links():
    _install({1: _page([
        _row("17:00", "13330", "マルハニチロ", "2027年3月期 第2四半期決算短信", "140120261027500001.pdf"),
        _row("15:30", "130A0", "ベリフ", "業績予想の修正に関するお知らせ", "140120261027500002.pdf"),
    ])})
    out = tdnet.fetch_tdnet_official_day("20261027")
    assert len(out) == 2, out
    a = out[0]
    assert a["code"] == "13330" and a["pub_date"] == "2026-10-27 17:00", a
    assert a["doc_url"] == "https://www.release.tdnet.info/inbs/140120261027500001.pdf", a
    assert out[1]["code"] == "130A0", out[1]   # alphanumeric codes kept as listed


def _n_rows(n, start, time="15:00"):
    return _page([_row(time, f"{1000 + start + i}0", "社", f"決算短信 {start + i}",
                       f"{start + i}.pdf") for i in range(n)])


def test_reads_every_page_whatever_the_page_size():
    """Newest first: the 15:00 wave sits on the later pages. A short page is
    not the end — only a 404 or an empty page is."""
    seen = _install({1: _n_rows(50, 0, "18:00"), 2: _n_rows(37, 50, "16:00"),
                     3: _n_rows(80, 87, "15:00")})
    out = tdnet.fetch_tdnet_official_day("20261027")
    assert len(out) == 167, len(out)
    assert out[-1]["pub_date"].endswith("15:00"), out[-1]
    assert len(seen) == 4, seen          # pages 1-3, then the 404 on page 4


def test_a_server_repeating_its_last_page_does_not_loop_or_double_count():
    last = _n_rows(10, 0)
    pages = {i: last for i in range(1, 40)}
    _install(pages)
    out = tdnet.fetch_tdnet_official_day("20261027")
    assert len(out) == 10, len(out)


def test_no_filings_that_day_is_empty_not_an_error():
    _install({})                          # page 1 is a 404
    assert tdnet.fetch_tdnet_official_day("20261025") == []


def test_unrecognised_layout_returns_empty_so_the_tab_falls_back():
    _install({1: "<html><body><table><tr><td>17:00</td><td>13330</td>"
                 "<td>名</td><td>題</td></tr></table></body></html>"})
    assert tdnet.fetch_tdnet_official_day("20261027") == []


def test_network_failure_returns_empty():
    mod = types.ModuleType("requests")

    def get(*a, **k):
        raise ConnectionError("down")

    mod.get = get
    sys.modules["requests"] = mod
    assert tdnet.fetch_tdnet_official_day("20261027") == []


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
