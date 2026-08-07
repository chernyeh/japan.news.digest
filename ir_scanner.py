"""
ir_scanner.py — best-effort scanner for company IR pages.

EDINET and TDnet only carry *statutory* disclosures. Investor materials like
earnings-call transcripts, Q&A notes, and presentation decks (with or without
speaker notes) generally only exist on a company's own IR site. There's no
structured feed for these — every company's IR page is laid out differently,
some are JS-rendered SPAs this can't see at all — so this fetches a given
page's HTML and heuristically finds links that look like investor documents,
for the user to review and selectively add. It is not, and can't be, a
reliable universal scraper: treat results as candidates, not ground truth.
"""

import re
from urllib.parse import urljoin, urlparse, unquote

USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

DOCUMENT_EXTENSIONS = (".pdf", ".ppt", ".pptx", ".xls", ".xlsx", ".doc", ".docx", ".zip")

# (keywords, guessed doc_type) — checked in order, first match wins. Mixes
# English and Japanese since most IR pages are Japanese-only or bilingual.
_TYPE_RULES = [
    (["transcript", "議事録", "トランスクリプト"], "Earnings Call Transcript"),
    (["q&a", "qanda", "q & a", "想定問答", "質疑応答"], "Q&A / Briefing Notes"),
    (["presentation", "説明会資料", "決算説明", "説明資料", "スライド", "slide", "決算補足"],
     "Investor Presentation"),
    (["annual report", "integrated report", "統合報告書", "アニュアルレポート", "factbook", "fact book"],
     "Annual Report (Company IR)"),
    (["shareholder meeting", "notice of convocation", "annual general meeting", " agm ",
      "招集通知", "株主総会", "議決権行使"], "Shareholder Meeting Materials"),
]


def _guess_doc_type(text: str, href: str) -> str:
    hay = f"{text} {href}".lower()
    for keywords, label in _TYPE_RULES:
        for kw in keywords:
            if kw in hay or kw in text:  # kw may be Japanese, unaffected by .lower()
                return label
    return "Other"


def _looks_like_document(text: str, href: str) -> bool:
    path = urlparse(href).path.lower()
    if path.endswith(DOCUMENT_EXTENSIONS):
        return True
    hay = f"{text} {href}"
    for keywords, _label in _TYPE_RULES:
        for kw in keywords:
            if kw in hay.lower() or kw in text:
                return True
    return False


def _title_from_url(url: str) -> str:
    name = urlparse(url).path.rsplit("/", 1)[-1]
    name = unquote(name)
    name = re.sub(r"\.[a-zA-Z0-9]{2,5}$", "", name)  # strip extension
    name = re.sub(r"[_\-]+", " ", name).strip()
    return name or url


def scan_page_for_documents(url: str, max_results: int = 60, timeout: int = 20) -> list:
    """Fetch `url` and return up to max_results candidate document links as
    [{"title": str, "url": str, "doc_type": str}, ...], deduped by URL.
    Raises on a fetch failure (bad URL, network error, non-2xx status) —
    callers should catch and surface that to the user."""
    import requests
    from bs4 import BeautifulSoup

    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "lxml")

    seen = set()
    out = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        abs_url = urljoin(url, href)
        if not abs_url.startswith(("http://", "https://")):
            continue
        text = a.get_text(strip=True)
        if not _looks_like_document(text, href):
            continue
        if abs_url in seen:
            continue
        seen.add(abs_url)
        out.append({
            "title": text or _title_from_url(abs_url),
            "url": abs_url,
            "doc_type": _guess_doc_type(text, href),
        })
        if len(out) >= max_results:
            break
    return out
