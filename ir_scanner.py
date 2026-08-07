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
    (["transcript", "議事録", "トランスクリプト"], "Transcript"),
    (["q&a", "qanda", "q & a", "想定問答", "質疑応答"], "Q&A"),
    (["presentation", "説明会資料", "決算説明", "説明資料", "スライド", "slide", "決算補足"],
     "Presentation"),
    (["annual report", "integrated report", "統合報告書", "アニュアルレポート", "factbook", "fact book"],
     "Annual Report (IR)"),
    (["shareholder meeting", "notice of convocation", "annual general meeting", " agm ",
      "招集通知", "株主総会", "議決権行使"], "AGM Materials"),
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


# Matches a link's own visible text when it carries no real information —
# just a file type and/or size ("PDF", "PDF(266KB)", "ダウンロード", "PDF 1.2MB").
# Very common: the actual description sits in a heading or row label instead.
_GENERIC_LABEL_RE = re.compile(
    r'^(pdf|ppt|pptx|xls|xlsx|doc|docx|zip|download|dl|view|詳細|見る|ダウンロード)'
    r'\s*[\(（]?\s*[\d.,]*\s*[kmg]?b?\s*[\)）]?$',
    re.IGNORECASE,
)


def _is_generic_label(text: str) -> bool:
    return not text.strip() or bool(_GENERIC_LABEL_RE.match(text.strip()))


def _nearby_context_text(a_tag) -> str:
    """Best-effort text to use instead of a generic link label: prefer other
    descriptive text in a nearby row/list item (common table/list layouts
    like "<tr><td>Quarterly Results</td><td><a>PDF</a></td></tr>"), falling
    back to the nearest heading before this link's containing block. The
    ancestor climb is bounded (depth and residual length) so a div-heavy
    layout with no li/tr structure doesn't walk out into unrelated page
    content and return some huge, useless blob of text."""
    own_text = a_tag.get_text(" ", strip=True)
    for depth, ancestor in enumerate(a_tag.parents):
        if depth >= 4 or ancestor.name in (None, "body", "html"):
            break
        row_text = ancestor.get_text(" ", strip=True)
        residual = row_text.replace(own_text, "", 1).strip(" ·-—|/")
        if residual and 2 < len(residual) <= 120 and not _is_generic_label(residual):
            return residual
    try:
        heading = a_tag.find_previous(["h1", "h2", "h3", "h4", "h5", "h6"])
    except Exception:
        heading = None
    if heading:
        heading_text = heading.get_text(strip=True)
        if heading_text:
            return heading_text
    return ""


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

        # A link's own text is very often just "PDF" or "PDF(266KB)" — the
        # real description sits in a nearby heading or row label instead.
        context = _nearby_context_text(a) if _is_generic_label(text) else ""
        title = context or text or _title_from_url(abs_url)
        doc_type = _guess_doc_type(context or text, href)

        out.append({"title": title, "url": abs_url, "doc_type": doc_type})
        if len(out) >= max_results:
            break
    return out
