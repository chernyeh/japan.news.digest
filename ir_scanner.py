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


def _href_filename(href: str) -> str:
    """Just the last path segment. Keyword-matching against the *full* href is
    too loose: on a site like mufg.jp/english/ir/presentation/index.html every
    link under that directory contains "presentation" in its path, so plain
    year-navigation links get scooped up and mislabelled. The filename is a
    far better signal (e.g. "fy26q1_presentation.pdf")."""
    return unquote(urlparse(href).path.rsplit("/", 1)[-1]).lower()


def _file_ext(href: str) -> str:
    """Document extension without the dot, or "" if this isn't a direct file link."""
    path = urlparse(href).path.lower()
    for ext in DOCUMENT_EXTENSIONS:
        if path.endswith(ext):
            return ext.lstrip(".")
    return ""


def _matches_type_rules(*texts) -> str:
    hay = " ".join(t for t in texts if t)
    hay_lower = hay.lower()
    for keywords, label in _TYPE_RULES:
        for kw in keywords:
            if kw in hay_lower or kw in hay:  # kw may be Japanese, unaffected by .lower()
                return label
    return ""


def _guess_doc_type(text: str, href: str) -> str:
    return _matches_type_rules(text, _href_filename(href)) or "Other"


def _looks_like_document(text: str, href: str) -> bool:
    if _file_ext(href):
        return True
    return bool(_matches_type_rules(text, _href_filename(href)))


# Link text that's pure navigation rather than a document: a bare year, a page
# number, or common pager/menu words. These show up constantly on IR archive
# pages ("2025", "2024", "Next") and are noise in the results.
_NAV_LABEL_RE = re.compile(
    r'^(19|20)\d{2}(年|年度)?$|^\d{1,3}$|'
    r'^(next|prev|previous|back|more|top|home|一覧|次へ|前へ|もっと見る)$',
    re.IGNORECASE,
)


def _is_nav_label(text: str) -> bool:
    return bool(_NAV_LABEL_RE.match(text.strip()))


_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}

_EN_QUARTER_WORDS = {
    "first": 1, "1st": 1, "second": 2, "2nd": 2, "third": 3, "3rd": 3, "fourth": 4, "4th": 4,
}

# "First Quarter Ended June 30, 2026" / "Fiscal Year Ended March 31, 2026" — a
# real calendar date, so this is the one case worth converting to ISO.
_EN_PERIOD_RE = re.compile(
    r'(?:(?P<q>first|second|third|fourth|1st|2nd|3rd|4th)\s+quarter\s+|fiscal\s+year\s+)?'
    r'end(?:ed|ing)?\s+(?P<month>[A-Za-z]+)\.?\s+(?P<day>\d{1,2}),?\s+(?P<year>\d{4})',
    re.IGNORECASE,
)

# Japanese fiscal-period notation, e.g. "2026年3月期第1四半期" (1Q of the fiscal
# year ending March 2026) or just "2026年3月期" for the full year. Fiscal
# year-end days vary by company and aren't derivable from this text alone, so
# this only ever produces a label, never a fabricated date.
_JP_PERIOD_RE = re.compile(r'(?P<year>\d{4})年(?P<month>\d{1,2})月期(?:第(?P<q>[1-4１２３４])四半期)?')
_JP_DIGIT_MAP = {"１": "1", "２": "2", "３": "3", "４": "4"}


def extract_period(text: str):
    """Best-effort fiscal-period detection from a title/context string.
    Returns {"date": iso_str_or_None, "period_label": str} or None if no
    period could be detected. English "Quarter Ended <Month> <Day>, <Year>"
    yields a real ISO date; Japanese "…年…月期" notation yields a label only,
    since fiscal year-end days vary by company and can't be inferred here."""
    if not text:
        return None

    m = _EN_PERIOD_RE.search(text)
    if m:
        month = _MONTH_NAMES.get(m.group("month").lower())
        if month:
            year, day = int(m.group("year")), int(m.group("day"))
            try:
                date_iso = f"{year:04d}-{month:02d}-{day:02d}"
                import datetime as _dt
                _dt.date(year, month, day)  # validate real calendar date
            except ValueError:
                date_iso = None
            if date_iso:
                q_word = m.group("q")
                qn = _EN_QUARTER_WORDS.get(q_word.lower()) if q_word else None
                label = f"Q{qn} FY{year}" if qn else f"FY{year}"
                return {"date": date_iso, "period_label": label}

    m = _JP_PERIOD_RE.search(text)
    if m:
        year, month, q = m.group("year"), m.group("month"), m.group("q")
        label = f"FY{year}/{int(month):02d}"
        if q:
            label = f"Q{_JP_DIGIT_MAP.get(q, q)} {label}"
        return {"date": None, "period_label": label}

    return None


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


def scan_page_for_documents(url: str, max_results: int = 120, timeout: int = 20) -> list:
    """Fetch `url` and return up to max_results candidate links as
    [{"title", "url", "doc_type", "kind", "ext"}, ...], deduped by URL.

    kind is "file" for a direct document download (.pdf/.pptx/…) or "page"
    for an HTML link that merely looks document-related — the caller is
    expected to surface that distinction, since only "file" entries can be
    downloaded directly. date/period_label carry a best-effort fiscal period
    (see extract_period) — undated siblings inherit the most recently seen
    period for a bounded run, since IR archive pages commonly show one dated
    "Summary of Consolidated Results for Q1 FY26" line followed by several
    undated Presentation/Q&A/Transcript links for that same quarter.

    Raises on a fetch failure (bad URL, network error, non-2xx status) —
    callers should catch and surface that to the user."""
    import requests
    from bs4 import BeautifulSoup

    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "lxml")

    seen_urls = set()
    seen_labels = set()
    out = []
    _CARRY_FORWARD_MAX = 6  # undated siblings after a dated line still inherit its period
    carried_period = None
    carry_remaining = 0
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
        if abs_url in seen_urls:
            continue

        ext = _file_ext(href)
        # Pure navigation ("2025", "Next") is only ever noise — but never drop
        # a real file link on the strength of its label alone.
        if not ext and _is_nav_label(text):
            continue

        # A link's own text is very often just "PDF" or "PDF(266KB)" — the
        # real description sits in a nearby heading or row label instead.
        context = _nearby_context_text(a) if _is_generic_label(text) else ""
        title = context or text or _title_from_url(abs_url)
        doc_type = _guess_doc_type(context or text, href)

        # Archive pages often repeat the same section link many times; a
        # same-title same-type *page* link adds nothing. Files are kept even
        # when titles collide (different documents legitimately share a label).
        label_key = (title.strip().lower(), doc_type)
        if not ext:
            if label_key in seen_labels:
                continue
            seen_labels.add(label_key)

        period = extract_period(context or text)
        if period:
            carried_period = period
            carry_remaining = _CARRY_FORWARD_MAX
        elif carry_remaining > 0:
            period = carried_period
            carry_remaining -= 1
        else:
            period = None

        seen_urls.add(abs_url)
        out.append({
            "title": title,
            "url": abs_url,
            "doc_type": doc_type,
            "kind": "file" if ext else "page",
            "ext": ext,
            "date": (period or {}).get("date") or "",
            "period_label": (period or {}).get("period_label") or "",
        })
        if len(out) >= max_results:
            break
    return out


def fetch_document_bytes(doc_url: str, timeout: int = 30, max_bytes: int = 40 * 1024 * 1024):
    """Download one document server-side, for bundling into a batch ZIP.
    Capped so a single unexpectedly huge file can't exhaust memory."""
    import requests
    r = requests.get(doc_url, headers={"User-Agent": USER_AGENT}, timeout=timeout, stream=True)
    r.raise_for_status()
    chunks, total = [], 0
    for chunk in r.iter_content(chunk_size=65536):
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise RuntimeError(f"file exceeds {max_bytes // (1024 * 1024)}MB cap")
        chunks.append(chunk)
    return b"".join(chunks)


def _safe_filename(title: str, ext: str, fallback: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", (title or fallback)).strip() or fallback
    name = re.sub(r"\s+", " ", name)[:120].rstrip(". ")
    return f"{name}.{ext}" if ext and not name.lower().endswith(f".{ext}") else name


def build_zip(items: list, timeout: int = 30):
    """Fetch each item ({"title","url","ext"}) and bundle into an in-memory
    ZIP. Returns (zip_bytes, ok_count, [(title, error), ...]) — a failed
    download is skipped and reported rather than aborting the whole batch."""
    import io
    import zipfile

    buf = io.BytesIO()
    failures = []
    ok = 0
    used_names = set()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for item in items:
            try:
                data = fetch_document_bytes(item["url"], timeout=timeout)
            except Exception as exc:
                failures.append((item.get("title") or item["url"], str(exc)))
                continue
            fname = _safe_filename(item.get("title", ""), item.get("ext", ""),
                                    _title_from_url(item["url"]))
            if fname.lower() in used_names:
                stem, dot, suffix = fname.rpartition(".")
                if not dot:  # no extension — rpartition puts everything in suffix
                    stem, suffix = fname, ""
                n = 2
                while f"{stem} ({n}){dot}{suffix}".lower() in used_names:
                    n += 1
                fname = f"{stem} ({n}){dot}{suffix}"
            used_names.add(fname.lower())
            zf.writestr(fname, data)
            ok += 1
    return buf.getvalue(), ok, failures
