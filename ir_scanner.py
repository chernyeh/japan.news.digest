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

DOCUMENT_EXTENSIONS = (".pdf", ".ppt", ".pptx", ".pptm", ".xls", ".xlsx", ".xlsm",
                       ".doc", ".docx", ".docm", ".csv", ".zip", ".7z")

# (keywords, guessed doc_type) — checked in order, first match wins. Mixes
# English and Japanese since most IR pages are Japanese-only or bilingual.
_TYPE_RULES = [
    (["transcript", "議事録", "トランスクリプト"], "Transcript"),
    (["q&a", "qanda", "q & a", "想定問答", "質疑応答"], "Q&A"),
    (["presentation", "説明会資料", "決算説明", "説明資料", "スライド", "slide", "決算補足"],
     "Presentation"),
    (["annual report", "integrated report", "統合報告書", "アニュアルレポート", "factbook", "fact book"],
     "Annual Report (IR)"),
    # Checked after Presentation so "決算説明資料" / "Financial Results Presentation"
    # still read as decks — these are the tanshin / results release itself.
    (["financial results", "financial summary", "results summary", "consolidated results",
      "earnings release", "決算短信", "決算概要", "決算発表"], "Financial Results"),
    (["financial data", "all financial data", "data book", "databook", "fact sheet",
      "財務データ", "決算データ", "データブック", "財務ハイライト"], "Financial Data"),
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
    """Document extension without the dot, or "" if this isn't a direct file link.

    Also reads the query string: a fair number of IR sites hand out documents
    through a download handler (".../download.cgi?f=20260730_qa.pdf") where the
    path alone gives no clue that the link is a file at all — those used to be
    dropped from a scan, or kept as an undownloadable "page" link."""
    parsed = urlparse(href)
    path = parsed.path.lower()
    for ext in DOCUMENT_EXTENSIONS:
        if path.endswith(ext):
            return ext.lstrip(".")
    query = unquote(parsed.query).lower()
    for ext in DOCUMENT_EXTENSIONS:
        # Anchored so "?view=pdfviewer" isn't mistaken for a .pdf download.
        if re.search(re.escape(ext) + r"(?:$|[&;])", query):
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
_MONTH_ABBR = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
# Longest names first so e.g. "June" isn't cut short by a "jun" match earlier
# in the alternation (both would match the same start; regex engines take
# the first alternative that matches, not the longest, unless ordered).
_MONTH_ALT = "|".join(sorted(_MONTH_NAMES, key=len, reverse=True))

_EN_QUARTER_WORDS = {
    "first": 1, "1st": 1, "second": 2, "2nd": 2, "third": 3, "3rd": 3, "fourth": 4, "4th": 4,
}


def _valid_date(year: int, month: int, day: int):
    """(iso_str, human_label) for a real calendar date, or (None, None)."""
    import datetime as _dt
    try:
        _dt.date(year, month, day)
    except ValueError:
        return None, None
    return f"{year:04d}-{month:02d}-{day:02d}", f"{_MONTH_ABBR[month]} {day}, {year}"


# "First Quarter Ended June 30, 2026" / "Fiscal Year Ended March 31, 2026" — a
# real calendar date tied to a specific quarter/FY, so this is checked first.
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

# Abbreviated fiscal-quarter labels IR sites use as a shorthand instead of
# spelling out "Ended <date>" — "FY2026 Q1", "Q1 FY26", "1Q FY26". Label
# only: no day-level date is implied by this notation.
_FY_Q_RE = re.compile(
    r'FY\s?(?P<fy1>\d{2,4})\s*Q(?P<q1>[1-4])'
    r'|Q(?P<q2>[1-4])\s*,?\s*FY\s?(?P<fy2>\d{2,4})'
    r'|(?P<qn>[1-4])Q\s?(?P<fy3>\d{2,4})\b',
    re.IGNORECASE,
)

# A bare calendar date with no fiscal-period wording at all — usually the
# document's upload/announcement date rather than the period it covers, but
# that's still exactly the kind of date-when info the user wants surfaced.
_JP_DATE_RE = re.compile(r'(?P<year>\d{4})年(?P<month>\d{1,2})月(?P<day>\d{1,2})日')
_EN_DATE_RE = re.compile(
    r'(?P<month>' + _MONTH_ALT + r')\.?\s+(?P<day>\d{1,2}),?\s+(?P<year>\d{4})',
    re.IGNORECASE,
)
_ISO_DATE_RE = re.compile(r'(?<!\d)(?P<year>20\d{2})[-/](?P<month>0?[1-9]|1[0-2])[-/](?P<day>0?[1-9]|[12]\d|3[01])(?!\d)')


def _normalize_fy(raw: str) -> int:
    fy = int(raw)
    return fy + 2000 if fy < 100 else fy


def extract_period(text: str):
    """Best-effort period/date detection from a title/context string, tried
    most-specific-first. Returns {"date": iso_str_or_None, "period_label":
    str, "period_sort": str} or None if nothing was found. period_sort is
    an ordering key only — never displayed, never persisted as a date — so
    month- and quarter-level periods can still be sorted sensibly without
    inventing a day that would end up in the saved record.

    - English "Quarter/Year Ended <Month> <Day>, <Year>" -> real ISO date.
    - Japanese "…年…月期(第N四半期)" fiscal notation -> label only (fiscal
      year-end days vary by company and can't be inferred from this text).
    - Abbreviated "FY2026 Q1" / "Q1 FY26" / "1Q26" -> label only.
    - A bare calendar date (Japanese "…年…月…日", English "Month Day, Year",
      or ISO-ish "YYYY-MM-DD") -> real ISO date; this is typically an
      upload/announcement date rather than a reporting period, but is still
      useful "when" information."""
    if not text:
        return None

    m = _EN_PERIOD_RE.search(text)
    if m:
        month = _MONTH_NAMES.get(m.group("month").lower())
        if month:
            date_iso, _ = _valid_date(int(m.group("year")), month, int(m.group("day")))
            if date_iso:
                year = m.group("year")
                q_word = m.group("q")
                qn = _EN_QUARTER_WORDS.get(q_word.lower()) if q_word else None
                label = f"Q{qn} FY{year}" if qn else f"FY{year}"
                return {"date": date_iso, "period_label": label, "period_sort": date_iso}

    m = _JP_PERIOD_RE.search(text)
    if m:
        year, month, q = m.group("year"), m.group("month"), m.group("q")
        label = f"FY{year}/{int(month):02d}"
        sort = f"{year}-{int(month):02d}"
        if q:
            qn = _JP_DIGIT_MAP.get(q, q)
            label = f"Q{qn} {label}"
            sort = f"{sort}-Q{qn}"
        return {"date": None, "period_label": label, "period_sort": sort}

    m = _FY_Q_RE.search(text)
    if m:
        fy_raw = m.group("fy1") or m.group("fy2") or m.group("fy3")
        qn = m.group("q1") or m.group("q2") or m.group("qn")
        if fy_raw and qn:
            fy = _normalize_fy(fy_raw)
            if 2000 <= fy <= 2099:
                return {"date": None, "period_label": f"Q{qn} FY{fy}", "period_sort": f"{fy}-Q{qn}"}

    for regex, is_en in ((_JP_DATE_RE, False), (_EN_DATE_RE, True), (_ISO_DATE_RE, False)):
        m = regex.search(text)
        if not m:
            continue
        raw_month = m.group("month")
        month = _MONTH_NAMES.get(raw_month.lower()) if is_en else int(raw_month)
        if not month:
            continue
        date_iso, label = _valid_date(int(m.group("year")), month, int(m.group("day")))
        if date_iso:
            return {"date": date_iso, "period_label": label, "period_sort": date_iso}

    return None


_URL_YYYYMMDD_RE = re.compile(r'(?<!\d)(?P<year>20\d{2})(?P<month>0[1-9]|1[0-2])(?P<day>0[1-9]|[12]\d|3[01])(?!\d)')
_URL_DATE_SEP_RE = re.compile(r'(?<!\d)(?P<year>20\d{2})[-_](?P<month>0?[1-9]|1[0-2])[-_](?P<day>0?[1-9]|[12]\d|3[01])(?!\d)')
_URL_YEAR_DIR_RE = re.compile(r'/(?P<year>20\d{2})/')


# A quarter marker inside a filename or path segment: "1q", "q1", "4q_" etc.
# Bounded by non-alphanumerics so "q1" doesn't match inside a random token.
_URL_QUARTER_RE = re.compile(r'(?<![a-z0-9])(?:(?P<qa>[1-4])q|q(?P<qb>[1-4]))(?![a-z0-9])', re.IGNORECASE)
# A 4-digit year anywhere in the URL, not just as its own directory.
_URL_ANY_YEAR_RE = re.compile(r'(?<!\d)(?P<year>20\d{2})(?!\d)')
# "202508" — year+month with no day. Common in IR filenames; deliberately
# yields a month-level label and NO date, since inventing a day would put a
# fabricated value in the saved record's date field.
_URL_YYYYMM_RE = re.compile(r'(?<!\d)(?P<year>20\d{2})(?P<month>0[1-9]|1[0-2])(?!\d)')


def extract_period_from_url(href: str):
    """Fallback when no period/date could be found in the link's surrounding
    text: IR document filenames very often embed a date ("20260807_qa.pdf"),
    a quarter marker ("fy25_1q_results.pdf"), or at least live under a
    fiscal-year folder (".../presentation/2025/...") even when the page text
    around them doesn't spell one out.

    Results carry "weak": True when the match pins down a year *or* a quarter
    but not both. A bare fiscal-year folder is the coarsest possible answer —
    a company's whole year of quarterly decks sits under /2025/, so calling
    them all "FY2025" is what made every Casio title look identical — and a
    bare quarter with no year is similarly partial. Callers prefer a period
    inherited from a dated neighbour over a weak match, but prefer a strong
    (self-describing) URL match over any inherited one."""
    if not href:
        return None

    filename = _href_filename(href)

    m = _URL_YYYYMMDD_RE.search(href) or _URL_DATE_SEP_RE.search(href)
    if m:
        date_iso, label = _valid_date(int(m.group("year")), int(m.group("month")), int(m.group("day")))
        if date_iso:
            return {"date": date_iso, "period_label": label, "period_sort": date_iso, "weak": False}

    # "fy26q1_deck.pdf" / "1q26_results.pdf" — the filename spells out both
    # halves itself, so trust it over the directory it happens to sit in.
    m = _FY_Q_RE.search(filename)
    if m:
        fy_raw = m.group("fy1") or m.group("fy2") or m.group("fy3")
        qn = m.group("q1") or m.group("q2") or m.group("qn")
        if fy_raw and qn:
            fy = _normalize_fy(fy_raw)
            if 2000 <= fy <= 2099:
                return {"date": None, "period_label": f"Q{qn} FY{fy}",
                        "period_sort": f"{fy}-Q{qn}", "weak": False}

    # Quarter marker in the filename + whatever year the URL carries — the
    # single most useful rescue for quarterly documents under a year folder.
    mq = _URL_QUARTER_RE.search(filename) or _URL_QUARTER_RE.search(href)
    if mq:
        qn = mq.group("qa") or mq.group("qb")
        my = _URL_YEAR_DIR_RE.search(href) or _URL_ANY_YEAR_RE.search(href)
        if my:
            year = int(my.group("year"))
            return {"date": None, "period_label": f"Q{qn} FY{year}",
                    "period_sort": f"{year}-Q{qn}", "weak": False}
        return {"date": None, "period_label": f"Q{qn}", "period_sort": f"0000-Q{qn}", "weak": True}

    m = _URL_YYYYMM_RE.search(filename)
    if m:
        year, month = int(m.group("year")), int(m.group("month"))
        return {"date": None, "period_label": f"{_MONTH_ABBR[month]} {year}",
                "period_sort": f"{year:04d}-{month:02d}", "weak": False}

    m = _URL_YEAR_DIR_RE.search(href)
    if m:
        year = int(m.group("year"))
        return {"date": None, "period_label": f"FY{year}", "period_sort": f"{year}", "weak": True}
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
    r'^(pdf|ppt[xm]?|xls[xm]?|doc[xm]?|zip|excel|word|powerpoint|'
    r'download|dl|view|open|詳細|見る|開く|ダウンロード)'
    r'\s*[\(（\[［]?\s*[\d.,]*\s*[kmg]?i?b?\s*[\)）\]］]?$',
    re.IGNORECASE,
)


def _is_generic_label(text: str) -> bool:
    return not text.strip() or bool(_GENERIC_LABEL_RE.match(text.strip()))


# A trailing size badge — "(620KB)", "[86KB]", "(1.40MB)" — and a trailing bare
# format word, which IR pages glue straight onto the link text.
# The inner run must not cross a bracket of its own, or an unbalanced label
# like "PDF(with script (1.07MB)" is matched from its *first* parenthesis and
# the whole "(with script (1.07MB)" disappears — taking with it the one word
# that tells a deck apart from the same deck with speaker notes.
_SIZE_BADGE_RE = re.compile(
    r'\s*[\(（\[［][^()\[\]（）［］]*?\d[\d.,]*\s*[kmg]?i?b[^()\[\]（）［］]*[\)）\]］]\s*$',
    re.IGNORECASE,
)

# An opening bracket with no closing bracket after it — the residue of markup
# like "PDF(with script (1.07MB)" once the size badge is gone. Drop the stray
# bracket and any format word glued in front of it, keeping the words.
_UNBALANCED_OPEN_RE = re.compile(
    r'\s*(?:pdf|ppt[xm]?|xls[xm]?|doc[xm]?|zip|excel)?\s*[\(（\[［]\s*'
    r'(?=[^()\[\]（）［］]*$)',
    re.IGNORECASE,
)
_TRAILING_FORMAT_RE = re.compile(
    r'\s*[\(（\[［]?\s*(?:pdf|ppt[xm]?|xls[xm]?|doc[xm]?|zip|excel|word|powerpoint)'
    r'\s*[\)）\]］]?\s*$',
    re.IGNORECASE,
)


def _strip_format_noise(text: str) -> str:
    """Drop the trailing "PDF (620KB)" / "[86KB]" badge from a link's own text,
    so "Management PlanPDF (620KB)" reads as "Management Plan". Only ever
    strips from the end, and never strips a label away entirely — a link whose
    text is *nothing but* a badge ("Excel [86KB]") is handed back unchanged for
    _is_generic_label to catch, so the row label is used instead."""
    out = (text or "").strip()
    for _ in range(3):
        before = out
        out = _SIZE_BADGE_RE.sub("", out).strip()
        tidied = _UNBALANCED_OPEN_RE.sub(" ", out).strip()
        if _HAS_WORD_RE.search(tidied):
            out = re.sub(r"\s{2,}", " ", tidied)
        without_format = _TRAILING_FORMAT_RE.sub("", out).strip()
        if without_format:  # never reduce the label to nothing
            out = without_format
        if out == before:
            break
    return out or (text or "").strip()


# Anything with no letters, digits or kana/kanji in it at all — a lone ")" or
# "-" left over once the anchors are excluded from a table cell.
_PUNCT_ONLY_RE = re.compile(r'^[\s\W_]+$', re.UNICODE)


def _is_meaningful(text: str) -> bool:
    """Whether a candidate title fragment actually says something. Guards the
    residual-text paths: a cell like "<a>PDF (167KB</a>)" leaves ")" behind,
    which is neither empty nor a recognised format badge and would otherwise
    sail through as a title."""
    t = (text or "").strip(" ·-—|/")
    return len(t) >= 2 and not _PUNCT_ONLY_RE.match(t) and not _is_generic_label(t)


def _loose_text(ancestor) -> str:
    """Ancestor's text with all descendant <a> elements' text excluded. A
    plain `ancestor.get_text()` plus stripping *this* link's own text once
    isn't enough: a row with several sibling format buttons in the same
    cell (e.g. three separate "PDF" download links) leaves the other
    buttons' text behind, producing garbage like "Summary of Q&A PDF PDF
    PDF" instead of the clean "Summary of Q&A" row label. Excluding every
    <a>'s text structurally, not just this one's, avoids that."""
    bits = []
    for s in ancestor.find_all(string=True):
        if s.find_parent("a") is not None:
            continue
        t = str(s).strip()
        if t:
            bits.append(t)
    return " ".join(bits)


# "Presenter: Shiro Kondo President and COO" sits between a sub-heading and its
# documents on most Japanese IR pages. It identifies a person, not a document,
# so it is skipped in favour of the heading above it.
_PRESENTER_RE = re.compile(r'^\s*(presenter|presented by|speaker|登壇者|説明者|発表者)\s*[:：]?',
                           re.IGNORECASE)
# A caption has to contain actual words — this rejects leftovers like "(1.07MB)"
# and "→", which are meaningful() but say nothing.
_HAS_WORD_RE = re.compile(r'[A-Za-z\u3040-\u30ff\u4e00-\u9fff]{3,}')


def _is_caption(text: str) -> bool:
    return bool(text) and bool(_HAS_WORD_RE.search(text)) and not _PRESENTER_RE.match(text)


def _cell_captions(cell) -> dict:
    """{id(<a>): the sub-heading it sits under, within this one cell}.

    A single IR table cell routinely lists several documents under their own
    sub-headings — "Energy Business Strategies" and its deck and script, then
    "Semiconductor Business Strategies" and its pair, and so on. Every one of
    those links has the same visible text ("Presentation Material PDF(3.51MB)"),
    and the cell's text as a whole is one undifferentiated blob, so without
    reading the headings each document ends up with an identical title
    distinguishable only by its filename stem.

    Walks the cell in document order, keeping the last heading seen. The
    heading stays with every link beneath it, so a deck and its with-script
    twin share one caption rather than only the first getting it."""
    links = cell.find_all("a", href=True)
    if len(links) < 2:
        return {}          # a lone link needs no telling apart
    out, caption = {}, ""
    for node in cell.descendants:
        if getattr(node, "name", None) == "a" and node.get("href"):
            if caption:
                out[id(node)] = caption[:120]
        elif isinstance(node, str) and node.find_parent("a") is None:
            text = re.sub(r"\s+", " ", str(node)).strip()
            if _is_caption(text):
                caption = text
    return out


def _nearby_context_text(a_tag) -> str:
    """Best-effort text to use instead of a generic link label: prefer other
    descriptive text in a nearby row/list item (common table/list layouts
    like "<tr><td>Quarterly Results</td><td><a>PDF</a></td></tr>"), falling
    back to the nearest heading before this link's containing block. The
    ancestor climb is bounded (depth and residual length) so a div-heavy
    layout with no li/tr structure doesn't walk out into unrelated page
    content and return some huge, useless blob of text."""
    for depth, ancestor in enumerate(a_tag.parents):
        if depth >= 4 or ancestor.name in (None, "body", "html"):
            break
        residual = _loose_text(ancestor).strip(" ·-—|/")
        if _is_meaningful(residual) and len(residual) <= 120:
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


def _period_context_text(a_tag, text: str) -> str:
    """Wider text pool for period/date detection than for the title.
    _nearby_context_text stops at the first ancestor residual that looks
    title-worthy — right for choosing one clean title, but a document's
    date/quarter is often split across a heading ("FY2025 Q2") and a
    sibling size badge ("(EXCEL / 230KB)") that alone doesn't reach the
    heading fallback (it already "counts" as a residual, short-circuiting
    the climb). So this always includes a couple of nearby ancestor levels
    *and* the nearest heading, unconditionally, bounded shallow so it
    doesn't pull in unrelated sibling rows/sections."""
    bits = [text or ""]
    for depth, ancestor in enumerate(a_tag.parents):
        if depth >= 2 or ancestor.name in (None, "body", "html"):
            break
        loose = _loose_text(ancestor)
        if loose:
            bits.append(loose)
    try:
        heading = a_tag.find_previous(["h1", "h2", "h3", "h4", "h5", "h6"])
    except Exception:
        heading = None
    if heading:
        heading_text = heading.get_text(strip=True)
        if heading_text:
            bits.append(heading_text)
    return " ".join(b for b in bits if b)


# ── Table structure ──────────────────────────────────────────────────────
# The single most common layout for a company IR document library is a matrix
# table: one row per document kind ("Presentation Material", "Financial
# Results", "Q&A", "All Financial Data") and one column per reporting period,
# with each cell holding just a bare "PDF (748KB)" link. Reading such a page
# link-by-link in DOM order walks *across* periods rather than along one, so
# neither the row label nor — critically — the column header ever reaches the
# link, and the undated-sibling carry-forward below actively smears the wrong
# period across a row. That is what made whole rows go missing from a period:
# they were scanned, but filed under a neighbour's quarter.
#
# So resolve each table into a real grid first and hand every link the row and
# column headings that actually describe it.


def _cell_text(cell) -> str:
    return re.sub(r"\s+", " ", cell.get_text(" ", strip=True)).strip()


def _table_grid(table):
    """[(tr, [(cell, col_start, col_end), ...]), ...] with colspan/rowspan
    resolved, so a cell can be lined up with the header above it. Rows
    belonging to a nested table are skipped — they're that table's problem."""
    grid = []
    spans = {}  # (row_idx, col) already taken by a cell spilling down from above
    rows = [tr for tr in table.find_all("tr") if tr.find_parent("table") is table]
    for r, tr in enumerate(rows):
        placed = []
        col = 0
        for cell in tr.find_all(["td", "th"], recursive=False):
            while spans.get((r, col)):
                col += 1

            def _span(attr):
                try:
                    return min(max(1, int(cell.get(attr, 1))), 64)
                except (TypeError, ValueError):
                    return 1

            cspan, rspan = _span("colspan"), _span("rowspan")
            for dr in range(1, rspan):
                for dc in range(cspan):
                    spans[(r + dr, col + dc)] = True
            placed.append((cell, col, col + cspan - 1))
            col += cspan
        grid.append((tr, placed))
    return grid


def _header_row_indices(grid) -> set:
    """Rows that label the columns: whatever is in <thead>, else the leading
    run of rows made up entirely of <th> cells."""
    thead = {i for i, (tr, _) in enumerate(grid) if tr.find_parent("thead") is not None}
    if thead:
        return thead
    leading = set()
    for i, (_, cells) in enumerate(grid):
        if cells and all(c.name == "th" for c, _, _ in cells):
            leading.add(i)
        else:
            break
    return leading


def _heading_period(label: str):
    """extract_period() for a table heading, with one extra step: headings very
    often spell out both the fiscal shorthand and the briefing date —
    "FY2026 Q1 (July 30, 2026)". extract_period stops at the first (most
    specific) form it recognises, so the quarter wins and the real date is
    dropped. Recover it here, where the text is a short page-authored heading
    rather than a wide blob of surrounding context that could contribute some
    unrelated date."""
    period = extract_period(label)
    if not period or period.get("date"):
        return period
    for regex, is_en in ((_JP_DATE_RE, False), (_EN_DATE_RE, True), (_ISO_DATE_RE, False)):
        m = regex.search(label)
        if not m:
            continue
        raw_month = m.group("month")
        month = _MONTH_NAMES.get(raw_month.lower()) if is_en else int(raw_month)
        if not month:
            continue
        date_iso, _ = _valid_date(int(m.group("year")), month, int(m.group("day")))
        if date_iso:
            return dict(period, date=date_iso)
    return period


def _column_label(grid, header_rows, col_start, col_end) -> str:
    """Heading text sitting above columns col_start..col_end. Multi-level
    headers ("FY2026" over "Q1") are joined, and a heading spanning the whole
    table applies to every column under it."""
    bits = []
    for r in sorted(header_rows):
        for cell, a, b in grid[r][1]:
            if a <= col_end and b >= col_start:
                t = _cell_text(cell)
                if t and t not in bits:
                    bits.append(t)
    return " ".join(bits)[:160]


def _row_label(cells) -> str:
    """The row's own heading — its <th>, or a leading plain cell that holds no
    links (plenty of IR tables use <td> throughout)."""
    for cell, _, _ in cells:
        if cell.name == "th":
            t = _cell_text(cell)
            if t:
                return t[:160]
    if cells:
        first = cells[0][0]
        if not first.find("a", href=True):
            return _cell_text(first)[:160]
    return ""


# A column/row header that names a period *slot* without pinning the year:
# "1Q", "Q3", "第2四半期", "Full Year", "通期", "Interim". These are the axis of
# a matrix table just as much as a dated header is — the year simply lives
# somewhere else on the page. Treating them as "not a period" is what let
# carry-forward back in and smear one quarter's date across a whole grid.
_AXIS_LABEL_RE = re.compile(
    r'^\s*(?:'
    r'(?:[1-4１２３４]\s*Q|Q\s*[1-4])'
    r'|第\s*[1-4１２３４]\s*四半期'
    r'|(?:full[\s-]?year|fy|interim|half[\s-]?year|1h|2h|h1|h2)'
    r'|通期|上期|下期|中間'
    r')\s*$',
    re.IGNORECASE,
)

_QUARTER_IN_LABEL_RE = re.compile(
    r'(?:(?P<a>[1-4])\s*Q|Q\s*(?P<b>[1-4])|第\s*(?P<c>[1-4１２３４])\s*四半期)', re.IGNORECASE)


def _axis_quarter(label: str) -> str:
    """"1Q" -> "1", "第2四半期" -> "2", "" when the label names no quarter."""
    m = _QUARTER_IN_LABEL_RE.search(label or "")
    if not m:
        return ""
    raw = m.group("a") or m.group("b") or m.group("c") or ""
    return _JP_DIGIT_MAP.get(raw, raw)


def _is_axis_label(label: str) -> bool:
    return bool(_AXIS_LABEL_RE.match((label or "").strip()))


def _year_from_links(cells) -> str:
    """The fiscal year a column is really about, taken from the dates its own
    links carry. Deliberately only its *own* links: borrowing a year from a
    neighbouring column is exactly the mistake this whole change exists to
    stop."""
    years = {}
    for cell in cells:
        for a in cell.find_all("a", href=True):
            period = extract_period_from_url(a["href"])
            sort = (period or {}).get("period_sort") or ""
            head = sort[:4]
            # extract_period_from_url writes "0000" when it found a quarter but
            # no year. Adopting that produced the nonsense label "Q2 FY0000".
            if head.isdigit() and 2000 <= int(head) <= 2099:
                years[head] = years.get(head, 0) + 1
    if not years:
        return ""
    top = max(years.values())
    winners = [y for y, n in years.items() if n == top]
    return winners[0] if len(winners) == 1 else ""


def build_table_link_context(soup) -> dict:
    """{id(<a>): {"row", "col", "cell", "mode"}} for every link inside a table.

    mode says which axis carries the reporting period, decided per table by
    seeing which one actually parses as one:
      "col" — periods across the top, document kinds down the side (Fuji
              Electric, Hitachi, Canon and most Japanese IR libraries)
      "row" — the transpose: one row per period, document kinds across the top
      ""    — neither; the table is just a layout, so nothing is claimed.
    An empty mode leaves the existing text/URL/carry-forward heuristics in
    charge, so tables that aren't period matrices behave exactly as before."""
    ctx = {}
    for table in soup.find_all("table"):
        grid = _table_grid(table)
        if not grid:
            continue
        header_rows = _header_row_indices(grid)

        entries = []          # (link, row_label, col_label, cell_text, caption)
        col_labels, row_labels = set(), set()
        cells_by_col = {}     # col_label -> [cell, ...], for year resolution
        for r, (_, cells) in enumerate(grid):
            if r in header_rows:
                continue
            row_lbl = _row_label(cells)
            for cell, a, b in cells:
                links = cell.find_all("a", href=True)
                if not links:
                    continue
                col_lbl = _column_label(grid, header_rows, a, b)
                if col_lbl:
                    col_labels.add(col_lbl)
                    cells_by_col.setdefault(col_lbl, []).append(cell)
                if row_lbl:
                    row_labels.add(row_lbl)
                cell_txt = _loose_text(cell)
                caps = _cell_captions(cell)
                for link in links:
                    entries.append((link, row_lbl, col_lbl, cell_txt, caps.get(id(link), "")))
        if not entries:
            continue

        n_col_periods = sum(1 for lbl in col_labels if extract_period(lbl))
        n_row_periods = sum(1 for lbl in row_labels if extract_period(lbl))
        # An axis of bare period slots ("1Q", "2Q", "通期") is still a period
        # axis; the year is just written elsewhere. Counting only fully-dated
        # headers is what previously dropped such tables to mode "" and handed
        # them back to carry-forward.
        n_col_axis = sum(1 for lbl in col_labels if _is_axis_label(lbl))
        n_row_axis = sum(1 for lbl in row_labels if _is_axis_label(lbl))
        if (n_col_periods or n_col_axis) and (n_col_periods + n_col_axis) >= (n_row_periods + n_row_axis):
            mode = "col"
        elif n_row_periods or n_row_axis:
            mode = "row"
        else:
            mode = ""

        # Resolve a bare quarter header against the years its own column's
        # links carry, so "1Q" becomes "Q1 FY2026" rather than nothing.
        axis_period = {}
        for lbl in col_labels if mode == "col" else ():
            # A cell spanning several period columns — a whole-year Excel under
            # an "All Financial Data" row, say — gets a joined header naming
            # every quarter it covers. It belongs to none of them individually,
            # so claim no period rather than pinning it to the first.
            if len(_QUARTER_IN_LABEL_RE.findall(lbl)) > 1:
                continue
            qn = _axis_quarter(lbl)
            if not qn or extract_period(lbl):
                continue
            year = _year_from_links(cells_by_col.get(lbl, []))
            if year:
                axis_period[lbl] = {"date": None, "period_label": f"Q{qn} FY{year}",
                                    "period_sort": f"{year}-Q{qn}"}
            else:
                # Honest and partial beats confident and wrong: a bare "Q1" is
                # far better than a neighbouring column's date.
                axis_period[lbl] = {"date": None, "period_label": f"Q{qn}",
                                    "period_sort": f"0000-Q{qn}"}

        # Any table with headings is a grid, and reading a grid in DOM order
        # walks across periods. Carry-forward must stay off for every link in
        # it, whatever mode we settled on.
        structured = bool(header_rows or row_labels or col_labels)

        for link, row_lbl, col_lbl, cell_txt, cap in entries:
            ctx[id(link)] = {"row": row_lbl, "col": col_lbl, "cell": cell_txt, "caption": cap,
                             "mode": mode, "structured": structured,
                             "axis": axis_period.get(col_lbl if mode == "col" else row_lbl)}
    return ctx


# Raising this from 120 mattered: a company that publishes five document kinds
# per quarter and keeps a few years of history online blows past it easily, and
# the scan used to stop dead at the cap with no hint that it had — the user
# just saw a short, arbitrarily-cut list. Scans now report truncation instead.
MAX_SCAN_RESULTS = 500


class ScanResults(list):
    """The list of result dicts, plus whether the scan hit its cap. A plain
    list subclass so every existing caller keeps working unchanged."""

    def __init__(self, items=(), truncated=False, limit=MAX_SCAN_RESULTS):
        super().__init__(items)
        self.truncated = truncated
        self.limit = limit


def scan_page_for_documents(url: str, max_results: int = MAX_SCAN_RESULTS,
                            timeout: int = 20) -> "ScanResults":
    """Fetch `url` and return up to max_results candidate links as
    [{"title", "url", "doc_type", "kind", "ext"}, ...], deduped by URL. The
    returned list also carries .truncated / .limit so the caller can tell the
    user the page had more documents than one scan will show.

    kind is "file" for a direct document download (.pdf/.pptx/…) or "page"
    for an HTML link that merely looks document-related — the caller is
    expected to surface that distinction, since only "file" entries can be
    downloaded directly. date/period_label carry a best-effort fiscal period
    (see extract_period).

    Two layouts are handled separately. In a *matrix table* (see
    build_table_link_context) each link is described by its row and column
    headings, which is the whole truth about it. Everywhere else — flat lists
    of links under dated headings — undated siblings inherit the most recently
    seen period for a bounded run, since IR archive pages commonly show one
    dated "Summary of Consolidated Results for Q1 FY26" line followed by
    several undated Presentation/Q&A/Transcript links for that same quarter.

    Raises on a fetch failure (bad URL, network error, non-2xx status) —
    callers should catch and surface that to the user."""
    import requests
    from bs4 import BeautifulSoup

    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "lxml")

    table_ctx = build_table_link_context(soup)

    seen_urls = set()
    seen_labels = set()
    by_url = {}
    out = []
    truncated = False
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
        ext = _file_ext(href)
        # Pure navigation ("2025", "Next") is only ever noise — but never drop
        # a real file link on the strength of its label alone.
        if not ext and _is_nav_label(text):
            continue

        if len(out) >= max_results:
            truncated = True
            break

        # ── What this table cell's row and column say about the link ──────
        tctx = table_ctx.get(id(a)) or {}
        mode = tctx.get("mode", "")
        structured = tctx.get("structured", False)
        row_lbl, col_lbl = tctx.get("row", ""), tctx.get("col", "")
        if mode == "col":
            table_period, label = _heading_period(col_lbl), row_lbl
        elif mode == "row":
            table_period, label = _heading_period(row_lbl), col_lbl
        else:
            table_period, label = None, (row_lbl or col_lbl)
        # A bare "1Q" header yields no period on its own; the axis resolution
        # in build_table_link_context pairs it with its own column's year.
        table_period = table_period or tctx.get("axis")
        if not _is_meaningful(label):
            label = ""

        # A link's own text is very often just "PDF" or "PDF(266KB)" — the
        # real description sits in a nearby heading or row label instead.
        own = _strip_format_noise(text)
        if not _is_meaningful(own):
            own = ""
        cell_text = tctx.get("cell", "")
        if not _is_meaningful(cell_text):
            cell_text = ""
        detail = own or cell_text

        # The sub-heading this link sits under, where its cell lists several
        # documents. Appended rather than substituted: "Presentation Material"
        # is still what the row calls it, and the caption is what tells this
        # one apart from the four identical decks beside it.
        caption = tctx.get("caption", "")
        if caption and _is_meaningful(caption):
            low = caption.lower()
            if low not in (detail or "").lower() and low not in (label or "").lower():
                detail = f"{detail} · {caption}" if detail else caption

        # The row/column heading names the document kind; the link's own text
        # tells apart several links sharing one cell ("Opening remarks by the
        # COO" vs "Management Plan"). Keep both when both say something.
        if label and detail:
            if label.lower() in detail.lower():
                title = detail
            elif detail.lower() in label.lower():
                title = label
            else:
                title = f"{label} — {detail}"
        else:
            title = label or detail

        context = ""
        if not title:
            # Not in a table (or a table that told us nothing): fall back to
            # the surrounding row/heading text, then to the filename — a
            # generic own label ("Download", "PDF") is a worse title than
            # "fy26q1_transcript.pdf" turned into "fy26q1 transcript".
            context = _nearby_context_text(a) if _is_generic_label(text) else ""
            if context:
                title = context
            elif not _is_generic_label(text):
                title = text
            else:
                title = _title_from_url(abs_url) or text

        doc_type = _guess_doc_type(
            " ".join(t for t in (label, cell_text, own or context or text) if t), href)

        # Archive pages often repeat the same section link many times; a
        # same-title same-type *page* link adds nothing. Files are kept even
        # when titles collide (different documents legitimately share a label).
        label_key = (title.strip().lower(), doc_type)
        if not ext:
            if label_key in seen_labels:
                continue
            seen_labels.add(label_key)

        # Period detection always looks at the surrounding row/heading, not
        # just when the title itself needed it — a link can have a perfectly
        # good own title ("Presentation Material") while the date/quarter it
        # covers still only exists in a sibling cell or nearby heading.
        #
        # Precedence, best evidence first:
        #   1. the heading of the table row/column this cell sits in
        #   2. the document's own surrounding text
        #   3. a *strong* URL match (filename pins down year and quarter/day)
        #   4. a period inherited from a recent dated neighbour
        #   5. a *weak* URL match (bare year folder, or quarter with no year)
        # 4 must outrank 5: a page listing one dated summary followed by
        # undated siblings gives those siblings a full "Q1 FY2026" with a
        # real date, which beats the bare "Q1" their filenames imply.
        url_period = extract_period_from_url(href)
        text_period = extract_period(_period_context_text(a, detail or text))
        if table_period:
            period, period_conf = table_period, 4
        elif text_period:
            period, period_conf = text_period, 3
        elif url_period and not url_period.get("weak"):
            period, period_conf = url_period, 2
        else:
            period, period_conf = None, -1

        if structured:
            # Every table with headings is a grid, and reading a grid in DOM
            # order walks *across* periods rather than along one. A neighbour
            # is not evidence about this cell, it is the cell next door — this
            # is what put 2Q documents and a whole-table Excel under 1Q's date.
            if period is None:
                period, period_conf = url_period, (0 if url_period else -1)
            carried_period, carry_remaining = None, 0
        elif period is not None:
            carried_period = period
            carry_remaining = _CARRY_FORWARD_MAX
        elif carry_remaining > 0:
            period, period_conf = carried_period, 1
            carry_remaining -= 1
        else:
            # Weak URL matches apply to this item only — they're too coarse
            # to be worth propagating onto the items that follow.
            period, period_conf = url_period, (0 if url_period else -1)

        entry = {
            "title": title,
            "url": abs_url,
            "doc_type": doc_type,
            "kind": "file" if ext else "page",
            "ext": ext,
            "date": (period or {}).get("date") or "",
            "period_label": (period or {}).get("period_label") or "",
            "period_sort": (period or {}).get("period_sort") or "",
            "_period_conf": period_conf,
            "_title_conf": 2 if (structured and label) else (1 if title else 0),
        }
        prior = by_url.get(abs_url)
        if prior is None:
            by_url[abs_url] = entry
            out.append(entry)
        else:
            # The same document is very often listed twice on one IR page —
            # once in the matrix at the top, again in a per-briefing block
            # further down — and the two listings know different things. The
            # matrix names the document ("Financial Results"); the briefing
            # block dates it ("announced on July 30, 2026"). Dropping whichever
            # came second threw half of that away, so merge instead: one row
            # per URL, each field taken from the listing that knew it best.
            _merge_occurrence(prior, entry)

    # Applied only once the merge has settled which period and title won —
    # baking the label in per-occurrence would stamp a losing period onto a
    # title that a later listing then replaced.
    for entry in out:
        label = entry.get("period_label") or ""
        if label and label not in entry["title"]:
            entry["title"] = f"{label} — {entry['title']}"
        entry.pop("_period_conf", None)
        entry.pop("_title_conf", None)
    return ScanResults(disambiguate_titles(out), truncated=truncated, limit=max_results)


def _merge_occurrence(prior: dict, other: dict):
    """Fold a second listing of the same URL into the first, field by field."""
    if other["_period_conf"] > prior["_period_conf"]:
        prior["date"] = other["date"]
        prior["period_label"] = other["period_label"]
        prior["period_sort"] = other["period_sort"]
        prior["_period_conf"] = other["_period_conf"]
    elif other["_period_conf"] == prior["_period_conf"] and other["date"] and not prior["date"]:
        # Same class of evidence, but one listing pins a calendar date and the
        # other only names the quarter. Take the date and keep the label: a
        # matrix column says "Q1 FY2026" for five documents, and the briefing
        # block below dates three of them. Adopting the date wholesale would
        # relabel those three "Jul 30, 2026" and split one quarter across two
        # groups — precisely the scattering this is meant to end.
        prior["date"] = other["date"]
    if other["_title_conf"] > prior["_title_conf"]:
        prior["title"] = other["title"]
        prior["_title_conf"] = other["_title_conf"]
    # "Other" is the catch-all, so anything specific beats it.
    if prior["doc_type"] == "Other" and other["doc_type"] != "Other":
        prior["doc_type"] = other["doc_type"]
    # A listing that reached the file directly wins over one that only linked
    # to a landing page for it.
    if not prior["ext"] and other["ext"]:
        prior["ext"], prior["kind"] = other["ext"], "file"


def disambiguate_titles(results: list) -> list:
    """Make every title in a scan tell its documents apart.

    Period detection is best-effort, so a page can still end up with several
    entries reading exactly the same — a company's four quarterly result
    decks all filed under one /2025/ folder become four identical
    "FY2025 — Consolidated Financial Results" rows, which is useless for
    picking the right one. Any title appearing more than once gets a
    discriminator drawn from its own filename (the part that actually
    differs), falling back to a counter if even the filenames match.

    Applied to the whole result set at once, so *all* members of a colliding
    group get a discriminator rather than every copy after the first —
    otherwise the set reads as one "real" entry plus some annotated
    duplicates."""
    counts = {}
    for r in results:
        counts[r["title"]] = counts.get(r["title"], 0) + 1

    used = set()
    for r in results:
        if counts[r["title"]] < 2:
            used.add(r["title"])
            continue
        stem = _title_from_url(r["url"])
        candidate = f'{r["title"]} · {stem}' if stem and stem.lower() not in r["title"].lower() else r["title"]
        if candidate in used:
            base, n = candidate, 2
            while candidate in used:
                candidate = f"{base} ({n})"
                n += 1
        r["title"] = candidate
        used.add(candidate)
    return results


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
