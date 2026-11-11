"""
yuho_extract.py — management, board, ownership, cross-holdings and the capex
plan, read out of a company's 有価証券報告書 (yuho) on a button press.

Nothing about who runs a Japanese company, who owns it, what it holds in
other companies and where it says it will spend next year's capex is in any
feed. All of it is in the yuho, once a year, in six sections:

    第3-1 設備投資等の概要          capex actually spent, by segment
    第3-3 設備の新設、除却等の計画   the capex plan: site, segment, amount, dates
    第4-1 株式等の状況 (所有者別)    foreign / treasury / holder counts (numbers)
    第4-1 大株主の状況              top-ten holders
    第4-4 役員の状況                every director: age, career, tenure, shares
    第4-4 株式の保有状況            cross-shareholdings, name by name
    第4-4 役員の報酬等              pay by class, individuals over ¥100m
    第1-5 従業員の状況              headcount, age, tenure, average salary (numbers)

EDINET serves every yuho's tagged XBRL values as a CSV zip (documents
endpoint, type=5). The numeric facts come straight off that file; the six
narrative sections are HTML text blocks inside it, which Claude turns into
JSON against a fixed schema — the same pattern as qa_extract, one call per
block so that one failure does not lose the others.

Sections are found by the tag's Japanese label as well as its element ID,
because the exact IDs vary between taxonomy years while the labels do not.

Only the extracted JSON is stored (data/yuho_extracts.json), never the
document. The store is per company, one entry per fiscal year, so a second
year's read gives the year-on-year lines.
"""

import csv
import io
import json
import re
import zipfile
from html.parser import HTMLParser

import models
import capital_events as CE

EDINET_API_BASE = "https://api.edinet-fsa.go.jp/api/v2"
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
YUHO_DOC_TYPE = "120"
STORE_PATH = "data/yuho_extracts.json"

MODEL = models.DOC_EXTRACT_MODEL
FALLBACK_MODEL = models.DOC_EXTRACT_FALLBACK_MODEL
INPUT_USD_PER_MTOK, OUTPUT_USD_PER_MTOK = models.price(MODEL)

# How much of a text block goes to the model. The officers table of a large
# board runs to ~40k characters; the cross-holdings table lists up to sixty
# names. Anything past this is almost certainly boilerplate.
MAX_BLOCK_CHARS = 120_000


class DocumentNotAvailable(Exception):
    pass


# ── 1. The filing to read ────────────────────────────────────────────────

def yuho_filings(edinet_rows: list, code: str) -> list:
    """The company's yuhos in the filing index, newest first. Amendments
    (訂正有価証券報告書, docTypeCode 130) are left out: they restate a few
    pages, not the sections read here."""
    out = []
    for r in edinet_rows or ():
        if str(r.get("DocTypeCode") or "") != YUHO_DOC_TYPE:
            continue
        if str(r.get("SecCode") or "")[:4] != str(code)[:4]:
            continue
        out.append(r)
    return sorted(out, key=lambda r: (r.get("PeriodEnd") or "", r.get("SubmitDateTime") or ""),
                  reverse=True)


def fy_label(period_end: str) -> str:
    """'FY2026' for a period ending 2026-03-31 — the app's own convention,
    the fiscal year named by the calendar year it ends in."""
    try:
        return f"FY{int(str(period_end)[:4])}"
    except (TypeError, ValueError):
        return ""


# ── 2. Fetching and parsing the CSV zip ──────────────────────────────────

def fetch_csv_zip(doc_id: str, api_key: str, timeout: int = 60) -> bytes:
    """The XBRL-to-CSV zip for one filing (type=5). The key goes in the query
    string, as edinet.fetch_edinet_document_bytes does; callers keep it
    server-side."""
    import requests
    url = f"{EDINET_API_BASE}/documents/{doc_id}"
    params = {"type": "5", "Subscription-Key": api_key}
    r = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    if r.status_code == 404:
        raise DocumentNotAvailable(f"No CSV data for {doc_id}")
    if r.status_code != 200:
        raise RuntimeError(f"EDINET HTTP {r.status_code}")
    body = r.content or b""
    if len(body) < 64 or not body.startswith(b"PK"):
        # A JSON body here is EDINET's own error (wrong key, no such
        # document) rather than a zip.
        try:
            msg = json.loads(body.decode("utf-8", "replace")).get("message", "")
        except Exception:
            msg = ""
        raise DocumentNotAvailable(msg or f"EDINET returned no CSV zip for {doc_id}")
    return body


def _decode(raw: bytes) -> str:
    for enc in ("utf-16", "utf-8-sig", "cp932"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", "replace")


# The CSV's columns, in EDINET's order. Header names are matched loosely so a
# stray space or a renamed column does not break the read.
_COLS = (("element", ("要素ID",)), ("label", ("項目名",)), ("context", ("コンテキストID",)),
         ("rel_year", ("相対年度",)), ("cons", ("連結・個別", "連結・個別")),
         ("period", ("期間・時点",)), ("unit_id", ("ユニットID",)), ("unit", ("単位",)),
         ("value", ("値",)))


def parse_csv_text(text: str) -> list:
    """Rows of {element, label, context, rel_year, cons, period, unit_id,
    unit, value} from one CSV file's text. Tab- or comma-separated."""
    text = text.lstrip("﻿")
    first = text.split("\n", 1)[0]
    delim = "\t" if first.count("\t") >= first.count(",") else ","
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    try:
        header = next(reader)
    except StopIteration:
        return []
    idx = {}
    for key, names in _COLS:
        for i, h in enumerate(header):
            if any(n in (h or "") for n in names):
                idx[key] = i
                break
    if "element" not in idx or "value" not in idx:
        return []
    out = []
    for row in reader:
        if not row or len(row) <= idx["value"]:
            continue
        out.append({k: (row[i] if i < len(row) else "") for k, i in idx.items()})
    return out


def parse_csv_zip(data: bytes) -> list:
    """Every row of every CSV in the zip. The yuho's own report is the
    jpcrp file; audit reports (jpaud) ride along and are harmless."""
    rows = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for name in sorted(z.namelist()):
            if not name.lower().endswith(".csv"):
                continue
            if "/jpaud" in name or name.startswith("jpaud"):
                continue
            rows.extend(parse_csv_text(_decode(z.read(name))))
    return rows


# ── 3. Picking the facts and the text blocks ─────────────────────────────

def _current(row: dict) -> bool:
    """A value for the year being reported, not the prior-year comparative."""
    ctx = row.get("context") or ""
    rel = row.get("rel_year") or ""
    if rel:
        return rel.startswith("当期")
    return ctx.startswith("CurrentYear") or ctx.startswith("FilingDate")


def _consolidated(row: dict) -> bool:
    """The context ID is the reliable signal (…_NonConsolidatedMember is the
    parent); the 連結・個別 column is the fallback where the ID is bare."""
    ctx = row.get("context") or ""
    if "NonConsolidated" in ctx:
        return False
    cons = row.get("cons") or ""
    if cons and "個別" in cons and "連結" not in cons:
        return False
    return True


def _plain_context(row: dict) -> bool:
    """No segment / class / member qualifier on the context — the company
    total rather than one of its parts."""
    ctx = row.get("context") or ""
    return "Member" not in ctx or ctx.endswith("NonConsolidatedMember")


def _num(v):
    try:
        f = float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return f if f == f else None


# (key, element-ID fragments, label fragments). Either match counts; the
# label is the fallback for a taxonomy year whose IDs differ.
_FACTS = (
    ("employees", ("NumberOfEmployees",), ("従業員数",)),
    ("avg_age", ("AverageAgeYears",), ("平均年齢",)),
    ("avg_tenure", ("AverageLengthOfServiceYears",), ("平均勤続年数",)),
    ("avg_salary", ("AverageAnnualSalary",), ("平均年間給与",)),
    ("shareholders", ("NumberOfShareholders",), ("株主数",)),
    ("cross_issues", ("NumberOfIssuesSpecifiedInvestmentShares",
                      "NumberOfIssuesInvestmentSharesHeldForPurposesOtherThanPureInvestment"),
     ("特定投資株式", "銘柄数")),
    ("cross_book_value", ("TotalAmountOnBalanceSheetSpecifiedInvestmentShares",
                          "TotalAmountOnBalanceSheetInvestmentSharesHeldForPurposesOtherThanPureInvestment"),
     ("特定投資株式", "貸借対照表計上額")),
)


def numeric_facts(rows: list) -> dict:
    """{key: {"value", "unit", "basis"}} for the facts a yuho tags as numbers.
    Employees come consolidated and parent-only; age, tenure and salary are
    parent-only by definition (the yuho reports them for the filing company)."""
    out = {}

    def take(key, row, basis):
        v = _num(row.get("value"))
        if v is None:
            return
        slot = out.setdefault(key, {})
        slot.setdefault(basis, {"value": v, "unit": row.get("unit") or "",
                                "label": row.get("label") or ""})

    for row in rows:
        el = row.get("element") or ""
        lbl = row.get("label") or ""
        for key, el_parts, lbl_parts in _FACTS:
            hit = any(p in el for p in el_parts) or all(p in lbl for p in lbl_parts)
            if not hit or not _current(row) or not _plain_context(row):
                continue
            if key == "cross_issues" and ("銘柄数" not in lbl and "NumberOfIssues" not in el):
                continue
            if key == "cross_book_value" and ("計上額" not in lbl and "AmountOnBalanceSheet" not in el):
                continue
            take(key, row, "consolidated" if _consolidated(row) else "parent")
    # Foreign ownership: the 所有者別状況 table carries a share-count ratio for
    # 外国法人等 (and separately for individuals among them). The combined
    # figure is the row whose label names the group and a ratio.
    best = None
    for row in rows:
        lbl = row.get("label") or ""
        el = row.get("element") or ""
        if "外国法人等" not in lbl and "ForeignCorporations" not in el:
            continue
        if "割合" not in lbl and "Ratio" not in el and "Percentage" not in el:
            continue
        if "個人" in lbl or "Individuals" in el or "うち" in lbl:
            continue
        v = _num(row.get("value"))
        if v is None or not _current(row):
            continue
        best = v if best is None else best
    if best is not None:
        out["foreign_pct"] = {"parent": {"value": best if best > 1 else best * 100,
                                         "unit": "%", "label": "外国法人等 所有株式数の割合"}}
    return out


# (kind, element-ID fragments, label fragments) — a hint, worth a few points.
_BLOCKS = (
    ("officers", ("InformationAboutOfficers", "InformationAboutDirectorsAndCorporateAuditors"),
     ("役員の状況",)),
    ("major_shareholders", ("MajorShareholders",), ("大株主の状況",)),
    ("share_status", ("ShareholdingByShareholderCategory", "StatusOfSharesByOwner"),
     ("所有者別状況",)),
    ("cross_holdings", ("InvestmentSharesHeld", "StatusOfSharesHeld", "ShareholdingStatus"),
     ("株式の保有状況",)),
    ("capex_plan", ("PlannedAdditions", "PlanForNewAdditions", "NewAdditionsRetirements"),
     ("設備の新設", "計画")),
    ("capex_overview", ("OverviewOfCapitalExpenditures",), ("設備投資等の概要",)),
    ("exec_pay", ("OfficersRemuneration", "RemunerationOfOfficers", "DirectorsRemuneration"),
     ("役員の報酬等",)),
)

# What each section's own text contains, whatever its tag is called. The
# first real filing read showed the tag names and labels are not what the
# taxonomy documentation suggests, so the text decides: an officers table
# always has 略歴 and 所有株式数, a cross-holdings table 特定投資株式, a capex
# plan 投資予定 and 着手 / 完了. Weighted keywords; a block is a candidate for
# a kind at MIN_SCORE, and the winner is the block most specific to that
# kind (its score less the best other kind's), longest on a tie — so the
# section's own sub-block beats a parent block that contains everything.
_SIGNATURES = {
    "officers": (("略歴", 3), ("所有株式数", 1), ("任期", 1), ("役職名", 1), ("生年月日", 1),
                 ("社外取締役", 1)),
    "major_shareholders": (("大株主", 3), ("氏名又は名称", 1), ("住所", 1), ("発行済株式", 1),
                           ("自己株式を除く", 1), ("信託口", 1)),
    "share_status": (("所有者別状況", 3), ("外国法人等", 2), ("政府及び地方公共団体", 2),
                     ("金融商品取引業者", 1), ("単元未満", 1)),
    "cross_holdings": (("特定投資株式", 3), ("政策保有", 2), ("保有目的", 1), ("貸借対照表計上額", 1),
                       ("純投資目的", 1), ("当社の株式の保有の有無", 2)),
    "capex_plan": (("投資予定", 3), ("既支払額", 2), ("資金調達方法", 2), ("設備の新設", 2),
                   ("着手", 1), ("完了", 1), ("完成後の増加能力", 2)),
    "capex_overview": (("設備投資等の概要", 3), ("設備投資", 1), ("当連結会計年度", 1), ("実施", 1),
                       ("主な設備投資", 2)),
    "exec_pay": (("役員区分", 3), ("報酬等の総額", 3), ("報酬等の種類別", 2), ("業績連動", 1),
                 ("対象となる役員の員数", 2), ("連結報酬等", 2)),
}
MIN_SCORE = 3
HINT_SCORE = 3


def _hint_kind(element: str, label: str) -> str:
    for kind, el_parts, lbl_parts in _BLOCKS:
        if any(p in element for p in el_parts) or all(p in label for p in lbl_parts):
            return kind
    return ""


def _scores(text: str, element: str = "", label: str = "") -> dict:
    out = {}
    hint = _hint_kind(element, label)
    for kind, words in _SIGNATURES.items():
        sc = sum(w for word, w in words if word in text)
        if kind == hint:
            sc += HINT_SCORE
        if sc:
            out[kind] = sc
    return out


def catalog(rows: list) -> list:
    """Every text block in the filing's current-year context: element, label,
    size, and the kind it was taken for (or ''). Stored with the entry so a
    filing whose sections went unread can be diagnosed from the store
    without the document."""
    out = []
    for row in rows:
        el = row.get("element") or ""
        if not el.endswith("TextBlock"):
            continue
        ctx = row.get("context") or ""
        if ctx.startswith("Prior"):
            continue
        val = row.get("value") or ""
        if not val.strip():
            continue
        text = html_to_text(val)
        sc = _scores(text, el, row.get("label") or "")
        best = max(sc, key=sc.get) if sc else ""
        out.append({"element": el.split(":")[-1][:80], "label": (row.get("label") or "")[:60],
                    "chars": len(text), "scores": sc,
                    "kind": best if sc.get(best, 0) >= MIN_SCORE else "",
                    "_text": text, "_html": val})
    out.sort(key=lambda c: -c["chars"])
    return out


def text_blocks(rows: list) -> dict:
    """{kind: html} — for each section kind, the block most specific to it."""
    cands = catalog(rows)
    out, best = {}, {}
    for c in cands:
        for kind, sc in c["scores"].items():
            if sc < MIN_SCORE:
                continue
            other = max((v for k, v in c["scores"].items() if k != kind), default=0)
            key = (sc - other, c["chars"])
            if kind not in best or key > best[kind]:
                best[kind] = key
                out[kind] = c["_html"]
    return out


class _Flattener(HTMLParser):
    """HTML → text that keeps a table's structure: one row per line, cells
    separated by ' | '. Enough for the model to read a career history or a
    holdings table without the markup."""

    def __init__(self):
        super().__init__()
        self.parts, self._cell, self._in_cell = [], [], False
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("style", "script"):
            self._skip += 1
        elif tag in ("td", "th"):
            self._in_cell, self._cell = True, []
        elif tag == "br":
            (self._cell if self._in_cell else self.parts).append("\n")
        elif tag in ("tr",):
            self.parts.append("\n")
        elif tag in ("p", "div", "li", "h1", "h2", "h3", "h4", "table"):
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("style", "script"):
            self._skip = max(0, self._skip - 1)
        elif tag in ("td", "th"):
            self.parts.append(" ".join("".join(self._cell).split()) + " | ")
            self._in_cell, self._cell = False, []
        elif tag == "tr":
            self.parts.append("\n")

    def handle_data(self, data):
        if self._skip:
            return
        (self._cell if self._in_cell else self.parts).append(data)

    def text(self) -> str:
        raw = "".join(self.parts)
        lines = [re.sub(r"[ \t　]+", " ", ln).strip(" |") for ln in raw.split("\n")]
        lines = [ln for ln in lines if ln]
        return "\n".join(lines)


def html_to_text(html: str) -> str:
    p = _Flattener()
    try:
        p.feed(html or "")
        p.close()
    except Exception:
        return re.sub(r"<[^>]+>", " ", html or "")
    return p.text()


# ── 4. What the model is asked for ───────────────────────────────────────

BACKGROUND_TAGS = ("finance_accounting", "sales_marketing", "engineering_rd", "manufacturing",
                   "overseas", "legal", "hr_admin", "planning_strategy", "ex_parent_group",
                   "ex_bank", "ex_government", "ex_trading_house", "academic",
                   "other_company_executive", "founder_family", "consulting")
ROLE_KINDS = ("representative_director", "director", "outside_director",
              "audit_committee_member", "corporate_auditor", "outside_auditor",
              "executive_officer", "other")
HOLDER_KINDS = ("parent_or_group", "trust_bank_nominee", "foreign_nominee", "bank_insurer",
                "founder_family", "employee_plan", "treasury", "business_partner", "other")
PURPOSE_KINDS = ("business_relationship", "financing_relationship", "alliance",
                 "quantified_effect_stated", "other")


def _schema_officers():
    return {
        "type": "object", "additionalProperties": False,
        "required": ["as_of", "officers", "notes"],
        "properties": {
            "as_of": {"type": "string", "description": "The date the table is as of, as "
                      "the document states it (e.g. '2026年6月24日現在'); empty if absent."},
            "officers": {"type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "required": ["name", "title", "role_kind", "outside", "age", "birth_year",
                             "joined_company_year", "director_since_year", "is_ceo",
                             "is_chair", "is_cfo", "career_summary", "background_tags",
                             "shares_held", "term_end"],
                "properties": {
                    "name": {"type": "string"},
                    "title": {"type": "string", "description": "役職名 as printed."},
                    "role_kind": {"type": "string", "enum": list(ROLE_KINDS)},
                    "outside": {"type": "boolean",
                                "description": "Marked 社外 (outside director / auditor)."},
                    "age": {"type": "integer", "description": "0 if not stated."},
                    "birth_year": {"type": "integer", "description": "Western year; 0 if absent."},
                    "joined_company_year": {"type": "integer",
                                            "description": "Year they joined this company "
                                            "(入社). 0 for an outsider or if not stated."},
                    "director_since_year": {"type": "integer",
                                            "description": "Year first appointed a director "
                                            "(取締役就任) of this company; 0 if not stated."},
                    "is_ceo": {"type": "boolean",
                               "description": "社長 / CEO / 代表執行役社長."},
                    "is_chair": {"type": "boolean", "description": "会長 / Chairman."},
                    "is_cfo": {"type": "boolean", "description": "CFO / 財務担当 / 経理担当."},
                    "career_summary": {"type": "string",
                                       "description": "In English, at most 50 words: prior "
                                       "employers and the functions held, in order."},
                    "background_tags": {"type": "array", "items": {
                        "type": "string", "enum": list(BACKGROUND_TAGS)},
                        "description": "Every tag the career history supports."},
                    "shares_held": {"type": "integer",
                                    "description": "所有株式数, in shares (not thousands); "
                                    "0 if none or not stated."},
                    "term_end": {"type": "string", "description": "任期 end as printed; "
                                 "empty if absent."},
                },
            }},
            "notes": {"type": "string", "description": "Anything the table says that the "
                      "fields cannot hold, e.g. a planned CEO change. Empty if none."},
        },
    }


def _schema_shareholders():
    return {
        "type": "object", "additionalProperties": False,
        "required": ["as_of", "holders", "treasury_shares", "notes"],
        "properties": {
            "as_of": {"type": "string"},
            "holders": {"type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "required": ["name", "shares", "pct", "kind"],
                "properties": {
                    "name": {"type": "string"},
                    "shares": {"type": "integer", "description": "In shares, not thousands."},
                    "pct": {"type": "number",
                            "description": "発行済株式（自己株式を除く）総数に対する割合, in percent."},
                    "kind": {"type": "string", "enum": list(HOLDER_KINDS)},
                },
            }},
            "treasury_shares": {"type": "integer",
                                "description": "自己株式 as stated in the notes; 0 if absent."},
            "notes": {"type": "string"},
        },
    }


def _schema_cross():
    return {
        "type": "object", "additionalProperties": False,
        "required": ["policy_summary", "total_issues", "total_book_value_jpy", "issues",
                     "reduction_stated", "notes"],
        "properties": {
            "policy_summary": {"type": "string",
                               "description": "In English, at most 60 words: the company's "
                               "stated policy on holding other companies' shares, and any "
                               "commitment to reduce them."},
            "total_issues": {"type": "integer",
                             "description": "Number of 特定投資株式 issues (純投資目的以外); "
                             "0 if not stated."},
            "total_book_value_jpy": {"type": "number",
                                     "description": "Their total balance-sheet value in yen "
                                     "(convert from 百万円); 0 if not stated."},
            "issues": {"type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "required": ["name", "shares", "book_value_jpy", "purpose", "purpose_kind",
                             "holds_our_shares", "change"],
                "properties": {
                    "name": {"type": "string"},
                    "shares": {"type": "integer", "description": "Current-year shares held."},
                    "book_value_jpy": {"type": "number",
                                       "description": "Current-year 貸借対照表計上額 in yen."},
                    "purpose": {"type": "string",
                                "description": "The stated purpose, in English, at most "
                                "25 words."},
                    "purpose_kind": {"type": "string", "enum": list(PURPOSE_KINDS),
                                     "description": "'quantified_effect_stated' only when "
                                     "a concrete figure for the holding's benefit is given."},
                    "holds_our_shares": {"type": "boolean",
                                         "description": "当社の株式の保有の有無 = 有."},
                    "change": {"type": "string",
                               "enum": ["increased", "decreased", "unchanged", "new", "sold",
                                        "unknown"]},
                },
            }, "description": "Every named issue in the 特定投資株式 table, largest first."},
            "reduction_stated": {"type": "boolean",
                                 "description": "The policy says holdings will be reduced."},
            "notes": {"type": "string"},
        },
    }


def _schema_capex():
    return {
        "type": "object", "additionalProperties": False,
        "required": ["actual_total_jpy", "actual_by_segment", "plan", "plan_total_jpy",
                     "plan_paid_jpy", "funding_summary", "notes"],
        "properties": {
            "actual_total_jpy": {"type": "number",
                                 "description": "Capex spent in the year just reported, in "
                                 "yen (convert from 百万円); 0 if not stated."},
            "actual_by_segment": {"type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "required": ["segment", "amount_jpy", "what"],
                "properties": {"segment": {"type": "string"},
                               "amount_jpy": {"type": "number"},
                               "what": {"type": "string",
                                        "description": "In English, at most 20 words."}},
            }},
            "plan": {"type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "required": ["site", "location", "segment", "description", "planned_total_jpy",
                             "paid_to_date_jpy", "funding", "start", "finish", "capacity_note"],
                "properties": {
                    "site": {"type": "string", "description": "会社名・事業所名."},
                    "location": {"type": "string"},
                    "segment": {"type": "string"},
                    "description": {"type": "string",
                                    "description": "設備の内容, in English, at most 20 words."},
                    "planned_total_jpy": {"type": "number", "description": "投資予定金額 総額 in yen."},
                    "paid_to_date_jpy": {"type": "number", "description": "既支払額 in yen; 0 if none."},
                    "funding": {"type": "string", "description": "資金調達方法 as printed."},
                    "start": {"type": "string", "description": "着手 year-month as printed."},
                    "finish": {"type": "string", "description": "完了予定 year-month as printed."},
                    "capacity_note": {"type": "string",
                                      "description": "完成後の増加能力, English, at most 15 words; "
                                      "empty if none."},
                },
            }, "description": "One entry per line of the 設備の新設、除却等の計画 table. "
               "Empty if the company states it has no significant plan."},
            "plan_total_jpy": {"type": "number", "description": "Sum of planned_total_jpy."},
            "plan_paid_jpy": {"type": "number", "description": "Sum of paid_to_date_jpy."},
            "funding_summary": {"type": "string",
                                "description": "In English, at most 20 words: how the plan "
                                "is funded (own funds, borrowings, bonds, leases)."},
            "notes": {"type": "string", "description": "E.g. 'no significant plan', "
                      "'plan given only as a company-wide total'."},
        },
    }


def _schema_pay():
    return {
        "type": "object", "additionalProperties": False,
        "required": ["by_class", "individuals", "policy_summary", "notes"],
        "properties": {
            "by_class": {"type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "required": ["class_name", "total_jpy", "headcount", "fixed_jpy",
                             "performance_jpy", "stock_jpy"],
                "properties": {
                    "class_name": {"type": "string", "description": "役員区分 as printed."},
                    "total_jpy": {"type": "number", "description": "In yen (convert from 百万円)."},
                    "headcount": {"type": "integer"},
                    "fixed_jpy": {"type": "number", "description": "基本報酬; 0 if not split."},
                    "performance_jpy": {"type": "number", "description": "業績連動報酬; 0 if not split."},
                    "stock_jpy": {"type": "number", "description": "株式報酬 / 非金銭報酬; 0 if not split."},
                },
            }},
            "individuals": {"type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "required": ["name", "title", "total_jpy"],
                "properties": {"name": {"type": "string"}, "title": {"type": "string"},
                               "total_jpy": {"type": "number"}},
            }, "description": "Officers named individually (連結報酬等の総額が1億円以上)."},
            "policy_summary": {"type": "string",
                               "description": "In English, at most 40 words: what pay is tied "
                               "to (which metrics, what split)."},
            "notes": {"type": "string"},
        },
    }


def _schema_share_status():
    return {
        "type": "object", "additionalProperties": False,
        "required": ["as_of", "foreign_pct", "financial_institutions_pct", "individuals_pct",
                     "treasury_shares", "shares_issued", "shareholders_count", "notes"],
        "properties": {
            "as_of": {"type": "string"},
            "foreign_pct": {"type": "number",
                            "description": "外国法人等 (corporations and individuals together) "
                            "share of shares held, in percent; 0 if not stated."},
            "financial_institutions_pct": {"type": "number", "description": "金融機関, percent."},
            "individuals_pct": {"type": "number", "description": "個人その他, percent."},
            "treasury_shares": {"type": "integer", "description": "自己株式 in shares; 0 if absent."},
            "shares_issued": {"type": "integer", "description": "発行済株式総数 in shares."},
            "shareholders_count": {"type": "integer", "description": "株主数 total; 0 if absent."},
            "notes": {"type": "string"},
        },
    }


SCHEMAS = {"officers": _schema_officers, "major_shareholders": _schema_shareholders,
           "share_status": _schema_share_status, "cross_holdings": _schema_cross,
           "capex": _schema_capex, "exec_pay": _schema_pay}

_PROMPTS = {
    "officers": (
        "Below is the 役員の状況 section of a Japanese annual securities report "
        "(有価証券報告書){who}, flattened from HTML: one officer per block, cells "
        "separated by ' | '. Return every officer listed, in the order given. Read "
        "the career history (略歴) carefully: the year they joined this company is the "
        "入社 line; the year they became a director is the first 取締役 appointment at "
        "this company. Ages and years must come from the text — never estimate. Tag "
        "backgrounds only where the career history supports the tag. Shares are "
        "given in the table's unit (usually 千株 or 株) — convert to shares."),
    "major_shareholders": (
        "Below is the 大株主の状況 section of a Japanese annual securities report "
        "(有価証券報告書){who}, flattened from HTML. Return every holder in the "
        "table with shares and percentage as printed (converted to shares if the "
        "table is in thousands). Classify each: a parent or group company, a trust "
        "bank's trust account (信託口), a foreign custodian or nominee (e.g. STATE "
        "STREET, JP MORGAN CHASE BANK), a bank or insurer, the founder's family, an "
        "employee shareholding association (従業員持株会), treasury stock, a business "
        "partner, or other."),
    "share_status": (
        "Below is the 所有者別状況 table of a Japanese annual securities report "
        "(有価証券報告書){who}, flattened from HTML: shares held by category of holder "
        "(government, financial institutions, securities firms, other corporations, "
        "foreign corporations and individuals, individuals and other, treasury). "
        "Return each category's share of shares held in percent as printed (the 割合 "
        "row), the shares issued, treasury shares and the shareholder count if stated. "
        "Shares may be in 単元 (units) — convert to shares using the 単元株式数 if given, "
        "else return the percentages only and say so in notes."),
    "cross_holdings": (
        "Below is the 株式の保有状況 section of a Japanese annual securities report "
        "(有価証券報告書){who}, flattened from HTML. It states the company's policy on "
        "holding other listed companies' shares for purposes other than pure "
        "investment (政策保有株式), the totals, and a table of named issues "
        "(特定投資株式) with shares, balance-sheet value, purpose and whether that "
        "company holds this company's shares. Return the policy, the totals and every "
        "named issue. Amounts in the table are usually 百万円 — convert to yen. Do not "
        "include みなし保有株式 (deemed holdings) in the issues list; mention them in "
        "notes if present."),
    "capex": (
        "Below are two sections of a Japanese annual securities report "
        "(有価証券報告書){who}, flattened from HTML: 設備投資等の概要 (capital "
        "expenditure in the year just reported, usually by segment) and 設備の新設、"
        "除却等の計画 (the plan for new facilities: site, location, segment, what is "
        "being built, planned total, amount already paid, funding, start and planned "
        "completion, capacity added). Return the actual spend by segment and every line "
        "of the plan table. Amounts are usually 百万円 — convert to yen. If the plan "
        "section says there is no significant plan, return an empty plan and say so in "
        "notes."),
    "exec_pay": (
        "Below is the 役員の報酬等 section of a Japanese annual securities report "
        "(有価証券報告書){who}, flattened from HTML. Return pay by officer class "
        "(the 役員区分 table), any individuals named with their total, and a short "
        "summary of what pay is tied to. Amounts are usually 百万円 — convert to yen."),
}


def estimate_cost(blocks: dict) -> dict:
    """A rough cost for reading these blocks: Japanese text runs about one token "
    "per character on this model family."""
    n_in = sum(min(len(html_to_text(v)), MAX_BLOCK_CHARS) for v in blocks.values())
    n_in += 600 * len(blocks)
    n_out = 2500 * len(blocks)
    usd = (n_in / 1e6) * INPUT_USD_PER_MTOK + (n_out / 1e6) * OUTPUT_USD_PER_MTOK
    return {"input_tokens": n_in, "output_tokens": n_out, "usd": usd}


def extract_block(kind: str, text: str, api_key: str, code: str = "", name: str = "",
                  model: str = MODEL) -> dict:
    """One block → the model's JSON (raw, not yet normalised). Raises."""
    import anthropic
    who = f" for {name} ({code})" if (name or code) else ""
    prompt = _PROMPTS[kind].format(who=who)
    text = (text or "")[:MAX_BLOCK_CHARS]
    client = anthropic.Anthropic(api_key=api_key)
    messages = [{"role": "user", "content": prompt + "\n\n<section>\n" + text + "\n</section>"}]
    schema = SCHEMAS[kind]()
    attempts = (
        {"output_config": {"format": {"type": "json_schema", "schema": schema},
                           "effort": models.DOC_EXTRACT_EFFORT},
         "thinking": {"type": "adaptive"}},
        {},
    )
    last_exc = None
    for extra in attempts:
        try:
            resp = client.messages.create(model=model, max_tokens=16000,
                                          messages=messages, **extra)
            break
        except TypeError as exc:
            last_exc = exc
        except anthropic.BadRequestError as exc:
            last_exc = exc
    else:
        raise RuntimeError(f"Could not call the model: {last_exc}")
    if getattr(resp, "stop_reason", "") == "refusal":
        raise RuntimeError("The model declined to read this section.")
    reply = next((b.text for b in resp.content if getattr(b, "type", "") == "text"), "")
    from fx_extract import _loads_lenient
    usage = getattr(resp, "usage", None)
    out = _loads_lenient(reply)
    out["_usage"] = {"input": getattr(usage, "input_tokens", 0) or 0,
                     "output": getattr(usage, "output_tokens", 0) or 0}
    return out


# ── 5. Normalising what came back ────────────────────────────────────────

def _i(v):
    try:
        return int(float(str(v).replace(",", "")))
    except (TypeError, ValueError):
        return 0


def _f(v):
    try:
        x = float(str(v).replace(",", ""))
        return x if x == x else 0.0
    except (TypeError, ValueError):
        return 0.0


def _s(v, n=300):
    return str(v or "").strip()[:n]


def normalise(kind: str, raw: dict) -> dict:
    raw = raw if isinstance(raw, dict) else {}
    if kind == "officers":
        offs = []
        for o in raw.get("officers") or []:
            if not _s(o.get("name")):
                continue
            offs.append({
                "name": _s(o.get("name"), 80), "title": _s(o.get("title"), 120),
                "role_kind": o.get("role_kind") if o.get("role_kind") in ROLE_KINDS else "other",
                "outside": bool(o.get("outside")), "age": _i(o.get("age")),
                "birth_year": _i(o.get("birth_year")),
                "joined_company_year": _i(o.get("joined_company_year")),
                "director_since_year": _i(o.get("director_since_year")),
                "is_ceo": bool(o.get("is_ceo")), "is_chair": bool(o.get("is_chair")),
                "is_cfo": bool(o.get("is_cfo")),
                "career_summary": _s(o.get("career_summary"), 400),
                "background_tags": [t for t in (o.get("background_tags") or [])
                                    if t in BACKGROUND_TAGS],
                "shares_held": _i(o.get("shares_held")), "term_end": _s(o.get("term_end"), 40),
            })
        return {"as_of": _s(raw.get("as_of"), 40), "officers": offs,
                "notes": _s(raw.get("notes"), 600)}
    if kind == "major_shareholders":
        hs = []
        for h in raw.get("holders") or []:
            if not _s(h.get("name")):
                continue
            hs.append({"name": _s(h.get("name"), 120), "shares": _i(h.get("shares")),
                       "pct": _f(h.get("pct")),
                       "kind": h.get("kind") if h.get("kind") in HOLDER_KINDS else "other"})
        return {"as_of": _s(raw.get("as_of"), 40), "holders": hs,
                "treasury_shares": _i(raw.get("treasury_shares")),
                "notes": _s(raw.get("notes"), 600)}
    if kind == "share_status":
        return {"as_of": _s(raw.get("as_of"), 40), "foreign_pct": _f(raw.get("foreign_pct")),
                "financial_institutions_pct": _f(raw.get("financial_institutions_pct")),
                "individuals_pct": _f(raw.get("individuals_pct")),
                "treasury_shares": _i(raw.get("treasury_shares")),
                "shares_issued": _i(raw.get("shares_issued")),
                "shareholders_count": _i(raw.get("shareholders_count")),
                "notes": _s(raw.get("notes"), 400)}
    if kind == "cross_holdings":
        iss = []
        for x in raw.get("issues") or []:
            if not _s(x.get("name")):
                continue
            iss.append({"name": _s(x.get("name"), 120), "shares": _i(x.get("shares")),
                        "book_value_jpy": _f(x.get("book_value_jpy")),
                        "purpose": _s(x.get("purpose"), 200),
                        "purpose_kind": (x.get("purpose_kind")
                                         if x.get("purpose_kind") in PURPOSE_KINDS else "other"),
                        "holds_our_shares": bool(x.get("holds_our_shares")),
                        "change": (x.get("change") if x.get("change") in
                                   ("increased", "decreased", "unchanged", "new", "sold", "unknown")
                                   else "unknown")})
        iss.sort(key=lambda x: -x["book_value_jpy"])
        return {"policy_summary": _s(raw.get("policy_summary"), 600),
                "total_issues": _i(raw.get("total_issues")),
                "total_book_value_jpy": _f(raw.get("total_book_value_jpy")),
                "issues": iss[:60], "reduction_stated": bool(raw.get("reduction_stated")),
                "notes": _s(raw.get("notes"), 600)}
    if kind == "capex":
        seg = [{"segment": _s(s.get("segment"), 80), "amount_jpy": _f(s.get("amount_jpy")),
                "what": _s(s.get("what"), 160)} for s in (raw.get("actual_by_segment") or [])
               if _s(s.get("segment"))]
        plan = []
        for p in raw.get("plan") or []:
            if not (_s(p.get("site")) or _s(p.get("description"))):
                continue
            plan.append({k: _s(p.get(k), 120) for k in
                         ("site", "location", "segment", "description", "funding", "start",
                          "finish", "capacity_note")}
                        | {"planned_total_jpy": _f(p.get("planned_total_jpy")),
                           "paid_to_date_jpy": _f(p.get("paid_to_date_jpy"))})
        tot = _f(raw.get("plan_total_jpy")) or sum(p["planned_total_jpy"] for p in plan)
        paid = _f(raw.get("plan_paid_jpy")) or sum(p["paid_to_date_jpy"] for p in plan)
        return {"actual_total_jpy": _f(raw.get("actual_total_jpy")) or sum(s["amount_jpy"] for s in seg),
                "actual_by_segment": seg, "plan": plan, "plan_total_jpy": tot,
                "plan_paid_jpy": paid, "funding_summary": _s(raw.get("funding_summary"), 200),
                "notes": _s(raw.get("notes"), 600)}
    if kind == "exec_pay":
        cls = [{"class_name": _s(c.get("class_name"), 80), "total_jpy": _f(c.get("total_jpy")),
                "headcount": _i(c.get("headcount")), "fixed_jpy": _f(c.get("fixed_jpy")),
                "performance_jpy": _f(c.get("performance_jpy")), "stock_jpy": _f(c.get("stock_jpy"))}
               for c in (raw.get("by_class") or []) if _s(c.get("class_name"))]
        ind = [{"name": _s(i.get("name"), 80), "title": _s(i.get("title"), 120),
                "total_jpy": _f(i.get("total_jpy"))}
               for i in (raw.get("individuals") or []) if _s(i.get("name"))]
        return {"by_class": cls, "individuals": ind,
                "policy_summary": _s(raw.get("policy_summary"), 400),
                "notes": _s(raw.get("notes"), 600)}
    return {}


# ── 6. The read, end to end ──────────────────────────────────────────────

def prepare(csv_zip: bytes) -> dict:
    """Everything that needs no model: the parsed facts, the text blocks, and
    a cost estimate for reading them. The screen shows this first, so the
    numbers are on screen before any money is spent."""
    rows = parse_csv_zip(csv_zip)
    blocks = text_blocks(rows)
    cat = [{k: v for k, v in c.items() if not k.startswith("_")} for c in catalog(rows)]
    return {"facts": numeric_facts(rows), "blocks": blocks, "n_rows": len(rows),
            "estimate": estimate_cost(blocks), "catalog": cat[:80]}


def read_blocks(blocks: dict, api_key: str, code: str = "", name: str = "",
                progress=None) -> tuple:
    """(sections, errors, usage) — every block the model could read, normalised.
    `progress(kind)` is called before each block so a screen can say which one
    is running. The capex plan and the capex overview go in one call."""
    sections, errors = {}, {}
    usage = {"input": 0, "output": 0}
    jobs = []
    for kind in ("officers", "major_shareholders", "share_status", "cross_holdings", "exec_pay"):
        if blocks.get(kind):
            jobs.append((kind, html_to_text(blocks[kind])))
    if blocks.get("capex_plan") or blocks.get("capex_overview"):
        txt = ""
        if blocks.get("capex_overview"):
            txt += "【設備投資等の概要】\n" + html_to_text(blocks["capex_overview"]) + "\n\n"
        if blocks.get("capex_plan"):
            txt += "【設備の新設、除却等の計画】\n" + html_to_text(blocks["capex_plan"])
        jobs.append(("capex", txt))
    for kind, text in jobs:
        if progress:
            progress(kind)
        try:
            raw = extract_block(kind, text, api_key, code, name)
        except Exception as exc:
            errors[kind] = str(exc)[:300]
            continue
        u = raw.pop("_usage", {}) or {}
        usage["input"] += u.get("input", 0)
        usage["output"] += u.get("output", 0)
        sections[kind] = normalise(kind, raw)
    return sections, errors, usage


def build_entry(filing: dict, prepared: dict, sections: dict, errors: dict,
                usage: dict, read_on: str) -> dict:
    """The store entry for one yuho, keyed by fiscal year."""
    fy = fy_label(filing.get("PeriodEnd"))
    return {
        "id": f"yuho_{fy}", "fy": fy, "date": (filing.get("SubmitDateTime") or "")[:10],
        "doc_id": filing.get("DocID", ""), "period_end": filing.get("PeriodEnd", ""),
        "read_on": read_on, "facts": prepared.get("facts", {}),
        "sections": sections, "errors": errors, "usage": usage,
        "blocks_found": sorted(prepared.get("blocks", {}).keys()),
        "blocks_catalog": [{"element": c["element"], "label": c["label"], "chars": c["chars"],
                            "kind": c["kind"]} for c in prepared.get("catalog", [])],
    }


# ── 7. Reading the card ──────────────────────────────────────────────────

def fact(entry: dict, key: str, basis: str = None):
    """A numeric fact's value: consolidated first, else parent, or None."""
    slot = (entry.get("facts") or {}).get(key) or {}
    if basis:
        return (slot.get(basis) or {}).get("value")
    for b in ("consolidated", "parent"):
        if b in slot:
            return slot[b]["value"]
    return None


def foreign_pct(entry: dict):
    """Foreign ownership in percent: the tagged value, else the read of the
    所有者別状況 table, else None."""
    v = fact(entry, "foreign_pct")
    if v is None:
        v = ((entry.get("sections") or {}).get("share_status") or {}).get("foreign_pct") or None
    return v


def shareholders_count(entry: dict):
    v = fact(entry, "shareholders")
    if v is None:
        v = ((entry.get("sections") or {}).get("share_status") or {}).get("shareholders_count") or None
    return v


_TAG_LABEL = {
    "finance_accounting": "finance", "sales_marketing": "sales", "engineering_rd": "engineering",
    "manufacturing": "manufacturing", "overseas": "overseas", "legal": "legal",
    "hr_admin": "HR/admin", "planning_strategy": "planning", "ex_parent_group": "ex-parent",
    "ex_bank": "ex-bank", "ex_government": "ex-government", "ex_trading_house": "ex-trading house",
    "academic": "academic", "other_company_executive": "other-company executive",
    "founder_family": "founder family", "consulting": "consulting",
}


def board_stats(officers: list, fy_year: int = None) -> dict:
    """Counts the card leads with. Directors only (auditors and executive
    officers are listed in the same table but are not the board)."""
    board = [o for o in officers if o["role_kind"] in
             ("representative_director", "director", "outside_director", "audit_committee_member")]
    if not board:
        return {}
    outside = [o for o in board if o["outside"]]
    ages = [o["age"] for o in board if o["age"]]
    ex_bank = sum(1 for o in board if "ex_bank" in o["background_tags"])
    ex_parent = sum(1 for o in board if "ex_parent_group" in o["background_tags"])
    lifers = sum(1 for o in board if not o["outside"] and o["joined_company_year"]
                 and fy_year and fy_year - o["joined_company_year"] >= 25)
    return {"size": len(board), "outside": len(outside),
            "outside_pct": len(outside) / len(board),
            "avg_age": (sum(ages) / len(ages)) if ages else None,
            "ex_bank": ex_bank, "ex_parent": ex_parent, "lifers": lifers,
            "insider_shares": sum(o["shares_held"] for o in board if not o["outside"])}


def key_people(officers: list) -> dict:
    """{ceo, chair, cfo} → officer dicts where identified."""
    out = {}
    for o in officers:
        if o["is_ceo"] and "ceo" not in out:
            out["ceo"] = o
        if o["is_chair"] and "chair" not in out:
            out["chair"] = o
        if o["is_cfo"] and "cfo" not in out:
            out["cfo"] = o
    return out


def bias_read(entry: dict, price=None) -> str:
    """One deterministic sentence on what the people at the top are likely
    biased toward, from their backgrounds and ownership. Rules, not a model,
    so the same facts always give the same sentence. Empty when there is
    not enough to say."""
    secs = entry.get("sections") or {}
    offs = (secs.get("officers") or {}).get("officers") or []
    if not offs:
        return ""
    fy_year = int(entry["fy"][2:]) if entry.get("fy", "").startswith("FY") else None
    kp = key_people(offs)
    bs = board_stats(offs, fy_year)
    bits = []
    ceo = kp.get("ceo")
    if ceo:
        tags = [t for t in ceo["background_tags"] if t in _TAG_LABEL]
        bg = ", ".join(_TAG_LABEL[t] for t in tags[:3]) or "unstated background"
        tenure = (f"{fy_year - ceo['joined_company_year']} years at the company"
                  if fy_year and ceo["joined_company_year"] else
                  "an outsider" if ceo["outside"] or "other_company_executive" in tags
                  else "tenure unstated")
        since = (f", CEO-level director since {ceo['director_since_year']}"
                 if ceo["director_since_year"] else "")
        own = ""
        if ceo["shares_held"] and price:
            own = f", holds ¥{ceo['shares_held'] * price / 1e6:,.0f}m of stock"
        elif ceo["shares_held"]:
            own = f", holds {ceo['shares_held']:,} shares"
        bits.append(f"CEO {ceo['name']}: {bg}, {tenure}{since}{own}")
        leans = []
        if "founder_family" in tags:
            leans.append("control and continuity over payout")
        if "engineering_rd" in tags or "manufacturing" in tags:
            leans.append("capex and product over buybacks")
        if "finance_accounting" in tags:
            leans.append("balance-sheet discipline, possibly over growth")
        if "sales_marketing" in tags or "overseas" in tags:
            leans.append("top-line and expansion")
        if "ex_bank" in tags or "ex_parent_group" in tags:
            leans.append("the lender's / parent's interests")
        if leans:
            bits.append("likely to favour " + "; ".join(leans))
    if bs:
        board_bits = [f"board of {bs['size']}, {bs['outside']} outside ({bs['outside_pct'] * 100:.0f}%)"]
        if bs["ex_bank"]:
            board_bits.append(f"{bs['ex_bank']} ex-bank")
        if bs["ex_parent"]:
            board_bits.append(f"{bs['ex_parent']} ex-parent")
        if bs["lifers"]:
            board_bits.append(f"{bs['lifers']} lifers (25+ years)")
        bits.append(", ".join(board_bits))
        if bs["outside_pct"] < 1 / 3:
            bits.append("outside directors under a third: expect the inside view to prevail")
    return "; ".join(bits) + "." if bits else ""


def cross_holdings_read(entry: dict, market_cap=None, equity=None) -> dict:
    """The cross-holdings in the two comparisons that matter: against market
    cap (what an unwind could return) and against equity (how much of the
    book is other companies' shares). Falls back to the numeric fact when the
    model's read lacks a total."""
    ch = (entry.get("sections") or {}).get("cross_holdings") or {}
    total = ch.get("total_book_value_jpy") or 0
    n = ch.get("total_issues") or 0
    if not total:
        v = fact(entry, "cross_book_value")
        unit = ((entry.get("facts") or {}).get("cross_book_value") or {})
        unit = (unit.get("consolidated") or unit.get("parent") or {}).get("unit", "")
        if v is not None:
            total = v * (1e6 if "百万" in unit else 1e3 if "千" in unit else 1)
    if not n:
        n = fact(entry, "cross_issues") or 0
    if not total:
        return {}
    issues = ch.get("issues") or []
    vague = sum(1 for x in issues if x["purpose_kind"] in ("business_relationship", "other"))
    return {"total_jpy": total, "issues": int(n),
            "pct_mcap": (total / market_cap) if market_cap else None,
            "pct_equity": (total / equity) if equity else None,
            "vague_purpose": vague, "named": len(issues),
            "mutual": sum(1 for x in issues if x["holds_our_shares"]),
            "reduction_stated": bool(ch.get("reduction_stated"))}


def capex_read(entry: dict, last_capex=None, dep_amort=None) -> dict:
    """The plan against what the company actually spent last year and
    against depreciation: is next year's capex a step up or down, and by how
    much. `last_capex` / `dep_amort` in yen, from the balance-sheet panel."""
    cx = (entry.get("sections") or {}).get("capex") or {}
    if not cx:
        return {}
    actual = cx.get("actual_total_jpy") or last_capex
    remaining = (cx.get("plan_total_jpy") or 0) - (cx.get("plan_paid_jpy") or 0)
    return {"actual_jpy": actual, "plan_total_jpy": cx.get("plan_total_jpy") or 0,
            "plan_paid_jpy": cx.get("plan_paid_jpy") or 0, "remaining_jpy": remaining,
            "remaining_vs_actual": (remaining / actual) if actual else None,
            "remaining_vs_da": (remaining / dep_amort) if dep_amort else None,
            "lines": len(cx.get("plan") or []), "notes": cx.get("notes", "")}


def yoy(entries: list, key: str, basis: str = None) -> list:
    """[(fy, value)] oldest first for one numeric fact across the years read."""
    out = []
    for e in sorted(entries or [], key=lambda e: e.get("fy", "")):
        v = fact(e, key, basis)
        if v is not None:
            out.append((e["fy"], v))
    return out


# ── 8. The store ─────────────────────────────────────────────────────────

def load_from_github(repo: str, token: str = None) -> dict:
    return CE.load_from_github(repo, token, STORE_PATH)


def save_via_github(repo: str, token: str, code: str, entry: dict) -> tuple:
    return CE.save_via_github(repo, token, code, [entry], (), path=STORE_PATH,
                              what="yuho extract")
