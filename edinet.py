"""
edinet.py — EDINET API v2 integration for the Research tab.

EDINET (https://disclosure2.edinet-fsa.go.jp/) is Japan's statutory corporate
disclosure system (annual/quarterly securities reports, extraordinary reports,
large shareholding reports, etc.) — distinct from TDnet, which carries the
"timely disclosure" earnings/announcement filings shown in the Reg Filings tab.

Free registration for an API subscription key: https://disclosure2.edinet-fsa.go.jp/
(look for "API利用" / "API Key" in the site's menu). Set it as EDINET_API_KEY in
Streamlit Secrets (for the Research tab's document downloads) and as a GitHub
Actions secret of the same name (for the daily_edinet.yml / backfill_edinet.yml
workflows that build the rolling filing index).

Key API limitation this module works around: EDINET only supports listing
"what was filed on date D" — there is no "list filings for company X" endpoint.
Two years of per-company history is therefore built by a scheduled job that
walks each date and appends matches to data/edinet_filings.csv (see
collect_edinet.py), which the Research tab reads via load_edinet_filings_from_github().
"""

import csv
import io
import os
import time
from datetime import datetime, date, timedelta

EDINET_API_BASE = "https://api.edinet-fsa.go.jp/api/v2"
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# The full documented set of EDINET docTypeCodes (all 42), used only for
# display labels — fetch_edinet_day no longer filters on this at all, so a
# type missing from this map still comes through with a generic fallback
# label rather than being silently dropped from the index.
EDINET_DOC_TYPES = {
    "010": "Securities Notification",
    "020": "Securities Notification (Amended)",
    "030": "Securities Registration Statement",
    "040": "Securities Registration Statement (Amended)",
    "050": "Securities Registration Withdrawal",
    "060": "Issuance Registration Notification",
    "070": "Shelf Registration",
    "080": "Issuance Registration Statement",
    "090": "Issuance Registration Statement (Amended)",
    "100": "Shelf Registration Supplement",
    "110": "Issuance Registration Withdrawal",
    "120": "Annual Securities Report",
    "130": "Annual Securities Report (Amended)",
    "135": "Confirmation Document",
    "136": "Confirmation Document (Amended)",
    "140": "Quarterly Report",
    "150": "Quarterly Report (Amended)",
    "160": "Semi-Annual Report",
    "170": "Semi-Annual Report (Amended)",
    "180": "Extraordinary Report",
    "190": "Extraordinary Report (Amended)",
    "200": "Parent Company Status Report",
    "210": "Parent Company Status Report (Amended)",
    "220": "Treasury Stock Buyback Report",
    "230": "Treasury Stock Buyback Report (Amended)",
    "235": "Internal Control Report",
    "236": "Internal Control Report (Amended)",
    "240": "Tender Offer Registration",
    "250": "Tender Offer Registration (Amended)",
    "260": "Tender Offer Withdrawal",
    "270": "Tender Offer Report",
    "280": "Tender Offer Report (Amended)",
    "290": "Tender Offer Opinion Statement",
    "300": "Tender Offer Opinion Statement (Amended)",
    "310": "Tender Offer Response to Questions",
    "320": "Tender Offer Response to Questions (Amended)",
    "330": "Tender Offer Exemption Application",
    "340": "Tender Offer Exemption Application (Amended)",
    "350": "Large Shareholding Report",
    "360": "Large Shareholding Report (Amended)",
    "370": "Large Shareholding Change Report",
    "380": "Large Shareholding Change Report (Amended)",
}

CSV_FIELDS = [
    "SecCode", "EdinetCode", "FilerName", "DocTypeCode", "DocDescription",
    "SubmitDateTime", "PeriodStart", "PeriodEnd", "DocID", "EnglishDocFlag", "PdfFlag",
]

# Kept to ~2 years + a small buffer so the index doesn't grow unbounded.
RETENTION_DAYS = 760


def _normalize_sec_code(raw: str) -> str:
    """EDINET secCode is often 5 digits (4-digit TSE code + trailing check digit,
    e.g. "72030" for Toyota's 7203). Normalize to the 4-digit code used
    everywhere else in this app; return "" for unlisted filers (no secCode)."""
    raw = (raw or "").strip()
    if not raw or not raw.isdigit():
        return ""
    if len(raw) == 5:
        return raw[:4]
    if len(raw) == 4:
        return raw
    return ""


def fetch_edinet_day(target_date, api_key: str, session=None, timeout: int = 20, retries: int = 3):
    """Fetch EDINET's filing list for one date, filtered to listed companies
    (secCode present) and non-withdrawn filings — every docTypeCode is kept,
    not just the ones in EDINET_DOC_TYPES, so a filing type this app doesn't
    have a friendly label for yet still shows up (with a generic fallback
    label) instead of silently vanishing. `target_date` is a date object or
    "YYYY-MM-DD" str. Returns a list of dicts matching CSV_FIELDS. Raises on
    repeated failure — callers should catch and continue so one bad day
    doesn't kill a backfill."""
    if isinstance(target_date, (date, datetime)):
        date_str = target_date.strftime("%Y-%m-%d")
    else:
        date_str = str(target_date)

    _req = session or __import__("requests")
    url = f"{EDINET_API_BASE}/documents.json"
    params = {"date": date_str, "type": "2", "Subscription-Key": api_key}
    headers = {"User-Agent": USER_AGENT}

    last_exc = None
    resp = None
    for attempt in range(retries):
        try:
            resp = _req.get(url, params=params, headers=headers, timeout=timeout)
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    if last_exc:
        raise last_exc
    if resp.status_code != 200:
        raise RuntimeError(f"EDINET documents.json HTTP {resp.status_code} for {date_str}: {resp.text[:200]}")

    payload = resp.json()
    results = payload.get("results") or []
    out = []
    for doc in results:
        doc_type = str(doc.get("docTypeCode") or "")
        # withdrawalStatus: "1" means the filing was withdrawn — skip those.
        if str(doc.get("withdrawalStatus") or "0") == "1":
            continue
        sec_code = _normalize_sec_code(doc.get("secCode") or "")
        if not sec_code:
            continue  # unlisted filer — not useful for per-company equity research
        out.append({
            "SecCode":        sec_code,
            "EdinetCode":     doc.get("edinetCode") or "",
            "FilerName":      doc.get("filerName") or "",
            "DocTypeCode":    doc_type,
            "DocDescription": doc.get("docDescription") or "",
            "SubmitDateTime": doc.get("submitDateTime") or "",
            "PeriodStart":    doc.get("periodStart") or "",
            "PeriodEnd":      doc.get("periodEnd") or "",
            "DocID":          doc.get("docID") or "",
            "EnglishDocFlag": "1" if str(doc.get("englishDocFlag") or "0") == "1" else "0",
            "PdfFlag":        "1" if str(doc.get("pdfFlag") or "0") == "1" else "0",
        })
    return out


class DocumentNotAvailable(Exception):
    """Raised when EDINET has no document of the requested type for this
    filing (e.g. no English version) — distinct from a real fetch failure,
    so callers can show a calm message instead of an alarming error."""


def fetch_edinet_document_bytes(doc_id: str, doc_type: int, api_key: str, timeout: int = 30):
    """Fetch raw document bytes server-side. doc_type: 2 = Japanese PDF, 4 = English
    document (zip). The Subscription-Key must never be exposed client-side (it would
    leak into the browser's address bar / history), so callers must render this as a
    Streamlit-fetched-then-download_button flow, never as a raw <a href> link.

    Not every filing has an English document — EDINET's own englishDocFlag on
    the list endpoint isn't reliable enough to gate the UI on, so callers are
    expected to just try the fetch and handle DocumentNotAvailable."""
    import requests as _requests
    url = f"{EDINET_API_BASE}/documents/{doc_id}"
    params = {"type": str(doc_type), "Subscription-Key": api_key}
    r = _requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    if r.status_code == 404:
        raise DocumentNotAvailable(f"No document of type {doc_type} for {doc_id}")
    if r.status_code != 200:
        raise RuntimeError(f"EDINET document download HTTP {r.status_code}")
    if not r.content or len(r.content) < 32:
        # EDINET has been observed returning a 200 with an empty/near-empty
        # body rather than a 404 when a document doesn't exist for a filing.
        raise DocumentNotAvailable(f"Empty response for document type {doc_type} on {doc_id}")
    return r.content


def load_edinet_filings_from_github(repo: str, token: str = None) -> list:
    """Load the pre-built rolling filing index from data/edinet_filings.csv
    (maintained by daily_edinet.yml / backfill_edinet.yml). Mirrors the
    raw.githubusercontent loader pattern used by jquants.py."""
    import requests

    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"

    url = f"https://raw.githubusercontent.com/{repo}/main/data/edinet_filings.csv"
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code == 404:
        return []  # Not yet generated by the collector workflow
    if r.status_code != 200:
        print(f"EDINET filings fetch error: {r.status_code}")
        return []

    out = []
    for row in csv.DictReader(io.StringIO(r.text)):
        code = _normalize_sec_code(row.get("SecCode", ""))
        if not code:
            continue
        row["SecCode"] = code
        out.append(row)
    print(f"EDINET filings loaded: {len(out)} rows")
    return out


def doc_type_label(doc_type_code: str) -> str:
    return EDINET_DOC_TYPES.get(str(doc_type_code or ""), f"Filing ({doc_type_code})")


def prune_and_dedupe(rows: list, retention_days: int = RETENTION_DAYS) -> list:
    """Dedupe by DocID (keeping the row seen) and drop anything older than
    retention_days based on SubmitDateTime."""
    cutoff = datetime.now() - timedelta(days=retention_days)
    seen = {}
    for row in rows:
        doc_id = row.get("DocID")
        if not doc_id:
            continue
        submit = row.get("SubmitDateTime", "")
        try:
            submit_dt = datetime.strptime(submit[:16], "%Y-%m-%d %H:%M")
        except Exception:
            submit_dt = None
        if submit_dt and submit_dt < cutoff:
            continue
        seen[doc_id] = row  # last write wins; harmless since content is stable per docID
    return list(seen.values())


def write_filings_csv(path: str, rows: list):
    rows = sorted(rows, key=lambda r: r.get("SubmitDateTime", ""), reverse=True)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in CSV_FIELDS})


def read_filings_csv(path: str) -> list:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))
