"""
tdnet.py — rolling TDnet (timely disclosure) history for the Research tab.

TDnet carries earnings summaries (決算短信 / "tanshin"), dividend/guidance
revisions, buyback announcements, and other timely disclosures — distinct
from EDINET's statutory filings. The Reg Filings tab already fetches a live
3-day rolling window directly from Yanoshin's TDnet mirror + the TSE's
English disclosure search; this module builds a much longer ~2-year history
the same way, via a scheduled collector (collect_tdnet.py), mirroring the
EDINET collector pattern (edinet.py / collect_edinet.py). No API key needed —
both sources are public.

Known limitation: Yanoshin's JP list endpoint isn't documented to paginate,
so the historical walk uses the same small (3-day) request window already
proven to work for the live Reg Filings tab, repeated across history, rather
than requesting one huge range that might silently truncate.
"""

import csv
import io
import os
import re
import time
from datetime import datetime, date, timedelta

USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

CSV_FIELDS = ["Code", "Name", "NameEN", "Title", "TitleEN", "PubDateTime", "DocUrl", "EngUrl"]
RETENTION_DAYS = 760

_TYPE_RULES = [
    (["決算短信"], "Tanshin"),
    (["配当"], "Dividend"),
    (["業績予想", "業績の修正", "業績予想の修正"], "Guidance"),
    (["自己株式", "自社株"], "Buyback"),
    (["株主優待"], "Shareholder Benefit"),
    (["株主総会", "招集"], "AGM Notice"),
    (["第三者割当", "新株予約権", "公募増資"], "Placement"),
    (["合併", "株式交換", "株式移転", "会社分割", "事業譲渡", "買収"], "M&A"),
    (["人事異動", "役員"], "Management Change"),
]


def classify_title(title: str) -> str:
    for keywords, label in _TYPE_RULES:
        for kw in keywords:
            if kw in title:
                return label
    return "Timely Disclosure"


def _normalize_code(raw: str) -> str:
    """Yanoshin's bracketed code is often 5 digits (4-digit TSE code + a
    trailing check digit, same convention as EDINET's secCode). Normalize to
    the 4-digit code used everywhere else in this app."""
    raw = (raw or "").strip()
    if not raw or not raw.isdigit():
        return ""
    if len(raw) == 5:
        return raw[:4]
    if len(raw) == 4:
        return raw
    return ""


def fetch_english_lookup(d_from: str, d_to: str, session=None, timeout: int = 20, max_pages: int = 400) -> dict:
    """TSE's English disclosure search over an arbitrary date range (YYYYMMDD
    strings), paginated. Returns {(4-digit code, "YYYY-MM-DD HH:MM"): {...}}.
    Non-fatal on failure — an empty lookup just means no EN cross-references."""
    import requests
    from bs4 import BeautifulSoup

    lookup = {}
    try:
        sess = session or requests.Session()
        sess.headers.update({"User-Agent": USER_AGENT})
        sess.get("https://www.release.tdnet.info/onsf/TDJFSearch_e/I_head", timeout=timeout)
        post_url = "https://www.release.tdnet.info/onsf/TDJFSearch_e/TDJFSearch_e"
        ref_url = "https://www.release.tdnet.info/onsf/TDJFSearch_e/I_head"

        page = 1
        while page <= max_pages:
            resp = sess.post(post_url, data={"t0": d_from, "t1": d_to, "q": "", "p": str(page)},
                              headers={"Referer": ref_url}, timeout=timeout + 10)
            soup = BeautifulSoup(resp.content, "lxml")
            rows = soup.find_all("tr")[1:]
            if not rows:
                break
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue
                date_raw = cells[0].get_text(strip=True)
                code = _normalize_code(cells[1].get_text(strip=True))
                name = cells[2].get_text(strip=True)
                title = cells[4].get_text(strip=True)
                a = cells[4].find("a")
                url = ""
                if a and a.get("href"):
                    href = a["href"]
                    url = "https://www.release.tdnet.info" + href if href.startswith("/") else href
                try:
                    dt = datetime.strptime(date_raw, "%Y/%m/%d %H:%M")
                    key_ts = dt.strftime("%Y-%m-%d %H:%M")
                except Exception:
                    key_ts = date_raw
                if code and title:
                    lookup[(code, key_ts)] = {"name_en": name, "title_en": title, "eng_url": url}
            if len(rows) < 200:
                break
            page += 1
    except Exception as exc:
        print(f"English TDnet lookup error (non-fatal): {exc}")
    return lookup


def fetch_tdnet_range(d_from: str, d_to: str, en_lookup: dict = None, session=None,
                       timeout: int = 20, retries: int = 3) -> list:
    """Fetch Yanoshin's JP filing list for a (typically short, e.g. 3-day)
    date range (YYYYMMDD strings), enriched with English cross-references
    from en_lookup if provided. Returns a list of dicts matching CSV_FIELDS.
    Raises on repeated failure — callers should catch and continue."""
    import requests
    from bs4 import BeautifulSoup

    _req = session or requests
    url = f"https://webapi.yanoshin.jp/webapi/tdnet/list/{d_from}-{d_to}.html?limit=500"
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,*/*", "Accept-Language": "ja,en;q=0.9"}

    last_exc = None
    resp = None
    for attempt in range(retries):
        try:
            resp = _req.get(url, headers=headers, timeout=timeout)
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    if last_exc:
        raise last_exc
    if resp.status_code != 200:
        raise RuntimeError(f"Yanoshin TDnet HTTP {resp.status_code} for {d_from}-{d_to}")

    soup = BeautifulSoup(resp.content, "lxml")
    en_lookup = en_lookup or {}
    out = []
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        pdate_str = cells[0].get_text(strip=True)
        try:
            pdate_str = datetime.strptime(pdate_str, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M")
        except Exception:
            pass

        company_raw = cells[1].get_text(strip=True)
        m = re.match(r"\[([^\]]+)\](.*)", company_raw)
        code = _normalize_code(m.group(1)) if m else ""
        name = m.group(2).strip() if m else company_raw

        title_cell = cells[2]
        title = title_cell.get_text(strip=True)
        a_tag = title_cell.find("a")
        doc_url = ""
        if a_tag and a_tag.get("href"):
            href = a_tag["href"]
            rd = re.search(r"/rd\.php\?(https?://.*)", href)
            if rd:
                doc_url = rd.group(1)
            elif href.startswith("http"):
                doc_url = href
            else:
                doc_url = "https://webapi.yanoshin.jp" + href

        if not title or not code:
            continue

        en_match = en_lookup.get((code, pdate_str))
        out.append({
            "Code": code,
            "Name": name,
            "NameEN": en_match["name_en"] if en_match else "",
            "Title": title,
            "TitleEN": en_match["title_en"] if en_match else "",
            "PubDateTime": pdate_str,
            "DocUrl": doc_url,
            "EngUrl": en_match["eng_url"] if en_match else "",
        })
    return out


def load_tdnet_filings_from_github(repo: str, token: str = None) -> list:
    """Load the pre-built rolling TDnet index from data/tdnet_filings.csv
    (maintained by daily_tdnet.yml / backfill_tdnet.yml)."""
    import requests

    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"

    url = f"https://raw.githubusercontent.com/{repo}/main/data/tdnet_filings.csv"
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code == 404:
        return []
    if r.status_code != 200:
        print(f"TDnet filings fetch error: {r.status_code}")
        return []

    out = []
    for row in csv.DictReader(io.StringIO(r.text)):
        code = _normalize_code(row.get("Code", ""))
        if not code:
            continue
        row["Code"] = code
        out.append(row)
    print(f"TDnet filings loaded: {len(out)} rows")
    return out


def row_key(row: dict):
    return (row.get("Code", ""), row.get("DocUrl") or row.get("Title", ""), row.get("PubDateTime", ""))


def prune_and_dedupe(rows: list, retention_days: int = RETENTION_DAYS) -> list:
    cutoff = datetime.now() - timedelta(days=retention_days)
    seen = {}
    for row in rows:
        key = row_key(row)
        if not key[0] or not key[1]:
            continue
        try:
            pub_dt = datetime.strptime(row.get("PubDateTime", "")[:16], "%Y-%m-%d %H:%M")
        except Exception:
            pub_dt = None
        if pub_dt and pub_dt < cutoff:
            continue
        seen[key] = row
    return list(seen.values())


def write_filings_csv(path: str, rows: list):
    rows = sorted(rows, key=lambda r: r.get("PubDateTime", ""), reverse=True)
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
