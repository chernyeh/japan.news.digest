"""
watchlist.py
Manages a user watchlist of TSE-listed companies.
Scans all fetched articles for mentions of watched companies.

**Storage: GitHub is the truth, the local file is a cache.** The local
watchlist.json is neither committed nor gitignored — it is written at runtime,
so on Streamlit Cloud it vanished on every restart and redeploy and the
watchlist silently emptied itself. The durable copy now lives at
data/watchlist.json in the repo, written through the GitHub Contents API by
the same read-sha-then-PUT-with-409-retry dance research_links.py uses.

The local file is still read on every `load_watchlist()` call, because that
happens several times per rerun and an HTTP round trip each time would be
unusable. `sync_from_github()` refreshes it once per session.

Entries are keyed by **4-digit TSE code**, `{code: {"name", "added_at"}}`, so
every other feature can join on the ticker. The old format was a bare list of
names with no code at all, which is why a freehand entry could never be matched
to a company. A name that cannot be resolved to a code is still stored, under
an `_unresolved:` key, rather than dropped.
"""

import json
import os
import re
from datetime import date

WATCHLIST_FILE = "watchlist.json"      # local cache
WATCHLIST_PATH = "data/watchlist.json"  # durable copy, in the repo
_UA = "japan-news-digest-watchlist"
_UNRESOLVED = "_unresolved:"

# Common TSE companies with their aliases for matching
KNOWN_COMPANIES = {
    "Toyota":             ["toyota", "トヨタ", "7203"],
    "Sony":               ["sony", "ソニー", "6758"],
    "SoftBank":           ["softbank", "ソフトバンク", "9984"],
    "Nintendo":           ["nintendo", "任天堂", "7974"],
    "Honda":              ["honda", "本田", "7267"],
    "Keyence":            ["keyence", "キーエンス", "6861"],
    "Tokyo Electron":     ["tokyo electron", "東京エレクトロン", "8035"],
    "Fanuc":              ["fanuc", "ファナック", "6954"],
    "NTT":                ["ntt", "日本電信電話", "9432"],
    "KDDI":               ["kddi", "9433"],
    "Recruit":            ["recruit", "リクルート", "6098"],
    "Mitsubishi UFJ":     ["mitsubishi ufj", "三菱UFJ", "8306"],
    "Sumitomo Mitsui":    ["sumitomo mitsui", "住友三井", "smbc", "8316"],
    "Mizuho":             ["mizuho", "みずほ", "8411"],
    "Daikin":             ["daikin", "ダイキン", "6367"],
    "Shin-Etsu Chemical": ["shin-etsu", "shinetsu", "信越化学", "4063"],
    "Chugai Pharma":      ["chugai", "中外製薬", "4519"],
    "Hitachi":            ["hitachi", "日立", "6501"],
    "Panasonic":          ["panasonic", "パナソニック", "6752"],
    "Denso":              ["denso", "デンソー", "6902"],
    "Murata":             ["murata", "村田製作所", "6981"],
    "Olympus":            ["olympus", "オリンパス", "7733"],
    "Fast Retailing":     ["fast retailing", "uniqlo", "ユニクロ", "ファーストリテイリング", "9983"],
    "Oriental Land":      ["oriental land", "disney japan", "オリエンタルランド", "4661"],
    "Hoya":               ["hoya", "ホヤ", "7741"],
    "Advantest":          ["advantest", "アドバンテスト", "6857"],
    "Lasertec":           ["lasertec", "レーザーテック", "6920"],
    "Disco":              ["disco corporation", "ディスコ", "6146"],
    "Rohm":               ["rohm", "ローム", "6963"],
    "Renesas":            ["renesas", "ルネサス", "6723"],
}


def _read_local() -> dict:
    """The local cache as {code: entry}. Accepts the legacy bare list of names
    and converts it, so an existing watchlist is carried over rather than lost
    the first time this runs."""
    if not os.path.exists(WATCHLIST_FILE):
        return {}
    try:
        with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        return {key_for(name): {"name": name, "added_at": ""} for name in raw if name}
    return {}


def _write_local(entries: dict):
    try:
        with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception as exc:
        print(f"Watchlist cache write failed: {exc}")


def _text(value) -> str:
    """A trimmed string for anything, including the things that are not strings.

    NAMES_LOOKUP is built from a pandas frame, and 134 of the 3,394 rows in
    metadata.csv have no Name -- which pandas reads as float NaN, not as an
    empty string. NaN is truthy, so it walks straight past an `if nm:` guard
    and only fails on `.strip()`, which is how favouriting a company from the
    Research tab died with "'float' object has no attribute 'strip'".
    """
    if value is None or isinstance(value, float):   # float covers NaN
        return ""
    return str(value).strip()


def resolve_code(name_or_code: str, names_lookup: dict = None) -> str:
    """A 4-digit TSE code for a name or code, or "" if it cannot be resolved.

    Three passes, cheapest first: the input already being a code, the hardcoded
    KNOWN_COMPANIES aliases, then the full name map the app loads from
    metadata.csv (passed in rather than imported, so this module stays free of
    any dependency on app.py)."""
    text = _text(name_or_code)
    if not text:
        return ""
    if re.fullmatch(r"\d{4}", text) or re.fullmatch(r"\d{3}[A-Z]", text):
        return text
    low = text.lower()
    for name, aliases in KNOWN_COMPANIES.items():
        if low == name.lower() or low in [a.lower() for a in aliases]:
            code = next((a for a in aliases if re.fullmatch(r"\d{4}", a)), "")
            if code:
                return code
    for code, nm in (names_lookup or {}).items():
        if _text(nm).lower() == low and _text(nm):
            return str(code)
    # Last resort: a unique substring match, so "Toyota" finds "TOYOTA MOTOR
    # CORPORATION". Ambiguous matches resolve to nothing rather than to a
    # coin flip between two companies.
    hits = [str(c) for c, nm in (names_lookup or {}).items()
            if _text(nm) and low in _text(nm).lower()]
    return hits[0] if len(hits) == 1 else ""


def key_for(name_or_code: str, names_lookup: dict = None) -> str:
    """The store key for an entry: its code, or an `_unresolved:` marker."""
    code = resolve_code(name_or_code, names_lookup)
    return code or f"{_UNRESOLVED}{_text(name_or_code)}"


def load_watchlist_entries() -> dict:
    """{code: {"name", "added_at"}} from the local cache."""
    return _read_local()


def load_watchlist() -> list:
    """Watched company **names**, which is what every existing caller expects —
    article matching, the digest and the Watchlist tab all key on the name."""
    return [e.get("name", "") for e in _read_local().values() if e.get("name")]


def load_watchlist_codes() -> list:
    """Watched 4-digit codes only, skipping entries that never resolved."""
    return [k for k in _read_local() if not k.startswith(_UNRESOLVED)]


def load_codes_from_disk(path: str = WATCHLIST_PATH) -> list:
    """Watched codes read straight off disk, for a CI runner that has the repo
    checked out but no session and no token.

    Prefers the durable copy committed to the repo and falls back to the local
    runtime cache, so this works both in a workflow and on a developer machine.
    Unresolved entries are skipped: they have no code to collect against."""
    for candidate in (path, WATCHLIST_FILE):
        if not candidate or not os.path.exists(candidate):
            continue
        try:
            with open(candidate, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            continue
        if isinstance(raw, dict):
            return [k for k in raw if not k.startswith(_UNRESOLVED)]
        if isinstance(raw, list):
            return [c for c in (resolve_code(n) for n in raw if n) if c]
    return []


def save_watchlist(watchlist):
    """Kept for backwards compatibility: accepts either the new dict or the old
    list of names."""
    if isinstance(watchlist, list):
        watchlist = {key_for(n): {"name": n, "added_at": ""} for n in watchlist if n}
    _write_local(watchlist)


def add_to_watchlist(company: str, repo: str = "", token: str = "",
                     names_lookup: dict = None) -> tuple:
    """Add a company. Returns (ok, message) — ok refers to the *durable* write,
    so a False with the cache updated means "kept for this session only", the
    same contract research_links.save_link uses."""
    # _text rather than .strip(): a caller reading a name out of a pandas frame
    # can hand this a NaN, which is truthy and has no .strip().
    company = _text(company)
    if not company:
        return False, "Nothing to add."
    entries = _read_local()
    key = key_for(company, names_lookup)
    if key in entries:
        return True, f"{company} is already on the watchlist."
    entries[key] = {"name": company, "added_at": date.today().isoformat()}
    _write_local(entries)
    return _push(entries, repo, token, f"add {company}")


def remove_from_watchlist(company: str, repo: str = "", token: str = "",
                          names_lookup: dict = None) -> tuple:
    """Remove by name or code. Returns (ok, message), as add_to_watchlist does."""
    entries = _read_local()
    key = key_for(company, names_lookup)
    if key not in entries:
        # Fall back to matching on the stored name, so removing works from a UI
        # that only ever had the display name to hand.
        key = next((k for k, e in entries.items()
                    if (e.get("name") or "").strip().lower() == (company or "").strip().lower()), "")
    if not key or key not in entries:
        return True, "Not on the watchlist."
    entries.pop(key)
    _write_local(entries)
    return _push(entries, repo, token, f"remove {company}")


# ── GitHub-backed durable copy ───────────────────────────────────────────
# Same shape as research_links.py: read via the public raw URL, write via the
# authenticated Contents API, retry once on a 409 (someone else's write landed
# between our read and our PUT).

def load_from_github(repo: str, token: str = "") -> dict:
    """{code: entry} from data/watchlist.json, or {} if it is not there yet."""
    import requests
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    url = f"https://raw.githubusercontent.com/{repo}/main/{WATCHLIST_PATH}"
    try:
        r = requests.get(url, headers=headers, timeout=15)
    except Exception as exc:
        print(f"Watchlist fetch error: {exc}")
        return {}
    if r.status_code == 404:
        return {}
    if r.status_code != 200:
        print(f"Watchlist fetch error: {r.status_code}")
        return {}
    try:
        data = r.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def sync_from_github(repo: str, token: str = "") -> dict:
    """Refresh the local cache from the durable copy. Call once per session.

    The union is deliberate rather than a straight overwrite: a company added
    while the token was missing lives only in the local cache, and a blind
    overwrite from GitHub would throw it away. Where both sides have the same
    key, GitHub wins."""
    remote = load_from_github(repo, token)
    local = _read_local()
    if not remote and not local:
        return {}
    merged = {**local, **remote}
    _write_local(merged)
    # First run against a repo that has no data/watchlist.json yet: seed it, so
    # the existing local list becomes durable instead of waiting for the next
    # add to carry it up.
    if local and not remote and token:
        _push(merged, repo, token, "seed from local cache")
    return merged


def _push(entries: dict, repo: str, token: str, what: str) -> tuple:
    if not token or not repo:
        return False, "No GITHUB_TOKEN configured — watchlist kept for this session only."
    import base64
    import requests
    api_url = f"https://api.github.com/repos/{repo}/contents/{WATCHLIST_PATH}"
    headers = {"User-Agent": _UA, "Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}
    body_content = base64.b64encode(
        json.dumps(entries, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    ).decode("ascii")
    for attempt in range(2):
        sha = None
        try:
            r = requests.get(api_url, headers=headers, timeout=15)
            if r.status_code == 200:
                sha = r.json().get("sha")
            elif r.status_code != 404:
                return False, f"GitHub read error: HTTP {r.status_code}"
        except Exception as exc:
            return False, f"GitHub read error: {exc}"
        body = {"message": f"chore: watchlist — {what} [skip ci]",
                "content": body_content, "branch": "main"}
        if sha:
            body["sha"] = sha
        try:
            put = requests.put(api_url, headers=headers, json=body, timeout=15)
        except Exception as exc:
            return False, f"GitHub write error: {exc}"
        if put.status_code in (200, 201):
            return True, "Saved."
        if put.status_code == 409 and attempt == 0:
            continue
        return False, f"GitHub write error: HTTP {put.status_code} — {put.text[:200]}"
    return False, "GitHub write failed after retry (concurrent edit)."


def get_company_aliases(company_name: str) -> list:
    """Get all search terms for a company."""
    # Check known companies first
    for name, aliases in KNOWN_COMPANIES.items():
        if company_name.lower() == name.lower():
            return [name.lower()] + [a.lower() for a in aliases]

    # For custom entries, just use the name itself
    return [company_name.lower()]


def scan_articles_for_company(company_name: str, all_articles: list) -> list:
    """
    Find all articles mentioning a specific company.
    Matches on: keyword aliases in title, AI-extracted company_code,
    and AI-extracted company_name_clean.
    """
    aliases = get_company_aliases(company_name)

    # Extract TSE code from aliases (4-digit numeric)
    tse_code = next((a for a in aliases if a.isdigit() and len(a) == 4), None)

    matches = []
    for article in all_articles:
        matched = False

        # 1. Match on AI-extracted company_code (fastest, most precise)
        if tse_code and article.get("company_code") == tse_code:
            matched = True

        # 2. Match on AI-extracted company_name_clean
        if not matched:
            ai_name = (article.get("company_name_clean") or "").lower()
            if ai_name and any(alias in ai_name or ai_name in alias
                               for alias in aliases if len(alias) > 3):
                matched = True

        # 3. Fallback: keyword search in title
        if not matched:
            text = (
                (article.get("translated_title") or article.get("title", "")) + " " +
                article.get("original_title", "")
            ).lower()
            for alias in aliases:
                if alias and len(alias) > 2 and alias in text:
                    matched = True
                    break

        if matched:
            matches.append(article)

    return matches


def scan_all_watchlist(watchlist: list, articles_by_sector: dict) -> dict:
    """
    Scan all articles for all watchlist companies.
    Returns dict of {company_name: [matching_articles]}
    """
    if not articles_by_sector or not isinstance(articles_by_sector, dict):
        return {}
    if not watchlist:
        return {}
    # Flatten all articles
    all_articles = [
        article
        for sector_articles in articles_by_sector.values()
        for article in (sector_articles or [])
    ]

    results = {}
    for company in watchlist:
        matches = scan_articles_for_company(company, all_articles)
        if matches:
            results[company] = matches

    return results
