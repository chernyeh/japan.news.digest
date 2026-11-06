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
    exact = name_candidates(text, names_lookup)
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        # Two listed companies share this exact name (TOA Corporation, Shinwa
        # Co., Ltd.). Taking the first was a coin flip that stored the wrong
        # ticker for one of them; nothing is better than the wrong company.
        return ""
    # Last resort: a unique substring match, so "Toyota" finds "TOYOTA MOTOR
    # CORPORATION". Ambiguous matches resolve to nothing rather than to a
    # coin flip between two companies.
    hits = [str(c) for c, nm in (names_lookup or {}).items()
            if _text(nm) and low in _text(nm).lower()]
    return hits[0] if len(hits) == 1 else ""


def name_candidates(name: str, names_lookup: dict = None) -> list:
    """Every code whose listed name is exactly `name` (case-insensitive)."""
    low = _text(name).lower()
    if not low:
        return []
    return sorted(str(c) for c, nm in (names_lookup or {}).items()
                  if _text(nm) and _text(nm).lower() == low)


def display_name(code: str, entry: dict = None, names_lookup: dict = None) -> str:
    """The name to show for an entry. An entry added by typing its code was
    stored with the code as its name ("6098"), which is no use in a table;
    the listed name wins whenever the stored one is blank or just the code."""
    stored = _text((entry or {}).get("name"))
    listed = _text((names_lookup or {}).get(code)) if not code.startswith(_UNRESOLVED) else ""
    if listed and (not stored or stored == code):
        return listed
    return stored or listed or code.replace(_UNRESOLVED, "")


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
    if key.startswith(_UNRESOLVED):
        both = name_candidates(company, names_lookup)
        if len(both) > 1:
            return False, (f'"{company}" is the listed name of {len(both)} companies '
                           f'({", ".join(both)}). Add it by its 4-digit code instead.')
    name = display_name(key, {"name": company}, names_lookup)
    if key in entries:
        return True, f"{name} is already on the watchlist."
    entries[key] = {"name": name, "added_at": date.today().isoformat()}
    _write_local(entries)
    return _push(entries, repo, token, f"add {name}")


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


def remove_many(keys: list, repo: str = "", token: str = "") -> tuple:
    """Remove several entries by their store key in one save (one commit)."""
    entries = _read_local()
    gone = [k for k in keys or [] if k in entries]
    if not gone:
        return True, "Nothing to remove."
    for k in gone:
        entries.pop(k)
    _write_local(entries)
    ok, msg = _push(entries, repo, token, "remove " + ", ".join(
        k.replace(_UNRESOLVED, "") for k in gone))
    return ok, (f"Removed {len(gone)} compan{'y' if len(gone) == 1 else 'ies'}." if ok else msg)


# ── Thesis: the numbers you hold each company to ───────────────────────────
# Typed in the app and kept on the entry itself, so they travel with the list
# into data/watchlist.json and every session sees the same figures.

ACTIONS = ("BUY", "ADD", "TRIM", "SELL", "WATCH")
THESIS_FIELDS = ("conviction", "munger_price", "bear_price", "exit_price", "action",
                 "review_date", "note")
NOTE_MAX = 200


def _num(value):
    """A positive float, or None for blank, zero, NaN or anything unparseable."""
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(str(value).replace(",", "").replace("¥", "").strip())
    except (TypeError, ValueError):
        return None
    return out if out > 0 and out == out else None


def clean_thesis(raw: dict) -> dict:
    """Validated thesis fields, with every blank or invalid value dropped.

    Conviction is the 1-10 score from the deep-dive template; prices are yen
    per share (Munger = margin-of-safety entry, Bear = bear-case value, the
    downside a reward-to-risk ratio is measured against, Exit = fully valued);
    the review date is ISO (YYYY-MM-DD). Returned without the blank keys so an
    untouched field never overwrites anything with None. `action_since` is
    not taken from input: set_theses stamps it."""
    raw = raw or {}
    out = {}
    conv = _num(raw.get("conviction"))
    if conv is not None and 1 <= conv <= 10:
        out["conviction"] = int(round(conv))
    for k in ("munger_price", "bear_price", "exit_price"):
        v = _num(raw.get(k))
        if v is not None:
            out[k] = round(v, 2)
    act = _text(raw.get("action")).upper()
    if act in ACTIONS:
        out["action"] = act
    rd = raw.get("review_date")
    if hasattr(rd, "isoformat"):
        rd = rd.isoformat()
    rd = _text(rd)[:10]
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", rd):
        try:
            date.fromisoformat(rd)
            out["review_date"] = rd
        except ValueError:
            pass
    note = _text(raw.get("note"))
    if note:
        out["note"] = note[:NOTE_MAX]
    return out


def get_thesis(code: str) -> dict:
    return dict((_read_local().get(code) or {}).get("thesis") or {})


def set_theses(updates: dict, repo: str = "", token: str = "") -> tuple:
    """Replace the thesis of each company in `updates` ({code: raw fields}) and
    save once. A company whose fields all come back blank has its thesis
    cleared. Returns (ok, message) with the same meaning as add_to_watchlist;
    ("nothing", ...) is not a thing — no change is reported as ok."""
    entries = _read_local()
    changed = []
    for code, raw in (updates or {}).items():
        if code not in entries:
            continue
        new = clean_thesis(raw)
        old = entries[code].get("thesis") or {}
        # When the action was set, so "on WATCH for 74 days" can be said. Kept
        # while the action is unchanged; restamped when it changes.
        if new.get("action"):
            new["action_since"] = (old["action_since"]
                                   if new["action"] == old.get("action") and old.get("action_since")
                                   else date.today().isoformat())
        if new == old:
            continue
        if new:
            entries[code]["thesis"] = new
            entries[code]["thesis_updated"] = date.today().isoformat()
        else:
            entries[code].pop("thesis", None)
            entries[code].pop("thesis_updated", None)
        changed.append(code)
    if not changed:
        return True, "No thesis figures changed."
    _write_local(entries)
    ok, msg = _push(entries, repo, token, "thesis " + ", ".join(changed))
    return ok, (f"Saved thesis for {len(changed)} compan{'y' if len(changed) == 1 else 'ies'}."
                if ok else msg)


# ── GitHub-backed durable copy ───────────────────────────────────────────
# Same shape as research_links.py: read via the public raw URL, write via the
# authenticated Contents API, retry once on a 409 (someone else's write landed
# between our read and our PUT).

def load_from_github(repo: str, token: str = "") -> dict:
    """{code: entry} from data/watchlist.json, or {} if it is not there yet."""
    import gh_read
    data = gh_read.raw_json(repo, WATCHLIST_PATH, token, {})
    return data if isinstance(data, dict) else {}


def sync_and_report(repo: str, token: str = "") -> dict:
    """sync_from_github, plus what the app needs to say whether the list is
    actually saved: {"entries", "remote_found", "remote_count", "seed"}.

    `remote_found` False with entries on hand is the state that silently lost
    the list on every restart: everything lived in the local cache and the
    repo had no data/watchlist.json at all. `seed` is the (ok, message) of the
    push that tries to fix that, or None if none was attempted."""
    remote = load_from_github(repo, token)
    local = _read_local()
    merged = {**local, **remote}
    if merged:
        _write_local(merged)
    seed = None
    if local and not remote and token:
        seed = _push(merged, repo, token, "seed from local cache")
    return {"entries": merged, "remote_found": bool(remote),
            "remote_count": len(remote), "seed": seed}


def save_all(repo: str, token: str = "") -> tuple:
    """Push the whole local list as it stands — the "Save now" button, for a
    list that only ever reached the session cache."""
    return _push(_read_local(), repo, token, "save")


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
    import gh_read
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
                return False, gh_read.write_error(r.status_code, r.text, token,
                                                  r.headers, repo)
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
        return False, gh_read.write_error(put.status_code, put.text, token,
                                          put.headers, repo)
    return False, "GitHub write failed after retry (concurrent edit)."


# Legal suffixes stripped to get the name a headline actually uses: "Kyocera
# Corporation" is "Kyocera" in every headline, so matching the full legal name
# found almost nothing for any company outside KNOWN_COMPANIES.
_SUFFIX_RE = re.compile(
    r"[\s,]+(co\.?,?\s*ltd\.?|co\.?,?\s*limited|company,?\s*limited|corporation|"
    r"corp\.?|incorporated|inc\.?|limited|ltd\.?|k\.?\s?k\.?|plc|holdings?|group)$",
    re.IGNORECASE)
# A one-word stem that names a whole group or a place matches other companies'
# news ("Mitsubishi Corporation" -> "Mitsubishi" would claim Mitsubishi UFJ's
# headlines), so such a stem is never used alone.
_GENERIC_STEMS = {
    "mitsubishi", "mitsui", "sumitomo", "nippon", "japan", "tokyo", "osaka",
    "toyota", "hitachi", "nissan", "daiwa", "nomura", "fuji", "yamaha", "seven",
    "east", "west", "central", "kansai", "kyushu", "hokkaido", "chubu", "tohoku",
    "shikoku", "chugoku", "sanyo", "taiyo", "nihon", "dai", "shin", "toa", "asahi",
}


def short_name(name: str) -> str:
    """The name with its legal suffixes taken off ("Hitachi, Ltd." -> "Hitachi")."""
    out = _text(name)
    while True:
        trimmed = _SUFFIX_RE.sub("", out).strip(" ,.")
        if trimmed == out or not trimmed:
            return out
        out = trimmed


def get_company_aliases(company_name: str, code: str = "") -> list:
    """Lower-case search terms for a company: its KNOWN_COMPANIES aliases where
    it is one of those, its name, its short name where that is specific
    enough, and its ticker."""
    name = _text(company_name)
    low = name.lower()
    out = []
    for known, aliases in KNOWN_COMPANIES.items():
        codes = [a for a in aliases if re.fullmatch(r"\d{4}", a)]
        if low == known.lower() or (code and code in codes):
            out += [known.lower()] + [a.lower() for a in aliases]
    if low:
        out.append(low)
    stem = short_name(name).lower()
    if stem and stem != low and len(stem) >= 4 and not (
            " " not in stem and stem in _GENERIC_STEMS):
        out.append(stem)
    if code and not code.startswith(_UNRESOLVED):
        out.append(code.lower())
    return list(dict.fromkeys(out))


def _alias_in(alias: str, text: str) -> bool:
    """Whole-word match, so "ntt" does not match inside "button" and "6098"
    does not match inside "160980". Japanese aliases have no word boundaries
    to speak of and are matched as plain substrings."""
    if not alias:
        return False
    if not alias.isascii():
        return alias in text
    return re.search(r"(?<![a-z0-9])" + re.escape(alias) + r"(?![a-z0-9])", text) is not None


def article_matches(article: dict, aliases: list, code: str = "") -> bool:
    """True if the article is about this company: tagged with its ticker by the
    AI extraction, extracted company name matches, or a headline mentions it."""
    tse_code = code or next((a for a in aliases if re.fullmatch(r"\d{4}", a)), "")
    if tse_code and (article.get("company_code") or "").strip() == tse_code:
        return True
    ai_name = (article.get("company_name_clean") or "").lower()
    if ai_name and any(len(a) > 3 and not a.isdigit() and (a in ai_name or ai_name in a)
                       for a in aliases):
        return True
    text = ((article.get("translated_title") or article.get("title", "") or "") + " "
            + (article.get("original_title", "") or "")).lower()
    return any(len(a) > 2 and _alias_in(a, text) for a in aliases)


def scan_articles_for_company(company_name: str, all_articles: list, code: str = "") -> list:
    """Every article mentioning one company (see article_matches)."""
    aliases = get_company_aliases(company_name, code)
    return [a for a in all_articles if article_matches(a, aliases, code)]


def scan_all_watchlist(watchlist, articles_by_sector: dict, names_lookup: dict = None) -> dict:
    """{display name: [matching articles]} for every watched company.

    `watchlist` is the entries dict ({code: entry}), so each company is matched
    on its ticker as well as its name; a plain list of names (the old shape)
    is still accepted and matched on names alone."""
    if not articles_by_sector or not isinstance(articles_by_sector, dict):
        return {}
    if not watchlist:
        return {}
    all_articles = [
        article
        for sector_articles in articles_by_sector.values()
        for article in (sector_articles or [])
    ]
    if isinstance(watchlist, dict):
        pairs = [(display_name(k, e, names_lookup),
                  "" if k.startswith(_UNRESOLVED) else k) for k, e in watchlist.items()]
    else:
        pairs = [(n, resolve_code(n, names_lookup)) for n in watchlist if _text(n)]
    results = {}
    for name, code in pairs:
        matches = scan_articles_for_company(name, all_articles, code)
        if matches:
            results[name] = matches
    return results
