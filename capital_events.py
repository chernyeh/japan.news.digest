"""
capital_events.py — buybacks, cancellations and dividend changes, as dated
events per company, in data/capital_events.json.

A dividend change is also visible in the estimates log (DPS guidance moving),
so it is logged here only when typed on the results-day screen, where it
carries the note and the source with it. A buyback is not visible anywhere
else in the data: TDnet tells us a notice was filed, not how much, so the
amount, share count and window are typed — prefilled from the notice title.

Every event keeps the date it was announced and the date it was logged, so
"what did they say they would do" can be checked later against what the share
count actually did.
"""

import base64
import json
import re

EVENTS_PATH = "data/capital_events.json"
_UA = "japan-news-digest"

EVENT_TYPES = ("Buyback announced", "Buyback completed", "Share cancellation",
               "Dividend change", "Special dividend", "Other")


def new_id(code: str, date: str, etype: str, existing: list) -> str:
    """A stable id: code, date, type, and a counter for two of a kind on one day."""
    slug = re.sub(r"[^a-z]+", "-", (etype or "event").lower()).strip("-")
    base = f"{code}-{date}-{slug}"
    n = sum(1 for e in existing or () if str(e.get("id", "")).startswith(base))
    return base if n == 0 else f"{base}-{n + 1}"


def pct_of_shares(shares, outstanding, treasury=None):
    """A buyback's size as a share of the shares in issue *excluding treasury
    stock* — the way a Japanese notice states it (発行済株式総数（自己株式を除く）
    に対する割合). None where the share count is unknown."""
    try:
        shares, outstanding = float(shares), float(outstanding)
    except (TypeError, ValueError):
        return None
    base = outstanding - (float(treasury) if treasury else 0.0)
    return shares / base if base > 0 and shares > 0 else None


# TDnet titles that announce a capital-return event. The decision notice is
# the one that carries the size; the result / completion notices say how much
# was actually bought.
_TITLE_RULES = (
    (("消却",), "Share cancellation"),
    (("取得結果", "取得終了", "取得状況", "買付結果", "取得の結果"), "Buyback completed"),
    (("自己株式の取得", "自己株式取得", "自社株買い", "自己株式立会外買付"), "Buyback announced"),
    (("特別配当", "記念配当"), "Special dividend"),
    (("増配", "配当予想の修正", "剰余金の配当", "配当予想"), "Dividend change"),
)


def classify_title(title: str) -> str:
    """The event type a TDnet title announces, or "" if it is not one. The
    first rule that matches wins, which is why "results and cancellation" in
    one notice reads as a cancellation: that is the part that moves the
    share count."""
    t = title or ""
    for keys, etype in _TITLE_RULES:
        if any(k in t for k in keys):
            return etype
    return ""


def suggestions(tdnet_rows: list, code: str, logged: list, since: str = "") -> list:
    """TDnet notices for `code` that announce a capital-return event and have
    not been logged yet, newest first: [{date, type, title, title_en, url}]."""
    have = {(e.get("date"), e.get("type")) for e in logged or ()}
    have_titles = {e.get("tdnet_title") for e in logged or () if e.get("tdnet_title")}
    out = []
    for r in tdnet_rows or ():
        if str(r.get("Code", ""))[:4] != str(code)[:4]:
            continue
        etype = classify_title(r.get("Title", ""))
        date = (r.get("PubDateTime") or "")[:10]
        if not etype or not date or date < since:
            continue
        if r.get("Title") in have_titles or (date, etype) in have:
            continue
        out.append({"date": date, "type": etype, "title": r.get("Title", ""),
                    "title_en": r.get("TitleEN", ""),
                    "url": r.get("EngUrl") or r.get("DocUrl", "")})
    return sorted(out, key=lambda e: e["date"], reverse=True)


def share_count_trend(log_rows: list) -> list:
    """[(date, shares, treasury)] from the estimates log's filing rows, oldest
    first — what the share count actually did, to set against what was
    announced."""
    pts = {}
    for r in log_rows or ():
        if r.get("metric") in ("shares", "treasury"):
            try:
                v = float(r.get("value"))
            except (TypeError, ValueError):
                continue
            pts.setdefault(r["date"], {})[r["metric"]] = v
    return [(d, p.get("shares"), p.get("treasury")) for d, p in sorted(pts.items())
            if p.get("shares")]


# ── GitHub-backed store ────────────────────────────────────────────────────

def load_from_github(repo: str, token: str = None) -> dict:
    import gh_read
    return gh_read.raw_json(repo, EVENTS_PATH, token, {})


def save_via_github(repo: str, token: str, code: str, upsert: list = (),
                    remove_ids=()) -> tuple:
    """Insert or replace events (by id) and drop `remove_ids` in `code`'s list,
    committed to main. (ok, message). Retries once on a 409."""
    import requests
    if not token:
        return False, "No GITHUB_TOKEN configured — kept for this session only."
    api = f"https://api.github.com/repos/{repo}/contents/{EVENTS_PATH}"
    headers = {"User-Agent": _UA, "Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}
    for attempt in range(2):
        r = requests.get(api, headers=headers, timeout=15)
        if r.status_code == 404:
            sha, data = None, {}
        elif r.status_code == 200:
            payload = r.json()
            sha = payload.get("sha")
            try:
                data = json.loads(base64.b64decode(payload.get("content", "")).decode("utf-8"))
            except Exception:
                data = {}
        else:
            return False, f"GitHub read error: HTTP {r.status_code}"
        data = apply(data, code, upsert, remove_ids)
        body = {"message": f"chore: capital events for {code} [skip ci]",
                "content": base64.b64encode(json.dumps(
                    data, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
                ).decode("ascii"),
                "branch": "main"}
        if sha:
            body["sha"] = sha
        put = requests.put(api, headers=headers, json=body, timeout=15)
        if put.status_code in (200, 201):
            parts = []
            if upsert:
                parts.append(f"Logged {len(upsert)} event(s).")
            if remove_ids:
                parts.append(f"Removed {len(remove_ids)}.")
            return True, " ".join(parts) or "Nothing to save."
        if put.status_code == 409 and attempt == 0:
            continue
        return False, f"GitHub write error: HTTP {put.status_code}"
    return False, "GitHub write failed after retry."


def apply(data: dict, code: str, upsert=(), remove_ids=()) -> dict:
    """Pure: the store with `upsert` merged into `code`'s events by id and
    `remove_ids` dropped, newest first."""
    data = dict(data or {})
    events = {e["id"]: e for e in data.get(code, []) if e.get("id")}
    for e in upsert or ():
        if e.get("id"):
            events[e["id"]] = dict(e)
    for i in remove_ids or ():
        events.pop(i, None)
    if events:
        data[code] = sorted(events.values(), key=lambda e: (e.get("date", ""), e["id"]),
                            reverse=True)
    else:
        data.pop(code, None)
    return data
