"""
ai_summary_store.py — durable, GitHub-backed store for finished AI briefings
(News, Reg Filings, Breaking, Market Wrap, etc.), so a briefing generated on
one device is still there after the app restarts/redeploys and you open the
app from another device.

Reads go through the public raw.githubusercontent.com URL (same pattern as
jquants.py's load_*_from_github helpers) — fast, no auth needed for a public
repo. Writes go through the authenticated GitHub Contents API and require a
GITHUB_TOKEN Streamlit secret with write access to this repo (the same one
used by the Research tab's saved links). Without a working token, briefings
still work for the lifetime of the running app process (via the existing
in-memory shared cache) but won't survive a restart.
"""

import base64
import json

SUMMARIES_PATH = "data/ai_summaries.json"
_UA = "japan-news-digest-ai-summaries"


def load_summaries_from_github(repo: str, token: str = None) -> dict:
    """Returns {session_key: {"text": str, "ts": iso str, "idx": {int: {...}}}}."""
    import requests
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    url = f"https://raw.githubusercontent.com/{repo}/main/{SUMMARIES_PATH}"
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code == 404:
        return {}
    if r.status_code != 200:
        print(f"AI summaries fetch error: {r.status_code}")
        return {}
    try:
        raw = r.json()
    except ValueError:
        return {}

    out = {}
    for key, entry in raw.items():
        if not isinstance(entry, dict):
            continue
        idx = {}
        for i, v in (entry.get("idx") or {}).items():
            try:
                idx[int(i)] = v
            except (TypeError, ValueError):
                pass
        out[key] = {"text": entry.get("text", ""), "ts": entry.get("ts", ""), "idx": idx}
    return out


def save_summary(repo: str, token: str, session_key: str, text: str, ts_iso: str, idx: dict) -> tuple:
    """Upserts session_key's entry in data/ai_summaries.json, committed straight
    to main (matching how the scheduled data-refresh workflows and the Research
    tab's saved links already commit data/ updates). Returns (ok, message).
    Retries once on a 409 (sha changed under us, e.g. a concurrent briefing
    finishing from another session)."""
    if not token:
        return False, "No GITHUB_TOKEN configured — briefing kept in-memory only for this app session."

    import requests
    api_url = f"https://api.github.com/repos/{repo}/contents/{SUMMARIES_PATH}"
    headers = {"User-Agent": _UA, "Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}

    entry = {"text": text, "ts": ts_iso, "idx": {str(k): v for k, v in (idx or {}).items()}}

    for attempt in range(2):
        r = requests.get(api_url, headers=headers, timeout=15)
        if r.status_code == 200:
            payload = r.json()
            sha = payload.get("sha")
            try:
                data = json.loads(base64.b64decode(payload.get("content", "")).decode("utf-8"))
            except Exception:
                data = {}
        elif r.status_code == 404:
            sha = None
            data = {}
        else:
            return False, f"GitHub read error: HTTP {r.status_code}"

        data[session_key] = entry
        new_content = base64.b64encode(
            json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
        ).decode("ascii")
        body = {
            "message": f"chore: update AI briefing ({session_key}) [skip ci]",
            "content": new_content,
            "branch": "main",
        }
        if sha:
            body["sha"] = sha

        put = requests.put(api_url, headers=headers, json=body, timeout=20)
        if put.status_code in (200, 201):
            return True, "Saved."
        if put.status_code == 409 and attempt == 0:
            continue  # sha changed under us — re-fetch and retry once
        return False, f"GitHub write error: HTTP {put.status_code} — {put.text[:200]}"

    return False, "GitHub write failed after retry (concurrent edit)."
