"""
research_links.py — GitHub-backed store for user-curated company research links
(IR pages, presentations, transcripts, etc.) shown in the Research tab.

Reads go through the public raw.githubusercontent.com URL (same pattern as
jquants.py's load_*_from_github helpers) — fast, no auth needed for a public
repo. Writes go through the authenticated GitHub Contents API, which requires
a GITHUB_TOKEN Streamlit secret with write access to this repo (a fine-grained
PAT scoped to just this repo with "Contents: Read and write" is the minimal
safe choice). Without a working token, added links still show for the rest of
the current session (kept in the shared in-memory cache) but are NOT
persisted — they'll disappear on the next restart/redeploy.
"""

import base64
import json

LINKS_PATH = "data/research_links.json"
_UA = "japan-news-digest-research-tab"

DOC_TYPES = [
    "IR Page",
    "Investor Presentation",
    "Earnings Call Transcript",
    "Q&A / Briefing Notes",
    "Annual Report (Company IR)",
    "Shareholder Meeting Materials",
    "Other",
]


def load_links_from_github(repo: str, token: str = None) -> dict:
    """Returns {sec_code: [link_dict, ...]}."""
    import requests
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    url = f"https://raw.githubusercontent.com/{repo}/main/{LINKS_PATH}"
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code == 404:
        return {}
    if r.status_code != 200:
        print(f"Research links fetch error: {r.status_code}")
        return {}
    try:
        return r.json()
    except ValueError:
        return {}


def _get_current(api_url: str, headers: dict):
    """Returns (sha_or_None, data_dict). 404 -> (None, {})."""
    import requests
    r = requests.get(api_url, headers=headers, timeout=15)
    if r.status_code == 404:
        return None, {}
    if r.status_code != 200:
        raise RuntimeError(f"GitHub read error: HTTP {r.status_code}")
    payload = r.json()
    sha = payload.get("sha")
    try:
        data = json.loads(base64.b64decode(payload.get("content", "")).decode("utf-8"))
    except Exception:
        data = {}
    return sha, data


def _put(api_url: str, headers: dict, data: dict, sha, message: str):
    import requests
    new_content = base64.b64encode(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    ).decode("ascii")
    body = {"message": message, "content": new_content, "branch": "main"}
    if sha:
        body["sha"] = sha
    return requests.put(api_url, headers=headers, json=body, timeout=15)


def save_link(repo: str, token: str, sec_code: str, link: dict) -> tuple:
    """Appends `link` to sec_code's list in data/research_links.json, committed
    straight to main (matching how the scheduled data-refresh workflows already
    commit data/ updates). Returns (ok, message). Retries once on a 409
    (sha changed under us, e.g. a concurrent add from another session)."""
    if not token:
        return False, "No GITHUB_TOKEN configured — link kept for this session only."

    api_url = f"https://api.github.com/repos/{repo}/contents/{LINKS_PATH}"
    headers = {"User-Agent": _UA, "Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}

    for attempt in range(2):
        try:
            sha, data = _get_current(api_url, headers)
        except RuntimeError as exc:
            return False, str(exc)
        data.setdefault(sec_code, []).append(link)
        msg = f"chore: add research link ({link.get('doc_type', 'link')}) for {sec_code} [skip ci]"
        put = _put(api_url, headers, data, sha, msg)
        if put.status_code in (200, 201):
            return True, "Saved."
        if put.status_code == 409 and attempt == 0:
            continue  # sha changed under us — re-fetch and retry once
        return False, f"GitHub write error: HTTP {put.status_code} — {put.text[:200]}"

    return False, "GitHub write failed after retry (concurrent edit)."


def delete_link(repo: str, token: str, sec_code: str, index: int) -> tuple:
    """Removes the link at `index` in sec_code's list. Same retry-on-409 pattern as save_link."""
    if not token:
        return False, "No GITHUB_TOKEN configured — cannot delete a persisted link."

    api_url = f"https://api.github.com/repos/{repo}/contents/{LINKS_PATH}"
    headers = {"User-Agent": _UA, "Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}

    for attempt in range(2):
        try:
            sha, data = _get_current(api_url, headers)
        except RuntimeError as exc:
            return False, str(exc)
        entries = data.get(sec_code, [])
        if index < 0 or index >= len(entries):
            return False, "Link no longer exists (already removed?)."
        removed = entries.pop(index)
        data[sec_code] = entries
        msg = f"chore: remove research link ({removed.get('doc_type', 'link')}) for {sec_code} [skip ci]"
        put = _put(api_url, headers, data, sha, msg)
        if put.status_code in (200, 201):
            return True, "Removed."
        if put.status_code == 409 and attempt == 0:
            continue
        return False, f"GitHub write error: HTTP {put.status_code} — {put.text[:200]}"

    return False, "GitHub write failed after retry (concurrent edit)."
