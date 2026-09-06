"""
gh_read.py — one way to read a file out of the repo, and one way to fail.

Every GitHub-backed store in this app fetched raw.githubusercontent.com itself,
and each one treated a 404 as "the file has not been created yet" — which is the
right reading, except for one case that is invisible and takes the whole app
down with it.

**An invalid Authorization header makes GitHub return 404, not 401**, even for a
file in a public repo that is plainly there. So a GITHUB_TOKEN that has expired,
been revoked, or was pasted with a typo turns every data file in the repo into
"does not exist": the forecast table disappears, the filing indexes report
themselves empty, market caps vanish, and the app helpfully suggests running a
backfill action that would not fix anything. Nothing is logged, because 404 is
the one status the loaders were built to expect.

So: on a 404 *while sending a token*, retry once without it. A public file comes
back, the app carries on working, and the bad token is recorded once in
TOKEN_STATE so the UI can say what is actually wrong. A file that 404s both ways
really is missing.
"""

import csv

_UA = "japan-news-digest"

# Set by the first read that proves the token is bad, so the UI can say so
# rather than leaving the reader to infer it from six empty panels.
TOKEN_STATE = {"bad": False, "checked": False}


def raw_url(repo: str, path: str) -> str:
    return f"https://raw.githubusercontent.com/{repo}/main/{path}"


def raw_get(repo: str, path: str, token: str = None, timeout: int = 15):
    """(status, text) for a file in the repo, transparently working around a
    bad token. status 404 means genuinely absent."""
    import requests
    url = raw_url(repo, path)
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
    except Exception as exc:
        print(f"{path} fetch error: {exc}")
        return 0, ""
    if r.status_code == 200:
        if token:
            TOKEN_STATE["checked"] = True
        return 200, r.text
    # The case this module exists for. Anonymous reads work on a public repo,
    # so if dropping the token turns a 404 into a 200, the token is the problem.
    if r.status_code == 404 and token:
        try:
            r2 = requests.get(url, headers={"User-Agent": _UA}, timeout=timeout)
        except Exception:
            return r.status_code, ""
        TOKEN_STATE["checked"] = True
        if r2.status_code == 200:
            if not TOKEN_STATE["bad"]:
                print("::warning::GITHUB_TOKEN is rejected by GitHub — it is "
                      "expired, revoked, or mistyped. Reading public files "
                      "anonymously instead. Writes (watchlist, saved links, "
                      "manual overrides) will not work until it is replaced.")
            TOKEN_STATE["bad"] = True
            return 200, r2.text
        return r2.status_code, ""
    if r.status_code != 404:
        print(f"{path} fetch error: {r.status_code}")
    return r.status_code, ""


def raw_csv(repo: str, path: str, token: str = None) -> list:
    """The file as a list of dict rows, or [] if it is genuinely not there."""
    status, text = raw_get(repo, path, token)
    if status != 200 or not text:
        return []
    return list(csv.DictReader(text.splitlines()))


def raw_json(repo: str, path: str, token: str = None, default=None):
    """The file parsed as JSON, or `default` if absent or unparseable."""
    import json
    status, text = raw_get(repo, path, token)
    if status != 200 or not text:
        return {} if default is None else default
    try:
        return json.loads(text)
    except ValueError:
        return {} if default is None else default


def token_warning() -> str:
    """One line for the UI, or "" while the token looks fine."""
    if not TOKEN_STATE.get("bad"):
        return ""
    return ("GITHUB_TOKEN is being rejected by GitHub — expired, revoked, or "
            "mistyped. Data is still being read (the repo is public), but "
            "anything that saves — the watchlist, saved links, manual "
            "overrides — will not persist until it is replaced in Streamlit "
            "Secrets.")
