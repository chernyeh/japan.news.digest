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


def token_kind(token: str) -> str:
    """"fine-grained", "classic", or "" — read off the prefix GitHub puts on
    every token it issues (github_pat_ / ghp_). A refused write is fixed on a
    different settings page for each kind, so the message has to know which."""
    token = (token or "").strip()
    if token.startswith("github_pat_"):
        return "fine-grained"
    if token.startswith("ghp_"):
        return "classic"
    return ""


def _header(headers, name: str):
    """A response header by name, case-insensitively, or None if absent.
    requests hands back a case-insensitive dict; a plain dict (tests, or a
    caller that copied the headers) is not, so match by hand."""
    if not headers:
        return None
    for k, v in dict(headers).items():
        if k.lower() == name.lower():
            return v
    return None


_TOKEN_UNCHANGED = (' If GitHub shows you a new token value after saving, paste that '
                    'into GITHUB_TOKEN under the app\'s Manage app → Settings → Secrets '
                    'on Streamlit Community Cloud; if not, nothing else needs changing '
                    '— try the save again.')


def write_error(status_code: int, response_text: str = "", token: str = "",
                headers=None, repo: str = "") -> str:
    """A plain-English reason a GitHub Contents API write (PUT/read-before-PUT)
    failed, for a UI whose only other option is dumping GitHub's raw JSON error
    body at the reader. 403 specifically means the token authenticates fine but
    was not granted write access — a different, more common problem than the
    expired/revoked-token 404 this module otherwise exists for, and one that
    the raw "Resource not accessible by personal access token" text does not
    explain on its own.

    Given the token and the response headers, a 403 says which of its causes
    this one is. A generic "it needs write access" was not enough to fix it:
    the most common cause is a fine-grained token created with Repository
    access left on "Public repositories", which *looks* like it covers a
    public repo but is read-only by design, and nothing in GitHub's reply
    says so. A classic token's reply does list the scopes it carries
    (X-OAuth-Scopes), so that case can be stated exactly."""
    if status_code == 403:
        # GitHub also answers 403 for a secondary rate limit, which no change
        # to the token would fix.
        if "rate limit" in (response_text or "").lower():
            return ("GitHub is rate-limiting this token for the moment — its "
                    "permissions are fine. Wait a minute and try again.")
        where = repo or "this repo"
        kind = token_kind(token)
        if kind == "fine-grained":
            return ('GITHUB_TOKEN does not have permission to save here. It is a '
                    'fine-grained token: GitHub accepted it but refused the write. '
                    'The usual cause is "Repository access" left on "Public '
                    'repositories", which is read-only even for your own public '
                    'repo, or "Contents" not set to "Read and write". To fix it on '
                    'GitHub: Settings → Developer settings → Personal access tokens '
                    '→ Fine-grained tokens → click this token → Edit. Under '
                    f'Repository access choose "Only select repositories" and pick '
                    f'{where}; under Permissions → Repository permissions set '
                    '"Contents" to "Read and write"; then save.' + _TOKEN_UNCHANGED)
        if kind == "classic":
            scopes = _header(headers, "X-OAuth-Scopes")
            granted = {s.strip() for s in (scopes or "").split(",") if s.strip()}
            if granted & {"repo", "public_repo"}:
                # The scope is there, so the refusal is about the account the
                # token belongs to, not the token.
                return ('GITHUB_TOKEN has the "repo" scope but GitHub still refused '
                        'the write, so the GitHub account that created the token '
                        f'does not have write access to {where}. Create the token '
                        'while signed in as the account that owns the repo, then '
                        'replace GITHUB_TOKEN under the app\'s Manage app → '
                        'Settings → Secrets on Streamlit Community Cloud.')
            if scopes is None:
                has = ""
            elif granted:
                has = f' It carries only these scopes: {", ".join(sorted(granted))}.'
            else:
                has = " It carries no scopes at all."
            return ('GITHUB_TOKEN does not have permission to save here. It is a '
                    f'classic token without the "repo" scope.{has} To fix it on '
                    'GitHub: Settings → Developer settings → Personal access tokens '
                    '→ Tokens (classic) → click this token, tick "repo", then '
                    '"Update token".' + _TOKEN_UNCHANGED)
        return ('GITHUB_TOKEN does not have permission to save here — GitHub '
                'accepted it but rejected the write because it lacks access '
                'to this repo. It needs write access: a fine-grained personal '
                'access token scoped to this repo with "Contents: Read and '
                'write" permission, or a classic token with the "repo" '
                'scope. Replace GITHUB_TOKEN under the app\'s Manage app → '
                'Settings → Secrets on Streamlit Community Cloud, then '
                'reboot the app.')
    if status_code == 401:
        return ('GITHUB_TOKEN was rejected as invalid (expired, revoked, or '
                'mistyped). Generate a new one and replace it under the '
                'app\'s Manage app → Settings → Secrets on Streamlit '
                'Community Cloud, then reboot the app.')
    if status_code == 404:
        return "GitHub could not find that repo or file to write to (HTTP 404) — check the repo name."
    return (f"GitHub write error: HTTP {status_code}"
            + (f" — {response_text[:200]}" if response_text else ""))
