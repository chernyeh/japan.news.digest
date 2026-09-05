"""
fx_store.py — the reviewed FX assumptions and sensitivities, GitHub-backed.

Same Contents API plumbing as research_links.py: read the sha, PUT, retry once
on a 409 when another session wrote underneath. Shape:

    {"<code>": {"<fiscal_year>": {"assumptions": [...], "sensitivities": [...],
                                  "notes": str, "saved_at": iso,
                                  "source": {"name", "url"}}}}

Keyed by fiscal year because the assumption is revised during the year, and a
guidance figure has to be read against the rate that guidance actually assumed
-- not against the latest one.

Also holds the one derived number this data exists to produce: how much of an
FX move the company's standing guidance has not yet recognised. That is a flag
to investigate, never a revised forecast, and fx_gap() returns the caveats
alongside the number so the caller cannot present it as one by accident.
"""

import base64
import json
from datetime import datetime, timezone

FX_ASSUMPTIONS_PATH = "data/fx_assumptions.json"
_UA = "japan-news-digest-fx"


def load_from_github(repo: str, token: str = "") -> dict:
    """{code: {fy: entry}} from the store, or {} if it is not there yet."""
    import requests
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    url = f"https://raw.githubusercontent.com/{repo}/main/{FX_ASSUMPTIONS_PATH}"
    try:
        r = requests.get(url, headers=headers, timeout=15)
    except Exception as exc:
        print(f"FX assumptions fetch error: {exc}")
        return {}
    if r.status_code == 404:
        return {}
    if r.status_code != 200:
        print(f"FX assumptions fetch error: {r.status_code}")
        return {}
    try:
        data = r.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _get_current(api_url: str, headers: dict):
    import requests
    r = requests.get(api_url, headers=headers, timeout=15)
    if r.status_code == 404:
        return None, {}
    if r.status_code != 200:
        raise RuntimeError(f"GitHub read error: HTTP {r.status_code}")
    payload = r.json()
    try:
        data = json.loads(base64.b64decode(payload.get("content", "")).decode("utf-8"))
    except Exception:
        data = {}
    return payload.get("sha"), data


def _put(api_url: str, headers: dict, data: dict, sha, message: str):
    import requests
    content = base64.b64encode(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    ).decode("ascii")
    body = {"message": message, "content": content, "branch": "main"}
    if sha:
        body["sha"] = sha
    return requests.put(api_url, headers=headers, json=body, timeout=15)


def save_entry(repo: str, token: str, code: str, fiscal_year: str,
               entry: dict) -> tuple:
    """Write one company-year's reviewed figures. Returns (ok, message)."""
    if not token:
        return False, ("No GITHUB_TOKEN configured — kept for this session only.")
    if not code or not fiscal_year:
        return False, "A company code and fiscal year are both required."

    api_url = f"https://api.github.com/repos/{repo}/contents/{FX_ASSUMPTIONS_PATH}"
    headers = {"User-Agent": _UA, "Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}
    payload = dict(entry or {})
    payload["saved_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for attempt in range(2):
        try:
            sha, data = _get_current(api_url, headers)
        except RuntimeError as exc:
            return False, str(exc)
        data.setdefault(code, {})[fiscal_year] = payload
        msg = f"chore: FX assumptions for {code} {fiscal_year} [skip ci]"
        put = _put(api_url, headers, data, sha, msg)
        if put.status_code in (200, 201):
            return True, "Saved."
        if put.status_code == 409 and attempt == 0:
            continue
        return False, f"GitHub write error: HTTP {put.status_code} — {put.text[:200]}"
    return False, "GitHub write failed after retry (concurrent edit)."


def fx_gap(entry: dict, spot: dict, pair: str = "USD/JPY") -> dict:
    """How much of an FX move the company's standing guidance has not priced.

    assumed rate vs spot, times the company's own disclosed sensitivity. Returns
    {} unless both halves are present -- an assumption without a sensitivity
    cannot produce a number, and inventing one is exactly the failure this whole
    feature is built to avoid.

    The caveats travel with the figure rather than sitting in a comment,
    because whoever renders this has to show them:

      * most exporters hedge months forward, so the near-term cash effect is
        smaller than this and sometimes zero;
      * a disclosed sensitivity is usually translation-only, and says so only
        when `scope` is not "unstated";
      * it is a local derivative -- it does not hold across a large move.

    So this is a flag to investigate, never a revised forecast.
    """
    if not entry or not spot:
        return {}
    assumed = next((a.get("rate") for a in (entry.get("assumptions") or [])
                    if a.get("pair") == pair and a.get("rate")), None)
    sens = next((s for s in (entry.get("sensitivities") or [])
                 if s.get("pair") == pair and s.get("op_impact_jpy_per_1yen")), None)
    live = spot.get(pair)
    if not (assumed and live and sens):
        return {}
    move = live - assumed
    impact = move * sens["op_impact_jpy_per_1yen"]
    return {
        "pair": pair,
        "assumed": assumed,
        "spot": live,
        "move_yen": move,
        "impact_jpy": impact,
        "scope": sens.get("scope", "unstated"),
        "sensitivity_per_yen": sens["op_impact_jpy_per_1yen"],
        # Named so a renderer has to acknowledge them.
        "caveats": [
            "hedging is not reflected — most exporters hedge months forward, so "
            "the near-term effect is smaller and can be nil",
            ("the company does not say what this sensitivity covers"
             if sens.get("scope", "unstated") == "unstated"
             else f"the company states this covers {sens.get('scope')} exposure only"),
            "a sensitivity is a local derivative and does not hold across a "
            "large move",
        ],
        "verdict": "flag to investigate, not a revised forecast",
    }
