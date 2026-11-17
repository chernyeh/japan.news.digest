"""
flag_dismissals.py — Decisions-waiting flags you have cleared, and the log of
them, in data/flag_dismissals.json.

A cleared flag is hidden from Decisions waiting. Nothing is ever deleted:
restoring a flag stamps `restored` on its entry instead of removing it, so
the file stays the full record of what was flagged, when it was cleared, and
whether it was brought back.

A flag is matched by its flag id = code | kind | key. The key names the particular
occurrence (an exit price, a result date, a review date — see
watchlist_grid.decision_flags), so a cleared flag comes back by itself when
the thing it is about changes. Consensus flags over a rolling window carry
`clear_days`: clearing them holds for that many days, after which a move
still in place is flagged again.

Same per-company JSON shape and GitHub commit path as capital_events.py.
"""

from datetime import date, timedelta

import capital_events as CE

PATH = "data/flag_dismissals.json"


def flag_id(code: str, flag: dict) -> str:
    return f"{code}|{flag.get('kind', '')}|{flag.get('key', '')}"


def make_entry(code: str, name: str, flag: dict, today: date, note: str = "") -> dict:
    fid = flag_id(code, flag)
    # One entry per clearing (flag id @ date), so clearing a flag again after
    # restoring it adds to the log instead of overwriting it.
    e = {"id": f"{fid}@{today.isoformat()}", "flag": fid, "date": today.isoformat(),
         "code": code,
         "name": name or code, "kind": flag.get("kind", ""), "short": flag.get("short", ""),
         "severity": flag.get("severity", ""), "text": flag.get("text", "")}
    if flag.get("clear_days"):
        e["until"] = (today + timedelta(days=int(flag["clear_days"]))).isoformat()
    if (note or "").strip():
        e["note"] = note.strip()
    return e


def restored_entry(entry: dict, today: date) -> dict:
    return {**entry, "restored": today.isoformat()}


def active(entry: dict, today: date) -> bool:
    """Is this clearing still hiding its flag?"""
    if entry.get("restored"):
        return False
    until = entry.get("until")
    return not until or until >= today.isoformat()


def hidden_ids(data: dict, today: date) -> set:
    return {e.get("flag") or e.get("id") for es in (data or {}).values() for e in es
            if e.get("id") and active(e, today)}


def split(code: str, flags: list, hidden: set) -> tuple:
    """(shown, cleared) for one company's flags."""
    shown, cleared = [], []
    for f in flags or ():
        (cleared if flag_id(code, f) in hidden else shown).append(f)
    return shown, cleared


def log(data: dict) -> list:
    """Every entry across companies, newest clearing first."""
    out = [e for es in (data or {}).values() for e in es]
    return sorted(out, key=lambda e: (e.get("date", ""), e.get("id", "")), reverse=True)


def load_from_github(repo: str, token: str = None) -> dict:
    return CE.load_from_github(repo, token, PATH)


def save_via_github(repo, token, code, upsert=()):
    return CE.save_via_github(repo, token, code, upsert, (), path=PATH,
                              what="cleared flags")


def apply(data: dict, code: str, upsert=()) -> dict:
    return CE.apply(data, code, upsert, ())
