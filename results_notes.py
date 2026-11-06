"""
results_notes.py — your own note on each result, per company, in
data/results_notes.json — one free-text box covering both what you made of
the numbers and the forward-looking points (guidance, outlook, what
management said about next quarter, what to watch).

One note per result — keyed by company, fiscal year and period (1Q, 2Q, 3Q,
FY) — so the next quarter's note never overwrites this one's, and the list
reads back as a diary of how your view moved result by result.

Same per-company JSON shape and GitHub commit path as capital_events.py and
one_offs.py.
"""

import capital_events as CE

NOTES_PATH = "data/results_notes.json"
PERIODS = ("1Q", "2Q", "3Q", "FY")


def note_id(code: str, fy: str, period: str) -> str:
    return f"{code}-{fy}-{period}"


def load_from_github(repo: str, token: str = None) -> dict:
    return CE.load_from_github(repo, token, NOTES_PATH)


def save_via_github(repo, token, code, upsert=(), remove_ids=()):
    return CE.save_via_github(repo, token, code, upsert, remove_ids,
                              path=NOTES_PATH, what="results notes")


def apply(data: dict, code: str, upsert=(), remove_ids=()) -> dict:
    return CE.apply(data, code, upsert, remove_ids)


def make_entry(code: str, fy: str, period: str, text: str,
               date: str, updated: str) -> dict:
    """A note, or None when the box is empty (an empty note is a delete)."""
    text = (text or "").strip()
    if not text:
        return None
    return {"id": note_id(code, fy, period), "fy": fy, "period": period,
            "date": date or updated, "text": text, "updated": updated}


def text_of(entry: dict) -> str:
    return ((entry or {}).get("text") or "").strip()


def find(entries: list, fy: str, period: str):
    return next((e for e in entries or () if e.get("fy") == fy
                 and e.get("period") == period), None)


def label(fy: str, period: str) -> str:
    """("FY2027", "3Q") -> "3Q FY2027"; the full year reads "Full year FY2027"."""
    return f"Full year {fy}" if period == "FY" else f"{period} {fy}"


def parse_label(text: str) -> tuple:
    """"3Q FY2027" -> ("FY2027", "3Q"); "Full year FY2027" -> ("FY2027", "FY")."""
    text = str(text or "").strip()
    if text.startswith("Full year "):
        fy = text[len("Full year "):].strip()
        return (fy, "FY") if fy.startswith("FY") else ("", "")
    parts = text.split()
    if len(parts) != 2 or parts[0] not in PERIODS[:3]:
        return "", ""
    return parts[1], parts[0]


def options(live_fy: str, next_period: str = "", n_years: int = 2) -> list:
    """Result labels to offer, newest first: every period of the year still
    running and of the year before it."""
    try:
        y = int(str(live_fy)[2:])
    except ValueError:
        return []
    out = []
    for fy in (f"FY{y - i}" for i in range(n_years)):
        for p in reversed(PERIODS):
            out.append(label(fy, p))
    return out


def preview(entry: dict, n: int = 90) -> str:
    text = " ".join(text_of(entry).split())
    return text if len(text) <= n else text[: n - 1].rstrip() + "…"
