"""
qa_extract.py — what management was asked, and what it did not answer.

The most informative moment in a Japanese earnings call is often the one where
a question is put twice and answered neither time. That is not in any feed, and
it is not in the prepared remarks; it is in the Q&A, and it only shows up if you
read the exchange rather than the summary of it.

ir_scanner already classifies Transcript and Q&A documents (質疑応答, 想定問答)
and research_links stores them per company, so the source is there. The PDF
plumbing is fx_extract's -- same base64 document block, same validation, same
two-attempt SDK ladder -- imported rather than copied.

**Only findings are stored, never the transcript.** A transcript is megabytes of
text with no analytical value once the exchange has been classified, and
committing them would be the one change in this repo capable of adding hundreds
of MB. data/qa_findings.json holds the question, the classification, and the
quoted span that justifies it.

The honest limit, which the UI states rather than hiding: many Japanese issuers
publish a 質疑応答要旨 -- a *summary* of the Q&A written by the company -- not a
verbatim transcript. A summary has already removed the evasion. So a thin result
on a summary document is a fact about the disclosure, not a failure of the read,
and the schema captures which kind of document it was looking at.
"""

import json
import re

from fx_extract import (MAX_PDF_BYTES, validate_pdf, page_count, _b64,
                        _loads_lenient, _usd, INPUT_USD_PER_MTOK,
                        OUTPUT_USD_PER_MTOK)

# Same reasoning as fx_extract.MODEL. Judging whether an answer actually
# addressed the question is a harder call than reading a number off a table,
# and the failure mode is worse: a false "evaded" is an accusation about named
# executives, made from their own words.
MODEL = "claude-opus-5"
_REPLY_TOKENS = 2500          # several exchanges with quoted spans

ANSWER_KINDS = ("answered", "partial", "deflected", "declined", "unanswered")

_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["document_kind", "event_label", "exchanges", "unreadable"],
    "properties": {
        "document_kind": {
            "type": "string",
            "enum": ["verbatim_transcript", "company_summary", "unclear"],
            "description": "A verbatim transcript reproduces the exchange as "
                           "spoken. A 質疑応答要旨 or similar is the company's own "
                           "summary, which has usually already smoothed away any "
                           "evasion — say so, because it changes what an empty "
                           "result means.",
        },
        "event_label": {
            "type": "string",
            "description": "The event as the document labels it, e.g. "
                           "'FY2026 Q1 results briefing'. Empty if not stated.",
        },
        "exchanges": {
            "type": "array",
            "description": "One entry per question asked. Include questions that "
                           "were answered directly as well — the ratio is the point, "
                           "and a file containing only evasions cannot be read "
                           "against anything.",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["question", "topic", "answered_by", "classification",
                             "evidence_quote", "why", "page_number"],
                "properties": {
                    "question": {"type": "string",
                                 "description": "The question, condensed to one "
                                                "sentence, in English."},
                    "topic": {"type": "string",
                              "description": "Two or three words: 'margin guidance', "
                                             "'China demand', 'buyback timing'."},
                    "answered_by": {"type": "string",
                                    "description": "Name and role of whoever "
                                                   "answered, as the document gives "
                                                   "them. Empty if not attributed."},
                    "classification": {
                        "type": "string",
                        "enum": list(ANSWER_KINDS),
                        "description": "answered: the question was addressed. "
                                       "partial: part addressed, part left. "
                                       "deflected: answered a different question, "
                                       "or answered in generalities where a "
                                       "specific was asked. declined: explicitly "
                                       "refused, e.g. 'we do not disclose that'. "
                                       "unanswered: the answer never engaged the "
                                       "question at all.",
                    },
                    "evidence_quote": {
                        "type": "string",
                        "description": "The span of the answer that justifies the "
                                       "classification, quoted exactly, in the "
                                       "document's own language. Required for every "
                                       "entry — a classification with nothing "
                                       "behind it is an opinion.",
                    },
                    "why": {"type": "string",
                            "description": "One sentence on why this classification "
                                           "and not 'answered'. For 'answered', say "
                                           "what was given."},
                    "page_number": {"type": "integer"},
                },
            },
        },
        "unreadable": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["page_number", "what", "why"],
                "properties": {
                    "page_number": {"type": "integer"},
                    "what": {"type": "string"},
                    "why": {"type": "string"},
                },
            },
        },
    },
}

_PROMPT = """You are reading the question-and-answer section of a Japanese \
company's earnings briefing{who}. Your job is to record what was asked and \
whether the answer actually addressed it.

First decide what kind of document this is. A verbatim transcript reproduces
the exchange as spoken. A 質疑応答要旨 or 主な質疑応答 is the company's own
written summary of the Q&A — it is usually already smoothed, and evasion will
mostly have been edited out of it. Say which you are looking at, because it
changes how an absence of findings should be read.

Then, for every question asked:

- Condense the question to one sentence.
- Classify the answer as one of: answered, partial, deflected, declined,
  unanswered. Definitions are in the schema. **Include the questions that were
  answered properly.** A file containing only the evasions cannot be read
  against anything, and the ratio is what makes any of this meaningful.
- Quote the exact span of the answer that justifies your classification, in the
  document's own language. Every entry needs one. A classification with nothing
  behind it is an opinion, and this is about named executives in their own
  words.

Be conservative. "Deflected" means the answer addressed a different question, or
gave generalities where a specific number was asked for. It does not mean the
answer was short, or that you disagree with it, or that the executive gave a
reason you find unconvincing. An explicit "we do not disclose that" is
`declined`, not `deflected` — declining to answer openly is a different act
from appearing to answer while not doing so.

If you cannot read part of the document, put it in `unreadable` rather than
guessing at what was said.

Return only the JSON object the schema describes."""


def estimate_cost(data: bytes, api_key: str = "", model: str = MODEL) -> dict:
    """{"input_tokens", "output_tokens", "usd", "measured", "pages"}."""
    pages = page_count(data)
    if api_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            counted = client.messages.count_tokens(
                model=model,
                messages=[{"role": "user", "content": _content_blocks(data, "", "")}])
            n_in = int(getattr(counted, "input_tokens", 0) or 0)
            if n_in:
                return {"input_tokens": n_in, "output_tokens": _REPLY_TOKENS,
                        "usd": _usd(n_in, _REPLY_TOKENS), "measured": True,
                        "pages": pages}
        except Exception:
            pass
    # A transcript is denser text than a slide deck, so the per-page estimate
    # is higher than fx_extract's.
    n_in = max(pages, 1) * 3000
    return {"input_tokens": n_in, "output_tokens": _REPLY_TOKENS,
            "usd": _usd(n_in, _REPLY_TOKENS), "measured": False, "pages": pages}


def _content_blocks(data: bytes, code: str, name: str) -> list:
    who = ""
    if name or code:
        who = f" for {name} {code}".replace("  ", " ").rstrip()
    return [
        {"type": "document",
         "source": {"type": "base64", "media_type": "application/pdf",
                    "data": _b64(data)}},
        {"type": "text", "text": _PROMPT.format(who=who)},
    ]


def extract_from_pdf(data: bytes, api_key: str, code: str = "", name: str = "",
                     model: str = MODEL) -> dict:
    """The normalised findings, or raises."""
    import anthropic

    ok, problems = validate_pdf(name, data)
    if not ok:
        raise ValueError("; ".join(problems))

    client = anthropic.Anthropic(api_key=api_key)
    messages = [{"role": "user", "content": _content_blocks(data, code, name)}]
    attempts = (
        {"output_config": {"format": {"type": "json_schema", "schema": _SCHEMA}},
         "thinking": {"type": "adaptive"}},
        {},
    )
    last_exc = None
    for extra in attempts:
        try:
            resp = client.messages.create(model=model, max_tokens=16000,
                                          messages=messages, **extra)
            break
        except TypeError as exc:
            last_exc = exc
        except anthropic.BadRequestError as exc:
            last_exc = exc
    else:
        raise RuntimeError(f"Could not call the model: {last_exc}")

    if getattr(resp, "stop_reason", "") == "refusal":
        raise RuntimeError("The model declined to read this document.")
    text = next((b.text for b in resp.content if getattr(b, "type", "") == "text"), "")
    return normalise(_loads_lenient(text))


def normalise(data: dict) -> dict:
    data = data if isinstance(data, dict) else {}
    kind = str(data.get("document_kind") or "unclear")
    out = {"document_kind": kind if kind in ("verbatim_transcript",
                                             "company_summary", "unclear") else "unclear",
           "event_label": str(data.get("event_label") or "").strip(),
           "exchanges": [], "unreadable": []}
    for e in (data.get("exchanges") or []):
        q = str(e.get("question") or "").strip()
        quote = str(e.get("evidence_quote") or "").strip()
        cls = str(e.get("classification") or "").lower()
        # A classification with no question or no quote behind it is dropped,
        # not shown. This is the rule the whole module rests on.
        if not q or not quote or cls not in ANSWER_KINDS:
            continue
        out["exchanges"].append({
            "question": q[:400], "topic": str(e.get("topic") or "").strip()[:60],
            "answered_by": str(e.get("answered_by") or "").strip()[:80],
            "classification": cls, "evidence_quote": quote[:600],
            "why": str(e.get("why") or "").strip()[:300],
            "page_number": _int(e.get("page_number")),
        })
    for u in (data.get("unreadable") or []):
        out["unreadable"].append({"page_number": _int(u.get("page_number")),
                                  "what": str(u.get("what") or "").strip()[:200],
                                  "why": str(u.get("why") or "").strip()[:200]})
    return out


def summarise(exchanges: list) -> dict:
    """Counts and a rate, or {}. The rate is what makes one event comparable
    with the same company's last one and with its peers."""
    rows = list(exchanges or [])
    if not rows:
        return {}
    counts = {k: sum(1 for r in rows if r.get("classification") == k)
              for k in ANSWER_KINDS}
    # "declined" is deliberately not in the numerator. Saying "we do not
    # disclose that" is an honest refusal, and lumping it with evasion would
    # punish the more candid answer.
    evaded = counts["deflected"] + counts["unanswered"]
    return {
        "total": len(rows),
        "counts": counts,
        "evaded": evaded,
        "evasion_rate": evaded / len(rows),
        "topics_evaded": sorted({r.get("topic", "") for r in rows
                                 if r.get("classification") in ("deflected", "unanswered")
                                 and r.get("topic")}),
        # Below this many questions the rate is not a rate. A briefing with
        # four questions and one deflection is not "25% evasion".
        "thin": len(rows) < 8,
    }


def _int(value) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


# ── The store ────────────────────────────────────────────────────────────
# Same Contents API plumbing as fx_store, imported rather than copied.
# Shape: {"<code>": [{event_label, doc_kind, summary, exchanges, source, saved_at}]}
QA_FINDINGS_PATH = "data/qa_findings.json"
_UA = "japan-news-digest-qa"


def load_from_github(repo: str, token: str = "") -> dict:
    """{code: [event, ...]} of stored findings, or {}."""
    import requests
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    url = f"https://raw.githubusercontent.com/{repo}/main/{QA_FINDINGS_PATH}"
    try:
        r = requests.get(url, headers=headers, timeout=15)
    except Exception as exc:
        print(f"Q&A findings fetch error: {exc}")
        return {}
    if r.status_code == 404:
        return {}
    if r.status_code != 200:
        print(f"Q&A findings fetch error: {r.status_code}")
        return {}
    try:
        data = r.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def save_event(repo: str, token: str, code: str, event: dict) -> tuple:
    """Append one event's findings for `code`. Returns (ok, message).

    Only the findings go in. The transcript itself is never written: it is
    megabytes of text with no analytical value once the exchanges are
    classified, and it is the one thing in this repo that could add hundreds of
    MB to a store the app loads."""
    from datetime import datetime, timezone
    from fx_store import _get_current, _put

    if not token:
        return False, "No GITHUB_TOKEN configured — kept for this session only."
    if not code:
        return False, "A company code is required."

    api_url = f"https://api.github.com/repos/{repo}/contents/{QA_FINDINGS_PATH}"
    headers = {"User-Agent": _UA, "Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}
    payload = dict(event or {})
    payload["saved_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for attempt in range(2):
        try:
            sha, data = _get_current(api_url, headers)
        except RuntimeError as exc:
            return False, str(exc)
        events = data.setdefault(code, [])
        # Re-reading the same document replaces its entry rather than stacking
        # a second copy beside it.
        label = payload.get("event_label", "")
        events[:] = [e for e in events if e.get("event_label") != label or not label]
        events.append(payload)
        msg = f"chore: Q&A findings for {code} [skip ci]"
        put = _put(api_url, headers, data, sha, msg)
        if put.status_code in (200, 201):
            return True, "Saved."
        if put.status_code == 409 and attempt == 0:
            continue
        return False, f"GitHub write error: HTTP {put.status_code} — {put.text[:200]}"
    return False, "GitHub write failed after retry (concurrent edit)."


def across_events(events: list) -> dict:
    """A read over several briefings for one company, or {}.

    One event's evasion rate is noise. The same topic going unanswered across
    three consecutive briefings is the finding — it says where management does
    not want to be pinned down, which is usually where the risk is."""
    rows = [e for e in (events or []) if e.get("summary")]
    if not rows:
        return {}
    total = sum(e["summary"].get("total", 0) for e in rows)
    evaded = sum(e["summary"].get("evaded", 0) for e in rows)
    repeat = {}
    for e in rows:
        for topic in e["summary"].get("topics_evaded", []):
            repeat[topic] = repeat.get(topic, 0) + 1
    return {
        "events": len(rows),
        "questions": total,
        "evaded": evaded,
        "evasion_rate": (evaded / total) if total else 0.0,
        "recurring": sorted((t for t, n in repeat.items() if n >= 2),
                            key=lambda t: -repeat[t]),
        "verbatim_events": sum(1 for e in rows
                               if e.get("document_kind") == "verbatim_transcript"),
        "thin": total < 20,
    }
