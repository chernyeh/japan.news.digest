"""
fx_extract.py — read a company's FX assumptions and sensitivity off its own deck.

Japanese issuers almost always state the exchange rate their guidance assumes
(前提為替レート), and many state a sensitivity beside it (感応度): what a one-yen
move does to full-year operating profit. Neither is in any feed. Both sit in a
results presentation, often on a Japanese-language slide, sometimes in a chart
label or a footnote rather than a table.

The Messages API takes PDFs directly, so this needs no PDF library: a base64
`document` block, 32 MB and 600 pages. ir_scanner.fetch_document_bytes already
downloads a document by URL with a size cap; today its bytes only ever go into
a ZIP.

Nothing here writes to the store. It returns candidates for a review grid, for
the same reason consensus_vision.py does: the highest-value answer is often
"not disclosed", and that is exactly where a model reaches for a plausible
number instead. The schema gives it somewhere to put "I could not read this"
so that declining is as easy as guessing.

What the numbers do and do not mean, which belongs in the UI and not only here:
a disclosed sensitivity is a translation figure computed under the company's
own assumptions; most exporters hedge months forward, so it overstates the
near-term cash effect; and it is not linear across large moves. Anything
derived from it is a flag to investigate, never a revised forecast.
"""

import base64
import json
import re

MAX_PDF_BYTES = 32 * 1024 * 1024      # the API's own request ceiling
MAX_PAGES = 600
SUPPORTED_MEDIA = ("application/pdf",)

# Same reasoning as consensus_vision.MODEL, and it applies more here rather
# than less. Four properties of this task punish a weaker extractor: the source
# is often a Japanese-language slide; assumption / sensitivity / scope is a
# semantic distinction that is easy to get subtly and invisibly wrong; the most
# valuable answer is frequently "not disclosed"; and a wrong sensitivity does
# not stay put -- it propagates into an FX-adjusted guidance figure. A results
# deck is also longer than a screenshot, so there is more to get wrong.
#
# Sonnet 5 can do this and the switch is this one constant. It costs $2/$10 per
# MTok against Opus 5's $5/$25, so roughly $3-5 a quarter at thirty documents.
# Settle it by measurement rather than argument: run both over the same 8-10
# decks and compare field by field, counting how often each invents a
# sensitivity that was never disclosed and how often it files a readable figure
# under `unreadable`. Haiku 4.5 is not a candidate at all -- 200K context
# against 1M, and a long deck can exceed it.
MODEL = "claude-opus-5"
FALLBACK_MODEL = "claude-sonnet-5"

INPUT_USD_PER_MTOK = 5.00     # claude-opus-5
OUTPUT_USD_PER_MTOK = 25.00
_REPLY_TOKENS = 1200          # a filled-in grid plus the model's reasoning

# Every field required and additionalProperties false, so a partial object is a
# schema error rather than a silently missing figure. `disclosed` and
# `unreadable` are the escape hatches: they let the model say "the company did
# not publish this" and "I could not read this page" instead of inventing.
_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["fiscal_year", "assumptions", "sensitivities", "unreadable", "notes"],
    "properties": {
        "fiscal_year": {
            "type": "string",
            "description": "The fiscal year the assumptions apply to, as the "
                           "document labels it, e.g. 'FY2026' or '2026年3月期'. "
                           "Empty string if the document does not say.",
        },
        "assumptions": {
            "type": "array",
            "description": "One entry per currency pair whose assumed rate the "
                           "company states. Empty array if none is stated.",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["pair", "rate", "page_number", "source_quote"],
                "properties": {
                    "pair": {"type": "string",
                             "description": "e.g. 'USD/JPY', 'EUR/JPY', 'CNY/JPY'"},
                    "rate": {"type": "number",
                             "description": "Yen per unit of the foreign currency."},
                    "page_number": {"type": "integer",
                                    "description": "1-indexed page it was read from."},
                    "source_quote": {"type": "string",
                                     "description": "The exact text or label the "
                                                    "figure was read from, in the "
                                                    "document's own language."},
                },
            },
        },
        "sensitivities": {
            "type": "array",
            "description": "One entry per disclosed sensitivity. Empty array if the "
                           "company discloses an assumed rate but no sensitivity, "
                           "which is common and is a valid answer.",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["pair", "op_impact_jpy_per_1yen", "scope",
                             "page_number", "source_quote"],
                "properties": {
                    "pair": {"type": "string"},
                    "op_impact_jpy_per_1yen": {
                        "type": "number",
                        "description": "Change in full-year operating profit, in yen "
                                       "(not millions or billions -- convert), for a "
                                       "one-yen move. Positive means a weaker yen "
                                       "(a higher rate) increases operating profit.",
                    },
                    "scope": {
                        "type": "string",
                        "enum": ["translation", "transaction", "both", "unstated"],
                        "description": "What the company says the figure covers. "
                                       "Use 'unstated' rather than guessing -- most "
                                       "issuers do not say, and assuming "
                                       "'translation' would be putting words in "
                                       "their mouth.",
                    },
                    "page_number": {"type": "integer"},
                    "source_quote": {"type": "string"},
                },
            },
        },
        "unreadable": {
            "type": "array",
            "description": "Anything you could see was an FX figure but could not "
                           "read with confidence: a chart label, a cut-off table, an "
                           "ambiguous unit. Put it here rather than guessing at it.",
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
        "notes": {
            "type": "string",
            "description": "Anything material about how to read these figures that "
                           "the company states: a hedging policy, a mid-year "
                           "revision to the assumption, a caveat on the "
                           "sensitivity. Empty string if none.",
        },
    },
}

_PROMPT = """You are reading a Japanese company's results presentation{who} to \
find two things, and only these two things.

1. THE ASSUMED EXCHANGE RATES behind the company's own full-year guidance
   (前提為替レート / 想定為替レート / assumed rate). These are usually on an
   early guidance slide or in a footnote under the forecast table.

2. THE DISCLOSED FX SENSITIVITY (感応度 / 為替感応度 / sensitivity): how much
   full-year operating profit changes for a one-yen move in a pair. Often a
   small separate table; sometimes only a sentence; frequently absent.

Rules that matter more than completeness:

- If the company does not disclose something, return an empty array for it.
  "Not disclosed" is a correct and useful answer. Do not infer a sensitivity
  from an assumed rate, from a year-on-year profit bridge, or from anything
  else -- only report a figure the company itself states as a sensitivity.
- If you can see a figure but cannot read it confidently, put it in
  `unreadable` with the page and why. Do not guess at a digit.
- Convert every sensitivity to yen of operating profit per one-yen move.
  Decks state these in 億円 (hundred millions) or 百万円 (millions); read the
  unit label carefully, because getting the unit wrong is the single most
  damaging error you can make here.
- Sign convention: positive means a WEAKER yen (a higher JPY-per-USD rate)
  INCREASES operating profit -- the exporter direction. If the company shows
  the effect of a stronger yen, flip the sign.
- `scope` records what the company says the figure covers. If it does not say,
  use "unstated". Do not assume "translation".
- Quote the exact source text in `source_quote`, in the document's own
  language, and give the 1-indexed page. A reviewer will spot-check these.

Return only the JSON object the schema describes."""


def validate_pdf(name: str, data: bytes) -> tuple:
    """(ok, problems) for one candidate document."""
    problems = []
    if not data:
        problems.append(f"{name or 'document'}: empty file.")
    elif len(data) > MAX_PDF_BYTES:
        problems.append(f"{name or 'document'}: {len(data) / 1048576:.1f} MB, "
                        f"over the {MAX_PDF_BYTES // 1048576} MB API limit.")
    # A PDF starts with %PDF-. Checking the bytes rather than the extension
    # catches the common case of an IR link that returns an HTML error page
    # with a .pdf in the URL, which would otherwise fail deep inside the API
    # call with a confusing message.
    if data and not data[:5] == b"%PDF-":
        problems.append(f"{name or 'document'}: does not look like a PDF "
                        f"(no %PDF- header) — the link may have returned a web "
                        f"page rather than the document.")
    return (not problems), problems


def page_count(data: bytes) -> int:
    """A rough page count from the raw bytes, or 0 if it cannot be determined.

    Only used to warn before a call that would be rejected; the API is the
    authority. Counting /Type /Page occurrences over-counts on some producers
    and under-counts on object streams, so it is never treated as exact."""
    if not data:
        return 0
    return len(re.findall(rb"/Type\s*/Page[^s]", data))


def estimate_cost(data: bytes, api_key: str = "", model: str = MODEL) -> dict:
    """{"input_tokens", "output_tokens", "usd", "measured"} for one extraction.

    Asks the API to count the tokens when a key is available, because a PDF's
    token cost depends on how much text and how many rendered pages it carries
    and cannot be inferred from the byte count. Falls back to a page-based
    guess so the button can still show a figure before a key is entered."""
    pages = page_count(data)
    if api_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            counted = client.messages.count_tokens(
                model=model,
                messages=[{"role": "user", "content": _content_blocks(data, "", "")}],
            )
            n_in = int(getattr(counted, "input_tokens", 0) or 0)
            if n_in:
                return {"input_tokens": n_in, "output_tokens": _REPLY_TOKENS,
                        "usd": _usd(n_in, _REPLY_TOKENS), "measured": True,
                        "pages": pages}
        except Exception:
            pass  # fall through to the estimate rather than blocking the UI
    # ~1,800 tokens a page is a working average for a results deck: dense
    # Japanese slides with charts run higher, a sparse appendix lower.
    n_in = max(pages, 1) * 1800
    return {"input_tokens": n_in, "output_tokens": _REPLY_TOKENS,
            "usd": _usd(n_in, _REPLY_TOKENS), "measured": False, "pages": pages}


def _usd(n_in: int, n_out: int) -> float:
    return (n_in / 1e6) * INPUT_USD_PER_MTOK + (n_out / 1e6) * OUTPUT_USD_PER_MTOK


def _b64(data: bytes) -> str:
    return base64.standard_b64encode(data).decode("ascii")


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


def _loads_lenient(text: str) -> dict:
    """Parse the model's JSON, tolerating a fence or surrounding prose. Only
    reached on the fallback path, where the reply was not schema-bound."""
    text = (text or "").strip()
    fence = re.search(r"```(?:json)?\s*(.+?)```", text, re.S)
    if fence:
        text = fence.group(1).strip()
    try:
        return json.loads(text)
    except ValueError:
        start, end = text.find("{"), text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start:end + 1])
    raise ValueError("The model's reply was not JSON.")


def extract_from_pdf(data: bytes, api_key: str, code: str = "", name: str = "",
                     model: str = MODEL) -> dict:
    """The normalised extraction, or raises.

    Raises rather than returning an empty result on failure: an empty grid reads
    as "the deck discloses nothing", which is a finding, and it must not be
    confused with "the call failed"."""
    import anthropic

    ok, problems = validate_pdf(name, data)
    if not ok:
        raise ValueError("; ".join(problems))

    client = anthropic.Anthropic(api_key=api_key)
    messages = [{"role": "user", "content": _content_blocks(data, code, name)}]

    # Structured output binds the reply to the schema. requirements.txt pins a
    # floor old enough that a deployment could still be running an SDK without
    # output_config or adaptive thinking, so fall back to a plain call and a
    # lenient parse rather than failing outright on a stale install. Note that
    # API-verified citations cannot be added here: citations and
    # output_config.format together return a 400, so page_number and
    # source_quote are model-reported provenance for a reviewer to spot-check,
    # exactly as consensus_vision treats from_image.
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
        except TypeError as exc:                    # kwarg unknown to this SDK
            last_exc = exc
        except anthropic.BadRequestError as exc:    # param unknown to the API
            last_exc = exc
    else:
        raise RuntimeError(f"Could not call the model: {last_exc}")

    if getattr(resp, "stop_reason", "") == "refusal":
        raise RuntimeError("The model declined to read this document.")
    text = next((b.text for b in resp.content if getattr(b, "type", "") == "text"), "")
    return normalise(_loads_lenient(text))


def normalise(data: dict) -> dict:
    """Coerce the reply into the shape the review grid expects, dropping
    entries that carry no usable number rather than showing a blank row."""
    data = data if isinstance(data, dict) else {}
    out = {"fiscal_year": str(data.get("fiscal_year") or "").strip(),
           "assumptions": [], "sensitivities": [], "unreadable": [],
           "notes": str(data.get("notes") or "").strip()}
    for a in (data.get("assumptions") or []):
        rate = _num(a.get("rate"))
        if rate is None:
            continue
        out["assumptions"].append({
            "pair": _pair(a.get("pair")), "rate": rate,
            "page_number": int(_num(a.get("page_number")) or 0),
            "source_quote": str(a.get("source_quote") or "").strip()[:300],
        })
    for s in (data.get("sensitivities") or []):
        impact = _num(s.get("op_impact_jpy_per_1yen"))
        if impact is None:
            continue
        scope = str(s.get("scope") or "unstated").lower()
        out["sensitivities"].append({
            "pair": _pair(s.get("pair")), "op_impact_jpy_per_1yen": impact,
            "scope": scope if scope in ("translation", "transaction", "both",
                                        "unstated") else "unstated",
            "page_number": int(_num(s.get("page_number")) or 0),
            "source_quote": str(s.get("source_quote") or "").strip()[:300],
        })
    for u in (data.get("unreadable") or []):
        out["unreadable"].append({
            "page_number": int(_num(u.get("page_number")) or 0),
            "what": str(u.get("what") or "").strip()[:200],
            "why": str(u.get("why") or "").strip()[:200],
        })
    return out


def _pair(value) -> str:
    """Normalise a currency pair to USD/JPY form. The decks write it every way
    there is -- USDJPY, $/¥, ドル円 -- and the store is keyed on it."""
    text = str(value or "").upper().strip()
    text = text.replace("／", "/").replace("＝", "/")
    for a, b in (("ドル円", "USD/JPY"), ("ユーロ円", "EUR/JPY"), ("人民元円", "CNY/JPY"),
                 ("$/¥", "USD/JPY"), ("€/¥", "EUR/JPY")):
        if a.upper() in text:
            return b
    compact = re.sub(r"[^A-Z]", "", text)
    if len(compact) == 6:
        return f"{compact[:3]}/{compact[3:]}"
    # Not "USD/JPY". Defaulting an unlabelled figure to the commonest pair
    # would silently file a euro sensitivity under the dollar, and the review
    # grid would show a plausible-looking row with no way to tell.
    return text or "UNKNOWN"


def _num(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").replace(",", "").strip()
    try:
        return float(text)
    except ValueError:
        return None
