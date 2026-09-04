"""
consensus_vision.py — read consensus figures off screenshots.

Koyfin and the other terminals cannot be scraped: there is no API, and
estimates are the one data class their vendor licence bars from export. What
you *can* do is look at your own screen. So: attach one or more captures and
Claude reads the numbers out of them.

All the images go into a **single** request rather than one call each. A
figure's meaning is regularly split across captures — the fiscal-year header
in one, the row in another, the units in a footnote in a third — and a model
that sees them together can join them, where separate calls would each return
a fragment that then has to be reconciled by guesswork.

Nothing here writes to the store. It returns candidates for the user to
confirm in a review grid, because a misread digit in an EPS figure is not the
kind of error that announces itself later.
"""

import base64
import json
import re

# Vision-capable formats the Messages API accepts.
SUPPORTED_MEDIA = ("image/png", "image/jpeg", "image/gif", "image/webp")
MAX_IMAGES = 8
MAX_IMAGE_BYTES = 5 * 1024 * 1024

# Deliberately not the claude-haiku-4-5 used elsewhere in this app for news
# summarisation. Transcribing financial figures is the one task here where a
# single misread digit silently corrupts the output, and the cost difference
# on a handful of screenshots is irrelevant next to that.
MODEL = "claude-opus-5"

# ── What a screenshot costs to read ──────────────────────────────────────
# Claude bills an image by area, not by file size: it is cut into 28×28-pixel
# patches and each patch is one visual token, so an image costs
# ceil(w/28) × ceil(h/28) input tokens. Claude 4.7 and later read at high
# resolution — a longer edge and roughly three times the patch budget of
# earlier models — and anything over either ceiling is downscaled first,
# preserving aspect ratio, which caps what a single image can ever cost.
#
# Rates and limits: platform.claude.com/docs/en/build-with-claude/vision
# and claude.com/pricing. Both change; they are constants here so a stale
# figure is visible in one place rather than buried in an f-string.
PATCH_PX = 28
MAX_LONG_EDGE = 2576          # high-resolution tier (Claude 4.7 and later)
MAX_VISUAL_TOKENS = 4784
INPUT_USD_PER_MTOK = 5.00     # claude-opus-5
OUTPUT_USD_PER_MTOK = 25.00

# The instructions above travel with every request, and a filled-in review grid
# comes back as JSON preceded by the model's reasoning, which bills as output.
# Both are small next to the images but not zero, and quoting an image-only
# figure would understate the bill. The reply figure is a rough average — it
# scales with how many cells are actually readable — so the estimate is
# presented as "about", and the visual-token count beside it is exact.
_PROMPT_TOKENS = 700
_REPLY_TOKENS = 1500


def image_tokens(width: int, height: int) -> int:
    """Visual tokens one image costs, after the downscaling the API applies to
    anything past the resolution ceiling."""
    import math
    if not width or not height:
        return 0
    scale = min(1.0, MAX_LONG_EDGE / max(width, height))
    w, h = width * scale, height * scale
    tokens = math.ceil(w / PATCH_PX) * math.ceil(h / PATCH_PX)
    if tokens > MAX_VISUAL_TOKENS:
        # Area-match the budget, then step down until the patch grid actually
        # fits: rounding up to whole patches on both edges can put a
        # perfectly area-sized image back over the ceiling.
        scale *= math.sqrt(MAX_VISUAL_TOKENS * PATCH_PX ** 2 / (w * h))
        for _ in range(24):
            w, h = width * scale, height * scale
            tokens = math.ceil(w / PATCH_PX) * math.ceil(h / PATCH_PX)
            if tokens <= MAX_VISUAL_TOKENS:
                break
            scale *= 0.995
    return tokens


def image_size(data: bytes):
    """(width, height) read from the file header. Pillow arrives with
    Streamlit, but this is not worth failing a parse over, so an unreadable
    header returns (0, 0) and the estimate simply omits that image."""
    try:
        import io
        from PIL import Image
        with Image.open(io.BytesIO(data)) as img:
            return img.size
    except Exception:
        return (0, 0)


def estimate_cost(images: list) -> dict:
    """{"visual_tokens", "input_tokens", "output_tokens", "usd", "measured"}
    for one parse of these screenshots.

    All the images go in one request, so this is the cost of the whole parse,
    not of each capture."""
    visual = 0
    measured = 0
    for img in images:
        w, h = image_size(img.get("data") or b"")
        if w and h:
            measured += 1
            visual += image_tokens(w, h)
    inp = visual + _PROMPT_TOKENS
    usd = (inp * INPUT_USD_PER_MTOK + _REPLY_TOKENS * OUTPUT_USD_PER_MTOK) / 1_000_000
    return {"visual_tokens": visual, "input_tokens": inp,
            "output_tokens": _REPLY_TOKENS, "usd": usd, "measured": measured}


def format_cost(est: dict) -> str:
    """A one-line "about 0.4 cents" for the UI. Fractions of a cent are the
    honest scale here and rounding them to $0.00 would read as "free"."""
    if not est.get("measured"):
        # Nothing whose dimensions could be read, so the image side of the
        # bill is unknown — saying "4 cents" here would be a floor dressed up
        # as an estimate.
        return f"a few cents, read by {MODEL}"
    usd = est.get("usd") or 0.0
    amount = f"{usd * 100:.1f}¢" if usd < 0.10 else f"${usd:.2f}"
    return (f"about {amount} — {est['visual_tokens']:,} visual tokens "
            f"+ prompt, read by {MODEL}")


# ordinary_profit was missing and is the single most important line for a
# JGAAP bank or insurer — 経常利益 is what the market quotes, and there is no
# operating profit above it to stand in.
#
# Deliberately NOT in this list: cet1_ratio, solvency_margin, esr, or any other
# regulatory capital measure. A vision model transcribing a capital ratio off a
# slide can misread which of several ratios it is looking at — CET1 before or
# after unrealised gains, group or bank-only, transitional or fully loaded —
# and a capital ratio that is wrong in that way is worse than absent. Those
# figures are not collected here at all; see fundamentals.CAPITAL_DISCLAIMER.
_METRICS = ["net_sales", "operating_profit", "ordinary_profit",
            "net_profit", "eps", "dps"]
_UNITS = ["jpy", "thousand_jpy", "million_jpy", "billion_jpy", "trillion_jpy", "percent"]

_SCHEMA = {
    "type": "object",
    "properties": {
        "company_name": {"type": ["string", "null"]},
        "sec_code": {"type": ["string", "null"]},
        "cells": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "metric": {"type": "string", "enum": _METRICS},
                    "fiscal_year": {"type": "string"},
                    "basis": {"type": "string", "enum": ["consensus", "company"]},
                    "value": {"type": "number"},
                    "unit": {"type": "string", "enum": _UNITS},
                    "n_analysts": {"type": ["integer", "null"]},
                    "from_image": {"type": ["string", "null"]},
                },
                "required": ["metric", "fiscal_year", "basis", "value", "unit",
                             "n_analysts", "from_image"],
                "additionalProperties": False,
            },
        },
        "unreadable": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["company_name", "sec_code", "cells", "unreadable"],
    "additionalProperties": False,
}

_PROMPT = """\
These screenshots come from a financial terminal showing analyst estimates for \
a Japanese listed company{who}.

Extract every forecast figure you can read into `cells`.

Rules, in order of importance:

1. **Never invent a number.** If a figure is cut off at an edge, obscured by a \
tooltip, blurred, or you are otherwise not certain what it says, leave it out \
of `cells` and add a short note to `unreadable` describing what you could not \
read and which image it was in. A missing value is recoverable; a wrong one is \
not.
2. **Record the unit as printed.** Do not convert. If the column header says \
"JPYmn" use million_jpy; if figures are in billions use billion_jpy; per-share \
figures are jpy. If the unit is not stated anywhere, treat the figure as \
unreadable rather than assuming one.
3. **Fiscal year as the company labels it** — "FY2027", matching the year the \
fiscal year *ends* in. If a column is headed only "FY+1" or "Next year" with no \
absolute year shown anywhere, that is unreadable.
4. `basis` is "consensus" for analyst/street estimates and "company" for the \
company's own guidance. Terminals usually show consensus; only use "company" \
where the screenshot explicitly labels it as guidance or company forecast.
5. The images may be different parts of one table. Join them: a header in one \
and rows in another describe the same figures. Do not report the same \
metric/year/basis twice — if two images disagree, prefer the clearer one and \
note the disagreement in `unreadable`.
6. Set `from_image` to the filename the figure came from.
7. **Do not record capital or solvency ratios.** CET1, total capital ratio, \
leverage ratio, solvency margin and ESR are not among the metrics above, and \
must not be mapped onto one that is. If a screenshot shows them, note that in \
`unreadable` and move on. An accounting equity-to-assets ratio is not a \
capital ratio either.
8. For a bank or an insurer, "ordinary income" (経常収益) is the top line — \
record it as net_sales — and "ordinary profit" (経常利益) is ordinary_profit. \
Do not record a net revenue or gross profit figure as net_sales for these \
issuers: it is a different measure, and leaving it out is correct.

Return only figures actually visible in the images."""


def _b64(data: bytes) -> str:
    return base64.standard_b64encode(data).decode("ascii")


def validate_images(images: list) -> tuple:
    """(ok, problems) for a list of {"name","media_type","data"} dicts."""
    problems = []
    if not images:
        problems.append("No screenshots attached.")
    if len(images) > MAX_IMAGES:
        problems.append(f"{len(images)} images attached; the limit is {MAX_IMAGES}.")
    for img in images:
        if img.get("media_type") not in SUPPORTED_MEDIA:
            problems.append(f"{img.get('name', '?')}: {img.get('media_type')} "
                            f"is not a supported image type.")
        if len(img.get("data") or b"") > MAX_IMAGE_BYTES:
            problems.append(f"{img.get('name', '?')}: larger than "
                            f"{MAX_IMAGE_BYTES // (1024 * 1024)}MB.")
    return (not problems), problems


def _content_blocks(images: list, code: str, name: str) -> list:
    who = ""
    if name or code:
        who = f" ({name} {code})".replace("  ", " ").rstrip()
    blocks = [{"type": "image",
               "source": {"type": "base64",
                          "media_type": img["media_type"],
                          "data": _b64(img["data"])}}
              for img in images]
    listing = "\n".join(f"- image {i + 1}: {img.get('name', '?')}"
                        for i, img in enumerate(images))
    blocks.append({"type": "text",
                   "text": _PROMPT.format(who=who) + "\n\nImages, in order:\n" + listing})
    return blocks


def _loads_lenient(text: str) -> dict:
    """Parse the model's JSON, tolerating a code fence or surrounding prose.
    Only used on the fallback path, where the response was not schema-bound."""
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
        raise


def extract_from_images(images: list, api_key: str, code: str = "", name: str = "",
                        model: str = MODEL) -> dict:
    """{"cells": [...], "unreadable": [...], "company_name", "sec_code"}.

    Raises on an API or parse failure — the caller surfaces it rather than
    silently showing an empty grid, which would read as "nothing in the
    screenshots" instead of "the call failed"."""
    import anthropic

    ok, problems = validate_images(images)
    if not ok:
        raise ValueError("; ".join(problems))

    client = anthropic.Anthropic(api_key=api_key)
    messages = [{"role": "user", "content": _content_blocks(images, code, name)}]

    # Structured output binds the reply to the schema above. The pinned floor in
    # requirements.txt is old enough that a deployment could still be running an
    # SDK without output_config or adaptive thinking, so fall back to a plain
    # call and a lenient parse rather than failing outright on a stale install.
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
        except TypeError as exc:                      # kwarg unknown to this SDK
            last_exc = exc
        except anthropic.BadRequestError as exc:      # param unknown to the API
            last_exc = exc
    else:
        raise RuntimeError(f"Could not call the model: {last_exc}")

    if getattr(resp, "stop_reason", "") == "refusal":
        raise RuntimeError("The model declined to read these images.")
    text = next((b.text for b in resp.content if getattr(b, "type", "") == "text"), "")
    data = _loads_lenient(text)
    return normalise(data)


def normalise(data: dict) -> dict:
    """Drop anything malformed and convert every figure to yen, so the review
    grid and the store speak one unit. A cell whose unit we cannot honour is
    moved to `unreadable` rather than being silently rescaled."""
    scale = {"jpy": 1.0, "thousand_jpy": 1e3, "million_jpy": 1e6,
             "billion_jpy": 1e9, "trillion_jpy": 1e12}
    cells, unreadable = [], list(data.get("unreadable") or [])
    seen = set()
    for raw in (data.get("cells") or []):
        metric, fy = raw.get("metric"), str(raw.get("fiscal_year") or "").strip()
        basis = raw.get("basis") or "consensus"
        if metric not in _METRICS or not fy:
            unreadable.append(f"Discarded an entry with metric={metric!r} fy={fy!r}.")
            continue
        try:
            value = float(raw.get("value"))
        except (TypeError, ValueError):
            unreadable.append(f"{metric} {fy}: value was not a number.")
            continue
        unit = raw.get("unit") or "jpy"
        if unit == "percent":
            unreadable.append(f"{metric} {fy}: reported as a percentage, not a figure.")
            continue
        if unit not in scale:
            unreadable.append(f"{metric} {fy}: unrecognised unit {unit!r}.")
            continue
        # Per-share figures are already per-share yen whatever the table's
        # headline unit says; scaling those by a millions header would be wrong
        # by six orders of magnitude.
        yen = value if metric in ("eps", "dps") else value * scale[unit]
        key = (metric, fy, basis)
        if key in seen:
            unreadable.append(f"{metric} {fy} {basis}: appeared more than once; kept the first.")
            continue
        seen.add(key)
        cells.append({"metric": metric, "fiscal_year": fy, "basis": basis,
                      "value": yen, "printed_unit": unit,
                      "n_analysts": raw.get("n_analysts"),
                      "from_image": raw.get("from_image") or ""})
    return {"company_name": data.get("company_name"), "sec_code": data.get("sec_code"),
            "cells": cells, "unreadable": unreadable}
