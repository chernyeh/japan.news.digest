"""
notice_reader.py — read the numbers out of a TDnet notice.

TDnet tells the app *that* a company filed a buyback or extraordinary-item
notice; the size is in the PDF. Three notice families carry the numbers the
forward reads need:

  * 自己株式の取得に係る事項の決定 — a buyback authorisation: how many shares,
    how much yen, over what window;
  * 自己株式の取得状況 / 取得結果 / 取得終了 — progress on it, cumulative;
  * 特別利益・特別損失の計上 / 減損損失 / 投資有価証券売却益 — one-offs, with
    the amount and where in the P&L they land.

One call per notice, on a button press, through the same document path as
fx_extract (same model, same structured output, same fallback). What comes
back is shown for review before anything is saved, as with every other reader
in this app.
"""

import fx_extract
import models

MODEL = models.DOC_EXTRACT_MODEL

_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["kind", "buyback", "one_offs", "notes"],
    "properties": {
        "kind": {"type": "string", "enum": [
            "buyback_authorisation", "buyback_progress", "buyback_completion",
            "share_cancellation", "one_offs", "other"]},
        "buyback": {
            "type": "object", "additionalProperties": False,
            "required": ["authorised_shares", "authorised_amount_jpy", "period_start",
                         "period_end", "cumulative_shares", "cumulative_amount_jpy", "as_of"],
            "properties": {
                "authorised_shares": {"type": ["number", "null"],
                                      "description": "取得し得る株式の総数, in shares"},
                "authorised_amount_jpy": {"type": ["number", "null"],
                                          "description": "株式の取得価額の総額 authorised, in yen"},
                "period_start": {"type": "string", "description": "YYYY-MM-DD or ''"},
                "period_end": {"type": "string", "description": "YYYY-MM-DD or ''"},
                "cumulative_shares": {"type": ["number", "null"],
                                      "description": "shares bought so far under this authorisation (累計)"},
                "cumulative_amount_jpy": {"type": ["number", "null"],
                                          "description": "yen spent so far under this authorisation (累計)"},
                "as_of": {"type": "string", "description": "date the cumulative figures are at"},
            }},
        "one_offs": {
            "type": "array",
            "items": {
                "type": "object", "additionalProperties": False,
                "required": ["item", "amount_jpy", "line", "period", "consolidated"],
                "properties": {
                    "item": {"type": "string", "description": "what it is, in English"},
                    "amount_jpy": {"type": "number",
                                   "description": "yen; positive for a gain, negative for a loss"},
                    "line": {"type": "string", "enum": ["operating", "non_operating",
                                                        "extraordinary"]},
                    "period": {"type": "string",
                               "description": "the period it is booked in, e.g. '2Q FY3/27'"},
                    "consolidated": {"type": "boolean"},
                }}},
        "notes": {"type": "string"},
    },
}

_PROMPT = """This is a Japanese timely-disclosure notice{who}. Read only what it states.

If it is about a share buyback (自己株式の取得):
- An authorisation (取得に係る事項の決定): the maximum shares and yen, and the
  acquisition window. kind = buyback_authorisation.
- A progress or results report (取得状況, 途中経過, 取得結果) that is not the end:
  the cumulative shares and yen bought under the authorisation (the 累計 figures
  in the ご参考 section) and the date they are at; also restate the
  authorisation's limits and window if the notice repeats them.
  kind = buyback_progress.
- The end of an authorisation (取得終了): the same, kind = buyback_completion.
- A cancellation (消却) only: kind = share_cancellation.

If it reports extraordinary or one-off items (特別利益, 特別損失, 減損損失,
投資有価証券売却益, 固定資産売却益, 引当金, 和解金 and the like): list each with
its amount in yen (a gain positive, a loss negative), where it lands —
operating (inside operating profit), non_operating (営業外, between operating
and ordinary profit) or extraordinary (特別損益, below ordinary profit) — the
period it is booked in, and whether the figure is consolidated. kind = one_offs.

Amounts in a Japanese notice are often in 百万円 or 千円: convert to yen.
Leave a field null or '' when the notice does not state it; never estimate.
Put anything a reviewer should know (e.g. figures are parent-only) in notes."""


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def normalise(data: dict) -> dict:
    data = data if isinstance(data, dict) else {}
    bb = data.get("buyback") or {}
    out = {"kind": str(data.get("kind") or "other"),
           "buyback": {k: (_num(bb.get(k)) if "shares" in k or "amount" in k
                           else str(bb.get(k) or "")[:10])
                       for k in ("authorised_shares", "authorised_amount_jpy", "period_start",
                                 "period_end", "cumulative_shares", "cumulative_amount_jpy",
                                 "as_of")},
           "one_offs": [], "notes": str(data.get("notes") or "").strip()}
    for o in data.get("one_offs") or ():
        amt = _num(o.get("amount_jpy"))
        if amt is None:
            continue
        line = o.get("line") if o.get("line") in ("operating", "non_operating",
                                                  "extraordinary") else "extraordinary"
        out["one_offs"].append({"item": str(o.get("item") or "").strip(), "amount_jpy": amt,
                                "line": line, "period": str(o.get("period") or "").strip(),
                                "consolidated": bool(o.get("consolidated", True))})
    return out


def read_notice(data: bytes, api_key: str, code: str = "", title: str = "",
                model: str = MODEL) -> dict:
    """The normalised reading of one notice PDF, or raises — an empty reading
    would look like "the notice states nothing", which is a finding."""
    import anthropic

    ok, problems = fx_extract.validate_pdf(title, data)
    if not ok:
        raise ValueError("; ".join(problems))
    who = f" filed by {code}" if code else ""
    messages = [{"role": "user", "content": [
        {"type": "document", "source": {"type": "base64", "media_type": "application/pdf",
                                        "data": fx_extract._b64(data)}},
        {"type": "text", "text": _PROMPT.format(who=who)}]}]
    client = anthropic.Anthropic(api_key=api_key)
    attempts = ({"output_config": {"format": {"type": "json_schema", "schema": _SCHEMA},
                                   "effort": models.DOC_EXTRACT_EFFORT},
                 "thinking": {"type": "adaptive"}}, {})
    last_exc = None
    for extra in attempts:
        try:
            resp = client.messages.create(model=model, max_tokens=8000,
                                          messages=messages, **extra)
            break
        except TypeError as exc:
            last_exc = exc
        except anthropic.BadRequestError as exc:
            last_exc = exc
    else:
        raise RuntimeError(f"Could not call the model: {last_exc}")
    if getattr(resp, "stop_reason", "") == "refusal":
        raise RuntimeError("The model declined to read this notice.")
    text = next((b.text for b in resp.content if getattr(b, "type", "") == "text"), "")
    return normalise(fx_extract._loads_lenient(text))


def to_event(reading: dict, code: str, date: str, title: str, existing: list) -> dict:
    """A capital-events entry from a buyback reading, or None for anything
    else. The id is the same one the editor would give it."""
    import capital_events as CE
    kind_to_type = {"buyback_authorisation": "Buyback announced",
                    "buyback_progress": "Buyback progress",
                    "buyback_completion": "Buyback completed",
                    "share_cancellation": "Share cancellation"}
    etype = kind_to_type.get(reading.get("kind"))
    if not etype:
        return None
    bb = reading.get("buyback") or {}
    ev = {"date": date, "type": etype, "source": "typed:Company data", "tdnet_title": title,
          "note": reading.get("notes", ""), "logged_at": date}
    if etype == "Buyback announced":
        ev.update({"amount_jpy": bb.get("authorised_amount_jpy"),
                   "shares": bb.get("authorised_shares"), "until": bb.get("period_end", "")})
    else:
        ev.update({"cum_amount_jpy": bb.get("cumulative_amount_jpy"),
                   "cum_shares": bb.get("cumulative_shares"), "as_of": bb.get("as_of", "")})
    ev["id"] = CE.new_id(code, date, etype, existing)
    return ev
