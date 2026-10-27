"""
models.py — every Claude model choice this app makes, in one place.

Until now the model IDs were string literals in five files: app.py (the
summary panels), collector.py (headline classification), emailer.py (the
email briefing), and the three document readers. Changing the summariser
meant editing all five and missing one. Import from here instead.

Nothing in here calls the API on import; `call()` takes a client the caller
built, so a module can import the constants without pulling in the SDK.
"""

# ── Which model does what ─────────────────────────────────────────────────
#
# The deep model is for the two briefings that carry analytical judgment:
# the 24h all-source briefing and the TDnet filings briefing. Both read
# 60-200 headlines and have to cluster them, weigh what matters, and say
# what it implies for positioning — the one place in this app where a
# better model changes the output rather than just the latency.
#
# Measured on this app's own stored briefings (data/ai_summaries.json): a
# 75-headline briefing is roughly 4,300 input and 3,500 output tokens, so
# about $0.02 a click on Haiku 4.5 against $0.11 on Opus 5 before thinking
# tokens. At one click of each panel per trading day that is ~$4/month
# against ~$13-24/month. Opus 5.5 uses Opus 5's tokenizer at 20% lower
# rates, so the same briefing is ~$0.09 and ~$10-19/month; Haiku 5.5 counts
# the same text as ~30% more tokens at a tenth of Haiku 4.5's rates, so well
# under a cent a click. Both are rescaled from the old measurements, not
# re-measured. The bill is not the binding constraint here;
# SUMMARY_MAX_TOKENS_DEEP below is.
SUMMARY_MODEL_DEEP = "claude-opus-5-5"

# The fast model keeps the high-frequency panels — Nikkei breaking news and
# the per-sector summaries — on Haiku. They get re-clicked all day, are read
# at a glance, and their job is closer to compression than analysis.
SUMMARY_MODEL_FAST = "claude-haiku-5-5"

# Headline classification: 60-130 headlines at a time into a fixed JSON
# shape (corp_action, direction, TSE code, confidence), on every collection
# run. Structured extraction from short text is what Haiku is for, and the
# volume here is the highest in the app — this is the one path where moving
# to Opus would be genuinely wasteful rather than merely more expensive.
CLASSIFY_MODEL = "claude-haiku-5-5"

# Document reading — results decks, Q&A transcripts, consensus screenshots.
# The reasoning for these lives next to each MODEL alias in fx_extract.py,
# qa_extract.py and consensus_vision.py; the IDs live here.
DOC_EXTRACT_MODEL = "claude-opus-5-5"
DOC_EXTRACT_FALLBACK_MODEL = "claude-sonnet-5-5"

# Opus 5 read these documents at the API's default effort of "high". Opus
# 5.5's default is "medium", so it is set explicitly to keep the readers
# where they were: a misread figure here is the costly failure, not the bill.
DOC_EXTRACT_EFFORT = "high"

# ── Output budget ─────────────────────────────────────────────────────────
#
# On Opus 5.5 adaptive thinking is always on, and thinking tokens both
# bill at the output rate and count against max_tokens. The stored briefings
# already run 3,500-4,600 output tokens on their own; thinking can add as
# much again or more, so the old 8,192 ceiling would cut the briefing off
# mid-bullet — exactly what the prompt's "never truncate" instruction
# exists to prevent. Hence the headroom, and hence `call()` streams: the
# SDKs require streaming at caps this large or the request can outlive the
# HTTP timeout.
SUMMARY_MAX_TOKENS_DEEP = 32000
# Haiku 5.5 thinks too, where Haiku 4.5 did not, and counts the same text as
# ~30% more tokens. Even at "low" effort the old 8,192 and 2,048 ceilings
# could cut a fast panel or the email off, so both get headroom. A ceiling
# is not a charge: only the tokens actually written are billed.
SUMMARY_MAX_TOKENS_FAST = 16000
EMAIL_BRIEFING_MAX_TOKENS = 8192

# How hard the deep model thinks. This is the cost dial: it trades thinking
# tokens against thoroughness within one model, and a news briefing is not a
# hard reasoning task, so the default is "medium" — which is also Opus 5.5's
# own default, but it is sent explicitly so the setting is visible here.
# Override with a SUMMARY_EFFORT secret (or env var) — "low" to cut the
# bill, "high"/"xhigh" on a heavy filings day.
DEFAULT_SUMMARY_EFFORT = "medium"

# How hard the fast model thinks — the fast panels, the email briefing and
# headline classification. Haiku 5.5 is the first Haiku that thinks, and it
# does so by default at "medium"; "low" keeps these close to what Haiku 4.5
# did, which is compress quickly. SUMMARY_EFFORT is the deep model's dial
# and does not move this one.
FAST_EFFORT = "low"

# Statuses that mean the request shape is wrong for this deployment rather
# than something being wrong with the account or the service: an unknown
# parameter (400), an endpoint or model this key cannot route to (404), a
# parameter the API parsed but would not accept (422).
_SHAPE_ERRORS = (400, 404, 422)
_EFFORT_LEVELS = ("low", "medium", "high", "xhigh", "max")

# Adaptive thinking and output_config.effort arrived with the 4.6 generation.
# Haiku 4.5 takes neither — sending either is a 400 — so request options are
# per-model, not global; Haiku 5.5 takes both. Add a model here only after
# checking it accepts both.
ADAPTIVE_THINKING_MODELS = frozenset({
    "claude-opus-5-5",
    "claude-sonnet-5-5",
    "claude-haiku-5-5",
    "claude-opus-5",
    "claude-sonnet-5",
    "claude-opus-4-8",
    "claude-opus-4-7",
})

# There is deliberately no server-side-fallback rung here. It was tried: it
# asks for a beta on the beta endpoint, and a deployment whose account does
# not have that beta gets a 404 or 403 rather than the 400 the ladder below
# degrades on, which took the briefing panels down outright. The thing it
# bought — an automatic re-run when the model declines — is worth very little
# on a list of news headlines, and nothing at all next to a panel that works.
# A refusal is now surfaced as a plain error instead, which is honest and
# roughly never happens. (Haiku 5.5 has no server-side fallback at all.)

# ── Prices, USD per million tokens ────────────────────────────────────────
# From claude.com/pricing. These move; a stale figure belongs here, visible
# in one place, rather than buried in an f-string. The previous models stay
# listed so a rollback is a one-line change to the IDs above.
#
# Haiku 5.5 has two rate cards: the figure here is for prompts of 100K
# tokens or fewer, and longer prompts bill at $0.50/$2.50. Nothing in this
# app sends Haiku a prompt anywhere near that — the classification batches
# and fast panels are a few thousand tokens.
PRICING_USD_PER_MTOK = {
    "claude-opus-5-5":   (4.00, 20.00),
    "claude-sonnet-5-5": (2.00, 10.00),
    "claude-haiku-5-5":  (0.10, 0.50),
    "claude-opus-5":     (5.00, 25.00),
    "claude-sonnet-5":   (2.00, 10.00),
    "claude-haiku-4-5":  (1.00, 5.00),
}


def price(model: str) -> tuple:
    """(input, output) USD per million tokens. Raises on an unpriced model:
    a cost estimate quoting $0.00 is worse than no estimate."""
    try:
        return PRICING_USD_PER_MTOK[model]
    except KeyError:
        raise KeyError(f"No price on file for {model} — add it to models.PRICING_USD_PER_MTOK")


def summary_effort(configured: str = "") -> str:
    """The configured effort level, or the default if it is unset or junk.
    Never raises: a typo'd secret should not take the summary panels down."""
    level = (configured or "").strip().lower()
    return level if level in _EFFORT_LEVELS else DEFAULT_SUMMARY_EFFORT


def effort_for(model: str, configured: str = "") -> str:
    """The effort a summary call on `model` runs at: the SUMMARY_EFFORT
    setting on the deep model, FAST_EFFORT on everything else."""
    return summary_effort(configured) if model == SUMMARY_MODEL_DEEP else FAST_EFFORT


def _attempts(model: str, effort: str) -> tuple:
    """Request options to try, best first. requirements.txt pins an SDK floor
    old enough that a deployment could still be running one that has never
    heard of output_config or adaptive thinking, so each rung drops what the
    rung above it needs — same ladder as fx_extract.extract, which has the
    reasoning in full. Every rung here is on the stable endpoint: a rung that
    needs a beta can fail in ways the ladder cannot catch (see above)."""
    if model not in ADAPTIVE_THINKING_MODELS:
        return ({},)
    return (
        {"thinking": {"type": "adaptive"}, "output_config": {"effort": effort}},
        {},
    )


def call(client, model: str, prompt: str, max_tokens: int, effort: str = "") -> str:
    """One summarisation call. Returns the reply text, or raises.

    Raises rather than returning "" on failure: an empty briefing rendered in
    the panel reads as "nothing happened today", which is a claim, and it must
    not be confused with "the call failed"."""
    import anthropic

    messages = [{"role": "user", "content": prompt}]
    effort = summary_effort(effort)
    last_exc = None

    attempts = _attempts(model, effort)
    for rung, extra in enumerate(attempts, 1):
        try:
            with client.messages.stream(model=model, max_tokens=max_tokens,
                                        messages=messages, **extra) as stream:
                msg = stream.get_final_message()
            break
        except TypeError as exc:                    # kwarg unknown to this SDK
            last_exc = exc
        except anthropic.APIStatusError as exc:
            # Degrade only on the statuses that mean "this deployment cannot
            # send this request shape". An expired key, a rate limit or an
            # outage is not fixed by dropping a parameter, and retrying the
            # whole ladder against it just triples the wait before the panel
            # shows an error — so those are raised as themselves, with the
            # API's own message intact.
            if getattr(exc, "status_code", 0) not in _SHAPE_ERRORS:
                raise
            last_exc = exc
        # Logged, not swallowed: when a deployment quietly runs on the bottom
        # rung the briefings get worse (no effort dial) with nothing on screen
        # to say so. This is the only trace of that.
        if rung < len(attempts):
            print(f"models.call: {model} rejected {sorted(extra)} "
                  f"({type(last_exc).__name__}: {last_exc}) — retrying simpler")
    else:
        raise RuntimeError(f"Could not call {model}: {last_exc}")

    if getattr(msg, "stop_reason", "") == "refusal":
        raise RuntimeError("The model declined to write this briefing.")

    text = next((b.text for b in msg.content if getattr(b, "type", "") == "text"), "")
    if not text.strip():
        # With thinking on, the first content block is a thinking block and the
        # text block can be missing entirely when the ceiling is hit first.
        if getattr(msg, "stop_reason", "") == "max_tokens":
            raise RuntimeError(
                f"The model used its whole {max_tokens:,}-token budget on thinking "
                "and returned no briefing — lower SUMMARY_EFFORT or raise the budget."
            )
        raise RuntimeError("The model returned an empty briefing.")
    return text
