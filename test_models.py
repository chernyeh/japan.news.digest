"""Tests for the shared model layer — `python test_models.py`.

No test framework and no network: models.call is exercised against a stubbed
Anthropic SDK. What it does is invisible until it fails in production, and the
failure modes are quiet ones — a briefing panel that renders empty because the
reply was read as content[0] while the deep model put a thinking block there,
or a stale SDK on Streamlit Cloud that rejects output_config and takes every
summary down with it. Both are checked here rather than by clicking Summarise.
"""

import sys
import types

import models


class _BadRequestError(Exception):
    """Stands in for anthropic.BadRequestError — raised when the API itself
    does not know a parameter, as opposed to TypeError from the SDK."""


def _stub_anthropic():
    """Install a fake `anthropic` for models.call's lazy import."""
    mod = types.ModuleType("anthropic")
    mod.BadRequestError = _BadRequestError
    sys.modules["anthropic"] = mod


_stub_anthropic()


class _Block:
    def __init__(self, type_, text=""):
        self.type, self.text = type_, text


class _Msg:
    def __init__(self, content, stop_reason="end_turn"):
        self.content, self.stop_reason = content, stop_reason


class _Stream:
    def __init__(self, msg):
        self._msg = msg

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_final_message(self):
        return self._msg


class _Messages:
    """A stubbed messages namespace. `known` is the set of optional kwargs this
    pretend SDK understands; anything else raises TypeError, the way a real
    older SDK does. `reject` are kwargs the pretend API rejects at request
    time, which is the other half of the ladder."""

    def __init__(self, calls, msg, known=None, reject=()):
        self._calls, self._msg = calls, msg
        self._known = {"model", "max_tokens", "messages"} | set(
            known if known is not None else ("thinking", "output_config", "betas", "fallbacks"))
        self._reject = set(reject)

    def stream(self, **kwargs):
        unknown = set(kwargs) - self._known
        if unknown:
            raise TypeError(f"unexpected keyword argument {sorted(unknown)[0]!r}")
        if set(kwargs) & self._reject:
            raise _BadRequestError("unknown parameter")
        self._calls.append(kwargs)
        return _Stream(self._msg)


class _Client:
    def __init__(self, msg, known=None, reject=(), beta=True):
        self.calls = []
        self.messages = _Messages(self.calls, msg, known, reject)
        if beta:
            self.beta = types.SimpleNamespace(
                messages=_Messages(self.calls, msg, known, reject))


_OK = _Msg([_Block("thinking", ""), _Block("text", "## BOJ & Rates\nHeld at 0.5% [1].")])


# ── Which options each model is asked for ────────────────────────────────

def test_deep_model_asks_for_thinking_effort_and_fallbacks():
    """The first attempt on the deep model carries everything: adaptive
    thinking, the configured effort, and server-side fallbacks so a refusal on
    a cyber-breach headline is re-run rather than shown to the reader."""
    c = _Client(_OK)
    models.call(c, models.SUMMARY_MODEL_DEEP, "prompt", 32000, "low")
    assert len(c.calls) == 1, c.calls
    sent = c.calls[0]
    assert sent["thinking"] == {"type": "adaptive"}, sent
    assert sent["output_config"] == {"effort": "low"}, sent
    assert sent["fallbacks"] == "default", sent
    assert sent["max_tokens"] == 32000, sent


def test_fast_model_sends_no_thinking_or_effort():
    """Haiku 4.5 takes neither adaptive thinking nor output_config.effort —
    sending either is a 400, so the fast panels must go out bare."""
    c = _Client(_OK)
    models.call(c, models.SUMMARY_MODEL_FAST, "prompt", 8192)
    assert len(c.calls) == 1, c.calls
    assert set(c.calls[0]) == {"model", "max_tokens", "messages"}, c.calls[0]


def test_invalid_effort_falls_back_to_the_default():
    c = _Client(_OK)
    models.call(c, models.SUMMARY_MODEL_DEEP, "prompt", 32000, "ludicrous")
    assert c.calls[0]["output_config"] == {"effort": models.DEFAULT_SUMMARY_EFFORT}


# ── Degrading onto an older SDK or API ───────────────────────────────────

def test_sdk_without_fallbacks_degrades_to_a_plain_thinking_call():
    """A deployment on an SDK that has never heard of `fallbacks` still gets
    the deep model with thinking, rather than an error."""
    c = _Client(_OK, known=("thinking", "output_config"))
    models.call(c, models.SUMMARY_MODEL_DEEP, "prompt", 32000)
    assert len(c.calls) == 1, "should have retried, not repeated"
    assert "fallbacks" not in c.calls[0]
    assert c.calls[0]["thinking"] == {"type": "adaptive"}


def test_sdk_without_output_config_degrades_all_the_way_to_bare():
    c = _Client(_OK, known=())
    models.call(c, models.SUMMARY_MODEL_DEEP, "prompt", 32000)
    assert set(c.calls[0]) == {"model", "max_tokens", "messages"}, c.calls[0]


def test_api_rejecting_a_parameter_degrades_too():
    """TypeError is the SDK not knowing a kwarg; BadRequestError is the API
    not knowing the parameter. Both have to step down the same ladder."""
    c = _Client(_OK, reject=("fallbacks",))
    models.call(c, models.SUMMARY_MODEL_DEEP, "prompt", 32000)
    assert "fallbacks" not in c.calls[0]


def test_sdk_without_a_beta_namespace_degrades():
    c = _Client(_OK, beta=False)
    models.call(c, models.SUMMARY_MODEL_DEEP, "prompt", 32000)
    assert "fallbacks" not in c.calls[0]


def test_every_attempt_failing_raises():
    class _Dead:
        beta = None
        class messages:
            @staticmethod
            def stream(**kwargs):
                raise TypeError("nope")
    try:
        models.call(_Dead(), models.SUMMARY_MODEL_FAST, "prompt", 8192)
    except RuntimeError as exc:
        assert "Could not call the model" in str(exc), exc
    else:
        assert False, "should have raised"


# ── Reading the reply ────────────────────────────────────────────────────

def test_text_is_read_past_the_thinking_block():
    """The regression this file exists for: with thinking on, content[0] is a
    thinking block and the old `content[0].text` would have rendered an empty
    briefing — which reads as 'nothing happened today'."""
    out = models.call(_Client(_OK), models.SUMMARY_MODEL_DEEP, "prompt", 32000)
    assert out.startswith("## BOJ & Rates"), out


def test_refusal_raises_rather_than_rendering_blank():
    c = _Client(_Msg([_Block("text", "")], stop_reason="refusal"))
    try:
        models.call(c, models.SUMMARY_MODEL_DEEP, "prompt", 32000)
    except RuntimeError as exc:
        assert "declined" in str(exc), exc
    else:
        assert False, "should have raised"


def test_budget_spent_on_thinking_raises_a_legible_error():
    """If the whole ceiling goes on thinking there is no text block at all.
    The error has to name the dial that fixes it, not just say 'empty'."""
    c = _Client(_Msg([_Block("thinking", "...")], stop_reason="max_tokens"))
    try:
        models.call(c, models.SUMMARY_MODEL_DEEP, "prompt", 32000)
    except RuntimeError as exc:
        assert "SUMMARY_EFFORT" in str(exc) and "32,000" in str(exc), exc
    else:
        assert False, "should have raised"


def test_empty_reply_raises():
    c = _Client(_Msg([_Block("text", "   ")]))
    try:
        models.call(c, models.SUMMARY_MODEL_FAST, "prompt", 8192)
    except RuntimeError as exc:
        assert "empty" in str(exc), exc
    else:
        assert False, "should have raised"


# ── The constants themselves ─────────────────────────────────────────────

def test_every_model_in_use_has_a_price():
    """A cost estimate quoting $0.00 is worse than no estimate, so an unpriced
    model has to fail loudly — and every model the app actually calls is one
    the estimator can price."""
    for model in (models.SUMMARY_MODEL_DEEP, models.SUMMARY_MODEL_FAST,
                  models.CLASSIFY_MODEL, models.DOC_EXTRACT_MODEL,
                  models.DOC_EXTRACT_FALLBACK_MODEL):
        inp, out = models.price(model)
        assert inp > 0 and out > 0, model
    try:
        models.price("claude-imaginary-9")
    except KeyError:
        pass
    else:
        assert False, "an unpriced model should raise"


def test_deep_budget_clears_the_briefings_this_app_actually_writes():
    """The stored briefings run ~4,600 output tokens; the budget has to leave
    room for that plus thinking, or the panel truncates mid-bullet."""
    assert models.SUMMARY_MAX_TOKENS_DEEP >= 4 * 4600, models.SUMMARY_MAX_TOKENS_DEEP


def test_fast_model_is_not_in_the_thinking_set():
    assert models.SUMMARY_MODEL_FAST not in models.ADAPTIVE_THINKING_MODELS
    assert models.CLASSIFY_MODEL not in models.ADAPTIVE_THINKING_MODELS
    assert models.SUMMARY_MODEL_DEEP in models.ADAPTIVE_THINKING_MODELS


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  ok   {name}")
        except AssertionError as exc:
            failed += 1
            print(f"  FAIL {name}: {exc or '(assertion)'}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
