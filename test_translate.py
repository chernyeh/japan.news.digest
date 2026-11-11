"""translate.py: Japanese headlines and filing titles come back in English, or
not at all — never as the Japanese posing as a translation."""
import json
import types

import pytest

import translate as T


@pytest.fixture(autouse=True)
def clean_state(monkeypatch):
    T._cache.clear()
    T._google_blocked_until = 0.0
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("DEEPL_API_KEY", raising=False)
    # No test may reach the network by accident.
    monkeypatch.setattr(T.requests, "get", lambda *a, **k: pytest.fail("unexpected Google call"))
    monkeypatch.setattr(T.requests, "post", lambda *a, **k: pytest.fail("unexpected DeepL call"))
    yield
    T._cache.clear()


class FakeClaude:
    """Stands in for anthropic.Anthropic: answers with `reply(body)` as JSON."""
    calls = []

    def __init__(self, reply, fail_first_with=None):
        self.reply, self.fail_first_with = reply, fail_first_with
        self.messages = self

    def create(self, **kw):
        FakeClaude.calls.append(kw)
        if self.fail_first_with and len(FakeClaude.calls) == 1:
            raise self.fail_first_with
        items = [line.split("\t", 1) for line in kw["messages"][0]["content"].split("\n")]
        text = json.dumps({"translations": self.reply(items)})
        return types.SimpleNamespace(stop_reason="end_turn",
                                     content=[types.SimpleNamespace(type="text", text=text)])


def use_claude(monkeypatch, reply, **kw):
    """A whole stand-in `anthropic` module, put in place for this test only.
    Not a patch on the real one, so the test does not depend on which SDK
    version is installed, or on one being installed at all."""
    import sys

    class APIError(Exception):
        pass

    class BadRequestError(APIError):
        pass

    class AuthenticationError(APIError):
        pass

    class PermissionDeniedError(APIError):
        pass

    fake = types.ModuleType("anthropic")
    fake.Anthropic = lambda **_: FakeClaude(reply, **kw)
    fake.APIError, fake.BadRequestError = APIError, BadRequestError
    fake.AuthenticationError, fake.PermissionDeniedError = AuthenticationError, PermissionDeniedError
    monkeypatch.setitem(sys.modules, "anthropic", fake)
    FakeClaude.calls = []
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")


def english(items):
    return [{"n": int(n), "en": f"EN:{t}"} for n, t in items]


def test_needs_translation():
    assert T.needs_translation("決算短信")
    assert T.needs_translation("Toyota 2Q 決算")
    assert T.needs_translation("ｶﾌﾞ")                       # half-width katakana
    assert not T.needs_translation("Notice regarding dividend")
    assert not T.needs_translation("")


def test_claude_translates_in_batches_and_keeps_order(monkeypatch):
    use_claude(monkeypatch, english)
    monkeypatch.setattr(T, "BATCH", 2)
    out = T.translate_many(["決算短信", "Already English", "配当予想", "自己株式", "決算短信"])
    assert out == ["EN:決算短信", "Already English", "EN:配当予想", "EN:自己株式", "EN:決算短信"]
    assert len(FakeClaude.calls) == 2                       # 3 unique Japanese titles, 2 a batch
    call = FakeClaude.calls[0]
    assert call["model"] == T.models.TRANSLATE_MODEL
    assert call["output_config"]["format"]["type"] == "json_schema"


def test_cache_spares_a_second_request(monkeypatch):
    use_claude(monkeypatch, english)
    T.translate_many(["決算短信"])
    T.translate_many(["決算短信"])
    assert len(FakeClaude.calls) == 1


def test_falls_back_to_a_plain_call_on_an_old_sdk(monkeypatch):
    use_claude(monkeypatch, english, fail_first_with=TypeError("unexpected keyword 'output_config'"))
    assert T.translate_many(["決算短信"]) == ["EN:決算短信"]
    assert "output_config" not in FakeClaude.calls[1]


def test_bad_items_are_dropped_not_stored(monkeypatch):
    def reply(items):
        return [{"n": 1, "en": "決算短信"},                       # handed back unchanged
                {"n": 2, "en": "<script>alert(1)</script>"},      # markup the input lacked
                {"n": 9, "en": "out of range"},
                {"n": 3, "en": "Dividend forecast"}]
    use_claude(monkeypatch, reply)
    # What Claude got wrong goes on to Google, which here refuses.
    monkeypatch.setattr(T.requests, "get", lambda *a, **k: types.SimpleNamespace(status_code=302))
    out = T.translate_many(["決算短信", "株主還元", "配当予想"])
    assert out == [None, None, "Dividend forecast"]


def test_google_refusal_returns_none_and_backs_off(monkeypatch):
    calls = []

    def blocked(*a, **k):
        calls.append(1)
        return types.SimpleNamespace(status_code=302)        # the google.com/sorry CAPTCHA
    monkeypatch.setattr(T.requests, "get", blocked)
    assert T.translate_many(["決算短信", "配当予想"]) == [None, None]
    assert len(calls) == 1                                   # stopped at the first refusal
    assert T.translate_many(["自己株式"]) == [None]
    assert len(calls) == 1                                   # and stays away during the cooldown


def test_google_is_used_when_there_is_no_key(monkeypatch):
    ok = types.SimpleNamespace(status_code=200, json=lambda: [[["Earnings summary", "決算短信"]]])
    monkeypatch.setattr(T.requests, "get", lambda *a, **k: ok)
    assert T.translate_many(["決算短信"]) == ["Earnings summary"]


def test_fill_translations_retries_the_japanese_the_old_helper_stored(monkeypatch):
    use_claude(monkeypatch, english)
    rows = [
        {"Title": "決算短信", "TitleEN": "決算短信", "PubDateTime": "2026-11-10 15:00"},  # old failure
        {"Title": "配当予想", "TitleEN": "", "PubDateTime": "2026-11-09 15:00"},          # never done
        {"Title": "自己株式", "TitleEN": "Treasury stock", "PubDateTime": "2026-11-08"},  # fine already
        {"Title": "Notice of AGM", "TitleEN": "", "PubDateTime": "2026-11-07"},          # English source
    ]
    asked, filled = T.fill_translations(rows, "Title", "TitleEN", newest_first=lambda r: r["PubDateTime"])
    assert (asked, filled) == (2, 2)
    assert [r["TitleEN"] for r in rows] == ["EN:決算短信", "EN:配当予想", "Treasury stock", "Notice of AGM"]


def test_fill_translations_clears_rather_than_keeps_japanese_on_failure(monkeypatch):
    monkeypatch.setattr(T.requests, "get", lambda *a, **k: types.SimpleNamespace(status_code=429))
    rows = [{"Title": "決算短信", "TitleEN": "決算短信"}, {"Title": "配当予想", "TitleEN": ""}]
    assert T.fill_translations(rows, "Title", "TitleEN") == (2, 0)
    assert [r["TitleEN"] for r in rows] == ["", ""]           # blank, so the next run retries


def test_fill_translations_takes_the_newest_first_under_a_limit(monkeypatch):
    use_claude(monkeypatch, english)
    rows = [{"Title": f"決算{i}", "TitleEN": "", "PubDateTime": f"2026-11-0{i}"} for i in range(1, 6)]
    T.fill_translations(rows, "Title", "TitleEN", limit=2, newest_first=lambda r: r["PubDateTime"])
    assert [bool(r["TitleEN"]) for r in rows] == [False, False, False, True, True]


def test_headlines_that_fail_get_no_translated_title(monkeypatch):
    import collector
    monkeypatch.setattr(T.requests, "get", lambda *a, **k: types.SimpleNamespace(status_code=302))
    arts = [{"language": "ja", "original_title": "日銀が利上げ", "title": "日銀が利上げ"}]
    collector.translate_articles(arts)
    assert "translated_title" not in arts[0]                 # so the next refresh tries again
    assert arts[0]["title"] == "日銀が利上げ"


def test_headlines_are_translated(monkeypatch):
    import collector
    use_claude(monkeypatch, english)
    arts = [{"language": "ja", "original_title": "日銀が利上げ", "title": ""},
            {"language": "en", "original_title": "BOJ hikes", "title": "BOJ hikes"}]
    collector.translate_articles(arts)
    assert arts[0]["translated_title"] == arts[0]["title"] == "EN:日銀が利上げ"
    assert "translated_title" not in arts[1]


def test_a_translator_crashing_does_not_escape(monkeypatch):
    """The headline fetch calls this; an unexpected error must not take it down."""
    def boom(*a, **k):
        raise RuntimeError("something nobody anticipated")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr(T, "_claude", boom)
    monkeypatch.setattr(T, "_google", boom)
    assert T.translate_many(["決算短信"]) == [None]
