"""Tests for the durable briefing store — `python test_ai_summary_store.py`.

No test framework and no network: requests is stubbed with a pretend GitHub
Contents API holding data/ai_summaries.json. The failure these guard against
is quiet: a Clear that empties the panels on screen while the durable copy
keeps every briefing, so they all come back on the next restart.
"""

import base64
import json
import sys
import types

import ai_summary_store


class _Resp:
    def __init__(self, status_code, payload=None):
        self.status_code, self._payload, self.text = status_code, payload or {}, ""

    def json(self):
        return self._payload


class _GitHub:
    """One file in a pretend repo. `puts` records every commit body."""

    def __init__(self, data=None):
        self.data, self.puts = data, []

    def get(self, url, headers, timeout):
        if self.data is None:
            return _Resp(404)
        content = base64.b64encode(json.dumps(self.data).encode()).decode()
        return _Resp(200, {"sha": "abc", "content": content})

    def put(self, url, headers, json, timeout):
        self.puts.append(json)
        self.data = __import__("json").loads(base64.b64decode(json["content"]))
        return _Resp(200)


def _install(gh):
    mod = types.ModuleType("requests")
    mod.get, mod.put = gh.get, gh.put
    sys.modules["requests"] = mod


_ENTRY = {"text": "## BOJ", "ts": "2026-10-27T09:00:00+09:00", "idx": {}}


def test_clear_empties_every_briefing():
    gh = _GitHub({"summary_breaking": _ENTRY, "summary_filings": _ENTRY})
    _install(gh)
    ok, _ = ai_summary_store.clear_summaries("o/r", "tok")
    assert ok and gh.data == {}, gh.data
    assert len(gh.puts) == 1 and gh.puts[0]["sha"] == "abc", gh.puts


def test_clear_on_an_empty_store_makes_no_commit():
    """Every Clear click would otherwise be a commit to main."""
    for data in ({}, None):
        gh = _GitHub(data)
        _install(gh)
        ok, _ = ai_summary_store.clear_summaries("o/r", "tok")
        assert ok and gh.puts == [], (data, gh.puts)


def test_save_still_upserts_one_entry():
    gh = _GitHub({"summary_breaking": _ENTRY})
    _install(gh)
    ok, _ = ai_summary_store.save_summary("o/r", "tok", "summary_filings", "## TDnet",
                                          "2026-10-27T10:00:00+09:00", {1: {"url": "u"}})
    assert ok, "save failed"
    assert set(gh.data) == {"summary_breaking", "summary_filings"}, gh.data
    assert gh.data["summary_filings"]["idx"] == {"1": {"url": "u"}}, gh.data


def test_no_token_reports_failure_rather_than_pretending():
    """The Clear button warns on this, because the briefings will come back."""
    ok, msg = ai_summary_store.clear_summaries("o/r", "")
    assert not ok and "GITHUB_TOKEN" in msg, msg


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
