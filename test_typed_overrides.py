"""Tests for typed forecast overrides — `python test_typed_overrides.py`.

No test framework and no network: plan_typed_overrides is pure, and
save_manual_overrides is exercised against a stubbed GitHub contents API. The
two together are what stands between a typed number and the consensus panel,
and both have failure modes that are invisible in the UI — a save that quietly
rewrites every collected row as an override, or a delete that leaves a
value-less key behind — so they are checked here rather than by clicking.
"""

import base64
import json
import sys
import types

import fundamentals as F

TODAY = "2026-08-27"

# What the panel shows for a typical name: collected EPS and net sales, company
# guidance for operating profit, and no street operating profit at all — Yahoo
# publishes only earnings and revenue estimate frames.
CURRENT = {
    ("eps", "FY2027", "consensus"):             {"value": 763.13, "source": "screenshot:IMG.png"},
    ("net_sales", "FY2027", "consensus"):       {"value": 1.3e12,  "source": "yfinance"},
    ("net_sales", "FY2027", "company"):         {"value": 1.29e12, "source": "jquants"},
    ("operating_profit", "FY2027", "company"):  {"value": 1.5e11,  "source": "jquants"},
}
SEED = list(CURRENT)
MANUAL = {"eps|FY2027|consensus": {"value": 763.13}}


def rows(*specs):
    return [{"metric": m, "fiscal_year": y, "basis": b, "value": v} for m, y, b, v in specs]


UNTOUCHED = rows(("eps", "FY2027", "consensus", 763.13),
                 ("net_sales", "FY2027", "consensus", 1300.0),
                 ("net_sales", "FY2027", "company", 1290.0),
                 ("operating_profit", "FY2027", "company", 150.0))


def plan(grid, seed=SEED):
    return F.plan_typed_overrides(grid, seed, CURRENT, MANUAL, TODAY)


def test_untouched_grid_writes_nothing():
    """The ¥bn round trip is lossy in the last bits. If that read as an edit,
    every save would freeze the whole panel against future collector runs."""
    assert plan(UNTOUCHED) == ({}, [], [])


def test_typed_street_operating_profit():
    """The case the feature exists for: a street number Yahoo cannot supply."""
    entries, remove, notes = plan(UNTOUCHED + rows(
        ("operating_profit", "FY2027", "consensus", 153.4)))
    assert (remove, notes) == ([], [])
    assert entries == {"operating_profit|FY2027|consensus": {
        "value": 153.4e9, "unit": "jpy_abs", "source": "typed", "as_of": TODAY}}


def test_units_follow_the_metric():
    """¥bn for absolute figures, yen per share for EPS and DPS — the grid prints
    them that way, so a value stored in the wrong one is out by a billion."""
    entries, _, _ = plan(rows(("eps", "FY2027", "consensus", 780.0),
                              ("dps", "FY2028", "company", 42.5),
                              ("net_profit", "FY2028", "consensus", 90.0)))
    assert entries["eps|FY2027|consensus"]["value"] == 780.0
    assert entries["eps|FY2027|consensus"]["unit"] == "jpy"
    assert entries["dps|FY2028|company"]["value"] == 42.5
    assert entries["net_profit|FY2028|consensus"]["value"] == 9e10
    assert entries["net_profit|FY2028|consensus"]["unit"] == "jpy_abs"


def test_deleting_an_override_clears_it():
    entries, remove, notes = plan(rows(("net_sales", "FY2027", "consensus", 1300.0),
                                       ("net_sales", "FY2027", "company", 1290.0)))
    assert remove == ["eps|FY2027|consensus"]
    # Two collected rows also went; that cannot be done and must not look done.
    assert len(notes) == 1 and "cannot be removed" in notes[0]


def test_bad_years_and_blanks_are_skipped_with_a_reason():
    """A mislabelled year lands a number in a column that looks right and is a
    year out, so it is refused rather than guessed at."""
    entries, _, notes = plan(rows(("eps", "2027", "consensus", 800.0),
                                  ("eps", "FY27", "consensus", 800.0),
                                  ("dps", "FY2027", "company", None),
                                  ("net_profit", "fy2028", "consensus", 90.0)), seed=[])
    assert list(entries) == ["net_profit|FY2028|consensus"]   # lowercase fy normalised
    assert sum("not a fiscal year" in n for n in notes) == 2
    assert sum("no value" in n for n in notes) == 1


def test_unmapped_rows_are_ignored():
    """A half-filled new row is a row in progress, not an instruction."""
    assert plan(rows(("", "FY2027", "consensus", 5.0),
                     ("eps", "FY2027", "", 5.0),
                     ("bogus", "FY2027", "consensus", 5.0)), seed=[]) == ({}, [], [])


def test_interim_guidance_is_editable():
    entries, _, _ = plan(rows(("net_sales", "FY2027", "company_h1", 600.0)), seed=[])
    assert entries["net_sales|FY2027|company_h1"]["value"] == 6e11


def test_override_reaches_the_panel_marked_typed():
    merged = F.apply_manual_overrides(CURRENT, {
        "operating_profit|FY2027|consensus": {"value": 1.534e11, "source": "typed"}})
    cell = merged[("operating_profit", "FY2027", "consensus")]
    assert cell["value"] == 1.534e11 and cell["source"] == "typed"


# ── save_manual_overrides, against a stubbed contents API ────────────────

class _Resp:
    def __init__(self, status, payload=None, text=""):
        self.status_code, self._payload, self.text = status, payload, text

    def json(self):
        return self._payload


def _stub_github(store: dict) -> list:
    """Install a fake `requests` for fundamentals' lazy import. Returns the list
    the PUT bodies land in."""
    puts = []

    def get(url, headers=None, timeout=None):
        return _Resp(200, {"sha": "sha1", "content": base64.b64encode(
            json.dumps(store).encode()).decode()})

    def put(url, headers=None, json=None, timeout=None):
        puts.append(json)
        return _Resp(200)

    module = types.ModuleType("requests")
    module.get, module.put = get, put
    sys.modules["requests"] = module
    return puts


def _written(puts):
    return json.loads(base64.b64decode(puts[-1]["content"]))


def test_save_adds_and_removes_in_one_commit():
    puts = _stub_github({"6504": dict(MANUAL), "9999": {"dps|FY2027|company": {"value": 10.0}}})
    ok, msg = F.save_manual_overrides(
        "o/r", "tok", "6504",
        {"operating_profit|FY2027|consensus": {"value": 1.534e11, "source": "typed"}},
        remove=["eps|FY2027|consensus"])
    data = _written(puts)
    assert ok and "Saved 1" in msg and "Cleared 1" in msg
    assert sorted(data["6504"]) == ["operating_profit|FY2027|consensus"]
    assert "9999" in data, "another company's overrides must survive the write"


def test_clearing_the_last_override_leaves_no_empty_block():
    puts = _stub_github({"9999": {"dps|FY2027|company": {"value": 10.0}}})
    ok, _ = F.save_manual_overrides("o/r", "tok", "9999", {}, remove=["dps|FY2027|company"])
    assert ok and "9999" not in _written(puts)


def test_screenshot_flow_still_calls_it_without_remove():
    puts = _stub_github({})
    ok, msg = F.save_manual_overrides("o/r", "tok", "6504",
                                      {"eps|FY2028|consensus": {"value": 828.38}})
    assert ok and msg == "Saved 1 override(s)."
    assert "eps|FY2028|consensus" in _written(puts)["6504"]


def test_no_token_keeps_the_value_for_the_session_only():
    ok, msg = F.save_manual_overrides("o/r", None, "6504", {"eps|FY2028|consensus": {"value": 1}})
    assert not ok and "session only" in msg


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
