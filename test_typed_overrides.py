"""Tests for the forecast panel's number handling — `python test_typed_overrides.py`.

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


# ── Implied 2H ───────────────────────────────────────────────────────────
# The one figure in the forecast table that no filing contains: the panel
# derives it from the two that do. A wrong one is indistinguishable from a
# filed one on screen, so the arithmetic is checked here rather than by eye.

def _near(expected, tol=1e-9):
    """Float equality for a subtraction of two decimals — 231.22 - 104.05 is
    not exactly 127.17 in binary."""
    class _Near:
        def __eq__(self, other):
            return abs(other - expected) <= tol
    return _Near()


def test_implied_h2_is_the_year_less_the_first_half():
    assert F.implied_h2(101e9, 45.5e9) == 55.5e9
    assert F.implied_h2(231.22, 104.05) == _near(127.17)


def test_implied_h2_is_none_unless_both_halves_are_filed():
    # A missing interim must not fall through to the full year — that would
    # print the whole year's guidance in the 2H column as if it were a half.
    assert F.implied_h2(101e9, None) is None
    assert F.implied_h2(None, 45.5e9) is None
    assert F.implied_h2(None, None) is None


def test_implied_h2_keeps_a_second_half_that_shrinks():
    # Guiding a first half above the full year is a real filing pattern —
    # a loss-making back half — and the negative must survive, not be zeroed.
    assert F.implied_h2(8e9, 10e9) == -2e9
    # A company guiding zero for the year still has an answer, not a blank.
    assert F.implied_h2(0.0, 4e9) == -4e9



# ── Financial-issuer presentation profiles ───────────────────────────────
# The forecast panel lays a bank's tanshin out differently from an
# industrial's, and every one of these decisions can be wrong in a way that is
# invisible on screen — a row labelled "Net sales" holding ordinary income, a
# P/E struck across a stock split, a capital ratio that is not a capital ratio.

BASE_ROWS = [("net_sales", "Net sales", 1e9, 1, "flow"),
             ("operating_profit", "Op. profit", 1e9, 1, "flow"),
             ("ordinary_profit", "Ord. profit", 1e9, 1, "flow"),
             ("net_profit", "Net profit", 1e9, 1, "flow"),
             ("eps", "EPS ¥", 1.0, 1, "flow"),
             ("dps", "DPS ¥", 1.0, 1, "flow")]


def labels(profile):
    return [lbl for _m, lbl, *_ in F.profile_rows(profile, BASE_ROWS)]


def test_bank_drops_operating_profit_and_renames_the_top_line():
    # MUFG files 経常収益 and no operating profit at all. Showing "Net sales"
    # over its ordinary income is the mislabelling this whole profile exists
    # to stop, and a permanently empty Op. profit row reads as missing data.
    assert labels("bank") == ["Ordinary income", "Ord. profit", "Profit attrib.",
                              "EPS ¥", "DPS ¥"]
    assert labels("financial")[0] == "Ordinary income"
    assert "Op. profit" not in labels("financial")


def test_securities_and_leasing_keep_their_operating_profit():
    # Daiwa Securities, ORIX and Credit Saison all file one. Hiding the row for
    # everything merely tagged "financial" would blank a real line.
    assert "Operating profit" in labels("securities") or "Op. profit" in labels("securities")
    assert labels("securities")[0] == "Operating revenue"
    assert labels("other_finance")[0] == "Operating revenue"


def test_ifrs_insurer_has_no_ordinary_profit_row():
    # Ordinary profit is a JGAAP concept. An IFRS insurer's equivalent line is
    # profit before tax, and its top line is insurance revenue.
    lab = labels("insurer_ifrs")
    assert "Ord. profit" not in lab
    assert lab[0] == "Insurance revenue"
    assert "Profit before tax" in lab


def test_general_profile_is_untouched():
    assert labels("general") == [lbl for _m, lbl, *_ in BASE_ROWS]


def test_profile_needs_both_a_sector_hint_and_the_data_shape():
    # J-Quants carries no operating profit for IFRS filers either, so the
    # shape test alone makes Mitsui & Co. and SoftBank look exactly like a
    # bank. Neither signal is trusted on its own.
    assert F.profile_for(has_operating_profit=False, has_ordinary_profit=True,
                         sector_hint="Industrials") == "general"
    assert F.profile_for(has_operating_profit=False, has_ordinary_profit=True,
                         sector_hint="Financial Services") == "financial"
    # A financial that does file an operating profit is on the general shape.
    assert F.profile_for(has_operating_profit=True, has_ordinary_profit=True,
                         sector_hint="Financial Services") == "general"


def test_financial_without_ordinary_profit_is_not_relabelled():
    # Sompo and Nomura file no ordinary profit, so they are not on the JGAAP
    # bank shape — the row set is trimmed, but the top line is not renamed to
    # "Ordinary income", which would be a guess dressed as a fact.
    prof = F.profile_for(has_operating_profit=False, has_ordinary_profit=False,
                         sector_hint="Financial Services")
    assert prof == "financial_other"
    assert labels(prof)[0] == "Revenue"
    assert "Ord. profit" not in labels(prof)


def test_sector33_code_beats_the_coarse_hint():
    assert F.profile_for(sector33="7050", sector_hint="Industrials") == "bank"
    assert F.profile_for(sector33="7150", sector_hint="") == "insurer"
    assert F.profile_for(sector33="7100") == "securities"
    assert F.profile_for(sector33="7200") == "other_finance"
    # The filing's own standard refines it: an IFRS insurer is not a JGAAP one.
    assert F.profile_for(sector33="7150",
                         doc_type="FYFinancialStatements_Consolidated_IFRS") == "insurer_ifrs"


def test_enterprise_value_is_not_computed_for_financials():
    # A bank's "debt" is its deposit and funding base; net debt against its
    # reserve balance is a number with no interpretation. MUFG's came out at
    # ¥18.3tn and sat in a tile beside the P/E.
    fundrow = {"bps": 1973.31, "debt": 108.4e12, "cash": 90.0e12, "ebitda": 1e12}
    bank = F.compute_valuations(3684.0, 11.87e9, 43.7e12, fundrow, {}, "bank")
    assert bank["net_debt"] is None
    assert bank["ev"] is None
    assert bank["ev_ebitda"] is None
    # An industrial keeps both.
    gen = F.compute_valuations(3684.0, 11.87e9, 43.7e12, fundrow, {}, "general")
    assert gen["net_debt"] == _near(18.4e12, 0.05)
    assert gen["ev_ebitda"] is not None


def test_roe_and_payout_replace_them():
    fundrow = {"bps": 1973.31}
    slots = {("eps", "fy1", "company"): 213.17, ("dps", "fy1", "company"): 86.0}
    v = F.compute_valuations(3684.0, 11.87e9, 43.7e12, fundrow, slots, "bank")
    assert v["roe_fy1_company"] == _near(0.10803, 0.01)
    assert v["payout_fy1_company"] == _near(0.40343, 0.01)


# ── Stock splits ─────────────────────────────────────────────────────────

def test_split_is_detected_from_guidance_against_the_filed_share_count():
    # SMFG, live: FY3/27 guidance of ¥1,700,000m and EPS ¥223.75 imply 7.598bn
    # shares against a filed 3.827bn. Its panel was showing P/E 31.2x for a
    # company on about 15.6x, and a +110% guidance-vs-street chip that was
    # nothing but the split.
    assert F.detect_split(1.70e12, 223.75, 3.8275e9) == 2.0


def test_no_split_is_reported_when_the_share_counts_agree():
    # MUFG: guidance net profit and EPS struck on the filed share count.
    assert F.detect_split(2.4272e12, 213.17, 11.3853e9) is None
    # And a company whose numbers are simply missing is not a split.
    assert F.detect_split(None, 223.75, 3.8e9) is None
    assert F.detect_split(1.7e12, None, 3.8e9) is None
    assert F.detect_split(1.7e12, 223.75, None) is None
    assert F.detect_split(1.7e12, 0.0, 3.8e9) is None


def test_a_detected_split_suppresses_every_per_share_multiple():
    # Wrong by exactly the split factor is the worst kind of wrong: it looks
    # like a number. Better to show nothing and say why.
    fundrow = {"bps": 4135.71}
    slots = {("eps", "fy1", "company"): 223.75, ("dps", "fy1", "company"): 135.0}
    v = F.compute_valuations(6971.0, 3.8275e9, 26.7e12, fundrow, slots, "financial", 2.0)
    assert v["pe_fy1_company"] is None
    assert v["yield_fy1_company"] is None
    assert v["roe_fy1_company"] is None
    assert v["split_factor"] == 2.0
    # Without the split flag the same inputs give the wrong 31.2x that was on
    # screen — which is what makes the suppression worth testing.
    unguarded = F.compute_valuations(6971.0, 3.8275e9, 26.7e12, fundrow, slots, "financial")
    assert unguarded["pe_fy1_company"] == _near(31.15, 0.01)


# ── Street revenue that is measuring something else ──────────────────────

def test_bank_street_revenue_is_flagged_as_a_different_measure():
    # Yahoo publishes net revenue for a bank; the tanshin files gross ordinary
    # income. MUFG 14.62tn against 6.63tn, SMFG 10.79 against 5.24.
    assert F.revenue_basis_mismatch(14.62e12, 6.63e12)
    assert F.revenue_basis_mismatch(10.79e12, 5.24e12)


def test_insurer_street_revenue_is_left_alone():
    # Insurers do not have the problem: Tokio Marine 8.87 against 8.77,
    # Dai-ichi Life 11.31 against 11.23. Nor does a growing industrial.
    assert not F.revenue_basis_mismatch(8.87e12, 8.77e12)
    assert not F.revenue_basis_mismatch(11.31e12, 11.23e12)
    assert not F.revenue_basis_mismatch(1.0e12, 0.86e12)
    assert not F.revenue_basis_mismatch(None, 5e12)
    assert not F.revenue_basis_mismatch(5e12, None)


# ── Capital ratios: the accounting one only, and only when it checks out ──

def test_equity_to_assets_resolves_the_filed_scale_against_the_balance_sheet():
    # J-Quants' encoding of 5.2% is not documented here, so it is checked
    # rather than assumed: whichever reading agrees with equity/total assets
    # from the same record is the right one.
    assert F.equity_to_assets(5.2, 24.18e12, 433.9e12) == _near(0.052, 0.001)
    assert F.equity_to_assets(0.052, 24.18e12, 433.9e12) == _near(0.052, 0.001)
    assert F.equity_to_assets(24.3, 8.05e12, 33.0e12) == _near(0.243, 0.001)


def test_equity_to_assets_is_dropped_when_it_agrees_with_neither_reading():
    # Better no number than a wrong one — the whole reason this is checked.
    assert F.equity_to_assets(85.0, 24.18e12, 433.9e12) is None
    assert F.equity_to_assets(None, None, None) is None
    # With no balance sheet to check against, a plausible fraction still passes
    # and an implausible one does not.
    assert F.equity_to_assets(5.2, None, None) == _near(0.052, 0.001)
    assert F.equity_to_assets(520.0, None, None) is None


def test_equity_to_assets_is_never_derived_when_none_was_filed():
    # equity/total_assets is a *different* ratio: it carries non-controlling
    # interests, which the filed one excludes — 5.5% against MUFG's filed 5.2%.
    # On a figure sitting this close to the subject of capital adequacy, that
    # is not a substitute, so the tile stays empty until the real one arrives.
    assert F.equity_to_assets(None, 24.18e12, 433.9e12) is None


def test_payout_is_suppressed_across_a_split_too():
    # SMFG files a ¥90 interim before the split and a ¥45 final after it, and
    # J-Quants has no annual total to use instead — so the summed ¥135 over a
    # restated EPS is a ratio of two different bases.
    fundrow = {"bps": 4135.71}
    slots = {("eps", "fy1", "company"): 223.75, ("dps", "fy1", "company"): 135.0}
    v = F.compute_valuations(6971.0, 3.8275e9, 26.7e12, fundrow, slots, "financial", 2.0)
    assert v["payout_fy1_company"] is None


def test_no_regulatory_capital_metric_exists_anywhere():
    # Deliberate, and load-bearing: no feed this app reads carries CET1, the
    # solvency margin or ESR, and a proxied capital ratio is worse than none.
    # If a future change adds one, this test is where the argument happens.
    import consensus_vision as V
    banned = ("cet1", "tier1", "tier_1", "solvency", "esr", "capital_adequacy",
              "leverage_ratio")
    for name in banned:
        assert name not in V._METRICS, name
        assert not any(name in m for m in V._METRICS), name
        assert not any(name in c for c in F._JQ_ALIASES), name
    assert "not a regulatory capital ratio" in F.CAPITAL_DISCLAIMER


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
