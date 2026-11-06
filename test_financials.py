"""Tests for financials.py — `python test_financials.py` (no network)."""

import pandas as pd

import financials as FIN


def _rec(per, fy_end, disc, **kw):
    r = {"CurPerType": per, "CurFYEn": fy_end, "DiscDate": disc}
    r.update({k: str(v) for k, v in kw.items()})
    return r


RECORDS = [
    _rec("FY", "2025-03-31", "2025-05-10", Eq=100e9, TA=200e9, EqAR=0.45, CashEq=50e9, CFO=20e9, CFI=-8e9),
    _rec("2Q", "2026-03-31", "2025-11-10", Eq=105e9, TA=205e9, EqAR=0.46, CashEq=52e9, CFO=9e9),
    _rec("FY", "2026-03-31", "2026-05-10", Eq=110e9, TA=210e9, EqAR=0.47, CashEq=60e9, CFO=25e9, CFI=-10e9),
    _rec("1Q", "2027-03-31", "2026-08-10", Eq=112e9, TA=212e9, EqAR=0.47, CashEq=61e9),
]


def test_jq_rows_take_full_years_and_halves():
    rows = {(r["fy"], r["period"]): r for r in FIN.jq_rows(RECORDS)}
    assert set(rows) == {("FY2025", "FY"), ("FY2026", "FY"), ("FY2026", "H1")}
    fy26 = rows[("FY2026", "FY")]
    assert fy26["cfo"] == 25e9 and fy26["cash"] == 60e9 and fy26["cfi"] == -10e9
    # Shareholders' equity is the filed ratio x total assets, not net assets.
    assert abs(fy26["sh_equity"] - 0.47 * 210e9) < 1
    assert rows[("FY2026", "H1")]["cfo"] == 9e9


def test_yahoo_capex_is_stored_positive_and_labelled_by_year_end():
    cf = pd.DataFrame({pd.Timestamp("2026-03-31"): [24e9, -7e9, 5e9],
                       pd.Timestamp("2025-03-31"): [19e9, -6e9, 4.5e9]},
                      index=["Operating Cash Flow", "Capital Expenditure",
                             "Depreciation And Amortization"])
    bs = pd.DataFrame({pd.Timestamp("2026-03-31"): [12e9]}, index=["Total Debt"])
    rows = {r["fy"]: r for r in FIN.yahoo_rows(cf, bs)}
    assert rows["FY2026"]["capex"] == 7e9
    assert rows["FY2026"]["debt"] == 12e9
    assert rows["FY2025"]["dep_amort"] == 4.5e9


def test_merge_prefers_filed_and_fills_from_yahoo():
    jq = FIN.jq_rows(RECORDS)
    yh = [{"fy": "FY2026", "period": "FY", "period_end": "2026-03-31",
           "cfo": 99e9, "capex": 7e9, "debt": 12e9,
           "_src": {"cfo": "yfinance:x", "capex": "yfinance:Capital Expenditure",
                    "debt": "yfinance:Total Debt"}}]
    rows = {(r["fy"], r["period"]): r for r in FIN.merge("9999", jq, yh, "2026-11-06")}
    r = rows[("FY2026", "FY")]
    assert r["cfo"] == 25e9          # filed beats Yahoo
    assert r["capex"] == 7e9 and r["debt"] == 12e9
    assert "capex=yfinance" in r["sources"] and "cfo=jquants" in r["sources"]


def test_typed_values_win_and_are_marked():
    hist = {("FY2026", "FY"): {"fy": "FY2026", "period": "FY", "capex": 7e9}}
    latest = {"cash": 60e9, "debt": 12e9}
    manual = {"capex|FY2026|fin": {"value": 8e9}, "debt|latest|fin": {"value": 10e9},
              "operating_profit|FY2027|company": {"value": 1}}
    h, l, typed = FIN.apply_manual(hist, latest, manual)
    assert h[("FY2026", "FY")]["capex"] == 8e9
    assert l["debt"] == 10e9
    assert ("FY2026", "FY", "capex") in typed and ("latest", "", "debt") in typed
    assert len(typed) == 2


def test_summary_net_cash_fcf_and_roe():
    hist = {("FY2025", "FY"): {"sh_equity": 90e9, "cfo": 20e9, "capex": 6e9},
            ("FY2026", "FY"): {"sh_equity": 110e9, "cfo": 25e9, "capex": 7e9}}
    s = FIN.summary(hist, {"cash": 60e9, "debt": 12e9, "sh_equity": 100e9}, 480e9,
                    {"FY2026": 15e9}, {"FY2026": 100e9})
    assert s["net_cash"] == 48e9
    assert abs(s["nc_to_equity"] - 0.48) < 1e-9 and abs(s["nc_to_mcap"] - 0.1) < 1e-9
    y = s["years"][-1]
    assert y["fcf"] == 18e9 and abs(y["roe"] - 15 / 100) < 1e-9
    assert abs(y["fcf_margin"] - 0.18) < 1e-9
    assert abs(s["fcf_yield"] - 18 / 480) < 1e-9
    # No prior year -> no average equity -> no ROE rather than a period-end one.
    assert s["years"][0]["roe"] is None


def test_fcf_needs_capex_not_investing_cash_flow():
    assert FIN.fcf({"cfo": 10e9, "cfi": -3e9}) is None


def test_grid_plan_types_changes_and_clears_overrides():
    before = {("capex", "FY2026"): 7.0, ("debt", "Latest"): 12.0, ("cfo", "1H FY2027"): None}
    after = {("capex", "FY2026"): 8.0, ("debt", "Latest"): None, ("cfo", "1H FY2027"): 9.5}
    manual_now = {"debt|latest|fin": {"value": 12e9}}
    entries, removed, ignored = FIN.plan_grid(before, after, manual_now, "2026-11-06")
    assert entries["capex|FY2026|fin"]["value"] == 8e9
    assert entries["cfo|FY2027|fin_h1"]["value"] == 9.5e9
    assert removed == ["debt|latest|fin"] and ignored == []
    # Unchanged cells write nothing.
    assert FIN.plan_grid(before, before, {}, "x") == ({}, [], [])


def test_first_half_takes_its_balance_sheet_and_latest_flags_a_flow():
    # The bug: cash / debt / equity typed under 1H were dropped without a word.
    after = {("cash", "1H FY2027"): 150.0, ("debt", "1H FY2027"): 20.0,
             ("sh_equity", "1H FY2027"): 600.0, ("capex", "Latest"): 5.0}
    entries, removed, ignored = FIN.plan_grid({}, after, {}, "x")
    assert set(entries) == {"cash|FY2027|fin_h1", "debt|FY2027|fin_h1",
                            "sh_equity|FY2027|fin_h1"}
    assert ignored == [("capex", "Latest")]
    assert FIN.describe_key("cash|FY2027|fin_h1") == "Cash & equivalents 1H FY2027"


def test_typed_first_half_shows_in_summary():
    hist = {("FY2026", "FY"): {"sh_equity": 500e9, "cfo": 100e9, "capex": 40e9}}
    manual = {"cash|FY2027|fin_h1": {"value": 150e9}, "debt|FY2027|fin_h1": {"value": 20e9},
              "sh_equity|FY2027|fin_h1": {"value": 600e9},
              "cfo|FY2027|fin_h1": {"value": 82.8e9}, "capex|FY2027|fin_h1": {"value": 30.3e9},
              "dep_amort|FY2027|fin_h1": {"value": 30.3e9}}
    h, l, typed = FIN.apply_manual(hist, {}, manual)
    s = FIN.summary(h, l, 1e12, {})
    h1 = s["h1"]
    assert h1["fy"] == "FY2027" and h1["net_cash"] == 130e9 and h1["dep_amort"] == 30.3e9
    assert abs(h1["fcf"] - 52.5e9) < 1 and abs(h1["nc_to_equity"] - 130 / 600) < 1e-9
    assert ("FY2027", "H1", "cash") in typed


if __name__ == "__main__":
    import sys
    fails = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"ok   {name}")
            except Exception as exc:
                fails += 1
                print(f"FAIL {name}: {exc!r}")
    sys.exit(1 if fails else 0)
