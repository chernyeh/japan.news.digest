"""
fundamentals.py — company guidance, street consensus, and price-based
valuation multiples for the Research tab's forecast panel.

Three sources, in order of preference:

  1. J-Quants /fins/summary — the company's own guidance (the 会社予想 that
     Japanese equities actually trade off) plus the balance-sheet items it
     carries. This is filed data, so it is preferred wherever it exists.
  2. Yahoo Finance via yfinance — analyst consensus, and the balance-sheet
     items J-Quants does not carry (interest-bearing debt, cash, D&A).
  3. The user, by hand or by screenshot (see data/consensus_manual.json) —
     always wins over both, because they are looking at a terminal we cannot.

Everything is normalised to **yen** internally; formatting happens at the
render edge only, so the spreadsheet exports carry raw numbers.

Field names for 1 are resolved through _JQ_ALIASES rather than hardcoded.
The V2 API abbreviates its columns (Eq, TA, BPS) and the repo has no captured
response to check against, so the collector probes the aliases and reports
which key it actually matched — see `collect_consensus.py --probe`. A guessed
schema that silently yields None everywhere is the failure mode this avoids.

Not every issuer files the same income statement. A Japanese bank files
ordinary income and ordinary profit and no operating profit at all; an IFRS
insurer files insurance revenue and profit before tax. J-Quants normalises
those into the same Sales / OdP / NP slots, which means the numbers are right
and the English labels are not. `profile_for` and PROFILES below resolve a
presentation per company so the panel names each line the way its filer does —
a rendering decision only, so the stored data model is unchanged.

Regulatory capital — Basel CET1, the solvency margin, ESR — is carried by none
of the three sources above and is therefore not collected, derived, proxied or
displayed anywhere. See CAPITAL_DISCLAIMER.
"""

import csv
import json
import math
import os
import re

CONSENSUS_PATH = "data/consensus.csv"
FUNDAMENTALS_PATH = "data/fundamentals.csv"
MANUAL_PATH = "data/consensus_manual.json"
# The list of companies the forecast panel collects for. build_universe.py
# writes it from J-Quants' own scale categories (TOPIX 1000 by default); the
# JPX-Nikkei 400 file is the fallback for a checkout that predates it.
UNIVERSE_PATH = "data/universe.csv"
UNIVERSE_FALLBACK_PATH = "data/jpxnikkei400.csv"
RUN_MANIFEST_PATH = "data/consensus_run.json"
# How company guidance for a given fiscal year has moved since it was first
# filed. Japanese issuers are known for guiding conservatively and revising up,
# and whether *this* management does that is a fact about them, not a vibe —
# it is in the filing history J-Quants already returns.
GUIDANCE_HISTORY_PATH = "data/guidance_history.csv"

_UA = "japan-news-digest-fundamentals"

# Long-format fact table: one row per company x metric x fiscal year x basis.
# Long rather than wide so it pivots directly in a spreadsheet and survives new
# metrics being added without a schema migration.
CONSENSUS_COLUMNS = ["code", "name", "metric", "fy", "basis",
                     "value", "unit", "source", "as_of"]

# Point-in-time snapshot, one row per company — the inputs P/B and EV/EBITDA
# need, which are not per-fiscal-year forecasts.
#
# equity_to_assets, cfo and doc_type were added for the financial-issuer
# profiles (see PROFILES below). doc_type is the filing's own declaration of
# which accounting standard it is drawn up under, which is what tells a bank
# from a general issuer and an IFRS insurer from a JGAAP one.
FUNDAMENTALS_COLUMNS = ["code", "name", "shares", "bps", "equity", "total_assets",
                        "debt", "cash", "ebitda", "dep_amort", "op_actual",
                        "equity_to_assets", "cfo", "doc_type",
                        "fy_end", "fy_end_next", "ebitda_basis", "sources", "as_of"]

# One row per company x fiscal year x metric. Deliberately a *summary* of the
# revision path rather than every filed value: the full sequence is always
# re-derivable from /fins/summary, which returns a company's whole filing
# history, so storing it would multiply the file size for nothing. First and
# latest plus a count is what answers "has this been raised, how often, and by
# how much".
GUIDANCE_HISTORY_COLUMNS = ["code", "name", "fy", "metric",
                            "first_value", "first_as_of",
                            "latest_value", "latest_as_of", "revisions"]

# Ordinary (recurring) profit sits between operating and net profit in every
# Japanese earnings table and is the line the market quotes; /fins/summary has
# carried it as OdP / FOdP / NxFOdP all along.
METRICS = ("net_sales", "operating_profit", "ordinary_profit", "net_profit", "eps", "dps")

# Interim guidance. Japanese issuers forecast the first half separately from
# the year, and /fins/summary files it under its own 2Q family of keys.
INTERIM_METRICS = ("net_sales", "operating_profit", "ordinary_profit",
                   "net_profit", "eps")

# The four instalments an annual dividend forecast is filed as. There is a
# FDivTotalAnn for the current year but *no* NxFDivTotalAnn, so a next-year
# annual dividend only exists as the sum of these — which is why the DPS row
# was empty for the companies that file no current-year total either.
DIV_QUARTERS = ("div_q1", "div_q2", "div_q3", "div_fy")

_MONTH_ABBR = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def dps_annual(pick, prefix: str = "f_"):
    """Annual dividend per share for one forecast horizon, or None.

    `pick` is a callable taking a concept name and returning (value, key) —
    normally the caller's newest-across-filings reader, so a dividend revision
    filed after the results is still picked up.

    The filed annual total is used where there is one. Where there is not, the
    four instalments are summed: /fins/summary has no next-year annual total at
    all, only NxFDiv1Q…NxFDivFY, so a company's next-year dividend forecast is
    invisible unless it is added up. A partial set still sums — a company
    paying nothing at Q1 and Q3 files those as 0, not as blanks — but a set
    that is entirely empty returns None rather than a spurious zero."""
    total, _ = pick(f"{prefix}dps")
    if total is not None:
        return total
    parts = [pick(f"{prefix}{q}")[0] for q in DIV_QUARTERS]  # "" prefix = the actuals
    present = [p for p in parts if p is not None]
    return sum(present) if present else None


def fy_end_short(fy_end: str, fallback: str = "") -> str:
    """"2027-03-31" -> "FYE 3/27". The compact form of the same fact the long
    note spells out: which March (or December, or February) a column's fiscal
    year closes in. `fallback` is shown when no date is known — normally the
    plain FY label."""
    month = fy_end_month(fy_end)
    if not month:
        return fallback
    return f"FYE {month}/{str(fy_end)[2:4]}"


def fy_end_dates(fy_labels, fy_end: str, fy_end_next: str) -> dict:
    """{fy_label: iso_date} for the columns on screen.

    Anchored by label, not by position: a filed year end dated 2027-03-31 is
    FY2027's, wherever FY2027 happens to sit among the columns. Positional
    anchoring broke the moment a reported year was added to the left of the
    forecasts and every column's year end shifted by one.

    Only two year ends are ever filed — the year guided and the one after it.
    Other columns exist because the street, or a screenshot of a terminal,
    reaches further than the company does, and because the year just reported
    sits alongside. Their closes are projected from a filed one by keeping the
    month and day. Projecting the *month* is what would be unsafe, and that
    never happens here."""
    out = {}
    anchors = [d for d in (fy_end, fy_end_next) if d and len(d) >= 10]
    for date in anchors:
        out[f"FY{date[:4]}"] = date
    if not anchors:
        return out
    base = anchors[-1]
    for label in fy_labels:
        if label in out or not label.startswith("FY"):
            continue
        try:
            step = int(label[2:]) - int(base[:4])
        except ValueError:
            continue
        out[label] = f"{int(base[:4]) + step:04d}{base[4:]}"
    return out


def fy_end_month(fy_end: str):
    """Month a fiscal year closes in, from a filed "2027-03-31". None when the
    date is missing or malformed — never a default, because a guessed March on
    a December-close company is worse than showing nothing."""
    try:
        month = int(str(fy_end)[5:7])
    except (TypeError, ValueError):
        return None
    return month if 1 <= month <= 12 else None


def fy_end_note(fy_end: str) -> str:
    """"2027-03-31" -> "yr to Mar 2027". Japanese fiscal years close in March
    for most issuers but in December, February or elsewhere for a meaningful
    minority, and "FY2027" alone does not say which — so every forecast column
    carries the month it actually runs to."""
    month = fy_end_month(fy_end)
    if not month:
        return ""
    return f"yr to {_MONTH_ABBR[month]} {str(fy_end)[:4]}"

# Per-share metrics are in yen each; the rest are absolute yen.
_PER_SHARE = {"eps", "dps"}


# ── Presentation profiles ────────────────────────────────────────────────
# A Japanese bank does not file "net sales" and does not file an operating
# profit. It files 経常収益 (ordinary income) and 経常利益 (ordinary profit),
# and J-Quants normalises those into the Sales and OdP slots — the right
# numbers under the wrong English names. An IFRS insurer files different lines
# again. So the panel resolves a *presentation profile* per company, which
# decides the row labels, which rows appear at all, and which multiples mean
# anything. The stored metric keys never change: this is a rendering decision,
# so consensus.csv, the manual override store and the exports are untouched.

# Deliberately absent from every profile: CET1, total capital, leverage ratio,
# solvency margin, ESR, EEV. None of them is carried by J-Quants, by Yahoo, or
# by the EDINET/TDnet indexes — they live in the Basel Pillar 3 disclosure
# (自己資本の充実の状況), the 有価証券報告書 and IR decks, which this app does
# not read. A capital ratio that is estimated, proxied or derived from the
# accounting equity ratio is worse than no capital ratio, so there is no row,
# no tile and no screenshot field for one. The equity-to-assets ratio below is
# the accounting figure the company itself files, labelled as such.
CAPITAL_DISCLAIMER = (
    "Equity/assets is the accounting ratio filed on the tanshin "
    "(自己資本比率) — shareholders' equity over total assets. It is not a "
    "regulatory capital ratio: Basel CET1, solvency margin and ESR are "
    "different measures on different denominators, none of them available "
    "from any data source this app reads."
)

# label overrides per profile; a metric mapped to None is hidden entirely.
_PROFILE_LABELS = {
    "general": {},
    "general_ifrs": {"net_sales": "Revenue", "ordinary_profit": None,
                     "net_profit": "Profit attrib."},
    # Banks and JGAAP insurers share an income statement shape: ordinary
    # income at the top, no operating profit, ordinary profit above the line.
    "financial": {"net_sales": "Ordinary income", "operating_profit": None},
    # A financial issuer that files no ordinary profit is not on the JGAAP
    # basis — ordinary profit is a JGAAP-only concept — but which basis it is
    # on cannot be read off the numbers. So the operating-profit row is
    # dropped, which is safe, and the top line is left generically named
    # rather than called ordinary income, which would be a guess.
    "financial_other": {"net_sales": "Revenue", "operating_profit": None,
                        "ordinary_profit": None, "net_profit": "Net income attrib."},
    "bank": {"net_sales": "Ordinary income", "operating_profit": None,
             "net_profit": "Profit attrib."},
    "insurer": {"net_sales": "Ordinary income", "operating_profit": None,
                "net_profit": "Net income"},
    "insurer_ifrs": {"net_sales": "Insurance revenue", "operating_profit": None,
                     "ordinary_profit": "Profit before tax",
                     "net_profit": "Net income attrib."},
    # Securities houses and leasing/consumer-credit companies really do file an
    # operating revenue and an operating profit, so their rows stay.
    "securities": {"net_sales": "Operating revenue"},
    "other_finance": {"net_sales": "Operating revenue"},
}

# Profiles whose balance sheet makes enterprise value meaningless. A bank's
# "debt" is its funding base and its "cash" is its reserve balance; subtracting
# one from the other produces a number with no interpretation, and dividing an
# insurer's EV by a vendor-supplied EBITDA produces another.
NO_ENTERPRISE_VALUE = frozenset({"financial", "financial_other", "bank", "insurer",
                                 "insurer_ifrs", "securities", "other_finance"})

# Profiles where the top line is an ordinary/insurance income figure that
# yfinance's revenue estimate does not measure the same way. Checked against
# the filed actual before anything is suppressed — see revenue_basis_mismatch.
FINANCIAL_PROFILES = NO_ENTERPRISE_VALUE

# JPX 33-sector codes. Filed classification, so there is nothing to guess
# where /listed/info has been collected; build_universe.py writes it.
_SECTOR33_PROFILE = {"7050": "bank", "7100": "securities",
                     "7150": "insurer", "7200": "other_finance"}
_SECTOR_NAME_PROFILE = (
    ("銀行業", "bank"), ("保険業", "insurer"),
    ("証券", "securities"), ("その他金融", "other_finance"),
    ("bank", "bank"), ("insurance", "insurer"),
    ("securities", "securities"), ("other financing", "other_finance"),
)


def profile_for(sector33: str = "", sector_name: str = "", doc_type: str = "",
                has_operating_profit: bool = True,
                has_ordinary_profit: bool = False,
                sector_hint: str = "") -> str:
    """Which presentation profile a company's forecast table should use.

    Resolution order, most authoritative first:

      1. The JPX 33-sector code or name, which is filed classification and
         names the industry exactly — bank, insurer, securities house,
         other finance.
      2. Failing that, a coarse sector hint (`sector_hint`, e.g. the
         "Financial Services" grouping in metadata.csv) *corroborated by the
         shape of the data*. Neither is trusted alone. The shape test on its
         own would sweep in every IFRS-reporting industrial, because J-Quants
         carries no operating profit for those either — Mitsui & Co. and
         SoftBank Group both look exactly like a bank to it. The sector hint
         on its own is too coarse to say which row set applies.

    The corroborated fallback yields only the generic financial profiles, never
    "bank" or "insurer": telling those apart needs the 33-sector code, and
    guessing between them would put a wrong word on a real number.

    `doc_type` refines the answer where it has been collected — an IFRS filer
    has no ordinary profit to show, and an IFRS insurer's top line is insurance
    revenue rather than ordinary income.
    """
    std = _standard(doc_type)
    base = ""
    if sector33:
        base = _SECTOR33_PROFILE.get(str(sector33).strip()[:4], "")
    if not base and sector_name:
        low = str(sector_name).lower()
        for needle, prof in _SECTOR_NAME_PROFILE:
            if needle in low or needle in str(sector_name):
                base = prof
                break
    if not base and not has_operating_profit and _is_financial_hint(sector_hint):
        # Ordinary profit is a JGAAP-only concept, so a company filing one is
        # on the JGAAP bank/insurer shape and its top line really is ordinary
        # income. A financial issuer without one is on some other basis, and
        # which one cannot be read off the numbers — so the row set is trimmed
        # but nothing is renamed to a line the company may not file.
        base = "financial" if has_ordinary_profit else "financial_other"

    if base == "insurer" and std == "IFRS":
        return "insurer_ifrs"
    if base in ("financial", "bank", "insurer") and std in ("IFRS", "US"):
        return "financial_other" if base == "financial" else base
    if not base:
        return "general_ifrs" if std in ("IFRS", "US") else "general"
    return base


def _is_financial_hint(sector_hint: str) -> bool:
    """Whether a coarse sector label marks a company as a financial. Matches
    the vocabulary metadata.csv actually uses ("Financial Services") plus the
    obvious variants, and nothing else — Real Estate is not a financial here."""
    low = str(sector_hint or "").lower()
    return any(w in low for w in ("financial", "bank", "insurance", "insurer"))


def _standard(doc_type: str) -> str:
    """"FYFinancialStatements_Consolidated_IFRS" -> "IFRS". Empty when the
    filing type was not collected — every caller treats that as "unknown" and
    falls back to what the numbers themselves show."""
    dt = str(doc_type or "").upper()
    for std in ("IFRS", "JMIS", "US", "JP"):
        if dt.endswith("_" + std) or ("_" + std + "_") in dt:
            return std
    return ""


def accounting_standard(doc_type: str) -> str:
    """Public wrapper for _standard, for the panel's basis-change banner."""
    return _standard(doc_type)


def profile_rows(profile: str, base_rows):
    """`base_rows` filtered and relabelled for `profile`.

    Each entry of base_rows is the panel's (metric, label, scale, decimals)
    tuple. Rows the profile maps to None are dropped; the rest keep their
    scale and precision and take the profile's label where it has one."""
    labels = _PROFILE_LABELS.get(profile, {})
    out = []
    for row in base_rows:
        metric, label = row[0], row[1]
        if metric in labels:
            if labels[metric] is None:
                continue
            label = labels[metric]
        out.append((metric, label) + tuple(row[2:]))
    return out


def revenue_basis_mismatch(actual, consensus, floor: float = 0.75):
    """True when a street revenue estimate is measuring something other than
    the filed top line, so the two must not be shown side by side.

    Yahoo's revenue estimate for a Japanese bank is net revenue — interest
    income net of interest expense, plus fees. The filed actual beside it is
    ordinary income, gross. Across this universe the street figure runs at
    0.45x-0.66x the filed number for banks and securities houses and at
    0.96x-0.99x for insurers, with the nearest non-financial at 0.86x, so a
    floor of 0.75 separates a definitional mismatch from ordinary forecast
    growth with room to spare. Only applied to financial profiles, so a
    genuinely shrinking industrial is never caught by it."""
    if not actual or consensus is None or actual <= 0:
        return False
    return (consensus / actual) < floor


# Ratios a split would leave inconsistent, and the factors worth testing. A
# Japanese issuer splitting 2:1 or 5:1 to cut its lot size is routine, and
# three of the thirteen live cases in this universe are banks.
_SPLIT_FACTORS = (1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 10.0)


def detect_split(net_profit, eps, shares, tolerance: float = 0.03):
    """The split factor implied by a company's own guidance, or None.

    Net profit divided by EPS is the share count the company used to strike
    that EPS. Where that lands within `tolerance` of a whole split factor times
    the filed share count, the per-share figures and the share count are on
    opposite sides of a stock split — which is exactly the state SMFG's May
    guidance is in against its pre-split share price: 1,700,000m / 223.75 =
    7.598bn shares against a filed 3.827bn, a factor of 1.985.

    A P/E struck across that boundary is out by the factor, and so is any gap
    chip comparing guidance with a consensus set on the other basis. The
    caller suppresses both rather than showing a number that is wrong by 2x."""
    if not net_profit or not eps or not shares or eps <= 0 or shares <= 0:
        return None
    implied = net_profit / eps
    if implied <= 0:
        return None
    ratio = implied / shares
    for factor in _SPLIT_FACTORS:
        for candidate in (factor, 1.0 / factor):
            if abs(ratio / candidate - 1.0) < tolerance:
                return candidate
    return None


def equity_to_assets(filed, equity=None, total_assets=None):
    """The filed equity-to-assets ratio as a fraction, or None.

    J-Quants publishes this as a number whose scale is not documented here —
    5.2 and 0.052 are both plausible encodings of MUFG's 5.2%. Rather than
    guess, the filed value is checked against equity/total_assets computed from
    the same record: whichever reading agrees is the right one, and a filed
    value that agrees with neither is dropped. The filed figure is preferred
    over the derived one because it uses the tanshin's own definition —
    shareholders' equity excluding subscription rights and non-controlling
    interests — which the raw equity field does not.

    The derived figure is used ONLY to pin down the scale, never as a
    substitute. The two are not the same measure — the filed ratio excludes
    non-controlling interests and subscription rights and the equity field does
    not, which for MUFG is the difference between 5.2% and 5.5%. On a number
    this close to the subject of capital adequacy, a 6% relative overstatement
    dressed as a filed figure is not worth having: no filed value, no tile.

    Returns a fraction (0.052), never a percentage, so the render edge is the
    only place a "%" is ever attached."""
    filed = to_num(filed)
    derived = None
    if equity is not None and total_assets:
        try:
            derived = float(equity) / float(total_assets)
        except (TypeError, ValueError, ZeroDivisionError):
            derived = None
    if filed is None:
        return None
    for reading in (filed, filed / 100.0):
        if not 0 < reading < 1:
            continue
        # Equity as filed includes non-controlling interests and the ratio does
        # not, so they differ by a few percent relative rather than matching to
        # the digit; a 35% band separates "same measure" from "wrong scale by
        # a factor of 100" without pretending to more precision than that.
        if derived is None or abs(reading - derived) <= 0.35 * max(reading, derived):
            return reading
    return None


# ── J-Quants field resolution ────────────────────────────────────────────
# Each concept maps to the candidate keys seen across V1 (long names) and V2
# (abbreviated). First key present with a non-empty value wins.
_JQ_ALIASES = {
    # Actuals
    "net_sales":        ("Sales", "NetSales"),
    "operating_profit": ("OP", "OperatingProfit"),
    "net_profit":       ("NP", "Profit", "NetProfit"),
    "eps":              ("EPS", "EarningsPerShare"),
    "dps":              ("DivAnn", "DivTotalAnn", "ResultDividendPerShareAnnual"),
    # NCOdP — the *non-consolidated* (parent-only) figure — is the last resort
    # and is flagged where it is used, never substituted silently. For a
    # holding company the parent's ordinary profit is largely dividends
    # received from its own subsidiaries, so putting it in a consolidated
    # column without saying so misstates the business by an order of
    # magnitude. See NONCONSOLIDATED_KEYS and build_rows().
    "ordinary_profit":  ("OdP", "OrdinaryProfit", "NCOdP"),
    "div_q1":           ("Div1Q",),
    "div_q2":           ("Div2Q",),
    "div_q3":           ("Div3Q",),
    "div_fy":           ("DivFY",),
    # Current-year company forecast
    "f_net_sales":        ("FSales", "ForecastNetSales", "FNCSales"),
    "f_operating_profit": ("FOP", "ForecastOperatingProfit", "FNCOP"),
    "f_net_profit":       ("FNP", "ForecastProfit", "FNCNP"),
    "f_eps":              ("FEPS", "ForecastEarningsPerShare", "FNCEPS"),
    "f_ordinary_profit":  ("FOdP", "ForecastOrdinaryProfit", "FNCOdP"),
    # FDivFY is deliberately NOT here: it is the year-end instalment alone, and
    # treating it as the annual figure silently under-reports a company that
    # pays an interim dividend. Where no total is filed the instalments are
    # summed instead (see dps_annual).
    "f_dps":              ("FDivTotalAnn", "FDivAnn",
                           "ForecastDividendPerShareAnnual"),
    "f_div_q1":           ("FDiv1Q",),
    "f_div_q2":           ("FDiv2Q",),
    "f_div_q3":           ("FDiv3Q",),
    "f_div_fy":           ("FDivFY",),
    # Interim (first-half) forecast, current year
    "f2q_net_sales":        ("FSales2Q", "FNCSales2Q"),
    "f2q_operating_profit": ("FOP2Q", "FNCOP2Q"),
    "f2q_ordinary_profit":  ("FOdP2Q", "FNCOdP2Q"),
    "f2q_net_profit":       ("FNP2Q", "FNCNP2Q"),
    "f2q_eps":              ("FEPS2Q", "FNCEPS2Q"),
    # Next-year company forecast (only filed alongside full-year results)
    "nx_net_sales":        ("NxFSales", "NextYearForecastNetSales", "NxFNCSales"),
    "nx_operating_profit": ("NxFOP", "NextYearForecastOperatingProfit", "NxFNCOP"),
    # NxFNp, not NxFNP — the V2 response really does use that casing.
    "nx_net_profit":       ("NxFNP", "NxFNp", "NextYearForecastProfit", "NxFNCNP"),
    "nx_eps":              ("NxFEPS", "NextYearForecastEarningsPerShare", "NxFNCEPS"),
    "nx_ordinary_profit":  ("NxFOdP", "NextYearForecastOrdinaryProfit", "NxFNCOdP"),
    # No NxFDivTotalAnn exists in the response; the annual figure is only ever
    # the sum of the four instalments below.
    "nx_dps":              ("NxFDivAnn", "NxFDivTotalAnn",
                           "NextYearForecastDividendPerShareAnnual"),
    "nx_div_q1":           ("NxFDiv1Q",),
    "nx_div_q2":           ("NxFDiv2Q",),
    "nx_div_q3":           ("NxFDiv3Q",),
    "nx_div_fy":           ("NxFDivFY",),
    # Interim (first-half) forecast, next year
    "nx2q_net_sales":        ("NxFSales2Q", "NxFNCSales2Q"),
    "nx2q_operating_profit": ("NxFOP2Q", "NxFNCOP2Q"),
    "nx2q_ordinary_profit":  ("NxFOdP2Q", "NxFNCOdP2Q"),
    "nx2q_net_profit":       ("NxFNp2Q", "NxFNP2Q", "NxFNCNP2Q"),
    "nx2q_eps":              ("NxFEPS2Q", "NxFNCEPS2Q"),
    # Balance sheet
    "equity":       ("Eq", "Equity", "NetAssets", "TotalNetAssets"),
    "total_assets": ("TA", "TotalAssets"),
    "bps":          ("BPS", "BookValuePerShare"),
    "cash":         ("CashEq", "Cash", "CashAndEquivalents", "CashAndCashEquivalents"),
    # Confirmed absent from the V2 /fins/summary response — it carries no
    # interest-bearing debt and no depreciation line, so enterprise value and
    # EBITDA keep coming from Yahoo. Kept here so a future field is picked up.
    "debt":         ("IBD", "InterestBearingDebt", "Borrowings", "TotalDebt"),
    "dep_amort":    ("Dep", "Depreciation", "DepreciationAndAmortization"),
    "shares":       ("ShOutFY", "NumberOfIssuedShares"),
    "treasury":     ("TrShFY", "NumberOfTreasuryStock"),
    "cfo":          ("CFO", "CashFlowsFromOperatingActivities"),
    # The tanshin's own equity-to-assets ratio (自己資本比率), filed on the
    # front page of every summary. This is an ACCOUNTING ratio — shareholders'
    # equity less subscription rights and non-controlling interests, over total
    # assets — and it is emphatically NOT a regulatory capital ratio. A bank's
    # Basel III CET1 ratio has a risk-weighted denominator and runs at roughly
    # twice this number; an insurer's solvency margin and ESR are different
    # constructions again. None of those three is available from any feed this
    # app reads, so none of them is shown anywhere. See CAPITAL_DISCLAIMER.
    "equity_to_assets": ("EqAR", "EquityToAssetRatio"),
    # Period metadata
    "period_type":  ("CurPerType", "TypeOfCurrentPeriod"),
    # Which accounting standard the filing is drawn up under, and whether it is
    # consolidated — e.g. "FYFinancialStatements_Consolidated_IFRS". Filed, so
    # there is nothing to infer: it is what names the income-statement lines.
    "doc_type":     ("DocType", "TypeOfDocument"),
    "fy_end":       ("CurFYEn", "CurrentFiscalYearEndDate"),
    # Filed rather than derived. Japanese fiscal years do not all close in
    # March — a December or February close is common enough that adding twelve
    # months to CurFYEn would quietly mislabel a whole column of forecasts.
    "nx_fy_end":    ("NxtFYEn", "NextFiscalYearEndDate"),
    "disc_date":    ("DiscDate", "DisclosedDate"),
}


def to_num(val):
    """J-Quants sends numbers as strings and absences as "" — and a NaN from
    yfinance is not falsy, so it has to be caught explicitly or it propagates
    silently through every multiple downstream."""
    if val is None or val == "":
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


# Concepts whose value is a date or a code, not a number. These must not go
# through to_num — a fiscal year end of "2027-03-31" coerces to None, which
# would leave every forecast unlabelled and silently unusable.
TEXT_CONCEPTS = frozenset({"period_type", "fy_end", "nx_fy_end", "disc_date",
                           "doc_type"})

# J-Quants keys that carry the parent-only (非連結 / 単体) figure. A value that
# matched one of these describes the holding company alone, not the group, and
# is marked as such everywhere it surfaces rather than being passed off as a
# consolidated number.
NONCONSOLIDATED_KEYS = frozenset({
    "NCOdP", "FNCOdP", "NxFNCOdP", "FNCOdP2Q", "NxFNCOdP2Q",
    "NCSales", "FNCSales", "NxFNCSales", "FNCSales2Q", "NxFNCSales2Q",
    "NCOP", "FNCOP", "NxFNCOP", "FNCOP2Q", "NxFNCOP2Q",
    "NCNP", "FNCNP", "NxFNCNP", "FNCNP2Q", "NxFNCNP2Q",
    "NCEPS", "FNCEPS", "NxFNCEPS", "FNCEPS2Q", "NxFNCEPS2Q",
})


def jq_pick(record: dict, concept: str):
    """(number, matched_key) for a numeric `concept`, or (None, "")."""
    for key in _JQ_ALIASES.get(concept, ()):
        if key in record:
            num = to_num(record[key])
            if num is not None:
                return num, key
    return None, ""


def jq_pick_str(record: dict, concept: str):
    """(text, matched_key) for a date/code `concept`, or ("", "")."""
    for key in _JQ_ALIASES.get(concept, ()):
        val = record.get(key)
        if val not in (None, ""):
            return str(val), key
    return "", ""


def jq_get(record: dict, concept: str):
    """Whichever picker suits the concept, so callers need not track which."""
    return (jq_pick_str(record, concept) if concept in TEXT_CONCEPTS
            else jq_pick(record, concept))


def jq_field_report(record: dict) -> dict:
    """{concept: matched_key_or_""} — what the probe mode prints, so the real
    response schema is discoverable from a workflow log."""
    return {concept: jq_get(record, concept)[1] for concept in _JQ_ALIASES}


# ── Valuation ────────────────────────────────────────────────────────────

def _ratio(num, den):
    """Guard every multiple against a zero, negative or missing denominator.
    A loss-making forecast gives a negative P/E, which is not a number worth
    showing — the panel renders those as "n.m." rather than a minus sign the
    eye reads as cheap."""
    if num is None or den is None or den == 0:
        return None
    out = num / den
    return out if out > 0 else None


def enterprise_value(market_cap, debt, cash):
    """Market cap + net debt, all in yen. None if market cap is unknown;
    a missing debt or cash leg is treated as zero only when the other is
    present, since a company with neither reported simply has no net debt
    figure worth pretending to."""
    if market_cap is None:
        return None
    if debt is None and cash is None:
        return None
    return market_cap + (debt or 0) - (cash or 0)


def compute_valuations(price, shares, market_cap, fund: dict, forecasts: dict,
                       profile: str = "general", split_factor=None) -> dict:
    """Multiples at `price`, from a fundamentals row and a
    {(metric, fy, basis): value} forecast map. Every entry may be None; the
    caller renders those as "—" rather than dropping the row, because a blank
    cell where a peer has a number is itself informative.

    `market_cap` in yen. `forecasts` values in yen (absolute) or yen-per-share.

    `profile` decides which multiples are computed at all: enterprise value and
    net debt are omitted for financial issuers, where they have no meaning, and
    return on equity and the payout ratio take their place. `split_factor`, if
    a stock split has been detected, suppresses every per-share multiple —
    price and EPS are then on opposite sides of the split and their quotient is
    wrong by that factor.
    """
    out = {}
    bps = fund.get("bps")
    if bps is None and fund.get("equity") is not None and shares:
        bps = fund["equity"] / shares

    out["profile"] = profile
    out["split_factor"] = split_factor
    out["pb"] = _ratio(price, bps)
    out["bps"] = bps
    out["equity_to_assets"] = equity_to_assets(
        fund.get("equity_to_assets"), fund.get("equity"), fund.get("total_assets"))

    # A financial issuer has no enterprise value worth quoting, so none is
    # computed rather than computed and hidden — a stray consumer downstream
    # cannot then pick up a number that was never meaningful.
    if profile in NO_ENTERPRISE_VALUE:
        out["ev"] = out["net_debt"] = out["ev_ebitda"] = None
    else:
        ev = enterprise_value(market_cap, fund.get("debt"), fund.get("cash"))
        out["ev"] = ev
        out["net_debt"] = (None if (fund.get("debt") is None and fund.get("cash") is None)
                           else (fund.get("debt") or 0) - (fund.get("cash") or 0))
        out["ev_ebitda"] = _ratio(ev, fund.get("ebitda"))

    for fy_key in ("fy1", "fy2", "fy3"):
        for basis in ("company", "consensus"):
            eps = forecasts.get(("eps", fy_key, basis))
            sales = forecasts.get(("net_sales", fy_key, basis))
            dps = forecasts.get(("dps", fy_key, basis))
            out[f"pe_{fy_key}_{basis}"] = None if split_factor else _ratio(price, eps)
            out[f"ps_{fy_key}_{basis}"] = _ratio(market_cap, sales)
            out[f"yield_{fy_key}_{basis}"] = (
                None if (dps is None or not price or split_factor) else dps / price)
            # Return on equity against the filed book value, and the payout
            # ratio out of the same year's earnings. Both are arithmetic on
            # numbers already on disk, and for a bank or an insurer they are
            # the two figures the P/E is actually a proxy for. Neither survives
            # a split boundary: EPS and BPS would be on different bases.
            out[f"roe_{fy_key}_{basis}"] = (
                None if (eps is None or not bps or bps <= 0 or split_factor)
                else eps / bps)
            # The payout ratio goes too: a split that leaves EPS restated and
            # the dividend instalments summed across the boundary — SMFG's
            # ¥90 interim plus ¥45 final — gives a ratio out by the factor.
            out[f"payout_{fy_key}_{basis}"] = (
                None if (dps is None or eps is None or eps <= 0 or split_factor)
                else dps / eps)

    # Growth and PEG lean on consensus for FY2, since company guidance for a
    # second year barely exists (see the module docstring on NxF*).
    e1 = forecasts.get(("eps", "fy1", "consensus")) or forecasts.get(("eps", "fy1", "company"))
    e2 = forecasts.get(("eps", "fy2", "consensus")) or forecasts.get(("eps", "fy2", "company"))
    growth = None
    if e1 and e2 and e1 > 0:
        growth = (e2 - e1) / e1
    out["eps_growth"] = growth
    pe_for_peg = out.get("pe_fy1_consensus") or out.get("pe_fy1_company")
    out["peg"] = (pe_for_peg / (growth * 100)
                  if (pe_for_peg and growth and growth > 0) else None)
    return out


def implied_h2(full_year, first_half):
    """Second-half guidance a company never files directly: the full year it
    guided, less the first half it guided.

    Every metric this is applied to is a flow over the period (sales, the three
    profit lines, EPS), so the two halves add up to the year — which is exactly
    why it is not applied to DPS, a per-year rate that no company splits this
    way, and why there is no interim DPS in the store to subtract.

    None unless both halves of the arithmetic are there: an implied 2H that
    silently equals the full year because no interim was filed is a wrong
    number, not a missing one."""
    if full_year is None or first_half is None:
        return None
    return full_year - first_half


def guidance_gap(company, consensus, threshold: float = 0.05):
    """Signed fractional gap of consensus vs company guidance, or None when
    either side is missing or the gap is inside `threshold`. Only a gap worth
    looking at earns a chip in the table."""
    if company in (None, 0) or consensus is None:
        return None
    gap = (consensus - company) / abs(company)
    return gap if abs(gap) >= threshold else None


# ── CSV / JSON persistence ───────────────────────────────────────────────

def read_universe(path: str = UNIVERSE_PATH) -> list:
    """[{"Code","MarketDiv","Name", ...}, ...] — empty if neither file exists.

    Falls back to the JPX-Nikkei 400 list so a checkout made before the wider
    universe was built still collects something, rather than reporting an
    empty universe and writing nothing."""
    for candidate in (path, UNIVERSE_FALLBACK_PATH):
        if candidate and os.path.exists(candidate):
            with open(candidate, newline="", encoding="utf-8") as fh:
                return list(csv.DictReader(fh))
    return []


def read_rows(path: str, columns: list) -> list:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return [{c: r.get(c, "") for c in columns} for r in csv.DictReader(fh)]


def write_rows(path: str, columns: list, rows: list):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=columns)
        w.writeheader()
        for row in rows:
            w.writerow({c: row.get(c, "") for c in columns})


def consensus_key(row: dict) -> tuple:
    return (row.get("code", ""), row.get("metric", ""),
            row.get("fy", ""), row.get("basis", ""))


def merge_consensus(existing: list, fresh: list) -> list:
    """A fresh run replaces everything it covers, per (code, basis), and leaves
    the rest alone. A run that fails halfway therefore degrades to a partial
    refresh rather than deleting the companies it never reached.

    Replacing by (code, basis) rather than by the full row key is what stops a
    run that got the fiscal labels wrong from leaving its rows behind forever.
    When the J-Quants key was rejected, the labels fell back to Yahoo's own
    year end and slipped a year, so consensus went in under FY2027/FY2028; the
    corrected run then wrote FY2026/FY2027 and the FY2028 rows survived as
    duplicates of FY2027, a whole phantom year of estimates. Guidance and
    consensus are scoped separately so a Yahoo-only refresh cannot wipe filed
    company numbers, and vice versa."""
    refreshed = {(r.get("code", ""), r.get("basis", "")) for r in fresh}
    merged = {consensus_key(r): r for r in existing
              if (r.get("code", ""), r.get("basis", "")) not in refreshed}
    for row in fresh:
        merged[consensus_key(row)] = row
    return sorted(merged.values(),
                  key=lambda r: (r.get("code", ""), r.get("fy", ""),
                                 r.get("metric", ""), r.get("basis", "")))


# ── GitHub-backed loaders (same pattern as jquants.load_*_from_github) ────

def _raw_csv(repo: str, path: str, token: str = None):
    import requests
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    url = f"https://raw.githubusercontent.com/{repo}/main/{path}"
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code != 200:
        if r.status_code != 404:
            print(f"{path} fetch error: {r.status_code}")
        return []
    return list(csv.DictReader(r.text.splitlines()))


def load_consensus_from_github(repo: str, token: str = None) -> dict:
    """{code: {(metric, fy, basis): {"value","unit","source","as_of"}}}"""
    out = {}
    for row in _raw_csv(repo, CONSENSUS_PATH, token):
        val = to_num(row.get("value"))
        if val is None:
            continue
        out.setdefault(row.get("code", ""), {})[
            (row.get("metric", ""), row.get("fy", ""), row.get("basis", ""))
        ] = {"value": val, "unit": row.get("unit", ""),
             "source": row.get("source", ""), "as_of": row.get("as_of", "")}
    return out


def load_fundamentals_from_github(repo: str, token: str = None) -> dict:
    """{code: {field: float_or_str}} for the balance-sheet snapshot."""
    numeric = {"shares", "bps", "equity", "total_assets", "debt",
               "cash", "ebitda", "dep_amort", "op_actual",
               "equity_to_assets", "cfo"}
    # fy_end / fy_end_next / doc_type stay strings: they are dates and a filing
    # type, and to_num would turn "2027-03-31" into None.
    out = {}
    for row in _raw_csv(repo, FUNDAMENTALS_PATH, token):
        code = row.get("code", "")
        if not code:
            continue
        out[code] = {k: (to_num(v) if k in numeric else v) for k, v in row.items()}
    return out


def load_guidance_history_from_github(repo: str, token: str = None) -> dict:
    """{code: {(fy, metric): row}} — how each guidance figure has moved."""
    out = {}
    for row in _raw_csv(repo, GUIDANCE_HISTORY_PATH, token):
        code, fy, metric = row.get("code", ""), row.get("fy", ""), row.get("metric", "")
        if not (code and fy and metric):
            continue
        out.setdefault(code, {})[(fy, metric)] = {
            "first_value": to_num(row.get("first_value")),
            "first_as_of": row.get("first_as_of", ""),
            "latest_value": to_num(row.get("latest_value")),
            "latest_as_of": row.get("latest_as_of", ""),
            "revisions": int(to_num(row.get("revisions")) or 0),
        }
    return out


def revision_move(entry: dict, threshold: float = 0.005):
    """(fraction, direction) for how far guidance has moved since it was first
    filed, or None where it has not moved or there is nothing to compare.

    The 0.5% threshold matches jquants.guidance_direction, so a rounding-level
    difference is not dressed up as a revision."""
    if not entry:
        return None
    first, latest = entry.get("first_value"), entry.get("latest_value")
    if first in (None, 0) or latest is None or not entry.get("revisions"):
        return None
    move = (latest - first) / abs(first)
    if abs(move) < threshold:
        return None
    return move, ("raised" if move > 0 else "cut")


def load_universe_from_github(repo: str, token: str = None) -> dict:
    """{code: {"name", "market_div", "scale", "sector", "sector33"}} for the
    collected universe.

    sector33 is the JPX 33-sector code and is what tells a bank from an
    insurer; it is absent from files written before build_universe.py started
    collecting it, in which case profile_for() falls back to the shape of the
    company's own numbers."""
    rows = _raw_csv(repo, UNIVERSE_PATH, token) or _raw_csv(repo, UNIVERSE_FALLBACK_PATH, token)
    return {r["Code"]: {"name": r.get("Name", ""), "market_div": r.get("MarketDiv", ""),
                        "scale": r.get("ScaleCategory", ""),
                        "sector": r.get("Sector", ""),
                        "sector33": r.get("Sector33", "")}
            for r in rows if r.get("Code")}


def load_manual_from_github(repo: str, token: str = None) -> dict:
    """{code: {"<metric>|<fy>|<basis>": {"value","unit","source","as_of"}}}"""
    import requests
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    url = f"https://raw.githubusercontent.com/{repo}/main/{MANUAL_PATH}"
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except ValueError:
        return {}


def load_run_manifest_from_github(repo: str, token: str = None) -> dict:
    """What the last collector run reached. Lets the panel say *why* a column
    is empty instead of showing dashes that could mean three different things."""
    import requests
    headers = {"User-Agent": _UA}
    if token:
        headers["Authorization"] = f"token {token}"
    url = f"https://raw.githubusercontent.com/{repo}/main/{RUN_MANIFEST_PATH}"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        return r.json() if r.status_code == 200 else {}
    except Exception:
        return {}


def apply_manual_overrides(auto: dict, manual_for_code: dict) -> dict:
    """Overlay a company's manual entries onto its auto-collected map. Manual
    always wins — the whole point is that the user is reading a terminal this
    app cannot."""
    merged = dict(auto)
    for flat_key, entry in (manual_for_code or {}).items():
        parts = flat_key.split("|")
        if len(parts) != 3:
            continue
        val = to_num(entry.get("value"))
        if val is None:
            continue
        merged[tuple(parts)] = {"value": val, "unit": entry.get("unit", ""),
                                "source": entry.get("source", "manual"),
                                "as_of": entry.get("as_of", "")}
    return merged


# ── Manual overrides (GitHub-backed, same pattern as research_links.py) ──

def manual_key(metric: str, fy: str, basis: str) -> str:
    """Flat "metric|fy|basis" so the store stays plain JSON — tuple keys do not
    survive a round trip."""
    return f"{metric}|{fy}|{basis}"


TYPED_BASES = ("actual", "company", "consensus", "company_h1")


def plan_typed_overrides(edited: list, seed_keys, current: dict, manual_keys,
                         as_of: str) -> tuple:
    """Turn the Research tab's typed grid into (entries, remove, notes).

    Pure on purpose: unit conversion, year validation, change detection and
    what a deleted row means are the whole substance of typed entry, and none
    of it is testable while it lives inside a Streamlit callback.

    `edited` rows carry metric/fy/basis already mapped back to their stored
    codes, and a value in the units the panel prints — yen for per-share
    metrics, ¥bn for everything else. `current` is the merged map the grid was
    seeded from, so a row that comes back untouched produces no entry: without
    that check every save would rewrite every collected row as an override and
    freeze the panel against future collector runs."""
    entries, remove, notes = {}, [], []
    kept = set()

    for row in edited or ():
        metric = (row.get("metric") or "").strip()
        fy = str(row.get("fiscal_year") or "").strip().upper()
        basis = (row.get("basis") or "").strip()
        value = to_num(row.get("value"))
        if metric not in METRICS or basis not in TYPED_BASES:
            continue
        # A mislabelled year is worse than a missing one: it lands the number
        # in a column that looks right and is a year out.
        if not re.fullmatch(r"FY\d{4}", fy):
            notes.append(f"\u201c{row.get('fiscal_year')}\u201d is not a fiscal year label "
                         "(expected e.g. FY2027) \u2014 that row was skipped.")
            continue
        if value is None:
            notes.append(f"{metric} {fy} has no value \u2014 that row was skipped.")
            continue
        kept.add((metric, fy, basis))
        yen = value if metric in _PER_SHARE else value * 1e9
        shown = (current.get((metric, fy, basis)) or {}).get("value")
        # The trip through ¥bn is lossy in the last bits, so an untouched row
        # must not read as an edit.
        if shown is not None and abs(yen - shown) <= max(abs(shown), 1.0) * 1e-9:
            continue
        entries[manual_key(metric, fy, basis)] = {
            "value": yen,
            "unit": "jpy" if metric in _PER_SHARE else "jpy_abs",
            "source": "typed",
            "as_of": as_of,
        }

    # A deleted row drops its override so the collected number shows through.
    # Where there was no override, there is nothing to delete and the row comes
    # back on the next paint — say so rather than letting it look like a bug.
    undeletable = 0
    for key in seed_keys:
        if tuple(key) in kept:
            continue
        flat = manual_key(*key)
        if flat in (manual_keys or ()):
            remove.append(flat)
        else:
            undeletable += 1
    if undeletable:
        notes.append(f"{undeletable} deleted row(s) are collected data, not overrides "
                     "\u2014 they cannot be removed here and will reappear.")
    return entries, remove, notes


def _saved_message(n_saved: int, n_removed: int) -> str:
    parts = []
    if n_saved:
        parts.append(f"Saved {n_saved} override(s).")
    if n_removed:
        parts.append(f"Cleared {n_removed} back to collected values.")
    return " ".join(parts) or "Nothing to save."


def save_manual_overrides(repo: str, token: str, sec_code: str, entries: dict,
                          remove=()) -> tuple:
    """Merge `entries` into sec_code's block of data/consensus_manual.json, and
    drop any key in `remove`, committed straight to main (matching how the
    scheduled data jobs already commit data/ updates). Returns (ok, message).
    Retries once on a 409, where the sha moved under us because another session
    wrote first.

    `remove` is what makes a typed value undoable: deleting an override is not
    storing a blank, it is taking the key out so the collected number shows
    through again. Writing None instead would leave a key that
    apply_manual_overrides silently skips — indistinguishable from a bug."""
    import base64
    import requests

    if not token:
        return False, "No GITHUB_TOKEN configured — overrides kept for this session only."

    api_url = f"https://api.github.com/repos/{repo}/contents/{MANUAL_PATH}"
    headers = {"User-Agent": _UA, "Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}

    for attempt in range(2):
        r = requests.get(api_url, headers=headers, timeout=15)
        if r.status_code == 404:
            sha, data = None, {}
        elif r.status_code == 200:
            payload = r.json()
            sha = payload.get("sha")
            try:
                data = json.loads(base64.b64decode(payload.get("content", "")).decode("utf-8"))
            except Exception:
                data = {}
        else:
            return False, f"GitHub read error: HTTP {r.status_code}"

        block = data.setdefault(sec_code, {})
        block.update(entries)
        for key in remove or ():
            block.pop(key, None)
        if not block:
            data.pop(sec_code, None)
        body = {
            "message": f"chore: consensus overrides for {sec_code} [skip ci]",
            "content": base64.b64encode(
                json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
            ).decode("ascii"),
            "branch": "main",
        }
        if sha:
            body["sha"] = sha
        put = requests.put(api_url, headers=headers, json=body, timeout=15)
        if put.status_code in (200, 201):
            return True, _saved_message(len(entries), len(remove or ()))
        if put.status_code == 409 and attempt == 0:
            continue
        return False, f"GitHub write error: HTTP {put.status_code} — {put.text[:160]}"

    return False, "GitHub write failed after retry (concurrent edit)."


# ── Spreadsheet export ───────────────────────────────────────────────────

EXPORT_COLUMNS = ["code", "name", "metric", "fy", "basis", "value",
                  "unit", "source", "as_of"]


def export_rows(code: str, name: str, forecast_map: dict) -> list:
    """Long-format rows for one company, values raw so the receiving sheet can
    compute on them. Never build these from the rendered strings — "¥1,150.0B"
    is display, not data."""
    rows = []
    for (metric, fy, basis), entry in sorted(forecast_map.items()):
        rows.append({
            "code": code, "name": name, "metric": metric, "fy": fy, "basis": basis,
            "value": entry.get("value"),
            "unit": entry.get("unit") or ("jpy" if metric in _PER_SHARE else "jpy_abs"),
            "source": entry.get("source", ""), "as_of": entry.get("as_of", ""),
        })
    return rows


def to_tsv(rows: list, columns: list = None) -> str:
    """Tab-separated, for pasting straight into Google Sheets or Excel — a
    paste splits on tabs with no import dialog. Rendered inside st.code(),
    which supplies its own copy button."""
    cols = columns or EXPORT_COLUMNS
    out = ["\t".join(cols)]
    for row in rows:
        out.append("\t".join("" if row.get(c) is None else str(row.get(c, "")) for c in cols))
    return "\n".join(out)


def to_csv_bytes(rows: list, columns: list = None) -> bytes:
    """UTF-8 **with BOM** — without it Excel mis-decodes the Japanese company
    names on open, which is the single most common complaint about CSV exports
    of Japanese data."""
    import io
    cols = columns or EXPORT_COLUMNS
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for row in rows:
        w.writerow({c: ("" if row.get(c) is None else row.get(c)) for c in cols})
    return buf.getvalue().encode("utf-8-sig")


def to_xlsx_bytes(rows: list, columns: list = None, sheet_name: str = "Consensus") -> bytes:
    """Same data as an .xlsx, via openpyxl (already a dependency for the JPX
    earnings-calendar parser). Numbers are written as numbers, not text, so
    the sheet can sum and chart them without a re-type."""
    import io
    from openpyxl import Workbook

    cols = columns or EXPORT_COLUMNS
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]
    ws.append(cols)
    for row in rows:
        ws.append([row.get(c) for c in cols])
    for i, col in enumerate(cols, start=1):
        width = max([len(col)] + [len(str(r.get(col, ""))) for r in rows[:200]]) + 2
        ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = min(width, 42)
    ws.freeze_panes = "A2"
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
