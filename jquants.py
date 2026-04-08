"""
jquants.py — J-Quants API integration for Japan Investment Digest

Handles:
  - Authentication (refresh token → ID token flow)
  - Earnings Calendar (/v2/equities/earnings-calendar)
  - Financial Summary (/v2/fins/summary)

Free plan limitations:
  - Stock prices / financial data: 2 years history, 12-week delayed
  - Earnings calendar: available, covers March/September fiscal year-end companies only
  - No CSV bulk download (API access only)

Usage:
  api_key = get_jquants_secret()
  token   = get_id_token(api_key)
  cal     = fetch_earnings_calendar(token)
  summary = fetch_financial_summary(token, code="7203")
"""

import requests
import os
from datetime import datetime, timedelta, date
from functools import lru_cache
import time


# ── Secret retrieval ──────────────────────────────────────────────────────────

def get_jquants_secret() -> str:
    """Get J-Quants API key from Streamlit secrets or environment."""
    try:
        import streamlit as st
        val = st.secrets.get("JQUANTS_API_KEY", "")
        if val:
            return val
    except Exception:
        pass
    return os.environ.get("JQUANTS_API_KEY", "")


# ── Authentication ────────────────────────────────────────────────────────────
# J-Quants uses a two-step auth:
#   1. POST /v1/token/auth_user with email+password → refresh token (valid 1 week)
#   2. POST /v1/token/auth_refresh with refresh token → ID token (valid 24h)
# But with API key auth (the recommended method for free tier), you can use the
# API key directly as the x-api-key header on all requests.

BASE = "https://api.jquants.com/v2"
HEADERS_TEMPLATE = {"Content-Type": "application/json"}


def _headers(api_key: str) -> dict:
    return {"x-api-key": api_key, "Content-Type": "application/json"}


def test_connection(api_key: str) -> tuple:
    """
    Test that the API key works.
    Returns (True, "OK") or (False, "error message").
    """
    if not api_key:
        return False, "No JQUANTS_API_KEY found in Streamlit Secrets."
    try:
        resp = requests.get(
            f"{BASE}/markets/calendar",
            headers=_headers(api_key),
            timeout=10,
        )
        if resp.status_code == 200:
            return True, "Connected"
        elif resp.status_code == 401:
            return False, "Invalid API key — check JQUANTS_API_KEY in Streamlit Secrets."
        elif resp.status_code == 403:
            return False, "Access denied — your plan may not include this endpoint."
        else:
            return False, f"HTTP {resp.status_code}: {resp.text[:100]}"
    except Exception as e:
        return False, f"Connection error: {e}"


# ── Earnings Calendar ─────────────────────────────────────────────────────────

def fetch_earnings_calendar(api_key: str) -> list:
    """
    Fetch the earnings announcement schedule.
    Returns list of dicts: {Date, Code, CoName, FY, SectorNm, FQ, Section}

    Note: Only covers March and September fiscal year-end companies for now.
    Date field is empty string if announcement date is not yet confirmed.
    """
    if not api_key:
        return []

    results = []
    pagination_key = None

    while True:
        params = {}
        if pagination_key:
            params["pagination_key"] = pagination_key

        try:
            resp = requests.get(
                f"{BASE}/equities/earnings-calendar",
                headers=_headers(api_key),
                params=params,
                timeout=20,
            )
            if resp.status_code != 200:
                print(f"Earnings calendar error: HTTP {resp.status_code} — {resp.text[:100]}")
                break

            data = resp.json()
            batch = data.get("data", [])
            results.extend(batch)

            pagination_key = data.get("pagination_key")
            if not pagination_key or not batch:
                break

        except Exception as e:
            print(f"Earnings calendar fetch error: {e}")
            break

    return results


def group_calendar_by_date(entries: list) -> dict:
    """
    Group earnings calendar entries by reporting date.
    Returns dict: {date_str: [entries]} sorted by date.
    Entries with no confirmed date go under key "TBD".
    """
    grouped = {}
    for entry in entries:
        d = entry.get("Date", "")
        key = d if d else "TBD"
        grouped.setdefault(key, []).append(entry)

    # Sort: dated entries first (ascending), TBD at end
    sorted_keys = sorted([k for k in grouped if k != "TBD"]) + (["TBD"] if "TBD" in grouped else [])
    return {k: grouped[k] for k in sorted_keys}


def label_date_bucket(date_str: str) -> str:
    """
    Label a date string relative to today:
    'Today', 'Tomorrow', 'This Week', 'Next Week', 'Next 30 Days', 'Later', 'TBD'
    """
    if not date_str or date_str == "TBD":
        return "TBD"
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        today = date.today()
        delta = (d - today).days
        if delta < 0:
            return "Past"
        elif delta == 0:
            return "Today"
        elif delta == 1:
            return "Tomorrow"
        elif delta <= 7:
            return "This Week"
        elif delta <= 14:
            return "Next Week"
        elif delta <= 30:
            return "Next 30 Days"
        else:
            return "Later"
    except Exception:
        return "TBD"


# ── Financial Summary ─────────────────────────────────────────────────────────

def fetch_financial_summary(api_key: str, code: str = None, date_str: str = None) -> list:
    """
    Fetch quarterly financial summary for a company or all companies on a date.
    Provide either code (e.g. "7203") or date_str (e.g. "2024-10-25").

    Returns list of summary dicts. Key fields:
      DiscDate, Code, CurPerType (1Q/2Q/3Q/4Q/FY),
      Sales, OP, NP, EPS,              ← actuals
      FSales, FOP, FNP, FEPS,          ← current year forecast
      NxFSales, NxFOP, NxFNP, NxFEPS,  ← next year forecast
      DivAnn, FDivAnn,                  ← actual + forecast annual dividend
    """
    if not api_key:
        return []
    if not code and not date_str:
        return []

    results = []
    pagination_key = None

    while True:
        params = {}
        if code:
            # J-Quants accepts 4 or 5 digit code
            params["code"] = code.replace(".T", "")
        if date_str:
            params["date"] = date_str
        if pagination_key:
            params["pagination_key"] = pagination_key

        try:
            resp = requests.get(
                f"{BASE}/fins/summary",
                headers=_headers(api_key),
                params=params,
                timeout=20,
            )
            if resp.status_code != 200:
                print(f"Financial summary error: HTTP {resp.status_code} — {resp.text[:100]}")
                break

            data = resp.json()
            batch = data.get("data", [])
            results.extend(batch)

            pagination_key = data.get("pagination_key")
            if not pagination_key or not batch:
                break

        except Exception as e:
            print(f"Financial summary fetch error: {e}")
            break

    # Sort by disclosure date ascending
    results.sort(key=lambda x: x.get("DiscDate", ""))
    return results


def format_summary_for_display(summaries: list) -> list:
    """
    Take raw financial summary records and return a cleaned list
    suitable for display — last 8 quarters, most recent first.
    """
    # Filter to main consolidated or standalone filings only
    # DocType contains 'FinancialStatements' or 'EarningsSummary'
    relevant = [
        s for s in summaries
        if s.get("CurPerType") in ("1Q", "2Q", "3Q", "4Q", "FY")
    ]
    # Most recent first
    relevant.sort(key=lambda x: x.get("DiscDate", ""), reverse=True)
    return relevant[:8]


def safe_num(val, divisor=1e9, decimals=1) -> str:
    """Format a large number in billions with ¥ prefix, or return '—'."""
    try:
        if val == "" or val is None:
            return "—"
        n = float(val) / divisor
        return f"¥{n:,.{decimals}f}B"
    except Exception:
        return "—"


def safe_pct(val, decimals=1) -> str:
    """Format a ratio as percentage, or return '—'."""
    try:
        if val == "" or val is None:
            return "—"
        return f"{float(val)*100:.{decimals}f}%"
    except Exception:
        return "—"


def guidance_direction(current: str, previous: str) -> str:
    """
    Compare two guidance figures and return direction emoji.
    Returns '▲ raised', '▼ cut', '— unchanged', or '' if unknown.
    """
    try:
        c = float(current)
        p = float(previous)
        if c > p * 1.005:
            return "▲ raised"
        elif c < p * 0.995:
            return "▼ cut"
        else:
            return "— unchanged"
    except Exception:
        return ""


# ── Price performance helpers ─────────────────────────────────────────────────

def get_performance_band(vs_topix_pct: float) -> dict:
    """
    Given a stock's return vs TOPIX (in percentage points), return
    display metadata: colour, label, emoji.
    """
    if vs_topix_pct is None:
        return {"color": "#9B8B7A", "bg": "#F0EDE8", "label": "N/A", "emoji": "●"}
    if vs_topix_pct >= 15:
        return {"color": "#1B5E20", "bg": "#C8E6C9", "label": f"+{vs_topix_pct:.1f}%", "emoji": "🟢"}
    elif vs_topix_pct >= 5:
        return {"color": "#2E7D32", "bg": "#DCEDC8", "label": f"+{vs_topix_pct:.1f}%", "emoji": "🟩"}
    elif vs_topix_pct >= -5:
        return {"color": "#6B4C00", "bg": "#FFF9C4", "label": f"{vs_topix_pct:+.1f}%", "emoji": "🟨"}
    elif vs_topix_pct >= -15:
        return {"color": "#E65100", "bg": "#FFE0B2", "label": f"{vs_topix_pct:.1f}%", "emoji": "🟧"}
    else:
        return {"color": "#B71C1C", "bg": "#FFCDD2", "label": f"{vs_topix_pct:.1f}%", "emoji": "🔴"}


def fetch_3m_performance_batch(codes: list) -> dict:
    """
    Fetch 3-month price performance for a list of TSE codes vs TOPIX.
    Uses yfinance. Returns dict: {code: vs_topix_pct or None}

    codes: list of 4-digit strings e.g. ["7203", "6758"]
    """
    try:
        import yfinance as yf
        import pandas as pd
        from datetime import date as _date, timedelta as _td
    except ImportError:
        return {}

    if not codes:
        return {}

    # Build ticker list
    tickers = [f"{c}.T" for c in codes] + ["^TOPX"]

    try:
        raw = yf.download(
            tickers,
            period="4mo",   # 4 months to ensure we have 3 full months
            auto_adjust=True,
            progress=False,
            threads=True,
            group_by="ticker",
        )
    except Exception as e:
        print(f"yfinance batch error: {e}")
        return {}

    results = {}

    # Get TOPIX 3M return
    topix_3m = None
    try:
        if len(tickers) > 1:
            topix_closes = raw["^TOPX"]["Close"].dropna() if "^TOPX" in raw.columns.get_level_values(0) else None
        else:
            topix_closes = raw["Close"].dropna()

        if topix_closes is not None and len(topix_closes) >= 2:
            cutoff = topix_closes.index[-1] - pd.Timedelta(days=91)
            old_prices = topix_closes[topix_closes.index <= cutoff]
            if len(old_prices) > 0:
                topix_3m = (topix_closes.iloc[-1] / old_prices.iloc[-1] - 1) * 100
    except Exception as e:
        print(f"TOPIX 3M error: {e}")

    # Get each stock's 3M return
    for code in codes:
        ticker = f"{code}.T"
        try:
            if len(tickers) > 1:
                closes = raw[ticker]["Close"].dropna() if ticker in raw.columns.get_level_values(0) else None
            else:
                closes = raw["Close"].dropna()

            if closes is None or len(closes) < 2:
                results[code] = None
                continue

            import pandas as _pd
            cutoff = closes.index[-1] - _pd.Timedelta(days=91)
            old_prices = closes[closes.index <= cutoff]
            if len(old_prices) == 0:
                results[code] = None
                continue

            stock_3m = (closes.iloc[-1] / old_prices.iloc[-1] - 1) * 100
            if topix_3m is not None:
                results[code] = round(float(stock_3m) - float(topix_3m), 1)
            else:
                results[code] = round(float(stock_3m), 1)
        except Exception:
            results[code] = None

    return results
