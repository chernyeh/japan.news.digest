"""
market_data.py — v7

PRIMARY: Alpha Vantage API (requires ALPHA_VANTAGE_KEY in Streamlit Secrets)
  - Reliable from Streamlit Cloud (proper API, not blocked)
  - Free tier: 25 calls/day — we use ~10 per refresh (6 indices + 4 forex)
  - Get free key: https://www.alphavantage.co/support/#api-key
  - Add to Streamlit Secrets: ALPHA_VANTAGE_KEY = "your_key"

SECONDARY: Stooq CSV (no key, for sub-indices not in Alpha Vantage)
  - May timeout on Streamlit Cloud — handled gracefully

Provides: current price, daily change, and MTD/1M/3M/YTD/1Y/3Y returns.
"""

import requests
import os
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://www.alphavantage.co/query"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


def get_secret(name: str) -> str:
    try:
        import streamlit as st
        val = st.secrets.get(name, "")
        if val:
            return val
    except Exception:
        pass
    return os.environ.get(name, "")


# ── Instrument definitions ────────────────────────────────────────────────────
# Alpha Vantage uses EWJ (iShares MSCI Japan ETF) as proxy for Nikkei/TOPIX
# since ^N225 and ^TOPX aren't directly supported.
# Better: use actual index symbols — AV supports them with "function=TIME_SERIES_DAILY"

INDICES = [
    # (key, av_symbol, label)
    # Alpha Vantage supports these Japan index ETFs/proxies reliably:
    ("nikkei",      "EWJ",      "Nikkei 225 (EWJ proxy)"),   # iShares MSCI Japan ETF
    ("topix",       "EZJ",      "TOPIX (EZJ proxy)"),          # Amundi ETF MSCI Japan
    ("topix_c30",   None,       "TOPIX Core 30"),
    ("topix_m400",  None,       "TOPIX Mid 400"),
    ("topix_1000",  None,       "TOPIX 1000"),
    ("tse_growth",  None,       "TSE Growth 250"),
]

# Actually, Alpha Vantage DOES support direct index lookups via GLOBAL_QUOTE
# for many Japan indices. Let's use the most reliable approach:
# TIME_SERIES_DAILY with outputsize=full gives us years of history for returns calc.

# Correct symbols for Alpha Vantage:
AV_INDICES = {
    "nikkei":  "EWJ",    # iShares MSCI Japan ETF — most liquid Japan proxy
    "topix":   "EZJ",    # Amundi ETF Japan — TOPIX proxy (London listed)
}

AV_FOREX = {
    "usdjpy": ("USD", "JPY", "USD/JPY"),
    "eurjpy": ("EUR", "JPY", "EUR/JPY"),
    "cnyjpy": ("CNY", "JPY", "CNY/JPY"),
    "sgdjpy": ("SGD", "JPY", "SGD/JPY"),
}

# Stooq symbols for sub-indices (best effort, may fail on cloud)
STOOQ_SUBINDICES = {
    "nikkei":     ("^NKX",    "Nikkei 225"),
    "topix":      ("^TPX",    "TOPIX"),
    "topix_c30":  ("^TPXC30", "TOPIX Core 30"),
    "topix_m400": ("^TPXM400","TOPIX Mid 400"),
    "topix_1000": ("^TPX1000","TOPIX 1000"),
    "tse_growth": ("^TSEG250","TSE Growth 250"),
}

STOOQ_FOREX = {
    "usdjpy": ("USDJPY", "USD/JPY"),
    "eurjpy": ("EURJPY", "EUR/JPY"),
    "cnyjpy": ("CNYJPY", "CNY/JPY"),
    "sgdjpy": ("SGDJPY", "SGD/JPY"),
}

TSE_STOCKS = [
    ("7203", "Toyota"),          ("6758", "Sony"),
    ("8306", "Mitsubishi UFJ"),  ("9984", "SoftBank Group"),
    ("6861", "Keyence"),         ("7974", "Nintendo"),
    ("4063", "Shin-Etsu Chem"),  ("8035", "Tokyo Electron"),
    ("6954", "Fanuc"),           ("9432", "NTT"),
    ("4519", "Chugai Pharma"),   ("6367", "Daikin"),
    ("7267", "Honda"),           ("8316", "Sumitomo Mitsui"),
    ("9433", "KDDI"),            ("6098", "Recruit Holdings"),
    ("4661", "Oriental Land"),   ("8411", "Mizuho Financial"),
    ("6501", "Hitachi"),         ("7741", "Hoya"),
]


# ── Return calculator ─────────────────────────────────────────────────────────

def compute_returns(rows: list) -> dict:
    """
    rows: list of (date_str "YYYY-MM-DD", close_price) sorted oldest→newest.
    Returns dict: {period_label: pct_return or None}
    """
    if not rows:
        return {}

    today = date.today()
    current = rows[-1][1]

    def price_on_or_before(target):
        target_s = target.strftime("%Y-%m-%d")
        best = None
        for d_s, p in rows:
            if d_s <= target_s:
                best = p
            else:
                break
        return best

    def pct(old, new):
        return (new - old) / old * 100 if old else None

    targets = {
        "MTD": date(today.year, today.month, 1) - timedelta(days=1),
        "1M":  today - timedelta(days=30),
        "3M":  today - timedelta(days=91),
        "YTD": date(today.year - 1, 12, 31),
        "1Y":  today - timedelta(days=365),
        "3Y":  today - timedelta(days=365 * 3),
    }

    return {label: pct(price_on_or_before(ref), current)
            for label, ref in targets.items()}


# ── Alpha Vantage fetchers ────────────────────────────────────────────────────

def av_fetch_equity(symbol: str, api_key: str, label: str) -> dict:
    """Fetch full daily history for an equity/ETF from Alpha Vantage."""
    try:
        resp = requests.get(BASE_URL, params={
            "function":   "TIME_SERIES_DAILY",
            "symbol":     symbol,
            "outputsize": "full",
            "datatype":   "json",
            "apikey":     api_key,
        }, timeout=20)

        data = resp.json()
        if "Note" in data:
            return {"price": 0, "label": label, "error": "AV rate limit hit"}
        if "Information" in data:
            return {"price": 0, "label": label, "error": "AV API key issue: " + data["Information"][:80]}

        ts = data.get("Time Series (Daily)", {})
        if not ts:
            return {"price": 0, "label": label, "error": "No time series data"}

        rows = sorted(
            [(d, float(v["4. close"])) for d, v in ts.items()],
            key=lambda x: x[0]
        )
        price  = rows[-1][1]
        prev   = rows[-2][1] if len(rows) >= 2 else price
        change = price - prev
        pct    = (change / prev * 100) if prev else 0

        return {
            "price": price, "change": change, "pct_change": pct,
            "state_label": "Last close", "label": label,
            "returns": compute_returns(rows), "source": "alphavantage",
        }
    except Exception as e:
        return {"price": 0, "label": label, "error": str(e)}


def av_fetch_forex(from_ccy: str, to_ccy: str, api_key: str, label: str) -> dict:
    """Fetch full daily FX history from Alpha Vantage."""
    try:
        resp = requests.get(BASE_URL, params={
            "function":    "FX_DAILY",
            "from_symbol": from_ccy,
            "to_symbol":   to_ccy,
            "outputsize":  "full",
            "datatype":    "json",
            "apikey":      api_key,
        }, timeout=20)

        data = resp.json()
        if "Note" in data:
            return {"price": 0, "label": label, "error": "AV rate limit hit"}
        if "Information" in data:
            return {"price": 0, "label": label, "error": "AV API key issue"}

        ts = data.get("Time Series FX (Daily)", {})
        if not ts:
            return {"price": 0, "label": label, "error": "No FX data"}

        rows = sorted(
            [(d, float(v["4. close"])) for d, v in ts.items()],
            key=lambda x: x[0]
        )
        price  = rows[-1][1]
        prev   = rows[-2][1] if len(rows) >= 2 else price
        change = price - prev
        pct    = (change / prev * 100) if prev else 0

        return {
            "price": price, "change": change, "pct_change": pct,
            "state_label": "Last close", "label": label,
            "returns": compute_returns(rows), "source": "alphavantage",
        }
    except Exception as e:
        return {"price": 0, "label": label, "error": str(e)}


# ── Stooq fetcher (secondary / sub-indices) ───────────────────────────────────

def stooq_fetch(symbol: str, label: str, years: int = 4) -> dict:
    """Fetch from Stooq CSV. May timeout on Streamlit Cloud — handled gracefully."""
    try:
        end   = datetime.today()
        start = end - timedelta(days=years * 366)
        url   = (
            f"https://stooq.com/q/d/l/?s={symbol.lower()}"
            f"&d1={start:%Y%m%d}&d2={end:%Y%m%d}&i=d"
        )
        resp = requests.get(url, headers=HEADERS, timeout=8)  # short timeout
        if resp.status_code != 200:
            return {"price": 0, "label": label, "error": f"HTTP {resp.status_code}"}

        text = resp.text.strip()
        if not text or "No data" in text or len(text) < 30:
            return {"price": 0, "label": label, "error": "No data"}

        lines = [l.strip() for l in text.splitlines() if l.strip()]
        rows = []
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) >= 5:
                try:
                    rows.append((parts[0], float(parts[4])))
                except (ValueError, IndexError):
                    pass

        if not rows:
            return {"price": 0, "label": label, "error": "No valid rows"}

        rows.sort(key=lambda x: x[0])
        price  = rows[-1][1]
        prev   = rows[-2][1] if len(rows) >= 2 else price
        change = price - prev
        pct    = (change / prev * 100) if prev else 0

        return {
            "price": price, "change": change, "pct_change": pct,
            "state_label": "Last close", "label": label,
            "returns": compute_returns(rows), "source": "stooq",
        }
    except Exception as e:
        return {"price": 0, "label": label, "error": str(e)}


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_market_overview() -> dict:
    """
    Returns {
      "indices": { key: {price, change, pct_change, returns, label, state_label, source} },
      "forex":   { key: {price, change, pct_change, returns, label, state_label, source} },
      "_source": str,
      "_error":  str (only if no key),
    }
    """
    api_key = get_secret("ALPHA_VANTAGE_KEY")
    results = {"indices": {}, "forex": {}}

    if not api_key:
        results["_source"] = "error"
        results["_error"] = (
            "No ALPHA_VANTAGE_KEY found in Streamlit Secrets. "
            "Get a free key at https://www.alphavantage.co/support/#api-key "
            "then add it to Streamlit Cloud → App Settings → Secrets."
        )
        # Still try Stooq for everything as fallback
        _fetch_stooq_all(results)
        return results

    # ── Fetch indices + forex concurrently via Alpha Vantage ──
    tasks = []
    for key, sym, label in [
        ("nikkei", "EWJ",  "Nikkei 225 (EWJ)"),
        ("topix",  "EZJ",  "TOPIX (EZJ)"),
    ]:
        tasks.append(("index_av", key, sym, label))

    for key, (fc, tc, label) in AV_FOREX.items():
        tasks.append(("forex_av", key, fc + "|" + tc, label))

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {}
        for t in tasks:
            kind, key, sym, label = t
            if kind == "index_av":
                f = ex.submit(av_fetch_equity, sym, api_key, label)
            else:
                fc, tc = sym.split("|")
                f = ex.submit(av_fetch_forex, fc, tc, api_key, label)
            futures[f] = (kind, key)

        for f in as_completed(futures):
            kind, key = futures[f]
            try:
                data = f.result()
                bucket = "indices" if "index" in kind else "forex"
                results[bucket][key] = data
            except Exception as e:
                bucket = "indices" if "index" in kind else "forex"
                results[bucket][key] = {"price": 0, "error": str(e)}

    # ── Try Stooq for sub-indices (TOPIX Core 30, Mid 400, 1000, TSE Growth) ──
    subindex_tasks = {
        "topix_c30":  ("^TPXC30",  "TOPIX Core 30"),
        "topix_m400": ("^TPXM400", "TOPIX Mid 400"),
        "topix_1000": ("^TPX1000", "TOPIX 1000"),
        "tse_growth": ("^TSEG250", "TSE Growth 250"),
    }
    # Also try to get proper Nikkei/TOPIX from Stooq to replace ETF proxies
    stooq_override = {
        "nikkei": ("^NKX", "Nikkei 225"),
        "topix":  ("^TPX", "TOPIX"),
    }

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {}
        for key, (sym, label) in {**stooq_override, **subindex_tasks}.items():
            futures[ex.submit(stooq_fetch, sym, label)] = key
        for f in as_completed(futures):
            key = futures[f]
            try:
                data = f.result()
                if data.get("price", 0) > 0:
                    results["indices"][key] = data  # override ETF proxy with real index
            except Exception:
                pass

    results["_source"] = "alphavantage+stooq"
    return results


def _fetch_stooq_all(results: dict):
    """Populate results entirely from Stooq (fallback when no AV key)."""
    with ThreadPoolExecutor(max_workers=10) as ex:
        idx_futures = {
            ex.submit(stooq_fetch, sym, label): key
            for key, (sym, label) in STOOQ_SUBINDICES.items()
        }
        fx_futures = {
            ex.submit(stooq_fetch, sym, label): key
            for key, (sym, label) in STOOQ_FOREX.items()
        }
        for f in as_completed(idx_futures):
            key = idx_futures[f]
            try:
                results["indices"][key] = f.result()
            except Exception as e:
                results["indices"][key] = {"price": 0, "error": str(e)}
        for f in as_completed(fx_futures):
            key = fx_futures[f]
            try:
                results["forex"][key] = f.result()
            except Exception as e:
                results["forex"][key] = {"price": 0, "error": str(e)}


def fetch_tse_movers() -> dict:
    """Fetch TSE movers via Alpha Vantage if key available, else Stooq."""
    api_key = get_secret("ALPHA_VANTAGE_KEY")
    movers = []

    if api_key:
        # Use AV for top 10 stocks to stay within daily limit (10 calls)
        subset = TSE_STOCKS[:10]
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {
                ex.submit(av_fetch_equity, f"{code}.TYO", api_key, name): (code, name)
                for code, name in subset
            }
            for f in as_completed(futures):
                code, name = futures[f]
                try:
                    q = f.result()
                    if q.get("price", 0) > 0:
                        movers.append({
                            "symbol": f"{code}.T", "name": name,
                            "price": q["price"], "change": q["change"],
                            "pct_change": q["pct_change"],
                        })
                except Exception:
                    pass

    # Fall back to Stooq for movers if AV returned nothing
    if not movers:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {
                ex.submit(stooq_fetch, f"{code}.jp", name, 1): (code, name)
                for code, name in TSE_STOCKS
            }
            for f in as_completed(futures):
                code, name = futures[f]
                try:
                    q = f.result()
                    if q.get("price", 0) > 0:
                        movers.append({
                            "symbol": f"{code}.T", "name": name,
                            "price": q["price"], "change": q["change"],
                            "pct_change": q["pct_change"],
                        })
                except Exception:
                    pass

    movers.sort(key=lambda x: x["pct_change"], reverse=True)
    return {
        "gainers": [m for m in movers if m["pct_change"] > 0][:5],
        "losers":  [m for m in reversed(movers) if m["pct_change"] < 0][:5],
        "all":     movers,
    }


def fetch_foreign_flow() -> dict:
    """Foreign investor weekly flow from JPX public CSV."""
    try:
        url = (
            "https://www.jpx.co.jp/markets/statistics-equities/"
            "investor-type/nlsgeu000000484c-att/s13.csv"
        )
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        if resp.status_code != 200:
            return {"available": False}
        for line in reversed(resp.text.strip().split("\n")[-10:]):
            parts = line.split(",")
            if len(parts) >= 4:
                try:
                    val = float(parts[-1].replace('"', '').replace(" ", ""))
                    return {
                        "available": True,
                        "net_billion_yen": val / 1e9,
                        "direction": "Net Buying" if val > 0 else "Net Selling",
                        "as_of": "Latest week",
                    }
                except Exception:
                    pass
    except Exception as e:
        print(f"Foreign flow error: {e}")
    return {
        "available": False,
        "jpx_url": (
            "https://www.jpx.co.jp/english/markets/"
            "statistics-equities/investor-type/index.html"
        ),
    }
