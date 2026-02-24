"""
market_data.py — v5

Data sources (in priority order):
1. Stooq.com — FREE, no API key, simple CSV, covers Nikkei/TOPIX/forex/TSE stocks
   URL pattern: https://stooq.com/q/d/l/?s=SYMBOL&i=d
2. Yahoo Finance v8 — FREE, no key, fallback if Stooq fails
3. Finnhub — FREE with API key (optional), most reliable from cloud servers

Stooq symbols:
  ^NKX     = Nikkei 225
  ^TPX     = TOPIX
  USDJPY   = USD/JPY
  MYRJPY   = MYR/JPY
  EURJPY   = EUR/JPY
  7203.JP  = Toyota (TSE stocks use .JP suffix)
"""

import requests
import os
import io
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


def get_secret(name: str) -> str:
    try:
        import streamlit as st
        val = st.secrets.get(name, "")
        if val:
            return val
    except Exception:
        pass
    return os.environ.get(name, "")


# ── Symbol maps ───────────────────────────────────────────────────────────────

STOOQ_OVERVIEW = {
    "nikkei": "^NKX",
    "topix":  "^TPX",
    "usdjpy": "USDJPY",
    "myrjpy": "MYRJPY",
    "eurjpy": "EURJPY",
}

YAHOO_OVERVIEW = {
    "nikkei": "^N225",
    "topix":  "^TOPX",
    "usdjpy": "USDJPY=X",
    "myrjpy": "MYRJPY=X",
    "eurjpy": "EURJPY=X",
}

# TSE stocks: Stooq uses .JP suffix, Yahoo uses .T suffix
TSE_STOCKS = [
    ("7203", "Toyota"),
    ("6758", "Sony"),
    ("8306", "Mitsubishi UFJ"),
    ("9984", "SoftBank Group"),
    ("6861", "Keyence"),
    ("7974", "Nintendo"),
    ("4063", "Shin-Etsu Chem"),
    ("8035", "Tokyo Electron"),
    ("6954", "Fanuc"),
    ("9432", "NTT"),
    ("4519", "Chugai Pharma"),
    ("6367", "Daikin"),
    ("7267", "Honda"),
    ("8316", "Sumitomo Mitsui"),
    ("9433", "KDDI"),
    ("6098", "Recruit Holdings"),
    ("4661", "Oriental Land"),
    ("8411", "Mizuho Financial"),
    ("6501", "Hitachi"),
    ("7741", "Hoya"),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ══════════════════════════════════════════════════════════════════════════════
# Stooq backend — primary source
# ══════════════════════════════════════════════════════════════════════════════

def stooq_fetch(symbol: str) -> dict:
    """
    Fetch last 5 days of daily data from Stooq CSV API.
    Returns price, change, pct_change.
    """
    try:
        end = datetime.today()
        start = end - timedelta(days=10)  # fetch 10 days to handle weekends/holidays
        url = (
            f"https://stooq.com/q/d/l/?s={symbol}"
            f"&d1={start:%Y%m%d}&d2={end:%Y%m%d}&i=d"
        )
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if resp.status_code != 200:
            return {"price": 0, "error": f"HTTP {resp.status_code}"}

        text = resp.text.strip()
        if not text or "No data" in text or len(text) < 20:
            return {"price": 0, "error": "No data returned"}

        # Parse CSV manually (avoid pandas dependency issues)
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        if len(lines) < 2:
            return {"price": 0, "error": "Not enough rows"}

        # header: Date,Open,High,Low,Close,Volume (Volume may be absent)
        rows = []
        for line in lines[1:]:  # skip header
            parts = line.split(",")
            if len(parts) >= 5:
                try:
                    close = float(parts[4])
                    rows.append(close)
                except ValueError:
                    pass

        if not rows:
            return {"price": 0, "error": "No valid close prices"}

        price = rows[-1]
        prev  = rows[-2] if len(rows) >= 2 else price
        change = price - prev
        pct    = (change / prev * 100) if prev else 0

        # Stooq only has daily data, so always "last close"
        state_label = "Last close"

        return {
            "price": price, "change": change, "pct_change": pct,
            "state_label": state_label, "symbol": symbol, "source": "stooq",
        }

    except Exception as e:
        return {"price": 0, "error": str(e), "symbol": symbol}


def stooq_fetch_all() -> dict:
    """Fetch all overview symbols from Stooq concurrently."""
    results = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(stooq_fetch, sym): key
                   for key, sym in STOOQ_OVERVIEW.items()}
        for f in as_completed(futures):
            key = futures[f]
            try:
                results[key] = f.result()
            except Exception as e:
                results[key] = {"price": 0, "error": str(e)}
    return results


def stooq_fetch_tse_movers() -> dict:
    """Fetch TSE large cap quotes from Stooq."""
    movers = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {
            ex.submit(stooq_fetch, f"{code}.JP"): (code, name)
            for code, name in TSE_STOCKS
        }
        for f in as_completed(futures):
            code, name = futures[f]
            try:
                q = f.result()
                if q.get("price", 0) > 0:
                    movers.append({
                        "symbol": f"{code}.T",
                        "name": name,
                        "price": q["price"],
                        "change": q["change"],
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


# ══════════════════════════════════════════════════════════════════════════════
# Yahoo Finance backend — fallback
# ══════════════════════════════════════════════════════════════════════════════

def yahoo_fetch(symbol: str) -> dict:
    """Fetch from Yahoo Finance v8 chart API."""
    try:
        for host in ["query1", "query2"]:
            url = f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}"
            try:
                resp = requests.get(
                    url, headers=HEADERS,
                    params={"interval": "1d", "range": "5d"},
                    timeout=12
                )
                if resp.status_code == 200:
                    break
            except Exception:
                continue
        else:
            return {"price": 0, "error": "Both Yahoo hosts failed"}

        data   = resp.json()
        result = data["chart"]["result"][0]
        meta   = result.get("meta", {})
        state  = meta.get("marketState", "CLOSED")

        price = (meta.get("regularMarketPrice") or
                 meta.get("postMarketPrice") or
                 meta.get("preMarketPrice") or 0)
        prev  = (meta.get("chartPreviousClose") or
                 meta.get("previousClose") or
                 meta.get("regularMarketPreviousClose") or 0)

        if price == 0:
            closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
            closes = [c for c in closes if c is not None]
            if closes:
                price = closes[-1]
                prev  = closes[-2] if len(closes) >= 2 else price

        if not prev:
            prev = price
        change = price - prev
        pct    = (change / prev * 100) if prev else 0

        state_label = {"REGULAR": "Live", "PRE": "Pre-market",
                       "POST": "After-hours"}.get(state, "Last close")

        return {"price": price, "change": change, "pct_change": pct,
                "state_label": state_label, "source": "yahoo"}
    except Exception as e:
        return {"price": 0, "error": str(e)}


def yahoo_fetch_all() -> dict:
    results = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(yahoo_fetch, sym): key
                   for key, sym in YAHOO_OVERVIEW.items()}
        for f in as_completed(futures):
            key = futures[f]
            try:
                results[key] = f.result()
            except Exception as e:
                results[key] = {"price": 0, "error": str(e)}
    return results


def yahoo_fetch_tse_movers() -> dict:
    movers = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(yahoo_fetch, f"{code}.T"): (code, name)
                   for code, name in TSE_STOCKS}
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


# ══════════════════════════════════════════════════════════════════════════════
# Finnhub backend — optional, requires API key
# ══════════════════════════════════════════════════════════════════════════════

FINNHUB_SYMBOLS = {
    "nikkei": ("^N225",  "stock"),
    "topix":  ("^TOPX",  "stock"),
    "usdjpy": ("USDJPY", "forex"),
    "myrjpy": ("MYRJPY", "forex"),
    "eurjpy": ("EURJPY", "forex"),
}

def finnhub_fetch(symbol: str, api_key: str, kind: str = "stock") -> dict:
    try:
        if kind == "forex":
            resp = requests.get(
                "https://finnhub.io/api/v1/forex/candle",
                params={"symbol": f"OANDA:{symbol}", "resolution": "D",
                        "count": 2, "token": api_key},
                timeout=12
            )
            d = resp.json()
            if d.get("s") == "ok" and d.get("c"):
                closes = d["c"]
                price = closes[-1]
                prev  = closes[-2] if len(closes) >= 2 else price
                chg   = price - prev
                pct   = (chg / prev * 100) if prev else 0
                return {"price": price, "change": chg, "pct_change": pct,
                        "state_label": "Last close", "source": "finnhub"}
        else:
            resp = requests.get(
                "https://finnhub.io/api/v1/quote",
                params={"symbol": symbol, "token": api_key},
                timeout=12
            )
            d = resp.json()
            price = d.get("c", 0) or d.get("pc", 0)
            prev  = d.get("pc", price)
            chg   = price - prev
            pct   = (chg / prev * 100) if prev else 0
            return {"price": price, "change": chg, "pct_change": pct,
                    "state_label": "Last close", "source": "finnhub"}
    except Exception as e:
        return {"price": 0, "error": str(e)}


def finnhub_fetch_all(api_key: str) -> dict:
    results = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(finnhub_fetch, sym, api_key, kind): key
                   for key, (sym, kind) in FINNHUB_SYMBOLS.items()}
        for f in as_completed(futures):
            key = futures[f]
            try:
                results[key] = f.result()
            except Exception as e:
                results[key] = {"price": 0, "error": str(e)}
    return results


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def _any_prices(results: dict) -> bool:
    return any(
        v.get("price", 0) > 0
        for k, v in results.items()
        if k != "_source" and isinstance(v, dict)
    )


def fetch_market_overview() -> dict:
    """
    Try Stooq → Yahoo Finance → Finnhub (if key available).
    Always returns a dict with keys: nikkei, topix, usdjpy, myrjpy, eurjpy.
    """
    # 1. Stooq (no key, most reliable from cloud)
    try:
        results = stooq_fetch_all()
        if _any_prices(results):
            results["_source"] = "stooq"
            print(f"✓ Market data from Stooq: {[(k, v.get('price',0)) for k,v in results.items() if k != '_source']}")
            return results
        print("Stooq returned no prices, trying Yahoo...")
    except Exception as e:
        print(f"Stooq failed: {e}")

    # 2. Yahoo Finance fallback
    try:
        results = yahoo_fetch_all()
        if _any_prices(results):
            results["_source"] = "yahoo"
            print(f"✓ Market data from Yahoo Finance")
            return results
        print("Yahoo returned no prices, trying Finnhub...")
    except Exception as e:
        print(f"Yahoo failed: {e}")

    # 3. Finnhub fallback (needs key)
    api_key = get_secret("FINNHUB_API_KEY")
    if api_key:
        try:
            results = finnhub_fetch_all(api_key)
            if _any_prices(results):
                results["_source"] = "finnhub"
                print(f"✓ Market data from Finnhub")
                return results
        except Exception as e:
            print(f"Finnhub failed: {e}")

    return {"_source": "error", "_error": "All market data sources failed"}


def fetch_tse_movers() -> dict:
    """Try Stooq first, fall back to Yahoo for TSE movers."""
    try:
        result = stooq_fetch_tse_movers()
        if result.get("all"):
            return result
    except Exception as e:
        print(f"Stooq movers failed: {e}")

    try:
        return yahoo_fetch_tse_movers()
    except Exception as e:
        print(f"Yahoo movers failed: {e}")
        return {"gainers": [], "losers": [], "all": [], "error": str(e)}


def fetch_foreign_flow() -> dict:
    """Foreign investor weekly flow from JPX public CSV."""
    try:
        url = (
            "https://www.jpx.co.jp/markets/statistics-equities/"
            "investor-type/nlsgeu000000484c-att/s13.csv"
        )
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if resp.status_code != 200:
            return {"available": False}
        lines = resp.text.strip().split("\n")
        for line in reversed(lines[-10:]):
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
