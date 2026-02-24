"""
market_data.py — v4
Primary: Finnhub API (requires FINNHUB_API_KEY in Streamlit Secrets)
Fallback: Yahoo Finance v8 chart API (no key needed, may be rate-limited on cloud)

Sign up for free Finnhub key at: https://finnhub.io (no credit card)
Then add to Streamlit Cloud: App Settings → Secrets → FINNHUB_API_KEY = "your_key"
"""

import requests
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


# ── Secret helper ─────────────────────────────────────────────────────────────
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
YAHOO_SYMBOLS = {
    "nikkei":  "^N225",
    "topix":   "^TOPX",
    "usdjpy":  "USDJPY=X",
    "myrjpy":  "MYRJPY=X",
    "eurjpy":  "EURJPY=X",
    "jgb10y":  "^JGBS",
}

FINNHUB_SYMBOLS = {
    "nikkei":  ("^N225",   "stock"),
    "topix":   ("^TOPX",   "stock"),
    "usdjpy":  ("USDJPY",  "forex"),
    "myrjpy":  ("MYRJPY",  "forex"),
    "eurjpy":  ("EURJPY",  "forex"),
}

TSE_LARGE_CAPS = [
    ("7203.T", "Toyota"),          ("6758.T", "Sony"),
    ("8306.T", "Mitsubishi UFJ"),  ("9984.T", "SoftBank Group"),
    ("6861.T", "Keyence"),         ("7974.T", "Nintendo"),
    ("4063.T", "Shin-Etsu Chem"),  ("8035.T", "Tokyo Electron"),
    ("6954.T", "Fanuc"),           ("9432.T", "NTT"),
    ("4519.T", "Chugai Pharma"),   ("6367.T", "Daikin"),
    ("7267.T", "Honda"),           ("8316.T", "Sumitomo Mitsui"),
    ("9433.T", "KDDI"),            ("6098.T", "Recruit Holdings"),
    ("4661.T", "Oriental Land"),   ("8411.T", "Mizuho Financial"),
    ("6501.T", "Hitachi"),         ("7741.T", "Hoya"),
]

YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


# ══════════════════════════════════════════════════════════════════════════════
# Yahoo Finance backend (no API key needed)
# ══════════════════════════════════════════════════════════════════════════════

def yahoo_fetch_quote(symbol: str) -> dict:
    """Fetch a single quote from Yahoo Finance v8 chart API."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        resp = requests.get(
            url, headers=YAHOO_HEADERS,
            params={"interval": "1d", "range": "5d"},
            timeout=12
        )
        if resp.status_code != 200:
            # Try query2 as fallback
            resp = requests.get(
                url.replace("query1", "query2"), headers=YAHOO_HEADERS,
                params={"interval": "1d", "range": "5d"},
                timeout=12
            )
        if resp.status_code != 200:
            return {"price": 0, "error": f"HTTP {resp.status_code}"}

        data = resp.json()
        result = data["chart"]["result"][0]
        meta   = result.get("meta", {})
        market_state = meta.get("marketState", "CLOSED")

        # Price: try live fields first, fall back to OHLC closes
        price = (
            meta.get("regularMarketPrice") or
            meta.get("postMarketPrice") or
            meta.get("preMarketPrice") or 0
        )
        prev = (
            meta.get("chartPreviousClose") or
            meta.get("previousClose") or
            meta.get("regularMarketPreviousClose") or 0
        )

        if price == 0:
            closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
            closes = [c for c in closes if c is not None]
            if closes:
                price = closes[-1]
                if len(closes) >= 2:
                    prev = closes[-2]

        if price == 0:
            return {"price": 0, "error": "No price in response"}

        if prev == 0:
            prev = price

        change = price - prev
        pct    = (change / prev * 100) if prev else 0

        state_label = {
            "REGULAR": "Live", "PRE": "Pre-market",
            "POST": "After-hours", "CLOSED": "Closed · Last price",
        }.get(market_state, "Last price")

        return {
            "price": price, "change": change, "pct_change": pct,
            "state_label": state_label, "symbol": symbol, "source": "yahoo",
        }
    except Exception as e:
        return {"price": 0, "error": str(e), "symbol": symbol}


def yahoo_fetch_all() -> dict:
    """Fetch all market overview symbols via Yahoo Finance concurrently."""
    results = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(yahoo_fetch_quote, sym): key
                   for key, sym in YAHOO_SYMBOLS.items()}
        for f in as_completed(futures):
            key = futures[f]
            try:
                results[key] = f.result()
            except Exception as e:
                results[key] = {"price": 0, "error": str(e)}
    return results


def yahoo_fetch_tse_movers() -> dict:
    """Fetch TSE large cap quotes from Yahoo Finance."""
    movers = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(yahoo_fetch_quote, sym): (sym, name)
                   for sym, name in TSE_LARGE_CAPS}
        for f in as_completed(futures):
            sym, name = futures[f]
            try:
                q = f.result()
                if q.get("price", 0) > 0:
                    movers.append({
                        "symbol": sym, "name": name,
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
# Finnhub backend (requires API key)
# ══════════════════════════════════════════════════════════════════════════════

def finnhub_fetch_quote(symbol: str, api_key: str, kind: str = "stock") -> dict:
    """Fetch a quote from Finnhub."""
    try:
        if kind == "forex":
            url = "https://finnhub.io/api/v1/forex/candle"
            params = {
                "symbol": f"OANDA:{symbol}",
                "resolution": "D", "count": 2,
                "token": api_key,
            }
            resp = requests.get(url, params=params, timeout=12)
            data = resp.json()
            if data.get("s") == "ok" and data.get("c"):
                closes = data["c"]
                price = closes[-1]
                prev  = closes[-2] if len(closes) >= 2 else price
                change = price - prev
                pct = (change / prev * 100) if prev else 0
                return {"price": price, "change": change, "pct_change": pct,
                        "state_label": "Last price", "source": "finnhub"}
        else:
            url = "https://finnhub.io/api/v1/quote"
            resp = requests.get(url, params={"symbol": symbol, "token": api_key}, timeout=12)
            data = resp.json()
            price = data.get("c", 0) or data.get("pc", 0)
            prev  = data.get("pc", 0)
            if not prev:
                prev = price
            change = price - prev
            pct = (change / prev * 100) if prev else 0
            return {"price": price, "change": change, "pct_change": pct,
                    "state_label": "Last price", "source": "finnhub"}
    except Exception as e:
        return {"price": 0, "error": str(e)}


def finnhub_fetch_all(api_key: str) -> dict:
    results = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(finnhub_fetch_quote, sym, api_key, kind): key
                   for key, (sym, kind) in FINNHUB_SYMBOLS.items()}
        for f in as_completed(futures):
            key = futures[f]
            try:
                results[key] = f.result()
            except Exception as e:
                results[key] = {"price": 0, "error": str(e)}
    return results


# ══════════════════════════════════════════════════════════════════════════════
# Public API — tries Finnhub first, falls back to Yahoo
# ══════════════════════════════════════════════════════════════════════════════

def fetch_market_overview() -> dict:
    """
    Fetch market overview. Uses Finnhub if key available, else Yahoo Finance.
    Always returns a dict. Individual keys have price=0 if unavailable.
    """
    api_key = get_secret("FINNHUB_API_KEY")

    if api_key:
        try:
            results = finnhub_fetch_all(api_key)
            # Check if we actually got data
            prices = [v.get("price", 0) for v in results.values()]
            if any(p > 0 for p in prices):
                results["_source"] = "finnhub"
                return results
            print("Finnhub returned all zeros, falling back to Yahoo")
        except Exception as e:
            print(f"Finnhub failed: {e}, falling back to Yahoo")

    # Yahoo Finance fallback
    try:
        results = yahoo_fetch_all()
        results["_source"] = "yahoo"
        return results
    except Exception as e:
        print(f"Yahoo also failed: {e}")
        return {"_source": "error", "_error": str(e)}


def fetch_tse_movers() -> dict:
    """Fetch TSE movers. Uses Yahoo Finance (Finnhub TSE coverage is limited)."""
    try:
        return yahoo_fetch_tse_movers()
    except Exception as e:
        print(f"TSE movers failed: {e}")
        return {"gainers": [], "losers": [], "all": [], "error": str(e)}


def fetch_foreign_flow() -> dict:
    """Foreign investor weekly flow from JPX public CSV."""
    try:
        url = "https://www.jpx.co.jp/markets/statistics-equities/investor-type/nlsgeu000000484c-att/s13.csv"
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
        "jpx_url": "https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html",
    }
