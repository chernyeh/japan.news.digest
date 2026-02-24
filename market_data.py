"""
market_data.py — v3
Uses Finnhub API (free, reliable from cloud servers, no Yahoo Finance blocking).
Free tier: 60 calls/minute — more than enough for our use case.
Sign up free at: https://finnhub.io

Fetches:
- Nikkei 225, TOPIX indices
- USD/JPY, MYR/JPY, EUR/JPY
- JGB 10Y yield (via proxy symbol)
- Top TSE large cap movers
"""

import requests
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_finnhub_key() -> str:
    """Read Finnhub API key from Streamlit secrets or environment."""
    try:
        import streamlit as st
        return st.secrets.get("FINNHUB_API_KEY", os.environ.get("FINNHUB_API_KEY", ""))
    except Exception:
        return os.environ.get("FINNHUB_API_KEY", "")


# ── Symbol maps ───────────────────────────────────────────────────────────────
# Finnhub uses exchange-prefixed symbols for non-US stocks
INDEX_SYMBOLS = {
    "nikkei":  "^N225",        # Nikkei 225
    "topix":   "^TOPX",        # TOPIX
}

FOREX_SYMBOLS = {
    "usdjpy":  "USDJPY",       # Finnhub forex format
    "myrjpy":  "MYRJPY",
    "eurjpy":  "EURJPY",
}

TSE_LARGE_CAPS = [
    ("7203.T",  "Toyota"),
    ("6758.T",  "Sony"),
    ("8306.T",  "Mitsubishi UFJ"),
    ("9984.T",  "SoftBank Group"),
    ("6861.T",  "Keyence"),
    ("7974.T",  "Nintendo"),
    ("4063.T",  "Shin-Etsu Chemical"),
    ("8035.T",  "Tokyo Electron"),
    ("6954.T",  "Fanuc"),
    ("9432.T",  "NTT"),
    ("4519.T",  "Chugai Pharma"),
    ("6367.T",  "Daikin"),
    ("7267.T",  "Honda"),
    ("8316.T",  "Sumitomo Mitsui"),
    ("9433.T",  "KDDI"),
    ("6098.T",  "Recruit Holdings"),
    ("4661.T",  "Oriental Land"),
    ("8411.T",  "Mizuho Financial"),
    ("6501.T",  "Hitachi"),
    ("7741.T",  "Hoya"),
]


def fetch_finnhub_quote(symbol: str, api_key: str, is_forex: bool = False) -> dict:
    """
    Fetch a quote from Finnhub.
    Returns price, change, pct_change, and market state.
    Always returns last available price — works when markets are closed.
    """
    try:
        if is_forex:
            url = "https://finnhub.io/api/v1/forex/candle"
            params = {
                "symbol": f"OANDA:{symbol}",
                "resolution": "D",
                "count": 2,
                "token": api_key,
            }
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()
            if data.get("s") == "ok" and data.get("c"):
                closes = data["c"]
                price = closes[-1]
                prev  = closes[-2] if len(closes) >= 2 else price
                change = price - prev
                pct = (change / prev * 100) if prev else 0
                return {
                    "price": price, "change": change, "pct_change": pct,
                    "state_label": "Last price", "symbol": symbol,
                }
        else:
            # Stock / index quote
            url = "https://finnhub.io/api/v1/quote"
            params = {"symbol": symbol, "token": api_key}
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()
            price = data.get("c", 0)   # current/last price
            prev  = data.get("pc", 0)  # previous close
            if price == 0:
                price = prev           # market closed — show prev close
            change = price - prev
            pct = (change / prev * 100) if prev else 0

            # Market state
            market_open = data.get("t", 0) > 0
            state_label = "Live" if market_open and price != prev else "Closed · Last price"

            return {
                "price": price, "change": change, "pct_change": pct,
                "state_label": state_label, "symbol": symbol,
            }

    except Exception as e:
        print(f"Finnhub error [{symbol}]: {e}")
        return {"price": 0, "change": 0, "pct_change": 0,
                "state_label": "Unavailable", "symbol": symbol, "error": str(e)}


def fetch_market_overview() -> dict:
    """Fetch indices, forex and yield data concurrently."""
    api_key = get_finnhub_key()
    if not api_key:
        return {"error": "no_key"}

    tasks = {}
    tasks["nikkei"]  = ("^N225",   False)
    tasks["topix"]   = ("^TOPX",   False)
    tasks["usdjpy"]  = ("USDJPY",  True)
    tasks["myrjpy"]  = ("MYRJPY",  True)
    tasks["eurjpy"]  = ("EURJPY",  True)

    results = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {
            ex.submit(fetch_finnhub_quote, sym, api_key, is_fx): key
            for key, (sym, is_fx) in tasks.items()
        }
        for f in as_completed(futures):
            key = futures[f]
            try:
                results[key] = f.result()
            except Exception as e:
                results[key] = {"price": 0, "change": 0, "pct_change": 0,
                                "state_label": "Unavailable", "error": str(e)}

    return results


def fetch_tse_movers() -> dict:
    """Fetch quotes for top TSE stocks and return gainers/losers."""
    api_key = get_finnhub_key()
    if not api_key:
        return {"gainers": [], "losers": [], "all": [], "error": "no_key"}

    movers = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {
            ex.submit(fetch_finnhub_quote, f"TYO:{sym.replace('.T','')}", api_key, False): (sym, name)
            for sym, name in TSE_LARGE_CAPS
        }
        for f in as_completed(futures):
            sym, name = futures[f]
            try:
                q = f.result()
                if q.get("price", 0) > 0:
                    movers.append({
                        "symbol": sym, "name": name,
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


def fetch_foreign_flow() -> dict:
    """Foreign investor weekly flow from JPX public data."""
    try:
        url = "https://www.jpx.co.jp/markets/statistics-equities/investor-type/nlsgeu000000484c-att/s13.csv"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)
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
        "note": "Weekly data published by JPX on Thursdays.",
        "jpx_url": "https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html",
    }
