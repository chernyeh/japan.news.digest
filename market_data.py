"""
market_data.py
Fetches live market data for the Japan Business Digest investment dashboard.
Uses yfinance (free, no API key needed) for all market data.

Data fetched:
- Nikkei 225, TOPIX indices
- USD/JPY, MYR/JPY currency pairs
- JGB 10-year yield
- Top TSE movers
- BOJ policy rate
"""

import requests
from datetime import datetime, timedelta

# ── Symbols ───────────────────────────────────────────────────────────────────
MARKET_SYMBOLS = {
    "Nikkei 225":   "^N225",
    "TOPIX":        "^TPX",
    "JPX-N400":     "^JN400",
    "USD/JPY":      "USDJPY=X",
    "MYR/JPY":      "MYRJPY=X",
    "EUR/JPY":      "EURJPY=X",
    "JGB 10Y":      "^TNX",      # US proxy — Japan JGB via alternative below
    "JGB 10Y (JP)": "^JGBS",
}

# Top TSE listed companies to track for movers
TSE_LARGE_CAPS = [
    "7203.T",  # Toyota
    "6758.T",  # Sony
    "8306.T",  # Mitsubishi UFJ
    "9984.T",  # SoftBank Group
    "6861.T",  # Keyence
    "7974.T",  # Nintendo
    "4063.T",  # Shin-Etsu Chemical
    "8035.T",  # Tokyo Electron
    "6954.T",  # Fanuc
    "9432.T",  # NTT
    "4519.T",  # Chugai Pharmaceutical
    "6367.T",  # Daikin
    "7267.T",  # Honda
    "8316.T",  # Sumitomo Mitsui
    "9433.T",  # KDDI
    "6098.T",  # Recruit Holdings
    "4661.T",  # Oriental Land (Disney)
    "8411.T",  # Mizuho Financial
    "6501.T",  # Hitachi
    "7741.T",  # Hoya
]


def fetch_quote(symbol: str) -> dict:
    """Fetch a single quote using Yahoo Finance's chart API."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        params = {"interval": "1d", "range": "2d"}
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        data = resp.json()

        result = data["chart"]["result"][0]
        meta = result["meta"]

        price = meta.get("regularMarketPrice", 0)
        prev_close = meta.get("chartPreviousClose", meta.get("previousClose", price))
        change = price - prev_close
        pct = (change / prev_close * 100) if prev_close else 0

        return {
            "price": price,
            "change": change,
            "pct_change": pct,
            "currency": meta.get("currency", ""),
            "market_state": meta.get("marketState", ""),
            "symbol": symbol,
        }
    except Exception as e:
        print(f"Quote error [{symbol}]: {e}")
        return {"price": 0, "change": 0, "pct_change": 0, "currency": "", "error": str(e)}


def fetch_market_overview() -> dict:
    """Fetch all key market indicators."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    symbols = {
        "nikkei":   "^N225",
        "topix":    "^TP.TSE",
        "usdjpy":   "USDJPY=X",
        "myrjpy":   "MYRJPY=X",
        "eurjpy":   "EURJPY=X",
        "jgb10y":   "^JGBS",
    }

    results = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(fetch_quote, sym): key for key, sym in symbols.items()}
        for f in as_completed(futures):
            key = futures[f]
            try:
                results[key] = f.result()
            except Exception as e:
                results[key] = {"price": 0, "change": 0, "pct_change": 0, "error": str(e)}

    return results


def fetch_tse_movers() -> dict:
    """Fetch top gainers and losers among major TSE stocks."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    quotes = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_quote, sym): sym for sym in TSE_LARGE_CAPS}
        for f in as_completed(futures):
            sym = futures[f]
            try:
                q = f.result()
                if q.get("price", 0) > 0:
                    quotes[sym] = q
            except Exception:
                pass

    # Company name map
    names = {
        "7203.T": "Toyota", "6758.T": "Sony", "8306.T": "Mitsubishi UFJ",
        "9984.T": "SoftBank Group", "6861.T": "Keyence", "7974.T": "Nintendo",
        "4063.T": "Shin-Etsu Chemical", "8035.T": "Tokyo Electron",
        "6954.T": "Fanuc", "9432.T": "NTT", "4519.T": "Chugai Pharma",
        "6367.T": "Daikin", "7267.T": "Honda", "8316.T": "Sumitomo Mitsui",
        "9433.T": "KDDI", "6098.T": "Recruit Holdings",
        "4661.T": "Oriental Land", "8411.T": "Mizuho Financial",
        "6501.T": "Hitachi", "7741.T": "Hoya",
    }

    movers = []
    for sym, q in quotes.items():
        movers.append({
            "symbol": sym,
            "name": names.get(sym, sym),
            "price": q["price"],
            "change": q["change"],
            "pct_change": q["pct_change"],
        })

    movers.sort(key=lambda x: x["pct_change"], reverse=True)

    return {
        "gainers": [m for m in movers if m["pct_change"] > 0][:5],
        "losers":  [m for m in reversed(movers) if m["pct_change"] < 0][:5],
        "all":     movers,
    }


def fetch_foreign_flow() -> dict:
    """
    Fetch latest foreign investor flow data from JPX via their public CSV.
    Returns net buy/sell figure and trend.
    """
    try:
        # JPX publishes weekly investor type data
        url = "https://www.jpx.co.jp/markets/statistics-equities/investor-type/nlsgeu000000484c-att/s13.csv"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)

        if resp.status_code != 200:
            return {"available": False, "note": "JPX data temporarily unavailable"}

        # Parse CSV — JPX format has foreign investor row
        lines = resp.text.strip().split("\n")
        # Find most recent foreign net figure
        latest = None
        for line in lines[-10:]:
            parts = line.split(",")
            if len(parts) >= 4:
                try:
                    val = float(parts[-1].replace('"', '').replace(" ", ""))
                    latest = val
                except Exception:
                    pass

        if latest is not None:
            return {
                "available": True,
                "net_billion_yen": latest / 1e9,
                "direction": "Net Buying" if latest > 0 else "Net Selling",
                "as_of": "Latest week",
            }

    except Exception as e:
        print(f"Foreign flow error: {e}")

    return {
        "available": False,
        "note": "Check jpx.co.jp/english/markets/statistics-equities/investor-type for latest data",
        "jpx_url": "https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html",
    }


def format_number(n: float, decimals: int = 2) -> str:
    """Format a number with commas and sign."""
    if abs(n) >= 1000:
        return f"{n:,.0f}"
    return f"{n:,.{decimals}f}"


def format_change(pct: float) -> tuple:
    """Return (formatted string, color, arrow) for a percentage change."""
    arrow = "▲" if pct >= 0 else "▼"
    color = "#2E7D32" if pct >= 0 else "#C62828"
    return f"{arrow} {abs(pct):.2f}%", color
