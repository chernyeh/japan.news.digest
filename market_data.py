"""
market_data.py — v6
Fetches current price + historical data to compute multi-period returns.
Periods: MTD, 1M, 3M, YTD, 1Y, 3Y

Primary: Stooq CSV API (no key, unlimited history)
Fallback: Yahoo Finance v8 (no key)

Confirmed Stooq symbols:
  ^NKX    = Nikkei 225
  ^TPX    = TOPIX (all)
  ^TPXC30 = TOPIX Core 30
  ^TPXM400 = TOPIX Mid400 (to verify)
  ^TPXL70  = TOPIX Large 70 (instead of Mid400 if needed)
  Forex: USDJPY, EURJPY, CNYJPY, SGDJPY (Stooq format, no =X)
"""

import requests
import os
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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

INDICES = [
    # (key,          stooq_sym,   yahoo_sym,  label)
    ("nikkei",      "^NKX",      "^N225",    "Nikkei 225"),
    ("topix",       "^TPX",      "^TOPX",    "TOPIX"),
    ("topix_c30",   "^TPXC30",   None,       "TOPIX Core 30"),
    ("topix_m400",  "^TPXM400",  None,       "TOPIX Mid 400"),
    ("topix_1000",  "^TPX1000",  None,       "TOPIX 1000"),
    ("tse_growth",  "^TSEG250",  None,       "TSE Growth 250"),
]

FOREX = [
    # (key,      stooq_sym,  yahoo_sym,     label)
    ("usdjpy",  "USDJPY",   "USDJPY=X",    "USD/JPY"),
    ("eurjpy",  "EURJPY",   "EURJPY=X",    "EUR/JPY"),
    ("cnyjpy",  "CNYJPY",   "CNYJPY=X",    "CNY/JPY"),
    ("sgdjpy",  "SGDJPY",   "SGDJPY=X",    "SGD/JPY"),
]

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


# ── Return period helpers ─────────────────────────────────────────────────────

def compute_returns(sorted_rows: list[tuple]) -> dict:
    """
    Given a list of (date_str, close_price) sorted oldest→newest,
    compute returns for MTD, 1M, 3M, YTD, 1Y, 3Y.
    Returns dict of period → pct or None.
    """
    if not sorted_rows:
        return {}

    today = date.today()
    current_price = sorted_rows[-1][1]

    def find_price_on_or_before(target_date):
        """Find the most recent close on or before target_date."""
        target_str = target_date.strftime("%Y-%m-%d")
        best = None
        for d_str, price in sorted_rows:
            if d_str <= target_str:
                best = price
            else:
                break
        return best

    def pct(old, new):
        if old and old != 0:
            return (new - old) / old * 100
        return None

    periods = {
        "MTD":  date(today.year, today.month, 1) - timedelta(days=1),
        "1M":   today - timedelta(days=30),
        "3M":   today - timedelta(days=91),
        "YTD":  date(today.year - 1, 12, 31),
        "1Y":   today - timedelta(days=365),
        "3Y":   today - timedelta(days=365 * 3),
    }

    returns = {}
    for label, ref_date in periods.items():
        old_price = find_price_on_or_before(ref_date)
        returns[label] = pct(old_price, current_price)

    return returns


# ── Stooq fetcher ─────────────────────────────────────────────────────────────

def stooq_fetch_history(symbol: str, years: int = 4) -> dict:
    """
    Fetch daily history from Stooq covering `years` years.
    Returns dict with price, change, pct_change, returns, state_label.
    """
    try:
        end   = datetime.today()
        start = end - timedelta(days=years * 366)
        url = (
            f"https://stooq.com/q/d/l/?s={symbol.lower()}"
            f"&d1={start:%Y%m%d}&d2={end:%Y%m%d}&i=d"
        )
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return {"price": 0, "error": f"HTTP {resp.status_code}"}

        text = resp.text.strip()
        if not text or "No data" in text or "Przekroczony" in text or len(text) < 30:
            return {"price": 0, "error": "No data"}

        lines = [l.strip() for l in text.splitlines() if l.strip()]
        if len(lines) < 2:
            return {"price": 0, "error": "Too few rows"}

        rows = []
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) >= 5:
                try:
                    d_str = parts[0]    # YYYY-MM-DD
                    close = float(parts[4])
                    rows.append((d_str, close))
                except (ValueError, IndexError):
                    pass

        if not rows:
            return {"price": 0, "error": "No valid rows"}

        rows.sort(key=lambda x: x[0])  # ensure chronological order

        price  = rows[-1][1]
        prev   = rows[-2][1] if len(rows) >= 2 else price
        change = price - prev
        pct    = (change / prev * 100) if prev else 0
        rets   = compute_returns(rows)

        return {
            "price": price, "change": change, "pct_change": pct,
            "state_label": "Last close",
            "returns": rets,
            "symbol": symbol, "source": "stooq",
        }

    except Exception as e:
        return {"price": 0, "error": str(e), "symbol": symbol}


# ── Yahoo Finance fetcher ─────────────────────────────────────────────────────

def yahoo_fetch_history(symbol: str, years: int = 4) -> dict:
    """Fetch history from Yahoo Finance v8 chart API."""
    try:
        period = f"{years}y"
        for host in ["query1", "query2"]:
            try:
                url  = f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}"
                resp = requests.get(
                    url, headers=HEADERS,
                    params={"interval": "1d", "range": period},
                    timeout=15
                )
                if resp.status_code == 200:
                    break
            except Exception:
                continue
        else:
            return {"price": 0, "error": "Yahoo unreachable"}

        data    = resp.json()
        result  = data["chart"]["result"][0]
        meta    = result.get("meta", {})
        state   = meta.get("marketState", "CLOSED")
        timestamps = result.get("timestamp", [])
        closes  = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])

        # Build (date_str, price) rows
        rows = []
        for ts, cl in zip(timestamps, closes):
            if cl is not None:
                d_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                rows.append((d_str, cl))
        rows.sort(key=lambda x: x[0])

        if not rows:
            return {"price": 0, "error": "No closes"}

        # Current price — try meta first for intraday
        price = (
            meta.get("regularMarketPrice") or
            meta.get("postMarketPrice") or
            meta.get("preMarketPrice") or
            rows[-1][1]
        )
        prev   = rows[-2][1] if len(rows) >= 2 else price
        change = price - prev
        pct    = (change / prev * 100) if prev else 0

        # Inject today's price into rows for return calculations
        if rows and rows[-1][0] < date.today().strftime("%Y-%m-%d"):
            rows.append((date.today().strftime("%Y-%m-%d"), price))

        state_label = {
            "REGULAR": "Live", "PRE": "Pre-market",
            "POST": "After-hours",
        }.get(state, "Last close")

        return {
            "price": price, "change": change, "pct_change": pct,
            "state_label": state_label,
            "returns": compute_returns(rows),
            "symbol": symbol, "source": "yahoo",
        }

    except Exception as e:
        return {"price": 0, "error": str(e), "symbol": symbol}


# ── Fetch with Stooq→Yahoo fallback ──────────────────────────────────────────

def fetch_instrument(stooq_sym: str, yahoo_sym: str) -> dict:
    """Try Stooq, fall back to Yahoo if price is 0."""
    result = stooq_fetch_history(stooq_sym)
    if result.get("price", 0) > 0:
        return result
    if yahoo_sym:
        result2 = yahoo_fetch_history(yahoo_sym)
        if result2.get("price", 0) > 0:
            return result2
    return result  # return stooq result even if price=0 (has error msg)


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_market_overview() -> dict:
    """
    Fetch all indices and forex pairs with full return history.
    Returns {
      "indices": { key: {price, change, pct_change, returns, label, state_label} },
      "forex":   { key: {price, change, pct_change, returns, label, state_label} },
      "_source": "stooq" | "mixed",
    }
    """
    results = {"indices": {}, "forex": {}, "_source": "stooq"}

    all_tasks = (
        [(key, stooq, yahoo, label, "index") for key, stooq, yahoo, label in INDICES] +
        [(key, stooq, yahoo, label, "forex") for key, stooq, yahoo, label in FOREX]
    )

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {
            ex.submit(fetch_instrument, stooq, yahoo): (key, label, kind)
            for key, stooq, yahoo, label, kind in all_tasks
        }
        for f in as_completed(futures):
            key, label, kind = futures[f]
            try:
                data = f.result()
                data["label"] = label
                if kind == "index":
                    results["indices"][key] = data
                else:
                    results["forex"][key] = data
            except Exception as e:
                bucket = "indices" if kind == "index" else "forex"
                results[bucket][key] = {
                    "price": 0, "label": label, "error": str(e)
                }

    return results


def fetch_tse_movers() -> dict:
    """Fetch TSE large cap movers using Stooq, fallback Yahoo."""
    movers = []

    def fetch_one(code, name):
        r = stooq_fetch_history(f"{code}.jp", years=1)
        if r.get("price", 0) == 0:
            r = yahoo_fetch_history(f"{code}.T", years=1)
        return code, name, r

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(fetch_one, code, name) for code, name in TSE_STOCKS]
        for f in as_completed(futures):
            try:
                code, name, q = f.result()
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
