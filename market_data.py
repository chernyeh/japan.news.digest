"""
market_data.py — v9

SOURCE WATERFALL (first success wins):
  1. Yahoo Finance via yfinance  — no key, works reliably from cloud IPs
  2. Stooq CSV                   — no key, may be blocked by datacenter IPs
  3. Alpha Vantage               — requires ALPHA_VANTAGE_KEY in Streamlit Secrets
                                   Free tier: 25 calls/day, 5/min

  Get free AV key: https://www.alphavantage.co/support/#api-key
  Add to Streamlit Secrets: ALPHA_VANTAGE_KEY = "your_key"
"""

import requests
import os
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False

BASE_AV = "https://www.alphavantage.co/query"
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


# ── Return calculator ─────────────────────────────────────────────────────────

def compute_returns(rows: list) -> dict:
    """rows: [(date_str, price)] sorted oldest→newest. Returns period→pct or None."""
    if not rows:
        return {p: None for p in ["MTD","1M","3M","YTD","1Y","3Y"]}

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
    return {label: pct(price_on_or_before(ref), current) for label, ref in targets.items()}


# ── Alpha Vantage ─────────────────────────────────────────────────────────────

def av_global_quote(symbol: str, api_key: str, label: str) -> dict:
    """Fetch current quote only — 1 API call, no history."""
    try:
        resp = requests.get(BASE_AV, params={
            "function": "GLOBAL_QUOTE",
            "symbol":   symbol,
            "apikey":   api_key,
        }, timeout=20)
        data = resp.json()

        if "Note" in data or "Information" in data:
            msg = data.get("Note", data.get("Information", "Rate limit"))
            return {"price": 0, "label": label, "error": "AV limit: " + msg[:60]}

        q = data.get("Global Quote", {})
        price = float(q.get("05. price", 0) or 0)
        prev  = float(q.get("08. previous close", 0) or 0)
        chg   = float(q.get("09. change", 0) or 0)
        pct   = float(q.get("10. change percent", "0%").replace("%","") or 0)

        if price == 0:
            return {"price": 0, "label": label, "error": "No price data"}

        return {
            "price": price, "change": chg, "pct_change": pct,
            "state_label": "Last close", "label": label,
            "returns": {p: None for p in ["MTD","1M","3M","YTD","1Y","3Y"]},
            "source": "alphavantage",
        }
    except Exception as e:
        return {"price": 0, "label": label, "error": str(e)}


def av_fx_daily(from_ccy: str, to_ccy: str, api_key: str, label: str) -> dict:
    """Fetch FX daily compact (100 days) — 1 API call, enough for MTD/1M/3M."""
    try:
        resp = requests.get(BASE_AV, params={
            "function":    "FX_DAILY",
            "from_symbol": from_ccy,
            "to_symbol":   to_ccy,
            "outputsize":  "compact",   # 100 days only — saves no extra calls but faster
            "apikey":      api_key,
        }, timeout=20)
        data = resp.json()

        if "Note" in data or "Information" in data:
            msg = data.get("Note", data.get("Information", "Rate limit"))
            return {"price": 0, "label": label, "error": "AV limit: " + msg[:60]}

        ts = data.get("Time Series FX (Daily)", {})
        if not ts:
            return {"price": 0, "label": label, "error": "No FX data"}

        rows = sorted([(d, float(v["4. close"])) for d, v in ts.items()], key=lambda x: x[0])
        price  = rows[-1][1]
        prev   = rows[-2][1] if len(rows) >= 2 else price
        change = price - prev
        pct    = (change / prev * 100) if prev else 0

        return {
            "price": price, "change": change, "pct_change": pct,
            "state_label": "Last close", "label": label,
            "returns": compute_returns(rows),
            "source": "alphavantage",
        }
    except Exception as e:
        return {"price": 0, "label": label, "error": str(e)}


# ── Stooq (secondary) ─────────────────────────────────────────────────────────

def stooq_fetch(symbol: str, label: str, years: int = 4) -> dict:
    """Stooq CSV — no key, full history for returns, but may timeout on cloud."""
    try:
        end   = datetime.today()
        start = end - timedelta(days=years * 366)
        url   = (
            f"https://stooq.com/q/d/l/?s={symbol.lower()}"
            f"&d1={start:%Y%m%d}&d2={end:%Y%m%d}&i=d"
        )
        resp = requests.get(url, headers=HEADERS, timeout=8)
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


# ── Yahoo Finance (primary — works from cloud IPs) ────────────────────────────

# Yahoo Finance ticker symbols
YF_INDEX_INSTRUMENTS = {
    "nikkei": ("^N225",   "Nikkei 225"),
    "topix":  ("^TOPX",   "TOPIX"),
}

YF_FOREX_INSTRUMENTS = {
    "usdjpy": ("JPY=X",    "USD/JPY"),
    "eurjpy": ("EURJPY=X", "EUR/JPY"),
    "cnyjpy": ("CNYJPY=X", "CNY/JPY"),
    "sgdjpy": ("SGDJPY=X", "SGD/JPY"),
}

# Yahoo Finance tickers for TSE movers  (appended .T for Tokyo Stock Exchange)
YF_TSE_SUFFIX = ".T"


def yf_fetch(ticker: str, label: str, years: int = 2) -> dict:
    """Fetch price + history via yfinance. Works reliably from Streamlit Cloud."""
    if not _YF_AVAILABLE:
        return {"price": 0, "label": label, "error": "yfinance not installed"}
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period=f"{years}y", auto_adjust=True)
        if hist.empty:
            # Fallback: just get fast_info
            info = t.fast_info
            price = getattr(info, "last_price", 0) or 0
            prev  = getattr(info, "previous_close", price) or price
            if not price:
                return {"price": 0, "label": label, "error": "No data"}
            chg = price - prev
            pct = (chg / prev * 100) if prev else 0
            return {
                "price": price, "change": chg, "pct_change": pct,
                "state_label": "Last close", "label": label,
                "returns": {p: None for p in ["MTD","1M","3M","YTD","1Y","3Y"]},
                "source": "yahoo",
            }

        rows = [(d.strftime("%Y-%m-%d"), float(c))
                for d, c in zip(hist.index, hist["Close"])
                if c and c == c]  # filter NaN
        if not rows:
            return {"price": 0, "label": label, "error": "Empty history"}

        rows.sort(key=lambda x: x[0])
        price  = rows[-1][1]
        prev   = rows[-2][1] if len(rows) >= 2 else price
        change = price - prev
        pct    = (change / prev * 100) if prev else 0

        return {
            "price": price, "change": change, "pct_change": pct,
            "state_label": "Last close", "label": label,
            "returns": compute_returns(rows),
            "source": "yahoo",
        }
    except Exception as e:
        return {"price": 0, "label": label, "error": str(e)}


# ── Instrument config ─────────────────────────────────────────────────────────

# AV equity symbols for Japan proxies
AV_EQUITY_INSTRUMENTS = {
    "nikkei": ("EWJ",  "Nikkei 225 (EWJ proxy)"),
    "topix":  ("EZJ",  "TOPIX (EZJ proxy)"),
}

# Stooq symbols — attempt to get real index data (overrides AV proxies if successful)
STOOQ_INDEX_INSTRUMENTS = {
    "nikkei":     ("^NKX",    "Nikkei 225"),
    "topix":      ("^TPX",    "TOPIX"),
    # Sub-indices removed — not reliably available from any free source without JS
}

AV_FOREX_INSTRUMENTS = {
    "usdjpy": ("USD", "JPY", "USD/JPY"),
    "eurjpy": ("EUR", "JPY", "EUR/JPY"),
    "cnyjpy": ("CNY", "JPY", "CNY/JPY"),
    "sgdjpy": ("SGD", "JPY", "SGD/JPY"),
}

STOOQ_FOREX_INSTRUMENTS = {
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


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_market_overview() -> dict:
    """
    Fetch all indices and forex using a waterfall:
      1. Yahoo Finance (yfinance) — reliable from cloud IPs, no key needed
      2. Stooq CSV                — no key, may be blocked from datacenter IPs
      3. Alpha Vantage            — requires ALPHA_VANTAGE_KEY in Streamlit Secrets

    Returns: {"indices": {key: data}, "forex": {key: data}, "_source": str, "_error": str}
    """
    api_key = get_secret("ALPHA_VANTAGE_KEY")
    results = {"indices": {}, "forex": {}}

    # ── Step 1: Yahoo Finance (concurrent, works from cloud IPs) ────────────
    if _YF_AVAILABLE:
        all_yf = {}
        all_yf.update({k: (sym, lbl, "index") for k, (sym, lbl) in YF_INDEX_INSTRUMENTS.items()})
        all_yf.update({k: (sym, lbl, "forex") for k, (sym, lbl) in YF_FOREX_INSTRUMENTS.items()})

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {
                ex.submit(yf_fetch, sym, lbl, 2): (k, kind)
                for k, (sym, lbl, kind) in all_yf.items()
            }
            for f in as_completed(futures):
                k, kind = futures[f]
                try:
                    d = f.result()
                    if d.get("price", 0) > 0:
                        bucket = "indices" if kind == "index" else "forex"
                        results[bucket][k] = d
                except Exception:
                    pass

    yf_got_indices = any(results["indices"].get(k, {}).get("price", 0) > 0
                         for k in ["nikkei", "topix"])
    yf_got_forex   = any(results["forex"].get(k, {}).get("price", 0) > 0
                         for k in ["usdjpy", "eurjpy"])

    # ── Step 2: Stooq for anything Yahoo missed ──────────────────────────────
    if not yf_got_indices or not yf_got_forex:
        stooq_tasks = {}
        if not yf_got_indices:
            stooq_tasks.update({k: (sym, lbl, "index")
                                 for k, (sym, lbl) in STOOQ_INDEX_INSTRUMENTS.items()})
        if not yf_got_forex:
            stooq_tasks.update({k: (sym, lbl, "forex")
                                 for k, (sym, lbl) in STOOQ_FOREX_INSTRUMENTS.items()})

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {
                ex.submit(stooq_fetch, sym, lbl, 4): (k, kind)
                for k, (sym, lbl, kind) in stooq_tasks.items()
            }
            for f in as_completed(futures):
                k, kind = futures[f]
                try:
                    d = f.result()
                    if d.get("price", 0) > 0:
                        bucket = "indices" if kind == "index" else "forex"
                        results[bucket][k] = d
                except Exception:
                    pass

    got_indices = any(results["indices"].get(k, {}).get("price", 0) > 0
                      for k in ["nikkei", "topix"])
    got_forex   = any(results["forex"].get(k, {}).get("price", 0) > 0
                      for k in ["usdjpy", "eurjpy"])

    # ── Step 3: Alpha Vantage for anything still missing ─────────────────────
    if api_key and (not got_indices or not got_forex):
        av_tasks = []
        if not got_indices:
            for key, (sym, label) in AV_EQUITY_INSTRUMENTS.items():
                av_tasks.append(("equity", key, sym, label))
        if not got_forex:
            for key, (fc, tc, label) in AV_FOREX_INSTRUMENTS.items():
                av_tasks.append(("forex", key, fc + "|" + tc, label))

        for i, task in enumerate(av_tasks):
            if i > 0 and i % 5 == 0:
                time.sleep(12)
            kind, key, sym, label = task
            if kind == "equity":
                d = av_global_quote(sym, api_key, label)
            else:
                fc, tc = sym.split("|")
                d = av_fx_daily(fc, tc, api_key, label)
            if d.get("price", 0) > 0:
                bucket = "indices" if kind == "equity" else "forex"
                results[bucket][key] = d

    # ── Determine source label ────────────────────────────────────────────────
    sources = set()
    for bucket in [results["indices"], results["forex"]]:
        for v in bucket.values():
            if isinstance(v, dict) and v.get("source"):
                sources.add(v["source"])

    results["_source"] = "+".join(sorted(sources)) if sources else "error"
    if not sources:
        results["_error"] = "All data sources failed or timed out."

    return results


def _stooq_all(results: dict):
    """Populate results entirely from Stooq (no AV key available)."""
    with ThreadPoolExecutor(max_workers=10) as ex:
        idx_f = {ex.submit(stooq_fetch, sym, label, 4): key
                 for key, (sym, label) in STOOQ_INDEX_INSTRUMENTS.items()}
        fx_f  = {ex.submit(stooq_fetch, sym, label, 4): key
                 for key, (sym, label) in STOOQ_FOREX_INSTRUMENTS.items()}
        for f in as_completed(idx_f):
            key = idx_f[f]
            try:
                results["indices"][key] = f.result()
            except Exception as e:
                results["indices"][key] = {"price": 0, "error": str(e)}
        for f in as_completed(fx_f):
            key = fx_f[f]
            try:
                results["forex"][key] = f.result()
            except Exception as e:
                results["forex"][key] = {"price": 0, "error": str(e)}


def fetch_tse_movers() -> dict:
    """Fetch TSE movers — tries Yahoo Finance first, falls back to Stooq."""
    movers = []

    def _fetch_one(code, name):
        # Try Yahoo Finance (.T suffix) first
        if _YF_AVAILABLE:
            d = yf_fetch(f"{code}{YF_TSE_SUFFIX}", name, 1)
            if d.get("price", 0) > 0:
                return d
        # Fallback to Stooq (.jp suffix)
        return stooq_fetch(f"{code}.jp", name, 1)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {
            ex.submit(_fetch_one, code, name): (code, name)
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
