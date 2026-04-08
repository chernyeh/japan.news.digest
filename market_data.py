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
        return {p: None for p in ["MTD","1M","3M","6M","YTD","1Y","3Y"]}

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
        "6M":  today - timedelta(days=182),
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
            "returns": {p: None for p in ["MTD","1M","3M","6M","YTD","1Y","3Y"]},
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
                "returns": {p: None for p in ["MTD","1M","3M","6M","YTD","1Y","3Y"]},
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
    # Mega caps / Top 30
    ("7203","Toyota Motor"),("8306","Mitsubishi UFJ Financial"),("6758","Sony Group"),
    ("9984","SoftBank Group"),("6861","Keyence"),("7974","Nintendo"),
    ("4063","Shin-Etsu Chemical"),("8035","Tokyo Electron"),("6954","Fanuc"),
    ("9432","NTT"),("4519","Chugai Pharmaceutical"),("6367","Daikin Industries"),
    ("7267","Honda Motor"),("8316","Sumitomo Mitsui Financial"),("9433","KDDI"),
    ("6098","Recruit Holdings"),("4661","Oriental Land"),("8411","Mizuho Financial"),
    ("6501","Hitachi"),("7741","Hoya"),("8001","Itochu"),("8058","Mitsubishi Corp"),
    ("8031","Mitsui & Co"),("8002","Marubeni"),("9434","SoftBank Corp"),
    ("4502","Takeda Pharmaceutical"),("6902","Denso"),("7751","Canon"),
    ("5401","Nippon Steel"),("8725","MS&AD Insurance"),
    # Tech & Semiconductors
    ("6702","Fujitsu"),("6971","Kyocera"),("6723","Renesas Electronics"),
    ("6594","Nidec"),("9613","NTT Data"),("6645","OMRON"),
    ("7735","Screen Holdings"),("6146","Disco Corp"),("6920","Lasertec"),
    ("4704","Trend Micro"),("6503","Mitsubishi Electric"),("6504","Fuji Electric"),
    ("6770","Alps Alpine"),("6752","Panasonic Holdings"),("6753","Sharp"),
    ("6479","Minebea Mitsumi"),("6771","Anritsu"),
    ("4901","Fujifilm Holdings"),("4902","Konica Minolta"),
    # Financials
    ("8750","Dai-ichi Life"),("8766","Tokio Marine"),("8604","Nomura Holdings"),
    ("8601","Daiwa Securities"),("8308","Resona Holdings"),
    ("8309","Sumitomo Mitsui Trust"),("8354","Fukuoka Financial"),
    ("8253","Credit Saison"),("8697","JPX Group"),
    ("8795","T&D Holdings"),("8630","Sompo Holdings"),
    # Industrials & Heavy Industry
    ("6326","Kubota"),("7011","Mitsubishi Heavy Industries"),("7012","Kawasaki Heavy"),
    ("7013","IHI Corp"),("6301","Komatsu"),("6302","Sumitomo Heavy Industries"),
    ("7752","Ricoh"),("6471","NSK"),("6472","NTN"),("6473","JTEKT"),
    ("6113","Amada Holdings"),("6103","Okuma"),("6141","DMG Mori"),
    # Automotive
    ("7201","Nissan Motor"),("7202","Isuzu Motors"),("7205","Hino Motors"),
    ("7211","Mitsubishi Motors"),("7269","Suzuki Motor"),("7270","Subaru"),
    ("7272","Yamaha Motor"),("7261","Mazda Motor"),("5108","Bridgestone"),
    ("5101","Yokohama Rubber"),("5110","Sumitomo Rubber"),
    ("6856","Horiba"),("7731","Nikon"),("7733","Olympus"),
    # Chemicals & Materials
    ("4005","Sumitomo Chemical"),("4183","Mitsui Chemicals"),
    ("4188","Mitsubishi Chemical Group"),("4208","UBE"),
    ("3436","SUMCO"),("4042","Tosoh"),("4091","Nippon Sanso"),
    ("4182","Mitsubishi Gas Chemical"),("4272","Nippon Kayaku"),
    ("5706","Mitsui Mining & Smelting"),("5714","DOWA Holdings"),
    ("5803","Fujikura"),("5802","Sumitomo Electric"),("5801","Furukawa Electric"),
    ("3407","Asahi Kasei"),
    # Energy & Utilities
    ("5019","Idemitsu Kosan"),("5020","ENEOS Holdings"),("1605","INPEX"),
    ("9501","Tokyo Electric Power"),("9502","Chubu Electric Power"),
    ("9503","Kansai Electric Power"),("9531","Tokyo Gas"),("9532","Osaka Gas"),
    # Pharma & Healthcare
    ("4503","Astellas Pharma"),("4506","Sumitomo Pharma"),("4507","Shionogi"),
    ("4151","Kyowa Kirin"),("4523","Eisai"),("4568","Daiichi Sankyo"),
    ("4530","Hisamitsu Pharmaceutical"),("4536","Santen Pharmaceutical"),
    ("4543","Terumo"),
    # Consumer Staples
    ("2802","Ajinomoto"),("2914","Japan Tobacco"),("2503","Kirin Holdings"),
    ("2502","Asahi Group Holdings"),("2501","Sapporo Holdings"),
    ("2269","Meiji Holdings"),("2871","Nichirei"),("2282","NH Foods"),
    ("2593","ITO EN"),("4452","Kao"),("4911","Shiseido"),
    ("4922","Kose"),("4927","Pola Orbis"),
    # Retail & Consumer Discretionary
    ("3382","Seven & i Holdings"),("8267","Aeon"),("3099","Isetan Mitsukoshi"),
    ("3086","J. Front Retailing"),("8233","Takashimaya"),
    ("9983","Fast Retailing"),("3092","ZOZO"),("2651","Lawson"),
    ("8028","FamilyMart"),
    # Real Estate
    ("8801","Mitsui Fudosan"),("8802","Mitsubishi Estate"),("8830","Sumitomo Realty"),
    ("3289","Tokyu Fudosan Holdings"),("3003","Hulic"),("8804","Tokyo Tatemono"),
    ("3231","Nomura Real Estate"),
    # Construction
    ("1801","Taisei Corp"),("1802","Obayashi Corp"),("1803","Shimizu Corp"),
    ("1812","Kajima Corp"),("1928","Sekisui House"),("1925","Daiwa House Industry"),
    ("5233","Taiheiyo Cement"),
    # Transport & Logistics
    ("9064","Yamato Holdings"),("9062","Nippon Express Holdings"),
    ("9101","Nippon Yusen"),("9104","Mitsui OSK Lines"),("9107","Kawasaki Kisen"),
    ("9020","JR East"),("9021","JR West"),("9022","JR Central"),
    ("9001","Tobu Railway"),("9005","Tokyu"),("9007","Odakyu Electric"),
    # Telecoms & Media
    ("4689","LY Corp"),("4751","CyberAgent"),("2433","Hakuhodo DY Holdings"),
    ("4324","Dentsu Group"),("4385","Mercari"),("3659","Nexon"),
    ("4755","Rakuten Group"),("2432","DeNA"),
    # Trading houses
    ("8015","Toyota Tsusho"),("8053","Sumitomo Corp"),
    # Misc large caps
    ("6988","Nitto Denko"),("4021","Nissan Chemical"),
    ("2768","Sojitz"),("9766","Konami Group"),("7832","Bandai Namco Holdings"),
    ("2784","Alfresa Holdings"),("3197","Skylark Holdings"),
    ("6367","Daikin Industries"),("5631","Japan Steel Works"),
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



def fetch_jpx_daily_movers() -> dict:
    """
    Fetch today's top gainers/losers from JPX daily CSV.
    JPX publishes a full-market equity price file each trading day.
    Returns top 10 gainers and losers by % change, with name and sector.
    Falls back to empty if unavailable.
    """
    # JPX publishes daily data; URL pattern uses today's date
    from datetime import date as _date
    today = _date.today()
    # Try last few trading days in case today is weekend/holiday
    for delta in range(5):
        d = today - timedelta(days=delta)
        # Skip weekends
        if d.weekday() >= 5:
            continue
        date_str = d.strftime("%Y%m%d")
        url = f"https://www.jpx.co.jp/markets/statistics-equities/daily/nlsgeu000000{date_str}-att/data_{date_str}.csv"
        try:
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            if resp.status_code != 200:
                continue
            # JPX CSV: Code, Name, Market, Sector, Close, PrevClose, Change, ChangeRatio, Volume, Value
            rows = []
            for line in resp.content.decode("shift_jis", errors="replace").splitlines()[1:]:
                parts = line.split(",")
                if len(parts) < 8:
                    continue
                try:
                    code   = parts[0].strip().strip('"')
                    name   = parts[1].strip().strip('"')
                    sector = parts[3].strip().strip('"') if len(parts) > 3 else ""
                    close  = float(parts[4].replace(",","").strip()) if parts[4].strip() else 0
                    prev   = float(parts[5].replace(",","").strip()) if parts[5].strip() else 0
                    chg_r  = float(parts[7].replace(",","").replace("%","").strip()) if parts[7].strip() else 0
                    if close > 0 and prev > 0 and code.isdigit():
                        rows.append({
                            "code": code, "name": name, "sector": sector,
                            "price": close, "prev": prev,
                            "change": close - prev, "pct_change": chg_r,
                        })
                except (ValueError, IndexError):
                    continue
            if rows:
                rows.sort(key=lambda x: x["pct_change"], reverse=True)
                return {
                    "date": d.strftime("%d %b %Y"),
                    "gainers": [r for r in rows if r["pct_change"] > 0][:10],
                    "losers":  list(reversed([r for r in rows if r["pct_change"] < 0]))[:10],
                    "total_stocks": len(rows),
                    "advancing": sum(1 for r in rows if r["pct_change"] > 0),
                    "declining": sum(1 for r in rows if r["pct_change"] < 0),
                    "unchanged": sum(1 for r in rows if r["pct_change"] == 0),
                    "source": "jpx",
                }
        except Exception as e:
            print(f"JPX CSV error ({date_str}): {e}")
            continue
    return {"gainers": [], "losers": [], "source": "unavailable"}


def fetch_topix_returns() -> dict:
    """
    Fetch TOPIX historical returns for benchmark comparison.
    Returns: {'3M': pct, '6M': pct, '12M': pct, 'price': float}
    """
    if not _YF_AVAILABLE:
        return {}
    try:
        t = yf.Ticker("^TOPX")
        hist = t.history(period="13mo", auto_adjust=True)
        if hist.empty:
            return {}
        rows = [(d.strftime("%Y-%m-%d"), float(c))
                for d, c in zip(hist.index, hist["Close"]) if c == c]
        rows.sort(key=lambda x: x[0])
        if not rows:
            return {}
        current = rows[-1][1]
        rets = compute_returns(rows)
        return {"price": current, "3M": rets.get("3M"), "6M": rets.get("6M"), "12M": rets.get("1Y")}
    except Exception as e:
        print(f"TOPIX benchmark error: {e}")
        return {}


def fetch_stock_performance(code: str, name: str) -> dict:
    """
    Fetch a single stock's 3M/6M/12M returns for underperformance screening.
    Returns dict with returns and underperformance vs TOPIX.
    """
    if not _YF_AVAILABLE:
        return {"code": code, "name": name, "error": "yfinance unavailable"}
    try:
        ticker = f"{code}{YF_TSE_SUFFIX}"
        t = yf.Ticker(ticker)
        hist = t.history(period="13mo", auto_adjust=True)
        if hist.empty:
            return {"code": code, "name": name, "price": 0, "error": "No data"}
        rows = [(d.strftime("%Y-%m-%d"), float(c))
                for d, c in zip(hist.index, hist["Close"]) if c == c]
        rows.sort(key=lambda x: x[0])
        if not rows:
            return {"code": code, "name": name, "price": 0, "error": "Empty"}
        price  = rows[-1][1]
        prev   = rows[-2][1] if len(rows) >= 2 else price
        rets   = compute_returns(rows)
        return {
            "code": code, "name": name, "price": price,
            "change": price - prev, "pct_change": (price - prev) / prev * 100 if prev else 0,
            "ret_3m":  rets.get("3M"),
            "ret_6m":  rets.get("6M"),
            "ret_12m": rets.get("1Y"),
            "ret_mtd": rets.get("MTD"),
            "symbol": ticker,
        }
    except Exception as e:
        return {"code": code, "name": name, "price": 0, "error": str(e)}


def _batch_yf_screen(codes_names: list, topix_3m, topix_6m, topix_12m) -> list:
    """
    Batch-fetch all TSE_STOCKS using yf.download() — one HTTP request for all tickers.
    ~10x faster than fetching stocks one by one.
    """
    if not _YF_AVAILABLE:
        return []

    tickers = [f"{c}{YF_TSE_SUFFIX}" for c, _ in codes_names]
    code_map = {f"{c}{YF_TSE_SUFFIX}": (c, n) for c, n in codes_names}

    try:
        # Download 13 months of daily closes for all tickers at once
        raw = yf.download(
            tickers, period="13mo", auto_adjust=True,
            progress=False, threads=True, group_by="ticker"
        )
    except Exception as e:
        print(f"Batch yf.download error: {e}")
        return []

    results = []
    for ticker in tickers:
        code, name = code_map.get(ticker, ("", ""))
        try:
            # Multi-ticker download returns MultiIndex columns
            if len(tickers) > 1:
                closes = raw[ticker]["Close"] if ticker in raw.columns.get_level_values(0) else None
            else:
                closes = raw["Close"]
            if closes is None or closes.empty:
                continue

            closes = closes.dropna()
            if len(closes) < 2:
                continue

            rows = [(d.strftime("%Y-%m-%d"), float(c))
                    for d, c in zip(closes.index, closes.values)]
            rows.sort(key=lambda x: x[0])

            price  = rows[-1][1]
            prev   = rows[-2][1]
            rets   = compute_returns(rows)
            r3, r6, r12 = rets.get("3M"), rets.get("6M"), rets.get("1Y")

            results.append({
                "code": code, "name": name, "price": price,
                "change": price - prev,
                "pct_change": (price - prev) / prev * 100 if prev else 0,
                "ret_3m": r3, "ret_6m": r6, "ret_12m": r12,
                "ret_mtd": rets.get("MTD"), "symbol": ticker,
                "under_3m":  (r3  - topix_3m)  if r3  is not None and topix_3m  is not None else None,
                "under_6m":  (r6  - topix_6m)  if r6  is not None and topix_6m  is not None else None,
                "under_12m": (r12 - topix_12m) if r12 is not None and topix_12m is not None else None,
            })
        except Exception:
            continue

    return results


def fetch_underperformance_screen(topix_returns: dict = None, max_workers: int = 20) -> list:
    """
    Screen all TSE_STOCKS for underperformance vs TOPIX over 3M, 6M, 12M.
    Uses batched yf.download() for speed (~15–20s vs 60–90s one-by-one).
    """
    if topix_returns is None:
        topix_returns = fetch_topix_returns()

    topix_3m  = topix_returns.get("3M")
    topix_6m  = topix_returns.get("6M")
    topix_12m = topix_returns.get("12M")

    # Try fast batch path first
    results = _batch_yf_screen(TSE_STOCKS, topix_3m, topix_6m, topix_12m)

    # Fall back to per-stock fetch for any missing
    fetched_codes = {d["code"] for d in results}
    missing = [(c, n) for c, n in TSE_STOCKS if c not in fetched_codes]
    if missing:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {
                ex.submit(fetch_stock_performance, code, name): (code, name)
                for code, name in missing
            }
            for f in as_completed(futures):
                try:
                    d = f.result()
                    if d.get("price", 0) > 0:
                        r3, r6, r12 = d.get("ret_3m"), d.get("ret_6m"), d.get("ret_12m")
                        d["under_3m"]  = (r3  - topix_3m)  if r3  is not None and topix_3m  is not None else None
                        d["under_6m"]  = (r6  - topix_6m)  if r6  is not None and topix_6m  is not None else None
                        d["under_12m"] = (r12 - topix_12m) if r12 is not None and topix_12m is not None else None
                        results.append(d)
                except Exception:
                    pass

    results.sort(key=lambda x: x.get("under_12m") or 0)
    return results


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
