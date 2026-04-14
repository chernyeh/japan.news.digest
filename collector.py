"""
collector.py  —  Phase 2
- Batch translation via DeepL free API (fast) with MyMemory fallback
- Additional RSS: Toyo Keizai, Diamond Online, Nikkei JP
- HTML scrapers for trade papers: Nikkan Kogyo, Nikkan Jidosha, Denki Shimbun,
  Dempa Shimbun, Kagaku Kogyo Nippo, Japan Marine Daily, Nikkan Kensetsu, Nihon Nogyo
- Concurrent fetching for speed
"""

import feedparser
import requests
import re
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── RSS Sources ───────────────────────────────────────────────────────────────
RSS_SOURCES = [
    # English (no translation needed)
    # Japan Times blocks cloud IPs on their native feed — use Google News proxy
    ("Japan Times Business", "https://news.google.com/rss/search?q=site:japantimes.co.jp+business&hl=en&gl=JP&ceid=JP:en", "en"),
    # ── Nikkei Group feeds ──────────────────────────────────────────────────────
    # Nikkei Asia (English) — confirmed working
    ("Nikkei Asia",           "https://asia.nikkei.com/rss/feed/nar",                        "en"),
    # Nikkei.com (Japanese flagship) — no direct RSS; Google News RSS proxy works reliably
    ("Nikkei Shimbun",        "https://news.google.com/rss/search?q=site:nikkei.com&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    # Nikkei Business (日経ビジネス) — confirmed RSS
    ("Nikkei Business",       "https://business.nikkei.com/rss/sns/nb.rdf",                  "ja"),
    # Nikkei Xtech (日経クロステック) — tech/manufacturing/auto/energy
    ("Nikkei Xtech",          "https://xtech.nikkei.com/rss/index.rdf",                      "ja"),
    ("Nikkei Xtech IT",       "https://xtech.nikkei.com/rss/xtech-it.rdf",                   "ja"),
    ("Nikkei Xtech Auto",     "https://xtech.nikkei.com/rss/xtech-at.rdf",                   "ja"),
    # Reuters killed public RSS in 2020. Use Google News proxy — search for Reuters Japan coverage.
    ("Reuters Japan",        "https://news.google.com/rss/search?q=reuters+japan+economy+OR+business+OR+markets&hl=en&gl=JP&ceid=JP:en", "en"),
    ("NHK World Business",   "https://www3.nhk.or.jp/nhkworld/en/news/feeds/business.xml", "en"),
    ("Japan Industry News",  "https://japanindustrynews.com/feed/",                         "en"),
    # Japanese (translated)
    ("Asahi Shimbun",        "https://rss.asahi.com/rss/asahi/newsheadlines.rdf",           "ja"),
    ("NHK Economics",        "https://www3.nhk.or.jp/rss/news/cat4.xml",                    "ja"),
    ("NHK Business",         "https://www3.nhk.or.jp/rss/news/cat5.xml",                    "ja"),
    ("Mainichi Shimbun",     "https://news.google.com/rss/search?q=site:mainichi.jp+%E7%B5%8C%E6%B8%88+OR+%E3%83%93%E3%82%B8%E3%83%8D%E3%82%B9&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    ("Sankei Shimbun",       "https://news.google.com/rss/search?q=site:sankei.com+%E7%B5%8C%E6%B8%88+OR+%E7%94%A3%E6%A5%AD&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    ("Yahoo Japan Business", "https://news.yahoo.co.jp/rss/topics/business.xml",            "ja"),
    ("Yahoo Japan Economy",  "https://news.yahoo.co.jp/rss/topics/economy.xml",             "ja"),
    ("IT Media Business",    "https://rss.itmedia.co.jp/rss/2.0/business_media.xml",        "ja"),
    ("Toyo Keizai",          "https://toyokeizai.net/list/feed/rss",                        "ja"),
    ("Diamond Online",       "https://diamond.jp/list/feed/rss/dol",                        "ja"),
    ("Nikkan Kogyo",         "https://news.google.com/rss/search?q=site:nikkan.co.jp&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    # ── Company / Micro news (earnings, M&A, guidance, analyst) ────────────────
    # Nikkei earnings & corporate actions
    ("Nikkei IR / Earnings",  "https://news.google.com/rss/search?q=site:nikkei.com+(決算OR業績OR増益OR減益OR配当OR自社株買い)&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    # Kabutan — Japan's primary dedicated earnings / IR news site
    ("Kabutan Corporate",     "https://kabutan.jp/rss/news_corporate.xml",                   "ja"),
    ("Kabutan Earnings",      "https://kabutan.jp/rss/news_kessan.xml",                       "ja"),
    # Minkabu — retail investor/analyst commentary, stock-specific
    ("Minkabu",               "https://minkabu.jp/rss/news",                                  "ja"),
    # Traders Web — corporate disclosures, earnings, analyst ratings
    ("Traders Web",           "https://www.traders.co.jp/news/rss_all.aspx",                  "ja"),
    # Reuters company-specific Japan
    ("Reuters Japan Companies", "https://news.google.com/rss/search?q=reuters+japan+(earnings+OR+profit+OR+forecast+OR+acquisition+OR+merger+OR+dividend)&hl=en&gl=JP&ceid=JP:en", "en"),
    # Bloomberg Japan company news via Google News proxy
    ("Bloomberg Japan",      "https://news.google.com/rss/search?q=bloomberg+japan+economy+OR+markets+OR+business&hl=en&gl=JP&ceid=JP:en", "en"),
    ("Bloomberg Japan Co",    "https://news.google.com/rss/search?q=bloomberg+japan+(earnings+OR+results+OR+forecast+OR+buyback+OR+dividend+OR+acquisition)&hl=en&gl=JP&ceid=JP:en", "en"),

    # ── New sources ──────────────────────────────────────────────────────────────
    # Kabutan Market News — morning/evening stock movers, "話題株" pre-open highlights
    ("Kabutan Market News",   "https://kabutan.jp/rss/news_marketnews.xml",            "ja"),

    # Fisco — stock analyst commentary, individual stock analysis, morning market notes
    ("Fisco",                 "https://news.google.com/rss/search?q=site:fisco.jp+OR+site:web.fisco.jp&hl=ja&gl=JP&ceid=JP:ja", "ja"),

    # Jiji Press — Japan wire service, fast on BOJ/government/corporate events
    ("Jiji Press",            "https://www.jiji.com/rss/ranking.rdf",                  "ja"),

    # JBpress — English/Japanese business analysis, strategy, international economy
    ("JBpress",               "https://jbpress.ismedia.jp/rss/latest",                 "ja"),

    # TSE Manebu (東証マネ部) — TSE investor education and disclosure guidance
    ("TSE Manebu",            "https://news.google.com/rss/search?q=site:jpx.co.jp+%E6%9D%B1%E8%A8%BC%E3%83%9E%E3%83%8D%E9%83%A8&hl=ja&gl=JP&ceid=JP:ja", "ja"),

    # President Online — CEO/business leadership, corporate strategy
    ("President Online",      "https://president.jp/list/feed/rss",                   "ja"),

    # Rakumachi — real estate investment news, J-REIT, property market
    ("Rakumachi",             "https://news.google.com/rss/search?q=site:rakumachi.jp&hl=ja&gl=JP&ceid=JP:ja", "ja"),

    # Zaikai Online — business leadership, major corporate news
    ("Zaikai Online",         "https://news.google.com/rss/search?q=site:zaikai.net&hl=ja&gl=JP&ceid=JP:ja", "ja"),

    # QUICK Money World — institutional-grade Japan market commentary
    ("QUICK Money World",     "https://news.google.com/rss/search?q=site:moneyworld.jp&hl=ja&gl=JP&ceid=JP:ja", "ja"),
]

# ── Trade paper scrape targets ────────────────────────────────────────────────
SCRAPE_SOURCES = [
    # Nikkan Kogyo moved to RSS_SOURCES (has RSS feed)
    ("Nikkan Jidosha",       "https://www.njd.jp/",                "h2, h3, .article-title, .news-list li a",    "ja"),
    ("Denki Shimbun",        "https://www.denkishimbun.com/",      "h2, h3, .article-title, .news-item a",       "ja"),
    ("Dempa Shimbun",        "https://www.dempa.com/",             "h2, h3, .article-title, .headline",          "ja"),
    ("Kagaku Kogyo Nippo",   "https://www.kagakukogyonippo.com/",  "h2, h3, .article-title, .headline-list a",   "ja"),
    ("Japan Marine Daily",   "https://www.jmd.co.jp/",             "h2, h3, .article-title, .news-title",        "ja"),
    ("Nikkan Kensetsu",      "https://www.constnews.com/",         "h2, h3, .article-title, .news-list a",       "ja"),
    ("Nihon Nogyo Shimbun",  "https://www.agrinews.co.jp/",        "h2, h3, .article-title, .headline",          "ja"),
    ("FACTA",                 "https://facta.co.jp/",               "h3",                                         "ja"),
]

# ── MSCI Sector keywords ──────────────────────────────────────────────────────
SECTOR_KEYWORDS = {
    "Energy": [
        "oil","gas","petroleum","energy","fuel","refinery","crude","lng",
        "solar","wind power","nuclear","power plant","electricity","renewable",
        "石油","ガス","エネルギー","燃料","原油","発電","電力","再生可能","原発",
    ],
    "Materials": [
        "steel","iron","aluminum","copper","chemical","plastic","rubber",
        "mining","metal","material","cement","paper","pulp","fiber","resin",
        "鉄鋼","アルミ","銅","化学","素材","金属","セメント","紙","繊維","樹脂",
    ],
    "Industrials": [
        "manufacturing","factory","industrial","machinery","equipment",
        "aerospace","defense","logistics","transport","shipping","railroad",
        "construction","infrastructure","engineering","robot","automation",
        "marine","shipbuilding",
        "製造","工場","機械","設備","物流","輸送","建設","インフラ","ロボット","造船",
    ],
    "Consumer Discretionary": [
        "retail","automobile","car","vehicle","toyota","honda","nissan",
        "fashion","apparel","luxury","restaurant","hotel","travel","tourism",
        "gaming","entertainment","advertising","e-commerce","ev ","electric vehicle",
        "小売","自動車","ファッション","ホテル","旅行","観光","ゲーム","広告","電気自動車",
    ],
    "Consumer Staples": [
        "food","beverage","drink","grocery","supermarket","tobacco",
        "household","cosmetic","beauty","personal care","agriculture","farming",
        "食品","飲料","食料品","スーパー","タバコ","化粧品","農業","農産物",
    ],
    "Health Care": [
        "health","medical","hospital","pharma","drug","vaccine","biotech",
        "clinical","patient","doctor","treatment","cancer","medicine",
        "医療","病院","製薬","薬","ワクチン","バイオ","臨床","患者","治療","創薬",
    ],
    "Financials": [
        "bank","finance","investment","insurance","asset management",
        "stock market","bond","yen","currency","credit","loan","interest rate",
        "boj","bank of japan","securities","fintech","nikkei 225","topix",
        "銀行","金融","投資","保険","株式","債券","円","為替","融資","金利","日銀",
    ],
    "Information Technology": [
        "technology","software","semiconductor","chip","ai","artificial intelligence",
        "cloud","cyber","digital","data","internet","startup","tech",
        "5g","quantum","saas","hardware","electronics","display",
        "技術","ソフトウェア","半導体","チップ","人工知能","クラウド","デジタル","電子部品",
    ],
    "Communication Services": [
        "telecom","communication","broadband","ntt","kddi","softbank",
        "media","broadcasting","newspaper","streaming","social media",
        "通信","ブロードバンド","メディア","放送","新聞",
    ],
    "Utilities": [
        "utility","electric power","water supply","tepco","kansai electric",
        "tohoku electric","gas utility","sewage",
        "電気","水道","東電","関西電力","中部電力",
    ],
    "Real Estate": [
        "real estate","property","housing","reit","apartment","office space",
        "land","mortgage","rent","lease property",
        "不動産","住宅","マンション","オフィス","土地","家賃","分譲",
    ],
}


# ── Translation ───────────────────────────────────────────────────────────────

def batch_translate_deepl(texts: list, api_key: str) -> list:
    """Translate a list of texts via DeepL free API in one request."""
    try:
        resp = requests.post(
            "https://api-free.deepl.com/v2/translate",
            data={
                "auth_key": api_key,
                "text": texts,
                "source_lang": "JA",
                "target_lang": "EN-US",
            },
            timeout=20,
        )
        if resp.status_code == 200:
            return [t["text"] for t in resp.json().get("translations", [])]
    except Exception as e:
        print(f"DeepL batch error: {e}")
    return texts


def translate_single_google(text: str) -> str:
    """
    Translate using Google Translate free unofficial endpoint.
    No API key, no daily cap. Falls back to original on error.
    """
    if not text:
        return text
    try:
        params = {
            "client": "gtx",
            "sl": "ja",
            "tl": "en",
            "dt": "t",
            "q": text[:500],
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params=params,
            headers=headers,
            timeout=8,
        )
        if resp.status_code == 200:
            data = resp.json()
            translated = "".join(seg[0] for seg in data[0] if seg[0])
            if translated and translated != text:
                return translated.strip()
    except Exception as e:
        print(f"Google Translate error: {e}")
    return text


def translate_articles(articles: list) -> list:
    """
    Translate all Japanese headlines.
    - DEEPL_API_KEY set: DeepL batch (fastest, best quality)
    - Otherwise: Google Translate free endpoint, concurrent (no cap, no key needed)
    """
    ja_articles = [a for a in articles if a.get("language") == "ja" and not a.get("translated_title")]
    if not ja_articles:
        return articles

    deepl_key = os.environ.get("DEEPL_API_KEY", "")

    if deepl_key:
        texts = [a["original_title"] for a in ja_articles]
        translated = batch_translate_deepl(texts, deepl_key)
        for article, trans in zip(ja_articles, translated):
            article["translated_title"] = trans
            article["title"] = trans
        print(f"✓ DeepL batch translated {len(ja_articles)} headlines")
    else:
        def translate_one(article):
            t = translate_single_google(article["original_title"])
            article["translated_title"] = t
            article["title"] = t
            return article

        with ThreadPoolExecutor(max_workers=10) as ex:
            list(as_completed({ex.submit(translate_one, a): a for a in ja_articles}))
        print(f"✓ Google Translate: translated {len(ja_articles)} headlines")

    return articles


# ── Sector classification ─────────────────────────────────────────────────────

def classify_sector(title: str, original: str = "") -> str:
    combined = (title + " " + original).lower()
    scores = {
        sector: sum(1 for kw in kws if kw.lower() in combined)
        for sector, kws in SECTOR_KEYWORDS.items()
    }
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "General / Macro"


# Sources that are inherently company/micro-focused
MICRO_SOURCES = {
    "Kabutan Corporate", "Kabutan Earnings", "Kabutan Market News",
    "Minkabu", "Traders Web", "Fisco",
    "Nikkei IR / Earnings", "Reuters Japan Companies", "Bloomberg Japan Co",
    "Nikkei Xtech IT", "Nikkei Xtech Auto",
    "QUICK Money World",
}

# Keyword signals for micro (company-level) news
MICRO_KEYWORDS = [
    # English
    "earnings", "profit", "revenue", "operating income", "net income", "forecast",
    "guidance", "results", "quarterly", "annual results", "fy20", "q1", "q2", "q3", "q4",
    "dividend", "buyback", "share repurchase", "acquisition", "merger", "takeover",
    "deal", "joint venture", "partnership", "contract", "order", "shipment",
    "analyst", "upgrade", "downgrade", "target price", "rating", "coverage",
    "ipo", "listing", "secondary offering", "rights issue",
    "restructuring", "job cuts", "layoffs", "plant closure", "spin-off",
    "ceo", "president", "management", "appointment", "resignation",
    # Japanese
    "決算", "業績", "純利益", "営業利益", "売上", "増益", "減益", "予想", "見通し",
    "配当", "自社株買い", "買収", "合併", "提携", "受注", "出荷",
    "アナリスト", "目標株価", "格上げ", "格下げ",
    "上場", "増資", "公募",
    "リストラ", "希望退職", "工場閉鎖", "分社",
    "社長", "代表取締役", "就任", "退任",
]

# Macro/policy keyword signals
MACRO_KEYWORDS = [
    "boj", "bank of japan", "fed", "federal reserve", "ecb", "interest rate",
    "inflation", "gdp", "trade balance", "current account", "fiscal",
    "budget", "tax", "policy", "regulation", "ministry", "government",
    "sanction", "tariff", "trade war", "geopolit",
    "日銀", "金融政策", "金利", "インフレ", "財政", "予算", "規制", "政策", "関税",
]


def classify_news_type(title: str, original: str, source: str) -> str:
    """
    Returns 'micro' (company-level) or 'macro' (economy/policy).
    Source membership takes priority; keyword scoring breaks ties.
    """
    if source in MICRO_SOURCES:
        return "micro"
    combined = (title + " " + original).lower()
    micro_score = sum(1 for kw in MICRO_KEYWORDS if kw in combined)
    macro_score = sum(1 for kw in MACRO_KEYWORDS if kw in combined)
    if micro_score > macro_score:
        return "micro"
    return "macro"



def parse_date(entry) -> tuple:
    """Returns (display_string, datetime_object) for sorting and display."""
    try:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            dt = datetime(*entry.published_parsed[:6])
            return dt.strftime("%b %d, %Y · %H:%M"), dt
    except Exception:
        pass
    return "", None


RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9,ja;q=0.8",
    "Cache-Control": "no-cache",
    "Referer": "https://www.google.com/",
}

def resolve_gnews_url(entry) -> str:
    """
    Google News RSS entries wrap the real article URL inside the <description> HTML.
    Extract it from there. Falls back to entry.link (the redirect URL) if not found.

    Google News description looks like:
      <a href="https://real-article.com/path">Title</a>&nbsp;<font>Source</font>
    """
    # Try description / summary first — contains the real <a href>
    for field in ("description", "summary", "content"):
        raw = ""
        if field == "content":
            content_list = entry.get("content", [])
            raw = content_list[0].get("value", "") if content_list else ""
        else:
            raw = entry.get(field, "")
        if raw:
            m = re.search(r'href=["\']?(https?://(?!news\.google\.)[^"\'>\s]+)', raw)
            if m:
                return m.group(1)

    # Fallback: return the entry link as-is (may be a Google redirect)
    return entry.get("link", "#")


# ── Corporate Action Classifier ───────────────────────────────────────────────

# Priority action types (shown more prominently in Signal Feed)
PRIORITY_ACTIONS = {
    "guidance_raise", "guidance_cut",
    "m&a_target", "m&a_acquirer", "m&a_rumour",
    "contract_win", "contract_loss",
    "mgmt_change_negative",
}

# Full taxonomy with directional bias and display labels
CORP_ACTION_META = {
    "earnings_beat":        {"dir": "positive", "label": "Earnings Beat",         "emoji": "📈"},
    "earnings_miss":        {"dir": "negative", "label": "Earnings Miss",         "emoji": "📉"},
    "guidance_raise":       {"dir": "positive", "label": "Guidance Raised",       "emoji": "⬆️"},
    "guidance_cut":         {"dir": "negative", "label": "Guidance Cut",          "emoji": "⬇️"},
    "dividend_increase":    {"dir": "positive", "label": "Dividend Increase",     "emoji": "💰"},
    "dividend_cut":         {"dir": "negative", "label": "Dividend Cut",          "emoji": "✂️"},
    "buyback":              {"dir": "positive", "label": "Buyback",               "emoji": "🔁"},
    "m&a_acquirer":         {"dir": "mixed",    "label": "M&A: Acquirer",         "emoji": "🤝"},
    "m&a_target":           {"dir": "positive", "label": "M&A: Target",           "emoji": "🎯"},
    "m&a_rumour":           {"dir": "mixed",    "label": "M&A: Rumour",           "emoji": "💬"},
    "m&a_cancelled":        {"dir": "negative", "label": "M&A: Cancelled",        "emoji": "❌"},
    "contract_win":         {"dir": "positive", "label": "Contract Win",          "emoji": "✅"},
    "contract_loss":        {"dir": "negative", "label": "Contract Lost",         "emoji": "🚫"},
    "regulatory_approval":  {"dir": "positive", "label": "Regulatory Approval",   "emoji": "✅"},
    "regulatory_fine":      {"dir": "negative", "label": "Fine / Penalty",        "emoji": "⚖️"},
    "regulatory_inquiry":   {"dir": "negative", "label": "Investigation",         "emoji": "🔍"},
    "mgmt_change_neutral":  {"dir": "neutral",  "label": "Management Change",     "emoji": "👤"},
    "mgmt_change_negative": {"dir": "negative", "label": "Mgmt Change (Trouble)", "emoji": "🚨"},
    "capital_raise":        {"dir": "negative", "label": "Capital Raise",         "emoji": "📋"},
    "asset_sale":           {"dir": "mixed",    "label": "Asset Sale",            "emoji": "🏷️"},
    "restructuring":        {"dir": "mixed",    "label": "Restructuring",         "emoji": "🔧"},
    "credit_upgrade":       {"dir": "positive", "label": "Credit Upgrade",        "emoji": "⭐"},
    "credit_downgrade":     {"dir": "negative", "label": "Credit Downgrade",      "emoji": "⭐"},
    "other_corporate":      {"dir": "neutral",  "label": "Corporate News",        "emoji": "📰"},
    "none":                 {"dir": "neutral",  "label": "",                      "emoji": ""},
}


def classify_articles_batch(articles: list, api_key: str) -> list:
    """
    Classify up to 50 articles in a single Claude Haiku API call.
    Adds corp_action, action_direction, company_code,
    company_name_clean, signal_confidence to each article in-place.
    Cost: ~$0.001-0.003 per batch of 50.
    """
    if not articles or not api_key:
        return articles

    import json

    lines = []
    for i, a in enumerate(articles, 1):
        title = a.get("translated_title") or a.get("title") or a.get("original_title", "")
        lines.append(f"{i}. {title}")
    article_block = "\n".join(lines)

    system = (
        "You are a Japan equity analyst AI. Classify each news headline.\n\n"
        "For each headline return a JSON object with:\n"
        '- "id": the number (integer)\n'
        '- "company_code": TSE 4-digit code if identifiable, else ""\n'
        '- "company_name": canonical English company name if identifiable, else ""\n'
        '- "action": one of: earnings_beat, earnings_miss, guidance_raise, guidance_cut, '
        "dividend_increase, dividend_cut, buyback, m&a_acquirer, m&a_target, m&a_rumour, "
        "m&a_cancelled, contract_win, contract_loss, regulatory_approval, regulatory_fine, "
        "regulatory_inquiry, mgmt_change_neutral, mgmt_change_negative, capital_raise, "
        "asset_sale, restructuring, credit_upgrade, credit_downgrade, other_corporate, none\n"
        '- "direction": positive, negative, mixed, or neutral\n'
        '- "confidence": high, medium, or low\n\n'
        "Use mgmt_change_negative when a management change follows poor results, a scandal, "
        "activist pressure, or strategic failure. Use mgmt_change_neutral for planned succession.\n\n"
        "Return ONLY a JSON array, no other text."
    )

    prompt = f"Classify these {len(articles)} headlines:\n\n{article_block}"

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type": "application/json"},
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 2000,
                "system": system,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=35,
        )
        raw = resp.json()
        text = raw.get("content", [{}])[0].get("text", "").strip()
        # Strip markdown fences
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip()

        results = json.loads(text)
        result_map = {int(r["id"]): r for r in results}

        for i, a in enumerate(articles, 1):
            r = result_map.get(i, {})
            action = r.get("action", "none")
            if action not in CORP_ACTION_META:
                action = "none"
            a["corp_action"]        = action
            a["action_direction"]   = r.get("direction", "neutral")
            a["company_code"]       = r.get("company_code", "")
            a["company_name_clean"] = r.get("company_name", "")
            a["signal_confidence"]  = r.get("confidence", "low")
            a["is_priority_signal"] = (
                action in PRIORITY_ACTIONS
                and a.get("signal_confidence") in ("high", "medium")
            )

    except Exception as e:
        print(f"Classifier batch error: {e}")
        for a in articles:
            a.setdefault("corp_action", "none")
            a.setdefault("action_direction", "neutral")
            a.setdefault("company_code", "")
            a.setdefault("company_name_clean", "")
            a.setdefault("signal_confidence", "low")
            a.setdefault("is_priority_signal", False)

    return articles



# ── Corporate action taxonomy ─────────────────────────────────────────────────

CORP_ACTION_TYPES = {
    # Earnings / financials
    "earnings_beat":      {"label": "Earnings Beat",      "emoji": "📈", "direction": "positive", "priority": True},
    "earnings_miss":      {"label": "Earnings Miss",      "emoji": "📉", "direction": "negative", "priority": True},
    "guidance_raise":     {"label": "Guidance ▲",         "emoji": "🎯", "direction": "positive", "priority": True},
    "guidance_cut":       {"label": "Guidance ▼",         "emoji": "⚠️", "direction": "negative", "priority": True},
    "guidance_initiate":  {"label": "Guidance Issued",    "emoji": "📋", "direction": "neutral",  "priority": False},
    # Capital / shareholder returns
    "dividend_raise":     {"label": "Dividend ▲",         "emoji": "💰", "direction": "positive", "priority": True},
    "dividend_cut":       {"label": "Dividend ▼",         "emoji": "✂️", "direction": "negative", "priority": True},
    "dividend_special":   {"label": "Special Dividend",   "emoji": "💰", "direction": "positive", "priority": True},
    "buyback":            {"label": "Buyback",            "emoji": "🔄", "direction": "positive", "priority": True},
    "capital_raise":      {"label": "Capital Raise",      "emoji": "🏦", "direction": "negative", "priority": True},
    "rights_issue":       {"label": "Rights Issue",       "emoji": "📄", "direction": "negative", "priority": True},
    # M&A / corporate structure
    "ma_acquirer":        {"label": "M&A: Acquirer",      "emoji": "🤝", "direction": "mixed",    "priority": True},
    "ma_target":          {"label": "M&A: Target",        "emoji": "🎯", "direction": "positive", "priority": True},
    "ma_rumour":          {"label": "M&A Rumour",         "emoji": "👂", "direction": "mixed",    "priority": True},
    "ma_blocked":         {"label": "M&A Blocked",        "emoji": "🚫", "direction": "negative", "priority": True},
    "spinoff":            {"label": "Spinoff/Divestiture","emoji": "✂️", "direction": "mixed",    "priority": False},
    "jv_partnership":     {"label": "JV/Partnership",     "emoji": "🤝", "direction": "positive", "priority": False},
    # Management
    "mgmt_change_ceo":    {"label": "CEO Change",         "emoji": "👔", "direction": "mixed",    "priority": True},
    "mgmt_change_other":  {"label": "Mgmt Change",        "emoji": "👤", "direction": "mixed",    "priority": False},
    # Regulatory / legal
    "regulatory_approval":{"label": "Reg Approval",      "emoji": "✅", "direction": "positive", "priority": False},
    "regulatory_fine":    {"label": "Fine/Penalty",       "emoji": "⚖️", "direction": "negative", "priority": True},
    "investigation":      {"label": "Investigation",      "emoji": "🔍", "direction": "negative", "priority": True},
    "lawsuit":            {"label": "Lawsuit",            "emoji": "⚖️", "direction": "negative", "priority": False},
    # Operations
    "contract_win":       {"label": "Contract Win",       "emoji": "✅", "direction": "positive", "priority": False},
    "contract_loss":      {"label": "Contract Loss",      "emoji": "❌", "direction": "negative", "priority": False},
    "credit_upgrade":     {"label": "Credit Upgrade",     "emoji": "⬆️", "direction": "positive", "priority": False},
    "credit_downgrade":   {"label": "Credit Downgrade",   "emoji": "⬇️", "direction": "negative", "priority": True},
    "recall":             {"label": "Recall",             "emoji": "⚠️", "direction": "negative", "priority": True},
    "bankruptcy":         {"label": "Bankruptcy",         "emoji": "💥", "direction": "negative", "priority": True},
    # Default
    "none":               {"label": "",                   "emoji": "",   "direction": "neutral",  "priority": False},
}

# Priority types (shown prominently in Signal Feed)
PRIORITY_ACTION_TYPES = {k for k, v in CORP_ACTION_TYPES.items() if v["priority"]}


# Aliases for app.py compatibility
CORP_ACTION_META   = CORP_ACTION_TYPES
PRIORITY_ACTIONS   = PRIORITY_ACTION_TYPES
# Direction sort order for Signal Feed (positive first)
DIRECTION_ORDER = {"positive": 0, "mixed": 1, "neutral": 2, "negative": 3}


def classify_articles_batch(articles: list, api_key: str) -> None:
    """
    Classify a list of articles for corporate action type, direction, and company.
    Modifies articles in-place. Single Claude Haiku call for the whole batch.
    Returns nothing — results written directly to article dicts.
    """
    if not articles or not api_key:
        return

    import json as _json
    import requests as _req

    # Build the prompt
    lines = []
    for i, a in enumerate(articles):
        title = a.get("translated_title") or a.get("title", "")
        orig  = a.get("original_title", "")
        src   = a.get("source", "")
        combo = title if not orig or orig == title else f"{title} [{orig}]"
        lines.append(f"{i}: [{src}] {combo}")

    article_block = "\n".join(lines)

    action_types_str = ", ".join(sorted(
        k for k in CORP_ACTION_TYPES if k != "none"
    ))

    prompt = f"""You are a Japan equity analyst. Classify each news headline for corporate action signals.

For EACH headline, return a JSON object with:
- "idx": the index number (integer)
- "corp_action": one of [{action_types_str}, none]
- "direction": one of [positive, negative, neutral, mixed]
- "company_code": TSE 4-digit code if identifiable (e.g. "7203"), else ""
- "company_name": canonical English company name if identifiable, else ""
- "confidence": one of [high, medium, low]

Rules:
- "none" = no specific corporate action, just general news
- Use "mixed" for M&A acquirer (may overpay), restructuring (short pain / long gain)
- Extract TSE code from article content or known companies (Toyota=7203, Sony=6758, SoftBank=9984, Nintendo=7974, Honda=7267, Keyence=6861, Tokyo Electron=8035, NTT=9432, KDDI=9433, Recruit=6098, Hitachi=6501, Fanuc=6954, Daikin=6367, Mitsubishi UFJ=8306, Sumitomo Mitsui=8316, Mizuho=8411, Fast Retailing=9983, etc.)
- high confidence = headline explicitly states the action; medium = implied; low = uncertain

Return ONLY a JSON array, no markdown, no explanation:
[{{"idx":0,"corp_action":"...","direction":"...","company_code":"...","company_name":"...","confidence":"..."}}]

Headlines:
{article_block}"""

    try:
        resp = _req.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 4000,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=45,
        )
        raw = resp.json()["content"][0]["text"].strip()

        # Strip any accidental markdown fences
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        results = _json.loads(raw)
        result_map = {r["idx"]: r for r in results}

        for i, a in enumerate(articles):
            r = result_map.get(i, {})
            ca  = r.get("corp_action", "none") or "none"
            if ca not in CORP_ACTION_TYPES:
                ca = "none"
            a["corp_action"]       = ca
            a["action_direction"]  = r.get("direction", "neutral") or "neutral"
            a["company_code"]      = r.get("company_code", "") or ""
            a["company_name_clean"]= r.get("company_name", "") or ""
            a["signal_confidence"] = r.get("confidence", "low") or "low"
            a["is_priority_signal"]= ca in PRIORITY_ACTION_TYPES and a["signal_confidence"] != "low"

    except Exception as e:
        print(f"Classifier error: {e}")
        # Safe defaults on failure
        for a in articles:
            a.setdefault("corp_action", "none")
            a.setdefault("action_direction", "neutral")
            a.setdefault("company_code", "")
            a.setdefault("company_name_clean", "")
            a.setdefault("signal_confidence", "low")
            a.setdefault("is_priority_signal", False)


def run_classifier_on_fetch(unique: list, api_key: str, max_articles: int = 50) -> list:
    """
    Run classifier on the top 50 micro articles from a fetch.
    Only classifies articles not already classified.
    """
    if not api_key:
        # Set safe defaults so UI doesn't crash
        for a in unique:
            a.setdefault("corp_action", "none")
            a.setdefault("action_direction", "neutral")
            a.setdefault("company_code", "")
            a.setdefault("company_name_clean", "")
            a.setdefault("signal_confidence", "low")
            a.setdefault("is_priority_signal", False)
        return unique

    candidates = [
        a for a in unique
        if (a.get("news_type") == "micro" or a.get("is_wadai_expand"))
        and not a.get("corp_action")
    ]
    candidates.sort(key=lambda a: a.get("pub_dt") or datetime.min, reverse=True)
    to_classify = candidates[:max_articles]

    if to_classify:
        print(f"Classifying {len(to_classify)} micro articles...")
        classify_articles_batch(to_classify, api_key)

    # Defaults for unclassified articles
    for a in unique:
        a.setdefault("corp_action", "none")
        a.setdefault("action_direction", "neutral")
        a.setdefault("company_code", "")
        a.setdefault("company_name_clean", "")
        a.setdefault("signal_confidence", "low")
        a.setdefault("is_priority_signal", False)

    return unique


_WADAI_PREFIX = "話題株先取り"

def _expand_wadai_article(source_name: str, entry, pub_display: str, pub_dt) -> list:
    """
    Expand a 話題株先取り aggregate article (Kabutan pre-open highlights) into
    individual per-company articles, one per TSE-listed company found.
    Returns a list of article dicts, or [] if no companies could be extracted.
    """
    title = re.sub(r"<[^>]+>", "", entry.get("title", "").strip())
    base_url = resolve_gnews_url(entry)

    # Pull summary/description from the entry for richer per-company snippets
    summary_raw = ""
    for attr in ("summary", "description"):
        val = getattr(entry, attr, None) or ""
        if val:
            summary_raw = val
            break
    summary = re.sub(r"<[^>]+>", "", summary_raw).strip()

    combined = title + "\n" + summary

    # Match "CompanyName（CODE）" or "CompanyName(CODE)" — CODE = 4 digits 1000-9999
    code_re = re.compile(
        r'([^\s、。，,！!・\n（(]{2,20})\s*[（(](\d{4})[）)]'
    )
    # Deduplicate by code, preserving first-seen order
    seen_codes: dict[str, str] = {}
    for m in code_re.finditer(combined):
        code, name = m.group(2), m.group(1).strip()
        if 1000 <= int(code) <= 9999 and code not in seen_codes:
            seen_codes[code] = name

    if not seen_codes:
        return []

    articles = []
    for code, company in seen_codes.items():
        # Try to find a per-company snippet line in the summary
        snippet = ""
        for line in summary.splitlines():
            if code in line or company in line:
                snippet = line.strip()
                break

        if snippet and snippet != title:
            orig_title = f"【話題株】{company}（{code}）: {snippet}"
        else:
            orig_title = f"【話題株】{company}（{code}）"

        # Fragment-based URL keeps each article unique for dedup logic
        art_url = f"{base_url}#wadai-{code}" if base_url else f"https://kabutan.jp/stock/?code={code}"

        articles.append({
            "source": source_name,
            "original_title": orig_title,
            "translated_title": "",
            "title": "",
            "url": art_url,
            "pub_date": pub_display,
            "pub_dt": pub_dt,
            "sector": "",
            "language": "ja",
            "is_wadai_expand": True,
        })

    return articles


def fetch_rss(source_name: str, url: str, language: str) -> list:
    articles = []
    try:
        # Use requests with browser headers to avoid blocks, then parse the content
        try:
            resp = requests.get(url, headers=RSS_HEADERS, timeout=15)
            feed = feedparser.parse(resp.content)
        except Exception:
            # Fall back to plain feedparser if requests fails
            feed = feedparser.parse(url)
        for entry in feed.entries[:20]:
            title = re.sub(r"<[^>]+>", "", entry.get("title", "").strip())
            if not title:
                continue
            pub_display, pub_dt = parse_date(entry)

            # Expand 話題株先取り aggregates into per-company articles
            if title.startswith(_WADAI_PREFIX):
                expanded = _expand_wadai_article(source_name, entry, pub_display, pub_dt)
                if expanded:
                    articles.extend(expanded)
                    continue  # skip adding the aggregate article itself

            articles.append({
                "source": source_name,
                "original_title": title,
                "translated_title": title if language == "en" else "",
                "title": title if language == "en" else "",
                "url": resolve_gnews_url(entry),
                "pub_date": pub_display,
                "pub_dt": pub_dt,
                "sector": "",
                "language": language,
            })
    except Exception as e:
        print(f"RSS error [{source_name}]: {e}")
    return articles


# ── HTML scraping ─────────────────────────────────────────────────────────────

def scrape_trade_paper(source_name: str, url: str, selectors: str, language: str) -> list:
    articles = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ja,en;q=0.9",
    }
    try:
        from bs4 import BeautifulSoup
        from urllib.parse import urlparse
        resp = requests.get(url, headers=headers, timeout=12)
        resp.encoding = resp.apparent_encoding
        soup = BeautifulSoup(resp.text, "html.parser")
        seen = set()

        for selector in [s.strip() for s in selectors.split(",")]:
            try:
                for el in soup.select(selector)[:15]:
                    title = el.get_text(strip=True)
                    if not title or len(title) < 8 or title in seen:
                        continue
                    seen.add(title)

                    link = el.get("href", "")
                    if not link:
                        a_tag = el.find_parent("a") or el.find("a")
                        if a_tag:
                            link = a_tag.get("href", "")
                    if link and link.startswith("/"):
                        parsed = urlparse(url)
                        link = f"{parsed.scheme}://{parsed.netloc}{link}"
                    if not link:
                        link = url

                    articles.append({
                        "source": source_name,
                        "original_title": title,
                        "translated_title": "",
                        "title": "",
                        "url": link,
                        "pub_date": datetime.now().strftime("%b %d, %Y"),
                        "sector": "",
                        "language": language,
                    })
                if articles:
                    break
            except Exception:
                continue

    except ImportError:
        print("BeautifulSoup not installed — skipping trade paper scraping")
    except Exception as e:
        print(f"Scrape error [{source_name}]: {e}")

    return articles[:12]


# ── Source directory (for By Source tab) ─────────────────────────────────────
# Maps display name → (url, language) for sources that have working RSS feeds
SOURCE_DIRECTORY = {
    # English sources
    "Japan Times Business": ("https://news.google.com/rss/search?q=site:japantimes.co.jp+business&hl=en&gl=JP&ceid=JP:en", "en"),
    "Nikkei Asia":          ("https://asia.nikkei.com/rss/feed/nar",                                          "en"),
    "Nikkei Shimbun":       ("https://news.google.com/rss/search?q=site:nikkei.com&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    "Nikkei Business":      ("https://business.nikkei.com/rss/sns/nb.rdf",                                  "ja"),
    "Nikkei Xtech":         ("https://xtech.nikkei.com/rss/index.rdf",                                      "ja"),
    "Nikkei Xtech IT":      ("https://xtech.nikkei.com/rss/xtech-it.rdf",                                   "ja"),
    "Nikkei Xtech Auto":    ("https://xtech.nikkei.com/rss/xtech-at.rdf",                                   "ja"),
    "Reuters Japan":        ("https://news.google.com/rss/search?q=reuters+japan+economy+OR+business+OR+markets&hl=en&gl=JP&ceid=JP:en", "en"),
    "NHK World Business":   ("https://www3.nhk.or.jp/nhkworld/en/news/feeds/business.xml", "en"),
    "Japan Industry News":  ("https://japanindustrynews.com/feed/",                  "en"),
    # Japanese sources (will be translated)
    "Asahi Shimbun":        ("https://rss.asahi.com/rss/asahi/newsheadlines.rdf",   "ja"),
    "NHK Economics":        ("https://www3.nhk.or.jp/rss/news/cat4.xml",             "ja"),
    "NHK Business":         ("https://www3.nhk.or.jp/rss/news/cat5.xml",             "ja"),
    "Mainichi Shimbun":     ("https://news.google.com/rss/search?q=site:mainichi.jp+%E7%B5%8C%E6%B8%88+OR+%E3%83%93%E3%82%B8%E3%83%8D%E3%82%B9&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    "Sankei Shimbun":       ("https://news.google.com/rss/search?q=site:sankei.com+%E7%B5%8C%E6%B8%88+OR+%E7%94%A3%E6%A5%AD&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    "Yahoo Japan Business": ("https://news.yahoo.co.jp/rss/topics/business.xml",     "ja"),
    "Yahoo Japan Economy":  ("https://news.yahoo.co.jp/rss/topics/economy.xml",      "ja"),
    "IT Media Business":    ("https://rss.itmedia.co.jp/rss/2.0/business_media.xml", "ja"),
    "Toyo Keizai":          ("https://toyokeizai.net/list/feed/rss",                 "ja"),
    "Diamond Online":       ("https://diamond.jp/list/feed/rss/dol",                 "ja"),
    "Nikkan Kogyo":         ("https://news.google.com/rss/search?q=site:nikkan.co.jp&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    "FACTA":                 ("https://facta.co.jp/",                                "ja"),
    # Company / Micro — earnings, IR, M&A, analyst
    "Nikkei IR / Earnings":  ("https://news.google.com/rss/search?q=site:nikkei.com+(決算OR業績OR増益OR減益OR配当OR自社株買い)&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    "Kabutan Corporate":     ("https://kabutan.jp/rss/news_corporate.xml",              "ja"),
    "Kabutan Earnings":      ("https://kabutan.jp/rss/news_kessan.xml",                 "ja"),
    "Minkabu":               ("https://minkabu.jp/rss/news",                            "ja"),
    "Traders Web":           ("https://www.traders.co.jp/news/rss_all.aspx",            "ja"),
    "Reuters Japan Companies": ("https://news.google.com/rss/search?q=reuters+japan+(earnings+OR+profit+OR+forecast+OR+acquisition+OR+merger+OR+dividend)&hl=en&gl=JP&ceid=JP:en", "en"),
    "Bloomberg Japan":      ("https://news.google.com/rss/search?q=bloomberg+japan+economy+OR+markets+OR+business&hl=en&gl=JP&ceid=JP:en", "en"),
    "Bloomberg Japan Co":    ("https://news.google.com/rss/search?q=bloomberg+japan+(earnings+OR+results+OR+forecast+OR+buyback+OR+dividend+OR+acquisition)&hl=en&gl=JP&ceid=JP:en", "en"),
    # New sources
    "Kabutan Market News":   ("https://kabutan.jp/rss/news_marketnews.xml",            "ja"),
    "Fisco":                 ("https://news.google.com/rss/search?q=site:fisco.jp+OR+site:web.fisco.jp&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    "Jiji Press":            ("https://www.jiji.com/rss/ranking.rdf",                  "ja"),
    "JBpress":               ("https://jbpress.ismedia.jp/rss/latest",                 "ja"),
    "TSE Manebu":            ("https://news.google.com/rss/search?q=site:jpx.co.jp+%E6%9D%B1%E8%A8%BC%E3%83%9E%E3%83%8D%E9%83%A8&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    "President Online":      ("https://president.jp/list/feed/rss",                   "ja"),
    "Rakumachi":             ("https://news.google.com/rss/search?q=site:rakumachi.jp&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    "Zaikai Online":         ("https://news.google.com/rss/search?q=site:zaikai.net&hl=ja&gl=JP&ceid=JP:ja", "ja"),
    "QUICK Money World":     ("https://news.google.com/rss/search?q=site:moneyworld.jp&hl=ja&gl=JP&ceid=JP:ja", "ja"),
}

# Group labels for the UI
SOURCE_GROUPS = {
    "🇬🇧 English — General": [
        "Japan Times", "Japan Times Business",
        "Reuters Japan", "NHK World Business", "Japan Industry News",
    ],
    "🇬🇧 English — Company News": [
        "Bloomberg Japan", "Reuters Japan Companies", "Bloomberg Japan Co",
    ],
    "📊 Nikkei Group": [
        "Nikkei Asia", "Nikkei Shimbun", "Nikkei Business",
        "Nikkei Xtech", "Nikkei Xtech IT", "Nikkei Xtech Auto",
        "Nikkei IR / Earnings",
    ],
    "🏢 Corporate / Earnings / IR": [
        "Kabutan Corporate", "Kabutan Earnings", "Kabutan Market News",
        "Minkabu", "Traders Web", "Fisco", "QUICK Money World",
    ],
    "🇯🇵 Japanese — General": [
        "Asahi Shimbun", "Mainichi Shimbun", "Sankei Shimbun",
        "NHK Economics", "NHK Business",
        "Yahoo Japan Business", "Yahoo Japan Economy",
    ],
    "📰 Japanese — Business / Finance": [
        "Toyo Keizai", "Diamond Online", "IT Media Business",
    ],
    "🏭 Japanese — Trade Papers": [
        "Nikkan Kogyo",
    ],
    "🔍 Japanese — Investigative": [
        "FACTA",
    ],
    "📡 Wire & Institutional": [
        "Jiji Press", "QUICK Money World",
    ],
    "📝 Business Analysis": [
        "JBpress", "President Online", "Zaikai Online", "TSE Manebu",
    ],
    "🏘️ Real Estate & REITs": [
        "Rakumachi",
    ],
}


# Sources that must be scraped rather than RSS-fetched
SCRAPE_ONLY_SOURCES = {"FACTA"}

def fetch_source_headlines(source_name: str, days: int = 14) -> list:
    """
    Fetch all available headlines for a single source.
    Uses RSS for most sources; falls back to HTML scraping for scrape-only sources.
    Translates Japanese headlines automatically.
    """
    if source_name not in SOURCE_DIRECTORY:
        return []

    url, language = SOURCE_DIRECTORY[source_name]

    # Scrape-only sources (no RSS available)
    if source_name in SCRAPE_ONLY_SOURCES:
        try:
            articles = scrape_trade_paper(source_name, url, "h3", language)
            articles = [a for a in articles if a.get("title") or a.get("original_title")]
            if language == "ja":
                articles = translate_articles(articles)
            return articles
        except Exception as e:
            print(f"Scrape error [{source_name}]: {e}")
            return []

    cutoff = datetime.now() - timedelta(days=days)

    try:
        feed = feedparser.parse(url)
        articles = []

        for entry in feed.entries[:50]:  # fetch more than usual for 2-week window
            title = re.sub(r"<[^>]+>", "", entry.get("title", "").strip())
            if not title:
                continue

            link    = resolve_gnews_url(entry)
            summary = re.sub(r"<[^>]+>", "", entry.get("summary", "")[:200])
            pub_str = parse_date(entry)

            # Date filter — include if within window or if date unknown
            include = True
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    pub_dt = datetime(*entry.published_parsed[:6])
                    if pub_dt < cutoff:
                        include = False
                except Exception:
                    pass

            if include:
                articles.append({
                    "source":           source_name,
                    "original_title":   title,
                    "translated_title": title if language == "en" else "",
                    "title":            title if language == "en" else "",
                    "url":              link,
                    "pub_date":         pub_str,
                    "summary":          summary,
                    "language":         language,
                    "sector":           "",
                })

        # Translate Japanese headlines
        if language == "ja" and articles:
            articles = translate_articles(articles)

        return articles

    except Exception as e:
        print(f"Source fetch error [{source_name}]: {e}")
        return []


# ── Main ──────────────────────────────────────────────────────────────────────

def fetch_all_news() -> tuple:
    """
    Fetch all news.
    Returns (sector_map: dict, source_map: dict).
    Always returns valid dicts, never None.
    """
    try:
        result = _fetch_all_news_inner()
        if isinstance(result, tuple) and len(result) == 2:
            return result
        # If somehow only sector_map returned
        if isinstance(result, dict):
            return result, {}
        return {}, {}
    except Exception as e:
        print(f"fetch_all_news failed: {e}")
        return {}, {}


def _fetch_all_news_inner() -> dict:
    all_articles = []

    # Concurrent RSS fetching
    with ThreadPoolExecutor(max_workers=15) as ex:
        futures = {ex.submit(fetch_rss, n, u, l): n for n, u, l in RSS_SOURCES}
        for f in as_completed(futures):
            try:
                all_articles.extend(f.result())
            except Exception as e:
                print(f"RSS future error: {e}")

    # Concurrent trade paper scraping
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(scrape_trade_paper, n, u, s, l): n for n, u, s, l in SCRAPE_SOURCES}
        for f in as_completed(futures):
            try:
                all_articles.extend(f.result())
            except Exception as e:
                print(f"Scrape future error: {e}")

    # Translate
    all_articles = translate_articles(all_articles)

    # Deduplicate
    seen_urls, unique = set(), []
    for a in all_articles:
        url = a.get("url", "")
        if url and url != "#" and url not in seen_urls:
            seen_urls.add(url)
            unique.append(a)

    # Classify sector + news_type
    for a in unique:
        title_en = a.get("translated_title") or a.get("title", "")
        title_orig = a.get("original_title", "")
        source = a.get("source", "")
        a["sector"] = classify_sector(title_en, title_orig)
        a["news_type"] = classify_news_type(title_en, title_orig, source)

    # Corporate action classification (AI, top 50 micro articles)
    try:
        import os as _os
        import streamlit as _st
        _api_key = _st.secrets.get("ANTHROPIC_API_KEY", "") if hasattr(_st, "secrets") else ""
        if not _api_key:
            _api_key = _os.environ.get("ANTHROPIC_API_KEY", "")
    except Exception:
        _api_key = _os.environ.get("ANTHROPIC_API_KEY", "") if "_os" in dir() else ""
    run_classifier_on_fetch(unique, _api_key, max_articles=50)


    # Group by MSCI sector
    order = [
        "Energy","Materials","Industrials","Consumer Discretionary","Consumer Staples",
        "Health Care","Financials","Information Technology","Communication Services",
        "Utilities","Real Estate","General / Macro",
    ]
    sector_map = {s: [] for s in order}
    for a in unique:
        sector_map.get(a.get("sector","General / Macro"), sector_map["General / Macro"]).append(a)

    for s in sector_map:
        sector_map[s].sort(key=lambda x: x.get("pub_dt") or datetime.min, reverse=True)

    # Also build a by-source map, sorted newest-first, up to 2 weeks
    cutoff = datetime.now() - timedelta(days=14)
    source_map = {}
    for a in unique:
        src = a.get("source", "Unknown")
        if src not in source_map:
            source_map[src] = []
        pub_dt = a.get("pub_dt")
        # Include article if date unknown (scraped) or within 2 weeks
        if pub_dt is None or pub_dt >= cutoff:
            source_map[src].append(a)
    for src in source_map:
        source_map[src].sort(key=lambda x: x.get("pub_dt") or datetime.min, reverse=True)
        # Ensure pub_date is always a string
        for a in source_map[src]:
            if not a.get("pub_date"):
                a["pub_date"] = ""

    print(f"✓ Total: {sum(len(v) for v in sector_map.values())} articles across {sum(1 for v in sector_map.values() if v)} sectors, {len(source_map)} sources")
    return sector_map, source_map


if __name__ == "__main__":
    results = fetch_all_news()
    for sector, arts in results.items():
        if arts:
            print(f"\n{sector} ({len(arts)}):")
            for a in arts[:2]:
                print(f"  [{a['source']}] {(a.get('translated_title') or a.get('title',''))[:80]}")
