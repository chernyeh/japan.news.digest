"""
collector.py
Fetches RSS feeds from Japanese business/news outlets,
translates titles to English, and classifies into MSCI sectors.
"""

import feedparser
import requests
import re
from datetime import datetime
from urllib.parse import urlparse

# ── RSS Sources ───────────────────────────────────────────────────────────────
# Each entry: (display_name, rss_url, language)
RSS_SOURCES = [
    # English-language outlets (no translation needed)
    ("Japan Times",         "https://www.japantimes.co.jp/feed/",                             "en"),
    ("Japan Times Business","https://www.japantimes.co.jp/feed/category/business/",            "en"),
    ("Nikkei Asia",         "https://asia.nikkei.com/rss/feed/nar",                            "en"),
    ("Reuters Japan",       "https://feeds.reuters.com/reuters/JPbusinessNews",                "en"),
    ("Reuters Japan (all)", "https://feeds.reuters.com/reuters/JPNews",                        "en"),
    ("NHK World Business",  "https://www3.nhk.or.jp/nhkworld/en/news/feeds/business.xml",     "en"),
    ("NHK World",           "https://www3.nhk.or.jp/nhkworld/en/news/feeds/top.xml",          "en"),

    # Japanese-language outlets (will be translated)
    ("Asahi Shimbun",       "https://rss.asahi.com/rss/asahi/newsheadlines.rdf",              "ja"),
    ("NHK",                 "https://www3.nhk.or.jp/rss/news/cat0.xml",                       "ja"),
    ("NHK Economics",       "https://www3.nhk.or.jp/rss/news/cat4.xml",                       "ja"),
    ("NHK Business",        "https://www3.nhk.or.jp/rss/news/cat5.xml",                       "ja"),
    ("Mainichi Shimbun",    "https://rss.mainichi.jp/rss/etc/mainichi-flash.xml",              "ja"),
    ("Sankei Shimbun",      "https://www.sankei.com/rss/news/flash/flash.xml",                 "ja"),
    ("Yahoo Japan News",    "https://news.yahoo.co.jp/rss/topics/business.xml",               "ja"),
    ("Yahoo Japan Finance", "https://news.yahoo.co.jp/rss/topics/economy.xml",               "ja"),
    ("IT Media Business",   "https://rss.itmedia.co.jp/rss/2.0/business_media.xml",           "ja"),
    ("Japan Industry News", "https://japanindustrynews.com/feed/",                             "en"),
]

# ── MSCI Sector Keywords ──────────────────────────────────────────────────────
# Maps sector name → list of keywords to match in translated title
SECTOR_KEYWORDS = {
    "Energy": [
        "oil", "gas", "petroleum", "energy", "fuel", "refinery", "crude", "LNG",
        "solar", "wind power", "nuclear", "power plant", "electricity", "renewable",
        "石油", "ガス", "エネルギー", "燃料", "原油", "発電", "電力", "再生可能",
    ],
    "Materials": [
        "steel", "iron", "aluminum", "copper", "chemical", "plastic", "rubber",
        "mining", "metal", "material", "cement", "paper", "pulp", "fiber",
        "鉄鋼", "アルミ", "銅", "化学", "素材", "金属", "セメント", "紙", "繊維",
    ],
    "Industrials": [
        "manufacturing", "factory", "industrial", "machinery", "equipment",
        "aerospace", "defense", "logistics", "transport", "shipping", "railroad",
        "construction", "infrastructure", "engineering", "robot", "automation",
        "製造", "工場", "機械", "設備", "物流", "輸送", "建設", "インフラ", "ロボット",
    ],
    "Consumer Discretionary": [
        "retail", "automobile", "car", "vehicle", "toyota", "honda", "nissan",
        "fashion", "apparel", "luxury", "restaurant", "hotel", "travel", "tourism",
        "gaming", "entertainment", "media", "advertising", "e-commerce",
        "小売", "自動車", "ファッション", "ホテル", "旅行", "観光", "ゲーム", "広告",
    ],
    "Consumer Staples": [
        "food", "beverage", "drink", "grocery", "supermarket", "tobacco",
        "household", "cosmetic", "beauty", "personal care", "drug store",
        "食品", "飲料", "食料品", "スーパー", "タバコ", "化粧品", "ドラッグ",
    ],
    "Health Care": [
        "health", "medical", "hospital", "pharma", "drug", "vaccine", "biotech",
        "clinical", "patient", "doctor", "treatment", "cancer", "insurance health",
        "医療", "病院", "製薬", "薬", "ワクチン", "バイオ", "臨床", "患者", "治療",
    ],
    "Financials": [
        "bank", "finance", "investment", "insurance", "asset management",
        "stock market", "bond", "yen", "currency", "credit", "loan", "interest rate",
        "boj", "bank of japan", "nippon life", "securities", "fintech",
        "銀行", "金融", "投資", "保険", "株式", "債券", "円", "為替", "融資", "金利",
    ],
    "Information Technology": [
        "technology", "software", "semiconductor", "chip", "AI", "artificial intelligence",
        "cloud", "cyber", "digital", "data", "internet", "startup", "tech",
        "5G", "quantum", "IT", "SaaS", "hardware", "apple", "google",
        "技術", "ソフトウェア", "半導体", "チップ", "人工知能", "クラウド", "デジタル",
    ],
    "Communication Services": [
        "telecom", "communication", "broadband", "NTT", "KDDI", "softbank",
        "media", "broadcasting", "newspaper", "streaming", "social media",
        "通信", "ブロードバンド", "メディア", "放送", "新聞",
    ],
    "Utilities": [
        "utility", "electric power", "water", "gas utility", "sewage",
        "tepco", "kansai electric", "tohoku electric",
        "電気", "水道", "ガス公益", "東電", "関西電力",
    ],
    "Real Estate": [
        "real estate", "property", "housing", "reit", "apartment", "office",
        "construction real", "land", "mortgage", "rent", "leasing property",
        "不動産", "住宅", "マンション", "オフィス", "土地", "家賃",
    ],
}


def translate_to_english(text: str, source_lang: str = "ja") -> str:
    """
    Translate text using MyMemory free API (no key required).
    Falls back to original text on error.
    """
    if source_lang == "en" or not text:
        return text

    try:
        url = "https://api.mymemory.translated.net/get"
        params = {
            "q": text[:500],  # API limit
            "langpair": f"{source_lang}|en",
        }
        resp = requests.get(url, params=params, timeout=8)
        data = resp.json()
        translated = data.get("responseData", {}).get("translatedText", "")
        if translated and translated.upper() != text.upper():
            return translated
    except Exception:
        pass

    return text  # fallback to original


def classify_sector(title: str, original_title: str = "") -> str:
    """
    Classify a news headline into an MSCI sector based on keyword matching.
    Returns sector name or 'General / Macro'.
    """
    combined = (title + " " + original_title).lower()

    scores = {}
    for sector, keywords in SECTOR_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw.lower() in combined)
        if score > 0:
            scores[sector] = score

    if scores:
        return max(scores, key=scores.get)

    return "General / Macro"


def parse_date(entry) -> str:
    """Extract a human-readable date from a feed entry."""
    try:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            dt = datetime(*entry.published_parsed[:6])
            return dt.strftime("%b %d, %Y · %H:%M")
    except Exception:
        pass
    return ""


def fetch_feed(source_name: str, url: str, language: str) -> list:
    """Fetch and parse a single RSS feed. Returns list of article dicts."""
    articles = []
    try:
        feed = feedparser.parse(url)
        entries = feed.entries[:20]  # max 20 per source

        for entry in entries:
            original_title = entry.get("title", "").strip()
            if not original_title:
                continue

            # Clean up HTML entities
            original_title = re.sub(r"<[^>]+>", "", original_title)

            # Translate if needed
            translated_title = translate_to_english(original_title, language)

            # Get URL
            link = entry.get("link", "#")

            # Classify sector
            sector = classify_sector(translated_title, original_title)

            articles.append({
                "source": source_name,
                "original_title": original_title if language == "ja" else "",
                "translated_title": translated_title,
                "title": translated_title,
                "url": link,
                "pub_date": parse_date(entry),
                "sector": sector,
                "language": language,
            })

    except Exception as e:
        print(f"Error fetching {source_name}: {e}")

    return articles


def fetch_all_news() -> dict:
    """
    Fetch all RSS sources, translate, classify, and return
    a dict of {sector_name: [articles]} sorted by sector.
    """
    all_articles = []

    for source_name, url, language in RSS_SOURCES:
        print(f"Fetching: {source_name}...")
        articles = fetch_feed(source_name, url, language)
        all_articles.append((source_name, articles))

    # Flatten and deduplicate by URL
    seen_urls = set()
    flat_articles = []
    for source_name, articles in all_articles:
        for article in articles:
            url = article["url"]
            if url and url != "#" and url not in seen_urls:
                seen_urls.add(url)
                flat_articles.append(article)

    # Group by sector
    sector_map = {sector: [] for sector, _ in [
        ("Energy", ""), ("Materials", ""), ("Industrials", ""),
        ("Consumer Discretionary", ""), ("Consumer Staples", ""),
        ("Health Care", ""), ("Financials", ""),
        ("Information Technology", ""), ("Communication Services", ""),
        ("Utilities", ""), ("Real Estate", ""), ("General / Macro", ""),
    ]}

    for article in flat_articles:
        sector = article.get("sector", "General / Macro")
        if sector in sector_map:
            sector_map[sector].append(article)
        else:
            sector_map["General / Macro"].append(article)

    # Sort articles within each sector by pub_date (most recent first)
    for sector in sector_map:
        sector_map[sector].sort(key=lambda x: x.get("pub_date", ""), reverse=True)

    return sector_map


if __name__ == "__main__":
    # Test run
    results = fetch_all_news()
    total = sum(len(v) for v in results.values())
    print(f"\n=== Fetched {total} articles ===")
    for sector, articles in results.items():
        if articles:
            print(f"\n{sector} ({len(articles)} articles):")
            for a in articles[:2]:
                print(f"  [{a['source']}] {a['translated_title'][:80]}")
