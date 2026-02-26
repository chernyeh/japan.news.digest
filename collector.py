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
    ("Japan Times",          "https://news.google.com/rss/search?q=site:japantimes.co.jp&hl=en&gl=JP&ceid=JP:en", "en"),
    ("Japan Times Business", "https://news.google.com/rss/search?q=site:japantimes.co.jp+business&hl=en&gl=JP&ceid=JP:en", "en"),
    ("Japan Times Economy",  "https://news.google.com/rss/search?q=site:japantimes.co.jp+economy&hl=en&gl=JP&ceid=JP:en", "en"),
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
    ("NHK World",            "https://www3.nhk.or.jp/nhkworld/en/news/feeds/top.xml",      "en"),
    ("Japan Industry News",  "https://japanindustrynews.com/feed/",                         "en"),
    # Japanese (translated)
    ("Asahi Shimbun",        "https://rss.asahi.com/rss/asahi/newsheadlines.rdf",           "ja"),
    ("NHK",                  "https://www3.nhk.or.jp/rss/news/cat0.xml",                    "ja"),
    ("NHK Economics",        "https://www3.nhk.or.jp/rss/news/cat4.xml",                    "ja"),
    ("NHK Business",         "https://www3.nhk.or.jp/rss/news/cat5.xml",                    "ja"),
    ("Mainichi Shimbun",     "https://rss.mainichi.jp/rss/etc/mainichi-flash.xml",          "ja"),
    ("Sankei Shimbun",       "https://www.sankei.com/rss/news/flash/flash.xml",             "ja"),
    ("Yahoo Japan Business", "https://news.yahoo.co.jp/rss/topics/business.xml",            "ja"),
    ("Yahoo Japan Economy",  "https://news.yahoo.co.jp/rss/topics/economy.xml",             "ja"),
    ("IT Media Business",    "https://rss.itmedia.co.jp/rss/2.0/business_media.xml",        "ja"),
    ("Toyo Keizai",          "https://toyokeizai.net/list/feed/rss",                        "ja"),
    ("Diamond Online",       "https://diamond.jp/list/feed/rss/dol",                        "ja"),
    ("Nikkan Kogyo",         "https://www.nikkan.co.jp/rss/nksrdf.rdf",                      "ja"),
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


# ── RSS fetch ─────────────────────────────────────────────────────────────────

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
            articles.append({
                "source": source_name,
                "original_title": title,
                "translated_title": title if language == "en" else "",
                "title": title if language == "en" else "",
                "url": entry.get("link", "#"),
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
    "Japan Times":          ("https://news.google.com/rss/search?q=site:japantimes.co.jp&hl=en&gl=JP&ceid=JP:en", "en"),
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
    "Mainichi Shimbun":     ("https://rss.mainichi.jp/rss/etc/mainichi-flash.xml",   "ja"),
    "Sankei Shimbun":       ("https://www.sankei.com/rss/news/flash/flash.xml",      "ja"),
    "Yahoo Japan Business": ("https://news.yahoo.co.jp/rss/topics/business.xml",     "ja"),
    "Yahoo Japan Economy":  ("https://news.yahoo.co.jp/rss/topics/economy.xml",      "ja"),
    "IT Media Business":    ("https://rss.itmedia.co.jp/rss/2.0/business_media.xml", "ja"),
    "Toyo Keizai":          ("https://toyokeizai.net/list/feed/rss",                 "ja"),
    "Diamond Online":       ("https://diamond.jp/list/feed/rss/dol",                 "ja"),
    "Nikkan Kogyo":         ("https://www.nikkan.co.jp/rss/nksrdf.rdf",              "ja"),
    "FACTA":                 ("https://facta.co.jp/",                                "ja"),
}

# Group labels for the UI
SOURCE_GROUPS = {
    "🇬🇧 English — General": [
        "Japan Times", "Japan Times Business",
        "Reuters Japan", "NHK World Business", "Japan Industry News",
    ],
    "📊 Nikkei Group": [
        "Nikkei Asia", "Nikkei Shimbun", "Nikkei Business",
        "Nikkei Xtech", "Nikkei Xtech IT", "Nikkei Xtech Auto",
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

            link    = entry.get("link", "#")
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

    # Classify
    for a in unique:
        a["sector"] = classify_sector(
            a.get("translated_title") or a.get("title", ""),
            a.get("original_title", "")
        )

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
