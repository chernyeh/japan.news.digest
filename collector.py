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
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── RSS Sources ───────────────────────────────────────────────────────────────
RSS_SOURCES = [
    # English (no translation needed)
    ("Japan Times",          "https://www.japantimes.co.jp/feed/",                          "en"),
    ("Japan Times Business", "https://www.japantimes.co.jp/feed/category/business/",        "en"),
    ("Nikkei Asia",          "https://asia.nikkei.com/rss/feed/nar",                        "en"),
    ("Reuters Japan",        "https://feeds.reuters.com/reuters/JPbusinessNews",            "en"),
    ("Reuters Japan (all)",  "https://feeds.reuters.com/reuters/JPNews",                    "en"),
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
    ("Nikkei JP",            "https://www.nikkei.com/rss/index.rdf",                        "ja"),
    ("Diamond Online",       "https://diamond.jp/feed/newest",                              "ja"),
]

# ── Trade paper scrape targets ────────────────────────────────────────────────
SCRAPE_SOURCES = [
    ("Nikkan Kogyo Shimbun", "https://www.nikkan.co.jp/",          "h2, h3, .news-title, article h2, .headline", "ja"),
    ("Nikkan Jidosha",       "https://www.njd.jp/",                "h2, h3, .article-title, .news-list li a",    "ja"),
    ("Denki Shimbun",        "https://www.denkishimbun.com/",      "h2, h3, .article-title, .news-item a",       "ja"),
    ("Dempa Shimbun",        "https://www.dempa.com/",             "h2, h3, .article-title, .headline",          "ja"),
    ("Kagaku Kogyo Nippo",   "https://www.kagakukogyonippo.com/",  "h2, h3, .article-title, .headline-list a",   "ja"),
    ("Japan Marine Daily",   "https://www.jmd.co.jp/",             "h2, h3, .article-title, .news-title",        "ja"),
    ("Nikkan Kensetsu",      "https://www.constnews.com/",         "h2, h3, .article-title, .news-list a",       "ja"),
    ("Nihon Nogyo Shimbun",  "https://www.agrinews.co.jp/",        "h2, h3, .article-title, .headline",          "ja"),
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


def translate_single_mymemory(text: str) -> str:
    """Translate single text via MyMemory (no key required, slower)."""
    if not text:
        return text
    try:
        resp = requests.get(
            "https://api.mymemory.translated.net/get",
            params={"q": text[:400], "langpair": "ja|en"},
            timeout=8,
        )
        result = resp.json().get("responseData", {}).get("translatedText", "")
        if result and result.upper() != text.upper():
            return result
    except Exception:
        pass
    return text


def translate_articles(articles: list) -> list:
    """
    Translate all Japanese headlines.
    DeepL batch (fast, high quality) if DEEPL_API_KEY env var is set,
    otherwise concurrent MyMemory calls (slower, no key needed).
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
            t = translate_single_mymemory(article["original_title"])
            article["translated_title"] = t
            article["title"] = t
            return article

        with ThreadPoolExecutor(max_workers=10) as ex:
            list(as_completed({ex.submit(translate_one, a): a for a in ja_articles}))
        print(f"✓ MyMemory translated {len(ja_articles)} headlines (add DEEPL_API_KEY for faster translation)")

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

def parse_date(entry) -> str:
    try:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            return datetime(*entry.published_parsed[:6]).strftime("%b %d, %Y · %H:%M")
    except Exception:
        pass
    return ""


def fetch_rss(source_name: str, url: str, language: str) -> list:
    articles = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries[:20]:
            title = re.sub(r"<[^>]+>", "", entry.get("title", "").strip())
            if not title:
                continue
            articles.append({
                "source": source_name,
                "original_title": title,
                "translated_title": title if language == "en" else "",
                "title": title if language == "en" else "",
                "url": entry.get("link", "#"),
                "pub_date": parse_date(entry),
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


# ── Main ──────────────────────────────────────────────────────────────────────

def fetch_all_news() -> dict:
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
        sector_map[s].sort(key=lambda x: x.get("pub_date",""), reverse=True)

    print(f"✓ Total: {sum(len(v) for v in sector_map.values())} articles across {sum(1 for v in sector_map.values() if v)} sectors")
    return sector_map


if __name__ == "__main__":
    results = fetch_all_news()
    for sector, arts in results.items():
        if arts:
            print(f"\n{sector} ({len(arts)}):")
            for a in arts[:2]:
                print(f"  [{a['source']}] {(a.get('translated_title') or a.get('title',''))[:80]}")
