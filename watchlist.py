"""
watchlist.py
Manages a user watchlist of TSE-listed companies.
Scans all fetched articles for mentions of watched companies.
Stores watchlist in a local JSON file.
"""

import json
import os
import re

WATCHLIST_FILE = "watchlist.json"

# Common TSE companies with their aliases for matching
KNOWN_COMPANIES = {
    "Toyota":             ["toyota", "トヨタ", "7203"],
    "Sony":               ["sony", "ソニー", "6758"],
    "SoftBank":           ["softbank", "ソフトバンク", "9984"],
    "Nintendo":           ["nintendo", "任天堂", "7974"],
    "Honda":              ["honda", "本田", "7267"],
    "Keyence":            ["keyence", "キーエンス", "6861"],
    "Tokyo Electron":     ["tokyo electron", "東京エレクトロン", "8035"],
    "Fanuc":              ["fanuc", "ファナック", "6954"],
    "NTT":                ["ntt", "日本電信電話", "9432"],
    "KDDI":               ["kddi", "9433"],
    "Recruit":            ["recruit", "リクルート", "6098"],
    "Mitsubishi UFJ":     ["mitsubishi ufj", "三菱UFJ", "8306"],
    "Sumitomo Mitsui":    ["sumitomo mitsui", "住友三井", "smbc", "8316"],
    "Mizuho":             ["mizuho", "みずほ", "8411"],
    "Daikin":             ["daikin", "ダイキン", "6367"],
    "Shin-Etsu Chemical": ["shin-etsu", "shinetsu", "信越化学", "4063"],
    "Chugai Pharma":      ["chugai", "中外製薬", "4519"],
    "Hitachi":            ["hitachi", "日立", "6501"],
    "Panasonic":          ["panasonic", "パナソニック", "6752"],
    "Denso":              ["denso", "デンソー", "6902"],
    "Murata":             ["murata", "村田製作所", "6981"],
    "Olympus":            ["olympus", "オリンパス", "7733"],
    "Fast Retailing":     ["fast retailing", "uniqlo", "ユニクロ", "ファーストリテイリング", "9983"],
    "Oriental Land":      ["oriental land", "disney japan", "オリエンタルランド", "4661"],
    "Hoya":               ["hoya", "ホヤ", "7741"],
    "Advantest":          ["advantest", "アドバンテスト", "6857"],
    "Lasertec":           ["lasertec", "レーザーテック", "6920"],
    "Disco":              ["disco corporation", "ディスコ", "6146"],
    "Rohm":               ["rohm", "ローム", "6963"],
    "Renesas":            ["renesas", "ルネサス", "6723"],
}


def load_watchlist() -> list:
    """Load watchlist from file."""
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def save_watchlist(watchlist: list):
    """Save watchlist to file."""
    with open(WATCHLIST_FILE, "w") as f:
        json.dump(watchlist, f, indent=2)


def add_to_watchlist(company: str) -> bool:
    """Add a company to the watchlist."""
    watchlist = load_watchlist()
    if company not in watchlist:
        watchlist.append(company)
        save_watchlist(watchlist)
    return True


def remove_from_watchlist(company: str) -> bool:
    """Remove a company from the watchlist."""
    watchlist = load_watchlist()
    if company in watchlist:
        watchlist.remove(company)
        save_watchlist(watchlist)
    return True


def get_company_aliases(company_name: str) -> list:
    """Get all search terms for a company."""
    # Check known companies first
    for name, aliases in KNOWN_COMPANIES.items():
        if company_name.lower() == name.lower():
            return [name.lower()] + [a.lower() for a in aliases]

    # For custom entries, just use the name itself
    return [company_name.lower()]


def scan_articles_for_company(company_name: str, all_articles: list) -> list:
    """Find all articles mentioning a specific company."""
    aliases = get_company_aliases(company_name)
    matches = []

    for article in all_articles:
        text = (
            (article.get("translated_title") or article.get("title", "")) + " " +
            article.get("original_title", "")
        ).lower()

        for alias in aliases:
            if alias and alias in text:
                matches.append(article)
                break

    return matches


def scan_all_watchlist(watchlist: list, articles_by_sector: dict) -> dict:
    """
    Scan all articles for all watchlist companies.
    Returns dict of {company_name: [matching_articles]}
    """
    if not articles_by_sector or not isinstance(articles_by_sector, dict):
        return {}
    if not watchlist:
        return {}
    # Flatten all articles
    all_articles = [
        article
        for sector_articles in articles_by_sector.values()
        for article in (sector_articles or [])
    ]

    results = {}
    for company in watchlist:
        matches = scan_articles_for_company(company, all_articles)
        if matches:
            results[company] = matches

    return results
