"""
sentiment.py
Lightweight keyword-based sentiment scoring for news headlines.
Scores each sector as Positive / Neutral / Negative based on
headline language. No external API needed.
"""

# Positive financial/business signals
POSITIVE_WORDS = [
    # English
    "rise", "rises", "rose", "gain", "gains", "gained", "surge", "surges",
    "surged", "jump", "jumps", "jumped", "rally", "rallies", "rallied",
    "record", "high", "growth", "grow", "grew", "profit", "profits",
    "beat", "beats", "exceeded", "strong", "stronger", "upgrade",
    "upgraded", "buy", "outperform", "positive", "recover", "recovery",
    "improve", "improvement", "improved", "expand", "expansion",
    "increase", "increased", "boost", "boosted", "win", "won",
    "success", "successful", "breakthrough", "invest", "investment",
    "deal", "merger", "acquisition", "dividend", "buyback", "upside",
    "bullish", "optimistic", "opportunity", "demand", "order",
    # Japanese
    "上昇", "増加", "増益", "黒字", "最高", "好調", "回復", "改善",
    "成長", "拡大", "増収", "上方修正", "買収", "配当", "増配",
]

# Negative financial/business signals
NEGATIVE_WORDS = [
    # English
    "fall", "falls", "fell", "drop", "drops", "dropped", "decline",
    "declines", "declined", "slump", "slumps", "slumped", "plunge",
    "plunges", "plunged", "crash", "crashes", "crashed", "loss",
    "losses", "losing", "weak", "weaker", "downgrade", "downgraded",
    "sell", "underperform", "negative", "risk", "risks", "concern",
    "concerns", "warning", "warnings", "miss", "missed", "disappoint",
    "disappointing", "cut", "cuts", "reduce", "reduced", "shrink",
    "shrinks", "shrunk", "layoff", "layoffs", "restructure",
    "restructuring", "deficit", "debt", "crisis", "recession",
    "slowdown", "inflation", "tariff", "tariffs", "sanction",
    "sanctions", "fine", "fined", "penalty", "lawsuit", "recall",
    "bearish", "pessimistic", "shortage", "supply chain",
    # Japanese
    "下落", "減少", "減益", "赤字", "低迷", "悪化", "縮小",
    "減収", "下方修正", "リストラ", "不況", "懸念", "リスク",
]

# Strong signal multipliers
STRONG_POSITIVE = ["record high", "all-time", "beat expectations", "上場来高値"]
STRONG_NEGATIVE = ["record low", "bankruptcy", "scandal", "fraud", "倒産", "不正", "損失拡大"]


def score_headline(title: str) -> int:
    """
    Score a single headline.
    Returns: +2 strong positive, +1 positive, 0 neutral, -1 negative, -2 strong negative
    """
    text = title.lower()
    score = 0

    for word in POSITIVE_WORDS:
        if word.lower() in text:
            score += 1

    for word in NEGATIVE_WORDS:
        if word.lower() in text:
            score -= 1

    for phrase in STRONG_POSITIVE:
        if phrase.lower() in text:
            score += 1  # additional boost

    for phrase in STRONG_NEGATIVE:
        if phrase.lower() in text:
            score -= 1  # additional penalty

    return max(-3, min(3, score))  # clamp to -3..+3


def score_sector(articles: list) -> dict:
    """
    Score an entire sector's articles.
    Returns sentiment summary dict.
    """
    if not articles:
        return {"label": "No Data", "score": 0, "color": "#9B8B7A", "icon": "—"}

    scores = [score_headline(
        a.get("translated_title") or a.get("title", "")
    ) for a in articles]

    total = sum(scores)
    avg = total / len(scores) if scores else 0

    positive_count = sum(1 for s in scores if s > 0)
    negative_count = sum(1 for s in scores if s < 0)
    neutral_count  = sum(1 for s in scores if s == 0)

    # Determine overall label
    if avg >= 0.4:
        label, color, icon = "Positive", "#2E7D32", "▲"
    elif avg <= -0.4:
        label, color, icon = "Negative", "#C62828", "▼"
    else:
        label, color, icon = "Neutral", "#6B6B6B", "●"

    return {
        "label":          label,
        "score":          round(avg, 2),
        "color":          color,
        "icon":           icon,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "neutral_count":  neutral_count,
        "total_articles": len(articles),
    }


def score_all_sectors(articles_by_sector: dict) -> dict:
    """Score all sectors at once. Handles None or non-dict input gracefully."""
    if not articles_by_sector or not isinstance(articles_by_sector, dict):
        return {}
    return {
        sector: score_sector(articles)
        for sector, articles in articles_by_sector.items()
        if articles
    }


def flag_high_value_articles(articles: list) -> list:
    """
    Flag articles likely to be high investment relevance:
    earnings, guidance, dividends, M&A, major orders.
    Returns list with 'flag' field added.
    """
    HIGH_VALUE_TERMS = [
        "earnings", "profit", "revenue", "guidance", "forecast",
        "dividend", "buyback", "acquisition", "merger", "takeover",
        "ipo", "listing", "restructur", "layoff", "recall",
        "quarterly", "annual result", "full year", "guidance",
        "upgrade", "downgrade", "target price",
        "決算", "業績", "配当", "買収", "合併", "上場", "増配",
        "減配", "自社株買い", "リストラ", "通期", "上方修正", "下方修正",
    ]

    flagged = []
    for article in articles:
        text = (
            (article.get("translated_title") or article.get("title", "")) + " " +
            article.get("original_title", "")
        ).lower()

        is_high_value = any(term in text for term in HIGH_VALUE_TERMS)
        article_copy = dict(article)
        article_copy["high_value"] = is_high_value
        flagged.append(article_copy)

    return flagged
