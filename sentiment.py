"""
sentiment.py
Lightweight keyword-based sentiment scoring for news headlines.
Scores each sector as Positive / Neutral / Negative based on
headline language. No external API needed.
"""

# ── Positive signals ──────────────────────────────────────────────────────────
POSITIVE_WORDS = [
    # Price / earnings momentum
    "rise", "rises", "rose", "gain", "gains", "gained",
    "surge", "surges", "surged", "jump", "jumps", "jumped",
    "rally", "rallies", "rallied", "rebound", "rebounds", "rebounded",
    "soar", "soars", "soared", "climb", "climbs", "climbed",
    "advance", "advances", "advanced",
    # Earnings / financial results
    "profit", "profits", "profitable", "profitability",
    "beat", "beats", "beat expectations", "exceeded", "exceeds",
    "outperform", "outperformed", "topped", "topped estimates",
    "record", "record high", "record profit", "record sales",
    "growth", "grow", "grew", "growing",
    "revenue growth", "earnings growth", "sales growth",
    "upgrade", "upgraded", "raised guidance", "raised forecast",
    "raised target", "strong results", "solid results",
    # Corporate actions — positive
    "dividend", "dividends", "special dividend", "dividend hike",
    "buyback", "share buyback", "share repurchase", "repurchase",
    "acquisition", "merger", "strategic deal", "partnership",
    "joint venture", "tie-up", "alliance",
    "ipo", "listing", "debut",
    # Business / operational
    "expansion", "expand", "expands", "expanded",
    "investment", "invests", "investing",
    "order", "orders", "large order", "major order",
    "contract", "awarded", "win", "wins", "won",
    "demand", "strong demand", "robust demand",
    "launch", "launches", "launched",
    "recovery", "recover", "recovers", "recovered",
    "improvement", "improve", "improves", "improved",
    "boost", "boosts", "boosted",
    "bullish", "optimistic", "positive outlook", "upbeat",
    "opportunity", "breakthrough",
    # Japanese
    "上昇", "増加", "増益", "純利益増", "営業増益", "黒字", "黒字転換",
    "最高益", "最高値", "最高", "好調", "回復", "改善", "上向き",
    "成長", "拡大", "増収", "増収増益", "上方修正", "目標上方修正",
    "買収", "配当", "増配", "特別配当", "自社株買い", "自社株消却",
    "受注", "大型受注", "提携", "業務提携", "契約", "落札",
    "新規上場", "IPO", "好業績", "業績好調",
]

# ── Negative signals ───────────────────────────────────────────────────────────
NEGATIVE_WORDS = [
    # Price / earnings weakness
    "fall", "falls", "fell", "drop", "drops", "dropped",
    "decline", "declines", "declined", "declining",
    "slump", "slumps", "slumped", "plunge", "plunges", "plunged",
    "crash", "crashes", "crashed", "tumble", "tumbles", "tumbled",
    "sink", "sinks", "sank", "slide", "slides", "slid",
    "weaken", "weakens", "weakened", "weak", "weaker",
    # Earnings / financial stress
    "loss", "losses", "net loss", "operating loss",
    "miss", "missed", "missed estimates", "below expectations",
    "disappoint", "disappointing", "disappoints", "disappointed",
    "downgrade", "downgraded", "cut guidance", "lowered forecast",
    "lowered target", "cut target", "profit warning",
    "write-down", "writedown", "write-off", "impairment",
    "deficit", "shortfall",
    # Corporate / operational negatives
    "restructure", "restructuring", "overhaul",
    "layoff", "layoffs", "job cut", "job cuts", "redundan",
    "plant closure", "factory closure", "shutdown",
    "recall", "product recall", "safety recall",
    "scandal", "fraud", "misconduct", "falsif", "corruption",
    "lawsuit", "sued", "legal action", "fine", "fined", "penalty",
    "bankruptcy", "insolvent", "default",
    # Macro risk
    "recession", "slowdown", "contraction",
    "inflation", "stagflation",
    "tariff", "tariffs", "trade war", "sanction", "sanctions",
    "crisis", "turmoil", "volatility",
    "concern", "concerns", "worry", "worried", "worries",
    "warning", "risk", "risks", "headwind", "headwinds",
    "uncertainty", "uncertain", "caution", "cautious",
    "debt", "debt burden", "leverage",
    "supply chain", "shortage", "disruption",
    "bearish", "pessimistic",
    "underperform", "sell", "avoid",
    # Japanese
    "下落", "急落", "暴落", "減少", "減益", "純利益減", "営業減益",
    "赤字", "赤字転落", "最終赤字", "低迷", "悪化", "縮小",
    "減収", "減収減益", "下方修正", "目標下方修正",
    "リストラ", "希望退職", "人員削減", "工場閉鎖",
    "不況", "景気後退", "懸念", "リスク", "警戒",
    "損失", "特別損失", "評価損", "不正", "不祥事",
    "倒産", "経営破綻", "民事再生", "訴訟",
    "業績悪化", "業績不振",
]

# ── Strong signal phrases (extra weight) ──────────────────────────────────────
STRONG_POSITIVE = [
    "record high", "all-time high", "beat expectations", "blowout",
    "massive order", "raised dividend", "special dividend",
    "transformative deal", "game changer",
    "上場来高値", "最高益更新", "大型受注", "増配発表", "自社株大幅買い",
]
STRONG_NEGATIVE = [
    "record low", "all-time low", "bankruptcy", "fraud", "scandal",
    "massive loss", "emergency", "halt", "suspended trading",
    "倒産", "不正", "損失拡大", "業績大幅下方修正", "取引停止",
    "経営危機",
]

# ── Neutral / noise words to suppress over-counting ──────────────────────────
# These appear in both positive and negative contexts — don't score them
NOISE_WORDS = [
    "investment",   # e.g. "investment risk" vs "investment opportunity"
    "deal",         # deal can be good or bad
    "cut",          # "cut costs" (good) vs "cut jobs" (bad)
    "risk",         # covered by NEGATIVE explicitly
]


def score_headline(title: str) -> int:
    """
    Score a single headline.
    Returns: -3 (very negative) to +3 (very positive)

    Improvements over v1:
    - Phrase matching before word matching (avoids double-counting)
    - Strong phrases give 2 points (not 1)
    - Noise words skipped
    - Caps at ±3
    """
    text = title.lower()
    score = 0
    matched_spans = set()  # track already-matched character ranges

    def _in_text(phrase, text):
        """Check phrase is in text and return start pos, or -1."""
        idx = text.find(phrase.lower())
        return idx

    # Strong phrases first (+2/-2 each), mark their spans
    for phrase in STRONG_POSITIVE:
        idx = _in_text(phrase, text)
        if idx >= 0:
            score += 2
            matched_spans.add((idx, idx + len(phrase)))

    for phrase in STRONG_NEGATIVE:
        idx = _in_text(phrase, text)
        if idx >= 0:
            score -= 2
            matched_spans.add((idx, idx + len(phrase)))

    # Single words (+1/-1) — skip if part of an already-matched strong phrase
    def _already_matched(idx, length):
        end = idx + length
        return any(s <= idx < e or s < end <= e for s, e in matched_spans)

    for word in POSITIVE_WORDS:
        if len(word) < 3:
            continue  # skip very short words
        idx = _in_text(word, text)
        if idx >= 0 and not _already_matched(idx, len(word)):
            score += 1

    for word in NEGATIVE_WORDS:
        if len(word) < 3:
            continue
        idx = _in_text(word, text)
        if idx >= 0 and not _already_matched(idx, len(word)):
            score -= 1

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
    if avg >= 0.25:
        label, color, icon = "Positive", "#2E7D32", "▲"
    elif avg <= -0.25:
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
