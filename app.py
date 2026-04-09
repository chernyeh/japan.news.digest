import streamlit as st
from datetime import datetime
import pytz
from collector import (fetch_all_news, fetch_source_headlines,
                        SOURCE_DIRECTORY, SOURCE_GROUPS,
                        CORP_ACTION_META, PRIORITY_ACTIONS, DIRECTION_ORDER)
from emailer import subscribe_email, send_digest, get_secret
from market_data import (fetch_market_overview, fetch_tse_movers, fetch_foreign_flow,
                          fetch_jpx_daily_movers, fetch_topix_returns,
                          fetch_underperformance_screen, TSE_STOCKS)
from watchlist import (load_watchlist, add_to_watchlist, remove_from_watchlist,
                       scan_all_watchlist, KNOWN_COMPANIES)
from sentiment import score_all_sectors, flag_high_value_articles
from jquants import (get_jquants_secret, fetch_earnings_calendar,
                     group_calendar_by_date, label_date_bucket,
                     fetch_financial_summary, format_summary_for_display,
                     safe_num, guidance_direction,
                     get_performance_band, fetch_3m_performance_batch,
                     fetch_market_data_batch, load_mktcap_from_github,
                     load_3m_perf_from_github, load_earnings_cal_from_github,
                     fetch_jpx_excel_from_github, filter_upcoming)

# ── Shared in-memory cache (survives browser close, lives as long as app is awake) ──
# Uses st.cache_resource so it's shared across ALL sessions on the same server instance.
# This means your data persists when you close and reopen the tab.
CACHE_TTL_HOURS = 3  # Show stale warning after this many hours

@st.cache_resource
def _get_app_cache():
    """Returns a single shared dict that persists across sessions."""
    return {
        "articles":         {},
        "source_map":       {},
        "market_data":      None,
        "movers":           None,
        "foreign_flow":     None,
        "sentiment_scores": {},
        "watchlist_hits":   {},
        "breaking_news":    [],
        "filings":          [],
        "ai_summaries":     {},   # key -> summary text
        "last_fetch":             None,
        "last_market_fetch":      None,
        "breaking_last_fetch":    None,
        "filings_last_fetch":     None,
    }

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Japan Investment Digest",
    page_icon="🗾",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Local timezone (Petaling Jaya = MYT = UTC+8) ─────────────────────────────
LOCAL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

def now_local():
    return datetime.now(LOCAL_TZ)

def format_local_dt(dt):
    """Format a datetime in local time."""
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt).astimezone(LOCAL_TZ)
    else:
        dt = dt.astimezone(LOCAL_TZ)
    return dt.strftime("%a, %d %b %Y · %H:%M MYT")

# ── Media source directory ────────────────────────────────────────────────────
MEDIA_SOURCES = [
    # General / Business news
    ("Japan Times",         "https://www.japantimes.co.jp/business/",     "🗞️"),
    ("Nikkei Asia",         "https://asia.nikkei.com/Business",           "📊"),
    ("Nikkei.com",          "https://www.nikkei.com/",                    "📰"),
    ("Nikkei Business",     "https://business.nikkei.com/",               "💼"),
    ("Nikkei Xtech",        "https://xtech.nikkei.com/",                  "⚙️"),
    ("Reuters Japan",       "https://www.reuters.com/world/japan/",       "📡"),
    ("NHK World Business",  "https://www3.nhk.or.jp/nhkworld/en/news/business/", "📺"),
    ("Asahi Shimbun",       "https://www.asahi.com/business/",            "🗞️"),
    ("Mainichi Shimbun",    "https://mainichi.jp/english/business/",      "🗞️"),
    ("Sankei Shimbun",      "https://www.sankei.com/economy/",            "🗞️"),
    ("Yahoo Japan Business","https://news.yahoo.co.jp/categories/business","🔎"),
    ("Toyo Keizai",         "https://toyokeizai.net/",                    "📈"),
    ("Diamond Online",      "https://diamond.jp/category/economy",       "💎"),
    # Specialist trade papers
    ("Nikkan Kogyo",        "https://www.nikkan.co.jp/",                  "⚙️"),
    ("Nikkan Jidosha",      "https://www.njd.jp/",                        "🚗"),
    ("Denki Shimbun",       "https://www.denkishimbun.com/",              "⚡"),
    ("Dempa Shimbun",       "https://www.dempa.com/",                     "📻"),
    ("Kagaku Kogyo Nippo",  "https://www.kagakukogyonippo.com/",          "🧪"),
    ("Japan Marine Daily",  "https://www.jmd.co.jp/",                     "🚢"),
    ("Nikkan Kensetsu",     "https://www.constnews.com/",                 "🏗️"),
    ("Nihon Nogyo",         "https://www.agrinews.co.jp/",                "🌾"),
    ("IT Media Business",   "https://www.itmedia.co.jp/business/",       "💻"),
    ("Bloomberg Japan",     "https://www.bloomberg.com/asia",               "📰"),
    ("Japan Industry News", "https://japanindustrynews.com/",             "🏭"),
    ("FACTA",               "https://facta.co.jp/",                        "🔍"),
    # New sources added
    ("Kabutan",             "https://kabutan.jp/news/marketnews/",          "📈"),
    ("Fisco",               "https://web.fisco.jp/",                        "📊"),
    ("Jiji Press",          "https://www.jiji.com/jc/list?g=eco",           "📡"),
    ("JBpress",             "https://jbpress.ismedia.jp/",                  "📝"),
    ("TSE Manebu",          "https://tokyoipo.com/tsemanebu/",              "🏛️"),
    ("President Online",    "https://president.jp/",                        "💼"),
    ("Rakumachi",           "https://www.rakumachi.jp/news/",               "🏘️"),
    ("Zaikai Online",       "https://www.zaikai.jp/",                       "🏢"),
    ("QUICK Money World",   "https://moneyworld.jp/",                       "💰"),
]

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700;900&family=Source+Sans+3:wght@300;400;600&family=Noto+Sans+JP:wght@300;400&display=swap');

html, body, [class*="css"] {
    font-family: 'Source Sans 3', sans-serif;
    background-color: #F7F4EF;
    color: #1A1A1A;
}
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1rem; padding-bottom: 3rem; max-width: 1060px; }

/* Masthead */
.masthead {
    border-top: 4px solid #1A1A1A; border-bottom: 1px solid #1A1A1A;
    padding: 0.9rem 0 0.6rem 0; margin-bottom: 0.3rem; text-align: center;
}
.masthead-title {
    font-family: 'Playfair Display', serif; font-size: 2.2rem;
    font-weight: 900; letter-spacing: -0.02em; line-height: 1; color: #1A1A1A;
}
.masthead-sub {
    font-size: 0.68rem; font-weight: 600; letter-spacing: 0.18em;
    text-transform: uppercase; color: #6B6B6B; margin-top: 0.25rem;
}
.masthead-date { font-size: 0.72rem; color: #6B6B6B; margin-top: 0.15rem; }
.dateline-strip {
    background: #1A1A1A; color: #F7F4EF; text-align: center;
    padding: 0.25rem; font-size: 0.64rem; letter-spacing: 0.14em;
    text-transform: uppercase; font-weight: 600; margin-bottom: 0.8rem;
}

/* Toolbar buttons — smaller */
.stButton button {
    font-size: 0.75rem !important;
    padding: 0.3rem 0.7rem !important;
    height: auto !important;
    min-height: 0 !important;
}

/* Ticker strip */
.ticker-strip {
    background: #1A1A1A; border-radius: 4px; padding: 0.5rem 0.9rem;
    margin-bottom: 0.8rem; display: flex; gap: 1.2rem;
    flex-wrap: wrap; align-items: center;
}
.ticker-item { display: flex; flex-direction: column; min-width: 70px; }
.ticker-label {
    font-size: 0.58rem; font-weight: 700; letter-spacing: 0.1em;
    text-transform: uppercase; color: #888; margin-bottom: 1px;
}
.ticker-price { font-size: 0.85rem; font-weight: 700; color: #F7F4EF; }
.ticker-change-up { font-size: 0.68rem; color: #66BB6A; font-weight: 600; }
.ticker-change-dn { font-size: 0.68rem; color: #EF5350; font-weight: 600; }
.ticker-state { font-size: 0.52rem; color: #555; margin-top: 1px; }
.ticker-divider { width: 1px; background: #333; align-self: stretch; }

/* Section headers */
.section-title {
    font-family: 'Playfair Display', serif; font-size: 1.1rem; font-weight: 700;
    color: #1A1A1A; border-bottom: 2px solid #1A1A1A;
    padding-bottom: 0.3rem; margin-bottom: 0.7rem;
}
.section-subtitle {
    font-size: 0.66rem; font-weight: 600; letter-spacing: 0.12em;
    text-transform: uppercase; color: #9B8B7A; margin-bottom: 0.7rem;
}

/* Market boxes */
.flow-box {
    background: white; border-radius: 3px; padding: 0.65rem 0.9rem;
    margin-bottom: 0.4rem; border: 1px solid #E8E3DC;
}
.flow-value-up { font-size: 1.3rem; font-weight: 700; color: #2E7D32; }
.flow-value-dn { font-size: 1.3rem; font-weight: 700; color: #C62828; }
.flow-label { font-size: 0.65rem; color: #9B8B7A; margin-top: 0.1rem; }

/* Movers */
.mover-card {
    background: white; border-radius: 3px; padding: 0.45rem 0.65rem;
    margin-bottom: 0.25rem; border-left: 3px solid transparent;
    display: flex; justify-content: space-between; align-items: center;
}
.mover-card.up { border-left-color: #2E7D32; }
.mover-card.dn { border-left-color: #C62828; }
.mover-name { font-size: 0.78rem; font-weight: 600; color: #1A1A1A; }
.mover-sym  { font-size: 0.62rem; color: #9B8B7A; }
.mover-pct-up { font-size: 0.82rem; font-weight: 700; color: #2E7D32; }
.mover-pct-dn { font-size: 0.82rem; font-weight: 700; color: #C62828; }

/* Sentiment */
.sentiment-row {
    display: flex; align-items: center; justify-content: space-between;
    padding: 0.3rem 0; border-bottom: 1px solid #EDE8E0; font-size: 0.78rem;
}
.sentiment-badge {
    font-size: 0.63rem; font-weight: 700; padding: 0.12rem 0.45rem;
    border-radius: 10px; letter-spacing: 0.04em; text-transform: uppercase;
}
.badge-pos { background: #E8F5E9; color: #2E7D32; }
.badge-neg { background: #FFEBEE; color: #C62828; }
.badge-neu { background: #F5F5F5; color: #6B6B6B; }

/* Watchlist */
.watchlist-hit {
    background: #FFF8E1; border-left: 3px solid #F9A825;
    border-radius: 2px; padding: 0.55rem 0.75rem; margin-bottom: 0.35rem;
}

/* Article cards */
.article-card { border-bottom: 1px solid #D9D3C8; padding: 0.7rem 0; }

/* Unified article headline styles — used across all tabs */
.article-title a {
    font-size: 1.05rem; font-weight: 700; color: #1A1A1A;
    text-decoration: none; line-height: 1.45;
}
.article-title a:hover { color: #8B4513; text-decoration: underline; }
/* article-link: By Time / Breaking News use this class — same style */
.article-link {
    display: block; font-size: 1.05rem; font-weight: 700; color: #1A1A1A;
    text-decoration: none; line-height: 1.45; margin-bottom: 0.1rem;
}
.article-link:hover { color: #8B4513; text-decoration: underline; }
/* Japanese original title — shown below English translation */
.article-title-jp {
    font-family: 'Noto Sans JP', sans-serif; font-size: 0.88rem;
    color: #9B8B7A; margin-top: 0.12rem; font-weight: 400;
}
/* original-title: By Time / Breaking News use this class — same style */
.original-title {
    font-family: 'Noto Sans JP', sans-serif; font-size: 0.88rem;
    color: #9B8B7A; margin-top: 0.12rem; font-weight: 400;
}
.article-source {
    font-size: 0.72rem; font-weight: 700; letter-spacing: 0.13em;
    text-transform: uppercase; color: #8B4513; margin-bottom: 0.15rem;
}
.article-meta { font-size: 0.72rem; color: #9B8B7A; margin-top: 0.12rem; }
/* Date headers in By Time tab */
.date-group { margin-bottom: 0.5rem; }
.date-header {
    font-family: 'Playfair Display', serif; font-size: 1.05rem; font-weight: 700;
    color: #1A1A1A; border-bottom: 2px solid #1A1A1A;
    padding-bottom: 0.3rem; margin: 1rem 0 0.5rem 0;
}
/* hv-badge: corp action tag in By Time */
.hv-badge {
    display: inline-block; background: #8B4513; color: white;
    font-size: 0.64rem; font-weight: 700; letter-spacing: 0.07em;
    text-transform: uppercase; padding: 0.08rem 0.35rem;
    border-radius: 2px; margin-left: 0.35rem; vertical-align: middle;
}
.high-value-tag {
    display: inline-block; background: #8B4513; color: white;
    font-size: 0.57rem; font-weight: 700; letter-spacing: 0.07em;
    text-transform: uppercase; padding: 0.08rem 0.35rem;
    border-radius: 2px; margin-left: 0.35rem; vertical-align: middle;
}
/* micro/macro badges — inline after headline */
.badge-micro {
    display: inline; background: #EBF5FB; color: #1B4F72;
    font-size: 0.5rem; font-weight: 700; letter-spacing: 0.06em;
    text-transform: uppercase; padding: 0.04rem 0.28rem;
    border-radius: 8px; margin-left: 0.3rem; vertical-align: middle;
    border: 1px solid #AED6F1; white-space: nowrap;
}
.badge-macro {
    display: inline; background: #F2F3F4; color: #5D6D7E;
    font-size: 0.5rem; font-weight: 700; letter-spacing: 0.06em;
    text-transform: uppercase; padding: 0.04rem 0.28rem;
    border-radius: 8px; margin-left: 0.3rem; vertical-align: middle;
    border: 1px solid #D5D8DC; white-space: nowrap;
}
/* Signal direction badges — compact pill style */
.signal-positive, .signal-negative, .signal-mixed, .signal-neutral {
    display: inline; font-size: 0.52rem; font-weight: 700;
    padding: 0.05rem 0.28rem; border-radius: 10px;
    margin-right: 0.25rem; vertical-align: middle;
    letter-spacing: 0.03em; white-space: nowrap;
}
.signal-positive  { background: #E8F5E9; color: #2E7D32; border: 1px solid #A5D6A7; }
.signal-negative  { background: #FFEBEE; color: #C62828; border: 1px solid #FFCDD2; }
.signal-mixed     { background: #FFF3E0; color: #E65100; border: 1px solid #FFCC80; }
.signal-neutral   { background: #F5F5F5; color: #6B6B6B; border: 1px solid #E0E0E0; }
.signal-priority {
    display: inline-block; background: #F9A825; color: #1A1A1A;
    font-size: 0.55rem; font-weight: 900; padding: 0.08rem 0.3rem;
    border-radius: 2px; margin-right: 0.2rem; vertical-align: middle;
    letter-spacing: 0.06em; text-transform: uppercase;
}
.signal-company {
    display: inline-block; background: #E8F4F8; color: #0D47A1;
    font-size: 0.6rem; font-weight: 700; padding: 0.06rem 0.32rem;
    border-radius: 2px; margin-right: 0.25rem; vertical-align: middle;
}
.signal-card {
    border-left: 4px solid #D9D3C8;
    padding: 0.55rem 0.6rem 0.45rem;
    margin-bottom: 0.4rem;
    background: #FDFCFB;
    border-radius: 0 3px 3px 0;
}
.signal-card.pos { border-left-color: #2E7D32; }
.signal-card.neg { border-left-color: #C62828; }
.signal-card.mix { border-left-color: #E65100; }
.sector-header {
    font-family: 'Playfair Display', serif; font-size: 1.45rem; font-weight: 700;
    color: #1A1A1A; border-bottom: 2px solid #1A1A1A;
    padding-bottom: 0.3rem; margin-bottom: 0.15rem;
}
.sector-count {
    font-size: 0.75rem; font-weight: 600; letter-spacing: 0.11em;
    text-transform: uppercase; color: #9B8B7A; margin-bottom: 0.75rem;
}

/* Media source grid */
.media-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
    gap: 0.5rem;
    margin-top: 0.5rem;
}
.media-card {
    background: white; border: 1px solid #E8E3DC; border-radius: 4px;
    padding: 0.55rem 0.7rem; text-decoration: none;
    display: flex; align-items: center; gap: 0.4rem;
    transition: border-color 0.15s, background 0.15s;
}
.media-card:hover { border-color: #8B4513; background: #FFF8F5; }
.media-icon { font-size: 0.9rem; flex-shrink: 0; }
.media-name {
    font-size: 0.72rem; font-weight: 600; color: #1A1A1A;
    line-height: 1.2; word-break: break-word;
}

/* Tabs — scrollable, compact, no wrap */
/* Buttons — slightly compact */
.stButton > button {
    font-size: 0.72rem !important;
    padding: 0.28rem 0.7rem !important;
    font-weight: 600 !important;
}

.stTabs [data-baseweb="tab-list"] {
    background: transparent;
    gap: 0.2rem;
    border-bottom: 2px solid #1A1A1A;
    overflow-x: auto !important;
    flex-wrap: nowrap !important;
    scrollbar-width: thin;
    scrollbar-color: #8B4513 #EDE8E0;
    padding-bottom: 2px;
    -webkit-overflow-scrolling: touch;
}
.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {
    height: 3px;
}
.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb {
    background: #8B4513; border-radius: 2px;
}
.stTabs [data-baseweb="tab"] {
    background: #EDE8E0; border-radius: 3px 3px 0 0;
    font-size: 0.62rem; font-weight: 600; padding: 0.28rem 0.45rem;
    color: #6B6B6B; border: none;
    white-space: nowrap !important;
    flex-shrink: 0 !important;
}
.stTabs [aria-selected="true"] {
    background: #1A1A1A !important; color: #F7F4EF !important;
}

/* Info box */
.info-box {
    background: #EDE8E0; border-radius: 3px; padding: 0.55rem 0.8rem;
    font-size: 0.76rem; color: #6B6B6B; margin-bottom: 0.5rem;
}

/* Empty state */
.empty-state {
    font-family: 'Playfair Display', serif; font-size: 0.95rem;
    color: #6B6B6B; text-align: center; padding: 2rem 1rem; line-height: 1.8;
}

/* Instrument cards (Markets tab) */
.instrument-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
    gap: 0.5rem;
    margin-bottom: 0.3rem;
}
.instrument-card {
    background: white; border: 1px solid #E8E3DC; border-radius: 4px;
    padding: 0.6rem 0.75rem; position: relative;
}
.instrument-card.unavailable { opacity: 0.5; }
.inst-header {
    display: flex; justify-content: space-between; align-items: baseline;
    margin-bottom: 0.15rem;
}
.inst-label {
    font-size: 0.68rem; font-weight: 700; letter-spacing: 0.08em;
    text-transform: uppercase; color: #8B4513;
}
.inst-state {
    font-size: 0.55rem; color: #AAAAAA; font-weight: 400;
}
.inst-price-row {
    display: flex; align-items: baseline; gap: 0.4rem;
    margin-bottom: 0.35rem;
}
.inst-price {
    font-size: 1.15rem; font-weight: 700; color: #1A1A1A;
    font-family: 'Playfair Display', serif;
}
.inst-change {
    font-size: 0.72rem; font-weight: 600;
}
.ret-grid {
    display: grid; grid-template-columns: repeat(6, 1fr);
    gap: 0.15rem; border-top: 1px solid #EDE8E0; padding-top: 0.3rem;
}
.ret-cell { text-align: center; }
.ret-period {
    font-size: 0.5rem; font-weight: 700; letter-spacing: 0.05em;
    text-transform: uppercase; color: #AAAAAA; margin-bottom: 0.05rem;
}
.ret-value { font-size: 0.62rem; font-weight: 600; }

/* Countdown timer for long loads */
.countdown-bar {
    font-size: 0.65rem; color: #9B8B7A; text-align: right;
    padding: 0.1rem 0; letter-spacing: 0.03em;
}

/* AI Summary panel */
.ai-summary {
    background: #FDFAF7; border: 1px solid #D9D3C8; border-left: 4px solid #8B4513;
    border-radius: 4px; padding: 1.2rem 1.4rem; margin: 0.6rem 0 1rem 0;
    font-size: 1.0rem; line-height: 1.8; color: #1A1A1A;
}
.ai-summary h2 {
    font-size: 0.95rem; font-weight: 800; letter-spacing: 0.05em;
    text-transform: uppercase; color: #8B4513; margin: 1.2rem 0 0.5rem 0;
    border-bottom: 2px solid #EDE8E0; padding-bottom: 0.25rem;
}
.ai-summary h2:first-child { margin-top: 0; }
.ai-summary ul { margin: 0.4rem 0 0.6rem 1.2rem; padding: 0; }
.ai-summary li { margin-bottom: 0.55rem; font-size: 1.0rem; line-height: 1.75; }
.ai-summary li strong, .ai-summary strong { font-weight: 700; color: #1A1A1A; }
.ai-summary a.summary-link {
    display: inline-block; margin-left: 0.4rem;
    background: #8B4513; color: #FFF !important; font-size: 0.72rem;
    font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase;
    padding: 0.1rem 0.45rem; border-radius: 3px; text-decoration: none !important;
    vertical-align: middle; line-height: 1.6;
}
.ai-summary a.summary-link:hover { background: #5C2E00; }
.summary-source-text {
    display: inline-block; background: #6B6B6B; color: white;
    font-size: 0.62rem; font-weight: 700; letter-spacing: 0.07em;
    text-transform: uppercase; padding: 0.1rem 0.45rem;
    border-radius: 3px; margin-left: 0.3rem; vertical-align: middle;
}
.ai-summary p { margin: 0.5rem 0; font-size: 1.0rem; line-height: 1.8; }
.ai-summary .intro-block {
    background: #F5F0EA; border-radius: 3px; padding: 0.6rem 0.9rem;
    margin-bottom: 0.8rem; font-size: 1.0rem; line-height: 1.8;
}

/* Filings table */
.filings-table {
    width: 100%; border-collapse: collapse; font-size: 0.82rem;
    background: white; border: 1px solid #E8E3DC;
}
.filings-table th {
    background: #F5F0EA; text-align: left; padding: 0.45rem 0.6rem;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.07em;
    text-transform: uppercase; color: #5C4033; border-bottom: 2px solid #D9D3C8;
    white-space: nowrap;
}
.filings-table td {
    padding: 0.4rem 0.6rem; border-bottom: 1px solid #EDE8E0;
    vertical-align: top; line-height: 1.4;
}
.filings-table tr:hover td { background: #FDFAF7; }

/* Mobile responsive */
@media (max-width: 600px) {
    .masthead-title { font-size: 1.6rem; }
    .ticker-strip { gap: 0.7rem; padding: 0.4rem 0.6rem; }
    .ticker-price { font-size: 0.78rem; }
    .media-grid { grid-template-columns: repeat(auto-fill, minmax(120px, 1fr)); }
    .stTabs [data-baseweb="tab"] { font-size: 0.58rem; padding: 0.25rem 0.35rem; }
}
</style>
""", unsafe_allow_html=True)

# ── MSCI Sectors ──────────────────────────────────────────────────────────────
MSCI_SECTORS = [
    ("Energy", "⛽"), ("Materials", "🏭"), ("Industrials", "⚙️"),
    ("Consumer Discretionary", "🛍️"), ("Consumer Staples", "🛒"),
    ("Health Care", "💊"), ("Financials", "🏦"),
    ("Information Technology", "💻"), ("Communication Services", "📡"),
    ("Utilities", "💡"), ("Real Estate", "🏢"), ("General / Macro", "🗾"),
]

# ── Session state ─────────────────────────────────────────────────────────────
for key, default in [
    ("articles", {}), ("last_fetch", None), ("selected_sector", None),
    ("market_data", None), ("movers", None), ("foreign_flow", None),
    ("sentiment_scores", {}), ("watchlist_hits", {}),
    ("last_market_fetch", None),
    ("source_cache", {}),
    ("source_map", {}),
    ("source_selected", None),
    ("source_group", None),
    # Market / JPX
    ("jpx_movers", {}), ("topix_returns", {}),
    ("screen_data", []), ("screen_last_fetch", None), ("wl_perf", {}),
    # Breaking news
    ("breaking_news", []), ("breaking_last_fetch", None),
    # Filings
    ("filings", []), ("filings_last_fetch", None),
    # AI summaries
    ("ai_market_wrap", None), ("ai_market_wrap_ts", None), ("ai_market_wrap_idx", {}),
    # Earnings / J-Quants
    ("earnings_cal", []), ("earnings_last_fetch", None),
    ("earnings_mkt_ts", None),
    ("mktcap_map", {}), ("mktcap_loaded_ts", None), ("mktcap_load_attempted", False),
    ("perf_3m_map", {}), ("perf_3m_loaded_ts", None),
    ("earnings_auto_loaded", False),
    ("earnings_perf", {}), ("fin_summary_cache", {}),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Restore from shared cache on first page load of a new session ─────────────
if "_cache_loaded" not in st.session_state:
    _c = _get_app_cache()
    if _c["articles"]:
        st.session_state.articles          = _c["articles"]
        st.session_state.source_map        = _c["source_map"]
        st.session_state.sentiment_scores  = _c["sentiment_scores"]
        st.session_state.watchlist_hits    = _c["watchlist_hits"]
        st.session_state.last_fetch        = _c["last_fetch"]
    if _c["market_data"]:
        st.session_state.market_data       = _c["market_data"]
        st.session_state.movers            = _c["movers"]
        st.session_state.foreign_flow      = _c["foreign_flow"]
        st.session_state.last_market_fetch = _c["last_market_fetch"]
    if _c["breaking_news"]:
        st.session_state.breaking_news       = _c["breaking_news"]
        st.session_state.breaking_last_fetch = _c["breaking_last_fetch"]
    if _c.get("filings"):
        st.session_state.filings             = _c["filings"]
        st.session_state.filings_last_fetch  = _c["filings_last_fetch"]
    for _sk, _sv in _c.get("ai_summaries", {}).items():
        if _sk not in st.session_state:
            # Discard old-format briefings that used [LINK](url) instead of [N] citations
            if isinstance(_sv, str) and "](" in _sv and "[LINK]" in _sv.upper():
                continue  # force regeneration
            st.session_state[_sk] = _sv
    # Also restore _idx lookups for citation rendering
    for _sk in list(st.session_state.keys()):
        if _sk.endswith("_idx") and _sk not in st.session_state:
            _cached_idx = _c.get("ai_summaries", {}).get(_sk)
            if _cached_idx:
                st.session_state[_sk] = _cached_idx
    st.session_state._cache_loaded = True


# ── AI Summary helper ─────────────────────────────────────────────────────────
def _summary_to_html(text: str, art_index: dict = None) -> str:
    """Convert AI summary markdown to styled HTML.
    art_index: optional dict of {int: {source, url}} for resolving [N] citations.
    """
    import re as _re2, html as _html2
    lines  = text.split("\n")
    out    = []
    in_ul  = False
    in_intro = True   # first paragraph(s) before any ## header get styled as intro

    for line in lines:
        line = line.rstrip()

        # Convert **bold** → <strong>
        line = _re2.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", line)

        # [N] resolver disabled — source pills now appended via keyword matching
        if False and art_index:
            def _resolve_idx(m):
                try:
                    idx = int(m.group(1))
                    info = art_index.get(idx, {})
                    _src = info.get("source", "")
                    _u   = info.get("url", "")
                    if _src and _u and _u.startswith("http") and len(_u) > 12:
                        _display = _src if len(_src) <= 40 else _src[:38] + "…"
                        return f'<a class="summary-link" href="{_safe_url(_u)}" target="_blank">{_display}</a>'
                    elif _src:
                        return f'<span class="summary-source-text">{_src}</span>'
                except (ValueError, AttributeError):
                    pass
                return m.group(0)  # leave unchanged if can't resolve
            # Match [N] or [N][M] patterns — only pure integers inside brackets
            line = _re2.sub(r"\[(\d+)\]", _resolve_idx, line)

        # Convert [Source Name](url) → source-labelled button (legacy format)
        def _make_link(m):
            _label = m.group(1).strip()
            _u     = m.group(2).strip()
            if not _u or not _u.startswith("http") or len(_u) < 12:
                return _label
            _display = _label if len(_label) <= 40 else _label[:38] + "…"
            return f'<a class="summary-link" href="{_safe_url(_u)}" target="_blank">{_display}</a>'
        line = _re2.sub(r"\[([^\]]+)\]\(([^)]+)\)", _make_link, line)

        if line.startswith("## "):
            if in_ul:
                out.append("</ul>"); in_ul = False
            if in_intro:
                if out: out.append("</div>")
                in_intro = False
            out.append(f'<h2>{line[3:]}</h2>')

        elif line.startswith("# "):
            if in_ul:
                out.append("</ul>"); in_ul = False
            if in_intro and out:
                out.append("</div>"); in_intro = False
            out.append(f'<h2>{line[2:]}</h2>')

        elif line.startswith("- ") or line.startswith("* "):
            if in_intro and out:
                out.append("</div>"); in_intro = False
            if not in_ul:
                out.append("<ul>"); in_ul = True
            bullet_text = line[2:]
            # Auto-append source pill by keyword matching against art_index
            if art_index:
                best_idx, best_score = None, 0
                bullet_lower = bullet_text.lower()
                for _ai, _ad in art_index.items():
                    if not _ad.get("url"):
                        continue
                    _title = (_ad.get("title_for_match") or "").lower()
                    _title_words = [w for w in _re2.sub(r"[^a-z0-9 ]", "", _title).split() if len(w) > 3]
                    score = sum(1 for w in _title_words if w in bullet_lower)
                    if score > best_score:
                        best_score = score
                        best_idx = _ai
                if best_idx and best_score >= 1:
                    _src = art_index[best_idx].get("source", "")
                    _u   = art_index[best_idx].get("url", "")
                    _disp = _src if len(_src) <= 40 else _src[:38] + "…"
                    if _src and _u and _u.startswith("http"):
                        bullet_text += f' <a class="summary-link" href="{_safe_url(_u)}" target="_blank">{_disp}</a>'
                    elif _src:
                        bullet_text += f' <span class="summary-source-text">{_disp}</span>'
            out.append(f"<li>{bullet_text}</li>")

        elif line.strip() == "":
            if in_ul:
                out.append("</ul>"); in_ul = False

        else:
            if in_ul:
                out.append("</ul>"); in_ul = False
            if in_intro:
                if not any("<div class" in o for o in out):
                    out.append('<div class="intro-block">')
                out.append(f"<p>{line}</p>")
            else:
                out.append(f"<p>{line}</p>")

    if in_ul:
        out.append("</ul>")
    if in_intro and any("<div class" in o for o in out):
        out.append("</div>")
    return "\n".join(out)


def _safe_url(url: str) -> str:
    """Sanitize a URL for safe insertion into an HTML href attribute."""
    if not url or url == "#":
        return "#"
    import re as _re_url, html as _html_url
    # Strip whitespace and control characters
    url = _re_url.sub(r'[\x00-\x1f\x7f]', '', url.strip())
    # Escape & for HTML attribute context
    url = url.replace("&", "&amp;")
    # Must start with http
    if not url.startswith("http"):
        return "#"
    return url


def _safe_text(text: str) -> str:
    """Escape text for safe insertion into HTML."""
    import html as _h
    return _h.escape(str(text)) if text else ""


def render_ai_summary(articles: list, context: str, session_key: str, max_articles: int = 60, _override_btn: bool = False):
    """
    Renders an AI-powered summary panel with a Generate button.
    Uses the Anthropic API (ANTHROPIC_API_KEY in Streamlit Secrets).
    articles: list of article dicts with title/translated_title/url/source/pub_date
    context:  short description for the prompt ("last 24h news", "co filings today", etc.)
    session_key: unique key for caching the summary in session_state
    """

    if session_key not in st.session_state:
        st.session_state[session_key] = None

    if _override_btn:
        # Button already rendered by caller — do NOT create another one
        gen_btn = _override_btn
        if st.session_state.get(session_key):
            _sum_ts = st.session_state.get(session_key + "_ts")
            _sum_ts_str = (" · " + format_local_dt(_sum_ts)) if _sum_ts else ""
            st.markdown(
                f'<div style="font-size:0.65rem;color:#9B8B7A;">✨ briefing generated{_sum_ts_str}</div>',
                unsafe_allow_html=True
            )
    else:
        col_s1, col_s2 = st.columns([4, 1])
        with col_s2:
            gen_btn = st.button("✨ Summarise", key=f"btn_{session_key}", use_container_width=True)
        with col_s1:
            if st.session_state.get(session_key):
                _sum_ts = st.session_state.get(session_key + "_ts")
                _sum_ts_str = (" · generated " + format_local_dt(_sum_ts)) if _sum_ts else ""
                st.markdown(
                    f'<div style="font-size:0.68rem;color:#9B8B7A;padding-top:0.45rem;">'
                    f'✨ AI briefing{_sum_ts_str} · click Summarise to refresh</div>',
                    unsafe_allow_html=True
                )


    if gen_btn:
        if not articles:
            st.warning("No articles to summarise — fetch news first.")
        else:
            try:
                import anthropic as _anthropic
            except ImportError:
                st.error("The `anthropic` package is not installed. Add `anthropic>=0.25.0` to requirements.txt and redeploy.")
                st.stop()
            api_key = get_secret("ANTHROPIC_API_KEY")
            if not api_key:
                st.warning("ANTHROPIC_API_KEY not found in Streamlit Secrets.")
                with st.expander("How to add it"):
                    st.markdown("""
1. Go to [console.anthropic.com](https://console.anthropic.com/) and sign in (or create a free account)
2. Navigate to **API Keys** and create a new key
3. In Streamlit Cloud, open your app → **⋮ menu → Settings → Secrets**
4. Add this line (paste your actual key):
```
ANTHROPIC_API_KEY = "sk-ant-..."
```
5. Click **Save** — the app restarts in ~30 seconds and summaries will work
""")
            else:
                # Build article list for the prompt (newest first, capped)
                subset = articles[:max_articles]
                # Build index lookup: article number → {source, url}
                _art_index = {}
                lines = []
                for i, a in enumerate(subset, 1):
                    title  = a.get("title") or a.get("translated_title") or a.get("original_title","")
                    url    = a.get("url","")
                    source = a.get("source","")
                    pub    = a.get("pub_date","")
                    _art_index[i] = {"source": source, "url": url, "title_for_match": title}
                    lines.append(f"{i}. [{source}] {title} ({pub})")
                article_text = "\n".join(lines)

                prompt = f"""You are an investment analyst helping a Malaysian fundamental investor monitor Japan equities and macroeconomics.

Here are {len(subset)} headlines from {context}:

{article_text}

Write a COMPLETE, INVESTMENT-FOCUSED briefing. For each story, go beyond the headline — provide context, historical levels where relevant (e.g. previous interest rate levels, prior guidance figures, historical precedent), and flag whether this is part of a trend or a one-off event. Highlight actionable implications for stock or sector positioning.

Structure:
1. Opening paragraph (3-4 sentences): overall market tone, dominant themes, and top investment implication
2. Thematic clusters with ## headers — group stories logically (e.g. "BOJ & Rates", "Corporate Earnings", "M&A", "Yen & FX", "Energy & Commodities", "Sector Moves", "Geopolitics")
3. Under each cluster: bullet points covering every significant story
4. Closing ## What to Watch section: 3-5 forward-looking points with specific catalysts to monitor

Format rules:
- ## headers for each cluster
- Each bullet: 2-4 sentences. Lead with the key fact, then add context/comparison (e.g. "vs prior quarter", "first time since...", "X-year high/low"), then state the investment implication or risk
- Write clean prose only — do NOT include any links, URLs, brackets, or citation markers of any kind
- Cover every meaningful story — do not skip or truncate
- No preamble, no filler, no generic observations

IMPORTANT: Complete the entire briefing including What to Watch. Never truncate mid-bullet.

Respond only with the briefing."""

                with st.spinner("Generating AI summary…"):
                    try:
                        client = _anthropic.Anthropic(api_key=api_key)
                        msg = client.messages.create(
                            model="claude-haiku-4-5-20251001",
                            max_tokens=8192,
                            messages=[{"role": "user", "content": prompt}]
                        )
                        st.session_state[session_key] = msg.content[0].text
                        st.session_state[session_key + "_ts"] = now_local()
                        st.session_state[session_key + "_idx"] = _art_index
                        # Persist AI summary to shared cache
                        _get_app_cache()["ai_summaries"][session_key] = msg.content[0].text
                        _get_app_cache()["ai_summaries"][session_key + "_ts"] = st.session_state[session_key + "_ts"]
                        _get_app_cache()["ai_summaries"][session_key + "_idx"] = _art_index
                    except Exception as e:
                        st.error(f"AI summary error: {e}")

    if st.session_state[session_key]:
        _art_idx_stored = st.session_state.get(session_key + "_idx", {})
        _ts_val = st.session_state.get(session_key + "_ts")
        _ts_bar = (
            f'<div style="font-size:0.65rem;color:#9B8B7A;margin-bottom:0.5rem;'
            f'padding:0.3rem 0.5rem;background:#F0EDE8;border-radius:3px;'
            f'border-left:3px solid #8B4513;">✨ Briefing generated: {format_local_dt(_ts_val)}</div>'
        ) if _ts_val else ""
        st.markdown(
            '<div class="ai-summary">' + _ts_bar + _summary_to_html(st.session_state[session_key], _art_idx_stored) + '</div>',
            unsafe_allow_html=True
        )

# ── Masthead ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="masthead">
    <div class="masthead-title">Japan Investment Digest</div>
    <div class="masthead-sub">Japan equities · macro · corporate news · TDnet filings · JPY rates</div>
    <div class="masthead-date">{now_local().strftime('%A, %d %B %Y · %H:%M MYT')}</div>
</div>
<div class="dateline-strip">Petaling Jaya · Nikkei 225 · TOPIX · JPY Rates · TSE Timely Disclosures · 42 News Sources</div>
""", unsafe_allow_html=True)

# ── Market ticker strip ───────────────────────────────────────────────────────
def render_ticker(label, data):
    if not data or data.get("price", 0) == 0:
        return (
            '<div class="ticker-item">'
            '<div class="ticker-label">' + label + '</div>'
            '<div class="ticker-price" style="color:#555;">—</div>'
            '</div>'
        )
    price = data["price"]
    pct = data.get("pct_change", 0)
    state = data.get("state_label", "")
    chg_class = "ticker-change-up" if pct >= 0 else "ticker-change-dn"
    arrow = "▲" if pct >= 0 else "▼"
    # Format: large numbers as integers, small as 3dp
    price_str = f"{price:,.0f}" if price > 100 else f"{price:,.2f}"
    return (
        '<div class="ticker-item">'
        '<div class="ticker-label">' + label + '</div>'
        '<div class="ticker-price">' + price_str + '</div>'
        '<div class="' + chg_class + '">' + arrow + ' ' + f"{abs(pct):.2f}%" + '</div>'
        + ('<div class="ticker-state">' + state + '</div>' if state else '') +
        '</div>'
    )

if st.session_state.market_data and st.session_state.market_data.get("_source") not in (None, "error"):
    md = st.session_state.market_data
    indices = md.get("indices", {})
    forex   = md.get("forex", {})
    # Build flat dicts for the ticker render function
    ticker_items = [
        ("Nikkei",  indices.get("nikkei")),
        ("TOPIX",   indices.get("topix")),
        ("USD/JPY", forex.get("usdjpy")),
        ("EUR/JPY", forex.get("eurjpy")),
        ("CNY/JPY", forex.get("cnyjpy")),
        ("SGD/JPY", forex.get("sgdjpy")),
    ]
    ticker_html = '<div class="ticker-strip">'
    first = True
    for label, data in ticker_items:
        if not first:
            ticker_html += '<div class="ticker-divider"></div>'
        ticker_html += render_ticker(label, data)
        first = False
    last_mkt = st.session_state.last_market_fetch
    updated_str = format_local_dt(last_mkt) if last_mkt else "—"
    ticker_html += '<div style="margin-left:auto;font-size:0.55rem;color:#555;align-self:center;">Updated<br>' + updated_str + '</div>'
    ticker_html += '</div>'
    st.markdown(ticker_html, unsafe_allow_html=True)

# ── Toolbar ───────────────────────────────────────────────────────────────────
col_info, col_spacer, col_refresh, col_clear = st.columns([3, 0.5, 0.9, 0.7])
with col_info:
    if st.session_state.last_fetch:
        total = sum(len(v) for v in st.session_state.articles.values())
        st.markdown(
            '<div style="font-size:0.72rem;color:#9B8B7A;padding-top:0.35rem;">News: '
            + format_local_dt(st.session_state.last_fetch)
            + ' · ' + str(total) + ' articles</div>',
            unsafe_allow_html=True
        )
with col_refresh:
    if st.button("🔄 Refresh", use_container_width=True):
        with st.spinner("Fetching news & markets..."):
            _cd_ph = st.empty()
            _cd_ph.markdown('<div class="countdown-bar">⏱ Fetching — typically 30–60s</div>', unsafe_allow_html=True)
            # News
            try:
                sector_map, source_map = fetch_all_news()
                st.session_state.articles   = sector_map if isinstance(sector_map, dict) else {}
                st.session_state.source_map = source_map if isinstance(source_map, dict) else {}
            except Exception as e:
                st.error("News fetch failed: " + str(e))
                st.session_state.articles = {}
                st.session_state.source_map = {}
            st.session_state.last_fetch = now_local()
            if st.session_state.articles:
                try:
                    st.session_state.sentiment_scores = score_all_sectors(st.session_state.articles)
                except Exception as e:
                    st.session_state.sentiment_scores = {}
                try:
                    wl = load_watchlist()
                    st.session_state.watchlist_hits = scan_all_watchlist(wl, st.session_state.articles)
                except Exception as e:
                    st.session_state.watchlist_hits = {}
                if not st.session_state.selected_sector:
                    for name, _ in MSCI_SECTORS:
                        if st.session_state.articles.get(name):
                            st.session_state.selected_sector = name
                            break
            # Markets
            st.session_state.market_data    = fetch_market_overview()
            st.session_state.movers         = fetch_tse_movers()
            st.session_state.foreign_flow   = fetch_foreign_flow()
            st.session_state.jpx_movers     = fetch_jpx_daily_movers()
            st.session_state.topix_returns  = fetch_topix_returns()
            st.session_state.last_market_fetch = now_local()
            # Save to shared cache
            _c = _get_app_cache()
            _c["articles"]          = st.session_state.articles
            _c["source_map"]        = st.session_state.source_map
            _c["sentiment_scores"]  = st.session_state.sentiment_scores
            _c["watchlist_hits"]    = st.session_state.watchlist_hits
            _c["last_fetch"]        = st.session_state.last_fetch
            _c["market_data"]       = st.session_state.market_data
            _c["movers"]            = st.session_state.movers
            _c["foreign_flow"]      = st.session_state.foreign_flow
            _c["last_market_fetch"] = st.session_state.last_market_fetch
        _cd_ph.empty()
        st.rerun()

with col_clear:
    if st.button("🗑️ Clear", use_container_width=True, help="Clear all cached data and summaries"):
        # Wipe the shared cache
        _c = _get_app_cache()
        for _k in list(_c.keys()):
            if isinstance(_c[_k], dict):
                _c[_k].clear()
            elif isinstance(_c[_k], list):
                _c[_k].clear()
            else:
                _c[_k] = None
        # Wipe session state
        for _k in list(st.session_state.keys()):
            del st.session_state[_k]
        st.rerun()

# ── Stale data banner (shows after 3 hours) ──────────────────────────────────
_now = now_local()
_stale_news   = st.session_state.last_fetch and (_now - st.session_state.last_fetch).total_seconds() > CACHE_TTL_HOURS * 3600
_stale_market = st.session_state.last_market_fetch and (_now - st.session_state.last_market_fetch).total_seconds() > CACHE_TTL_HOURS * 3600
_from_cache   = st.session_state.get("_cache_loaded") and (st.session_state.last_fetch or st.session_state.last_market_fetch)

if _stale_news or _stale_market:
    _stale_parts = []
    if _stale_news:
        _stale_parts.append("news (" + format_local_dt(st.session_state.last_fetch) + ")")
    if _stale_market:
        _stale_parts.append("market data (" + format_local_dt(st.session_state.last_market_fetch) + ")")
    st.markdown(
        '<div style="background:#FFF8E1;border:1px solid #FFD54F;border-radius:3px;'
        'padding:0.35rem 0.8rem;font-size:0.75rem;color:#795548;margin-bottom:0.4rem;">'
        '⏱ Data may be stale — last fetched: ' + " · ".join(_stale_parts) +
        '. Click <strong>📈 Markets</strong> or <strong>🔄 News</strong> to refresh.</div>',
        unsafe_allow_html=True
    )
elif _from_cache and not _stale_news and not _stale_market:
    # Fresh data restored from cache — show a quiet note
    _cache_time = st.session_state.last_fetch or st.session_state.last_market_fetch
    st.markdown(
        '<div style="font-size:0.66rem;color:#9B8B7A;margin-bottom:0.3rem;">'
        '✓ Restored from previous session · fetched ' + format_local_dt(_cache_time) + '</div>',
        unsafe_allow_html=True
    )

st.markdown("<div style='margin-bottom:0.2rem'></div>", unsafe_allow_html=True)


# ── Digest webhook (triggered by cron-job.org or GitHub Actions) ─────────────
# Hit: https://your-app.streamlit.app/?digest=premarket  or  ?digest=close
_digest_trigger = st.query_params.get("digest", "")
if _digest_trigger in ("premarket", "close"):
    _wh_token  = get_secret("DIGEST_WEBHOOK_TOKEN")
    _req_token = st.query_params.get("token", "")
    if _wh_token and _req_token != _wh_token:
        st.error("Unauthorised digest request.")
        st.stop()
    # Ensure we have data — fetch if needed
    _wh_articles = st.session_state.get("articles") or {}
    _wh_market   = st.session_state.get("market_data")
    _wh_filings  = st.session_state.get("filings", [])
    if not _wh_articles:
        from collector import fetch_all_news as _fn
        _wh_sector_map, _ = _fn()
        _wh_articles = _wh_sector_map
    if not _wh_market:
        _wh_market = fetch_market_overview()
    try:
        from emailer import send_digest as _sd
        _ok = _sd(
            articles_by_sector=_wh_articles,
            edition=_digest_trigger,
            market_data=_wh_market,
            filings=_wh_filings,
        )
        st.success(f"✅ {_digest_trigger.title()} digest sent to all subscribers." if _ok
                   else "⚠️ Digest send failed — check SENDGRID_API_KEY.")
    except Exception as _e:
        st.error(f"Digest webhook error: {_e}")
    st.stop()


# ── Tabs ──────────────────────────────────────────────────────────────────────
(tab_market, tab_bytime, tab_breaking, tab_signals, tab_filings,
 tab_earnings, tab_bysource, tab_news, tab_watchlist, tab_screener,
 tab_sentiment, tab_subscribe, tab_sources) = st.tabs([
    "📊 Markets", "🕐 By Time", "⚡ Breaking News",
    "🚦 Signals", "📋 Reg Filings", "📅 Earnings",
    "📁 By Source", "📰 By Industry", "⭐ Watchlist", "🔬 Screener",
    "🌡️ Sentiment", "📬 Subscribe", "🔗 Sources",
])

# ════════════════════════════════════════════════════════════
# ════════════════════════════════════════════════════════════
# TAB — BY TIME (all headlines newest first)
# ════════════════════════════════════════════════════════════
with tab_bytime:
    st.markdown('<div class="section-title">🕐 All Headlines — Latest First</div>', unsafe_allow_html=True)
    all_articles = []
    sm = st.session_state.get("sector_map", {})
    seen_urls = set()
    for sector_articles in sm.values():
        for a in sector_articles:
            url = a.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_articles.append(a)
    # Also pull from source_map for any articles not in sector_map
    srm = st.session_state.get("source_map", {})
    for src_articles in srm.values():
        for a in src_articles:
            url = a.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_articles.append(a)

    # 24h subset for summary
    from datetime import datetime as _dtnow, timedelta as _td24
    _24h_ago = _dtnow.now() - _td24(hours=24)
    articles_24h = [a for a in all_articles if a.get("pub_dt") and a["pub_dt"] >= _24h_ago]

    if not all_articles:
        st.markdown('<div class="empty-state">Fetch news first — click <strong>🔄 Fetch All News</strong>.</div>', unsafe_allow_html=True)
    else:
        _bt_col1, _bt_col2 = st.columns([4, 1])
        with _bt_col1:
            st.markdown('<div style="font-size:0.78rem;font-weight:700;letter-spacing:0.04em;color:#1A1A1A;padding-top:0.3rem;">✨ AI Briefing — Last 24 Hours</div>', unsafe_allow_html=True)
        with _bt_col2:
            _bytime_gen_btn = st.button("✨ Summarise", key="btn_summary_bytime", use_container_width=True)
        render_ai_summary(
            articles_24h or all_articles[:60],
            "the last 24 hours of Japan business news across all sources",
            "summary_bytime",
            _override_btn=_bytime_gen_btn
        )
        st.markdown("<hr style='border-color:#D9D3C8;margin:0.5rem 0'>", unsafe_allow_html=True)

        # Ensure high_value flags are set
        all_articles = flag_high_value_articles(all_articles)

        # Sort by pub_dt descending (articles without dates go last)
        def sort_key(a):
            dt = a.get("pub_dt")
            if dt is None:
                from datetime import datetime
                return datetime.min
            return dt
        all_articles.sort(key=sort_key, reverse=True)


        _micro_count = sum(1 for a in all_articles if a.get("news_type") == "micro")
        _macro_count = len(all_articles) - _micro_count
        st.markdown(
            f'<div class="info-box">{len(all_articles)} headlines — '
            f'<span style="color:#1B4F72;font-weight:700;">🏢 {_micro_count} company/micro</span> · '
            f'<span style="color:#4A4A4A;font-weight:700;">🌐 {_macro_count} macro/policy</span></div>',
            unsafe_allow_html=True
        )

        # ── News type filter ──────────────────────────────────────────────
        news_type_filter = st.radio(
            "Filter:", ["All", "🏢 Company / Micro", "🌐 Macro / Policy"],
            horizontal=True, key="nt_filter_bytime", label_visibility="collapsed",
        )
        _nt_map = {"All": None, "🏢 Company / Micro": "micro", "🌐 Macro / Policy": "macro"}
        _nt_sel = _nt_map[news_type_filter]
        filtered_bytime = [a for a in all_articles
                           if _nt_sel is None or a.get("news_type", "macro") == _nt_sel]

        html = ""
        last_date = None
        for a in filtered_bytime:
            pub_dt = a.get("pub_dt")
            if pub_dt:
                day_str = pub_dt.strftime("%A, %d %B %Y")
            else:
                day_str = "Undated"
            if day_str != last_date:
                if last_date is not None:
                    html += "</div>"
                html += (
                    '<div class="date-group">'
                    '<div class="date-header">' + day_str + '</div>'
                )
                last_date = day_str

            title  = a.get("title") or a.get("translated_title") or a.get("original_title", "")
            orig   = a.get("original_title", "")
            url    = a.get("url", "#")
            source = a.get("source", "")
            pub    = a.get("pub_date", "")
            is_jp  = a.get("language", "en") == "ja"
            hv = a.get("high_value", False)
            news_type = a.get("news_type", "macro")
            badge_html = '<span class="hv-badge">★ Corp Action</span>' if hv else ""
            nt_badge = ('<span class="badge-micro">🏢 Co</span>' if news_type == "micro"
                        else '<span class="badge-macro">🌐 Macro</span>')

            time_str = ""
            if pub_dt:
                time_str = format_local_dt(pub_dt)

            html += (
                '<div class="article-card">'
                '<div class="article-meta">'
                + source
                + (' · ' + time_str if time_str else '')
                + '</div>'
                '<div style="line-height:1.5;">'
                '<a class="article-link" href="' + _safe_url(url) + '" target="_blank">' + _safe_text(title) + '</a>'
                + nt_badge + (badge_html if badge_html else '')
                + '</div>'
                + ('<div class="original-title">' + orig + '</div>' if is_jp and orig and orig != title else '')
                + '</div>'
            )
        if last_date:
            html += "</div>"
        st.markdown(html, unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# TAB — BREAKING NEWS (Nikkei.com via Google News RSS)
# ════════════════════════════════════════════════════════════
with tab_breaking:
    st.markdown('<div class="section-title">⚡ Breaking News — Nikkei Shimbun</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="info-box">Latest headlines from Nikkei Shimbun (日本経済新聞) via Google News feed. '
        'Auto-translated. Click any headline to read on nikkei.com (subscription may be required).</div>',
        unsafe_allow_html=True
    )

    if "breaking_news" not in st.session_state:
        st.session_state.breaking_news = []
    if "breaking_last_fetch" not in st.session_state:
        st.session_state.breaking_last_fetch = None

    col_b1, col_b2 = st.columns([3, 1])
    with col_b2:
        fetch_breaking = st.button("🔄 Refresh", key="btn_breaking", use_container_width=True)
    with col_b1:
        if st.session_state.breaking_last_fetch:
            st.markdown(
                '<div style="font-size:0.7rem;color:#9B8B7A;padding-top:0.5rem;">Updated: '
                + format_local_dt(st.session_state.breaking_last_fetch) + '</div>',
                unsafe_allow_html=True
            )

    if fetch_breaking or not st.session_state.breaking_news:
        with st.spinner("Fetching Nikkei breaking news…"):
            try:
                from collector import fetch_rss, translate_articles
                # Google News RSS for nikkei.com — broader than category page
                breaking = fetch_rss(
                    "Nikkei Shimbun",
                    "https://news.google.com/rss/search?q=site:nikkei.com&hl=ja&gl=JP&ceid=JP:ja",
                    "ja"
                )
                # Also fetch the Nikkei main news category RSS if available
                import feedparser, requests as _req
                try:
                    _hdrs = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36", "Accept": "application/rss+xml,*/*", "Referer": "https://www.google.com/"}
                    _r = _req.get("https://news.google.com/rss/search?q=nikkei+%E6%97%A5%E7%B5%8C+%E7%B5%8C%E6%B8%88&hl=ja&gl=JP&ceid=JP:ja", headers=_hdrs, timeout=12)
                    _feed = feedparser.parse(_r.content)
                    import re as _re
                    from collector import parse_date
                    for entry in _feed.entries[:20]:
                        _title = _re.sub(r"<[^>]+>", "", entry.get("title","").strip())
                        if not _title or "nikkei" not in entry.get("link","").lower():
                            continue
                        _pub, _dt = parse_date(entry)
                        _url = entry.get("link","#")
                        if not any(a["url"] == _url for a in breaking):
                            breaking.append({"source":"Nikkei Shimbun","original_title":_title,"translated_title":"","title":"","url":_url,"pub_date":_pub,"pub_dt":_dt,"sector":"","language":"ja"})
                except Exception:
                    pass
                breaking = translate_articles(breaking)
                breaking.sort(key=lambda a: a.get("pub_dt") or __import__("datetime").datetime.min, reverse=True)
                st.session_state.breaking_news = breaking
                st.session_state.breaking_last_fetch = now_local()
                _c = _get_app_cache()
                _c["breaking_news"]       = breaking
                _c["breaking_last_fetch"] = st.session_state.breaking_last_fetch
            except Exception as e:
                st.error(f"Error fetching breaking news: {e}")

    items = st.session_state.breaking_news
    if not items:
        st.markdown('<div class="empty-state">No headlines loaded. Click Refresh.</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="section-title" style="font-size:0.78rem;margin-top:0.2rem;">✨ AI Briefing</div>', unsafe_allow_html=True)
        render_ai_summary(items, "Nikkei Shimbun breaking news", "summary_breaking")
        st.markdown("<hr style='border-color:#D9D3C8;margin:0.5rem 0'>", unsafe_allow_html=True)
        html = ""
        for a in items[:60]:
            title  = a.get("title") or a.get("translated_title") or a.get("original_title","")
            orig   = a.get("original_title","")
            url    = a.get("url","#")
            pub_dt = a.get("pub_dt")
            time_str = format_local_dt(pub_dt) if pub_dt else a.get("pub_date","")
            html += (
                '<div class="article-card">'
                '<div class="article-meta">Nikkei Shimbun'
                + (' · ' + time_str if time_str else '') + '</div>'
                '<a class="article-link" href="' + _safe_url(url) + '" target="_blank">' + _safe_text(title) + '</a>'
                + ('<div class="original-title">' + orig + '</div>' if orig and orig != title else '')
                + '</div>'
            )
        st.markdown(html, unsafe_allow_html=True)

# TAB — BY INDUSTRY (formerly News)
# ════════════════════════════════════════════════════════════
with tab_news:
    if not st.session_state.articles:
        st.markdown('<div class="empty-state">Click <strong>🔄 News</strong> above to load today\'s headlines.</div>', unsafe_allow_html=True)
    else:
        available_sectors = [
            (name, icon, len(st.session_state.articles.get(name, [])))
            for name, icon in MSCI_SECTORS
            if st.session_state.articles.get(name)
        ]
        sector_labels = [f"{icon} {name}  ({count})" for name, icon, count in available_sectors]
        sector_names  = [name for name, icon, count in available_sectors]

        # Fix issue 2: use a key tied to articles so selectbox resets cleanly
        current_index = 0
        if st.session_state.selected_sector in sector_names:
            current_index = sector_names.index(st.session_state.selected_sector)

        def _on_sector_change():
            st.session_state.selected_sector = sector_names[
                sector_labels.index(st.session_state._sector_sel)
            ]

        selected_label = st.selectbox(
            "Sector:", options=sector_labels, index=current_index,
            label_visibility="collapsed", key="_sector_sel",
            on_change=_on_sector_change,
        )
        # Sync in case on_change hasn't fired yet (first render)
        _cur = sector_names[sector_labels.index(selected_label)]
        if _cur != st.session_state.selected_sector:
            st.session_state.selected_sector = _cur

        sector_name = st.session_state.selected_sector
        raw_articles = st.session_state.articles.get(sector_name, [])
        # AI summary for this sector
        if raw_articles:
            render_ai_summary(
                raw_articles,
                f"{sector_name} sector news",
                f"summary_industry_{sector_name.replace(' ','_').replace('/','_')}"
            )
        articles = flag_high_value_articles(raw_articles)
        icon = next((i for n, i in MSCI_SECTORS if n == sector_name), "📰")

        sent = st.session_state.sentiment_scores.get(sector_name, {})
        sent_label = sent.get("label", "")
        sent_color = sent.get("color", "#6B6B6B")
        badge_class = "badge-pos" if sent_label == "Positive" else ("badge-neg" if sent_label == "Negative" else "badge-neu")
        sentiment_html = (' &nbsp;<span class="sentiment-badge ' + badge_class + '">' + sent.get("icon","") + ' ' + sent_label + '</span>') if sent_label else ""

        # ── News type filter ─────────────────────────────────────────────
        _micro_n = sum(1 for a in articles if a.get("news_type") == "micro")
        _macro_n = len(articles) - _micro_n
        _ni_filter = st.radio(
            "Filter:", ["All", "🏢 Company / Micro", "🌐 Macro / Policy"],
            horizontal=True, key=f"nt_filter_industry_{sector_name}", label_visibility="collapsed",
        )
        _ni_map = {"All": None, "🏢 Company / Micro": "micro", "🌐 Macro / Policy": "macro"}
        _ni_sel = _ni_map[_ni_filter]
        if _ni_sel:
            articles = [a for a in articles if a.get("news_type", "macro") == _ni_sel]

        _micro_shown = sum(1 for a in articles if a.get("news_type") == "micro")
        count_label = (
            str(len(articles)) + " article" + ("s" if len(articles) != 1 else "") +
            f' &nbsp;<span style="color:#1B4F72;font-size:0.62rem;">🏢 {_micro_n} co</span>'
            f' &nbsp;<span style="color:#4A4A4A;font-size:0.62rem;">🌐 {_macro_n} macro</span>'
        )
        cards = []
        for article in articles:
            orig      = article.get("original_title", "")
            trans     = article.get("translated_title", article.get("title", ""))
            source    = article.get("source", "")
            url       = article.get("url", "#")
            date      = article.get("pub_date", "")
            hv        = article.get("high_value", False)
            news_type = article.get("news_type", "macro")

            hv_tag    = '<span class="high-value-tag">★ Corp Action</span>' if hv else ""
            nt_badge  = ('<span class="badge-micro">🏢 Co</span>' if news_type == "micro"
                         else '<span class="badge-macro">🌐 Macro</span>')
            orig_part = '<div class="article-title-jp">' + orig + '</div>' if orig and orig != trans else ""
            date_part = '<div class="article-meta">' + date + '</div>' if date else ""

            cards.append(
                '<div class="article-card">'
                '<div class="article-source">' + source + '</div>'
                '<div class="article-title"><a href="' + _safe_url(url) + '" target="_blank">' + _safe_text(trans) + '</a>'
                + nt_badge + hv_tag + '</div>'
                + orig_part + date_part + '</div>'
            )

        st.markdown(
            '<div class="sector-header">' + icon + ' ' + sector_name + sentiment_html + '</div>'
            '<div class="sector-count">' + count_label + '</div>'
            + ''.join(cards),
            unsafe_allow_html=True
        )

# ════════════════════════════════════════════════════════════
# TAB 2 — MARKETS
# ════════════════════════════════════════════════════════════
with tab_market:
    md = st.session_state.market_data

    if not md:
        st.markdown('<div class="empty-state">Click <strong>📈 Markets</strong> above to load live data.</div>', unsafe_allow_html=True)
    elif md.get("_source") == "error":
        err = md.get("_error", "Unknown error")
        st.warning("⚠️ " + err)
        # Still show any sub-index data that came through from Stooq
        if md.get("indices") or md.get("forex"):
            st.markdown("Partial data from Stooq (may be incomplete):", unsafe_allow_html=False)
    else:
        RETURN_PERIODS = ["MTD", "1M", "3M", "YTD", "1Y", "3Y"]

        def fmt_price(price, is_forex=False):
            if is_forex:
                return f"{price:,.2f}"
            return f"{price:,.2f}" if price < 1000 else f"{price:,.0f}"

        def fmt_ret(v):
            if v is None:
                return '<span style="color:#BBBBBB">—</span>'
            color = "#2E7D32" if v >= 0 else "#C62828"
            arrow = "▲" if v >= 0 else "▼"
            return f'<span style="color:{color}">{arrow}{abs(v):.1f}%</span>'

        def instrument_card(data, is_forex=False):
            """Render a full instrument card with price + return grid."""
            if not data or data.get("price", 0) == 0:
                label = data.get("label", "—") if data else "—"
                err   = data.get("error", "No data") if data else "No data"
                return (
                    '<div class="instrument-card unavailable">'
                    '<div class="inst-label">' + label + '</div>'
                    '<div style="font-size:0.75rem;color:#9B8B7A;">' + err + '</div>'
                    '</div>'
                )

            price   = data["price"]
            pct     = data.get("pct_change", 0)
            chg     = data.get("change", 0)
            state   = data.get("state_label", "Last close")
            label   = data.get("label", "")
            rets    = data.get("returns", {})

            price_str = fmt_price(price, is_forex)
            chg_str   = (f"{chg:+,.3f}" if is_forex else (f"{chg:+,.0f}" if price >= 1000 else f"{chg:+,.2f}"))
            pct_color = "#2E7D32" if pct >= 0 else "#C62828"
            pct_arrow = "▲" if pct >= 0 else "▼"

            # Return cells
            ret_cells = ""
            for p in RETURN_PERIODS:
                v = rets.get(p)
                ret_cells += (
                    '<div class="ret-cell">'
                    '<div class="ret-period">' + p + '</div>'
                    '<div class="ret-value">' + fmt_ret(v) + '</div>'
                    '</div>'
                )

            return (
                '<div class="instrument-card">'
                '<div class="inst-header">'
                '<div class="inst-label">' + label + '</div>'
                '<div class="inst-state">' + state + '</div>'
                '</div>'
                '<div class="inst-price-row">'
                '<span class="inst-price">' + price_str + '</span>'
                '<span class="inst-change" style="color:' + pct_color + ';">'
                + pct_arrow + ' ' + f"{abs(pct):.2f}%" + ' (' + chg_str + ')'
                '</span>'
                '</div>'
                '<div class="ret-grid">' + ret_cells + '</div>'
                '</div>'
            )

        # ── Indices ──────────────────────────────────────────
        st.markdown('<div class="section-title">📈 Japanese Indices</div>', unsafe_allow_html=True)
        indices = md.get("indices", {})
        idx_html = '<div class="instrument-grid">'
        for key in ["nikkei", "topix"]:
            data = indices.get(key)
            if data:
                idx_html += instrument_card(data, is_forex=False)
        idx_html += '</div>'
        st.markdown(idx_html, unsafe_allow_html=True)
        # TOPIX sub-indices compact row
        _sub_items = [(k, indices.get(k)) for k in ["topix_large","topix_mid","topix_small"]
                      if indices.get(k) and indices[k].get("price",0)]
        if _sub_items:
            sub_html = '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:0.4rem;margin-top:0.35rem;">'
            for _sk, _sd in _sub_items:
                _sp = _sd.get("price",0); _spct = _sd.get("pct_change",0) or 0
                _slab = _sd.get("label", _sk.replace("_"," ").title())
                _scol = "#2E7D32" if _spct >= 0 else "#C62828"
                _sarr = "▲" if _spct >= 0 else "▼"
                sub_html += (
                    '<div style="background:white;border:1px solid #E8E3DC;border-radius:4px;padding:0.4rem 0.6rem;">'
                    f'<div style="font-size:0.6rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#6B6B6B;">{_slab}</div>'
                    f'<div style="font-size:0.9rem;font-weight:700;">{_sp:,.2f}</div>'
                    f'<div style="font-size:0.7rem;color:{_scol};font-weight:600;">{_sarr} {abs(_spct):.2f}%</div>'
                    '</div>')
            sub_html += '</div>'
            st.markdown(sub_html, unsafe_allow_html=True)

        st.markdown("<hr style='border-color:#D9D3C8;margin:0.9rem 0'>", unsafe_allow_html=True)

        # ── Forex — compact 4-column row ─────────────────────
        st.markdown('<div class="section-title">💱 Currency Pairs vs JPY</div>', unsafe_allow_html=True)
        forex = md.get("forex", {})
        _fx_pairs = [(k, forex.get(k)) for k in ["usdjpy","eurjpy","cnyjpy","sgdjpy"]
                     if forex.get(k) and forex[k].get("price")]
        if _fx_pairs:
            fx_html = '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:0.4rem;">'
            for _fk, _fd in _fx_pairs:
                _fp = _fd["price"]; _fchg = _fd.get("pct_change",0) or 0
                _flab = _fd.get("label", _fk.upper())
                _fcol = "#2E7D32" if _fchg >= 0 else "#C62828"
                _farr = "▲" if _fchg >= 0 else "▼"
                fx_html += (
                    '<div style="background:white;border:1px solid #E8E3DC;border-radius:4px;padding:0.4rem 0.6rem;">'
                    f'<div style="font-size:0.6rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#6B6B6B;">{_flab}</div>'
                    f'<div style="font-size:1.0rem;font-weight:700;">{_fp:,.2f}</div>'
                    f'<div style="font-size:0.72rem;color:{_fcol};font-weight:600;">{_farr} {abs(_fchg):.2f}%</div>'
                    '</div>')
            fx_html += '</div>'
            st.markdown(fx_html, unsafe_allow_html=True)

        st.markdown("<hr style='border-color:#D9D3C8;margin:0.9rem 0'>", unsafe_allow_html=True)


    # ── TSE Movers ───────────────────────────────────────
    movers    = st.session_state.movers or {}
    scr_data  = st.session_state.get("screen_data", [])
    topix_ret = st.session_state.get("topix_returns", {})
    _under_lookup = {d["code"]: d for d in scr_data}  # code → screen row

    # Threshold for flag — share the screener slider value if set, else 10%
    _mover_threshold = st.session_state.get("scr_threshold", 10)

    def _under_flags(symbol: str) -> str:
        """Return underperformance badges for a mover card given its .T symbol."""
        code = symbol.replace(".T", "")
        d = _under_lookup.get(code)
        if not d:
            return ""
        flags = []
        for period, key in [("3M", "under_3m"), ("6M", "under_6m"), ("12M", "under_12m")]:
            val = d.get(key)
            if val is not None and val < -_mover_threshold:
                flags.append(period)
        if not flags:
            return ""
        return (
            f' <span style="background:#C62828;color:white;font-size:0.52rem;font-weight:700;'
            f'padding:0.04rem 0.28rem;border-radius:2px;letter-spacing:0.05em;vertical-align:middle;">'
            f'⚠ {" ".join(flags)}</span>'
        )

    # ── Sector Gainers / Losers ──────────────────────────
    # Computed from the jpx_movers data by grouping stocks by sector
    _jpx_for_sector = st.session_state.get("jpx_movers", {})
    _all_stocks = _jpx_for_sector.get("gainers", []) + _jpx_for_sector.get("losers", [])
    if _all_stocks:
        # Group by sector and average pct_change
        from collections import defaultdict as _dd2
        _sect_totals = _dd2(list)
        for _s in _all_stocks:
            _sec = _s.get("sector", "") or "Other"
            if _sec:
                _sect_totals[_sec].append(_s.get("pct_change", 0) or 0)
        _sect_avgs = {s: sum(v)/len(v) for s, v in _sect_totals.items() if len(v) >= 2}
        if _sect_avgs:
            _sect_sorted = sorted(_sect_avgs.items(), key=lambda x: x[1], reverse=True)
            _sec_g = [x for x in _sect_sorted if x[1] > 0][:5]
            _sec_l = list(reversed([x for x in _sect_sorted if x[1] < 0]))[:5]
            if _sec_g or _sec_l:
                st.markdown('<div class="section-title">🏭 Sector Movers</div>', unsafe_allow_html=True)
                _sg_col, _sl_col = st.columns(2)
                with _sg_col:
                    st.markdown('<div style="font-size:0.65rem;font-weight:700;color:#2E7D32;letter-spacing:0.08em;text-transform:uppercase;margin-bottom:0.3rem;">Top Sectors</div>', unsafe_allow_html=True)
                    for _sn, _sv in _sec_g:
                        st.markdown(
                            f'<div style="display:flex;justify-content:space-between;align-items:center;'
                            f'padding:0.2rem 0.4rem;margin-bottom:0.15rem;background:#F0FAF0;border-radius:3px;">'
                            f'<span style="font-size:0.72rem;">{_sn}</span>'
                            f'<span style="font-size:0.72rem;font-weight:700;color:#2E7D32;">▲ {_sv:.2f}%</span>'
                            f'</div>', unsafe_allow_html=True)
                with _sl_col:
                    st.markdown('<div style="font-size:0.65rem;font-weight:700;color:#C62828;letter-spacing:0.08em;text-transform:uppercase;margin-bottom:0.3rem;">Weakest Sectors</div>', unsafe_allow_html=True)
                    for _sn, _sv in _sec_l:
                        st.markdown(
                            f'<div style="display:flex;justify-content:space-between;align-items:center;'
                            f'padding:0.2rem 0.4rem;margin-bottom:0.15rem;background:#FFF0F0;border-radius:3px;">'
                            f'<span style="font-size:0.72rem;">{_sn}</span>'
                            f'<span style="font-size:0.72rem;font-weight:700;color:#C62828;">▼ {abs(_sv):.2f}%</span>'
                            f'</div>', unsafe_allow_html=True)
                st.markdown("<hr style='border-color:#D9D3C8;margin:0.9rem 0'>", unsafe_allow_html=True)

    col3, col4 = st.columns(2)
    with col3:
        st.markdown('<div class="section-title">🚀 Top Gainers</div>', unsafe_allow_html=True)
        gainers = movers.get("gainers", [])
        if gainers:
            html = ""
            for m in gainers:
                flags_html = _under_flags(m["symbol"])
                html += (
                    '<div class="mover-card up">'
                    '<div><div class="mover-name">' + m["name"] + flags_html + '</div>'
                    '<div class="mover-sym">' + m["symbol"] + " · ¥" + f'{m["price"]:,.0f}' + '</div></div>'
                    '<div class="mover-pct-up">▲ ' + f'{m["pct_change"]:.2f}%' + '</div>'
                    '</div>'
                )
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.markdown('<div class="info-box">No mover data available.</div>', unsafe_allow_html=True)
    with col4:
        st.markdown('<div class="section-title">📉 Top Losers</div>', unsafe_allow_html=True)
        losers = movers.get("losers", [])
        if losers:
            html = ""
            for m in losers:
                flags_html = _under_flags(m["symbol"])
                html += (
                    '<div class="mover-card dn">'
                    '<div><div class="mover-name">' + m["name"] + flags_html + '</div>'
                    '<div class="mover-sym">' + m["symbol"] + " · ¥" + f'{m["price"]:,.0f}' + '</div></div>'
                    '<div class="mover-pct-dn">▼ ' + f'{abs(m["pct_change"]):.2f}%' + '</div>'
                    '</div>'
                )
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.markdown('<div class="info-box">No mover data available.</div>', unsafe_allow_html=True)
    if scr_data:
        st.markdown(
            f'<div style="font-size:0.62rem;color:#9B8B7A;margin-top:0.2rem;">'
            f'⚠ badge = underperforms TOPIX by >{_mover_threshold}% · '
            f'Run <strong>🔬 Screener</strong> tab to populate flags</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div style="font-size:0.62rem;color:#9B8B7A;margin-top:0.2rem;">'
            'Run the <strong>🔬 Screener</strong> tab to add underperformance flags to movers.</div>',
            unsafe_allow_html=True
        )

    st.markdown("<hr style='border-color:#D9D3C8;margin:0.9rem 0'>", unsafe_allow_html=True)

    # ── Foreign flow ─────────────────────────────────────
    st.markdown('<div class="section-title">🌍 Foreign Investor Flow</div>', unsafe_allow_html=True)
    flow = st.session_state.foreign_flow
    if flow and flow.get("available"):
        net = flow["net_billion_yen"]
        val_class = "flow-value-up" if net > 0 else "flow-value-dn"
        arrow = "▲" if net > 0 else "▼"
        st.markdown(
            '<div class="flow-box">'
            '<div class="ticker-label">Weekly Net Flow — Foreign Investors (TSE)</div>'
            '<div class="' + val_class + '">' + arrow + " ¥" + f"{abs(net):.1f}B" + '</div>'
            '<div class="flow-label">' + flow.get("direction","") + " · " + flow.get("as_of","") + '</div>'
            '</div>',
            unsafe_allow_html=True
        )
    else:
        jpx_url = (flow or {}).get("jpx_url", "https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html")
        st.markdown(
            '<div class="info-box">Foreign flow data published weekly by JPX (Thursdays). '
            '<a href="' + jpx_url + '" target="_blank" style="color:#8B4513;">→ View on JPX</a></div>',
            unsafe_allow_html=True
        )

    st.markdown("""
    <div class="info-box" style="margin-top:0.8rem">
        <strong>Key BOJ/macro themes:</strong> Rate normalisation · YCC exit · Yen carry trade ·
        Shunto wage growth · Core CPI · TSE capital efficiency reforms (PBR &lt; 1x pressure)
    </div>
    """, unsafe_allow_html=True)

    # ── Daily Market Wrap ─────────────────────────────────────────────────
    st.markdown("<hr style='border-color:#D9D3C8;margin:1rem 0 0.5rem'>", unsafe_allow_html=True)
    st.markdown('<div class="section-title" style="font-size:0.95rem;">📰 Daily Market Wrap</div>', unsafe_allow_html=True)

    jpx = st.session_state.get("jpx_movers", {})
    topix_ret = st.session_state.get("topix_returns", {})

    if not jpx:
        st.markdown(
            '<div class="empty-state">Click <strong>📈 Markets</strong> to load today\'s market wrap.</div>',
            unsafe_allow_html=True
        )
    else:
        jpx_date   = jpx.get("date", "")
        advancing  = jpx.get("advancing", 0)
        declining  = jpx.get("declining", 0)
        unchanged  = jpx.get("unchanged", 0)
        total      = jpx.get("total_stocks", 0)
        src_label  = jpx.get("source", "")

        # Breadth bar
        if total > 0:
            adv_pct = advancing / total * 100
            dec_pct = declining / total * 100
            st.markdown(
                f'<div style="margin:0.4rem 0 0.6rem;">'
                f'<span style="font-size:0.65rem;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#9B8B7A;">Market breadth · {jpx_date}</span><br>'
                f'<span style="color:#2E7D32;font-weight:700;">▲ {advancing} advancing</span>'
                f'  <span style="color:#9B8B7A;font-size:0.8rem;">·</span>  '
                f'<span style="color:#C62828;font-weight:700;">▼ {declining} declining</span>'
                f'  <span style="color:#9B8B7A;font-size:0.8rem;">·</span>  '
                f'<span style="color:#9B8B7A;">{unchanged} unchanged</span>'
                f'  <span style="color:#9B8B7A;font-size:0.75rem;">of {total} stocks</span>'
                f'</div>',
                unsafe_allow_html=True
            )

        # Top movers table
        col_g, col_l = st.columns(2)
        def _mover_row(m):
            pct = m.get("pct_change", 0)
            col  = "#2E7D32" if pct >= 0 else "#C62828"
            sign = "+" if pct >= 0 else ""
            return (
                f'<div style="padding:0.25rem 0;border-bottom:1px solid #EDE8E0;">'
                f'<span style="font-size:0.78rem;font-weight:600;">{m.get("name","")}</span> '
                f'<span style="font-size:0.65rem;color:#9B8B7A;">{m.get("code","")}</span><br>'
                f'<span style="font-size:0.75rem;color:#9B8B7A;">{m.get("sector","")[:28]}</span>'
                f'<span style="float:right;font-weight:700;color:{col};">{sign}{pct:.2f}%</span>'
                f'</div>'
            )

        with col_g:
            st.markdown('<div style="font-size:0.68rem;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#2E7D32;margin-bottom:0.3rem;">Top Gainers</div>', unsafe_allow_html=True)
            gainer_html = "".join(_mover_row(m) for m in jpx.get("gainers", [])[:8])
            st.markdown(gainer_html or "<div style='color:#9B8B7A;font-size:0.8rem;'>No data</div>", unsafe_allow_html=True)

        with col_l:
            st.markdown('<div style="font-size:0.68rem;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#C62828;margin-bottom:0.3rem;">Top Losers</div>', unsafe_allow_html=True)
            loser_html = "".join(_mover_row(m) for m in jpx.get("losers", [])[:8])
            st.markdown(loser_html or "<div style='color:#9B8B7A;font-size:0.8rem;'>No data</div>", unsafe_allow_html=True)

        # AI Market Wrap narrative
        st.markdown("<div style='margin-top:0.8rem;'></div>", unsafe_allow_html=True)
        st.markdown('<div class="section-title" style="font-size:0.78rem;margin-top:0.2rem;">✨ AI Market Wrap</div>', unsafe_allow_html=True)

        if "ai_market_wrap" not in st.session_state:
            st.session_state.ai_market_wrap = None

        col_w1, col_w2 = st.columns([4, 1])
        with col_w2:
            gen_wrap = st.button("✨ Generate", key="btn_market_wrap", use_container_width=True)
        with col_w1:
            if st.session_state.ai_market_wrap:
                _wrap_ts = st.session_state.get("ai_market_wrap_ts")
                _wrap_ts_str = (" · generated " + format_local_dt(_wrap_ts)) if _wrap_ts else ""
                st.markdown(
                    f'<div style="font-size:0.68rem;color:#9B8B7A;padding-top:0.45rem;">'
                    f'✨ Market wrap{_wrap_ts_str} · click Generate to refresh</div>',
                    unsafe_allow_html=True
                )

        if gen_wrap:
            api_key = get_secret("ANTHROPIC_API_KEY")
            if not api_key:
                st.warning("ANTHROPIC_API_KEY not set in Streamlit Secrets.")
            else:
                import anthropic as _ant
                # Build context: market breadth + movers + recent filings + news
                gainers_txt = "\n".join(f"  +{m['pct_change']:.2f}% {m['name']} ({m.get('sector','')})" for m in jpx.get("gainers",[])[:8])
                losers_txt  = "\n".join(f"  {m['pct_change']:.2f}% {m['name']} ({m.get('sector','')})" for m in jpx.get("losers",[])[:8])
                topix_txt   = ""
                if topix_ret:
                    topix_txt = f"TOPIX benchmark: 3M {topix_ret.get('3M','N/A')}, 6M {topix_ret.get('6M','N/A')}, 12M {topix_ret.get('12M','N/A')}"
                # Recent news
                _news_arts = []
                for _sec_arts in st.session_state.get("articles", {}).values():
                    _news_arts.extend(_sec_arts)
                _news_arts.sort(key=lambda a: a.get("pub_dt") or __import__("datetime").datetime.min, reverse=True)
                _wrap_art_index = {}
                _news_lines_list = []
                for _wi, _wa in enumerate(_news_arts[:30], 1):
                    _wrap_art_index[_wi] = {"source": _wa.get("source",""), "url": _wa.get("url","")}
                    _news_lines_list.append(f"{_wi}. [{_wa.get('source','')}] {_wa.get('translated_title') or _wa.get('title','')}")
                news_lines = "\n".join(_news_lines_list)
                # Recent filings
                filings = st.session_state.get("filings", [])
                filing_lines = "\n".join(
                    f"- [{f.get('code','')} {f.get('name_en') or f.get('name','')}] {f.get('title_en') or f.get('title','')}"
                    for f in filings[:15]
                )

                prompt = f"""You are a Japan equity analyst writing a concise daily market wrap for an investor.

Date: {jpx_date}
Market breadth: {advancing} advancing / {declining} declining / {unchanged} unchanged ({total} total TSE stocks)
{topix_txt}

TOP GAINERS today:
{gainers_txt}

TOP LOSERS today:
{losers_txt}

RECENT NEWS HEADLINES:
{news_lines}

RECENT TDnet FILINGS:
{filing_lines}

Write a COMPLETE investment-focused daily market wrap:
1. Opening paragraph: market tone, breadth context, and top investment implication (3-4 sentences)
2. ## Sector Moves — biggest movers with sector context; note if part of a trend or one-off
3. ## Corporate Catalysts — filings or news driving movers; include prior guidance/earnings for context
4. ## Macro & FX — yen levels, rates, key macro developments with historical context
5. ## What to Watch — 3-5 forward-looking catalysts with specific triggers to monitor

Format rules:
- ## headers for each section
- Each bullet 2-3 sentences: key fact → context/comparison → investment implication
- Write clean prose only — do NOT include any links, URLs, brackets, or citation markers
- Be direct and analytical — no padding
- COMPLETE the entire wrap, never truncate

Respond only with the market wrap."""

                with st.spinner("Generating market wrap..."):
                    try:
                        _client = _ant.Anthropic(api_key=api_key)
                        _resp   = _client.messages.create(
                            model="claude-haiku-4-5-20251001",
                            max_tokens=2000,
                            messages=[{"role": "user", "content": prompt}]
                        )
                        st.session_state.ai_market_wrap = _resp.content[0].text
                        st.session_state.ai_market_wrap_ts = now_local()
                        st.session_state.ai_market_wrap_idx = _wrap_art_index
                    except Exception as e:
                        st.error(f"AI wrap error: {e}")

        if st.session_state.ai_market_wrap:
            _wi = st.session_state.get("ai_market_wrap_idx", {})
            st.markdown(
                _summary_to_html(st.session_state.ai_market_wrap, _wi),
                unsafe_allow_html=True
            )

# ════════════════════════════════════════════════════════════
# TAB 3 — WATCHLIST
# ════════════════════════════════════════════════════════════
with tab_watchlist:
    st.markdown('<div class="section-title">⭐ My Company Watchlist</div>', unsafe_allow_html=True)
    watchlist = load_watchlist()

    col_sel, col_add = st.columns([3, 1])
    with col_sel:
        options = sorted(KNOWN_COMPANIES.keys()) + ["— Enter custom name —"]
        selected_known = st.selectbox("Add company:", options)
    with col_add:
        st.markdown("<div style='margin-top:1.55rem'>", unsafe_allow_html=True)
        if st.button("➕ Add", use_container_width=True):
            if selected_known and selected_known != "— Enter custom name —":
                add_to_watchlist(selected_known)
                st.rerun()

    custom_col, custom_btn = st.columns([3, 1])
    with custom_col:
        custom = st.text_input("Custom name / TSE code:", placeholder="e.g. Recruit, 6098.T")
    with custom_btn:
        st.markdown("<div style='margin-top:1.55rem'>", unsafe_allow_html=True)
        if st.button("➕ Add Custom", use_container_width=True):
            if custom.strip():
                add_to_watchlist(custom.strip())
                st.rerun()

    st.markdown("<hr style='border-color:#D9D3C8;margin:0.7rem 0'>", unsafe_allow_html=True)
    watchlist = load_watchlist()

    if not watchlist:
        st.markdown('<div class="info-box">Watchlist empty. Add companies above.</div>', unsafe_allow_html=True)
    else:
        for company in watchlist:
            c1, c2 = st.columns([4, 1])
            with c1:
                hits = st.session_state.watchlist_hits.get(company, [])
                hit_text = f"— {len(hits)} mention{'s' if len(hits)!=1 else ''} today" if hits else "— no mentions today"
                st.markdown(
                    '<div style="font-size:0.86rem;font-weight:600;padding:0.25rem 0;">'
                    + company + ' <span style="font-size:0.7rem;color:#9B8B7A;">' + hit_text + '</span></div>',
                    unsafe_allow_html=True
                )
            with c2:
                if st.button("Remove", key=f"rm_{company}"):
                    remove_from_watchlist(company)
                    st.rerun()

    if st.session_state.watchlist_hits:
        st.markdown("<hr style='border-color:#D9D3C8;margin:0.8rem 0'>", unsafe_allow_html=True)
        st.markdown('<div class="section-title">📌 Watchlist Mentions</div>', unsafe_allow_html=True)
        for company, arts in st.session_state.watchlist_hits.items():
            html = '<div style="font-size:0.78rem;font-weight:700;color:#F9A825;letter-spacing:0.07em;text-transform:uppercase;margin:0.5rem 0 0.25rem;">★ ' + company + '</div>'
            for a in arts[:5]:
                title  = a.get("translated_title") or a.get("title","")
                url    = a.get("url","#")
                source = a.get("source","")
                date   = a.get("pub_date","")
                date_p = '<div class="article-meta">' + date + '</div>' if date else ""
                _ca      = a.get("corp_action", "none")
                _ca_meta = CORP_ACTION_META.get(_ca, CORP_ACTION_META["none"])
                _ca_dirn = a.get("action_direction", "neutral")
                _sig_cls = f"signal-{_ca_dirn}" if _ca_dirn in ("positive","negative","mixed","neutral") else "signal-neutral"
                _ca_badge = (
                    f'<span class="{_sig_cls}" style="font-size:0.52rem;padding:0.05rem 0.25rem;">'
                    f'{_ca_meta.get("emoji","")} {_ca_meta.get("label","")}</span> '
                ) if _ca_meta.get("label") else ""
                _co_code = a.get("company_code","")
                _co_badge = f'<span class="signal-company" style="font-size:0.55rem;">{_co_code}</span> ' if _co_code else ""
                html += (
                    '<div class="watchlist-hit">'
                    '<div style="font-size:0.62rem;font-weight:700;color:#F9A825;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:0.15rem;">' + source + '</div>'
                    '<div>' + _ca_badge + _co_badge + '<a href="' + _safe_url(url) + '" target="_blank" style="font-size:0.88rem;font-weight:600;color:#1A1A1A;text-decoration:none;">' + _safe_text(title) + '</a></div>'
                    + date_p + '</div>'
                )
            st.markdown(html, unsafe_allow_html=True)

    # ── Underperformance vs TOPIX ─────────────────────────────────────────────
    st.markdown("<hr style='border-color:#D9D3C8;margin:0.8rem 0'>", unsafe_allow_html=True)
    st.markdown('<div class="section-title" style="font-size:0.95rem;">📉 Underperformance vs TOPIX</div>', unsafe_allow_html=True)

    topix_ret = st.session_state.get("topix_returns", {})
    screen_data = st.session_state.get("screen_data", [])

    # Build a lookup: company name → screen row (from TSE_STOCKS codes)
    # Map KNOWN_COMPANIES names to TSE codes
    _name_to_code = {}
    for cname, aliases in KNOWN_COMPANIES.items():
        for a in aliases:
            if a.isdigit() and len(a) == 4:
                _name_to_code[cname] = a
                break

    wl_threshold = st.slider("Flag if underperforms TOPIX by more than (%):", 0, 30, 10, 1, key="wl_under_threshold")

    col_wl_fetch, _ = st.columns([2, 3])
    with col_wl_fetch:
        if st.button("📊 Load Performance Data", key="btn_wl_perf", use_container_width=True):
            with st.spinner("Fetching performance data for watchlist..."):
                if not topix_ret:
                    topix_ret = fetch_topix_returns()
                    st.session_state.topix_returns = topix_ret
                # Fetch only the watchlist companies
                _wl_codes = [(c, n) for n in watchlist
                             for c in [_name_to_code.get(n)] if c]
                from market_data import fetch_stock_performance
                _wl_results = {}
                for code, name in _wl_codes:
                    d = fetch_stock_performance(code, name)
                    if d.get("price", 0) > 0:
                        t3  = topix_ret.get("3M")
                        t6  = topix_ret.get("6M")
                        t12 = topix_ret.get("12M")
                        d["under_3m"]  = (d.get("ret_3m")  - t3)  if d.get("ret_3m")  is not None and t3  is not None else None
                        d["under_6m"]  = (d.get("ret_6m")  - t6)  if d.get("ret_6m")  is not None and t6  is not None else None
                        d["under_12m"] = (d.get("ret_12m") - t12) if d.get("ret_12m") is not None and t12 is not None else None
                        _wl_results[name] = d
                st.session_state["wl_perf"] = _wl_results
            st.rerun()

    wl_perf = st.session_state.get("wl_perf", {})
    topix_3m  = topix_ret.get("3M")
    topix_6m  = topix_ret.get("6M")
    topix_12m = topix_ret.get("12M")

    if not wl_perf:
        st.markdown('<div class="info-box">Click <strong>📊 Load Performance Data</strong> to check underperformance for your watchlist.</div>', unsafe_allow_html=True)
    else:
        def _under_badge(val, threshold):
            if val is None: return '<span style="color:#9B8B7A;font-size:0.72rem;">N/A</span>'
            color = "#C62828" if val < -threshold else ("#2E7D32" if val >= 0 else "#6B6B6B")
            flag  = " ⚠️" if val < -threshold else ""
            return f'<span style="color:{color};font-weight:700;font-size:0.78rem;">{val:+.1f}%{flag}</span>'

        # TOPIX reference row
        bench_html = (
            f'<div style="margin-bottom:0.5rem;padding:0.4rem 0.6rem;background:#F0EDE8;border-radius:3px;">'
            f'<span style="font-size:0.68rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#6B6B6B;">TOPIX Benchmark</span>'
            f'&nbsp;&nbsp;'
            f'<span style="font-size:0.75rem;color:#1A1A1A;">3M: <strong>{f"{topix_3m:+.1f}%" if topix_3m else "N/A"}</strong></span>'
            f'&nbsp;·&nbsp;'
            f'<span style="font-size:0.75rem;color:#1A1A1A;">6M: <strong>{f"{topix_6m:+.1f}%" if topix_6m else "N/A"}</strong></span>'
            f'&nbsp;·&nbsp;'
            f'<span style="font-size:0.75rem;color:#1A1A1A;">12M: <strong>{f"{topix_12m:+.1f}%" if topix_12m else "N/A"}</strong></span>'
            f'</div>'
        )
        st.markdown(bench_html, unsafe_allow_html=True)

        for name in watchlist:
            d = wl_perf.get(name)
            if not d:
                st.markdown(
                    f'<div style="padding:0.35rem 0;border-bottom:1px solid #EDE8E0;">'
                    f'<span style="font-size:0.86rem;font-weight:600;">{name}</span>'
                    f'&nbsp;<span style="color:#9B8B7A;font-size:0.75rem;">— no price data found</span>'
                    f'</div>', unsafe_allow_html=True)
                continue
            flags = []
            if d.get("under_3m")  is not None and d["under_3m"]  < -wl_threshold: flags.append("3M")
            if d.get("under_6m")  is not None and d["under_6m"]  < -wl_threshold: flags.append("6M")
            if d.get("under_12m") is not None and d["under_12m"] < -wl_threshold: flags.append("12M")
            flag_html = (
                f'&nbsp;<span style="background:#C62828;color:white;font-size:0.55rem;font-weight:700;'
                f'padding:0.05rem 0.3rem;border-radius:2px;letter-spacing:0.06em;">⚠ UNDERPERFORM '
                f'{" ".join(flags)}</span>'
            ) if flags else ""
            pct_today = d.get("pct_change", 0)
            today_col = "#2E7D32" if pct_today >= 0 else "#C62828"
            row_html = (
                f'<div style="padding:0.4rem 0;border-bottom:1px solid #EDE8E0;">'
                f'<div style="display:flex;justify-content:space-between;align-items:baseline;">'
                f'<span style="font-size:0.86rem;font-weight:600;">{name}</span>{flag_html}'
                f'<span style="font-size:0.8rem;color:{today_col};font-weight:700;">¥{d["price"]:,.0f} ({pct_today:+.2f}% today)</span>'
                f'</div>'
                f'<div style="margin-top:0.2rem;">'
                f'<span style="font-size:0.7rem;color:#6B6B6B;">vs TOPIX → </span>'
                f'<span style="font-size:0.72rem;margin-right:0.6rem;">3M: {_under_badge(d.get("under_3m"), wl_threshold)}</span>'
                f'<span style="font-size:0.72rem;margin-right:0.6rem;">6M: {_under_badge(d.get("under_6m"), wl_threshold)}</span>'
                f'<span style="font-size:0.72rem;">12M: {_under_badge(d.get("under_12m"), wl_threshold)}</span>'
                f'</div>'
                f'</div>'
            )
            st.markdown(row_html, unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════
# TAB — CO FILINGS (TDnet via Yanoshin RSS — most reliable method)
# ════════════════════════════════════════════════════════════
with tab_filings:
    st.markdown('<div class="section-title">📋 Corporate Filings — TDnet Timely Disclosures</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="info-box">Timely disclosures (&#9002;&#26178;&#38283;&#31034;) from the Tokyo Stock Exchange &mdash; '
        'last 5 days. <strong>JPN</strong> opens the Japanese PDF directly. '
        '<strong>All Filings</strong> (&nearr;) opens the company filing history on Yanoshin where English PDFs '
        'are listed when available (required for TSE Prime companies from April 2025). '
        'Sourced via <a href="https://webapi.yanoshin.jp/tdnet/" target="_blank" style="color:#8B4513;">Yanoshin TDnet</a>.</div>',
        unsafe_allow_html=True
    )

    if "filings" not in st.session_state:
        st.session_state.filings = []
    if "filings_last_fetch" not in st.session_state:
        st.session_state.filings_last_fetch = None

    # ── Controls: keyword filter + refresh only ──
    col_f1, col_f2, col_f3 = st.columns([3, 0.8, 0.8])
    with col_f1:
        keyword_filter = st.text_input("Filter by keyword or company:", key="filings_keyword",
                                       placeholder="e.g. Toyota, 決算, dividend")
    with col_f2:
        st.markdown("<div style='margin-top:1.55rem'>", unsafe_allow_html=True)
        fetch_filings_btn = st.button("🔄 Refresh", key="btn_filings", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with col_f3:
        st.markdown("<div style='margin-top:1.55rem'>", unsafe_allow_html=True)
        _filings_sum_btn = st.button("✨ Summarise", key="btn_filings_sum", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # Auto-load on first visit OR on button click
    if fetch_filings_btn or not st.session_state.filings:
        with st.spinner("Fetching TDnet disclosures (last 5 days)…"):
            try:
                import requests as _req
                import re as _re
                import html as _html_mod
                import datetime as _dtmod
                from datetime import timedelta as _td
                from bs4 import BeautifulSoup as _BS

                _today = _dtmod.datetime.now()
                _d_from = (_today - _td(days=5)).strftime("%Y%m%d")
                _d_to   = _today.strftime("%Y%m%d")

                # Scrape Yanoshin HTML table — most reliable, clear structure
                # Format: date | [CODE]Company | FilingTitle(linked to PDF)
                _url = f"https://webapi.yanoshin.jp/webapi/tdnet/list/{_d_from}-{_d_to}.html?limit=500"
                _hdrs = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,*/*",
                    "Accept-Language": "ja,en;q=0.9",
                }
                _resp = _req.get(_url, headers=_hdrs, timeout=20)
                _soup = _BS(_resp.content, "lxml")

                filings = []
                for _row in _soup.find_all("tr"):
                    _cells = _row.find_all("td")
                    if len(_cells) < 3:
                        continue

                    # Cell 0: date/time  e.g. "2026-02-25 15:00:00"
                    _pdate_str = _cells[0].get_text(strip=True)
                    _pdate_dt  = None
                    try:
                        _pdate_dt = _dtmod.datetime.strptime(_pdate_str, "%Y-%m-%d %H:%M:%S")
                        _pdate_str = _pdate_dt.strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        try:
                            _pdate_dt = _dtmod.datetime.strptime(_pdate_str[:16], "%Y-%m-%d %H:%M")
                        except Exception:
                            pass

                    # Cell 1: [CODE]CompanyName  e.g. "[19590]クラフティア"
                    _company_raw = _cells[1].get_text(strip=True)
                    _code, _name = "", _company_raw
                    _cm = _re.match(r"\[([^\]]+)\](.*)", _company_raw)
                    if _cm:
                        _code = _cm.group(1).strip()
                        _name = _cm.group(2).strip()

                    # Cell 2: FilingTitle (with link to PDF)
                    _title_cell = _cells[2]
                    _filing_title = _title_cell.get_text(strip=True)
                    _a_tag = _title_cell.find("a")
                    _doc_url = ""
                    if _a_tag and _a_tag.get("href"):
                        _href = _a_tag["href"]
                        # Yanoshin links: /rd.php?https://... → extract real URL
                        _rd = _re.search(r"/rd\.php\?(https?://.*)", _href)
                        if _rd:
                            _doc_url = _rd.group(1)
                        elif _href.startswith("http"):
                            _doc_url = _href
                        else:
                            _doc_url = "https://webapi.yanoshin.jp" + _href

                    if not _filing_title:
                        continue

                    filings.append({
                        "code": _code,
                        "name": _name,
                        "title": _filing_title,
                        "title_en": "",          # filled by translation below
                        "pub_date": _pdate_str,
                        "pub_dt": _pdate_dt,
                        "doc_url": _doc_url,
                    })

                # Translate Japanese titles to English using Google free translate
                from collector import translate_single_google
                _to_translate = [f for f in filings if f["title"] and not f["title_en"]]
                # Translate both title AND company name in batches of 20
                for _i in range(0, min(len(_to_translate), 200), 20):
                    _batch = _to_translate[_i:_i+20]
                    for _f in _batch:
                        try:
                            _en = translate_single_google(_f["title"])
                            _f["title_en"] = _en if _en and _en != _f["title"] else _f["title"]
                        except Exception:
                            _f["title_en"] = _f["title"]
                        try:
                            _nm = translate_single_google(_f["name"])
                            _f["name_en"] = _nm if _nm and _nm != _f["name"] else _f["name"]
                        except Exception:
                            _f["name_en"] = _f["name"]

                # Sort newest first (all pub_dt are naive UTC now)
                filings.sort(key=lambda x: x["pub_dt"] or _dt.min, reverse=True)
                st.session_state.filings = filings
                st.session_state.filings_last_fetch = now_local()
                _c = _get_app_cache()
                _c["filings"]             = filings
                _c["filings_last_fetch"]  = st.session_state.filings_last_fetch

            except Exception as e:
                st.error(f"Error fetching TDnet data: {e}")
                import traceback; print(traceback.format_exc())

    filings = st.session_state.filings
    if st.session_state.filings_last_fetch:
        st.markdown(
            '<div style="font-size:0.7rem;color:#9B8B7A;margin-bottom:0.4rem;">Updated: '
            + format_local_dt(st.session_state.filings_last_fetch) + f" · {len(filings)} disclosures</div>",
            unsafe_allow_html=True
        )

    # Apply keyword filter
    kw = (keyword_filter or "").strip().lower()
    if kw:
        filings = [f for f in filings if kw in (f.get("name_en") or f.get("name","")).lower() or kw in (f.get("title_en") or f.get("title","")).lower() or kw in f.get("code","").lower()]

    if not filings:
        st.markdown('<div class="empty-state">No filings found. Click 🔄 Load to fetch disclosures.</div>', unsafe_allow_html=True)
    else:
        # ── AI summary: last 3 days only ──
        from datetime import datetime as _dt3, timedelta as _td3
        _3d_ago = _dt3.now() - _td3(days=3)
        filings_3d = [f for f in filings if f.get("pub_dt") and f["pub_dt"].replace(tzinfo=None) >= _3d_ago]
        filing_articles = [
            {
                "title": f.get("title_en") or f.get("title",""),
                "source": f.get("name_en") or f.get("name",""),
                # Use doc_url only if it's a proper tdnet URL, else skip linking
                "url": f.get("doc_url","") if f.get("doc_url","").startswith("https://www.release.tdnet") else "",
                "pub_date": f.get("pub_date",""),
                "pub_dt": None,
                "translated_title": f.get("title_en") or f.get("title",""),
                "original_title": f.get("title",""),
            }
            for f in (filings_3d or filings)[:80]
        ]
        render_ai_summary(
            filing_articles,
            "TDnet corporate filings — last 3 days",
            "summary_filings",
            max_articles=80,
            _override_btn=_filings_sum_btn
        )
        st.markdown("<hr style='border-color:#D9D3C8;margin:0.5rem 0'>", unsafe_allow_html=True)

        # ── Table header (matches TDnet layout) ──
        table_html = """
        <div style="overflow-x:auto;margin-top:0.4rem;">
        <table class="filings-table">
        <thead>
          <tr>
            <th style="width:70px">Code</th>
            <th style="width:160px">Company</th>
            <th>Title</th>
            <th style="width:120px">Date/Time</th>
            <th style="width:50px">PDF</th>
          </tr>
        </thead>
        <tbody>
        """
        for f in filings:
            _display_title = f.get("title_en") or f.get("title", "")
            _orig_title    = f.get("title", "")
            _orig_note     = (f'<div style="font-size:0.65rem;color:#9B8B7A;margin-top:2px;">{_orig_title}</div>'
                              if _orig_title and _orig_title != _display_title else "")
            # Japanese PDF link
            _jpn_url  = f.get("doc_url", "")
            _jpn_link = f'<a href="{_jpn_url}" target="_blank" style="color:#8B4513;font-size:0.75rem;font-weight:600;">PDF ↗</a>' if _jpn_url else "—"
            # "All Filings" — links to TDnet English search for this company code
            # English PDFs are only filed by some companies; this page shows all available versions
            table_html += (
                "<tr>"
                f'<td style="font-family:monospace;font-size:0.75rem;white-space:nowrap;">{f.get("code","")}</td>'
                f'<td style="font-size:0.8rem;font-weight:600;white-space:nowrap;">{f.get("name_en") or f.get("name","")}</td>'
                f'<td style="font-size:0.8rem;">{_display_title}{_orig_note}</td>'
                f'<td style="font-size:0.72rem;color:#9B8B7A;white-space:nowrap;">{f.get("pub_date","")}</td>'
                f'<td style="text-align:center;">{_jpn_link}</td>'
                "</tr>"
            )
        table_html += "</tbody></table></div>"
        st.markdown(table_html, unsafe_allow_html=True)

# TAB 4 — SENTIMENT
# ════════════════════════════════════════════════════════════
with tab_sentiment:
    st.markdown('<div class="section-title">🌡️ Sector Sentiment</div>', unsafe_allow_html=True)
    if not st.session_state.sentiment_scores:
        st.markdown('<div class="empty-state">Fetch news to generate sentiment scores.</div>', unsafe_allow_html=True)
    else:
        scores = st.session_state.sentiment_scores
        pos = sum(1 for s in scores.values() if s.get("label") == "Positive")
        neg = sum(1 for s in scores.values() if s.get("label") == "Negative")
        neu = sum(1 for s in scores.values() if s.get("label") == "Neutral")
        st.markdown(
            '<div class="info-box">Today: '
            '<strong style="color:#2E7D32">' + str(pos) + ' positive</strong> · '
            '<strong style="color:#6B6B6B">' + str(neu) + ' neutral</strong> · '
            '<strong style="color:#C62828">' + str(neg) + ' negative</strong>'
            '</div>',
            unsafe_allow_html=True
        )
        rows = ""
        for name, icon in MSCI_SECTORS:
            s = scores.get(name)
            if not s:
                continue
            label = s.get("label","Neutral")
            bc = "badge-pos" if label=="Positive" else ("badge-neg" if label=="Negative" else "badge-neu")
            rows += (
                '<div class="sentiment-row">'
                '<div style="font-weight:600;">' + icon + ' ' + name + '</div>'
                '<div style="font-size:0.7rem;color:#9B8B7A;flex:1;padding:0 0.8rem;">'
                + str(s.get("positive_count",0)) + '↑ · ' + str(s.get("negative_count",0)) + '↓ of ' + str(s.get("total_articles",0)) + '</div>'
                '<span class="sentiment-badge ' + bc + '">' + s.get("icon","") + ' ' + label + '</span>'
                '</div>'
            )
        st.markdown(rows, unsafe_allow_html=True)
        st.markdown('<div class="info-box" style="margin-top:0.8rem">Keyword-based scoring. Use as directional indicator only.</div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# TAB 5 — BY SOURCE
# ════════════════════════════════════════════════════════════
with tab_bysource:
    st.markdown('<div class="section-title">📁 Headlines by Source</div>', unsafe_allow_html=True)

    # ── Source picker: group tabs then source buttons ─────────────────────
    group_names = list(SOURCE_GROUPS.keys())
    if st.session_state.source_group not in group_names:
        st.session_state.source_group = group_names[0]

    # Group selector as compact radio
    _sel_grp = st.radio("Group:", group_names,
                        index=group_names.index(st.session_state.source_group),
                        horizontal=True, key="_src_group_radio",
                        label_visibility="collapsed")
    if _sel_grp != st.session_state.source_group:
        st.session_state.source_group = _sel_grp
        st.session_state.source_selected = None

    sources_in_group = SOURCE_GROUPS.get(st.session_state.source_group, [])
    available = [s for s in sources_in_group if s in SOURCE_DIRECTORY]

    if not available:
        st.markdown('<div class="info-box">No sources in this group.</div>', unsafe_allow_html=True)
    else:
        if st.session_state.source_selected not in available:
            st.session_state.source_selected = available[0]

        # Source pills — compact button grid
        _src_cols = st.columns(min(len(available), 4))
        for _si, _sn in enumerate(available):
            _is_sel = (_sn == st.session_state.source_selected)
            _btn_style = (
                "background:#1A1A1A;color:#F7F4EF;font-size:0.62rem;font-weight:700;"
                "padding:0.2rem 0.5rem;border-radius:3px;border:none;width:100%;cursor:pointer;"
                if _is_sel else
                "background:#EDE8E0;color:#1A1A1A;font-size:0.62rem;font-weight:600;"
                "padding:0.2rem 0.5rem;border-radius:3px;border:1px solid #D9D3C8;width:100%;cursor:pointer;"
            )
            with _src_cols[_si % 4]:
                if st.button(_sn, key=f"src_pill_{_sn}",
                             use_container_width=True,
                             type="primary" if _is_sel else "secondary"):
                    if st.session_state.source_selected != _sn:
                        st.session_state.source_selected = _sn
                        st.rerun()

        selected_source = st.session_state.source_selected

        # Load / status row
        cached = st.session_state.source_cache.get(selected_source)
        _src_l1, _src_l2 = st.columns([4, 1])
        with _src_l1:
            if cached is not None:
                st.markdown(
                    f'<div style="font-size:0.68rem;color:#9B8B7A;padding-top:0.3rem;">'
                    f'{len(cached)} headlines · {selected_source}</div>',
                    unsafe_allow_html=True)
            else:
                st.markdown(
                    f'<div style="font-size:0.68rem;color:#9B8B7A;padding-top:0.3rem;">'
                    f'{selected_source} — not yet loaded</div>',
                    unsafe_allow_html=True)
        with _src_l2:
            if st.button("🔄 Load", use_container_width=True, key="load_source"):
                with st.spinner(f"Fetching {selected_source}..."):
                    results = fetch_source_headlines(selected_source, days=14)
                    st.session_state.source_cache[selected_source] = results
                st.rerun()

        st.markdown("<div style='margin-bottom:0.3rem'></div>", unsafe_allow_html=True)

        # Display headlines
        articles = st.session_state.source_cache.get(selected_source)

        if articles is None:
            st.markdown(
                '<div class="empty-state">Click <strong>🔄 Load Headlines</strong> to fetch from ' + selected_source + '.</div>',
                unsafe_allow_html=True
            )
        elif len(articles) == 0:
            st.markdown(
                '<div class="info-box">No headlines found for ' + selected_source + ' in the last 14 days. '
                'The feed may carry fewer articles than expected.</div>',
                unsafe_allow_html=True
            )
        else:
            from sentiment import flag_high_value_articles
            articles = flag_high_value_articles(articles)

            # Group by date for cleaner layout
            by_date = {}
            undated = []
            for a in articles:
                date = a.get("pub_date", "")
                # Extract just the date portion (before the · time separator)
                date_key = date.split("·")[0].strip() if "·" in date else date
                if date_key:
                    by_date.setdefault(date_key, []).append(a)
                else:
                    undated.append(a)

            def render_source_articles(arts):
                cards = []
                for a in arts:
                    trans     = a.get("translated_title", a.get("title", ""))
                    orig      = a.get("original_title", "")
                    url       = a.get("url", "#")
                    date      = a.get("pub_date", "")
                    hv        = a.get("high_value", False)
                    news_type = a.get("news_type", "macro")
                    hv_tag    = '<span class="high-value-tag">★ Corp Action</span>' if hv else ""
                    nt_badge  = ('<span class="badge-micro">🏢 Co</span>' if news_type == "micro"
                                 else '<span class="badge-macro">🌐 Macro</span>')
                    orig_p    = '<div class="article-title-jp">' + orig + '</div>' if orig and orig != trans else ""
                    time_p    = ""
                    if "·" in date:
                        time_p = '<div class="article-meta">' + date.split("·")[1].strip() + '</div>'
                    cards.append(
                        '<div class="article-card">'
                        '<div class="article-title"><a href="' + _safe_url(url) + '" target="_blank">'
                        + _safe_text(trans) + '</a>' + nt_badge + hv_tag + '</div>'
                        + orig_p + time_p +
                        '</div>'
                    )
                return ''.join(cards)

            # Render grouped by date
            date_header_style = (
                'font-size:0.72rem;font-weight:700;letter-spacing:0.1em;'
                'text-transform:uppercase;color:#8B4513;margin:0.8rem 0 0.1rem 0;'
                'border-bottom:1px solid #D9D3C8;padding-bottom:0.2rem;'
            )
            for date_key in sorted(by_date.keys(), reverse=True):
                safe_date_key = str(date_key) if date_key else "Unknown date"
                st.markdown(
                    '<div style="' + date_header_style + '">' + safe_date_key + '</div>',
                    unsafe_allow_html=True
                )
                st.markdown(render_source_articles(by_date[date_key]), unsafe_allow_html=True)

            if undated:
                undated_style = (
                    'font-size:0.72rem;font-weight:700;letter-spacing:0.1em;'
                    'text-transform:uppercase;color:#8B4513;margin:0.8rem 0 0.1rem 0;'
                )
                st.markdown(
                    '<div style="' + undated_style + '">Date Unknown</div>',
                    unsafe_allow_html=True
                )
                st.markdown(render_source_articles(undated), unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# TAB 5 — SOURCES
# ════════════════════════════════════════════════════════════
with tab_sources:
    st.markdown('<div class="section-title">🔗 News Sources</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Tap any source to open its business section</div>', unsafe_allow_html=True)

    # General news
    st.markdown('<div style="font-size:0.72rem;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#8B4513;margin-bottom:0.4rem;">General &amp; Business News</div>', unsafe_allow_html=True)
    general = [s for s in MEDIA_SOURCES if s[2] in ["🗞️","📊","📡","📺","🔎","📈","💎"]]
    grid = '<div class="media-grid">'
    for name, url, icon in general:
        grid += '<a href="' + _safe_url(url) + '" target="_blank" class="media-card"><span class="media-icon">' + icon + '</span><span class="media-name">' + name + '</span></a>'
    grid += '</div>'
    st.markdown(grid, unsafe_allow_html=True)

    st.markdown('<div style="font-size:0.72rem;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#8B4513;margin:1rem 0 0.4rem 0;">Specialist Trade Papers</div>', unsafe_allow_html=True)
    trade = [s for s in MEDIA_SOURCES if s[2] not in ["🗞️","📊","📡","📺","🔎","📈","💎"]]
    grid2 = '<div class="media-grid">'
    for name, url, icon in trade:
        grid2 += '<a href="' + _safe_url(url) + '" target="_blank" class="media-card"><span class="media-icon">' + icon + '</span><span class="media-name">' + name + '</span></a>'
    grid2 += '</div>'
    st.markdown(grid2, unsafe_allow_html=True)

    # ── Official Data Sources ──────────────────────────────
    st.markdown('<div style="font-size:0.72rem;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#8B4513;margin:1rem 0 0.4rem 0;">Official Regulatory & Earnings Data</div>', unsafe_allow_html=True)
    official_links = [
        ("JPX Earnings Calendar", "https://www.jpx.co.jp/listing/event-schedules/financial-announcement/index.html", "📅",
         "Download Excel files of scheduled earnings announcement dates for all TSE-listed companies by fiscal quarter-end month."),
        ("EDINET (FSA Disclosures)", "https://disclosure2.edinet-fsa.go.jp/week0020.aspx", "📋",
         "Japan's official electronic disclosure system. Search annual securities reports, quarterly reports, and large shareholding filings for all listed companies."),
        ("TDnet (TSE Timely Disclosures)", "https://www.release.tdnet.info/inbs/I_main_00.html", "📢",
         "Tokyo Stock Exchange real-time corporate disclosure service. Earnings releases, guidance revisions, M&A announcements, and all material information."),
        ("JPX Listed Company Search", "https://www.jpx.co.jp/english/listing/co-search/index.html", "🔍",
         "Search individual company profiles, segment information, and announcement schedules on the TSE."),
    ]
    for _name, _url, _icon, _desc in official_links:
        st.markdown(
            f'<div style="padding:0.5rem 0;border-bottom:1px solid #EDE8E0;">'
            f'<a href="{_url}" target="_blank" style="font-size:0.88rem;font-weight:700;color:#8B4513;text-decoration:none;">{_icon} {_name} ↗</a>'
            f'<div style="font-size:0.72rem;color:#6B6B6B;margin-top:0.15rem;">{_desc}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

# ════════════════════════════════════════════════════════════
# TAB — SCREENER (Underperformance vs TOPIX)
# ════════════════════════════════════════════════════════════
with tab_screener:
    st.markdown('<div class="section-title">🔬 Performance Screener — TOPIX Top 200</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="info-box">Screen ~200 major TSE stocks for underperformance vs TOPIX over 3, 6 and 12 months. '
        'Data via Yahoo Finance. Batch fetch takes ~15–20 seconds.</div>',
        unsafe_allow_html=True
    )

    topix_ret   = st.session_state.get("topix_returns", {})
    screen_data = st.session_state.get("screen_data", [])
    screen_ts   = st.session_state.get("screen_last_fetch")

    # Controls row
    scr_col1, scr_col2, scr_col3 = st.columns([2, 2, 1])
    with scr_col1:
        scr_threshold = st.slider(
            "Underperform threshold (%):", 0, 30, 10, 1, key="scr_threshold"
        )
    with scr_col2:
        scr_period = st.radio(
            "Flag period:", ["3M", "6M", "12M", "Any"],
            horizontal=True, key="scr_period", label_visibility="collapsed"
        )
    with scr_col3:
        st.markdown("<div style='margin-top:1.4rem'>", unsafe_allow_html=True)
        fetch_screen = st.button("🔄 Run Screen", use_container_width=True, key="btn_run_screen")

    if fetch_screen:
        with st.spinner("Fetching performance data for ~200 stocks (~15–20s)..."):
            if not topix_ret:
                topix_ret = fetch_topix_returns()
                st.session_state.topix_returns = topix_ret
            screen_data = fetch_underperformance_screen(topix_returns=topix_ret, max_workers=25)
            st.session_state.screen_data = screen_data
            st.session_state.screen_last_fetch = now_local()
        st.rerun()

    if screen_ts:
        st.markdown(
            f'<div style="font-size:0.68rem;color:#9B8B7A;margin-bottom:0.4rem;">Last run: {screen_ts}</div>',
            unsafe_allow_html=True
        )

    if not screen_data:
        st.markdown(
            '<div class="empty-state">Click <strong>🔄 Run Screen</strong> to fetch performance data.</div>',
            unsafe_allow_html=True
        )
    else:
        topix_3m  = topix_ret.get("3M")
        topix_6m  = topix_ret.get("6M")
        topix_12m = topix_ret.get("12M")

        # Filter by underperformance
        def _is_flagged(d):
            t = -scr_threshold
            if scr_period == "3M":
                return d.get("under_3m") is not None and d["under_3m"] < t
            elif scr_period == "6M":
                return d.get("under_6m") is not None and d["under_6m"] < t
            elif scr_period == "12M":
                return d.get("under_12m") is not None and d["under_12m"] < t
            else:  # Any
                return (
                    (d.get("under_3m")  is not None and d["under_3m"]  < t) or
                    (d.get("under_6m")  is not None and d["under_6m"]  < t) or
                    (d.get("under_12m") is not None and d["under_12m"] < t)
                )

        flagged   = [d for d in screen_data if _is_flagged(d)]
        unflagged = [d for d in screen_data if not _is_flagged(d)]

        # Summary stats
        total_ok  = len([d for d in screen_data if d.get("price", 0) > 0])
        st.markdown(
            f'<div class="info-box">'
            f'<strong>{len(flagged)}</strong> of {total_ok} stocks underperform TOPIX by >{scr_threshold}% '
            f'over {scr_period} &nbsp;·&nbsp; '
            f'TOPIX: 3M <strong>{f"{topix_3m:+.1f}%" if topix_3m else "N/A"}</strong> · '
            f'6M <strong>{f"{topix_6m:+.1f}%" if topix_6m else "N/A"}</strong> · '
            f'12M <strong>{f"{topix_12m:+.1f}%" if topix_12m else "N/A"}</strong>'
            f'</div>',
            unsafe_allow_html=True
        )

        # Show toggle
        show_all = st.checkbox("Show all stocks (not just flagged)", key="scr_show_all")
        rows_to_show = screen_data if show_all else flagged

        if not rows_to_show:
            st.markdown(
                '<div class="info-box">No stocks match the current filter. Try lowering the threshold or changing the period.</div>',
                unsafe_allow_html=True
            )
        else:
            # Sort selector
            sort_by = st.radio(
                "Sort by:", ["12M underperformance", "6M underperformance", "3M underperformance", "Today % change"],
                horizontal=True, key="scr_sort", label_visibility="collapsed"
            )
            sort_key_map = {
                "12M underperformance": lambda d: d.get("under_12m") or 0,
                "6M underperformance":  lambda d: d.get("under_6m")  or 0,
                "3M underperformance":  lambda d: d.get("under_3m")  or 0,
                "Today % change":       lambda d: d.get("pct_change") or 0,
            }
            rows_to_show = sorted(rows_to_show, key=sort_key_map[sort_by])

            # Table header
            header = (
                '<div style="display:grid;grid-template-columns:2fr 1fr 1fr 1fr 1fr 1fr;'
                'gap:0.3rem;padding:0.3rem 0.4rem;background:#1A1A1A;color:#F7F4EF;'
                'font-size:0.62rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;'
                'border-radius:3px 3px 0 0;margin-top:0.5rem;">'
                '<div>Company</div><div style="text-align:right;">Price</div>'
                '<div style="text-align:right;">Today</div>'
                '<div style="text-align:right;">3M vs TOPIX</div>'
                '<div style="text-align:right;">6M vs TOPIX</div>'
                '<div style="text-align:right;">12M vs TOPIX</div>'
                '</div>'
            )
            st.markdown(header, unsafe_allow_html=True)

            def _cell(val, threshold):
                if val is None:
                    return '<div style="text-align:right;color:#9B8B7A;font-size:0.75rem;">N/A</div>'
                color = "#C62828" if val < -threshold else ("#2E7D32" if val >= 0 else "#6B6B6B")
                flag  = " ⚠" if val < -threshold else ""
                return f'<div style="text-align:right;color:{color};font-weight:700;font-size:0.75rem;">{val:+.1f}%{flag}</div>'

            rows_html = ""
            for i, d in enumerate(rows_to_show):
                bg = "#FAFAF8" if i % 2 == 0 else "#F7F4EF"
                pct = d.get("pct_change", 0)
                today_col = "#2E7D32" if pct >= 0 else "#C62828"
                flagged_row = _is_flagged(d)
                left_border = "border-left:3px solid #C62828;" if flagged_row else "border-left:3px solid transparent;"
                rows_html += (
                    f'<div style="display:grid;grid-template-columns:2fr 1fr 1fr 1fr 1fr 1fr;'
                    f'gap:0.3rem;padding:0.35rem 0.4rem;background:{bg};{left_border}'
                    f'border-bottom:1px solid #EDE8E0;">'
                    f'<div><span style="font-size:0.8rem;font-weight:600;">{d["name"]}</span>'
                    f'&nbsp;<span style="font-size:0.62rem;color:#9B8B7A;">{d["code"]}</span></div>'
                    f'<div style="text-align:right;font-size:0.78rem;">¥{d["price"]:,.0f}</div>'
                    f'<div style="text-align:right;color:{today_col};font-weight:700;font-size:0.78rem;">{pct:+.2f}%</div>'
                    + _cell(d.get("under_3m"), scr_threshold)
                    + _cell(d.get("under_6m"), scr_threshold)
                    + _cell(d.get("under_12m"), scr_threshold)
                    + '</div>'
                )
            st.markdown(rows_html, unsafe_allow_html=True)
            st.markdown(
                f'<div style="font-size:0.65rem;color:#9B8B7A;margin-top:0.4rem;">'
                f'Showing {len(rows_to_show)} stocks · ⚠ = underperforms TOPIX by >{scr_threshold}% · Data via Yahoo Finance</div>',
                unsafe_allow_html=True
            )


# ════════════════════════════════════════════════════════════
# TAB — SIGNAL FEED (Corporate Action Signals)
# ════════════════════════════════════════════════════════════
with tab_signals:
    st.markdown('<div class="section-title">🚦 Corporate Action Signal Feed</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="info-box">AI-classified corporate action signals from the latest news fetch. '
        'Positive signals first. Only articles where Claude identified a specific corporate action '
        'with medium or high confidence are shown. Fetch news first to populate.</div>',
        unsafe_allow_html=True
    )

    # Collect all classified articles
    _all_sig = []
    _seen_sig = set()
    for _sec_arts in st.session_state.get("articles", {}).values():
        for _a in _sec_arts:
            _url = _a.get("url", "")
            if _url and _url not in _seen_sig and _a.get("corp_action", "none") != "none":
                _seen_sig.add(_url)
                _all_sig.append(_a)
    # Also check source_map
    for _src_arts in st.session_state.get("source_map", {}).values():
        for _a in _src_arts:
            _url = _a.get("url", "")
            if _url and _url not in _seen_sig and _a.get("corp_action", "none") != "none":
                _seen_sig.add(_url)
                _all_sig.append(_a)

    if not _all_sig:
        st.markdown(
            '<div class="empty-state">No signals yet — click <strong>🔄 News</strong> to fetch '
            'and classify articles. Signals appear after the first fetch with ANTHROPIC_API_KEY set.</div>',
            unsafe_allow_html=True
        )
    else:
        # ── Filters ──────────────────────────────────────────────────────────
        _sig_col1, _sig_col2, _sig_col3 = st.columns([2, 2, 2])
        with _sig_col1:
            _dir_filter = st.multiselect(
                "Direction:", ["positive", "negative", "mixed", "neutral"],
                default=["positive", "negative", "mixed"],
                key="sig_dir_filter", label_visibility="collapsed",
                placeholder="Filter by direction..."
            )
        with _sig_col2:
            _action_opts = sorted({
                a.get("corp_action", "none")
                for a in _all_sig
                if a.get("corp_action", "none") != "none"
            })
            _action_labels = {
                k: f"{CORP_ACTION_META.get(k, {}).get('emoji', '')} {CORP_ACTION_META.get(k, {}).get('label', k)}"
                for k in _action_opts
            }
            _act_filter = st.multiselect(
                "Action type:", options=_action_opts,
                format_func=lambda k: _action_labels.get(k, k),
                key="sig_act_filter", label_visibility="collapsed",
                placeholder="Filter by action type..."
            )
        with _sig_col3:
            _conf_filter = st.radio(
                "Confidence:", ["All", "High + Medium", "High only"],
                horizontal=True, key="sig_conf_filter", label_visibility="collapsed"
            )

        # ── Apply filters ─────────────────────────────────────────────────────
        _filtered = _all_sig
        if _dir_filter:
            _filtered = [a for a in _filtered if a.get("action_direction", "neutral") in _dir_filter]
        if _act_filter:
            _filtered = [a for a in _filtered if a.get("corp_action") in _act_filter]
        if _conf_filter == "High + Medium":
            _filtered = [a for a in _filtered if a.get("signal_confidence") in ("high", "medium")]
        elif _conf_filter == "High only":
            _filtered = [a for a in _filtered if a.get("signal_confidence") == "high"]

        # ── Sort: positive first, then mixed, then negative, then neutral;
        #         within each group newest first ────────────────────────────
        _dir_order = {"positive": 0, "mixed": 1, "negative": 2, "neutral": 3}
        _priority_first = sorted(
            _filtered,
            key=lambda a: (
                0 if a.get("is_priority_signal") else 1,
                _dir_order.get(a.get("action_direction", "neutral"), 3),
                -(a.get("pub_dt") or datetime.min).timestamp() if a.get("pub_dt") else 0
            )
        )

        # Summary line
        _pos = sum(1 for a in _filtered if a.get("action_direction") == "positive")
        _neg = sum(1 for a in _filtered if a.get("action_direction") == "negative")
        _mix = sum(1 for a in _filtered if a.get("action_direction") == "mixed")
        st.markdown(
            f'<div style="font-size:0.72rem;color:#6B6B6B;margin:0.3rem 0 0.6rem;">'
            f'<strong>{len(_filtered)}</strong> signals · '
            f'<span style="color:#2E7D32;font-weight:700;">{_pos} positive</span> · '
            f'<span style="color:#C62828;font-weight:700;">{_neg} negative</span> · '
            f'<span style="color:#E65100;font-weight:700;">{_mix} mixed</span>'
            f'</div>',
            unsafe_allow_html=True
        )

        if not _priority_first:
            st.markdown(
                '<div class="info-box">No signals match the current filters.</div>',
                unsafe_allow_html=True
            )
        else:
            def _signal_card(a):
                action   = a.get("corp_action", "none")
                meta     = CORP_ACTION_META.get(action, CORP_ACTION_META["none"])
                dirn     = a.get("action_direction", "neutral")
                conf     = a.get("signal_confidence", "low")
                title    = a.get("translated_title") or a.get("title") or a.get("original_title", "")
                url      = a.get("url", "#")
                source   = a.get("source", "")
                pub      = a.get("pub_date", "")
                co_code  = a.get("company_code", "")
                co_name  = a.get("company_name_clean", "")
                is_prio  = a.get("is_priority_signal", False)
                is_jp    = a.get("language", "en") == "ja"
                orig     = a.get("original_title", "")

                # Direction class for border
                dir_class = {"positive": "pos", "negative": "neg", "mixed": "mix"}.get(dirn, "")
                # Signal badge class
                sig_class = f"signal-{dirn}" if dirn in ("positive","negative","mixed","neutral") else "signal-neutral"

                prio_badge = '<span class="signal-priority">★ Priority</span>' if is_prio else ""
                action_badge = (
                    f'<span class="{sig_class}">'
                    f'{meta.get("emoji","")} {meta.get("label","")}'
                    f'</span>'
                ) if meta.get("label") else ""
                co_badge = (
                    f'<span class="signal-company">{co_code} {co_name}</span>'
                ) if co_code or co_name else ""
                conf_dot = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(conf, "")
                orig_part = (
                    f'<div style="font-size:0.68rem;color:#9B8B7A;margin-top:0.1rem;">{orig}</div>'
                ) if is_jp and orig and orig != title else ""

                return (
                    f'<div class="signal-card {dir_class}">'
                    f'<div style="margin-bottom:0.18rem;">'
                    f'{prio_badge}{action_badge}{co_badge}'
                    f'<span style="font-size:0.6rem;color:#9B8B7A;">{conf_dot} {conf} confidence</span>'
                    f'</div>'
                    f'<div style="font-size:0.87rem;font-weight:600;line-height:1.35;">'
                    f'<a href="{url}" target="_blank" style="color:#1A1A1A;text-decoration:none;">{title}</a>'
                    f'</div>'
                    f'{orig_part}'
                    f'<div style="font-size:0.63rem;color:#9B8B7A;margin-top:0.18rem;">'
                    f'{source}{(" · " + pub) if pub else ""}'
                    f'</div>'
                    f'</div>'
                )

            html_out = "".join(_signal_card(a) for a in _priority_first)
            st.markdown(html_out, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# ════════════════════════════════════════════════════════════
# TAB — EARNINGS CALENDAR (JPX Excel + J-Quants)
# ════════════════════════════════════════════════════════════
with tab_earnings:
    st.markdown('<div class="section-title">📅 Earnings Calendar — TSE Listed Companies</div>', unsafe_allow_html=True)

    # ── How this works info box ──────────────────────────────────────────────
    st.markdown(
        '<div style="font-size:0.65rem;color:#9B8B7A;margin-bottom:0.5rem;">'
        'Data from JPX Excel files in your GitHub repo (<code>data/jpx_earnings/</code>). '
        '<a href="https://www.jpx.co.jp/listing/event-schedules/financial-announcement/index.html" '
        'target="_blank" style="color:#8B4513;">Download from JPX ↗</a>'
        '</div>',
        unsafe_allow_html=True
    )

    # ── Repo config (silent — set via Streamlit Secrets) ────────────────────
    _ec_repo_default = "chernyeh/japan.news.digest"
    _gh_token = None
    try:
        import streamlit as _st2
        _ec_repo = _st2.secrets.get("GITHUB_REPO", _ec_repo_default)
        _gh_token = _st2.secrets.get("GITHUB_TOKEN", None)
    except Exception:
        _ec_repo = _ec_repo_default

    # ── Auto-load all cron-generated data once per session ─────────────────
    # All three datasets load silently from GitHub CSVs at session start.
    # No button press needed. Data is a few days old at most (cron cadence).

    # 1. Market cap (daily cron CSV)
    # Reload if empty or not yet attempted
    if not st.session_state.get("mktcap_map") and not st.session_state.get("mktcap_load_attempted"):
        st.session_state.mktcap_load_attempted = True
        try:
            _mc_map = load_mktcap_from_github(_ec_repo, _gh_token)
            if _mc_map:
                st.session_state.mktcap_map = _mc_map
                st.session_state.mktcap_loaded_ts = now_local()
                st.session_state.mktcap_load_attempted = False  # allow refresh
        except Exception as _mce:
            print(f"Mktcap load error: {_mce}")

    # 2. 3M vs TOPIX performance (weekly cron)
    if not st.session_state.get("perf_3m_map"):
        try:
            _p3m_map = load_3m_perf_from_github(_ec_repo, _gh_token)
            if _p3m_map:
                st.session_state.perf_3m_map = _p3m_map
                st.session_state.perf_3m_loaded_ts = now_local()
        except Exception:
            pass

    # 3. Earnings calendar (weekly cron auto-downloads from JPX)
    if not st.session_state.get("earnings_auto_loaded") and not st.session_state.earnings_cal:
        try:
            _auto_cal = load_earnings_cal_from_github(_ec_repo, _gh_token)
            if _auto_cal:
                st.session_state.earnings_cal = _auto_cal
                st.session_state.earnings_last_fetch = now_local()
        except Exception:
            pass
        st.session_state.earnings_auto_loaded = True


    ec_col1, ec_col2, ec_col3 = st.columns([2, 2, 1])
    with ec_col1:
        ec_bucket = st.radio(
            "Show:", ["Next 2 Days", "This Week", "Next Week", "Next 14 Days", "Next 30 Days", "All"],
            horizontal=True, key="ec_bucket", label_visibility="collapsed",
        )
    with ec_col2:
        ec_search = st.text_input(
            "Search:", placeholder="Company name or TSE code…",
            key="ec_search", label_visibility="collapsed",
        )
    with ec_col3:
        ec_fetch = st.button("📥 Load Earnings", use_container_width=True, key="btn_ec_fetch")

    # Market cap filter + sort row
    _mf_col1, _mf_col2, _mf_col3 = st.columns([2, 2, 2])
    with _mf_col1:
        ec_mcap_min = st.number_input("Mkt Cap min (¥B)", min_value=0, value=0,
                                      step=50, key="ec_mcap_min", label_visibility="visible")
    with _mf_col2:
        ec_mcap_max = st.number_input("Mkt Cap max (¥B, 0=no limit)", min_value=0, value=0,
                                      step=500, key="ec_mcap_max", label_visibility="visible")
    with _mf_col3:
        ec_sort = st.selectbox("Sort by", ["Date", "Mkt Cap ↓", "Mkt Cap ↑", "3M vs TOPIX ↓", "3M vs TOPIX ↑"],
                              key="ec_sort", label_visibility="visible")

    if ec_fetch:
        with st.spinner("Updating earnings calendar from GitHub…"):
            import requests as _ecr
            _ec_headers = {"Accept": "application/vnd.github.v3+json"}
            if _gh_token:
                _ec_headers["Authorization"] = f"token {_gh_token}"
            _api_url = f"https://api.github.com/repos/{_ec_repo}/contents/data/jpx_earnings"
            try:
                _dir_resp = _ecr.get(_api_url, headers=_ec_headers, timeout=10)
                if _dir_resp.status_code != 200:
                    st.error(f"GitHub error {_dir_resp.status_code} — check repo settings.")
                else:
                    _xlsx_files = [f for f in _dir_resp.json()
                                   if isinstance(f, dict) and f.get("name","").lower().endswith(".xlsx")]
                    if not _xlsx_files:
                        st.warning("No .xlsx files found in data/jpx_earnings/ — upload JPX Excel files first.")
                    else:
                        _all_entries = []
                        for _xf in _xlsx_files:
                            try:
                                _dl = _ecr.get(_xf["download_url"], headers=_ec_headers, timeout=30)
                                if _dl.status_code == 200:
                                    import importlib, jquants as _jq_mod
                                    importlib.reload(_jq_mod)
                                    _all_entries.extend(
                                        _jq_mod.parse_jpx_earnings_excel(_dl.content, source_label=_xf["name"])
                                    )
                            except Exception as _xe:
                                st.error(f"Error loading {_xf['name']}: {_xe}")
                        if _all_entries:
                            _all_entries.sort(key=lambda x: x.get("announcement_date") or "9999")
                            st.session_state.earnings_cal = _all_entries
                            st.session_state.earnings_last_fetch = now_local()
                        else:
                            st.warning("Files found but no entries parsed — check Streamlit logs.")
            except Exception as _ece:
                st.error(f"Connection error: {_ece}")

        st.session_state.earnings_perf = {}  # reset market data cache on reload

    cal_all  = st.session_state.earnings_cal
    perf_map = st.session_state.earnings_perf
    wl       = load_watchlist()

    if not cal_all:
        st.markdown("""
<div class="info-box" style="border-left:4px solid #8B4513;">
<strong>No earnings data loaded yet.</strong><br><br>
<strong>Step 1:</strong> Download Excel files from <a href="https://www.jpx.co.jp/listing/event-schedules/financial-announcement/index.html" target="_blank">JPX Earnings Schedule ↗</a><br>
<strong>Step 2:</strong> Create folder <code>data/jpx_earnings/</code> in your GitHub repo and upload the files<br>
<strong>Step 3:</strong> Click <strong>🔄 Load from GitHub</strong> above
</div>""", unsafe_allow_html=True)
    else:
        # ── Stats bar ─────────────────────────────────────────────────────
        _jq_key = get_jquants_secret()
        _tomorrow_count = 0
        if _jq_key:
            _jq_cal = fetch_earnings_calendar(_jq_key)
            _tomorrow_count = len([e for e in _jq_cal if e.get("Date")])

        _mkt_ts = st.session_state.get("earnings_mkt_ts")
        _mktcap_ts = st.session_state.get("mktcap_loaded_ts")
        _p3m_ts = st.session_state.get("perf_3m_loaded_ts")
        _ts_parts = []
        if st.session_state.earnings_last_fetch:
            _ts_parts.append(f"Calendar: {format_local_dt(st.session_state.earnings_last_fetch)}")
        if _mktcap_ts:
            _mktcap_count = len(st.session_state.get("mktcap_map", {}))
            _ts_parts.append(f"Mkt cap: {_mktcap_count:,} cos · {format_local_dt(_mktcap_ts)}")
        if _p3m_ts:
            _p3m_count = len(st.session_state.get("perf_3m_map", {}))
            _ts_parts.append(f"3M perf: {_p3m_count:,} cos · {format_local_dt(_p3m_ts)}")
        elif _mkt_ts:
            _ts_parts.append(f"3M perf (partial): {format_local_dt(_mkt_ts)}")
        if _tomorrow_count:
            _ts_parts.append(f"{_tomorrow_count} filing tomorrow")
        st.markdown(
            f'<div style="font-size:0.65rem;color:#9B8B7A;margin-bottom:0.3rem;">'
            + " &nbsp;·&nbsp; ".join(f"<span>{p}</span>" for p in _ts_parts)
            + '</div>',
            unsafe_allow_html=True,
        )

        # ── Legend ────────────────────────────────────────────────────────
        st.markdown(
            '<div style="font-size:0.65rem;color:#6B6B6B;margin-bottom:0.5rem;">'
            '3M vs TOPIX: '
            '<span style="background:#C8E6C9;color:#1B5E20;padding:0.1rem 0.4rem;border-radius:2px;font-weight:700;font-size:0.62rem;">🟢 &gt;+15%</span> '
            '<span style="background:#DCEDC8;color:#2E7D32;padding:0.1rem 0.4rem;border-radius:2px;font-weight:700;font-size:0.62rem;">🟩 +5–15%</span> '
            '<span style="background:#FFF9C4;color:#6B4C00;padding:0.1rem 0.4rem;border-radius:2px;font-weight:700;font-size:0.62rem;">🟨 ±5%</span> '
            '<span style="background:#FFE0B2;color:#E65100;padding:0.1rem 0.4rem;border-radius:2px;font-weight:700;font-size:0.62rem;">🟧 -5–15%</span> '
            '<span style="background:#FFCDD2;color:#B71C1C;padding:0.1rem 0.4rem;border-radius:2px;font-weight:700;font-size:0.62rem;">🔴 &lt;-15%</span>'
            ' &nbsp;·&nbsp; <span style="color:#F9A825;font-weight:700;">★ on your watchlist</span>'
            '</div>',
            unsafe_allow_html=True,
        )

        # ── Apply filters ─────────────────────────────────────────────────
        # Bucket filter
        if ec_bucket == "Next 2 Days":
            cal_filtered = filter_upcoming(cal_all, 2)
        elif ec_bucket == "Next 14 Days":
            cal_filtered = filter_upcoming(cal_all, 14)
        elif ec_bucket == "Next 30 Days":
            cal_filtered = filter_upcoming(cal_all, 30)
        elif ec_bucket == "All":
            cal_filtered = cal_all
        else:  # This Week, Next Week
            cal_filtered = [
                e for e in cal_all
                if label_date_bucket(e.get("announcement_date", "")) == ec_bucket
            ]

        # Search filter
        _search = ec_search.strip().lower()
        if _search:
            cal_filtered = [
                e for e in cal_filtered
                if _search in (e.get("name") or "").lower()
                or _search in (e.get("code") or "")
            ]


        # Market cap filter (only applied when data is loaded)
        _ec_mcap_min = st.session_state.get("ec_mcap_min", 0) or 0
        _ec_mcap_max = st.session_state.get("ec_mcap_max", 0) or 0
        _mktcap_map_ec = st.session_state.get("mktcap_map", {})
        if (_ec_mcap_min > 0 or _ec_mcap_max > 0) and _mktcap_map_ec:
            def _mcap_val(e):
                return _mktcap_map_ec.get(e.get("code", ""))
            if _ec_mcap_min > 0:
                cal_filtered = [e for e in cal_filtered
                                if (_mcap_val(e) or 0) >= _ec_mcap_min]
            if _ec_mcap_max > 0:
                cal_filtered = [e for e in cal_filtered
                                if 0 < (_mcap_val(e) or 0) <= _ec_mcap_max]

        _perf_3m_map_ec = st.session_state.get("perf_3m_map", {})
        # Watchlist lookup
        _wl_codes = set()
        for _wn in wl:
            from watchlist import get_company_aliases as _gca
            for _a in _gca(_wn):
                if _a.isdigit() and len(_a) == 4:
                    _wl_codes.add(_a)

        if not cal_filtered:
            st.markdown('<div class="info-box">No companies match the current filter.</div>', unsafe_allow_html=True)
        else:
            # Summary
            _wl_hits = sum(1 for e in cal_filtered if e.get("code") in _wl_codes)
            _mkt_col1, _mkt_col2 = st.columns([5, 2])
            with _mkt_col1:
                st.markdown(
                    f'<div style="font-size:0.72rem;color:#6B6B6B;margin-bottom:0.3rem;">'
                    f'<strong>{len(cal_filtered):,}</strong> companies · '
                    f'<span style="color:#F9A825;font-weight:700;">★ {_wl_hits} on your watchlist</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            with _mkt_col2:
                _load_mkt = st.button("📊 Load Mkt Cap & Perf", key="btn_ec_mkt",
                                      use_container_width=True,
                                      help="Fetches market cap and 3M vs TOPIX for the companies currently visible")
            if _load_mkt:
                _vis_codes = list({e.get("code","") for e in cal_filtered if e.get("code")})
                if _vis_codes:
                    with st.spinner(f"Fetching market data for {len(_vis_codes)} companies… (~30s)"):
                        _new_perf = fetch_market_data_batch(_vis_codes)
                        # Merge into existing cache
                        for _mc, _mv in _new_perf.items():
                            st.session_state.earnings_perf[_mc] = _mv
                    st.session_state.earnings_mkt_ts = now_local()
                    st.rerun()

            # Group by announcement date
            from collections import defaultdict as _dd
            _by_date = _dd(list)
            _no_date = []
            for e in cal_filtered:
                d = e.get("announcement_date", "")
                if d:
                    _by_date[d].append(e)
                else:
                    _no_date.append(e)

            _sorted_dates = sorted(_by_date.keys())

            # ── Render date groups ─────────────────────────────────────────
            for _date_key in _sorted_dates:
                _entries = _by_date[_date_key]
                _bucket  = label_date_bucket(_date_key)

                try:
                    from datetime import datetime as _dtp
                    _date_display = _dtp.strptime(_date_key, "%Y-%m-%d").strftime("%A, %d %B %Y")
                except Exception:
                    _date_display = _date_key

                _bucket_badge = {
                    "Today":    '<span style="background:#1B5E20;color:white;font-size:0.58rem;font-weight:700;padding:0.1rem 0.3rem;border-radius:2px;margin-left:0.4rem;">TODAY</span>',
                    "Tomorrow": '<span style="background:#E65100;color:white;font-size:0.58rem;font-weight:700;padding:0.1rem 0.3rem;border-radius:2px;margin-left:0.4rem;">TOMORROW</span>',
                    "This Week":'<span style="background:#1565C0;color:white;font-size:0.58rem;font-weight:700;padding:0.1rem 0.3rem;border-radius:2px;margin-left:0.4rem;">THIS WEEK</span>',
                }.get(_bucket, "")

                st.markdown(
                    f'<div style="font-size:1.0rem;font-weight:700;color:#1A1A1A;'
                    f'border-bottom:2px solid #1A1A1A;padding-bottom:0.2rem;margin:1rem 0 0.3rem;">'
                    f'{_date_display}{_bucket_badge}'
                    f' <span style="font-size:0.68rem;font-weight:400;color:#9B8B7A;">'
                    f'— {len(_entries)} companies</span></div>',
                    unsafe_allow_html=True,
                )


                # Table header
                st.markdown(
                    '<div style="display:grid;grid-template-columns:1.5rem 0.9fr 0.5fr 0.65fr 0.9fr 0.8fr 0.9fr;"'
                    ' gap:0.25rem;padding:0.2rem 0.35rem;background:#1A1A1A;color:#F7F4EF;'
                    'font-size:0.57rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;'
                    'border-radius:3px 3px 0 0;">'
                    '<div></div><div>Company</div><div>Code</div>'
                    '<div>Period</div><div>Sector</div>'
                    '<div style="text-align:right;">Mkt Cap ¥B</div>'
                    '<div style="text-align:right;">3M vs TOPIX</div>'
                    '</div>',
                    unsafe_allow_html=True,
                )


                # Sort: watchlist first, then by selected sort key
                _ec_sort_key = st.session_state.get("ec_sort", "Date")
                def _sort_key_fn(e):
                    _wl_flag = 0 if e.get("code","") in wl else 1
                    _mc = st.session_state.get("mktcap_map", {}).get(e.get("code","")) or 0
                    _d = perf_map.get(e.get("code",""), {})
                    # Use cron CSV perf as default, button-loaded as override
                    _pf = st.session_state.get("perf_3m_map", {}).get(e.get("code",""))
                    if isinstance(_d, dict) and _d.get("vs_topix_3m") is not None:
                        _pf = _d["vs_topix_3m"]
                    elif isinstance(_d, (int, float)):
                        _pf = _d
                    _pf_sort = _pf if _pf is not None else -9999
                    if _ec_sort_key == "Mkt Cap ↓":
                        return (_wl_flag, -_mc)
                    elif _ec_sort_key == "Mkt Cap ↑":
                        return (_wl_flag, _mc if _mc > 0 else 999999)
                    elif _ec_sort_key == "3M vs TOPIX ↓":
                        return (_wl_flag, _pf_sort)
                    elif _ec_sort_key == "3M vs TOPIX ↑":
                        return (_wl_flag, -(_pf_sort if _pf_sort != -9999 else 9999))
                    else:  # Date (default)
                        return (_wl_flag, e.get("announcement_date",""), e.get("name",""))
                _sorted_entries = sorted(_entries, key=_sort_key_fn)




                rows_html = ""
                for _idx, _e in enumerate(_sorted_entries):
                    _code   = _e.get("code", "")
                    _name   = _e.get("name", "")
                    _period = _e.get("period_type", "")
                    _sector = (_e.get("sector") or "")[:22]
                    _is_wl  = _code in _wl_codes
                    _bg     = "#FFFDE7" if _is_wl else ("#FAFAF8" if _idx % 2 == 0 else "#F7F4EF")
                    _border = "border-left:3px solid #F9A825;" if _is_wl else "border-left:3px solid transparent;"
                    _star   = "★ " if _is_wl else ""
                    _weight = "700" if _is_wl else "500"

                    # Market cap from J-Quants cron CSVs (fast, no API call)
                    _mcap = st.session_state.get("mktcap_map", {}).get(_code)
                    _mcap_str = f"¥{_mcap:,.0f}B" if _mcap else "—"
                    # 3M vs TOPIX: cron CSV first, button-loaded perf_map as override
                    _perf = st.session_state.get("perf_3m_map", {}).get(_code)
                    _mkt_data = perf_map.get(_code, {})
                    if isinstance(_mkt_data, dict) and _mkt_data.get("vs_topix_3m") is not None:
                        _perf = _mkt_data["vs_topix_3m"]  # button-loaded takes priority
                    elif isinstance(_mkt_data, (int, float)):
                        _perf = _mkt_data
                    _band = get_performance_band(_perf)

                    _perf_cell = (
                        f'<div style="text-align:right;">'
                        f'<span style="background:{_band["bg"]};color:{_band["color"]};'
                        f'font-size:0.68rem;font-weight:700;padding:0.08rem 0.35rem;border-radius:3px;">'
                        f'{_band["emoji"]} {_band["label"]}</span></div>'
                    )

                    _mcap_cell = (
                        f'<div style="text-align:right;font-size:0.68rem;color:#1A1A1A;font-family:monospace;">{_mcap_str}</div>'
                    )
                    rows_html += (
                        f'<div style="display:grid;grid-template-columns:1.5rem 0.9fr 0.5fr 0.65fr 0.9fr 0.8fr 0.9fr;'
                        f'gap:0.25rem;padding:0.28rem 0.35rem;background:{_bg};{_border}'
                        f'border-bottom:1px solid #EDE8E0;align-items:center;">'
                        f'<div style="color:#F9A825;font-size:0.72rem;">{_star}</div>'
                        f'<div style="font-size:0.78rem;font-weight:{_weight};">{_name}</div>'
                        f'<div style="font-size:0.68rem;color:#6B6B6B;font-family:monospace;">{_code}</div>'
                        f'<div style="font-size:0.68rem;">{_period}</div>'
                        f'<div style="font-size:0.62rem;color:#6B6B6B;">{_sector}</div>'
                        f'{_mcap_cell}'
                        f'{_perf_cell}'
                        f'</div>'
                    )

                st.markdown(rows_html, unsafe_allow_html=True)

                # Watchlist financial snapshots
                _wl_here = [e for e in _sorted_entries if e.get("code") in _wl_codes]
                if _wl_here and get_jquants_secret():
                    _jq_key2 = get_jquants_secret()
                    with st.expander(f"📊 Financial snapshots for {len(_wl_here)} watchlist company/ies on {_date_display}", expanded=False):
                        for _we in _wl_here:
                            _wcode = _we.get("code", "")
                            _wname = _we.get("name", "")
                            st.markdown(f'<div style="font-size:0.82rem;font-weight:700;color:#8B4513;margin:0.4rem 0 0.2rem;">📋 {_wname} ({_wcode})</div>', unsafe_allow_html=True)
                            _fs_key = f"fs_{_wcode}"
                            if _fs_key not in st.session_state.fin_summary_cache:
                                with st.spinner(f"Fetching financials for {_wname}…"):
                                    _fs_raw  = fetch_financial_summary(_jq_key2, code=_wcode)
                                    _fs_data = format_summary_for_display(_fs_raw)
                                    st.session_state.fin_summary_cache[_fs_key] = _fs_data
                            _fs = st.session_state.fin_summary_cache.get(_fs_key, [])
                            if not _fs:
                                st.markdown('<div style="color:#9B8B7A;font-size:0.75rem;">No financial data (12-week delay on J-Quants free plan — data only for past quarters).</div>', unsafe_allow_html=True)
                            else:
                                _mini = (
                                    '<div style="display:grid;grid-template-columns:0.8fr 1fr 1fr 1fr 1fr 1fr;'
                                    'gap:0.15rem;padding:0.2rem 0.3rem;background:#F0EDE8;'
                                    'font-size:0.57rem;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;">'
                                    '<div>Period</div><div>Revenue</div><div>Op Profit</div>'
                                    '<div>Net Profit</div><div>EPS</div><div>FY Guidance OP</div></div>'
                                )
                                st.markdown(_mini, unsafe_allow_html=True)
                                for _qi, _q in enumerate(_fs[:4]):
                                    _qbg  = "#FAFAF8" if _qi % 2 == 0 else "#F7F4EF"
                                    _per  = f"{_q.get('CurPerType','')} {_q.get('CurFYEn','')[:4]}"
                                    _eps  = f"¥{float(_q['EPS']):.1f}" if _q.get("EPS") and _q["EPS"] != "" else "—"
                                    st.markdown(
                                        f'<div style="display:grid;grid-template-columns:0.8fr 1fr 1fr 1fr 1fr 1fr;'
                                        f'gap:0.15rem;padding:0.2rem 0.3rem;background:{_qbg};'
                                        f'font-size:0.67rem;border-bottom:1px solid #EDE8E0;">'
                                        f'<div style="font-weight:600;">{_per}</div>'
                                        f'<div>{safe_num(_q.get("Sales"))}</div>'
                                        f'<div>{safe_num(_q.get("OP"))}</div>'
                                        f'<div>{safe_num(_q.get("NP"))}</div>'
                                        f'<div style="font-family:monospace;">{_eps}</div>'
                                        f'<div>{safe_num(_q.get("FOP"))}</div>'
                                        f'</div>',
                                        unsafe_allow_html=True,
                                    )
                                if len(_fs) >= 2:
                                    _gdir = guidance_direction(_fs[0].get("FOP",""), _fs[1].get("FOP",""))
                                    if _gdir:
                                        _gc = "#2E7D32" if "raised" in _gdir else ("#C62828" if "cut" in _gdir else "#6B6B6B")
                                        st.markdown(f'<div style="font-size:0.7rem;color:{_gc};font-weight:700;margin-top:0.2rem;">FY Operating Profit guidance: {_gdir} vs prior quarter</div>', unsafe_allow_html=True)

            # TBD entries
            if _no_date:
                st.markdown(
                    f'<div style="margin-top:1rem;font-size:0.78rem;color:#9B8B7A;">'
                    f'{len(_no_date)} companies have not yet confirmed their announcement date.</div>',
                    unsafe_allow_html=True,
                )

with tab_subscribe:
    st.markdown('<div class="section-title">📬 Email Digest</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="info-box">
    Subscribe to receive the <strong>Japan Investment Digest</strong> by email — automatically sent twice daily:<br><br>
    <strong>🌅 Pre-market edition</strong> &nbsp;·&nbsp; 07:00 JST (2 hours before TSE open at 09:00)&nbsp; — <em>6:00 MYT</em><br>
    &nbsp;&nbsp;&nbsp;&nbsp;Market recap, overnight news, what to watch at open<br><br>
    <strong>🌆 Close-of-day edition</strong> &nbsp;·&nbsp; 19:00 JST (4 hours after TSE close at 15:30)&nbsp; — <em>18:00 MYT</em><br>
    &nbsp;&nbsp;&nbsp;&nbsp;Full day summary · AI briefing · corporate filings · sector moves · market data
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='height:0.6rem'></div>", unsafe_allow_html=True)

    # ── Scheduler setup instructions ──────────────────────────────────────────
    with st.expander("⚙️ Set up automatic digest delivery", expanded=False):
        app_url = st.text_input(
            "Your Streamlit app URL:",
            placeholder="https://your-app.streamlit.app",
            key="app_url_input",
        )
        token = get_secret("DIGEST_WEBHOOK_TOKEN") or "(not set)"
        tok_display = token if token == "(not set)" else token[:6] + "…"
        st.markdown(f"""
**How it works:** The app has a built-in webhook that sends the digest when pinged with a URL parameter.
Add `DIGEST_WEBHOOK_TOKEN = "your-secret"` to Streamlit Secrets to protect it.

**Step 1 — Add to Streamlit Secrets:**
```
DIGEST_WEBHOOK_TOKEN = "choose-a-secret-token"
SENDGRID_API_KEY = "your-sendgrid-key"
DIGEST_FROM_EMAIL = "digest@yourdomain.com"
```

**Step 2 — Set up cron-job.org (free):**

Go to [cron-job.org](https://cron-job.org) → New cronjob:

| Edition | URL | Schedule |
|---|---|---|
| Pre-market (07:00 JST) | `{app_url or 'https://your-app.streamlit.app'}/?digest=premarket&token=your-secret` | `0 22 * * 0-4` (UTC Sun–Thu) |
| Close-of-day (19:00 JST) | `{app_url or 'https://your-app.streamlit.app'}/?digest=close&token=your-secret` | `0 10 * * 1-5` (UTC Mon–Fri) |

Current webhook token: `{tok_display}`
""", unsafe_allow_html=False)

    st.markdown("<div style='height:0.4rem'></div>", unsafe_allow_html=True)
    col_tab1, col_tab2 = st.columns(2)
    with col_tab1:
        st.markdown('<div class="section-subtitle">Subscribe</div>', unsafe_allow_html=True)
        with st.form("subscribe_form"):
            email_input = st.text_input("Email address:", placeholder="your@email.com", key="sub_email")
            col_sub1, col_sub2 = st.columns(2)
            with col_sub1:
                want_premarket = st.checkbox("Pre-market (07:00 JST)", value=True)
            with col_sub2:
                want_close     = st.checkbox("Close-of-day (19:00 JST)", value=True)
            submitted = st.form_submit_button("Subscribe", use_container_width=True)
            if submitted:
                if email_input and "@" in email_input:
                    if subscribe_email(email_input):
                        editions = []
                        if want_premarket: editions.append("pre-market")
                        if want_close:     editions.append("close-of-day")
                        edition_str = " & ".join(editions) if editions else "selected"
                        st.success(f"Subscribed to {edition_str} digest.")
                    else:
                        st.error("Subscription failed.")
                else:
                    st.error("Enter a valid email address.")

    with col_tab2:
        st.markdown('<div class="section-subtitle">Unsubscribe</div>', unsafe_allow_html=True)
        with st.form("unsubscribe_form"):
            unsub_email = st.text_input("Email address:", placeholder="your@email.com", key="unsub_email")
            unsub_submitted = st.form_submit_button("Unsubscribe", use_container_width=True)
            if unsub_submitted:
                if unsub_email and "@" in unsub_email:
                    from emailer import unsubscribe_email
                    if unsubscribe_email(unsub_email):
                        st.success(f"Removed from all digest lists.")
                    else:
                        st.error("Could not unsubscribe.")
                else:
                    st.error("Enter a valid email address.")

    st.markdown("<hr style='border-color:#D9D3C8;margin:1rem 0'>", unsafe_allow_html=True)

    st.markdown("""
    <div style="font-size:0.72rem;color:#9B8B7A;line-height:1.7;">
    <strong>What's in each edition:</strong><br>
    📊 Nikkei 225, TOPIX, FX rates with daily change<br>
    📰 AI-generated news briefing with article links<br>
    📋 Corporate filings summary (TDnet) with PDF links<br>
    ⭐ Watchlist alerts — mentions of companies you track<br>
    🏭 Sector-by-sector headlines across MSCI sectors
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="font-size:0.72rem;background:#FFF8F0;border:1px solid #F0C080;border-radius:3px;padding:0.6rem 0.8rem;margin-top:0.6rem;line-height:1.7;">
    <strong>⚠️ Important — to receive emails you need to:</strong><br>
    1. Add <code>SENDGRID_API_KEY</code> and <code>DIGEST_FROM_EMAIL</code> to Streamlit Secrets (for sending)<br>
    2. Add <code>SUBSCRIBER_EMAILS = "your@email.com"</code> to Streamlit Secrets — this is the <strong>persistent</strong> subscriber list.<br>
    &nbsp;&nbsp;&nbsp;The Subscribe form above saves to a temporary file that is <strong>erased on every app restart</strong> (Streamlit Cloud limitation).<br>
    &nbsp;&nbsp;&nbsp;Emails in <code>SUBSCRIBER_EMAILS</code> are always loaded regardless of restarts.<br>
    3. A scheduled trigger (e.g. GitHub Actions cron job) is needed to call the send function at 07:00 and 19:00 JST.
    </div>
    """, unsafe_allow_html=True)

    # ── Manual send (for admin use) ──
    if get_secret("SENDGRID_API_KEY"):
        st.markdown("<hr style='border-color:#D9D3C8;margin:0.8rem 0'>", unsafe_allow_html=True)
        st.markdown('<div style="font-size:0.72rem;font-weight:700;color:#8B4513;margin-bottom:0.4rem;">ADMIN: Send Digest Now</div>', unsafe_allow_html=True)
        manual_email = st.text_input("Send to email:", placeholder="your@email.com", key="manual_email")
        if st.button("Send Digest Now", use_container_width=False):
            if manual_email and "@" in manual_email:
                with st.spinner("Sending…"):
                    send_digest(st.session_state.articles, [manual_email])
                st.success(f"✓ Sent to {manual_email}")
            else:
                st.error("Enter a valid email address.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;margin-top:2rem;padding-top:0.7rem;
            border-top:1px solid #D9D3C8;font-size:0.66rem;color:#9B8B7A;letter-spacing:0.07em;">
    JAPAN INVESTMENT DIGEST<br>
    Market data via Stooq / Alpha Vantage · News via RSS · TDnet filings via Yanoshin · For informational purposes only · Not financial advice.
</div>
""", unsafe_allow_html=True)
