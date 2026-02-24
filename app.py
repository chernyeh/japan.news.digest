import streamlit as st
from datetime import datetime
import pytz
from collector import fetch_all_news
from emailer import subscribe_email, send_digest
from market_data import fetch_market_overview, fetch_tse_movers, fetch_foreign_flow
from watchlist import (load_watchlist, add_to_watchlist, remove_from_watchlist,
                       scan_all_watchlist, KNOWN_COMPANIES)
from sentiment import score_all_sectors, flag_high_value_articles

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
    ("Japan Industry News", "https://japanindustrynews.com/",             "🏭"),
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
.article-source {
    font-size: 0.63rem; font-weight: 700; letter-spacing: 0.13em;
    text-transform: uppercase; color: #8B4513; margin-bottom: 0.15rem;
}
.article-title a {
    font-size: 0.91rem; font-weight: 600; color: #1A1A1A;
    text-decoration: none; line-height: 1.4;
}
.article-title a:hover { color: #8B4513; text-decoration: underline; }
.article-title-jp {
    font-family: 'Noto Sans JP', sans-serif; font-size: 0.7rem;
    color: #9B8B7A; margin-top: 0.1rem; font-weight: 300;
}
.article-meta { font-size: 0.65rem; color: #9B8B7A; margin-top: 0.12rem; }
.high-value-tag {
    display: inline-block; background: #8B4513; color: white;
    font-size: 0.57rem; font-weight: 700; letter-spacing: 0.07em;
    text-transform: uppercase; padding: 0.08rem 0.35rem;
    border-radius: 2px; margin-left: 0.35rem; vertical-align: middle;
}
.sector-header {
    font-family: 'Playfair Display', serif; font-size: 1.3rem; font-weight: 700;
    color: #1A1A1A; border-bottom: 2px solid #1A1A1A;
    padding-bottom: 0.3rem; margin-bottom: 0.15rem;
}
.sector-count {
    font-size: 0.66rem; font-weight: 600; letter-spacing: 0.11em;
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

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    background: transparent; gap: 0.25rem; border-bottom: 2px solid #1A1A1A;
}
.stTabs [data-baseweb="tab"] {
    background: #EDE8E0; border-radius: 3px 3px 0 0;
    font-size: 0.75rem; font-weight: 600; padding: 0.35rem 0.75rem;
    color: #6B6B6B; border: none;
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

/* Mobile responsive */
@media (max-width: 600px) {
    .masthead-title { font-size: 1.6rem; }
    .ticker-strip { gap: 0.7rem; padding: 0.4rem 0.6rem; }
    .ticker-price { font-size: 0.78rem; }
    .media-grid { grid-template-columns: repeat(auto-fill, minmax(120px, 1fr)); }
    .stTabs [data-baseweb="tab"] { font-size: 0.68rem; padding: 0.3rem 0.5rem; }
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
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Masthead ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="masthead">
    <div class="masthead-title">Japan Investment Digest</div>
    <div class="masthead-sub">日本経済・市場情報 — MSCI Sector Edition · TSE Investment Intelligence</div>
    <div class="masthead-date">{now_local().strftime('%A, %d %B %Y · %H:%M MYT')}</div>
</div>
<div class="dateline-strip">Petaling Jaya · Live Market Data · RSS News · TSE Intelligence · Foreign Flow Tracker</div>
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
    price_str = f"{price:,.0f}" if price > 100 else f"{price:,.3f}"
    return (
        '<div class="ticker-item">'
        '<div class="ticker-label">' + label + '</div>'
        '<div class="ticker-price">' + price_str + '</div>'
        '<div class="' + chg_class + '">' + arrow + ' ' + f"{abs(pct):.2f}%" + '</div>'
        + ('<div class="ticker-state">' + state + '</div>' if state else '') +
        '</div>'
    )

if st.session_state.market_data and not st.session_state.market_data.get("error"):
    md = st.session_state.market_data
    ticker_html = '<div class="ticker-strip">'
    ticker_html += render_ticker("Nikkei 225", md.get("nikkei"))
    ticker_html += '<div class="ticker-divider"></div>'
    ticker_html += render_ticker("TOPIX", md.get("topix"))
    ticker_html += '<div class="ticker-divider"></div>'
    ticker_html += render_ticker("USD/JPY", md.get("usdjpy"))
    ticker_html += '<div class="ticker-divider"></div>'
    ticker_html += render_ticker("MYR/JPY", md.get("myrjpy"))
    ticker_html += '<div class="ticker-divider"></div>'
    ticker_html += render_ticker("EUR/JPY", md.get("eurjpy"))
    last_mkt = st.session_state.last_market_fetch
    updated_str = format_local_dt(last_mkt) if last_mkt else "—"
    ticker_html += '<div style="margin-left:auto;font-size:0.55rem;color:#555;align-self:center;">Updated<br>' + updated_str + '</div>'
    ticker_html += '</div>'
    st.markdown(ticker_html, unsafe_allow_html=True)

# ── Toolbar ───────────────────────────────────────────────────────────────────
col_info, col_spacer, col_mkt, col_news = st.columns([3, 1, 1, 1])
with col_info:
    if st.session_state.last_fetch:
        total = sum(len(v) for v in st.session_state.articles.values())
        st.markdown(
            '<div style="font-size:0.72rem;color:#9B8B7A;padding-top:0.35rem;">News: '
            + format_local_dt(st.session_state.last_fetch)
            + ' · ' + str(total) + ' articles</div>',
            unsafe_allow_html=True
        )
with col_mkt:
    if st.button("📈 Markets", use_container_width=True):
        with st.spinner("Fetching..."):
            st.session_state.market_data = fetch_market_overview()
            st.session_state.movers = fetch_tse_movers()
            st.session_state.foreign_flow = fetch_foreign_flow()
            st.session_state.last_market_fetch = now_local()
        st.rerun()
with col_news:
    if st.button("🔄 News", use_container_width=True):
        with st.spinner("Fetching & translating..."):
            st.session_state.articles = fetch_all_news()
            st.session_state.last_fetch = now_local()
            st.session_state.sentiment_scores = score_all_sectors(st.session_state.articles)
            wl = load_watchlist()
            st.session_state.watchlist_hits = scan_all_watchlist(wl, st.session_state.articles)
            if not st.session_state.selected_sector:
                for name, _ in MSCI_SECTORS:
                    if st.session_state.articles.get(name):
                        st.session_state.selected_sector = name
                        break
            if not st.session_state.market_data:
                st.session_state.market_data = fetch_market_overview()
                st.session_state.movers = fetch_tse_movers()
                st.session_state.foreign_flow = fetch_foreign_flow()
                st.session_state.last_market_fetch = now_local()
        st.rerun()

st.markdown("<div style='margin-bottom:0.4rem'></div>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_news, tab_market, tab_watchlist, tab_sentiment, tab_sources, tab_subscribe = st.tabs([
    "📰 News", "📊 Markets", "⭐ Watchlist", "🌡️ Sentiment", "🔗 Sources", "📬 Subscribe",
])

# ════════════════════════════════════════════════════════════
# TAB 1 — NEWS
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

        selected_label = st.selectbox(
            "Sector:", options=sector_labels, index=current_index,
            label_visibility="collapsed", key="sector_selector"
        )
        # Update immediately on change — no double-click needed
        new_sector = sector_names[sector_labels.index(selected_label)]
        if new_sector != st.session_state.selected_sector:
            st.session_state.selected_sector = new_sector
            st.rerun()

        sector_name = st.session_state.selected_sector
        raw_articles = st.session_state.articles.get(sector_name, [])
        articles = flag_high_value_articles(raw_articles)
        icon = next((i for n, i in MSCI_SECTORS if n == sector_name), "📰")

        sent = st.session_state.sentiment_scores.get(sector_name, {})
        sent_label = sent.get("label", "")
        sent_color = sent.get("color", "#6B6B6B")
        badge_class = "badge-pos" if sent_label == "Positive" else ("badge-neg" if sent_label == "Negative" else "badge-neu")
        sentiment_html = (' &nbsp;<span class="sentiment-badge ' + badge_class + '">' + sent.get("icon","") + ' ' + sent_label + '</span>') if sent_label else ""

        count_label = str(len(articles)) + " article" + ("s" if len(articles) != 1 else "")
        cards = []
        for article in articles:
            orig   = article.get("original_title", "")
            trans  = article.get("translated_title", article.get("title", ""))
            source = article.get("source", "")
            url    = article.get("url", "#")
            date   = article.get("pub_date", "")
            hv     = article.get("high_value", False)

            hv_tag    = '<span class="high-value-tag">★ Corp Action</span>' if hv else ""
            orig_part = '<div class="article-title-jp">' + orig + '</div>' if orig and orig != trans else ""
            date_part = '<div class="article-meta">' + date + '</div>' if date else ""

            cards.append(
                '<div class="article-card">'
                '<div class="article-source">' + source + '</div>'
                '<div class="article-title"><a href="' + url + '" target="_blank">' + trans + '</a>' + hv_tag + '</div>'
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
    if not st.session_state.market_data:
        st.markdown('<div class="empty-state">Click <strong>📈 Markets</strong> above to load live data.</div>', unsafe_allow_html=True)
    elif st.session_state.market_data.get("error") == "no_key":
        st.warning("⚠️ No Finnhub API key found. Add **FINNHUB_API_KEY** to your Streamlit Secrets. Sign up free at https://finnhub.io — no credit card needed.")
    else:
        md = st.session_state.market_data

        def metric_box(label, data):
            if not data or data.get("price", 0) == 0:
                return '<div class="flow-box"><div class="ticker-label">' + label + '</div><div style="font-size:1.1rem;color:#9B8B7A;">Unavailable</div></div>'
            price = data["price"]
            pct   = data.get("pct_change", 0)
            chg   = data.get("change", 0)
            state = data.get("state_label", "")
            price_str = f"{price:,.0f}" if price > 100 else f"{price:,.3f}"
            chg_str   = (f"{chg:+,.0f}" if price > 100 else f"{chg:+,.3f}")
            val_class = "flow-value-up" if pct >= 0 else "flow-value-dn"
            arrow = "▲" if pct >= 0 else "▼"
            state_html = '<div style="font-size:0.63rem;color:#9B8B7A;margin-top:0.1rem;">' + state + '</div>' if state else ""
            return (
                '<div class="flow-box">'
                '<div class="ticker-label">' + label + '</div>'
                '<div class="' + val_class + '">' + price_str + '</div>'
                '<div class="flow-label">' + arrow + ' ' + f"{abs(pct):.2f}%" + ' (' + chg_str + ')</div>'
                + state_html + '</div>'
            )

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-title">📈 Indices</div>', unsafe_allow_html=True)
            st.markdown(metric_box("Nikkei 225", md.get("nikkei")), unsafe_allow_html=True)
            st.markdown(metric_box("TOPIX", md.get("topix")), unsafe_allow_html=True)
        with col2:
            st.markdown('<div class="section-title">💱 Currencies</div>', unsafe_allow_html=True)
            st.markdown(metric_box("USD / JPY", md.get("usdjpy")), unsafe_allow_html=True)
            st.markdown(metric_box("MYR / JPY", md.get("myrjpy")), unsafe_allow_html=True)
            st.markdown(metric_box("EUR / JPY", md.get("eurjpy")), unsafe_allow_html=True)

        st.markdown("<hr style='border-color:#D9D3C8;margin:0.8rem 0'>", unsafe_allow_html=True)

        col3, col4 = st.columns(2)
        movers = st.session_state.movers or {}
        with col3:
            st.markdown('<div class="section-title">🚀 Top Gainers</div>', unsafe_allow_html=True)
            gainers = movers.get("gainers", [])
            if gainers:
                html = ""
                for m in gainers:
                    html += (
                        '<div class="mover-card up">'
                        '<div><div class="mover-name">' + m["name"] + '</div>'
                        '<div class="mover-sym">' + m["symbol"] + ' · ¥' + f'{m["price"]:,.0f}' + '</div></div>'
                        '<div class="mover-pct-up">▲ ' + f'{m["pct_change"]:.2f}%' + '</div>'
                        '</div>'
                    )
                st.markdown(html, unsafe_allow_html=True)
            else:
                st.markdown('<div class="info-box">No data — fetch markets first.</div>', unsafe_allow_html=True)
        with col4:
            st.markdown('<div class="section-title">📉 Top Losers</div>', unsafe_allow_html=True)
            losers = movers.get("losers", [])
            if losers:
                html = ""
                for m in losers:
                    html += (
                        '<div class="mover-card dn">'
                        '<div><div class="mover-name">' + m["name"] + '</div>'
                        '<div class="mover-sym">' + m["symbol"] + ' · ¥' + f'{m["price"]:,.0f}' + '</div></div>'
                        '<div class="mover-pct-dn">▼ ' + f'{abs(m["pct_change"]):.2f}%' + '</div>'
                        '</div>'
                    )
                st.markdown(html, unsafe_allow_html=True)
            else:
                st.markdown('<div class="info-box">No data — fetch markets first.</div>', unsafe_allow_html=True)

        st.markdown("<hr style='border-color:#D9D3C8;margin:0.8rem 0'>", unsafe_allow_html=True)

        st.markdown('<div class="section-title">🌍 Foreign Investor Flow</div>', unsafe_allow_html=True)
        flow = st.session_state.foreign_flow
        if flow and flow.get("available"):
            net = flow["net_billion_yen"]
            val_class = "flow-value-up" if net > 0 else "flow-value-dn"
            arrow = "▲" if net > 0 else "▼"
            st.markdown(
                '<div class="flow-box">'
                '<div class="ticker-label">Weekly Net Flow — Foreign Investors (TSE)</div>'
                '<div class="' + val_class + '">' + arrow + ' ¥' + f'{abs(net):.1f}B' + '</div>'
                '<div class="flow-label">' + flow.get("direction","") + ' · ' + flow.get("as_of","") + '</div>'
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
                html += (
                    '<div class="watchlist-hit">'
                    '<div style="font-size:0.62rem;font-weight:700;color:#F9A825;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:0.15rem;">' + source + '</div>'
                    '<div><a href="' + url + '" target="_blank" style="font-size:0.88rem;font-weight:600;color:#1A1A1A;text-decoration:none;">' + title + '</a></div>'
                    + date_p + '</div>'
                )
            st.markdown(html, unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
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
        grid += '<a href="' + url + '" target="_blank" class="media-card"><span class="media-icon">' + icon + '</span><span class="media-name">' + name + '</span></a>'
    grid += '</div>'
    st.markdown(grid, unsafe_allow_html=True)

    st.markdown('<div style="font-size:0.72rem;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#8B4513;margin:1rem 0 0.4rem 0;">Specialist Trade Papers</div>', unsafe_allow_html=True)
    trade = [s for s in MEDIA_SOURCES if s[2] not in ["🗞️","📊","📡","📺","🔎","📈","💎"]]
    grid2 = '<div class="media-grid">'
    for name, url, icon in trade:
        grid2 += '<a href="' + url + '" target="_blank" class="media-card"><span class="media-icon">' + icon + '</span><span class="media-name">' + name + '</span></a>'
    grid2 += '</div>'
    st.markdown(grid2, unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# TAB 6 — SUBSCRIBE
# ════════════════════════════════════════════════════════════
with tab_subscribe:
    st.markdown('<div class="section-title">📬 Email Digest</div>', unsafe_allow_html=True)
    st.markdown('<div class="info-box">Subscribe to receive the daily Japan investment digest by email — market data, movers, watchlist alerts, and sector news.</div>', unsafe_allow_html=True)

    with st.form("subscribe_form"):
        email_input = st.text_input("Email address:", placeholder="your@email.com")
        if st.form_submit_button("Subscribe", use_container_width=True):
            if email_input and "@" in email_input:
                if subscribe_email(email_input):
                    st.success(f"✓ {email_input} subscribed.")
                else:
                    st.error("Subscription failed. Try again.")
            else:
                st.error("Enter a valid email address.")

    if st.session_state.articles:
        st.markdown("<hr style='border-color:#D9D3C8;margin:0.8rem 0'>", unsafe_allow_html=True)
        st.markdown('<div class="section-title">⚙️ Send Test Digest</div>', unsafe_allow_html=True)
        test_email = st.text_input("Send to:", placeholder="your@email.com", key="test_email")
        if st.button("Send Test Digest", use_container_width=True):
            if test_email:
                with st.spinner("Sending..."):
                    send_digest(st.session_state.articles, [test_email])
                st.success(f"✓ Sent to {test_email}")
            else:
                st.error("Enter an email address first.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;margin-top:2rem;padding-top:0.7rem;
            border-top:1px solid #D9D3C8;font-size:0.66rem;color:#9B8B7A;letter-spacing:0.07em;">
    JAPAN INVESTMENT DIGEST · MSCI SECTOR EDITION<br>
    Market data via Finnhub · News via RSS · For informational purposes only · Not financial advice.
</div>
""", unsafe_allow_html=True)
