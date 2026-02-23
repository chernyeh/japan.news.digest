import streamlit as st
from datetime import datetime
from collector import fetch_all_news
from emailer import subscribe_email, send_digest
from market_data import fetch_market_overview, fetch_tse_movers, fetch_foreign_flow, format_number, format_change
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
.block-container { padding-top: 1.2rem; padding-bottom: 3rem; max-width: 1100px; }

/* Masthead */
.masthead {
    border-top: 4px solid #1A1A1A;
    border-bottom: 1px solid #1A1A1A;
    padding: 1rem 0 0.7rem 0;
    margin-bottom: 0.3rem;
    text-align: center;
}
.masthead-title {
    font-family: 'Playfair Display', serif;
    font-size: 2.4rem;
    font-weight: 900;
    letter-spacing: -0.02em;
    line-height: 1;
    color: #1A1A1A;
}
.masthead-sub {
    font-size: 0.7rem;
    font-weight: 600;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: #6B6B6B;
    margin-top: 0.3rem;
}
.masthead-date { font-size: 0.75rem; color: #6B6B6B; margin-top: 0.15rem; }
.dateline-strip {
    background: #1A1A1A; color: #F7F4EF; text-align: center;
    padding: 0.28rem; font-size: 0.66rem; letter-spacing: 0.15em;
    text-transform: uppercase; font-weight: 600; margin-bottom: 1rem;
}

/* ── Market ticker strip ── */
.ticker-strip {
    background: #1A1A1A;
    border-radius: 4px;
    padding: 0.6rem 1rem;
    margin-bottom: 1rem;
    display: flex;
    gap: 1.5rem;
    flex-wrap: wrap;
    align-items: center;
}
.ticker-item { display: flex; flex-direction: column; min-width: 80px; }
.ticker-label {
    font-size: 0.6rem; font-weight: 700; letter-spacing: 0.12em;
    text-transform: uppercase; color: #888; margin-bottom: 1px;
}
.ticker-price { font-size: 0.88rem; font-weight: 700; color: #F7F4EF; }
.ticker-change-up { font-size: 0.72rem; color: #66BB6A; font-weight: 600; }
.ticker-change-dn { font-size: 0.72rem; color: #EF5350; font-weight: 600; }
.ticker-divider {
    width: 1px; background: #333; align-self: stretch; margin: 0 0.2rem;
}

/* ── Section headers ── */
.section-title {
    font-family: 'Playfair Display', serif;
    font-size: 1.15rem; font-weight: 700; color: #1A1A1A;
    border-bottom: 2px solid #1A1A1A;
    padding-bottom: 0.3rem; margin-bottom: 0.8rem;
    letter-spacing: -0.01em;
}
.section-subtitle {
    font-size: 0.68rem; font-weight: 600; letter-spacing: 0.12em;
    text-transform: uppercase; color: #9B8B7A; margin-bottom: 0.8rem;
}

/* ── Movers cards ── */
.mover-card {
    background: white; border-radius: 3px; padding: 0.5rem 0.7rem;
    margin-bottom: 0.3rem; border-left: 3px solid transparent;
    display: flex; justify-content: space-between; align-items: center;
}
.mover-card.up { border-left-color: #2E7D32; }
.mover-card.dn { border-left-color: #C62828; }
.mover-name { font-size: 0.8rem; font-weight: 600; color: #1A1A1A; }
.mover-sym  { font-size: 0.65rem; color: #9B8B7A; }
.mover-pct-up { font-size: 0.85rem; font-weight: 700; color: #2E7D32; }
.mover-pct-dn { font-size: 0.85rem; font-weight: 700; color: #C62828; }

/* ── Foreign flow box ── */
.flow-box {
    background: white; border-radius: 3px; padding: 0.7rem 1rem;
    margin-bottom: 0.5rem; border: 1px solid #E8E3DC;
}
.flow-value-up { font-size: 1.4rem; font-weight: 700; color: #2E7D32; }
.flow-value-dn { font-size: 1.4rem; font-weight: 700; color: #C62828; }
.flow-label { font-size: 0.68rem; color: #9B8B7A; margin-top: 0.1rem; }

/* ── Sentiment badges ── */
.sentiment-row {
    display: flex; align-items: center; justify-content: space-between;
    padding: 0.35rem 0; border-bottom: 1px solid #EDE8E0;
    font-size: 0.8rem;
}
.sentiment-sector { font-weight: 600; color: #1A1A1A; }
.sentiment-badge {
    font-size: 0.65rem; font-weight: 700; padding: 0.15rem 0.5rem;
    border-radius: 10px; letter-spacing: 0.05em; text-transform: uppercase;
}
.badge-pos { background: #E8F5E9; color: #2E7D32; }
.badge-neg { background: #FFEBEE; color: #C62828; }
.badge-neu { background: #F5F5F5; color: #6B6B6B; }

/* ── Watchlist ── */
.watchlist-hit {
    background: #FFF8E1; border-left: 3px solid #F9A825;
    border-radius: 2px; padding: 0.6rem 0.8rem; margin-bottom: 0.4rem;
}
.watchlist-company { font-size: 0.65rem; font-weight: 700; color: #F9A825;
    letter-spacing: 0.1em; text-transform: uppercase; margin-bottom: 0.2rem; }

/* ── Article cards ── */
.article-card { border-bottom: 1px solid #D9D3C8; padding: 0.75rem 0; }
.article-source {
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.14em;
    text-transform: uppercase; color: #8B4513; margin-bottom: 0.15rem;
}
.article-title a {
    font-size: 0.93rem; font-weight: 600; color: #1A1A1A;
    text-decoration: none; line-height: 1.4;
}
.article-title a:hover { color: #8B4513; text-decoration: underline; }
.article-title-jp {
    font-family: 'Noto Sans JP', sans-serif; font-size: 0.72rem;
    color: #9B8B7A; margin-top: 0.1rem; font-weight: 300;
}
.article-meta { font-size: 0.67rem; color: #9B8B7A; margin-top: 0.15rem; }
.high-value-tag {
    display: inline-block; background: #8B4513; color: white;
    font-size: 0.58rem; font-weight: 700; letter-spacing: 0.08em;
    text-transform: uppercase; padding: 0.1rem 0.4rem; border-radius: 2px;
    margin-left: 0.4rem; vertical-align: middle;
}

/* ── Sector count ── */
.sector-header {
    font-family: 'Playfair Display', serif; font-size: 1.35rem;
    font-weight: 700; color: #1A1A1A; border-bottom: 2px solid #1A1A1A;
    padding-bottom: 0.3rem; margin-bottom: 0.15rem;
}
.sector-count {
    font-size: 0.68rem; font-weight: 600; letter-spacing: 0.12em;
    text-transform: uppercase; color: #9B8B7A; margin-bottom: 0.8rem;
}

/* ── Tab nav ── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent; gap: 0.3rem; border-bottom: 2px solid #1A1A1A;
}
.stTabs [data-baseweb="tab"] {
    background: #EDE8E0; border-radius: 3px 3px 0 0;
    font-size: 0.78rem; font-weight: 600; padding: 0.4rem 0.9rem;
    color: #6B6B6B; border: none;
}
.stTabs [aria-selected="true"] {
    background: #1A1A1A !important; color: #F7F4EF !important;
}

/* ── Info boxes ── */
.info-box {
    background: #EDE8E0; border-radius: 3px; padding: 0.6rem 0.9rem;
    font-size: 0.78rem; color: #6B6B6B; margin-bottom: 0.5rem;
}

/* ── Subscribe ── */
.subscribe-strip {
    background: #1A1A1A; color: #F7F4EF; padding: 1.2rem 1.5rem;
    border-radius: 4px; margin-top: 2rem;
}
.subscribe-title {
    font-family: 'Playfair Display', serif; font-size: 1rem;
    font-weight: 700; color: #F7F4EF; margin-bottom: 0.3rem;
}
.subscribe-sub { font-size: 0.78rem; color: #A09890; margin-bottom: 0.8rem; }

/* ── Empty state ── */
.empty-state {
    font-family: 'Playfair Display', serif; font-size: 1rem; color: #6B6B6B;
    text-align: center; padding: 2.5rem 1rem; line-height: 1.8;
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
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Masthead ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="masthead">
    <div class="masthead-title">Japan Investment Digest</div>
    <div class="masthead-sub">日本経済・市場情報 — MSCI Sector Edition · TSE Investment Intelligence</div>
    <div class="masthead-date">{datetime.now().strftime('%A, %B %d, %Y')}</div>
</div>
<div class="dateline-strip">Petaling Jaya · Live Market Data · RSS News · TSE Intelligence · Foreign Flow Tracker</div>
""", unsafe_allow_html=True)

# ── Market ticker strip ───────────────────────────────────────────────────────
def render_ticker(label, data, is_index=True):
    if not data or data.get("price", 0) == 0:
        return '<div class="ticker-item"><div class="ticker-label">' + label + '</div><div class="ticker-price">—</div></div>'
    price = data["price"]
    pct = data.get("pct_change", 0)
    state_label = data.get("state_label", "")
    chg_class = "ticker-change-up" if pct >= 0 else "ticker-change-dn"
    arrow = "▲" if pct >= 0 else "▼"
    price_str = f"{price:,.0f}" if is_index else f"{price:,.3f}"
    state_html = '<div style="font-size:0.55rem;color:#555;margin-top:1px;">' + state_label + '</div>' if state_label else ""
    return (
        '<div class="ticker-item">'
        '<div class="ticker-label">' + label + '</div>'
        '<div class="ticker-price">' + price_str + '</div>'
        '<div class="' + chg_class + '">' + arrow + ' ' + f"{abs(pct):.2f}%" + '</div>'
        + state_html +
        '</div>'
    )

if st.session_state.market_data:
    md = st.session_state.market_data
    ticker_html = '<div class="ticker-strip">'
    ticker_html += render_ticker("Nikkei 225", md.get("nikkei"), True)
    ticker_html += '<div class="ticker-divider"></div>'
    ticker_html += render_ticker("TOPIX", md.get("topix"), True)
    ticker_html += '<div class="ticker-divider"></div>'
    ticker_html += render_ticker("USD/JPY", md.get("usdjpy"), False)
    ticker_html += '<div class="ticker-divider"></div>'
    ticker_html += render_ticker("MYR/JPY", md.get("myrjpy"), False)
    ticker_html += '<div class="ticker-divider"></div>'
    ticker_html += render_ticker("EUR/JPY", md.get("eurjpy"), False)
    ticker_html += '<div class="ticker-divider"></div>'
    ticker_html += render_ticker("JGB 10Y", md.get("jgb10y"), False)
    ticker_html += f'<div style="margin-left:auto;font-size:0.6rem;color:#555;">Updated {st.session_state.last_fetch.strftime("%H:%M") if st.session_state.last_fetch else "—"}</div>'
    ticker_html += '</div>'
    st.markdown(ticker_html, unsafe_allow_html=True)

# ── Fetch controls ────────────────────────────────────────────────────────────
col_info, col_mkt, col_news = st.columns([3, 1, 1])
with col_info:
    if st.session_state.last_fetch:
        total = sum(len(v) for v in st.session_state.articles.values())
        st.markdown(
            f'<div style="font-size:0.75rem;color:#9B8B7A;">Last updated: '
            f'{st.session_state.last_fetch.strftime("%H:%M")} · {total} articles loaded</div>',
            unsafe_allow_html=True
        )
with col_mkt:
    if st.button("📈 Refresh Markets", use_container_width=True):
        with st.spinner("Fetching market data..."):
            st.session_state.market_data = fetch_market_overview()
            st.session_state.movers = fetch_tse_movers()
            st.session_state.foreign_flow = fetch_foreign_flow()
        st.rerun()
with col_news:
    if st.button("🔄 Fetch News", use_container_width=True):
        with st.spinner("Fetching & translating headlines..."):
            st.session_state.articles = fetch_all_news()
            st.session_state.last_fetch = datetime.now()
            st.session_state.sentiment_scores = score_all_sectors(st.session_state.articles)
            watchlist = load_watchlist()
            st.session_state.watchlist_hits = scan_all_watchlist(watchlist, st.session_state.articles)
            if not st.session_state.selected_sector:
                for name, _ in MSCI_SECTORS:
                    if st.session_state.articles.get(name):
                        st.session_state.selected_sector = name
                        break
            if not st.session_state.market_data:
                st.session_state.market_data = fetch_market_overview()
                st.session_state.movers = fetch_tse_movers()
                st.session_state.foreign_flow = fetch_foreign_flow()
        st.rerun()

st.markdown("<div style='margin-bottom:0.5rem'></div>", unsafe_allow_html=True)

# ── Main tabs ─────────────────────────────────────────────────────────────────
tab_news, tab_market, tab_watchlist, tab_sentiment, tab_subscribe = st.tabs([
    "📰 News by Sector",
    "📊 Markets & Macro",
    "⭐ My Watchlist",
    "🌡️ Sentiment",
    "📬 Subscribe",
])

# ════════════════════════════════════════════════════════════
# TAB 1 — NEWS BY SECTOR
# ════════════════════════════════════════════════════════════
with tab_news:
    if not st.session_state.articles:
        st.markdown("""
        <div class="empty-state">
            Click <strong>🔄 Fetch News</strong> above to load today's Japan investment headlines.<br><br>
            <span style="font-size:0.8rem;font-family:'Source Sans 3',sans-serif;color:#9B8B7A;">
                Headlines are fetched live, translated to English,<br>
                flagged for earnings/dividend/M&A signals, and classified by MSCI sector.
            </span>
        </div>
        """, unsafe_allow_html=True)
    else:
        available_sectors = [
            (name, icon, len(st.session_state.articles.get(name, [])))
            for name, icon in MSCI_SECTORS
            if st.session_state.articles.get(name)
        ]
        sector_labels = [f"{icon} {name}  ({count})" for name, icon, count in available_sectors]
        sector_names  = [name for name, icon, count in available_sectors]

        current_index = 0
        if st.session_state.selected_sector in sector_names:
            current_index = sector_names.index(st.session_state.selected_sector)

        selected_label = st.selectbox(
            "Select sector:", options=sector_labels,
            index=current_index, label_visibility="collapsed"
        )
        chosen_index = sector_labels.index(selected_label)
        st.session_state.selected_sector = sector_names[chosen_index]

        sector_name = st.session_state.selected_sector
        raw_articles = st.session_state.articles.get(sector_name, [])
        articles = flag_high_value_articles(raw_articles)
        icon = next((i for n, i in MSCI_SECTORS if n == sector_name), "📰")

        # Sentiment for this sector
        sent = st.session_state.sentiment_scores.get(sector_name, {})
        sent_label = sent.get("label", "")
        sent_color = sent.get("color", "#6B6B6B")
        sent_icon  = sent.get("icon", "")

        count_label = str(len(articles)) + " article" + ("s" if len(articles) != 1 else "")
        sentiment_html = ""
        if sent_label:
            badge_class = "badge-pos" if sent_label == "Positive" else ("badge-neg" if sent_label == "Negative" else "badge-neu")
            sentiment_html = f' &nbsp;<span class="sentiment-badge {badge_class}">{sent_icon} {sent_label}</span>'

        cards = []
        for article in articles:
            original_title   = article.get("original_title", "")
            translated_title = article.get("translated_title", article.get("title", ""))
            source           = article.get("source", "")
            url              = article.get("url", "#")
            pub_date         = article.get("pub_date", "")
            is_high_value    = article.get("high_value", False)

            hv_tag = '<span class="high-value-tag">★ Earnings/Corp Action</span>' if is_high_value else ""
            orig_part = ('<div class="article-title-jp">' + original_title + '</div>'
                        if original_title and original_title != translated_title else "")
            date_part = '<div class="article-meta">' + pub_date + '</div>' if pub_date else ""

            cards.append(
                '<div class="article-card">'
                '<div class="article-source">' + source + '</div>'
                '<div class="article-title"><a href="' + url + '" target="_blank">' + translated_title + '</a>' + hv_tag + '</div>'
                + orig_part + date_part +
                '</div>'
            )

        full_html = (
            '<div class="sector-header">' + icon + ' ' + sector_name + sentiment_html + '</div>'
            '<div class="sector-count">' + count_label + '</div>'
            + ''.join(cards)
        )
        st.markdown(full_html, unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# TAB 2 — MARKETS & MACRO
# ════════════════════════════════════════════════════════════
with tab_market:
    if not st.session_state.market_data:
        st.markdown("""
        <div class="empty-state">
            Click <strong>📈 Refresh Markets</strong> or <strong>🔄 Fetch News</strong> above<br>
            to load live market data.
        </div>
        """, unsafe_allow_html=True)
    else:
        md = st.session_state.market_data

        # ── Indices & FX ──────────────────────────────────────────────────────
        col1, col2 = st.columns(2)

        with col1:
            st.markdown('<div class="section-title">📈 Japanese Indices</div>', unsafe_allow_html=True)

            def metric_box(label, data, is_index=True):
                if not data or data.get("price", 0) == 0:
                    return '<div class="flow-box"><div class="ticker-label">' + label + '</div><div style="font-size:1.2rem;color:#9B8B7A;">Unavailable</div></div>'
                price = data["price"]
                pct   = data.get("pct_change", 0)
                chg   = data.get("change", 0)
                state_label = data.get("state_label", "")
                price_str = f"{price:,.0f}" if is_index else f"{price:,.3f}"
                chg_str   = f"{chg:+,.0f}" if is_index else f"{chg:+,.3f}"
                val_class = "flow-value-up" if pct >= 0 else "flow-value-dn"
                arrow = "▲" if pct >= 0 else "▼"
                state_html = '<div style="font-size:0.65rem;color:#9B8B7A;margin-top:0.15rem;">' + state_label + '</div>' if state_label else ""
                return (
                    '<div class="flow-box">'
                    '<div class="ticker-label">' + label + '</div>'
                    '<div class="' + val_class + '">' + price_str + '</div>'
                    '<div class="flow-label">' + arrow + ' ' + f"{abs(pct):.2f}%" + ' &nbsp;(' + chg_str + ')</div>'
                    + state_html +
                    '</div>'
                )

            st.markdown(metric_box("Nikkei 225", md.get("nikkei"), True), unsafe_allow_html=True)
            st.markdown(metric_box("TOPIX", md.get("topix"), True), unsafe_allow_html=True)

        with col2:
            st.markdown('<div class="section-title">💱 Currency & Rates</div>', unsafe_allow_html=True)
            st.markdown(metric_box("USD / JPY", md.get("usdjpy"), False), unsafe_allow_html=True)
            st.markdown(metric_box("MYR / JPY", md.get("myrjpy"), False), unsafe_allow_html=True)
            st.markdown(metric_box("EUR / JPY", md.get("eurjpy"), False), unsafe_allow_html=True)

        st.markdown("<hr style='border-color:#D9D3C8;margin:1rem 0'>", unsafe_allow_html=True)

        # ── TSE Movers ────────────────────────────────────────────────────────
        col3, col4 = st.columns(2)
        movers = st.session_state.movers

        with col3:
            st.markdown('<div class="section-title">🚀 Top Gainers (TSE)</div>', unsafe_allow_html=True)
            if movers and movers.get("gainers"):
                cards_html = ""
                for m in movers["gainers"]:
                    cards_html += (
                        '<div class="mover-card up">'
                        '<div><div class="mover-name">' + m["name"] + '</div>'
                        '<div class="mover-sym">' + m["symbol"] + ' · ¥' + f'{m["price"]:,.0f}' + '</div></div>'
                        '<div class="mover-pct-up">▲ ' + f'{m["pct_change"]:.2f}%' + '</div>'
                        '</div>'
                    )
                st.markdown(cards_html, unsafe_allow_html=True)
            else:
                st.markdown('<div class="info-box">No gainer data available.</div>', unsafe_allow_html=True)

        with col4:
            st.markdown('<div class="section-title">📉 Top Losers (TSE)</div>', unsafe_allow_html=True)
            if movers and movers.get("losers"):
                cards_html = ""
                for m in movers["losers"]:
                    cards_html += (
                        '<div class="mover-card dn">'
                        '<div><div class="mover-name">' + m["name"] + '</div>'
                        '<div class="mover-sym">' + m["symbol"] + ' · ¥' + f'{m["price"]:,.0f}' + '</div></div>'
                        '<div class="mover-pct-dn">▼ ' + f'{abs(m["pct_change"]):.2f}%' + '</div>'
                        '</div>'
                    )
                st.markdown(cards_html, unsafe_allow_html=True)
            else:
                st.markdown('<div class="info-box">No loser data available.</div>', unsafe_allow_html=True)

        st.markdown("<hr style='border-color:#D9D3C8;margin:1rem 0'>", unsafe_allow_html=True)

        # ── Foreign Investor Flow ─────────────────────────────────────────────
        st.markdown('<div class="section-title">🌍 Foreign Investor Flow (TSE)</div>', unsafe_allow_html=True)
        flow = st.session_state.foreign_flow

        if flow and flow.get("available"):
            net = flow["net_billion_yen"]
            direction = flow["direction"]
            val_class = "flow-value-up" if net > 0 else "flow-value-dn"
            arrow = "▲" if net > 0 else "▼"
            st.markdown(
                f'<div class="flow-box">'
                f'<div class="ticker-label">Weekly Net Flow — Foreign Investors</div>'
                f'<div class="{val_class}">{arrow} ¥{abs(net):.1f}B</div>'
                f'<div class="flow-label">{direction} · {flow.get("as_of","")}</div>'
                f'</div>',
                unsafe_allow_html=True
            )
        else:
            note = flow.get("note", "") if flow else ""
            jpx_url = flow.get("jpx_url", "https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html") if flow else ""
            st.markdown(
                f'<div class="info-box">Foreign flow data is published weekly by JPX (Thursday). '
                f'{note}<br><a href="{jpx_url}" target="_blank" style="color:#8B4513;">→ View on JPX website</a></div>',
                unsafe_allow_html=True
            )

        st.markdown("<hr style='border-color:#D9D3C8;margin:1rem 0'>", unsafe_allow_html=True)

        # ── JGB & BOJ context ─────────────────────────────────────────────────
        st.markdown('<div class="section-title">🏦 BOJ & Rates Context</div>', unsafe_allow_html=True)
        jgb = md.get("jgb10y")
        if jgb and jgb.get("price", 0) > 0:
            st.markdown(metric_box("JGB 10-Year Yield", jgb, False), unsafe_allow_html=True)
        st.markdown("""
        <div class="info-box">
            <strong>Key BOJ themes to watch:</strong> Rate normalisation cycle · Yield curve control (YCC) exit ·
            Yen carry trade dynamics · Wage growth (Shunto) · Core CPI trajectory ·
            TSE capital efficiency reforms (PBR &lt; 1x pressure)
        </div>
        """, unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# TAB 3 — WATCHLIST
# ════════════════════════════════════════════════════════════
with tab_watchlist:
    st.markdown('<div class="section-title">⭐ My Company Watchlist</div>', unsafe_allow_html=True)

    watchlist = load_watchlist()

    # Add company
    col_add, col_btn = st.columns([3, 1])
    with col_add:
        known_names = sorted(KNOWN_COMPANIES.keys())
        all_options = known_names + ["— Enter custom name —"]
        selected_known = st.selectbox("Add from common TSE companies:", all_options, label_visibility="visible")
    with col_btn:
        st.markdown("<div style='margin-top:1.6rem'>", unsafe_allow_html=True)
        if st.button("➕ Add", use_container_width=True):
            if selected_known and selected_known != "— Enter custom name —":
                add_to_watchlist(selected_known)
                st.rerun()

    custom_name = st.text_input("Or add any company name / TSE code:", placeholder="e.g. Recruit, 6098.T, Lasertec")
    if st.button("➕ Add Custom", use_container_width=True):
        if custom_name.strip():
            add_to_watchlist(custom_name.strip())
            st.rerun()

    st.markdown("<hr style='border-color:#D9D3C8;margin:0.8rem 0'>", unsafe_allow_html=True)

    # Current watchlist
    watchlist = load_watchlist()
    if not watchlist:
        st.markdown('<div class="info-box">Your watchlist is empty. Add companies above to track them in the news.</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="section-subtitle">Tracking {len(watchlist)} companies</div>', unsafe_allow_html=True)

        for company in watchlist:
            col_c, col_r = st.columns([4, 1])
            with col_c:
                hits = st.session_state.watchlist_hits.get(company, [])
                hit_text = f"— {len(hits)} article{'s' if len(hits) != 1 else ''} in today's news" if hits else "— No mentions in today's news"
                st.markdown(
                    f'<div style="font-size:0.88rem;font-weight:600;padding:0.3rem 0;">'
                    f'{company} <span style="font-size:0.72rem;color:#9B8B7A;">{hit_text}</span></div>',
                    unsafe_allow_html=True
                )
            with col_r:
                if st.button("Remove", key=f"rm_{company}"):
                    remove_from_watchlist(company)
                    st.rerun()

    # Watchlist news hits
    if st.session_state.watchlist_hits:
        st.markdown("<hr style='border-color:#D9D3C8;margin:1rem 0'>", unsafe_allow_html=True)
        st.markdown('<div class="section-title">📌 Watchlist Mentions in Today\'s News</div>', unsafe_allow_html=True)

        for company, articles in st.session_state.watchlist_hits.items():
            cards_html = f'<div style="font-size:0.8rem;font-weight:700;color:#F9A825;letter-spacing:0.08em;text-transform:uppercase;margin:0.6rem 0 0.3rem 0;">★ {company}</div>'
            for article in articles[:5]:
                title  = article.get("translated_title") or article.get("title", "")
                url    = article.get("url", "#")
                source = article.get("source", "")
                date   = article.get("pub_date", "")
                date_part = '<div class="article-meta">' + date + '</div>' if date else ""
                cards_html += (
                    '<div class="watchlist-hit">'
                    '<div class="watchlist-company">' + source + '</div>'
                    '<div class="article-title"><a href="' + url + '" target="_blank" style="font-size:0.9rem;font-weight:600;color:#1A1A1A;text-decoration:none;">' + title + '</a></div>'
                    + date_part +
                    '</div>'
                )
            st.markdown(cards_html, unsafe_allow_html=True)

    elif st.session_state.articles:
        st.markdown('<div class="info-box">Fetch news to see watchlist mentions.</div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# TAB 4 — SENTIMENT
# ════════════════════════════════════════════════════════════
with tab_sentiment:
    st.markdown('<div class="section-title">🌡️ Sector Sentiment Today</div>', unsafe_allow_html=True)

    if not st.session_state.sentiment_scores:
        st.markdown('<div class="empty-state">Fetch news to generate sentiment scores.</div>', unsafe_allow_html=True)
    else:
        scores = st.session_state.sentiment_scores

        # Summary counts
        pos = sum(1 for s in scores.values() if s.get("label") == "Positive")
        neg = sum(1 for s in scores.values() if s.get("label") == "Negative")
        neu = sum(1 for s in scores.values() if s.get("label") == "Neutral")

        st.markdown(
            f'<div class="info-box">'
            f'Market tone today: <strong style="color:#2E7D32">{pos} sectors positive</strong> · '
            f'<strong style="color:#6B6B6B">{neu} neutral</strong> · '
            f'<strong style="color:#C62828">{neg} negative</strong>'
            f'</div>',
            unsafe_allow_html=True
        )

        # Sector-by-sector sentiment table
        rows_html = ""
        for sector_name, icon in MSCI_SECTORS:
            s = scores.get(sector_name)
            if not s:
                continue
            label = s.get("label", "Neutral")
            badge_class = "badge-pos" if label == "Positive" else ("badge-neg" if label == "Negative" else "badge-neu")
            sent_icon = s.get("icon", "●")
            pos_c = s.get("positive_count", 0)
            neg_c = s.get("negative_count", 0)
            total = s.get("total_articles", 0)
            breakdown = f'{pos_c}↑ · {neg_c}↓ of {total} articles'
            rows_html += (
                '<div class="sentiment-row">'
                '<div class="sentiment-sector">' + icon + ' ' + sector_name + '</div>'
                '<div style="font-size:0.72rem;color:#9B8B7A;flex:1;padding:0 1rem;">' + breakdown + '</div>'
                '<span class="sentiment-badge ' + badge_class + '">' + sent_icon + ' ' + label + '</span>'
                '</div>'
            )
        st.markdown(rows_html, unsafe_allow_html=True)

        st.markdown("""
        <div class="info-box" style="margin-top:1rem">
            <strong>How sentiment is scored:</strong> Each headline is scanned for positive signals
            (growth, record, beat, dividend, buyback) and negative signals (decline, loss, recall,
            restructure, warning). The net score across all sector headlines determines the overall tone.
            This is keyword-based — treat it as a quick directional indicator, not a precise measure.
        </div>
        """, unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# TAB 5 — SUBSCRIBE
# ════════════════════════════════════════════════════════════
with tab_subscribe:
    st.markdown('<div class="section-title">📬 Email Digest Subscription</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="info-box">
        Subscribe to receive today's Japan investment digest by email each morning —
        market data, top movers, watchlist alerts, and sector news all in one email.
    </div>
    """, unsafe_allow_html=True)

    with st.form("subscribe_form"):
        email_input = st.text_input("Your email address:", placeholder="your@email.com")
        submitted = st.form_submit_button("Subscribe", use_container_width=True)
        if submitted:
            if email_input and "@" in email_input:
                result = subscribe_email(email_input)
                if result:
                    st.success(f"✓ {email_input} subscribed successfully.")
                else:
                    st.error("Subscription failed. Please try again.")
            else:
                st.error("Please enter a valid email address.")

    st.markdown("<hr style='border-color:#D9D3C8;margin:1rem 0'>", unsafe_allow_html=True)

    if st.session_state.articles:
        st.markdown('<div class="section-title">⚙️ Send Test Digest</div>', unsafe_allow_html=True)
        test_email = st.text_input("Send test to:", placeholder="your@email.com", key="test_email")
        if st.button("Send Test Digest Now", use_container_width=True):
            if test_email:
                with st.spinner("Sending..."):
                    send_digest(st.session_state.articles, [test_email])
                st.success(f"✓ Digest sent to {test_email}")
            else:
                st.error("Enter an email address first.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;margin-top:2.5rem;padding-top:0.8rem;
            border-top:1px solid #D9D3C8;font-size:0.68rem;color:#9B8B7A;letter-spacing:0.08em;">
    JAPAN INVESTMENT DIGEST · MSCI SECTOR EDITION · TSE INTELLIGENCE<br>
    Market data via Yahoo Finance · News: Japan Times · Nikkei Asia · Reuters · NHK · Asahi · Yahoo Japan & more<br>
    For informational purposes only. Not financial advice.
</div>
""", unsafe_allow_html=True)
