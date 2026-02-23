import streamlit as st
import json
import os
from datetime import datetime
from collector import fetch_all_news
from emailer import subscribe_email, send_digest

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Japan Business Digest",
    page_icon="🗾",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700;900&family=Source+Sans+3:wght@300;400;600&family=Noto+Sans+JP:wght@300;400&display=swap');

/* Base */
html, body, [class*="css"] {
    font-family: 'Source Sans 3', sans-serif;
    background-color: #F7F4EF;
    color: #1A1A1A;
}

/* Hide Streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 2rem; padding-bottom: 4rem; max-width: 1100px; }

/* Masthead */
.masthead {
    border-top: 4px solid #1A1A1A;
    border-bottom: 1px solid #1A1A1A;
    padding: 1.5rem 0 1rem 0;
    margin-bottom: 0.5rem;
    text-align: center;
}
.masthead-title {
    font-family: 'Playfair Display', serif;
    font-size: 3rem;
    font-weight: 900;
    letter-spacing: -0.02em;
    line-height: 1;
    color: #1A1A1A;
    margin: 0;
}
.masthead-sub {
    font-family: 'Source Sans 3', sans-serif;
    font-size: 0.78rem;
    font-weight: 600;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: #6B6B6B;
    margin-top: 0.4rem;
}
.masthead-date {
    font-family: 'Source Sans 3', sans-serif;
    font-size: 0.82rem;
    color: #6B6B6B;
    margin-top: 0.25rem;
}

/* Dateline strip */
.dateline-strip {
    background: #1A1A1A;
    color: #F7F4EF;
    text-align: center;
    padding: 0.35rem;
    font-size: 0.72rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    font-weight: 600;
    margin-bottom: 2rem;
}

/* Sector header */
.sector-header {
    font-family: 'Playfair Display', serif;
    font-size: 1.35rem;
    font-weight: 700;
    color: #1A1A1A;
    border-bottom: 2px solid #1A1A1A;
    padding-bottom: 0.4rem;
    margin-top: 0.5rem;
    margin-bottom: 0.1rem;
}
.sector-count {
    font-family: 'Source Sans 3', sans-serif;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #9B8B7A;
    margin-bottom: 0.8rem;
}

/* Article card */
.article-card {
    border-bottom: 1px solid #D9D3C8;
    padding: 0.85rem 0;
    transition: background 0.15s;
}
.article-card:hover {
    background: rgba(255,255,255,0.6);
    padding-left: 0.5rem;
    border-radius: 4px;
}
.article-source {
    font-size: 0.68rem;
    font-weight: 600;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #8B4513;
    margin-bottom: 0.2rem;
}
.article-title a {
    font-family: 'Source Sans 3', sans-serif;
    font-size: 0.97rem;
    font-weight: 600;
    color: #1A1A1A;
    text-decoration: none;
    line-height: 1.4;
}
.article-title a:hover {
    color: #8B4513;
    text-decoration: underline;
}
.article-title-jp {
    font-family: 'Noto Sans JP', sans-serif;
    font-size: 0.75rem;
    color: #9B8B7A;
    margin-top: 0.15rem;
    font-weight: 300;
}
.article-meta {
    font-size: 0.7rem;
    color: #9B8B7A;
    margin-top: 0.2rem;
}

/* Sector icon pill */
.sector-pill {
    display: inline-block;
    background: #1A1A1A;
    color: #F7F4EF;
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.15rem 0.55rem;
    border-radius: 2px;
    margin-right: 0.4rem;
    font-weight: 600;
}

/* Email subscription */
.subscribe-box {
    background: #1A1A1A;
    color: #F7F4EF;
    padding: 2rem;
    border-radius: 4px;
    margin-top: 3rem;
    text-align: center;
}
.subscribe-box h3 {
    font-family: 'Playfair Display', serif;
    font-size: 1.4rem;
    margin-bottom: 0.5rem;
    color: #F7F4EF;
}
.subscribe-box p {
    font-size: 0.85rem;
    color: #A09890;
    margin-bottom: 1rem;
}

/* Loading spinner text */
.loading-text {
    font-family: 'Playfair Display', serif;
    font-size: 1.1rem;
    color: #6B6B6B;
    text-align: center;
    padding: 2rem;
}

/* Refresh bar */
.refresh-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.75rem;
    color: #9B8B7A;
    border-bottom: 1px solid #D9D3C8;
    padding-bottom: 0.5rem;
    margin-bottom: 1.5rem;
}

/* No articles */
.no-articles {
    font-size: 0.85rem;
    color: #9B8B7A;
    font-style: italic;
    padding: 0.5rem 0 1rem 0;
}

/* Divider */
.section-divider {
    border: none;
    border-top: 1px solid #D9D3C8;
    margin: 2rem 0;
}
</style>
""", unsafe_allow_html=True)

# ── MSCI Sector definitions & icons ──────────────────────────────────────────
MSCI_SECTORS = [
    ("Energy", "⛽"),
    ("Materials", "🏭"),
    ("Industrials", "⚙️"),
    ("Consumer Discretionary", "🛍️"),
    ("Consumer Staples", "🛒"),
    ("Health Care", "💊"),
    ("Financials", "🏦"),
    ("Information Technology", "💻"),
    ("Communication Services", "📡"),
    ("Utilities", "💡"),
    ("Real Estate", "🏢"),
    ("General / Macro", "🗾"),
]

# ── Session state ─────────────────────────────────────────────────────────────
if "articles" not in st.session_state:
    st.session_state.articles = {}
if "last_fetch" not in st.session_state:
    st.session_state.last_fetch = None
if "loading" not in st.session_state:
    st.session_state.loading = False

# ── Masthead ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="masthead">
    <div class="masthead-title">Japan Business Digest</div>
    <div class="masthead-sub">日本経済ニュース — MSCI Sector Edition</div>
    <div class="masthead-date">{datetime.now().strftime('%A, %B %d, %Y')}</div>
</div>
<div class="dateline-strip">Petaling Jaya · RSS Edition · Major Japanese Business &amp; Trade Media</div>
""", unsafe_allow_html=True)

# ── Refresh controls ──────────────────────────────────────────────────────────
col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    if st.session_state.last_fetch:
        st.markdown(f'<div style="font-size:0.78rem;color:#9B8B7A;">Last updated: {st.session_state.last_fetch.strftime("%H:%M")} · {len([a for sector in st.session_state.articles.values() for a in sector])} articles across {sum(1 for s in st.session_state.articles.values() if s)} sectors</div>', unsafe_allow_html=True)

with col3:
    if st.button("🔄 Fetch Latest News", use_container_width=True):
        with st.spinner("Gathering news from Japanese media..."):
            st.session_state.articles = fetch_all_news()
            st.session_state.last_fetch = datetime.now()
        st.rerun()

# ── Auto-load on first visit ──────────────────────────────────────────────────
if not st.session_state.articles:
    st.markdown('<div class="loading-text">Click <strong>Fetch Latest News</strong> to load today\'s Japan business headlines.</div>', unsafe_allow_html=True)
    st.markdown("""
    <div style="text-align:center;margin-top:1rem;">
        <div style="font-size:0.8rem;color:#9B8B7A;max-width:500px;margin:auto;">
            Headlines are fetched live from RSS feeds, translated to English, 
            and classified into MSCI Global Industry Classification Standard sectors.
        </div>
    </div>
    """, unsafe_allow_html=True)
else:
    # ── News display by sector ────────────────────────────────────────────────
    articles_by_sector = st.session_state.articles

    for sector_name, icon in MSCI_SECTORS:
        articles = articles_by_sector.get(sector_name, [])
        if not articles:
            continue

        st.markdown(f'<div class="sector-header">{icon} {sector_name}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="sector-count">{len(articles)} article{"s" if len(articles) != 1 else ""}</div>', unsafe_allow_html=True)

        for article in articles:
            original_title = article.get("original_title", "")
            translated_title = article.get("translated_title", article.get("title", ""))
            source = article.get("source", "")
            url = article.get("url", "#")
            pub_date = article.get("pub_date", "")

            original_html = f'<div class="article-title-jp">{original_title}</div>' if original_title and original_title != translated_title else ""

            st.markdown(f"""
            <div class="article-card">
                <div class="article-source">{source}</div>
                <div class="article-title"><a href="{url}" target="_blank">{translated_title}</a></div>
                {original_html}
                <div class="article-meta">{pub_date}</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

# ── Email subscription ────────────────────────────────────────────────────────
st.markdown("""
<div class="subscribe-box">
    <h3>📬 Morning Digest Email</h3>
    <p>Receive today's Japan business headlines — organized by MSCI sector — delivered to your inbox each morning.</p>
</div>
""", unsafe_allow_html=True)

with st.form("subscribe_form"):
    col_a, col_b = st.columns([3, 1])
    with col_a:
        email_input = st.text_input("", placeholder="your@email.com", label_visibility="collapsed")
    with col_b:
        subscribe_btn = st.form_submit_button("Subscribe", use_container_width=True)

    if subscribe_btn:
        if email_input and "@" in email_input:
            result = subscribe_email(email_input)
            if result:
                st.success(f"✓ {email_input} subscribed. You'll receive tomorrow's digest.")
            else:
                st.warning("Subscription saved locally. Configure SendGrid API key to activate email delivery.")
        else:
            st.error("Please enter a valid email address.")

# ── Send digest button (for testing) ─────────────────────────────────────────
if st.session_state.articles:
    with st.expander("⚙️ Admin: Send digest now (for testing)"):
        test_email = st.text_input("Send test digest to:", placeholder="your@email.com")
        if st.button("Send Test Digest"):
            if test_email:
                send_digest(st.session_state.articles, [test_email])
                st.success(f"Digest sent to {test_email}")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;margin-top:3rem;padding-top:1rem;border-top:1px solid #D9D3C8;font-size:0.72rem;color:#9B8B7A;letter-spacing:0.08em;">
    JAPAN BUSINESS DIGEST · RSS EDITION · Sources include: Japan Times, Asahi Shimbun, NHK, Reuters Japan, Nikkei Asia, Yahoo Japan News, Mainichi, Sankei & more
</div>
""", unsafe_allow_html=True)
