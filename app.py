import streamlit as st
from datetime import datetime
from collector import fetch_all_news
from emailer import subscribe_email, send_digest

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Japan Business Digest",
    page_icon="🗾",
    layout="wide",
    initial_sidebar_state="expanded",
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
.block-container { padding-top: 1.5rem; padding-bottom: 3rem; max-width: 900px; }

/* Masthead */
.masthead {
    border-top: 4px solid #1A1A1A;
    border-bottom: 1px solid #1A1A1A;
    padding: 1.2rem 0 0.8rem 0;
    margin-bottom: 0.4rem;
    text-align: center;
}
.masthead-title {
    font-family: 'Playfair Display', serif;
    font-size: 2.4rem;
    font-weight: 900;
    letter-spacing: -0.02em;
    line-height: 1;
    color: #1A1A1A;
    margin: 0;
}
.masthead-sub {
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: #6B6B6B;
    margin-top: 0.35rem;
}
.masthead-date {
    font-size: 0.78rem;
    color: #6B6B6B;
    margin-top: 0.2rem;
}
.dateline-strip {
    background: #1A1A1A;
    color: #F7F4EF;
    text-align: center;
    padding: 0.3rem;
    font-size: 0.68rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    font-weight: 600;
    margin-bottom: 1.5rem;
}

/* Sector header in main area */
.sector-header {
    font-family: 'Playfair Display', serif;
    font-size: 1.5rem;
    font-weight: 700;
    color: #1A1A1A;
    border-bottom: 2px solid #1A1A1A;
    padding-bottom: 0.4rem;
    margin-bottom: 0.2rem;
}
.sector-count {
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #9B8B7A;
    margin-bottom: 1rem;
}

/* Article cards */
.article-card {
    border-bottom: 1px solid #D9D3C8;
    padding: 0.85rem 0;
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
    font-size: 0.97rem;
    font-weight: 600;
    color: #1A1A1A;
    text-decoration: none;
    line-height: 1.4;
}
.article-title a:hover { color: #8B4513; text-decoration: underline; }
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

/* Sidebar styling */
[data-testid="stSidebar"] {
    background-color: #1A1A1A !important;
}
[data-testid="stSidebar"] * {
    color: #F7F4EF !important;
}
/* All sidebar buttons — unified style */
[data-testid="stSidebar"] .stButton button,
[data-testid="stSidebar"] .stFormSubmitButton button {
    background-color: #2A2A2A !important;
    color: #F7F4EF !important;
    border: 1px solid #3A3A3A !important;
    text-align: left !important;
    font-size: 0.85rem !important;
    padding: 0.4rem 0.7rem !important;
    margin-bottom: 0.2rem !important;
    border-radius: 3px !important;
    width: 100% !important;
}
[data-testid="stSidebar"] .stButton button:hover,
[data-testid="stSidebar"] .stFormSubmitButton button:hover {
    background-color: #3A3A3A !important;
    border-color: #8B4513 !important;
    color: #F7F4EF !important;
}
[data-testid="stSidebar"] .stButton button p,
[data-testid="stSidebar"] .stFormSubmitButton button p {
    color: #F7F4EF !important;
    font-size: 0.85rem !important;
}
[data-testid="stSidebar"] .stRadio label {
    font-size: 0.85rem !important;
    padding: 0.3rem 0 !important;
    cursor: pointer;
}
[data-testid="stSidebar"] .stRadio > div {
    gap: 0rem !important;
}
[data-testid="stSidebar"] hr {
    border-color: #444 !important;
    margin: 0.8rem 0 !important;
}
/* Sidebar text inputs */
[data-testid="stSidebar"] .stTextInput input {
    background-color: #2A2A2A !important;
    color: #F7F4EF !important;
    border: 1px solid #3A3A3A !important;
}
[data-testid="stSidebar"] .stTextInput input::placeholder {
    color: #888 !important;
}

/* Sidebar sector badge counts */
.sector-badge {
    display: inline-block;
    background: #3A3A3A;
    color: #F7F4EF;
    font-size: 0.65rem;
    padding: 0.1rem 0.4rem;
    border-radius: 10px;
    margin-left: 0.4rem;
    font-weight: 600;
    vertical-align: middle;
}

/* Empty state */
.empty-state {
    font-family: 'Playfair Display', serif;
    font-size: 1rem;
    color: #6B6B6B;
    text-align: center;
    padding: 3rem 1rem;
    line-height: 1.8;
}

/* Subscribe box */
.subscribe-box {
    background: #1A1A1A;
    color: #F7F4EF;
    padding: 1.5rem;
    border-radius: 4px;
    margin-top: 2rem;
    text-align: center;
}
.subscribe-box h3 {
    font-family: 'Playfair Display', serif;
    font-size: 1.2rem;
    margin-bottom: 0.4rem;
    color: #F7F4EF;
}
.subscribe-box p {
    font-size: 0.82rem;
    color: #A09890;
    margin-bottom: 0.8rem;
}
</style>
""", unsafe_allow_html=True)

# ── MSCI Sectors ──────────────────────────────────────────────────────────────
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
if "selected_sector" not in st.session_state:
    st.session_state.selected_sector = None

# ── Masthead (main area) ──────────────────────────────────────────────────────
st.markdown(f"""
<div class="masthead">
    <div class="masthead-title">Japan Business Digest</div>
    <div class="masthead-sub">日本経済ニュース — MSCI Sector Edition</div>
    <div class="masthead-date">{datetime.now().strftime('%A, %B %d, %Y')}</div>
</div>
<div class="dateline-strip">Petaling Jaya · RSS Edition · Major Japanese Business &amp; Trade Media</div>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="font-family:'Playfair Display',serif;font-size:1.1rem;font-weight:700;
                color:#F7F4EF;letter-spacing:0.05em;padding:0.5rem 0 0.3rem 0;
                border-bottom:1px solid #444;margin-bottom:0.8rem;">
        📰 MSCI Sectors
    </div>
    """, unsafe_allow_html=True)

    # Fetch button
    if st.button("🔄 Fetch Latest News", use_container_width=True):
        with st.spinner("Fetching & translating..."):
            st.session_state.articles = fetch_all_news()
            st.session_state.last_fetch = datetime.now()
            # Auto-select first sector with articles
            for name, _ in MSCI_SECTORS:
                if st.session_state.articles.get(name):
                    st.session_state.selected_sector = name
                    break
        st.rerun()

    # Last updated info
    if st.session_state.last_fetch:
        total = sum(len(v) for v in st.session_state.articles.values())
        st.markdown(f"""
        <div style="font-size:0.68rem;color:#888;margin:0.5rem 0 0.8rem 0;line-height:1.5;">
            Updated: {st.session_state.last_fetch.strftime("%H:%M")}<br>
            {total} articles loaded
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    # Sector list — only show sectors that have articles
    if st.session_state.articles:
        available = [
            (name, icon)
            for name, icon in MSCI_SECTORS
            if st.session_state.articles.get(name)
        ]

        for sector_name, icon in available:
            count = len(st.session_state.articles.get(sector_name, []))
            is_selected = st.session_state.selected_sector == sector_name

            # Highlight selected sector
            bg = "#3A3A3A" if is_selected else "transparent"
            border = "2px solid #8B4513" if is_selected else "2px solid transparent"

            button_label = f"{icon} {sector_name}  ({count})"
            if st.button(
                button_label,
                key=f"sector_{sector_name}",
                use_container_width=True,
            ):
                st.session_state.selected_sector = sector_name
                st.rerun()

    else:
        st.markdown("""
        <div style="font-size:0.78rem;color:#888;padding:0.5rem 0;line-height:1.6;">
            Click "Fetch Latest News"<br>to load today's headlines.
        </div>
        """, unsafe_allow_html=True)

    # Email subscribe at bottom of sidebar
    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown("""
    <div style="font-size:0.72rem;font-weight:700;letter-spacing:0.1em;
                text-transform:uppercase;color:#888;margin-bottom:0.5rem;">
        📬 Email Digest
    </div>
    """, unsafe_allow_html=True)

    with st.form("subscribe_form"):
        email_input = st.text_input(
            "", placeholder="your@email.com", label_visibility="collapsed"
        )
        if st.form_submit_button("Subscribe", use_container_width=True):
            if email_input and "@" in email_input:
                subscribe_email(email_input)
                st.success("✓ Subscribed!")
            else:
                st.error("Enter a valid email.")

# ── Main content area ─────────────────────────────────────────────────────────
if not st.session_state.articles:
    # No data yet
    st.markdown("""
    <div class="empty-state">
        Click <strong>🔄 Fetch Latest News</strong> in the sidebar<br>
        to load today's Japan business headlines.<br><br>
        <span style="font-size:0.8rem;font-family:'Source Sans 3',sans-serif;color:#9B8B7A;">
            Headlines are fetched live, translated to English,<br>
            and organised by MSCI Global Industry Classification sector.
        </span>
    </div>
    """, unsafe_allow_html=True)

elif not st.session_state.selected_sector:
    # Data loaded but no sector selected yet — prompt user
    st.markdown("""
    <div class="empty-state">
        ← Select a sector from the sidebar to view headlines.
    </div>
    """, unsafe_allow_html=True)

else:
    # Show selected sector's articles
    sector_name = st.session_state.selected_sector
    articles = st.session_state.articles.get(sector_name, [])

    # Find icon
    icon = next((i for n, i in MSCI_SECTORS if n == sector_name), "📰")

    st.markdown(f'<div class="sector-header">{icon} {sector_name}</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="sector-count">{len(articles)} article{"s" if len(articles) != 1 else ""}</div>',
        unsafe_allow_html=True
    )

    if not articles:
        st.markdown('<div class="no-articles">No articles found for this sector.</div>', unsafe_allow_html=True)
    else:
        for article in articles:
            original_title  = article.get("original_title", "")
            translated_title = article.get("translated_title", article.get("title", ""))
            source   = article.get("source", "")
            url      = article.get("url", "#")
            pub_date = article.get("pub_date", "")

            original_html = (
                f'<div class="article-title-jp">{original_title}</div>'
                if original_title and original_title != translated_title else ""
            )
            date_html = (
                f'<div class="article-meta">{pub_date}</div>'
                if pub_date else ""
            )

            st.markdown(f"""
            <div class="article-card">
                <div class="article-source">{source}</div>
                <div class="article-title"><a href="{url}" target="_blank">{translated_title}</a></div>
                {original_html}
                {date_html}
            </div>
            """, unsafe_allow_html=True)

# ── Admin panel ───────────────────────────────────────────────────────────────
if st.session_state.articles:
    with st.expander("⚙️ Admin: Send test digest"):
        test_email = st.text_input("Send digest to:", placeholder="your@email.com")
        if st.button("Send Test Digest"):
            if test_email:
                send_digest(st.session_state.articles, [test_email])
                st.success(f"Digest sent to {test_email}")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;margin-top:3rem;padding-top:1rem;
            border-top:1px solid #D9D3C8;font-size:0.7rem;color:#9B8B7A;letter-spacing:0.08em;">
    JAPAN BUSINESS DIGEST · MSCI SECTOR EDITION<br>
    Japan Times · Nikkei Asia · Reuters · NHK · Asahi · Mainichi · Sankei · Yahoo Japan · Toyo Keizai & more
</div>
""", unsafe_allow_html=True)
