import streamlit as st
from datetime import datetime
from collector import fetch_all_news
from emailer import subscribe_email, send_digest

# ── Page config ───────────────────────────────────────────────────────────────
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

html, body, [class*="css"] {
    font-family: 'Source Sans 3', sans-serif;
    background-color: #F7F4EF;
    color: #1A1A1A;
}
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1.5rem; padding-bottom: 3rem; max-width: 1000px; }

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
    font-size: 2.6rem;
    font-weight: 900;
    letter-spacing: -0.02em;
    line-height: 1;
    color: #1A1A1A;
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
    margin-bottom: 1.2rem;
}

/* Sector tab bar */
.sector-tab-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    padding: 0.8rem 0;
    border-bottom: 2px solid #1A1A1A;
    margin-bottom: 1.5rem;
}
.sector-tab {
    display: inline-block;
    background: #1A1A1A;
    color: #F7F4EF;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.35rem 0.75rem;
    border-radius: 2px;
    cursor: pointer;
    border: 2px solid transparent;
    white-space: nowrap;
}
.sector-tab:hover {
    background: #8B4513;
}
.sector-tab.active {
    background: #F7F4EF;
    color: #1A1A1A;
    border: 2px solid #1A1A1A;
}
.sector-tab .tab-count {
    font-size: 0.62rem;
    opacity: 0.7;
    margin-left: 0.3rem;
}

/* Streamlit selectbox styled as tabs */
[data-testid="stSelectbox"] > div > div {
    background-color: #1A1A1A !important;
    color: #F7F4EF !important;
    border: 1px solid #3A3A3A !important;
    border-radius: 3px !important;
    font-size: 0.9rem !important;
}

/* Sector header */
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

/* Toolbar row */
.toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 0.8rem;
}
.last-updated {
    font-size: 0.75rem;
    color: #9B8B7A;
}

/* Subscribe strip */
.subscribe-strip {
    background: #1A1A1A;
    color: #F7F4EF;
    padding: 1.2rem 1.5rem;
    border-radius: 4px;
    margin-top: 2.5rem;
    display: flex;
    align-items: center;
    gap: 1rem;
    flex-wrap: wrap;
}
.subscribe-strip-label {
    font-family: 'Playfair Display', serif;
    font-size: 1rem;
    font-weight: 700;
    white-space: nowrap;
    color: #F7F4EF;
}
.subscribe-strip-sub {
    font-size: 0.78rem;
    color: #A09890;
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

/* Footer */
.footer {
    text-align: center;
    margin-top: 3rem;
    padding-top: 1rem;
    border-top: 1px solid #D9D3C8;
    font-size: 0.7rem;
    color: #9B8B7A;
    letter-spacing: 0.08em;
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

# ── Masthead ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="masthead">
    <div class="masthead-title">Japan Business Digest</div>
    <div class="masthead-sub">日本経済ニュース — MSCI Sector Edition</div>
    <div class="masthead-date">{datetime.now().strftime('%A, %B %d, %Y')}</div>
</div>
<div class="dateline-strip">Petaling Jaya · RSS Edition · Major Japanese Business &amp; Trade Media</div>
""", unsafe_allow_html=True)

# ── Toolbar: fetch button + last updated ──────────────────────────────────────
col_info, col_btn = st.columns([3, 1])
with col_info:
    if st.session_state.last_fetch:
        total = sum(len(v) for v in st.session_state.articles.values())
        active = sum(1 for v in st.session_state.articles.values() if v)
        st.markdown(
            f'<div class="last-updated">Last updated: '
            f'{st.session_state.last_fetch.strftime("%H:%M")} · '
            f'{total} articles across {active} sectors</div>',
            unsafe_allow_html=True
        )
with col_btn:
    if st.button("🔄 Fetch Latest News", use_container_width=True):
        with st.spinner("Fetching & translating headlines..."):
            st.session_state.articles = fetch_all_news()
            st.session_state.last_fetch = datetime.now()
            # Auto-select first populated sector
            for name, _ in MSCI_SECTORS:
                if st.session_state.articles.get(name):
                    st.session_state.selected_sector = name
                    break
        st.rerun()

# ── Sector tab bar (dropdown on small screens) ────────────────────────────────
if st.session_state.articles:
    available_sectors = [
        (name, icon, len(st.session_state.articles.get(name, [])))
        for name, icon in MSCI_SECTORS
        if st.session_state.articles.get(name)
    ]

    # Build label list for selectbox
    sector_labels = [
        f"{icon} {name}  ({count})"
        for name, icon, count in available_sectors
    ]
    sector_names = [name for name, icon, count in available_sectors]

    # Find current index
    current_index = 0
    if st.session_state.selected_sector in sector_names:
        current_index = sector_names.index(st.session_state.selected_sector)

    selected_label = st.selectbox(
        "Select sector:",
        options=sector_labels,
        index=current_index,
        label_visibility="collapsed",
    )

    # Update selected sector from dropdown choice
    chosen_index = sector_labels.index(selected_label)
    st.session_state.selected_sector = sector_names[chosen_index]

# ── Main content: selected sector articles ────────────────────────────────────
if not st.session_state.articles:
    st.markdown("""
    <div class="empty-state">
        Click <strong>🔄 Fetch Latest News</strong> above to load today's Japan business headlines.<br><br>
        <span style="font-size:0.8rem;font-family:'Source Sans 3',sans-serif;color:#9B8B7A;">
            Headlines are fetched live, translated to English,<br>
            and organised by MSCI Global Industry Classification sector.
        </span>
    </div>
    """, unsafe_allow_html=True)

elif st.session_state.selected_sector:
    sector_name = st.session_state.selected_sector
    articles = st.session_state.articles.get(sector_name, [])
    icon = next((i for n, i in MSCI_SECTORS if n == sector_name), "\U0001f4f0")

    # Build all article cards as one HTML string, rendered in a single st.markdown call
    # This avoids Streamlit escaping HTML inside nested f-strings
    count_label = str(len(articles)) + " article" + ("s" if len(articles) != 1 else "")

    cards = []
    for article in articles:
        original_title   = article.get("original_title", "")
        translated_title = article.get("translated_title", article.get("title", ""))
        source           = article.get("source", "")
        url              = article.get("url", "#")
        pub_date         = article.get("pub_date", "")

        orig_part = ""
        if original_title and original_title != translated_title:
            orig_part = "<div class=\"article-title-jp\">" + original_title + "</div>"

        date_part = ""
        if pub_date:
            date_part = "<div class=\"article-meta\">" + pub_date + "</div>"

        card = (
            "<div class=\"article-card\">"
            "<div class=\"article-source\">" + source + "</div>"
            "<div class=\"article-title\"><a href=\"" + url + "\" target=\"_blank\">" + translated_title + "</a></div>"
            + orig_part + date_part +
            "</div>"
        )
        cards.append(card)

    full_html = (
        "<div class=\"sector-header\">" + icon + " " + sector_name + "</div>"
        "<div class=\"sector-count\">" + count_label + "</div>"
        + "".join(cards)
    )
    st.markdown(full_html, unsafe_allow_html=True)

# ── Email subscription strip ──────────────────────────────────────────────────
st.markdown("""
<div class="subscribe-strip">
    <div>
        <div class="subscribe-strip-label">📬 Morning Digest Email</div>
        <div class="subscribe-strip-sub">Receive today's headlines by sector, every morning.</div>
    </div>
</div>
""", unsafe_allow_html=True)

with st.form("subscribe_form"):
    col_a, col_b = st.columns([3, 1])
    with col_a:
        email_input = st.text_input(
            "", placeholder="your@email.com", label_visibility="collapsed"
        )
    with col_b:
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

# ── Admin panel ───────────────────────────────────────────────────────────────
if st.session_state.articles:
    with st.expander("⚙️ Admin: Send test digest"):
        test_email = st.text_input("Send digest to:", placeholder="your@email.com", key="test_email")
        if st.button("Send Test Digest", key="send_test"):
            if test_email:
                with st.spinner("Sending..."):
                    send_digest(st.session_state.articles, [test_email])
                st.success(f"✓ Digest sent to {test_email}")
            else:
                st.error("Enter an email address first.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="footer">
    JAPAN BUSINESS DIGEST · MSCI SECTOR EDITION<br>
    Japan Times · Nikkei Asia · Reuters · NHK · Asahi · Mainichi · Sankei · Yahoo Japan · Toyo Keizai & more
</div>
""", unsafe_allow_html=True)
