import streamlit as st
from datetime import datetime
import pytz
from collector import fetch_all_news, fetch_source_headlines, SOURCE_DIRECTORY, SOURCE_GROUPS
from emailer import subscribe_email, send_digest, get_secret
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
    ("Japan Industry News", "https://japanindustrynews.com/",             "🏭"),
    ("FACTA",               "https://facta.co.jp/",                        "🔍"),
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
    font-size: 0.62rem; font-weight: 600; padding: 0.28rem 0.45rem;
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
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ── AI Summary helper ─────────────────────────────────────────────────────────
def render_ai_summary(articles: list, context: str, session_key: str, max_articles: int = 60):
    """
    Renders an AI-powered summary panel with a Generate button.
    Uses the Anthropic API (ANTHROPIC_API_KEY in Streamlit Secrets).
    articles: list of article dicts with title/translated_title/url/source/pub_date
    context:  short description for the prompt ("last 24h news", "co filings today", etc.)
    session_key: unique key for caching the summary in session_state
    """

    if session_key not in st.session_state:
        st.session_state[session_key] = None

    col_s1, col_s2 = st.columns([4, 1])
    with col_s2:
        gen_btn = st.button("✨ Summarise", key=f"btn_{session_key}", use_container_width=True)
    with col_s1:
        if st.session_state[session_key]:
            st.markdown(
                '<div style="font-size:0.68rem;color:#9B8B7A;padding-top:0.45rem;">AI summary generated · click ✨ Summarise to refresh</div>',
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
                lines = []
                for i, a in enumerate(subset, 1):
                    title  = a.get("title") or a.get("translated_title") or a.get("original_title","")
                    url    = a.get("url","")
                    source = a.get("source","")
                    pub    = a.get("pub_date","")
                    lines.append(f"{i}. [{source}] {title} | {pub} | {url}")
                article_text = "\n".join(lines)

                prompt = f"""You are an analyst helping a Malaysian investor track Japan business and investment news.

Here are {len(subset)} headlines from {context}:

{article_text}

Write a structured briefing that:
1. Opens with 2-3 sentences on the overall mood/theme of the day
2. Groups stories into 4-6 thematic clusters (e.g. "BOJ & Macro", "Corporate Earnings", "M&A / Restructuring", "Yen & FX", "Sector Moves" — use whatever fits the actual stories)
3. Under each cluster: 3-5 bullet points summarising the key developments, with each bullet ending with a markdown hyperlink [Source Name](url) to the most relevant article
4. Closes with 2-3 sentences on what to watch next

Format rules:
- Use markdown headers (##) for cluster names
- Keep each bullet to one clear sentence + link
- If multiple articles support a point, link the most important one only
- Be factual and concise — no padding or filler phrases
- Do not reproduce article titles verbatim; synthesise them

Respond only with the briefing, no preamble."""

                with st.spinner("Generating AI summary…"):
                    try:
                        client = _anthropic.Anthropic(api_key=api_key)
                        msg = client.messages.create(
                            model="claude-haiku-4-5-20251001",
                            max_tokens=1200,
                            messages=[{"role": "user", "content": prompt}]
                        )
                        st.session_state[session_key] = msg.content[0].text
                    except Exception as e:
                        st.error(f"AI summary error: {e}")

    if st.session_state[session_key]:
        def _summary_to_html(text: str) -> str:
            """Convert AI summary markdown to styled HTML."""
            import re as _re2, html as _html2
            lines  = text.split("\n")
            out    = []
            in_ul  = False
            in_intro = True   # first paragraph(s) before any ## header get styled as intro

            for line in lines:
                line = line.rstrip()

                # Convert **bold** → <strong>
                line = _re2.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", line)

                # Convert [text](url) → Link button, skip if URL looks truncated/invalid
                def _make_link(m):
                    _u = m.group(2).strip()
                    if not _u or not _u.startswith("http") or len(_u) < 12:
                        return m.group(1)  # just return the text, no link
                    return f'<a class="summary-link" href="{_u}" target="_blank">Link</a>'
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
                    out.append(f"<li>{line[2:]}</li>")

                elif line.strip() == "":
                    if in_ul:
                        out.append("</ul>"); in_ul = False
                    # blank line — skip, spacing handled by CSS

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

        st.markdown(
            '<div class="ai-summary">' + _summary_to_html(st.session_state[session_key]) + '</div>',
            unsafe_allow_html=True
        )

# ── Masthead ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="masthead">
    <div class="masthead-title">Japan Investment Digest</div>
    <div class="masthead-sub">Japan equities · macro · corporate news · TDnet filings · JPY rates</div>
    <div class="masthead-date">{now_local().strftime('%A, %d %B %Y · %H:%M MYT')}</div>
</div>
<div class="dateline-strip">Petaling Jaya · Nikkei 225 · TOPIX · JPY Rates · TSE Timely Disclosures · 36 News Sources</div>
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
            try:
                sector_map, source_map = fetch_all_news()
                st.session_state.articles = sector_map if isinstance(sector_map, dict) else {}
                st.session_state.source_map = source_map if isinstance(source_map, dict) else {}
            except Exception as e:
                st.error("News fetch failed: " + str(e))
                st.session_state.articles = {}
                st.session_state.source_map = {}
            st.session_state.last_fetch = now_local()
            # Only score if we actually have articles
            if st.session_state.articles:
                try:
                    st.session_state.sentiment_scores = score_all_sectors(st.session_state.articles)
                except Exception as e:
                    print(f"Sentiment scoring failed: {e}")
                    st.session_state.sentiment_scores = {}
                try:
                    wl = load_watchlist()
                    st.session_state.watchlist_hits = scan_all_watchlist(wl, st.session_state.articles)
                except Exception as e:
                    print(f"Watchlist scan failed: {e}")
                    st.session_state.watchlist_hits = {}
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
(tab_market, tab_bytime, tab_breaking, tab_news, tab_bysource,
 tab_sources, tab_filings, tab_sentiment, tab_watchlist, tab_subscribe) = st.tabs([
    "📊 Markets", "🕐 By Time", "⚡ Breaking News", "📰 By Industry",
    "📁 By Source", "🔗 Sources", "📋 Co Filings",
    "🌡️ Sentiment", "⭐ Watchlist", "📬 Subscribe",
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
        st.markdown('<div class="section-title" style="font-size:0.78rem;margin-top:0.2rem;">✨ AI Briefing — Last 24 Hours</div>', unsafe_allow_html=True)
        render_ai_summary(
            articles_24h or all_articles[:60],
            "the last 24 hours of Japan business news across all sources",
            "summary_bytime"
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

        st.markdown(
            f'<div class="info-box">{len(all_articles)} headlines from all sources, newest first.</div>',
            unsafe_allow_html=True
        )

        html = ""
        last_date = None
        for a in all_articles:
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
            badge_html = '<span class="hv-badge">★ Corp Action</span>' if hv else ""

            time_str = ""
            if pub_dt:
                time_str = format_local_dt(pub_dt)

            html += (
                '<div class="article-card">'
                '<div class="article-meta">'
                + source
                + (' · ' + time_str if time_str else '')
                + '</div>'
                '<a class="article-link" href="' + url + '" target="_blank">' + title + '</a>'
                + (badge_html if badge_html else '')
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
    # AI summary caches
    for _sk in ["summary_bytime", "summary_breaking", "summary_industry", "summary_filings"]:
        if _sk not in st.session_state:
            st.session_state[_sk] = None

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
                '<a class="article-link" href="' + url + '" target="_blank">' + title + '</a>'
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
        index_order = ["nikkei", "topix"]
        idx_html = '<div class="instrument-grid">'
        for key in index_order:
            data = indices.get(key)
            if data:
                idx_html += instrument_card(data, is_forex=False)
        idx_html += '</div>'
        st.markdown(idx_html, unsafe_allow_html=True)

        st.markdown("<hr style='border-color:#D9D3C8;margin:0.9rem 0'>", unsafe_allow_html=True)

        # ── Forex ────────────────────────────────────────────
        st.markdown('<div class="section-title">💱 Currency Pairs vs JPY</div>', unsafe_allow_html=True)
        forex = md.get("forex", {})
        forex_order = ["usdjpy", "eurjpy", "cnyjpy", "sgdjpy"]
        fx_html = '<div class="instrument-grid">'
        for key in forex_order:
            data = forex.get(key)
            if data:
                fx_html += instrument_card(data, is_forex=True)
        fx_html += '</div>'
        st.markdown(fx_html, unsafe_allow_html=True)

        st.markdown("<hr style='border-color:#D9D3C8;margin:0.9rem 0'>", unsafe_allow_html=True)

        # ── TSE Movers ───────────────────────────────────────
        movers = st.session_state.movers or {}
        col3, col4 = st.columns(2)
        with col3:
            st.markdown('<div class="section-title">🚀 Top Gainers</div>', unsafe_allow_html=True)
            gainers = movers.get("gainers", [])
            if gainers:
                html = ""
                for m in gainers:
                    html += (
                        '<div class="mover-card up">'
                        '<div><div class="mover-name">' + m["name"] + '</div>'
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
                    html += (
                        '<div class="mover-card dn">'
                        '<div><div class="mover-name">' + m["name"] + '</div>'
                        '<div class="mover-sym">' + m["symbol"] + " · ¥" + f'{m["price"]:,.0f}' + '</div></div>'
                        '<div class="mover-pct-dn">▼ ' + f'{abs(m["pct_change"]):.2f}%' + '</div>'
                        '</div>'
                    )
                st.markdown(html, unsafe_allow_html=True)
            else:
                st.markdown('<div class="info-box">No mover data available.</div>', unsafe_allow_html=True)

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

# ════════════════════════════════════════════════════════════
# TAB — CO FILINGS (TDnet via Yanoshin RSS — most reliable method)
# ════════════════════════════════════════════════════════════
with tab_filings:
    st.markdown('<div class="section-title">📋 Corporate Filings — TDnet Timely Disclosures</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="info-box">Timely disclosures (適時開示) from the Tokyo Stock Exchange — '
        'last 5 days. Sourced via <a href="https://webapi.yanoshin.jp/tdnet/" target="_blank" style="color:#8B4513;">Yanoshin TDnet</a>. '
        'Covers earnings, dividends, buybacks, M&amp;A, guidance changes.</div>',
        unsafe_allow_html=True
    )

    if "filings" not in st.session_state:
        st.session_state.filings = []
    if "filings_last_fetch" not in st.session_state:
        st.session_state.filings_last_fetch = None

    # ── Controls: keyword filter + refresh only ──
    col_f1, col_f2 = st.columns([3, 1])
    with col_f1:
        keyword_filter = st.text_input("Filter by keyword or company:", key="filings_keyword",
                                       placeholder="e.g. Toyota, 決算, dividend")
    with col_f2:
        st.markdown("<div style='margin-top:1.55rem'>", unsafe_allow_html=True)
        fetch_filings_btn = st.button("🔄 Refresh", key="btn_filings", use_container_width=True)
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
            max_articles=80
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
            <th style="width:42px">JPN</th>
            <th style="width:42px">ENG</th>
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
            # English PDF: same filename but under /inbs_e/ instead of /inbs/
            import re as _re_pdf
            _eng_url  = _re_pdf.sub(r'/inbs/', '/inbs_e/', _jpn_url) if _jpn_url else ""
            # Only show English link if the URL was actually transformed
            _eng_link = (f'<a href="{_eng_url}" target="_blank" style="color:#1565C0;font-size:0.75rem;font-weight:600;">PDF ↗</a>'
                         if _eng_url and _eng_url != _jpn_url else
                         '<span style="color:#ccc;font-size:0.72rem;">—</span>')
            table_html += (
                "<tr>"
                f'<td style="font-family:monospace;font-size:0.75rem;white-space:nowrap;">{f.get("code","")}</td>'
                f'<td style="font-size:0.8rem;font-weight:600;white-space:nowrap;">{f.get("name_en") or f.get("name","")}</td>'
                f'<td style="font-size:0.8rem;">{_display_title}{_orig_note}</td>'
                f'<td style="font-size:0.72rem;color:#9B8B7A;white-space:nowrap;">{f.get("pub_date","")}</td>'
                f'<td style="text-align:center;">{_jpn_link}</td>'
                f'<td style="text-align:center;">{_eng_link}</td>'
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
    st.markdown(
        '<div class="info-box">Select a publication to browse its headlines from the last 14 days. '
        'Japanese sources are auto-translated. Older articles may not appear if the feed does not carry them.</div>',
        unsafe_allow_html=True
    )

    # Group selector
    group_names = list(SOURCE_GROUPS.keys())
    selected_group = st.selectbox(
        "Publication group:", group_names,
        index=group_names.index(st.session_state.source_group) if st.session_state.source_group in group_names else 0,
        label_visibility="collapsed", key="group_selector"
    )
    if selected_group != st.session_state.source_group:
        st.session_state.source_group = selected_group
        st.session_state.source_selected = None
        st.rerun()

    # Source selector within group
    sources_in_group = SOURCE_GROUPS.get(selected_group, [])
    # Only show sources that are in SOURCE_DIRECTORY
    available = [s for s in sources_in_group if s in SOURCE_DIRECTORY]

    if not available:
        st.markdown('<div class="info-box">No sources available in this group.</div>', unsafe_allow_html=True)
    else:
        # Default to first source in group if none selected or selection changed group
        if st.session_state.source_selected not in available:
            st.session_state.source_selected = available[0]

        selected_source = st.selectbox(
            "Publication:", available,
            index=available.index(st.session_state.source_selected),
            label_visibility="collapsed", key="source_selector"
        )
        if selected_source != st.session_state.source_selected:
            st.session_state.source_selected = selected_source
            st.rerun()

        # Fetch / cache button
        col_src_info, col_src_btn = st.columns([3, 1])
        cached = st.session_state.source_cache.get(selected_source)
        with col_src_info:
            if cached is not None:
                st.markdown(
                    '<div style="font-size:0.72rem;color:#9B8B7A;padding-top:0.35rem;">'
                    + str(len(cached)) + ' headlines loaded for ' + selected_source + '</div>',
                    unsafe_allow_html=True
                )
        with col_src_btn:
            if st.button("🔄 Load Headlines", use_container_width=True, key="load_source"):
                with st.spinner("Fetching " + selected_source + "..."):
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
                    trans  = a.get("translated_title", a.get("title", ""))
                    orig   = a.get("original_title", "")
                    url    = a.get("url", "#")
                    date   = a.get("pub_date", "")
                    hv     = a.get("high_value", False)
                    hv_tag = '<span class="high-value-tag">★ Corp Action</span>' if hv else ""
                    orig_p = '<div class="article-title-jp">' + orig + '</div>' if orig and orig != trans else ""
                    time_p = ""
                    if "·" in date:
                        time_p = '<div class="article-meta">' + date.split("·")[1].strip() + '</div>'
                    cards.append(
                        '<div class="article-card">'
                        '<div class="article-title"><a href="' + url + '" target="_blank">'
                        + trans + '</a>' + hv_tag + '</div>'
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
