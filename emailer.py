"""
emailer.py — v2
Handles email subscriptions and sends the daily digest via SendGrid.
Supports two scheduled editions:
  - Pre-market:    07:00 JST (06:00 MYT) — 2h before TSE open
  - Close-of-day: 19:00 JST (18:00 MYT) — 4h after TSE close
Each edition includes: market data, AI briefing, filings summary, sector news.
"""

import os, json
from datetime import datetime

SUBSCRIBERS_FILE = "subscribers.json"
# NOTE: Streamlit Community Cloud has an ephemeral filesystem — subscribers.json
# is lost on every app restart/redeploy. For persistent storage, add a
# SUBSCRIBER_EMAILS secret (comma-separated emails) as a seed list, e.g.:
#   SUBSCRIBER_EMAILS = "you@email.com,other@email.com"
# Any emails added via the Subscribe form during the session are kept in memory
# but won't survive a restart unless the secret is updated.

SECTOR_ICONS = {
    "Energy": "⛽", "Materials": "🏭", "Industrials": "⚙️",
    "Consumer Discretionary": "🛍️", "Consumer Staples": "🛒",
    "Health Care": "💊", "Financials": "🏦",
    "Information Technology": "💻", "Communication Services": "📡",
    "Utilities": "💡", "Real Estate": "🏢", "General / Macro": "🗾",
}


def get_secret(key: str, default: str = "") -> str:
    try:
        import streamlit as st
        return st.secrets.get(key, os.environ.get(key, default))
    except Exception:
        return os.environ.get(key, default)


def load_subscribers() -> list:
    """Load subscribers from file + SUBSCRIBER_EMAILS secret as seed."""
    subscribers = []
    # 1. Load from file (works locally, lost on Streamlit Cloud restart)
    if os.path.exists(SUBSCRIBERS_FILE):
        try:
            with open(SUBSCRIBERS_FILE, "r") as f:
                subscribers = json.load(f)
        except Exception:
            subscribers = []
    # 2. Merge in any emails from the SUBSCRIBER_EMAILS secret
    seed = get_secret("SUBSCRIBER_EMAILS", "")
    for email in seed.split(","):
        email = email.strip()
        if email and "@" in email and email not in subscribers:
            subscribers.append(email)
    return subscribers


def save_subscribers(subscribers: list):
    with open(SUBSCRIBERS_FILE, "w") as f:
        json.dump(subscribers, f, indent=2)


def subscribe_email(email: str) -> bool:
    try:
        subs = load_subscribers()
        if email not in subs:
            subs.append(email)
            save_subscribers(subs)
        return True
    except Exception as e:
        print(f"Subscribe error: {e}")
        return False


def unsubscribe_email(email: str) -> bool:
    subs = load_subscribers()
    if email in subs:
        subs.remove(email)
        save_subscribers(subs)
    return True


# ── AI Briefing helper ────────────────────────────────────────────────────────

def generate_ai_briefing(articles: list, context: str, api_key: str) -> str:
    """Generate an AI briefing for inclusion in the email."""
    try:
        import anthropic
        subset = articles[:60]
        lines  = []
        for i, a in enumerate(subset, 1):
            title  = a.get("title") or a.get("translated_title") or a.get("original_title", "")
            url    = a.get("url", "")
            source = a.get("source", "")
            pub    = a.get("pub_date", "")
            lines.append(f"{i}. [{source}] {title} | {pub} | {url}")

        prompt = f"""You are an analyst briefing a Malaysian investor on Japan business news.

Headlines from {context}:
{chr(10).join(lines)}

Write a complete briefing covering all significant stories that:
1. Opens with 1-2 sentences on the day's overall theme
2. Groups key stories into 3-5 clusters with ## headers
3. Each cluster has 2-4 bullet points with [Source](url) links
4. Closes with 1-2 sentences on what to watch

Use markdown. Be factual. No filler."""

        client = anthropic.Anthropic(api_key=api_key)
        msg    = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}]
        )
        return msg.content[0].text
    except Exception as e:
        return f"AI briefing unavailable: {e}"


# ── Market data formatter ─────────────────────────────────────────────────────

def fmt_market_html(market_data: dict) -> str:
    if not market_data or market_data.get("_source") == "error":
        return "<p style='color:#999;font-size:12px;'>Market data unavailable.</p>"

    indices = market_data.get("indices", {})
    forex   = market_data.get("forex", {})

    def cell(label, data, is_fx=False):
        if not data or data.get("price", 0) == 0:
            return f"<td style='padding:6px 10px;border-right:1px solid #eee;'><b>{label}</b><br><span style='color:#999;font-size:11px;'>N/A</span></td>"
        p   = data["price"]
        pct = data.get("pct_change", 0)
        col = "#2E7D32" if pct >= 0 else "#C62828"
        arr = "▲" if pct >= 0 else "▼"
        fmt = f"{p:,.2f}" if is_fx else (f"{p:,.0f}" if p >= 1000 else f"{p:,.2f}")
        return (
            f"<td style='padding:6px 10px;border-right:1px solid #eee;text-align:center;'>"
            f"<div style='font-size:10px;color:#666;font-weight:700;text-transform:uppercase;letter-spacing:1px;'>{label}</div>"
            f"<div style='font-size:16px;font-weight:700;'>{fmt}</div>"
            f"<div style='font-size:11px;color:{col};font-weight:600;'>{arr} {abs(pct):.2f}%</div>"
            f"</td>"
        )

    idx_order = ["nikkei","topix","topix_c30"]
    fx_order  = ["usdjpy","eurjpy","cnyjpy","sgdjpy"]

    idx_cells = "".join(cell(indices[k]["label"].split("(")[0].strip(), indices[k]) for k in idx_order if k in indices)
    fx_cells  = "".join(cell(forex[k]["label"],   forex[k], is_fx=True)  for k in fx_order  if k in forex)

    return f"""
<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #eee;border-radius:4px;margin-bottom:16px;">
<tr style="background:#F5F0EA;">{idx_cells}</tr>
<tr style="background:#FDFAF7;">{fx_cells}</tr>
</table>"""


# ── Markdown → simple HTML converter ─────────────────────────────────────────

def md_to_html(text: str) -> str:
    """Minimal markdown → HTML for email use."""
    import re
    lines, out, in_ul = text.split("\n"), [], False
    for line in lines:
        line = re.sub(r'\[([^\]]+)\]\(([^)]+)\)',
                      r'<a href="\2" style="color:#8B4513;">\1</a>', line)
        if line.startswith("## "):
            if in_ul: out.append("</ul>"); in_ul = False
            out.append(f'<h3 style="font-family:Georgia,serif;font-size:14px;border-bottom:1px solid #eee;padding-bottom:4px;margin:14px 0 6px 0;color:#1A1A1A;">{line[3:]}</h3>')
        elif line.startswith("# "):
            if in_ul: out.append("</ul>"); in_ul = False
            out.append(f'<h2 style="font-size:16px;font-weight:900;margin:10px 0 4px;">{line[2:]}</h2>')
        elif line.startswith("- ") or line.startswith("* "):
            if not in_ul: out.append('<ul style="margin:0 0 6px 18px;padding:0;">'); in_ul = True
            out.append(f'<li style="margin-bottom:4px;font-size:13px;line-height:1.5;">{line[2:]}</li>')
        else:
            if in_ul: out.append("</ul>"); in_ul = False
            if line.strip():
                out.append(f'<p style="font-size:13px;margin:6px 0;line-height:1.6;">{line}</p>')
    if in_ul: out.append("</ul>")
    return "\n".join(out)


# ── Email builder ─────────────────────────────────────────────────────────────

def build_html_email(
    articles_by_sector: dict,
    edition: str = "close",          # "premarket" or "close"
    market_data: dict = None,
    filings: list = None,
    ai_briefing: str = None,
) -> str:

    now_jst  = datetime.utcnow()  # approximation; real JST would use pytz
    today    = datetime.now().strftime("%B %d, %Y")
    ed_label = "🌅 Pre-Market Edition" if edition == "premarket" else "🌆 Close-of-Day Edition"
    ed_time  = "07:00 JST (06:00 MYT)" if edition == "premarket" else "19:00 JST (18:00 MYT)"

    # ── Market data ──
    mkt_html = fmt_market_html(market_data) if market_data else ""

    # ── AI Briefing ──
    briefing_html = ""
    if ai_briefing:
        briefing_html = f"""
<tr><td style="padding:16px 0 8px;">
  <div style="font-size:15px;font-weight:700;font-family:Georgia,serif;border-bottom:2px solid #1A1A1A;padding-bottom:4px;margin-bottom:8px;">📰 News Briefing</div>
  <div style="background:#FDFAF7;border-left:3px solid #8B4513;padding:10px 14px;border-radius:2px;">
    {md_to_html(ai_briefing)}
  </div>
</td></tr>"""

    # ── Filings summary ──
    filings_html = ""
    if filings:
        HIGH_VALUE = ["決算","配当","買収","合併","業績","上方修正","下方修正","自社株買い",
                      "earnings","dividend","acquisition","merger","buyback","forecast"]
        hv = [f for f in filings if any(t in f.get("title","").lower() for t in HIGH_VALUE)][:20]
        if hv:
            rows = "".join(
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:5px 6px;font-family:monospace;font-size:11px;color:#666;">{f["code"]}</td>'
                f'<td style="padding:5px 6px;font-size:12px;font-weight:600;">{f["name"]}</td>'
                f'<td style="padding:5px 6px;font-size:12px;">'
                + (f'<a href="{f["doc_url"]}" style="color:#8B4513;">{f["title"]}</a>' if f.get("doc_url") else f['title'])
                + f'</td><td style="padding:5px 6px;font-size:11px;color:#999;">{f["pub_date"]}</td></tr>'
                for f in hv
            )
            filings_html = f"""
<tr><td style="padding:16px 0 8px;">
  <div style="font-size:15px;font-weight:700;font-family:Georgia,serif;border-bottom:2px solid #1A1A1A;padding-bottom:4px;margin-bottom:8px;">📋 Key Corporate Filings Today</div>
  <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-size:12px;">
  <tr style="background:#F5F0EA;"><th style="padding:5px 6px;text-align:left;font-size:10px;letter-spacing:1px;text-transform:uppercase;">Code</th><th style="padding:5px 6px;text-align:left;">Company</th><th style="padding:5px 6px;text-align:left;">Filing</th><th style="padding:5px 6px;text-align:left;">Time</th></tr>
  {rows}
  </table>
  <p style="font-size:10px;color:#999;margin-top:6px;">Showing earnings, dividends, buybacks, M&A, guidance changes only. Full list at <a href="https://webapi.yanoshin.jp/webapi/tdnet/list/today.html" style="color:#8B4513;">TDnet</a>.</p>
</td></tr>"""

    # ── Sector news ──
    total = sum(len(v) for v in articles_by_sector.values())
    sectors_html = ""
    for sector, arts in articles_by_sector.items():
        if not arts:
            continue
        icon = SECTOR_ICONS.get(sector, "📰")
        top  = arts[:6]
        rows = "".join(
            f'<tr><td style="padding:7px 0;border-bottom:1px solid #f0f0f0;">'
            f'<div style="font-size:10px;font-weight:700;color:#8B4513;text-transform:uppercase;letter-spacing:1px;margin-bottom:2px;">{a.get("source","")}</div>'
            f'<a href="{a.get("url","#")}" style="font-size:13px;font-weight:600;color:#1A1A1A;text-decoration:none;line-height:1.4;">'
            f'{a.get("title") or a.get("translated_title","")}</a>'
            + (f'<div style="font-size:11px;color:#999;margin-top:2px;">{a.get("original_title","")}</div>'
               if a.get("original_title") and a.get("original_title") != (a.get("title") or a.get("translated_title","")) else "")
            + f'</td></tr>'
            for a in top
        )
        sectors_html += f"""
<tr><td style="padding:14px 0 6px;">
  <div style="font-size:15px;font-weight:700;font-family:Georgia,serif;border-bottom:2px solid #1A1A1A;padding-bottom:4px;margin-bottom:4px;">{icon} {sector}</div>
  <div style="font-size:10px;letter-spacing:1px;text-transform:uppercase;color:#999;margin-bottom:4px;">{len(arts)} articles</div>
  <table width="100%" cellpadding="0" cellspacing="0">{rows}</table>
</td></tr>"""

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#F7F4EF;font-family:Helvetica,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#F7F4EF;">
<tr><td align="center" style="padding:20px;">
<table width="620" cellpadding="0" cellspacing="0" style="max-width:620px;width:100%;">
  <tr><td style="border-top:4px solid #1A1A1A;border-bottom:1px solid #1A1A1A;padding:16px 0;text-align:center;">
    <div style="font-size:26px;font-weight:900;font-family:Georgia,serif;letter-spacing:-1px;">Japan Investment Digest</div>
    <div style="font-size:11px;font-weight:700;letter-spacing:3px;text-transform:uppercase;color:#6B6B6B;margin-top:4px;">{ed_label} · {ed_time}</div>
    <div style="font-size:12px;color:#6B6B6B;margin-top:4px;">{today}</div>
  </td></tr>
  <tr><td style="background:#1A1A1A;color:#F7F4EF;text-align:center;padding:7px;font-size:10px;letter-spacing:2px;text-transform:uppercase;font-weight:700;">
    {total} HEADLINES · {sum(1 for v in articles_by_sector.values() if v)} SECTORS
  </td></tr>
  <tr><td style="padding:14px 0 4px;">{mkt_html}</td></tr>
  {briefing_html}
  {filings_html}
  {sectors_html}
  <tr><td style="border-top:1px solid #D9D3C8;padding:14px 0;text-align:center;font-size:10px;color:#9B8B7A;letter-spacing:1px;">
    JAPAN INVESTMENT DIGEST · {ed_label.upper()}<br>
    For informational purposes only · Not financial advice
  </td></tr>
</table></td></tr></table>
</body></html>"""


def send_digest(
    articles_by_sector: dict,
    recipients: list = None,
    edition: str = "close",
    market_data: dict = None,
    filings: list = None,
    generate_ai: bool = True,
):
    if recipients is None:
        recipients = load_subscribers()
    if not recipients:
        print("No recipients.")
        return False

    ai_briefing = None
    if generate_ai:
        api_key = get_secret("ANTHROPIC_API_KEY")
        if api_key:
            all_articles = [a for arts in articles_by_sector.values() for a in arts]
            context = "today's Japan business news"
            ai_briefing = generate_ai_briefing(all_articles, context, api_key)

    html_content = build_html_email(
        articles_by_sector,
        edition=edition,
        market_data=market_data,
        filings=filings,
        ai_briefing=ai_briefing,
    )
    ed_label = "Pre-Market" if edition == "premarket" else "Close-of-Day"
    subject  = f"Japan Investment Digest — {ed_label} · {datetime.now().strftime('%B %d, %Y')}"

    sendgrid_key = get_secret("SENDGRID_API_KEY")
    sender_email = get_secret("DIGEST_FROM_EMAIL", "digest@yourdomain.com")

    if not sendgrid_key:
        print("No SENDGRID_API_KEY found.")
        return False

    try:
        import sendgrid
        from sendgrid.helpers.mail import Mail, Email, To, Content
        sg = sendgrid.SendGridAPIClient(api_key=sendgrid_key)
        for recipient in recipients:
            message = Mail(
                from_email=Email(sender_email, "Japan Investment Digest"),
                to_emails=To(recipient),
                subject=subject,
                html_content=Content("text/html", html_content),
            )
            response = sg.send(message)
            print(f"Sent to {recipient}: {response.status_code}")
        return True
    except ImportError:
        print("SendGrid not installed.")
        return False
    except Exception as e:
        print(f"SendGrid error: {e}")
        return False
