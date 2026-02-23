"""
emailer.py
Handles email subscriptions and sends the daily digest via SendGrid.
Reads API keys from Streamlit secrets (st.secrets) with os.environ fallback.
"""

import os
import json
from datetime import datetime

SUBSCRIBERS_FILE = "subscribers.json"

SECTOR_ICONS = {
    "Energy": "⛽", "Materials": "🏭", "Industrials": "⚙️",
    "Consumer Discretionary": "🛍️", "Consumer Staples": "🛒",
    "Health Care": "💊", "Financials": "🏦",
    "Information Technology": "💻", "Communication Services": "📡",
    "Utilities": "💡", "Real Estate": "🏢", "General / Macro": "🗾",
}


def get_secret(key: str, default: str = "") -> str:
    """
    Read a secret from Streamlit secrets first, then fall back to environment variables.
    This ensures it works both on Streamlit Cloud and locally.
    """
    try:
        import streamlit as st
        return st.secrets.get(key, os.environ.get(key, default))
    except Exception:
        return os.environ.get(key, default)


def load_subscribers() -> list:
    if os.path.exists(SUBSCRIBERS_FILE):
        try:
            with open(SUBSCRIBERS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def save_subscribers(subscribers: list):
    with open(SUBSCRIBERS_FILE, "w") as f:
        json.dump(subscribers, f, indent=2)


def subscribe_email(email: str) -> bool:
    """Add email to subscriber list."""
    try:
        subscribers = load_subscribers()
        if email not in subscribers:
            subscribers.append(email)
            save_subscribers(subscribers)
        return True
    except Exception as e:
        print(f"Subscribe error: {e}")
        return False


def unsubscribe_email(email: str) -> bool:
    subscribers = load_subscribers()
    if email in subscribers:
        subscribers.remove(email)
        save_subscribers(subscribers)
    return True


def build_html_email(articles_by_sector: dict) -> str:
    """Build formatted HTML digest email."""
    today = datetime.now().strftime("%B %d, %Y")
    total = sum(len(v) for v in articles_by_sector.values())

    sectors_html = ""
    for sector, articles in articles_by_sector.items():
        if not articles:
            continue
        icon = SECTOR_ICONS.get(sector, "📰")
        rows = ""
        for a in articles[:10]:
            title  = a.get("translated_title") or a.get("title", "")
            url    = a.get("url", "#")
            source = a.get("source", "")
            orig   = a.get("original_title", "")
            orig_html = (
                f'<div style="font-size:11px;color:#999;margin-top:2px;">{orig}</div>'
                if orig and orig != title else ""
            )
            rows += f"""
            <tr>
              <td style="padding:10px 0;border-bottom:1px solid #eee;">
                <div style="font-size:10px;font-weight:700;letter-spacing:1px;
                            text-transform:uppercase;color:#8B4513;margin-bottom:3px;">{source}</div>
                <a href="{url}" style="font-size:14px;font-weight:600;color:#1A1A1A;
                                       text-decoration:none;line-height:1.4;">{title}</a>
                {orig_html}
              </td>
            </tr>"""

        sectors_html += f"""
        <tr>
          <td style="padding:20px 0 8px 0;">
            <div style="font-size:17px;font-weight:700;font-family:Georgia,serif;
                        border-bottom:2px solid #1A1A1A;padding-bottom:5px;margin-bottom:4px;">
              {icon} {sector}
            </div>
            <div style="font-size:10px;letter-spacing:1px;text-transform:uppercase;
                        color:#999;margin-bottom:6px;">{len(articles)} articles</div>
            <table width="100%" cellpadding="0" cellspacing="0">{rows}</table>
          </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#F7F4EF;font-family:Helvetica,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#F7F4EF;">
<tr><td align="center" style="padding:20px;">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;">
  <tr>
    <td style="border-top:4px solid #1A1A1A;border-bottom:1px solid #1A1A1A;
               padding:18px 0;text-align:center;">
      <div style="font-size:28px;font-weight:900;font-family:Georgia,serif;
                  letter-spacing:-1px;">Japan Business Digest</div>
      <div style="font-size:10px;font-weight:700;letter-spacing:3px;
                  text-transform:uppercase;color:#6B6B6B;margin-top:4px;">
        日本経済ニュース — MSCI Sector Edition
      </div>
      <div style="font-size:12px;color:#6B6B6B;margin-top:4px;">{today}</div>
    </td>
  </tr>
  <tr>
    <td style="background:#1A1A1A;color:#F7F4EF;text-align:center;
               padding:7px;font-size:10px;letter-spacing:2px;
               text-transform:uppercase;font-weight:700;">
      {total} HEADLINES · {sum(1 for v in articles_by_sector.values() if v)} SECTORS
    </td>
  </tr>
  {sectors_html}
  <tr>
    <td style="border-top:1px solid #D9D3C8;padding:16px 0;text-align:center;
               font-size:10px;color:#9B8B7A;letter-spacing:1px;">
      JAPAN BUSINESS DIGEST · RSS EDITION<br>
      Japan Times · Nikkei Asia · Reuters · NHK · Asahi · Yahoo Japan & more
    </td>
  </tr>
</table>
</td></tr>
</table>
</body>
</html>"""


def send_digest(articles_by_sector: dict, recipients: list = None):
    """Send the digest email via SendGrid."""
    if recipients is None:
        recipients = load_subscribers()
    if not recipients:
        print("No recipients.")
        return False

    html_content = build_html_email(articles_by_sector)
    subject = f"Japan Business Digest — {datetime.now().strftime('%B %d, %Y')}"

    sendgrid_key = get_secret("SENDGRID_API_KEY")
    sender_email = get_secret("DIGEST_FROM_EMAIL", "digest@yourdomain.com")

    if not sendgrid_key:
        print("No SENDGRID_API_KEY found in Streamlit secrets or environment.")
        return False

    try:
        import sendgrid
        from sendgrid.helpers.mail import Mail, Email, To, Content

        sg = sendgrid.SendGridAPIClient(api_key=sendgrid_key)

        for recipient in recipients:
            message = Mail(
                from_email=Email(sender_email, "Japan Business Digest"),
                to_emails=To(recipient),
                subject=subject,
                html_content=Content("text/html", html_content),
            )
            response = sg.send(message)
            print(f"Sent to {recipient}: status {response.status_code}")

        return True

    except ImportError:
        print("SendGrid package not installed.")
        return False
    except Exception as e:
        print(f"SendGrid error: {e}")
        return False
