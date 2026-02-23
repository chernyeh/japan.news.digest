"""
emailer.py
Handles email subscriptions and sends the daily digest.
Uses SendGrid (free tier: 100 emails/day) or prints to console if not configured.
"""

import os
import json
from datetime import datetime

# ── Subscriber storage (local file — replace with DB for production) ──────────
SUBSCRIBERS_FILE = "subscribers.json"

# ── MSCI sector icons for email ───────────────────────────────────────────────
SECTOR_ICONS = {
    "Energy": "⛽",
    "Materials": "🏭",
    "Industrials": "⚙️",
    "Consumer Discretionary": "🛍️",
    "Consumer Staples": "🛒",
    "Health Care": "💊",
    "Financials": "🏦",
    "Information Technology": "💻",
    "Communication Services": "📡",
    "Utilities": "💡",
    "Real Estate": "🏢",
    "General / Macro": "🗾",
}


def load_subscribers() -> list:
    """Load subscriber list from local JSON file."""
    if os.path.exists(SUBSCRIBERS_FILE):
        try:
            with open(SUBSCRIBERS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def save_subscribers(subscribers: list):
    """Save subscriber list to local JSON file."""
    with open(SUBSCRIBERS_FILE, "w") as f:
        json.dump(subscribers, f, indent=2)


def subscribe_email(email: str) -> bool:
    """Add email to subscriber list. Returns True if successful."""
    subscribers = load_subscribers()
    if email not in subscribers:
        subscribers.append(email)
        save_subscribers(subscribers)
        print(f"Subscribed: {email}")
    return True


def unsubscribe_email(email: str) -> bool:
    """Remove email from subscriber list."""
    subscribers = load_subscribers()
    if email in subscribers:
        subscribers.remove(email)
        save_subscribers(subscribers)
    return True


def build_html_email(articles_by_sector: dict) -> str:
    """Build a clean HTML email with all articles organized by MSCI sector."""
    today = datetime.now().strftime("%B %d, %Y")
    total = sum(len(v) for v in articles_by_sector.values())

    # Build sector sections
    sectors_html = ""
    for sector, articles in articles_by_sector.items():
        if not articles:
            continue
        icon = SECTOR_ICONS.get(sector, "📰")
        articles_html = ""
        for a in articles[:10]:  # max 10 per sector in email
            title = a.get("translated_title") or a.get("title", "")
            url = a.get("url", "#")
            source = a.get("source", "")
            original = a.get("original_title", "")
            orig_html = f'<div style="font-size:11px;color:#999;margin-top:2px;">{original}</div>' if original else ""

            articles_html += f"""
            <tr>
                <td style="padding:10px 0;border-bottom:1px solid #eee;">
                    <div style="font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:#8B4513;margin-bottom:3px;">{source}</div>
                    <a href="{url}" style="font-size:14px;font-weight:600;color:#1A1A1A;text-decoration:none;line-height:1.4;">{title}</a>
                    {orig_html}
                </td>
            </tr>"""

        sectors_html += f"""
        <tr>
            <td style="padding:24px 0 8px 0;">
                <div style="font-size:18px;font-weight:700;font-family:Georgia,serif;border-bottom:2px solid #1A1A1A;padding-bottom:6px;margin-bottom:4px;">
                    {icon} {sector}
                </div>
                <div style="font-size:10px;letter-spacing:1px;text-transform:uppercase;color:#999;margin-bottom:8px;">{len(articles)} articles</div>
                <table width="100%" cellpadding="0" cellspacing="0">{articles_html}</table>
            </td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#F7F4EF;font-family:'Source Sans 3',Helvetica,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#F7F4EF;">
<tr><td align="center" style="padding:20px;">
<table width="600" cellpadding="0" cellspacing="0" style="background:#F7F4EF;max-width:600px;">

  <!-- Header -->
  <tr>
    <td style="border-top:4px solid #1A1A1A;border-bottom:1px solid #1A1A1A;padding:20px 0;text-align:center;">
      <div style="font-size:32px;font-weight:900;font-family:Georgia,serif;letter-spacing:-1px;">Japan Business Digest</div>
      <div style="font-size:10px;font-weight:700;letter-spacing:3px;text-transform:uppercase;color:#6B6B6B;margin-top:4px;">日本経済ニュース — MSCI Sector Edition</div>
      <div style="font-size:12px;color:#6B6B6B;margin-top:4px;">{today}</div>
    </td>
  </tr>

  <!-- Stats bar -->
  <tr>
    <td style="background:#1A1A1A;color:#F7F4EF;text-align:center;padding:8px;font-size:10px;letter-spacing:2px;text-transform:uppercase;font-weight:700;">
      {total} HEADLINES · {sum(1 for v in articles_by_sector.values() if v)} SECTORS
    </td>
  </tr>

  <!-- Articles -->
  {sectors_html}

  <!-- Footer -->
  <tr>
    <td style="border-top:1px solid #D9D3C8;padding:20px 0;text-align:center;font-size:10px;color:#9B8B7A;letter-spacing:1px;">
      JAPAN BUSINESS DIGEST · RSS EDITION<br>
      Sources include Japan Times, Asahi, NHK, Reuters Japan, Nikkei Asia, Yahoo Japan & more<br><br>
      <a href="{{unsubscribe_link}}" style="color:#9B8B7A;">Unsubscribe</a>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""
    return html


def send_digest(articles_by_sector: dict, recipients: list = None):
    """
    Send the daily digest email.
    Uses SendGrid if SENDGRID_API_KEY is set, otherwise prints to console.
    """
    if recipients is None:
        recipients = load_subscribers()

    if not recipients:
        print("No subscribers to send to.")
        return

    html_content = build_html_email(articles_by_sector)
    subject = f"Japan Business Digest — {datetime.now().strftime('%B %d, %Y')}"

    sendgrid_key = os.environ.get("SENDGRID_API_KEY", "")
    sender_email = os.environ.get("DIGEST_FROM_EMAIL", "digest@yourdomain.com")

    if sendgrid_key:
        # ── Send via SendGrid ─────────────────────────────────────────────────
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
                print(f"Email sent to {recipient}: {response.status_code}")

        except ImportError:
            print("SendGrid not installed. Run: pip install sendgrid")
        except Exception as e:
            print(f"SendGrid error: {e}")

    else:
        # ── No API key: save digest to file for inspection ────────────────────
        output_file = f"digest_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"[No SENDGRID_API_KEY] Digest saved to: {output_file}")
        print(f"Would have sent to: {', '.join(recipients)}")
        print("To enable email delivery, add SENDGRID_API_KEY to your Streamlit secrets.")


if __name__ == "__main__":
    # Test subscriber management
    subscribe_email("test@example.com")
    subs = load_subscribers()
    print(f"Subscribers: {subs}")
