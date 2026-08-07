"""
translate.py — shared Google Translate helper for standalone collector scripts
(collect_edinet.py, collect_tdnet.py). Deliberately self-contained (just
`requests`) rather than importing collector.py's translate_single_google,
which would drag cloudscraper/feedparser into workflows that don't need them.
"""

import requests

USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def translate_ja_to_en(text: str) -> str:
    """Translate using Google Translate's free unofficial endpoint. No API
    key, no daily cap. Falls back to the original text on any error."""
    if not text:
        return text
    try:
        params = {"client": "gtx", "sl": "ja", "tl": "en", "dt": "t", "q": text[:500]}
        resp = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params=params, headers={"User-Agent": USER_AGENT}, timeout=8,
        )
        if resp.status_code == 200:
            data = resp.json()
            translated = "".join(seg[0] for seg in data[0] if seg[0])
            if translated and translated != text:
                return translated.strip()
    except Exception as e:
        print(f"Translate error: {e}")
    return text
