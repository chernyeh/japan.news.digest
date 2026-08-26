"""
build_jpx400.py — turn JPX's published JPX-Nikkei Index 400 constituent PDF
into data/jpxnikkei400.csv.

JPX reviews the index every August and publishes the new constituent list as a
PDF (English edition: ".../<yyyymmdd>E_2.pdf"). There is no feed for it, so this
is run by hand once a year against the freshly downloaded file:

    python build_jpx400.py ~/Downloads/20260807E_2.pdf

The PDF is a plain two-column table of Code / Market Division / Issue, but the
text layer runs every cell together with no separators — "1332PNissui
Corporation3148PCREATE SD HOLDINGS CO.,LTD." — so entries are recovered by
matching the code+division prefix and taking everything up to the next one.

Only the extracted CSV is committed, never the PDF itself.
"""

import argparse
import csv
import re
import sys
import zlib

# A TSE code is four characters. Historically all digits; since 2024 the
# exchange also issues alphanumeric codes ending in a letter ("417A"), which a
# \d{4} pattern silently drops — two of the current 400 are of that form.
_CODE = r"\d{3}[0-9A-Z]"
# Market division: P)rime, S)tandard, G)rowth.
_ENTRY_RE = re.compile(rf"({_CODE})([PSG])(.+?)(?=(?:{_CODE})[PSG]|\n|$)")

# Page furniture to strip before parsing. The copyright line is encoded with
# CJK-range glyph substitution, so it survives as a run of unrelated kanji
# rather than readable ASCII, and would otherwise be swallowed into the last
# issue name on each page.
_HEADING_RE = re.compile(r"JPX-Nikkei Index 400 Constituents.*?reserved", re.S)
_COLUMN_HDR = "CodeMarket DivisionIssue"
_GLYPH_JUNK_RE = re.compile(r"[　-鿿＀-￯]{6,}.*$", re.S)

_APPLIED_RE = re.compile(r"applied on ([A-Z][a-z]+ \d{1,2}, \d{4})")


def extract_text(pdf_path: str) -> str:
    """Pull the text layer out of the PDF's Flate-compressed content streams.

    Deliberately dependency-free: this runs once a year on one known-shape
    file, and adding pypdf to requirements.txt for that would put a parser in
    the Streamlit image that nothing else needs."""
    data = open(pdf_path, "rb").read()
    if not data.startswith(b"%PDF"):
        raise SystemExit(f"{pdf_path} is not a PDF")

    out = []
    for m in re.finditer(rb"stream\r?\n", data):
        start = m.end()
        end = data.find(b"endstream", start)
        if end < 0:
            continue
        try:
            body = zlib.decompress(data[start:end])
        except zlib.error:
            continue
        if b"Tj" not in body and b"TJ" not in body:
            continue
        out.append(body)

    chunks = []
    token = re.compile(rb"\((?:[^()\\]|\\.)*\)|<[0-9A-Fa-f\s]+>|\bTd\b|\bTD\b|\bT\*\b")
    for body in out:
        for tm in token.finditer(body):
            tok = tm.group(0)
            if tok in (b"Td", b"TD", b"T*"):
                chunks.append("\n")
            elif tok.startswith(b"("):
                lit = tok[1:-1].replace(rb"\(", b"(").replace(rb"\)", b")")
                chunks.append(lit.decode("latin-1"))
            else:
                hexs = re.sub(rb"\s", b"", tok[1:-1])
                try:
                    chunks.append(bytes.fromhex(hexs.decode()).decode("utf-16-be", "replace"))
                except ValueError:
                    pass
    return "".join(chunks)


def parse_constituents(text: str):
    """[(code, market_div, name), ...] in the order the PDF lists them."""
    applied = _APPLIED_RE.search(text)
    applied_on = applied.group(1) if applied else ""

    body = _HEADING_RE.sub("", text).replace(_COLUMN_HDR, "\n")
    rows = []
    for code, div, name in _ENTRY_RE.findall(body):
        name = _GLYPH_JUNK_RE.sub("", name).strip()
        # A page number is left glued to the final name on each page.
        name = re.sub(r"\d$", "", name).strip()
        if name:
            rows.append((code, div, name))
    return rows, applied_on


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("pdf", help="JPX-Nikkei 400 constituent PDF (English edition)")
    ap.add_argument("-o", "--out", default="data/jpxnikkei400.csv")
    ap.add_argument("--expect", type=int, default=400,
                    help="constituent count to assert (0 to skip the check)")
    args = ap.parse_args()

    rows, applied_on = parse_constituents(extract_text(args.pdf))

    seen, unique = set(), []
    for code, div, name in rows:
        if code not in seen:
            seen.add(code)
            unique.append((code, div, name))

    if args.expect and len(unique) != args.expect:
        # Refuse to write a half-parsed index — a short list would silently
        # shrink the app's coverage rather than fail visibly.
        print(f"ERROR: parsed {len(unique)} constituents, expected {args.expect}. "
              f"The PDF layout has probably changed; inspect it before committing.",
              file=sys.stderr)
        return 1

    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["Code", "MarketDiv", "Name", "AppliedOn"])
        for code, div, name in unique:
            w.writerow([code, div, name, applied_on])

    divs = {}
    for _, div, _ in unique:
        divs[div] = divs.get(div, 0) + 1
    print(f"Wrote {len(unique)} constituents to {args.out} "
          f"(applied {applied_on or 'unknown'}; {divs})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
