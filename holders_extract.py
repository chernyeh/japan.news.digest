"""
holders_extract.py — who crossed 5%, and how shareholders voted.

Two filings the EDINET index already lists for every company, read on the
same button press as the yuho:

  大量保有報告書 / 変更報告書 (docTypeCode 350; corrections 360)
      Filed by whoever crosses 5% of a listed company, and again on every
      1% move. The index alone says who and when: the filer *is* the holder.
      A "（特例対象株券等）" filing is a passive institution reporting on the
      relaxed schedule (trust banks, asset managers); the ones without it are
      the general filers, which is where activists, strategic buyers and
      founders show up. The percentage and the stated purpose need the
      filing's own data (type=5 CSV), fetched only when asked.

  臨時報告書 (docTypeCode 180) filed in the days after the AGM
      The voting results: for / against / abstain and the approval rate for
      every resolution — which, for director elections, is the one number
      that quantifies shareholder dissent, person by person. An extraordinary
      report is filed for many reasons, so the candidates are the ones in the
      AGM window after the fiscal year end whose text has a voting table.

Only findings are stored (data/holders_votes.json), never the documents.
"""

import re
from datetime import date, timedelta

import models
import capital_events as CE
import yuho_extract as YX

STORE_PATH = "data/holders_votes.json"
LARGE_HOLDING = ("350", "360")
EXTRAORDINARY = "180"

# Names that mean "activist" when they appear as the filer. Matched as
# substrings of the filer name, case-insensitively, in either script. Kept
# deliberately short and well-known; a miss is shown as a plain general
# filer, which is still visible.
ACTIVISTS = (
    ("エフィッシモ", "Effissimo"), ("オアシス", "Oasis"), ("エリオット", "Elliott"),
    ("３Ｄインベストメント", "3D Investment"), ("3Dインベストメント", "3D Investment"),
    ("ダルトン", "Dalton"), ("ストラテジックキャピタル", "Strategic Capital"),
    ("シティインデックスイレブンス", "Murakami-linked (City Index Eleventh)"),
    ("エスグラント", "Murakami-linked (S-Grant)"), ("南青山不動産", "Murakami-linked"),
    ("野村絢", "Murakami-linked"), ("村上世彰", "Murakami"), ("レノ", "Murakami-linked (Reno)"),
    ("バリューアクト", "ValueAct"), ("ひびき", "Hibiki Path"), ("ニッポン・アクティブ・バリュー", "NAVF"),
    ("アセット・バリュー・インベスターズ", "AVI"), ("Asset Value Investors", "AVI"),
    ("カナメキャピタル", "Kaname Capital"), ("パリサー", "Palliser"), ("シルチェスター", "Silchester"),
    ("ファラロン", "Farallon"), ("ブラックロック", ""),  # BlackRock: passive; listed so it is never mis-tagged
)


def _desc_kind(desc: str) -> str:
    d = desc or ""
    if "訂正" in d:
        return "correction"
    if "変更報告書" in d:
        return "change"
    if "大量保有報告書" in d:
        return "new"
    return "other"


def activist_label(filer: str) -> str:
    f = (filer or "")
    low = f.lower()
    for key, label in ACTIVISTS:
        if key.lower() in low:
            return label
    return ""


def large_holding_rows(edinet_rows: list, code: str) -> list:
    """Every 5%-rule filing for `code` in the index, newest first, with what
    the index alone can say. No fetch."""
    out = []
    for r in edinet_rows or ():
        if str(r.get("DocTypeCode") or "") not in LARGE_HOLDING:
            continue
        if str(r.get("SecCode") or "")[:4] != str(code)[:4]:
            continue
        desc = r.get("DocDescription") or ""
        out.append({
            "id": f"lh_{r.get('DocID', '')}", "doc_id": r.get("DocID", ""),
            "date": (r.get("SubmitDateTime") or "")[:10],
            "holder": r.get("FilerName", ""), "kind": _desc_kind(desc),
            "special": "特例" in desc, "activist": activist_label(r.get("FilerName", "")),
            "description": desc,
        })
    return sorted(out, key=lambda x: (x["date"], x["doc_id"]), reverse=True)


def holders_summary(rows: list) -> list:
    """One line per holder: their latest filing, how many, and whether they
    are a general (non-特例) filer. Newest first."""
    seen = {}
    for r in rows:
        h = r["holder"]
        s = seen.setdefault(h, {"holder": h, "filings": 0, "latest": r["date"],
                               "first": r["date"], "special": r["special"],
                               "activist": r["activist"], "latest_kind": r["kind"],
                               "pct": None, "prev_pct": None, "purpose": ""})
        s["filings"] += 1
        s["first"] = min(s["first"], r["date"])
        if r.get("pct") is not None and (s["pct"] is None or r["date"] >= s["latest"]):
            s["pct"], s["prev_pct"], s["purpose"] = r["pct"], r.get("prev_pct"), r.get("purpose", "")
    return sorted(seen.values(), key=lambda s: s["latest"], reverse=True)


# ── The filing's own numbers (one fetch per filing) ──────────────────────

def _pick(rows: list, label_parts, el_parts=(), want_num=True):
    for row in rows:
        lbl = row.get("label") or ""
        el = row.get("element") or ""
        if all(p in lbl for p in label_parts) or (el_parts and any(p in el for p in el_parts)):
            v = row.get("value") or ""
            if want_num:
                n = YX._num(v)
                if n is not None:
                    return n
            elif v.strip():
                return v.strip()
    return None


def parse_large_holding(rows: list) -> dict:
    """{pct, prev_pct, purpose, holder, event_date} from a 5%-rule filing's
    tagged values. Percentages are stated as percent in the filing."""
    pct = _pick(rows, ("株券等保有割合",), ("HoldingRatioOfShareCertificatesEtc",))
    prev = _pick(rows, ("直前", "保有割合"), ("HoldingRatioOfShareCertificatesEtcPerLastReport",))
    purpose = _pick(rows, ("保有目的",), ("PurposeOfHolding",), want_num=False)
    holder = _pick(rows, ("氏名又は名称",), ("NameOfFiler", "Name"), want_num=False)
    when = _pick(rows, ("報告義務発生日",), ("DateWhenFilingRequirementArose",), want_num=False)
    return {"pct": pct, "prev_pct": prev, "purpose": (purpose or "")[:300],
            "holder": holder or "", "event_date": when or ""}


def purpose_read(purpose: str) -> str:
    p = purpose or ""
    if "重要提案行為" in p:
        return "activist (重要提案行為)"
    if "純投資" in p:
        return "pure investment"
    if "政策投資" in p or "業務提携" in p or "取引関係" in p:
        return "strategic"
    return "other" if p else ""


# ── The AGM vote ─────────────────────────────────────────────────────────

def agm_candidates(edinet_rows: list, code: str, period_end: str,
                   window=(60, 125)) -> list:
    """Extraordinary reports filed in the AGM window after `period_end`
    (Japanese AGMs sit within three months of year end; the results report
    follows within days). Newest first."""
    try:
        pe = date.fromisoformat(str(period_end)[:10])
    except (TypeError, ValueError):
        return []
    lo, hi = pe + timedelta(days=window[0]), pe + timedelta(days=window[1])
    out = []
    for r in edinet_rows or ():
        if str(r.get("DocTypeCode") or "") != EXTRAORDINARY:
            continue
        if str(r.get("SecCode") or "")[:4] != str(code)[:4]:
            continue
        d = (r.get("SubmitDateTime") or "")[:10]
        try:
            dd = date.fromisoformat(d)
        except ValueError:
            continue
        if lo <= dd <= hi:
            out.append(r)
    return sorted(out, key=lambda r: r.get("SubmitDateTime") or "", reverse=True)


def vote_text(rows: list) -> str:
    """The text of the report if it carries a voting table, else ''."""
    parts = []
    for row in rows:
        v = row.get("value") or ""
        if "議決権" in v and ("賛成" in v or "可決" in v):
            parts.append(YX.html_to_text(v) if "<" in v else v)
    text = "\n".join(parts)
    return text if ("賛成" in text and "議決権" in text) else ""


RES_KINDS = ("director_election", "auditor_election", "dividend", "articles_amendment",
             "compensation", "buyback", "merger_or_restructuring", "shareholder_proposal",
             "other")


def _schema_agm():
    return {
        "type": "object", "additionalProperties": False,
        "required": ["agm_date", "resolutions", "notes"],
        "properties": {
            "agm_date": {"type": "string", "description": "ISO date of the meeting; "
                         "empty if not stated."},
            "resolutions": {"type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "required": ["number", "title", "kind", "person_name", "person_name_en",
                             "proposed_by_shareholder",
                             "votes_for", "votes_against", "votes_abstain", "approval_pct",
                             "passed"],
                "properties": {
                    "number": {"type": "string", "description": "e.g. '第1号議案' or '第2号議案 3'."},
                    "title": {"type": "string", "description": "In English, at most 15 words."},
                    "kind": {"type": "string", "enum": list(RES_KINDS)},
                    "person_name": {"type": "string",
                                    "description": "For an election: the candidate's name as "
                                    "printed. Empty otherwise."},
                    "person_name_en": {"type": "string",
                                       "description": "The same name romanised, given name "
                                       "first. Empty otherwise."},
                    "proposed_by_shareholder": {"type": "boolean"},
                    "votes_for": {"type": "integer", "description": "賛成 (in voting units "
                                  "as printed); 0 if not stated."},
                    "votes_against": {"type": "integer"},
                    "votes_abstain": {"type": "integer"},
                    "approval_pct": {"type": "number",
                                     "description": "賛成割合 in percent as printed; 0 if "
                                     "not stated (it is then computed from the counts)."},
                    "passed": {"type": "boolean", "description": "可決."},
                },
            }, "description": "One entry per resolution, and one per candidate where a "
               "resolution elects several people with separate counts."},
            "notes": {"type": "string", "description": "How the company counts the rate "
                      "(e.g. excluding votes cast at the meeting), if stated."},
        },
    }


_PROMPT = (
    "Below is a Japanese extraordinary report (臨時報告書) on the results of a "
    "shareholders' meeting{who}, flattened from HTML. Return every resolution with "
    "its for / against / abstain counts and approval percentage as printed. Where "
    "one resolution elects several directors with a separate line each, return one "
    "entry per candidate, with the candidate's name and its romanisation. Mark "
    "shareholder proposals. "
    "Do not compute what the document does not state; leave it 0."
)


def extract_agm(text: str, api_key: str, code: str = "", name: str = "",
                model: str = YX.MODEL) -> dict:
    import anthropic
    from fx_extract import _loads_lenient
    who = f" for {name} ({code})" if (name or code) else ""
    client = anthropic.Anthropic(api_key=api_key)
    messages = [{"role": "user", "content": _PROMPT.format(who=who)
                 + "\n\n<report>\n" + (text or "")[:YX.MAX_BLOCK_CHARS] + "\n</report>"}]
    attempts = (
        {"output_config": {"format": {"type": "json_schema", "schema": _schema_agm()},
                           "effort": models.DOC_EXTRACT_EFFORT},
         "thinking": {"type": "adaptive"}},
        {},
    )
    last_exc = None
    for extra in attempts:
        try:
            resp = client.messages.create(model=model, max_tokens=12000,
                                          messages=messages, **extra)
            break
        except TypeError as exc:
            last_exc = exc
        except anthropic.BadRequestError as exc:
            last_exc = exc
    else:
        raise RuntimeError(f"Could not call the model: {last_exc}")
    if getattr(resp, "stop_reason", "") == "refusal":
        raise RuntimeError("The model declined to read this report.")
    reply = next((b.text for b in resp.content if getattr(b, "type", "") == "text"), "")
    out = normalise_agm(_loads_lenient(reply))
    usage = getattr(resp, "usage", None)
    out["usage"] = {"input": getattr(usage, "input_tokens", 0) or 0,
                    "output": getattr(usage, "output_tokens", 0) or 0}
    return out


def normalise_agm(raw: dict) -> dict:
    raw = raw if isinstance(raw, dict) else {}
    res = []
    for r in raw.get("resolutions") or []:
        f, a, ab = YX._i(r.get("votes_for")), YX._i(r.get("votes_against")), YX._i(r.get("votes_abstain"))
        pct = YX._f(r.get("approval_pct"))
        if not pct and (f + a + ab):
            pct = 100.0 * f / (f + a + ab)
        res.append({"number": YX._s(r.get("number"), 40), "title": YX._s(r.get("title"), 120),
                    "kind": r.get("kind") if r.get("kind") in RES_KINDS else "other",
                    "person_name": YX._s(r.get("person_name"), 80),
                    "person_name_en": YX._s(r.get("person_name_en"), 80),
                    "proposed_by_shareholder": bool(r.get("proposed_by_shareholder")),
                    "votes_for": f, "votes_against": a, "votes_abstain": ab,
                    "approval_pct": round(pct, 2), "passed": bool(r.get("passed"))})
    return {"agm_date": YX._s(raw.get("agm_date"), 12), "resolutions": res,
            "notes": YX._s(raw.get("notes"), 400)}


LOW_APPROVAL = 80.0


def agm_read(agm: dict, officers: list = (), prior: dict = None) -> dict:
    """What the vote says: the CEO's approval, the weakest director, anyone
    under the threshold, shareholder proposals and their support, and the
    change against the prior year's vote for the same person."""
    res = (agm or {}).get("resolutions") or []
    elections = [r for r in res if r["kind"] in ("director_election", "auditor_election")
                 and r["person_name"] and r["approval_pct"]]
    if not res:
        return {}
    prior_by = prior_lookup(prior)
    ceo_keys = {_full(o["name"]) for o in officers if o.get("is_ceo")}
    ceo_surnames = {_surname(o["name"]) for o in officers if o.get("is_ceo")}
    ceo = (next((r for r in elections if _full(r["person_name"]) in ceo_keys), None)
           or next((r for r in elections if _surname(r["person_name"]) in ceo_surnames), None))
    weakest = min(elections, key=lambda r: r["approval_pct"]) if elections else None
    low = [r for r in elections if r["approval_pct"] < LOW_APPROVAL]
    props = [r for r in res if r["proposed_by_shareholder"]]
    drops = []
    for r in elections:
        p = prior_pct(prior_by, r["person_name"])
        if p and r["approval_pct"] - p <= -5:
            drops.append((r["person_name"], p, r["approval_pct"]))
    return {"n": len(res), "elections": len(elections),
            "ceo": (ceo["person_name"], ceo["approval_pct"]) if ceo else None,
            "ceo_prior": prior_pct(prior_by, ceo["person_name"]) if ceo else None,
            "weakest": (weakest["person_name"], weakest["approval_pct"]) if weakest else None,
            "low": [(r["person_name"], r["approval_pct"]) for r in low],
            "shareholder_proposals": [(r["title"], r["approval_pct"], r["passed"]) for r in props],
            "drops": drops,
            "median": (sorted(r["approval_pct"] for r in elections)[len(elections) // 2]
                       if elections else None)}


def _full(name: str) -> str:
    """The name with every kind of space removed: '服部 真二' and '服部真二' match."""
    return re.sub(r"[\s\u3000]+", "", (name or "")).strip()


def _surname(name: str) -> str:
    """The family name, whatever the spacing: '服部 真二' → '服部'."""
    n = re.sub(r"[\s\u3000]+", " ", (name or "").strip())
    return n.split(" ")[0] if " " in n else n[:2]


def prior_lookup(prior: dict) -> dict:
    """{full name: approval %} plus {surname: %} for last year's elections —
    the surname entry only where it is unique, so 'Director A' and
    'Director B' (or two 佐藤s) never borrow each other's figure."""
    by_full, by_sur, dup = {}, {}, set()
    for r in ((prior or {}).get("resolutions") or []):
        if r.get("person_name") and r.get("approval_pct"):
            by_full[_full(r["person_name"])] = r["approval_pct"]
            s = _surname(r["person_name"])
            if s in by_sur:
                dup.add(s)
            by_sur[s] = r["approval_pct"]
    return {"full": by_full, "sur": {k: v for k, v in by_sur.items() if k not in dup}}


def prior_pct(lookup: dict, name: str):
    """Last year's approval for `name`: an exact match first, else a unique
    surname, else None."""
    if not lookup or not name:
        return None
    return lookup.get("full", {}).get(_full(name)) or lookup.get("sur", {}).get(_surname(name))


# ── The store ────────────────────────────────────────────────────────────
# Shape per company: a list of entries — {"id": "agm_FY2026", ...} for a
# vote, {"id": "lh_<DocID>", ...} for a 5%-rule filing's parsed numbers.

def load_from_github(repo: str, token: str = None) -> dict:
    return CE.load_from_github(repo, token, STORE_PATH)


def save_via_github(repo: str, token: str, code: str, entries: list) -> tuple:
    return CE.save_via_github(repo, token, code, list(entries), (), path=STORE_PATH,
                              what="holders and votes")
