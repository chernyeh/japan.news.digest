"""
forward.py — the forward-looking reads on the Research tab.

Each function answers one question an investor asks before the next result,
from data the app already holds (the estimates log, capital events, the
panel's merged map). All pure, so the thresholds are testable; the panel only
lays out what comes back.

Every read returns a dict with at least:
  tone     "good" / "warn" / "bad" / "info" — the colour of its flag
  title    a few words for the chip
  detail   one or two sentences, with the numbers, for when it is opened
or None when there is nothing to say.
"""

import estimates_log as EL

# A beat or a raise smaller than this share of full-year guidance is noise:
# rounding in a tanshin, or a yen move. Below it, the reads say "on plan".
NOISE = 0.01


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _chg(new, old):
    if new is None or old is None or old <= 0:
        return None
    return new / old - 1


def _pct(v, dp=0):
    return "—" if v is None else f"{v * 100:+.{dp}f}%"


def _bn(v):
    return "—" if v is None else f"¥{v / 1e9:,.0f}bn"


def _latest(rows, metric, fy, basis):
    s = EL.series(rows, metric, fy, basis)
    return s[-1][1] if s else None


def prior_fy(fy: str) -> str:
    try:
        return f"FY{int(fy[2:]) - 1}"
    except (TypeError, ValueError):
        return ""


# ── 1. Buyback window ──────────────────────────────────────────────────────

def buyback_status(events: list, today: str, next_results: str = "",
                   unread_progress: int = 0) -> dict:
    """Whether a buyback authorisation is still running, and how far through
    it the company is.

    A running authorisation that outlasts the next results date means a fresh
    buyback announcement then is unlikely — the company has one in hand. A
    finished one is the reverse: room for a new one. Progress comes from
    "Buyback progress" events (the monthly 取得状況 notices, read or typed);
    without them the share done is unknown and the read says so."""
    events = sorted(events or (), key=lambda e: e.get("date", ""))
    auths = [e for e in events if e.get("type") == "Buyback announced"]
    if not auths:
        return None
    a = auths[-1]
    after = [e for e in events if e.get("date", "") >= a.get("date", "")]
    done = next((e for e in after if e.get("type") == "Buyback completed"), None)
    until = a.get("until") or ""
    size = (f"¥{a['amount_jpy'] / 1e9:,.0f}bn" if a.get("amount_jpy") else
            f"{a['shares'] / 1e6:,.1f}m shares" if a.get("shares") else "size not logged")

    if done or (until and until < today):
        return {"tone": "info", "state": "finished", "title": "Buyback finished",
                "detail": f"The {size} buyback announced {a.get('date')} "
                          f"{'completed ' + done['date'] if done else 'ran to ' + until}. "
                          "Nothing is running — a new authorisation is possible "
                          + (f"with the results on {next_results}." if next_results
                             else "at the next results."),
                "until": until, "pct_done": None}

    prog = [e for e in after if e.get("type") == "Buyback progress"]
    pct = None
    if prog:
        p = prog[-1]
        if a.get("amount_jpy") and p.get("cum_amount_jpy"):
            pct = p["cum_amount_jpy"] / a["amount_jpy"]
        elif a.get("shares") and p.get("cum_shares"):
            pct = p["cum_shares"] / a["shares"]
    runs_past = bool(until and next_results and until >= next_results)
    pct_txt = (f"~{pct * 100:.0f}% done as of {prog[-1]['date']}" if pct is not None
               else (f"progress not read — {unread_progress} monthly report(s) on TDnet"
                     if unread_progress else "progress unknown"))
    detail = (f"{size} authorised {a.get('date')}"
              + (f", running to {until}" if until else "") + f"; {pct_txt}. ")
    if runs_past:
        detail += (f"It runs past the next results ({next_results}), so a new buyback "
                   "announcement then is unlikely.")
    elif until:
        detail += "It ends before the next results — a top-up or a new one is possible."
    return {"tone": "info", "state": "running",
            "title": f"Buyback running{' to ' + until[:7] if until else ''}"
                     + (f" · {pct * 100:.0f}% done" if pct is not None else ""),
            "detail": detail, "until": until, "pct_done": pct, "runs_past": runs_past}


# ── 2. Quality of a guidance change ────────────────────────────────────────

def _usual_progress(rows, metric, fy, n, years=3):
    """The company's median year-to-date share of the full year at quarter n,
    over the prior `years` reported years."""
    vals, y = [], fy
    for _ in range(years):
        y = prior_fy(y)
        ytd, full = _latest(rows, metric, y, f"ytd_{n}q"), _latest(rows, metric, y, "actual")
        if ytd is not None and full and full > 0:
            vals.append(ytd / full)
    if not vals:
        return None
    vals.sort()
    return vals[len(vals) // 2]


def raise_quality(rows: list, fy: str, metric: str = "operating_profit") -> dict:
    """How the company's latest quarterly result moved its full-year guidance,
    set against how far the year to date beat the company's own plan.

    The plan for the year to date is the first-half guidance at 2Q where there
    is one; otherwise the old full-year guidance times this company's usual
    progress at that quarter. Then:

      beat  = YTD actual − planned YTD
      raise = new FY guidance − old FY guidance
      rest of year, implied = new FY guidance − YTD actual

    A raise about the size of the beat leaves the rest of the year where it
    was: the company has banked the outperformance and assumed none of it
    persists. Where the rest of the year then implies much weaker growth than
    the year to date delivered, that is the underwhelming raise."""
    dates = EL.results_dates(rows)
    quarters = sorted(((d, int(p[0])) for (y, p), d in dates.items()
                       if y == fy and p in ("1Q", "2Q", "3Q")), reverse=True)
    if not quarters:
        return None
    date, n = quarters[0]
    ytd = EL.value_on(rows, metric, fy, f"ytd_{n}q", date)
    old = EL.value_before(rows, metric, fy, "company", date)
    new = EL.value_on(rows, metric, fy, "company", date)
    if ytd is None or not old or new is None:
        return None

    h1 = EL.value_before(rows, metric, fy, "company_h1", date) if n == 2 else None
    if h1:
        plan, plan_basis = h1, "the 1H guidance"
    else:
        usual = _usual_progress(rows, metric, fy, n)
        if usual is None:
            return None
        plan, plan_basis = old * usual, f"the usual {usual * 100:.0f}% of the year by {n}Q"
    beat, rise = ytd - plan, new - old
    rest_new = new - ytd

    py = prior_fy(fy)
    p_ytd, p_full = _latest(rows, metric, py, f"ytd_{n}q"), _latest(rows, metric, py, "actual")
    ytd_yoy = _chg(ytd, p_ytd)
    rest_yoy = _chg(rest_new, (p_full - p_ytd) if (p_full is not None and p_ytd is not None) else None)

    out = {"date": date, "period": f"{n}Q", "fy": fy, "metric": metric,
           "old": old, "new": new, "ytd": ytd, "plan": plan, "plan_basis": plan_basis,
           "beat": beat, "raise": rise, "rest_new": rest_new,
           "ytd_yoy": ytd_yoy, "rest_yoy": rest_yoy}
    noise = abs(old) * NOISE
    growth = (f" The rest of the year now implies {_pct(rest_yoy)} YoY against "
              f"{_pct(ytd_yoy)} in the year to date." if rest_yoy is not None and ytd_yoy is not None
              else "")
    head = (f"{n}Q year to date {_bn(ytd)} vs plan {_bn(plan)} ({plan_basis}): "
            f"{'beat' if beat >= 0 else 'missed'} by {_bn(abs(beat))}. "
            f"Guidance {_bn(old)} → {_bn(new)} ({_pct(_chg(new, old), 1)}).")

    if abs(beat) <= noise:
        tone, title = ("good", "On plan, guidance raised") if rise > noise else (
            ("bad", "On plan, guidance cut") if rise < -noise else ("info", "On plan"))
    elif beat > 0:
        if rise <= noise:
            tone, title = "warn", "Beat, guidance not raised"
            head += " The beat was not added to the year: the rest of the year was cut."
        elif rise < beat * 0.9:
            tone, title = "warn", "Raised by less than the beat"
            head += " The rest of the year was trimmed."
        elif rise <= beat * 1.1:
            tone, title = "warn", "Raise only banks the beat"
            head += " The rest of the year is unchanged: none of the outperformance is assumed to persist."
        else:
            tone, title = "good", "Raised beyond the beat"
            head += " The rest of the year was raised too."
    else:
        if rise < -noise:
            tone, title = "bad", "Missed plan and cut"
        else:
            tone, title = "warn", "Missed plan, guidance held"
            head += " Holding the year now needs a stronger rest of year — risk of a later cut."
    # Underwhelming in growth terms, whatever the raise: the rest of the year
    # implies a clear slowdown from what the year to date delivered.
    if (rest_yoy is not None and ytd_yoy is not None and ytd_yoy - rest_yoy > 0.10
            and tone != "bad"):
        tone = "warn"
        if title in ("Raised beyond the beat", "On plan, guidance raised", "On plan"):
            title += " — but implies a slowdown"
    out.update({"tone": tone, "title": title, "detail": head + growth})
    return out


# ── 3. Where guidance sits against the street ──────────────────────────────

def guidance_stance(guide: dict, street: dict, record: dict = None) -> dict:
    """Street vs company guidance for the year, on the first line both have
    (operating profit, net profit, EPS — Yahoo publishes no street OP for most
    Japanese names, so EPS is often the one), read with this management's own
    record of raising or cutting."""
    for metric, label in (("operating_profit", "OP"), ("net_profit", "net profit"),
                          ("eps", "EPS")):
        g, s = _num(guide.get(metric)), _num(street.get(metric))
        if g and s is not None and g > 0:
            gap = s / g - 1
            break
    else:
        return None
    rec = ""
    if record and not record.get("thin"):
        rec = (f" This management has raised {record['raised']} and cut {record['cut']} "
               f"of its opening numbers over {record['years']} years.")
    if gap > 0.03:
        tone = "good" if record and record.get("raised", 0) > record.get("cut", 0) else "info"
        title = f"Street {gap * 100:.0f}% above guidance"
        detail = (f"The street's {label} is {gap * 100:.1f}% above the company's guidance: "
                  "the market already expects a raise." + rec)
    elif gap < -0.03:
        tone, title = "warn", f"Street {abs(gap) * 100:.0f}% below guidance"
        detail = (f"The street's {label} is {abs(gap) * 100:.1f}% below guidance: "
                  "the market doubts the company's number." + rec)
    else:
        tone, title = "info", "Street in line with guidance"
        detail = f"Street {label} within 3% of guidance ({gap * 100:+.1f}%)." + rec
    return {"tone": tone, "title": title, "detail": detail, "gap": gap, "metric": metric}


# ── 4. The next quarter ────────────────────────────────────────────────────

def next_quarter(get, fy: str, metric: str = "operating_profit") -> dict:
    """The next unreported quarter of `fy`, its street estimate if one was
    typed, and the year-on-year growth that estimate implies."""
    import fundamentals as F
    bd = F.quarter_breakdown(get, metric, fy)
    for n in (1, 2, 3, 4):
        q = bd.get(f"q{n}")
        if q and q[1] in ("actual", "derived"):
            continue
        street = get(metric, fy, f"consensus_q{n}")
        last = (F.quarter_breakdown(get, metric, prior_fy(fy)).get(f"q{n}") or (None,))[0]
        return {"period": f"{n}Q", "fy": fy, "street": street, "last_year": last,
                "yoy": _chg(street, last)}
    return None


# ── 5. Forward P/E through time ────────────────────────────────────────────

def pe_history(prices: list, rows: list, basis: str = "company") -> list:
    """[(date, pe, fy)] — each week's close over the EPS that was then the
    forecast for the year being guided, as it stood that week.

    "The year being guided" on a date is the earliest fiscal year with an EPS
    forecast logged by then whose full-year result had not yet been reported.
    Both the price and the EPS are as they were that week, so a stock split
    between the two is the one thing that can put a point out — the Research
    tab's split warning covers it."""
    reported = {fy: d for (fy, p), d in EL.results_dates(rows).items() if p == "FY"}
    by_fy = {}
    for r in EL.compact(rows):
        if r["metric"] == "eps" and r["basis"] == basis:
            by_fy.setdefault(r["fy"], []).append((r["date"], _num(r["value"])))
    out = []
    for d, close in sorted(prices or ()):
        for fy in sorted(by_fy):
            if reported.get(fy, "9999") <= d:
                continue
            eps = None
            for dd, v in by_fy[fy]:
                if dd <= d:
                    eps = v
                else:
                    break
            if eps is not None:
                if eps > 0 and close:
                    out.append((d, close / eps, fy))
                break
    return out
