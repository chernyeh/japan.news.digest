"""Tests for the watchlist store and the comparison grid — `python test_watchlist.py`.

No framework and no network: every test runs in a scratch directory with no
GITHUB_TOKEN, so each save stops at "kept for this session only" and never
reaches GitHub. The names are real listed ones, including the two pairs of
companies that share a name (TOA Corporation, Shinwa Co., Ltd.).
"""

import os
import sys
import tempfile
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import watchlist as W
import watchlist_grid as G
import collect_consensus as CC

NAMES = {
    "6971": "Kyocera Corporation",
    "5331": "Noritake Co., Limited",
    "6098": "Recruit Holdings Co.,Ltd.",
    "6501": "Hitachi, Ltd.",
    "3447": "Shinwa Co., Ltd.",
    "7607": "Shinwa Co., Ltd.",
    "8058": "Mitsubishi Corporation",
}


def _in_scratch(fn):
    """Run `fn` with the working directory in a fresh temp dir, so the local
    watchlist.json cache it writes is thrown away afterwards."""
    def wrapped():
        old = os.getcwd()
        with tempfile.TemporaryDirectory() as d:
            os.chdir(d)
            try:
                fn()
            finally:
                os.chdir(old)
    wrapped.__name__ = fn.__name__
    return wrapped


# ── Store: keys, names, ambiguity ───────────────────────────────────────────

@_in_scratch
def test_adding_by_code_stores_the_listed_name_not_the_code():
    ok, msg = W.add_to_watchlist("6098", names_lookup=NAMES)
    assert not ok and "session only" in msg      # no token: cache only
    e = W.load_watchlist_entries()
    assert e["6098"]["name"] == "Recruit Holdings Co.,Ltd.", e


def test_display_name_repairs_an_entry_saved_with_its_code_as_name():
    assert W.display_name("6098", {"name": "6098"}, NAMES) == "Recruit Holdings Co.,Ltd."
    assert W.display_name("6971", {"name": "Kyocera"}, NAMES) == "Kyocera"
    assert W.display_name("_unresolved:Foo", {"name": "Foo"}, NAMES) == "Foo"


@_in_scratch
def test_a_name_shared_by_two_companies_is_refused_not_guessed():
    ok, msg = W.add_to_watchlist("Shinwa Co., Ltd.", names_lookup=NAMES)
    assert not ok and "3447" in msg and "7607" in msg, msg
    assert W.load_watchlist_entries() == {}
    assert W.resolve_code("Shinwa Co., Ltd.", NAMES) == ""
    # By code, each one lands on its own ticker.
    W.add_to_watchlist("7607", names_lookup=NAMES)
    assert list(W.load_watchlist_entries()) == ["7607"]


@_in_scratch
def test_remove_many_removes_in_one_go():
    for c in ("6971", "5331", "6098"):
        W.add_to_watchlist(c, names_lookup=NAMES)
    W.remove_many(["6971", "6098", "9999"])
    assert list(W.load_watchlist_entries()) == ["5331"]


# ── Thesis ──────────────────────────────────────────────────────────────────

def test_clean_thesis_keeps_valid_fields_and_drops_the_rest():
    t = W.clean_thesis({"conviction": 7.0, "munger_price": "2,400", "exit_price": float("nan"),
                        "action": "watch", "review_date": date(2027, 1, 31), "note": " x "})
    assert t == {"conviction": 7, "munger_price": 2400.0, "action": "WATCH",
                 "review_date": "2027-01-31", "note": "x"}, t
    assert W.clean_thesis({"conviction": 11, "munger_price": -5, "action": "HOLD",
                           "review_date": "2027-02-30"}) == {}
    # What a cleared DateColumn cell comes back as.
    class NaT:
        def isoformat(self):
            return "NaT"
    assert W.clean_thesis({"review_date": NaT()}) == {}


@_in_scratch
def test_set_theses_saves_changes_and_clears_blank_rows():
    W.add_to_watchlist("6971", names_lookup=NAMES)
    W.add_to_watchlist("5331", names_lookup=NAMES)
    W.set_theses({"6971": {"munger_price": 1800, "exit_price": 3200, "action": "BUY"},
                  "5331": {}})
    assert W.get_thesis("6971") == {"munger_price": 1800.0, "exit_price": 3200.0, "action": "BUY",
                                    "action_since": date.today().isoformat()}
    assert W.get_thesis("5331") == {}
    ok, msg = W.set_theses({"6971": {"munger_price": 1800, "exit_price": 3200, "action": "BUY"}})
    assert ok and "No thesis" in msg
    W.set_theses({"6971": {}})
    assert "thesis" not in W.load_watchlist_entries()["6971"]


def test_thesis_metrics_against_the_price():
    m = G.thesis_metrics(2000, {"munger_price": 2500, "exit_price": 3000})
    assert abs(m["vs_munger"] - (-0.2)) < 1e-9       # 20% under the Munger price
    assert abs(m["to_exit"] - 0.5) < 1e-9
    assert G.thesis_metrics(None, {"munger_price": 2500})["vs_munger"] is None


# ── News matching ───────────────────────────────────────────────────────────

def test_aliases_use_the_short_name_but_not_a_group_name():
    assert "kyocera" in W.get_company_aliases("Kyocera Corporation", "6971")
    assert "recruit" in W.get_company_aliases("Recruit Holdings Co.,Ltd.", "6098")
    al = W.get_company_aliases("Mitsubishi Corporation", "8058")
    assert "mitsubishi" not in al and "8058" in al, al
    # A KNOWN_COMPANIES ticker brings its hardcoded aliases, Japanese included.
    assert "日立" in W.get_company_aliases("Hitachi, Ltd.", "6501")


def test_article_matching_on_ticker_short_name_and_word_boundary():
    al = W.get_company_aliases("Kyocera Corporation", "6971")
    assert W.article_matches({"company_code": "6971", "title": "x"}, al, "6971")
    assert W.article_matches({"title": "Kyocera raises guidance"}, al, "6971")
    assert not W.article_matches({"title": "Code 169710 in a list"}, al, "6971")
    ntt = W.get_company_aliases("NTT", "9432")
    assert not W.article_matches({"title": "Press the button"}, ntt, "9432")


def test_scan_all_watchlist_takes_the_entries_dict():
    arts = {"Tech": [{"title": "Kyocera to buy back shares"},
                     {"title": "Mitsubishi UFJ results"},
                     {"company_code": "8058", "title": "Trading house earnings"}]}
    entries = {"6971": {"name": "Kyocera Corporation"},
               "8058": {"name": "Mitsubishi Corporation"}}
    hits = W.scan_all_watchlist(entries, arts, NAMES)
    assert len(hits["Kyocera Corporation"]) == 1
    # Matched on its ticker only — not on Mitsubishi UFJ's headline.
    assert [a["title"] for a in hits["Mitsubishi Corporation"]] == ["Trading house earnings"]


# ── Grid arithmetic ─────────────────────────────────────────────────────────

def test_rank_shades_respect_direction_and_skip_losses():
    rows = [{"pe": 8}, {"pe": 30}, {"pe": 15}, {"pe": -4}, {"pe": None}, {"pe": 12}]
    G.POSITIVE_ONLY.add("pe")
    try:
        sh = G.rank_shades(rows, "pe", -1)
    finally:
        G.POSITIVE_ONLY.discard("pe")
    assert sh[0] == "good" and sh[1] == "bad"
    assert sh[3] == "" and sh[4] == ""          # a loss is not "cheap"
    assert G.rank_shades(rows[:2], "pe", -1) == ["", ""]


def test_medians_skip_blanks_and_negative_multiples():
    m = G.medians([{"pe_st": 10}, {"pe_st": 20}, {"pe_st": -3}, {"pe_st": None}], ["pe_st"])
    assert m["pe_st"] == 15 and m["_n"] == 4
    sm = G.sector_medians([{"sector": "A", "pb": 1.0}, {"sector": "A", "pb": 3.0},
                           {"sector": "B", "pb": 2.0}], ["pb"])
    assert sm["A"]["pb"] == 2.0 and sm["B"]["_n"] == 1


def _weekly(start: date, closes: list) -> list:
    return [((start + timedelta(weeks=i)).isoformat(), c) for i, c in enumerate(closes)]


def test_relative_performance_from_weekly_history():
    start = date(2026, 4, 17)
    n = 30
    stock = _weekly(start, [100.0] * (n - 1) + [120.0])     # +20% at the end
    topix = _weekly(start, [100.0] * (n - 1) + [110.0])     # +10%
    p = G.rel_perf_from_history(stock, topix)
    expect = round((1.2 / 1.1 - 1) * 100, 1)
    assert p["vs3m"] == expect and p["vs6m"] == expect, p
    assert p["vs12m"] is None                                # history too short


def test_relative_performance_is_split_adjusted():
    start = date(2026, 4, 17)
    closes = [100.0] * 20 + [50.0] * 10                      # 2-for-1 split, no real move
    stock = _weekly(start, closes)
    topix = _weekly(start, [100.0] * 30)
    ex = date.fromisoformat(stock[20][0])
    p = G.rel_perf_from_history(stock, topix, [{"ex_date": ex, "factor": 2.0, "status": "split"}])
    assert p["vs6m"] == 0.0, p
    unresolved = [{"ex_date": ex, "factor": 2.0, "status": "unresolved"}]
    assert G.rel_perf_from_history(stock, topix, unresolved)["vs6m"] is None


def test_format_and_sort():
    assert G.fmt(0.0312, "pct1") == "3.1%"
    assert G.fmt(-0.05, "spct1") == "-5.0%"
    assert G.fmt(12.345, "x1") == "12.3x"
    assert G.fmt(None, "x1") == "" and G.fmt(float("nan"), "x1") == ""
    rows = [{"k": 3}, {"k": None}, {"k": 1}]
    assert [r["k"] for r in G.sort_rows(rows, "k")] == [1, 3, None]
    assert [r["k"] for r in G.sort_rows(rows, "k", True)] == [3, 1, None]


# ── Phase 2: reward-to-risk, reaction, decision flags ──────────────────────

def test_reward_to_risk():
    assert abs(G.reward_to_risk(1000, 800, 1600) - 3.0) < 1e-9     # 600 up / 200 down
    assert G.reward_to_risk(1000, 1000, 1600) == float("inf")      # no measured downside
    assert G.reward_to_risk(1700, 800, 1600) is None               # past the exit
    assert G.reward_to_risk(1000, None, 1600) is None
    assert G.fmt(3.04, "rr") == "3.0:1" and G.fmt(float("inf"), "rr") == "∞"


@_in_scratch
def test_action_since_is_stamped_on_change_and_kept_otherwise():
    W.add_to_watchlist("6971", names_lookup=NAMES)
    W.set_theses({"6971": {"action": "WATCH"}})
    t = W.get_thesis("6971")
    assert t["action_since"] == date.today().isoformat()
    # Pretend it was set long ago; an unrelated edit must not reset the clock.
    e = W.load_watchlist_entries()
    e["6971"]["thesis"]["action_since"] = "2026-01-05"
    W.save_watchlist(e)
    W.set_theses({"6971": {"action": "WATCH", "munger_price": 2000}})
    assert W.get_thesis("6971")["action_since"] == "2026-01-05"
    W.set_theses({"6971": {"action": "BUY", "munger_price": 2000}})
    assert W.get_thesis("6971")["action_since"] == date.today().isoformat()
    # The input cannot forge the stamp.
    assert "action_since" not in W.clean_thesis({"action_since": "2020-01-01"})


def _fridays(start: date, closes: list) -> list:
    return [((start + timedelta(weeks=i)).isoformat(), c) for i, c in enumerate(closes)]


def test_reaction_after_a_result():
    start = date(2026, 9, 4)                      # a Friday
    stock = _fridays(start, [100, 100, 100, 95, 94])
    topix = _fridays(start, [100, 100, 100, 101, 101])
    # Results on Tue 2026-09-22: base is Fri 09-18 (100); Fri 09-25 is only
    # three days on, so the first close 5+ days later is Fri 10-02 (94).
    rel, end, settled = G.reaction_after(stock, topix, "2026-09-22")
    assert end == "2026-10-02" and settled
    assert rel == round((0.94 / 1.01 - 1) * 100, 1)
    # Results the day of the latest close: nothing to measure yet.
    assert G.reaction_after(stock, topix, "2026-10-02") is None
    # Results two days before the latest close: the move so far, unsettled.
    rel2, end2, settled2 = G.reaction_after(stock, topix, "2026-09-29")
    assert end2 == "2026-10-02" and not settled2
    # A hole in the history: the nearest closes are a month apart, which would
    # measure drift, not the reaction.
    gappy = [("2026-07-28", 100), ("2026-08-28", 131)]
    tpx = _fridays(date(2026, 7, 24), [100] * 6)
    assert G.reaction_after(gappy, tpx, "2026-08-07") is None


def _row(**kw):
    base = {"code": "6971", "name": "Kyocera", "sector": "Technology", "price": 1000}
    base.update(kw)
    base.update({k: v for k, v in G.thesis_metrics(base["price"], kw.get("thesis")).items()
                 if v is not None})
    return base


TODAY = date(2026, 11, 6)


def _kinds(row, **cfg):
    return [f["kind"] for f in G.decision_flags(row, TODAY, cfg or None)]


def test_flag_below_munger_and_still_on_watch():
    r = _row(thesis={"munger_price": 1100, "action": "WATCH"})
    fl = G.decision_flags(r, TODAY)
    assert fl[0]["kind"] == "below_munger" and fl[0]["severity"] == "act"
    assert "Still on WATCH" in fl[0]["text"]


def test_flags_near_and_past_exit_unless_already_trimming():
    assert _kinds(_row(thesis={"exit_price": 1080})) == ["near_exit"]
    assert _kinds(_row(thesis={"exit_price": 950})) == ["past_exit"]
    assert _kinds(_row(thesis={"exit_price": 950, "action": "TRIM"})) == []
    assert _kinds(_row(thesis={"exit_price": 1300})) == []


def test_flag_stuck_on_watch_needs_both_the_hurdle_and_the_days():
    th = {"bear_price": 900, "exit_price": 1400, "action": "WATCH"}   # 4.0:1
    assert "stuck_watch" in _kinds(_row(thesis={**th, "action_since": "2026-08-01"}))
    assert "stuck_watch" not in _kinds(_row(thesis={**th, "action_since": "2026-10-20"}))
    weak = {"bear_price": 800, "exit_price": 1300, "action": "WATCH",
            "action_since": "2026-01-01"}                             # 1.5:1
    assert "stuck_watch" not in _kinds(_row(thesis=weak))
    # A tighter hurdle setting flags it.
    assert "stuck_watch" in _kinds(_row(thesis=weak), rr_hurdle=1.5)
    # No action_since (a thesis saved before stamping): falls back to added_at.
    assert "stuck_watch" in _kinds(_row(thesis=th, added_at="2026-07-01"))


def test_flag_review_due():
    assert "review_due" in _kinds(_row(thesis={"review_date": "2026-11-01"}))
    assert "review_due" in _kinds(_row(thesis={"review_date": "2026-11-06"}))
    assert "review_due" not in _kinds(_row(thesis={"review_date": "2026-11-30"}))


def test_flag_beat_but_stock_fell_rule_10():
    r = _row(op_vs_street=0.06, reaction=-3.2, reaction_settled=True,
             result_date="2026-10-28", result_label="2Q FY2027, reported 2026-10-28")
    fl = [f for f in G.decision_flags(r, TODAY) if f["kind"] == "beat_fell"]
    assert fl and "+6.0%" in fl[0]["text"] and "-3.2%" in fl[0]["text"]
    assert "beat_fell" not in _kinds(_row(op_vs_street=0.06, reaction=2.0, result_date="2026-10-28"))
    assert "beat_fell" not in _kinds(_row(op_vs_street=-0.1, reaction=-5.0, result_date="2026-10-28"))
    # No street figure: a raised guide counts as the good news.
    assert "beat_fell" in _kinds(_row(guide_move=0.08, reaction=-1.0, result_date="2026-10-28"))
    # Too old to be a reaction any more.
    assert "beat_fell" not in _kinds(_row(op_vs_street=0.06, reaction=-3.0, result_date="2026-05-01"))


def test_peer_flag_reaches_other_names_in_the_same_sector():
    a = _row(code="8035", name="TEL", op_vs_street=0.05, reaction=-4.0, result_date="2026-11-06")
    b = _row(code="6857", name="Advantest")
    c = _row(code="7203", name="Toyota", sector="Consumer Cyclical")
    pf = G.peer_flags([a, b, c], TODAY)
    assert list(pf) == ["6857"] and "TEL" in pf["6857"][0]["text"]


# ── Collection coverage ─────────────────────────────────────────────────────

def test_watchlist_companies_join_the_collection_universe():
    with tempfile.TemporaryDirectory() as d:
        meta = os.path.join(d, "metadata.csv")
        with open(meta, "w", encoding="utf-8") as fh:
            fh.write("Code,Name,Shares,Sector\n5331,\"Noritake Co., Limited\",1,X\n")
        uni = [{"Code": "6971", "Name": "Kyocera"}]
        out = CC.with_watchlist(uni, ["6971", "5331", "5331"], meta)
        assert [u["Code"] for u in out] == ["6971", "5331"]
        assert out[1]["Name"] == "Noritake Co., Limited"
        assert CC.with_watchlist(uni, [], meta) == uni


def test_named_companies_are_collected_even_outside_the_universe(capsys):
    """weekly_consensus.yml's 'only' box: a company asked for by code is
    collected now, whether or not it is in the universe or on the watchlist
    (Seiko Corporation, 6286, is in neither list's TOPIX 1000)."""
    uni = [{"Code": "7203", "Name": "Toyota"}, {"Code": "6501", "Name": "Hitachi"}]
    out = CC.select_companies(uni, " 6286, 7203 ,,72031,x")
    assert [u["Code"] for u in out] == ["7203", "6286"]
    assert out[1]["Name"] == "Seiko Corporation"        # from data/metadata.csv
    assert "skipping 72031, X" in capsys.readouterr().out
    assert CC.select_companies(uni, "") == uni


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  ok   {name}")
        except AssertionError as exc:
            failed += 1
            print(f"  FAIL {name}: {exc or '(assertion)'}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
