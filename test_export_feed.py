"""export_feed.py: what goes into the public feed Gloomberg reads, and what
must not."""
import csv
import json
import os
from datetime import datetime, timezone

import pytest

import export_feed as EF
import jquants

NOW = datetime(2026, 11, 10, 20, 0, tzinfo=timezone.utc)   # 11 Nov 05:00 JST


def _csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    d = tmp_path / "data"
    (d / "jpx_earnings").mkdir(parents=True)
    _csv(d / "metadata.csv", [
        {"Code": "4203", "Name": "Sumitomo Bakelite", "Shares": "1", "Sector": "Basic Materials"},
        {"Code": "7203", "Name": "Toyota", "Shares": "1", "Sector": "Consumer Cyclical"},
    ])
    _csv(d / "consensus.csv", [
        {"code": "4203", "name": "x", "metric": "eps", "fy": "FY2025", "basis": "actual",
         "value": "250", "unit": "jpy", "source": "jquants", "as_of": "2025-05-10"},
        {"code": "4203", "name": "x", "metric": "eps", "fy": "FY2026", "basis": "actual",
         "value": "319.52", "unit": "jpy", "source": "jquants", "as_of": "2026-08-03"},
        {"code": "4203", "name": "x", "metric": "eps", "fy": "FY2027", "basis": "consensus",
         "value": "400", "unit": "jpy", "source": "yfinance", "as_of": "2026-11-07"},
        {"code": "4203", "name": "x", "metric": "eps", "fy": "FY2027", "basis": "ytd_1q",
         "value": "90", "unit": "jpy", "source": "jquants", "as_of": "2026-08-03"},
        {"code": "4203", "name": "x", "metric": "shares", "fy": "FY2026", "basis": "actual",
         "value": "9", "unit": "count", "source": "jquants", "as_of": "2026-08-03"},
    ])
    (d / "consensus_manual.json").write_text(json.dumps({"4203": {
        "eps|FY2027|consensus": {"value": 413.3, "unit": "jpy", "source": "typed:Koyfin", "as_of": "2026-11-08"},
        "net_sales|latest|fin": {"value": 1, "unit": "jpy_abs", "source": "typed", "as_of": "2026-11-08"},
    }}))
    _csv(d / "tdnet_filings.csv", [
        {"Code": "4203", "Name": "n", "NameEN": "", "Title": "決算短信", "TitleEN": "Results",
         "PubDateTime": "2026-11-04 11:30", "DocUrl": "https://www.release.tdnet.info/inbs/1.pdf",
         "EngUrl": "https://www.release.tdnet.info/inbs/ek/1.pdf"},
        {"Code": "4203", "Name": "n", "NameEN": "", "Title": "配当予想の修正", "TitleEN": "配当予想の修正",
         "PubDateTime": "2026-11-04 11:30", "DocUrl": "javascript:alert(1)", "EngUrl": ""},
        {"Code": "4203", "Name": "n", "NameEN": "", "Title": "old", "TitleEN": "",
         "PubDateTime": "2026-09-01 15:00", "DocUrl": "https://www.release.tdnet.info/inbs/0.pdf",
         "EngUrl": ""},
    ])
    (d / "capital_events.json").write_text(json.dumps({"4203": [
        {"date": "2026-11-07", "type": "Dividend change", "dps_old": 120, "dps_new": 140,
         "note": "with 2Q", "id": "internal", "logged_at": "2026-11-07"},
        {"date": "2020-01-01", "type": "Buyback", "amount_bn": 5},
    ]}))
    _csv(d / "liquidity.csv", [
        {"code": "4203", "close": "4000", "mcap_b": "380", "adv_jpy_20": "2e9", "adv_jpy_60": "1.8e9",
         "adv_shares_20": "1", "obs_20": "20", "obs_60": "60", "span_days_20": "28",
         "participation": "0.2", "days_to_exit_100m": "0.3", "tier": "liquid",
         "first_obs": "", "last_obs": "", "as_of": "2026-11-10"},
        # An ETF: liquidity and nothing else, no name -- left out.
        {"code": "1306", "close": "402", "mcap_b": "", "adv_jpy_20": "1e10", "adv_jpy_60": "",
         "adv_shares_20": "", "obs_20": "", "obs_60": "", "span_days_20": "", "participation": "",
         "days_to_exit_100m": "", "tier": "", "first_obs": "", "last_obs": "", "as_of": "2026-11-10"},
    ])
    _csv(d / "jquants_3m_perf.csv", [
        {"Code": "4203", "VsTopix3M": "-12.5", "VsTopix6M": "", "VsTopix12M": "3.1",
         "TopixReturn3M": "", "TopixReturn6M": "", "TopixReturn12M": "", "ComputedDate": "2026-11-10"},
    ])
    (d / "jpx_earnings" / "kessan.xlsx").write_bytes(b"stub")
    monkeypatch.setattr(jquants, "parse_jpx_earnings_excel", lambda b, source_label="": [
        {"code": "4203", "announcement_date": "2026-11-04", "period_type": "Q2", "fiscal_year_end": "2027-03-31"},
        {"code": "4203", "announcement_date": "2027-02-05", "period_type": "Q3", "fiscal_year_end": "2027-03-31"},
        {"code": "4203", "announcement_date": "2026-11-20", "period_type": "Q2", "fiscal_year_end": "2027-03-31"},
        {"code": "7203", "announcement_date": "2026-11-11", "period_type": "Q2", "fiscal_year_end": "2027-03-31"},
    ])
    return str(d)


def test_results_dates_take_the_nearest_ahead_and_behind(data_dir):
    c = EF.build_feed(data_dir, NOW)["companies"]
    assert c["4203"]["next_results"]["date"] == "2026-11-20"
    assert c["4203"]["last_results"]["date"] == "2026-11-04"
    # "Today" is the Tokyo date: 11 Nov in Tokyo, though still 10 Nov in UTC.
    assert c["7203"]["next_results"]["date"] == "2026-11-11"


def test_estimates_apply_typed_overrides_and_keep_only_live_years(data_dir):
    est = EF.build_feed(data_dir, NOW)["companies"]["4203"]["estimates"]
    assert est["FY2027"]["eps"]["consensus"] == [413.3, "typed:Koyfin", "2026-11-08"]
    assert "FY2025" not in est                       # older than the latest actual
    assert est["FY2026"]["eps"]["actual"][0] == 319.52
    assert "ytd_1q" not in est["FY2027"]["eps"]      # full years only
    assert "shares" not in est["FY2026"]
    assert "latest" not in est                       # balance-sheet overrides stay out


def test_filings_window_titles_and_unsafe_links(data_dir):
    f = EF.build_feed(data_dir, NOW)["companies"]["4203"]["filings"]
    assert [x["title"] for x in f] == ["決算短信", "配当予想の修正"]  # 2026-09-01 is past the window
    assert f[0]["title_en"] == "Results" and f[0]["url_en"].endswith("/ek/1.pdf")
    assert "title_en" not in f[1]                    # was just the Japanese title again
    assert "url" not in f[1]                         # javascript: is not a link


def test_capital_events_drop_internal_fields_and_old_events(data_dir):
    ev = EF.build_feed(data_dir, NOW)["companies"]["4203"]["capital_events"]
    assert len(ev) == 1 and ev[0]["dps_new"] == 140
    assert "id" not in ev[0] and "logged_at" not in ev[0]


def test_liquidity_only_codes_are_left_out(data_dir):
    c = EF.build_feed(data_dir, NOW)["companies"]
    assert "1306" not in c
    assert c["4203"]["liquidity"]["days_to_exit_100m"] == 0.3
    assert c["4203"]["perf_vs_topix"] == {"vs_topix_3m": -12.5, "vs_topix_12m": 3.1, "as_of": "2026-11-10"}


def test_feed_carries_no_portfolio_data(data_dir):
    """The repo is public. A watchlist file sitting in data/ must not leak in."""
    with open(os.path.join(data_dir, "watchlist.json"), "w") as f:
        json.dump({"9999": {"name": "Secret Holding"}}, f)
    text = EF.render(EF.build_feed(data_dir, NOW))
    assert "9999" not in text and "Secret Holding" not in text


def test_rendered_file_is_json_and_only_rewritten_on_change(data_dir, tmp_path):
    out = str(tmp_path / "export" / "digest_feed.json")
    feed = EF.build_feed(data_dir, NOW)
    assert EF.write_if_changed(feed, out) is True
    assert json.load(open(out, encoding="utf-8")) == feed
    later = EF.build_feed(data_dir, NOW.replace(minute=30))   # same content, new timestamp
    assert later["generated_at"] != feed["generated_at"]
    assert EF.write_if_changed(later, out) is False
    with open(out, encoding="utf-8") as f:
        company_lines = [l for l in f.read().splitlines() if l.startswith('    "')]
    assert len(company_lines) == len(feed["companies"])          # one line per company
    assert company_lines[0].startswith('    "4203": {')
