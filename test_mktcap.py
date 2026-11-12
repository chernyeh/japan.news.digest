"""Market cap at the latest close, not the stale stored snapshot."""
import jquants as J


def test_mktcap_is_close_times_shares_and_stored_only_fills_gaps():
    prices = [{"Code": "8050", "Close": "9842"}, {"Code": "8136", "Close": "1126"},
              {"Code": "9999", "Close": ""}]
    shares = J.shares_map_from_rows(
        [{"Code": "8050", "SharesOutstanding": "81756000"}],
        [{"Code": "8050", "Shares": "1"}, {"Code": "8136", "Shares": "1212439000"}])
    assert shares["8050"] == 81756000            # J-Quants beats metadata
    stored = [{"Code": "8050", "MarketCapB": "515.0"}, {"Code": "1301", "MarketCapB": "60.6"},
              {"Code": "9999", "MarketCapB": "12"}]
    mc = J.mktcap_from_rows(prices, shares, stored)
    assert mc["8050"] == round(9842 * 81756000 / 1e9, 2)   # not the stale 515
    assert mc["8136"] == round(1126 * 1212439000 / 1e9, 2)
    assert mc["1301"] == 60.6 and mc["9999"] == 12.0       # no current close: stored
