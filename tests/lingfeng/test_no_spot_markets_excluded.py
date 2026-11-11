"""冀北 has no spot market yet and 广州 is a regional exchange with no spot
price — both failed every LingFeng daily run (Dipeng 2026-11-11)."""
from services.lingfeng import run_daily


def test_no_spot_markets_not_ingested():
    assert not set(run_daily._NO_SPOT_MARKETS) & set(run_daily._ALL_MARKETS)
    assert len(run_daily._ALL_MARKETS) == 27 == len(set(run_daily._ALL_MARKETS))
