# tests/services/retail_risk/test_schemas.py
import datetime
from services.retail_risk import schemas as s


def test_book_name_and_spot_alias():
    assert s.book_name("冀南") == "景融售电-冀南"
    assert s.spot_province("冀南") == "河北南网"
    assert s.spot_province("山东") == "山东"


def test_term_channel_instrument_map():
    assert s.TERM_CHANNEL_INSTRUMENT["年度双边"] == ("annual", "bilateral")
    assert s.TERM_CHANNEL_INSTRUMENT["月度竞价"] == ("monthly_auction", "forward")
    assert s.TERM_CHANNEL_INSTRUMENT["月度挂牌"] == ("monthly_listed", "forward")
    assert s.TERM_CHANNEL_INSTRUMENT["日滚动"] == ("intramonth_match", "forward")


def test_category_longest_prefix():
    # 河北: 0202030002 偏差收益回收 -> imbalance, although 0202 -> market_redistribution
    assert s.category_for_code("0202030002", s.JINAN_CATEGORY_RULES) == "imbalance"
    assert s.category_for_code("0101", s.JINAN_CATEGORY_RULES) == "midlong_energy"
    assert s.category_for_code("01020203", s.JINAN_CATEGORY_RULES) == "spot_energy"
    assert s.category_for_code("9999", s.JINAN_CATEGORY_RULES) == "other"
    # 安徽 uses different code for 中长期
    assert s.category_for_code("01010201", s.ANHUI_CATEGORY_RULES) == "midlong_energy"


def test_frame_columns():
    assert s.TRADES_COLS == ["delivery_date", "hour", "channel", "instrument_type",
                             "direction", "volume_mwh", "price_cny_mwh",
                             "counterparty", "source_term", "source_file"]
    assert "estimated" in s.VOLUMES_COLS
