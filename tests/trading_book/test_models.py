from datetime import date
from libs.trading_book.models import CST_TZ, Strategy, WaterfallLine

def test_cst_tz_is_named_zone():
    assert str(CST_TZ) == "Asia/Shanghai"  # never an interval literal (POSIX inversion lesson)

def test_strategy_scope_required():
    s = Strategy(scope="nodal_agent", day=date(2026, 11, 1), series=[0.0] * 96, primary=True)
    assert s.scope == "nodal_agent" and s.primary is True and len(s.series) == 96

def test_waterfall_line_named():
    line = WaterfallLine(name="forecast_error", delta_cny=-1234.5, note="forecast vs actual")
    assert line.name == "forecast_error"
