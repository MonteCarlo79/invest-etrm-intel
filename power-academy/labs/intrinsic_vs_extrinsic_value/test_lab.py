from . import compute as c


def test_intrinsic():
    assert c.intrinsic(c.SPREADS) == 80.0


def test_expected_total_and_extrinsic():
    v_tot = c.expected_value(c.SPREADS, c.SHOCK)
    assert v_tot == 92.5
    assert v_tot - c.intrinsic(c.SPREADS) == 12.5


def test_extrinsic_concentrates_in_negative_forward_hours():
    rows = c.hourly_table(c.SPREADS, c.SHOCK)
    extra = {i: ev - iv for i, s, ev, iv in rows}
    assert extra[1] == 5.0 and extra[3] == 7.5    # the two negative-forward hours
    assert extra[2] == 0.0 and extra[4] == 0.0    # deep ITM hours gain nothing


def test_extrinsic_non_negative_and_zero_without_shock():
    assert c.expected_value(c.SPREADS, 0.0) == c.intrinsic(c.SPREADS)
    assert c.expected_value(c.SPREADS, 50.0) >= c.intrinsic(c.SPREADS)
