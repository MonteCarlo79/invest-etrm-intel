from . import compute as c


def test_run_hours_and_gross_margin():
    assert sum(c.run_hours_no_constraints()) == 9
    assert abs(c.margin_no_constraints() - 12_900) < 1e-9


def test_blocks():
    bl = c.blocks()
    assert [(b, e) for b, e, _ in bl] == [(6, 8), (15, 20)]
    assert abs(bl[0][2] - 900) < 1e-9 and abs(bl[1][2] - 12_000) < 1e-9


def test_optimal_skips_morning_starts_evening():
    sched, margin, nstarts = c.optimal_schedule()
    assert sum(sched) == 6 and nstarts == 1
    assert sched[15] == 1 and sched[6] == 0
    assert abs(margin - 7_000) < 1e-9


def test_dip_extension_rejected():
    bl = c.blocks()
    dip_loss = sum(c.MEL * (c.K - p) for p in c.PRICES[9:15])
    assert abs(dip_loss - 4_200) < 1e-9
    assert bl[0][2] - dip_loss < 0          # extending back through the dip loses money


def test_morning_start_would_pay_with_higher_prices():
    rich = [70.0] * 3 + c.PRICES[9:]
    sched, margin, nstarts = c.optimal_schedule(rich)
    # morning block now nets 4500-5000 <0 still; bump more:
    richer = [80.0] * 3 + c.PRICES[9:]
    sched, margin, nstarts = c.optimal_schedule(richer)
    assert sched[6] == 1                     # now the morning start pays
