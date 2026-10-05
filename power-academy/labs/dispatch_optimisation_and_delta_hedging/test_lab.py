import numpy as np

from . import compute as c


def _week():
    return c.synthetic_week()


def test_dp_beats_greedy():
    s = _week()
    p_dp, _ = c.dp_dispatch(s)
    p_gr, _ = c.greedy_dispatch(s)
    assert p_dp >= p_gr
    assert abs((p_dp - p_gr) - 21_943) < 2_000


def test_schedule_respects_constraints():
    s = _week()
    _, sched = c.dp_dispatch(s)
    assert sched.sum() == 118
    starts = int(np.diff(np.r_[0, sched]).clip(0).sum())
    assert starts == 6
    # min up: every run is at least MIN_UP hours
    d = np.diff(np.r_[0, sched, 0])
    runs = list(zip(np.where(d == 1)[0], np.where(d == -1)[0]))
    assert all(e - b >= c.MIN_UP for b, e in runs)
    # min down: every gap between a run end and the next start is >= MIN_DOWN hours
    starts_i = np.where(d == 1)[0]
    ends_i = np.where(d == -1)[0]
    gaps = zip(ends_i[:-1], starts_i[1:])
    assert all(nxt - end >= c.MIN_DOWN for end, nxt in gaps)


def test_deltas_track_run_hours():
    s = _week()
    _, sched = c.dp_dispatch(s)
    deltas = c.day_deltas(s)
    assert (deltas >= 0).all()
    runh = np.array([sched[d * 24:(d + 1) * 24].sum() for d in range(7)])
    assert np.abs(deltas - runh).max() <= 2.0
    assert abs(deltas.sum() / runh.sum() - 1) < 0.05


def test_profit_monotone_in_curve_shift():
    s = _week()
    v0, _ = c.dp_dispatch(s)
    v1, _ = c.dp_dispatch(s + 5.0)
    assert v1 > v0
