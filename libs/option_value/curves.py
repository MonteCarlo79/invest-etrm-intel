"""Expected-curve construction for the option-value pricing engine.

Pure computation only: no DB, no file I/O, no streamlit.
Prices are ¥/MWh internally everywhere.
"""

from datetime import date

import numpy as np
import pandas as pd

from .contracts import ExpectedCurve

_DAYS_PER_MONTH = 30.44
_MIN_COVERAGE_MONTHS = 11.5


def build_expected_curve(
    prices: pd.Series, window_end: date, months: int = 12
) -> ExpectedCurve:
    """Build the expected price curve from an hourly price series.

    Parameters
    ----------
    prices:
        Hourly Series with a naive datetime index, ¥/MWh, already
        normalized/fallback-filled by the readers.
    window_end:
        Anchor date of the history window.
    months:
        Depth of the history window in months.

    Window semantics: ``window_start`` is the first day of the calendar
    month ``months`` before ``window_end``; days before ``window_start``
    are excluded. Days after ``window_end`` are kept — the series is
    expected to end near ``window_end`` by construction.

    A *complete day* has exactly 24 non-NaN hourly values; incomplete
    days are dropped. ``month_hour_mean[m % 12, h]`` is the mean price
    for calendar month ``m`` and hour ``h`` over the complete days in
    the window (row 0 = December, rows 1-11 = January-November).
    ``hourly`` is the tiled curve: each complete day's 24 hours replaced
    by its month-hour row, in chronological order. ``floor``/``cap`` are
    the observed min/max of the complete-day series.
    """
    window_start = (
        (pd.Timestamp(window_end) - pd.DateOffset(months=months))
        .replace(day=1)
        .date()
    )

    s = prices[prices.index.date >= window_start]

    if s.empty:
        return ExpectedCurve(
            province="",
            window_start=window_start,
            window_end=window_end,
            month_hour_mean=np.full((12, 24), np.nan),
            hourly=np.empty(0),
            floor=float("nan"),
            cap=float("nan"),
            n_days=0,
            coverage_months=0.0,
            insufficient_history=True,
        )

    # Complete days: exactly 24 non-NaN hourly values.
    hours_per_day = s.groupby(s.index.date).count()  # count() ignores NaN
    complete_days = set(hours_per_day[hours_per_day == 24].index)

    frame = pd.DataFrame(
        {
            "price": s.to_numpy(dtype=float),
            "day": s.index.date,
            "month": s.index.month.to_numpy(),
            "hour": s.index.hour.to_numpy(),
        }
    )
    frame = frame[frame["day"].isin(complete_days)]

    mat = frame.pivot_table(
        index="day", columns="hour", values="price", aggfunc="mean"
    ).reindex(columns=range(24))
    mat = mat.sort_index()
    values = mat.to_numpy(dtype=float)  # (n_days, 24), chronological rows
    row_months = np.array([d.month for d in mat.index])
    n_days = int(values.shape[0])

    month_hour_mean = np.full((12, 24), np.nan)
    for m in range(1, 13):
        rows = values[row_months == m]
        if rows.size:
            month_hour_mean[m % 12] = np.nanmean(rows, axis=0)

    hourly = (
        np.concatenate([month_hour_mean[m % 12] for m in row_months])
        if n_days
        else np.empty(0)
    )

    floor = float(np.nanmin(values)) if n_days else float("nan")
    cap = float(np.nanmax(values)) if n_days else float("nan")

    coverage_months = n_days / _DAYS_PER_MONTH

    return ExpectedCurve(
        province="",
        window_start=window_start,
        window_end=window_end,
        month_hour_mean=month_hour_mean,
        hourly=hourly,
        floor=floor,
        cap=cap,
        n_days=n_days,
        coverage_months=coverage_months,
        insufficient_history=coverage_months < _MIN_COVERAGE_MONTHS,
    )
