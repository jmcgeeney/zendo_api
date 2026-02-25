"""Tests for lib.time_util."""

import pytest
from datetime import date, datetime, timedelta

from lib.time_util import (
    date_to_datetime,
    day_window,
    downsample_factor,
    interval_hours,
    interval_minutes,
    interval_timedelta,
    intervals_per_day,
    timestamps_for_day,
)


# ---------------------------------------------------------------------------
# interval_hours
# ---------------------------------------------------------------------------


def test_interval_hours_hourly():
    assert interval_hours("hourly") == 1.0


def test_interval_hours_30m():
    assert interval_hours("30m") == 0.5


def test_interval_hours_15m():
    assert interval_hours("15m") == 0.25


# ---------------------------------------------------------------------------
# interval_minutes
# ---------------------------------------------------------------------------


def test_interval_minutes_hourly():
    assert interval_minutes("hourly") == 60


def test_interval_minutes_30m():
    assert interval_minutes("30m") == 30


def test_interval_minutes_15m():
    assert interval_minutes("15m") == 15


# ---------------------------------------------------------------------------
# downsample_factor
# ---------------------------------------------------------------------------


def test_downsample_factor_hourly():
    assert downsample_factor("hourly") == 1


def test_downsample_factor_30m():
    assert downsample_factor("30m") == 2


def test_downsample_factor_15m():
    assert downsample_factor("15m") == 4


# ---------------------------------------------------------------------------
# intervals_per_day
# ---------------------------------------------------------------------------


def test_intervals_per_day_hourly():
    assert intervals_per_day("hourly") == 24


def test_intervals_per_day_30m():
    assert intervals_per_day("30m") == 48


def test_intervals_per_day_15m():
    assert intervals_per_day("15m") == 96


# ---------------------------------------------------------------------------
# interval_timedelta
# ---------------------------------------------------------------------------


def test_interval_timedelta_hourly():
    assert interval_timedelta("hourly") == timedelta(hours=1)


def test_interval_timedelta_30m():
    assert interval_timedelta("30m") == timedelta(minutes=30)


def test_interval_timedelta_15m():
    assert interval_timedelta("15m") == timedelta(minutes=15)


# ---------------------------------------------------------------------------
# date_to_datetime
# ---------------------------------------------------------------------------


def test_date_to_datetime_returns_midnight():
    d = date(2025, 6, 15)
    dt = date_to_datetime(d)
    assert dt == datetime(2025, 6, 15, 0, 0, 0)


def test_date_to_datetime_is_naive():
    dt = date_to_datetime(date(2024, 1, 1))
    assert dt.tzinfo is None


def test_date_to_datetime_preserves_date():
    d = date(2026, 12, 31)
    dt = date_to_datetime(d)
    assert dt.year == d.year
    assert dt.month == d.month
    assert dt.day == d.day


# ---------------------------------------------------------------------------
# day_window
# ---------------------------------------------------------------------------


def test_day_window_start_is_midnight():
    d = date(2025, 3, 10)
    start, _ = day_window(d)
    assert start == datetime(2025, 3, 10, 0, 0, 0)


def test_day_window_end_is_next_midnight():
    d = date(2025, 3, 10)
    _, end = day_window(d)
    assert end == datetime(2025, 3, 11, 0, 0, 0)


def test_day_window_span_is_24_hours():
    d = date(2025, 3, 10)
    start, end = day_window(d)
    assert (end - start) == timedelta(days=1)


# ---------------------------------------------------------------------------
# timestamps_for_day
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("interval,expected_count", [
    ("hourly", 24),
    ("30m", 48),
    ("15m", 96),
])
def test_timestamps_for_day_count(interval, expected_count):
    d = date(2025, 1, 1)
    ts = timestamps_for_day(d, interval)
    assert len(ts) == expected_count


def test_timestamps_for_day_first_is_midnight():
    d = date(2025, 4, 20)
    ts = timestamps_for_day(d, "15m")
    assert ts[0] == datetime(2025, 4, 20, 0, 0, 0)


def test_timestamps_for_day_last_is_not_next_day():
    d = date(2025, 4, 20)
    ts = timestamps_for_day(d, "15m")
    assert ts[-1] < datetime(2025, 4, 21, 0, 0, 0)


def test_timestamps_for_day_evenly_spaced():
    d = date(2025, 6, 1)
    ts = timestamps_for_day(d, "30m")
    delta = interval_timedelta("30m")
    for i in range(1, len(ts)):
        assert ts[i] - ts[i - 1] == delta


def test_timestamps_for_day_all_same_date():
    d = date(2025, 8, 15)
    ts = timestamps_for_day(d, "hourly")
    for t in ts:
        assert t.date() == d
