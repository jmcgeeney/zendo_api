"""Tests for lib.series_util — interpolate_steps, interpolate, and interpolate_time_series."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from lib.series_util import interpolate, interpolate_steps, interpolate_time_series
from lib.types import TimeSeriesPoint


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ts(hour: int, minute: int = 0) -> datetime:
    """Return a UTC datetime for a fixed date at the given hour/minute."""
    return datetime(2026, 2, 25, hour, minute, tzinfo=timezone.utc)


def make_point(hour: int, value: Optional[float], minute: int = 0) -> TimeSeriesPoint:
    return TimeSeriesPoint(timestamp=ts(hour, minute), value=value)


# ---------------------------------------------------------------------------
# interpolate_steps
# ---------------------------------------------------------------------------


def test_interpolate_steps_single_gap():
    # One step between 0 and 4  →  midpoint 2
    assert interpolate_steps(0.0, 4.0, 1) == pytest.approx([2.0])


def test_interpolate_steps_two_gaps():
    # Two steps between 0 and 9  →  3, 6
    assert interpolate_steps(0.0, 9.0, 2) == pytest.approx([3.0, 6.0])


def test_interpolate_steps_three_gaps():
    # Three steps between 0 and 4  →  1, 2, 3
    assert interpolate_steps(0.0, 4.0, 3) == pytest.approx([1.0, 2.0, 3.0])


def test_interpolate_steps_negative_slope():
    # Descending: 10 → 0, one step  →  5
    assert interpolate_steps(10.0, 0.0, 1) == pytest.approx([5.0])


def test_interpolate_steps_same_values():
    # Both anchors equal → all steps return that value
    assert interpolate_steps(7.0, 7.0, 3) == pytest.approx([7.0, 7.0, 7.0])


def test_interpolate_steps_left_none():
    # No left anchor — fill with right value
    assert interpolate_steps(None, 5.0, 3) == pytest.approx([5.0, 5.0, 5.0])


def test_interpolate_steps_right_none():
    # No right anchor — fill with left value
    assert interpolate_steps(3.0, None, 2) == pytest.approx([3.0, 3.0])


def test_interpolate_steps_both_none():
    # Both anchors missing — fill with None
    assert interpolate_steps(None, None, 2) == [None, None]


def test_interpolate_steps_zero_steps():
    # Edge case: no gap slots to fill
    assert interpolate_steps(1.0, 5.0, 0) == []


# ---------------------------------------------------------------------------
# interpolate
# ---------------------------------------------------------------------------


def test_interpolate_empty():
    assert interpolate([]) == []


def test_interpolate_no_gaps():
    data = [1.0, 2.0, 3.0, 4.0]
    assert interpolate(data) == pytest.approx(data)


def test_interpolate_single_interior_gap():
    # [0, None, 4]  →  [0, 2, 4]
    assert interpolate([0.0, None, 4.0]) == pytest.approx([0.0, 2.0, 4.0])


def test_interpolate_multiple_interior_gaps():
    # [0, None, None, 9]  →  [0, 3, 6, 9]
    assert interpolate([0.0, None, None, 9.0]) == pytest.approx([0.0, 3.0, 6.0, 9.0])


def test_interpolate_consecutive_gaps():
    # Two separate single-gap runs
    # [0, None, 2, 4, None, 6]  →  [0, 1, 2, 4, 5, 6]
    assert interpolate([0.0, None, 2.0, 4.0, None, 6.0]) == pytest.approx(
        [0.0, 1.0, 2.0, 4.0, 5.0, 6.0]
    )


def test_interpolate_leading_none():
    # Leading Nones have no left anchor — should be filled with first known value
    result = interpolate([None, None, 4.0, 8.0])
    assert result[2] == pytest.approx(4.0)
    assert result[3] == pytest.approx(8.0)
    # Leading values use right anchor (no left available)
    assert result[0] == pytest.approx(4.0)
    assert result[1] == pytest.approx(4.0)


def test_interpolate_trailing_none():
    # Trailing Nones have no right anchor — should be filled with last known value
    result = interpolate([0.0, 2.0, None, None])
    assert result[0] == pytest.approx(0.0)
    assert result[1] == pytest.approx(2.0)
    # Trailing values use left anchor (no right available)
    assert result[2] == pytest.approx(2.0)
    assert result[3] == pytest.approx(2.0)


def test_interpolate_all_none():
    # All None — nothing to anchor from, values stay None
    result = interpolate([None, None, None])
    assert result == [None, None, None]


def test_interpolate_single_known_value():
    result = interpolate([None, 5.0, None])
    assert result == pytest.approx([5.0, 5.0, 5.0])


def test_interpolate_does_not_mutate_input():
    original = [0.0, None, 4.0]
    interpolate(original)
    assert original == [0.0, None, 4.0]


def test_interpolate_float_precision():
    # Values should be linearly spaced with no rounding surprises
    result = interpolate([0.0, None, 1.0])
    assert result == pytest.approx([0.0, 0.5, 1.0])


# ---------------------------------------------------------------------------
# interpolate_time_series
# ---------------------------------------------------------------------------


def test_interpolate_time_series_empty():
    assert interpolate_time_series([]) == []


def test_interpolate_time_series_no_gaps():
    points = [make_point(0, 10.0), make_point(1, 20.0), make_point(2, 30.0)]
    result = interpolate_time_series(points)
    assert [p.value for p in result] == pytest.approx([10.0, 20.0, 30.0])


def test_interpolate_time_series_single_interior_gap():
    # [0.0, None, 4.0]  →  [0.0, 2.0, 4.0]
    points = [make_point(0, 0.0), make_point(1, None), make_point(2, 4.0)]
    result = interpolate_time_series(points)
    assert [p.value for p in result] == pytest.approx([0.0, 2.0, 4.0])


def test_interpolate_time_series_multiple_interior_gaps():
    # [0, None, None, 9]  →  [0, 3, 6, 9]
    points = [
        make_point(0, 0.0),
        make_point(1, None),
        make_point(2, None),
        make_point(3, 9.0),
    ]
    result = interpolate_time_series(points)
    assert [p.value for p in result] == pytest.approx([0.0, 3.0, 6.0, 9.0])


def test_interpolate_time_series_leading_gap():
    # No left anchor — leading Nones filled with first known value
    points = [make_point(0, None), make_point(1, None), make_point(2, 6.0)]
    result = interpolate_time_series(points)
    assert [p.value for p in result] == pytest.approx([6.0, 6.0, 6.0])


def test_interpolate_time_series_trailing_gap():
    # No right anchor — trailing Nones filled with last known value
    points = [make_point(0, 4.0), make_point(1, None), make_point(2, None)]
    result = interpolate_time_series(points)
    assert [p.value for p in result] == pytest.approx([4.0, 4.0, 4.0])


def test_interpolate_time_series_all_none():
    points = [make_point(0, None), make_point(1, None)]
    result = interpolate_time_series(points)
    assert all(p.value is None for p in result)


def test_interpolate_time_series_single_known_value():
    # Single known value surrounded by Nones — all filled with that value
    points = [make_point(0, None), make_point(1, 7.0), make_point(2, None)]
    result = interpolate_time_series(points)
    assert [p.value for p in result] == pytest.approx([7.0, 7.0, 7.0])


def test_interpolate_time_series_preserves_timestamps():
    # Timestamps must be unchanged after interpolation
    points = [make_point(0, 0.0), make_point(1, None), make_point(2, 2.0)]
    result = interpolate_time_series(points)
    assert [p.timestamp for p in result] == [ts(0), ts(1), ts(2)]


def test_interpolate_time_series_does_not_mutate_input():
    points = [make_point(0, 0.0), make_point(1, None), make_point(2, 4.0)]
    original_values = [p.value for p in points]
    interpolate_time_series(points)
    assert [p.value for p in points] == original_values


def test_interpolate_time_series_returns_new_objects():
    # The returned list should contain new objects, not the same instances
    points = [make_point(0, 0.0), make_point(1, None), make_point(2, 4.0)]
    result = interpolate_time_series(points)
    assert result is not points
    assert all(r is not p for r, p in zip(result, points))


def test_interpolate_time_series_consecutive_gap_runs():
    # [0, None, 2, 4, None, 6]  →  [0, 1, 2, 4, 5, 6]
    points = [
        make_point(0, 0.0),
        make_point(1, None),
        make_point(2, 2.0),
        make_point(3, 4.0),
        make_point(4, None),
        make_point(5, 6.0),
    ]
    result = interpolate_time_series(points)
    assert [p.value for p in result] == pytest.approx([0.0, 1.0, 2.0, 4.0, 5.0, 6.0])


def test_interpolate_time_series_single_point():
    points = [make_point(0, 42.0)]
    result = interpolate_time_series(points)
    assert result[0].value == pytest.approx(42.0)
