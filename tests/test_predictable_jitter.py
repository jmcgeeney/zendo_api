"""Tests for lib.predictable_jitter."""

import pytest

from lib.predictable_jitter import predictable_jitter


# ---------------------------------------------------------------------------
# Return type and rounding
# ---------------------------------------------------------------------------


def test_returns_numeric():
    result = predictable_jitter(42.0)
    assert isinstance(result, (int, float))


def test_default_round_to_two_decimal_places():
    result = predictable_jitter(1.23456789)
    # Two decimal places means at most 2 digits after the point
    assert round(result, 2) == result


def test_custom_round_to():
    result = predictable_jitter(7.0, round_to=0)
    assert result == int(result)


# ---------------------------------------------------------------------------
# Range
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", [0.0, 1.0, 42.0, -7.5, 999.99, 1e6])
def test_default_range(value):
    result = predictable_jitter(value)
    assert -10.0 <= result <= 10.0


@pytest.mark.parametrize("jitter_range", [1.0, 5.0, 50.0, 100.0])
def test_custom_range(jitter_range):
    for v in [0.0, 1.0, 123.456]:
        result = predictable_jitter(v, jitter_range=jitter_range)
        assert -jitter_range <= result <= jitter_range


def test_zero_jitter_range():
    result = predictable_jitter(42.0, jitter_range=0.0)
    assert result == 0.0


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_same_input_same_output():
    assert predictable_jitter(3.14) == predictable_jitter(3.14)


def test_same_input_repeated_calls():
    val = 123.456
    results = [predictable_jitter(val) for _ in range(10)]
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# Sensitivity to input
# ---------------------------------------------------------------------------


def test_different_inputs_can_differ():
    results = {predictable_jitter(float(i)) for i in range(20)}
    # With 20 distinct inputs it's overwhelmingly likely to get >1 distinct output
    assert len(results) > 1


def test_close_inputs_may_differ():
    a = predictable_jitter(1.0)
    b = predictable_jitter(2.0)
    # Values are hash-derived so they should differ for distinct floats
    assert a != b
