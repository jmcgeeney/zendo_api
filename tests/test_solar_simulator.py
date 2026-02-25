"""Basic tests for api.simulators.solar.SolarSimulator."""

import pytest

from api.simulators.solar import SolarSimulator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sim() -> SolarSimulator:
    """Default 100 kWp simulator."""
    return SolarSimulator(installed_capacity_kw=100.0, performance_ratio=0.80, temp_coefficient=-0.004, jitter=0.0)


# ---------------------------------------------------------------------------
# Construction / validation
# ---------------------------------------------------------------------------


def test_valid_construction():
    sim = SolarSimulator(installed_capacity_kw=50.0, performance_ratio=0.80, temp_coefficient=-0.004, jitter=0.0)
    assert sim.installed_capacity_kw == 50.0
    assert sim.performance_ratio == 0.80
    assert sim.jitter == 0.0


def test_invalid_capacity():
    with pytest.raises(ValueError, match="installed_capacity_kw must be positive"):
        SolarSimulator(installed_capacity_kw=0.0, performance_ratio=0.80, temp_coefficient=-0.004)


def test_invalid_performance_ratio():
    with pytest.raises(ValueError, match="performance_ratio must be in"):
        SolarSimulator(installed_capacity_kw=10.0, performance_ratio=0.0, temp_coefficient=-0.004)


# ---------------------------------------------------------------------------
# simulate() — basic behaviour
# ---------------------------------------------------------------------------


def test_zero_irradiance_gives_zero_output(sim):
    result = sim.simulate([0.0, 0.0, 0.0])
    assert result == [0.0, 0.0, 0.0]


def test_negative_irradiance_clamped_to_zero(sim):
    result = sim.simulate([-50.0, -0.001])
    assert result == [0.0, 0.0]


def test_stc_irradiance_at_stc_temperature(sim):
    """At 1000 W/m² and T_amb=25 °C the NOCT model raises T_cell to 50 °C.

    T_cell = 25 + 25 * (1000/1000) = 50 °C
    f_temp = 1 + (-0.004) * (50 - 25) = 0.90
    P_ac   = 100 × 1.0 × 0.80 × 0.90 = 72 kW
    """
    result = sim.simulate([1000.0], temperatures=[25.0])
    assert len(result) == 1
    assert result[0] == pytest.approx(72.0)


def test_stc_irradiance_no_temperature(sim):
    """Without a temperature series, derating is disabled (T_cell = 25 °C)."""
    result = sim.simulate([1000.0])
    assert result[0] == pytest.approx(100.0 * 0.80)


def test_output_length_matches_input(sim):
    irr = [200.0, 400.0, 600.0, 800.0, 1000.0]
    result = sim.simulate(irr)
    assert len(result) == len(irr)


def test_higher_irradiance_yields_more_power(sim):
    low, high = sim.simulate([200.0]), sim.simulate([800.0])
    assert high[0] > low[0]


def test_temperature_mismatch_raises(sim):
    with pytest.raises(ValueError, match="temperatures length"):
        sim.simulate([500.0, 600.0], temperatures=[25.0])


def test_high_temperature_reduces_output(sim):
    cool = sim.simulate([1000.0], temperatures=[10.0])
    hot = sim.simulate([1000.0], temperatures=[40.0])
    assert cool[0] > hot[0]


# ---------------------------------------------------------------------------
# peak_output_kw
# ---------------------------------------------------------------------------


def test_peak_output_kw(sim):
    assert sim.peak_output_kw() == pytest.approx(100.0 * 0.80)


# ---------------------------------------------------------------------------
# capacity_factor
# ---------------------------------------------------------------------------


def test_capacity_factor_at_stc(sim):
    """Constant STC irradiance, no temperature → CF = PR."""
    cf = sim.capacity_factor([1000.0] * 10)
    assert cf == pytest.approx(0.80)


def test_capacity_factor_empty_raises(sim):
    with pytest.raises(ValueError, match="irradiance must not be empty"):
        sim.capacity_factor([])


def test_capacity_factor_zero_irradiance(sim):
    assert sim.capacity_factor([0.0, 0.0, 0.0]) == pytest.approx(0.0)
