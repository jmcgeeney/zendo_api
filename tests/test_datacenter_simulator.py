"""Basic tests for api.simulators.datacenter.DatacenterSimulator."""

import pytest

from api.simulators.datacenter import DatacenterSimulator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sim() -> DatacenterSimulator:
    """Default 500 kW IT-load simulator."""
    return DatacenterSimulator(it_load_kw=500.0, jitter=0.0)


# ---------------------------------------------------------------------------
# Construction / validation
# ---------------------------------------------------------------------------


def test_valid_construction():
    sim = DatacenterSimulator(it_load_kw=200.0)
    assert sim.it_load_kw == 200.0
    assert sim.pue_base == 1.40


def test_invalid_utilisation_zero():
    with pytest.raises(ValueError, match="utilisation must be in"):
        DatacenterSimulator(it_load_kw=100.0, utilisation=0.0)


def test_invalid_utilisation_above_one():
    with pytest.raises(ValueError, match="utilisation must be in"):
        DatacenterSimulator(it_load_kw=100.0, utilisation=1.5)


def test_invalid_pue_base():
    with pytest.raises(ValueError, match="pue_base must be >= 1.0"):
        DatacenterSimulator(it_load_kw=100.0, pue_base=0.9)


def test_invalid_alpha_below_zero():
    with pytest.raises(ValueError, match="alpha must be in"):
        DatacenterSimulator(it_load_kw=100.0, alpha=-0.1)


def test_invalid_alpha_above_one():
    with pytest.raises(ValueError, match="alpha must be in"):
        DatacenterSimulator(it_load_kw=100.0, alpha=1.1)


def test_invalid_tau_cooling():
    with pytest.raises(ValueError, match="time constants must be positive"):
        DatacenterSimulator(it_load_kw=100.0, tau_cooling_hours=0.0)


# ---------------------------------------------------------------------------
# simulate() — basic behaviour
# ---------------------------------------------------------------------------


def test_empty_temperatures_returns_empty(sim):
    assert sim.simulate([]) == []


def test_output_length_matches_input(sim):
    result = sim.simulate([15.0] * 24)
    assert len(result) == 24


def test_all_outputs_positive(sim):
    result = sim.simulate([0.0, 10.0, 20.0, 30.0, 40.0])
    assert all(p > 0 for p in result)


def test_hotter_ambient_yields_higher_load(sim):
    cold = sim.simulate([5.0] * 48)
    hot = sim.simulate([35.0] * 48)
    assert hot[-1] > cold[-1]


def test_below_setpoint_pue_is_base(sim):
    """When ambient never exceeds temp_setpoint, PUE should equal pue_base."""
    # Run a long series at exactly the setpoint so lag converges
    temps = [sim.temp_setpoint] * 200
    result = sim.simulate(temps)
    expected = sim.it_load_kw * sim.utilisation * sim.pue_base
    assert result[-1] == pytest.approx(expected, rel=1e-3)


def test_t_initial_overrides_first_step(sim):
    """Passing t_initial should seed the lag states rather than temperatures[0]."""
    result_default = sim.simulate([20.0], t_initial=None)
    result_hot_init = sim.simulate([20.0], t_initial=40.0)
    # Hot initial condition should produce higher first-step load
    assert result_hot_init[0] > result_default[0]


# ---------------------------------------------------------------------------
# Properties / convenience
# ---------------------------------------------------------------------------


def test_it_power_kw(sim):
    assert sim.it_power_kw == pytest.approx(500.0 * 0.50)


def test_pue_atemp_setpoint(sim):
    assert sim.pue_atemp_setpoint == pytest.approx(sim.pue_base)


def test_steady_state_load_atemp_setpoint(sim):
    expected = sim.it_load_kw * sim.utilisation * sim.pue_base
    assert sim.steady_state_load(sim.temp_setpoint) == pytest.approx(expected)


def test_steady_state_load_above_setpoint(sim):
    temp = sim.temp_setpoint + 10.0
    expected_pue = sim.pue_base + sim.pue_temp_coeff * 10.0
    expected = sim.it_load_kw * sim.utilisation * expected_pue
    assert sim.steady_state_load(temp) == pytest.approx(expected)


def test_steady_state_load_below_setpoint_equals_base(sim):
    """Below setpoint, free cooling applies and PUE stays at pue_base."""
    assert sim.steady_state_load(sim.temp_setpoint - 5.0) == pytest.approx(
        sim.it_load_kw * sim.utilisation * sim.pue_base
    )
