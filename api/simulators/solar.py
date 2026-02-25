"""Synthetic solar power production simulator.

Converts a time series of solar irradiance (GHI, W/m²) into an estimated
AC power output time series for a PV installation.

Model
-----
GHI is used as a direct proxy for plane-of-array irradiance.  Tilt and
azimuth corrections are folded into the performance ratio, which is
physically equivalent to assuming a moderately south-tilted array in a
mid-latitude location.

The production model has three stages:

1.  **Irradiance → DC power** using the STC normalisation:

        P_dc[n] = P_installed × (G[n] / G_stc)

    where ``G_stc = 1000 W/m²`` is the Standard Test Condition reference.

2.  **Temperature derating** accounts for the reduction in panel efficiency
    as cells heat above the 25 °C STC reference.  Cell temperature is
    estimated from ambient temperature and irradiance via the NOCT
    (Nominal Operating Cell Temperature) approximation:

        T_cell[n] = T_amb[n] + delta_t × (G[n] / G_stc)

    where ``delta_t ≈ 25 °C`` represents the cell-above-ambient rise at
    full irradiance.  The derating factor is then:

        f_temp[n] = 1 + γ × (T_cell[n] − 25)

    with ``γ ≈ −0.004 /°C`` (a typical crystalline silicon value).
    When no temperature series is supplied, T_cell is fixed at 25 °C so
    the derating factor is identically 1.0.

3.  **System losses** are captured by a single performance ratio:

        P_ac[n] = P_dc[n] × f_temp[n] × PR

    ``PR ≈ 0.80`` covers inverter efficiency, wiring losses, soiling, and
    clipping in aggregate.

Combined:

    P_ac[n] = P_installed × (G[n] / 1000) × PR × (1 + γ × (T_cell[n] − 25))
"""

from __future__ import annotations

from typing import Sequence

_G_STC = 1000.0  # W/m² — Standard Test Condition irradiance reference
_NOCT_DELTA_T = 25.0  # °C — typical cell temperature rise above ambient at 1000 W/m²

class SolarSimulator:
    """Estimate AC solar power output from a GHI irradiance time series.

    Produces one power value per irradiance sample using the STC
    normalisation, an optional NOCT-based temperature derating, and a
    fixed performance ratio that aggregates all other system losses.

    Args:
        installed_capacity_kw: Peak DC capacity of the installation at STC
                               (kWp).  This is the nameplate figure typically
                               quoted by installers.
        performance_ratio:     Dimensionless system efficiency factor
                               accounting for inverter efficiency, wiring
                               losses, soiling, and clipping.  Defaults to
                               0.80, a reasonable mid-range value.
        temp_coefficient:      Panel power temperature coefficient (per °C).
                               Defaults to −0.004 /°C, typical for
                               crystalline silicon.  Set to 0 to disable
                               temperature derating entirely.
        interval_hours:        Duration of each time step in hours.  Does not
                               affect the power calculation but is recorded
                               for consistency with sibling simulators.
    """

    def __init__(
        self,
        installed_capacity_kw: float,
        performance_ratio: float,
        temp_coefficient: float,
    ) -> None:
        if installed_capacity_kw <= 0:
            raise ValueError("installed_capacity_kw must be positive")
        if not 0.0 < performance_ratio <= 1.0:
            raise ValueError("performance_ratio must be in (0, 1]")

        self.installed_capacity_kw = installed_capacity_kw
        self.performance_ratio = performance_ratio
        self.temp_coefficient = temp_coefficient

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _cell_temperature(self, ambient_temp: float, ghi: float) -> float:
        """Estimate cell temperature using the NOCT approximation.

        Args:
            t_amb: Ambient temperature (°C).
            ghi:   Global horizontal irradiance (W/m²).

        Returns:
            Estimated cell temperature (°C).
        """
        return ambient_temp + _NOCT_DELTA_T * (ghi / _G_STC)

    def _temp_derating(self, t_cell: float) -> float:
        """Return the temperature derating factor for a given cell temperature.

        The factor equals 1.0 at STC (25 °C) and decreases linearly above it.
        Values below 25 °C produce a slight gain, which is physically correct.

        Args:
            t_cell: Cell temperature (°C).

        Returns:
            Derating factor (dimensionless).
        """
        return 1.0 + self.temp_coefficient * (t_cell - 25.0)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def simulate(
        self,
        irradiance: Sequence[float],
        temperatures: Sequence[float] | None = None,
    ) -> list[float]:
        """Convert a GHI irradiance series into an AC power output series.

        Each element of the output corresponds to the same time step as the
        input irradiance sample.  If ``temperatures`` is supplied it must be
        the same length as ``irradiance``; each value is the concurrent
        ambient temperature (°C).  When omitted, cells are assumed to operate
        at 25 °C (STC), so temperature derating is effectively disabled.

        Args:
            irradiance:   Sequence of GHI values (W/m²).  Negative values
                          are clamped to zero.
            temperatures: Optional concurrent ambient temperatures (°C).
                          Must have the same length as ``irradiance`` when
                          provided.

        Returns:
            List of estimated AC power outputs (kW), one per input sample.

        Raises:
            ValueError: If ``temperatures`` is provided but has a different
                        length from ``irradiance``.
        """
        if temperatures is not None and len(temperatures) != len(irradiance):
            raise ValueError(
                f"temperatures length ({len(temperatures)}) must match "
                f"irradiance length ({len(irradiance)})"
            )

        output: list[float] = []

        for i, ghi in enumerate(irradiance):
            ghi = max(ghi, 0.0)  # clamp — sensors occasionally return small negatives at night

            # DC power normalised by STC irradiance
            p_dc = self.installed_capacity_kw * (ghi / _G_STC)

            # Temperature derating
            if temperatures is not None:
                t_cell = self._cell_temperature(temperatures[i], ghi)
            else:
                t_cell = 25.0
            f_temp = self._temp_derating(t_cell)

            # AC output after system losses
            p_ac = p_dc * f_temp * self.performance_ratio

            output.append(max(p_ac, 0.0))  # clamp — derating can't make output negative

        return output

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def peak_output_kw(self) -> float:
        """Theoretical peak AC output at STC with no temperature derating (kW).

        Equals ``installed_capacity_kw × performance_ratio``.
        """
        return self.installed_capacity_kw * self.performance_ratio

    def capacity_factor(
        self,
        irradiance: Sequence[float],
        temperatures: Sequence[float] | None = None,
    ) -> float:
        """Mean output as a fraction of installed capacity over the series.

        Args:
            irradiance:   GHI time series (W/m²).
            temperatures: Optional ambient temperature series (°C).

        Returns:
            Capacity factor in [0, 1].

        Raises:
            ValueError: If ``irradiance`` is empty.
        """
        if not irradiance:
            raise ValueError("irradiance must not be empty")

        series = self.simulate(irradiance, temperatures)
        return sum(series) / (self.installed_capacity_kw * len(series))
