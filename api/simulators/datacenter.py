"""Datacenter load simulator.

Models the electrical demand of a datacenter over time given a temperature
time series.  Two thermal lag terms capture the physical realities that:

  1. Cooling plant (chillers, CRAC units) responds to ambient temperature
     with a delay of ~30-90 minutes (``tau_cooling_hours``).
  2. The building's thermal mass absorbs heat slowly, keeping the interior
     warm for hours after a spike (``tau_mass_hours``).

PUE is therefore a function of a blended *effective* temperature rather than
the instantaneous reading, so a sudden spike from a cold night produces a
lower cooling penalty than a sustained hot period.

Physical model
--------------

Each lag is an exponential moving average:

    T_short[n] = T_short[n-1] + (dt / tau_short) * (T_amb[n] - T_short[n-1])
    T_long[n]  = T_long[n-1]  + (dt / tau_long)  * (T_amb[n] - T_long[n-1])

Effective temperature:

    T_eff[n] = alpha * T_short[n] + (1 - alpha) * T_long[n]

PUE:

    PUE[n] = pue_base + pue_temp_coeff * max(0, T_eff[n] - t_setpoint)

Total facility power:

    P[n] = it_load_kw * utilisation * PUE[n]
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence


@dataclass
class DatacenterSimulator:
    """
    Simple simulator for datacenter power demand based on ambient temperature.
    """

    it_load_kw: float
    utilisation: float = 0.50
    pue_base: float = 1.40
    pue_temp_coeff: float = 0.01
    t_setpoint: float = 20.0
    tau_cooling_hours: float = 1.0
    tau_mass_hours: float = 6.0
    alpha: float = 0.70
    interval_hours: float = 0.50

    # ------------------------------------------------------------------ #
    # Validation                                                           #
    # ------------------------------------------------------------------ #

    def __post_init__(self) -> None:
        if not 0.0 < self.utilisation <= 1.0:
            raise ValueError("utilisation must be in (0, 1]")
        if self.pue_base < 1.0:
            raise ValueError("pue_base must be >= 1.0")
        if not 0.0 <= self.alpha <= 1.0:
            raise ValueError("alpha must be in [0, 1]")
        if self.tau_cooling_hours <= 0 or self.tau_mass_hours <= 0:
            raise ValueError("time constants must be positive")
        if self.interval_hours <= 0:
            raise ValueError("interval_hours must be positive")

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def simulate(
        self,
        temperatures: Sequence[float],
        t_initial: float | None = None,
    ) -> list[float]:
        """
        Given a time series of ambient temperatures, return the corresponding datacenter power demand at each time step.
        """
        if not temperatures:
            return []

        t0 = t_initial if t_initial is not None else temperatures[0]
        t_short = t0
        t_long = t0

        dt = self.interval_hours
        k_short = dt / self.tau_cooling_hours
        k_long = dt / self.tau_mass_hours

        # Clamp to [0, 1] in case tau < dt (very short time constants)
        k_short = min(k_short, 1.0)
        k_long = min(k_long, 1.0)

        loads: list[float] = []

        for t_amb in temperatures:
            # Update both lag states
            t_short += k_short * (t_amb - t_short)
            t_long += k_long * (t_amb - t_long)

            # Blend into a single effective temperature
            t_eff = self.alpha * t_short + (1.0 - self.alpha) * t_long

            # PUE rises only above the free-cooling setpoint
            pue = self.pue_base + self.pue_temp_coeff * max(0.0, t_eff - self.t_setpoint)

            # Total facility power = IT load × utilisation × PUE
            loads.append(self.it_load_kw * self.utilisation * pue)

        return loads

    # ------------------------------------------------------------------ #
    # Convenience                                                          #
    # ------------------------------------------------------------------ #

    @property
    def it_power_kw(self) -> float:
        """Active IT power draw independent of cooling (kW)."""
        return self.it_load_kw * self.utilisation

    @property
    def pue_at_setpoint(self) -> float:
        """PUE when ambient is exactly at the free-cooling setpoint."""
        return self.pue_base

    def steady_state_load(self, temperature: float) -> float:
        """Facility power (kW) after infinitely long exposure to *temperature*.

        Useful for sanity-checking parameters without running a full
        simulation.
        """
        pue = self.pue_base + self.pue_temp_coeff * max(0.0, temperature - self.t_setpoint)
        return self.it_load_kw * self.utilisation * pue
