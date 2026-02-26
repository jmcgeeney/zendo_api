from __future__ import annotations

from typing import Sequence

from lib.predictable_jitter import predictable_jitter


class DatacenterSimulator:
    """
    Simple simulator for datacenter power demand based on ambient temperature.
    """

    def __init__(
        self,
        it_load_kw: float,
        utilisation: float = 0.50,
        pue_base: float = 1.40,
        pue_temp_coeff: float = 0.01,
        temp_setpoint: float = 20.0,
        tau_cooling_hours: float = 1.0,
        tau_mass_hours: float = 6.0,
        alpha: float = 0.70,
        interval_hours: float = 0.25,
        jitter: int = 0,
    ) -> None:
        if not 0.0 < utilisation <= 1.0:
            raise ValueError("utilisation must be in (0, 1]")
        if pue_base < 1.0:
            raise ValueError("pue_base must be >= 1.0")
        if not 0.0 <= alpha <= 1.0:
            raise ValueError("alpha must be in [0, 1]")
        if tau_cooling_hours <= 0 or tau_mass_hours <= 0:
            raise ValueError("time constants must be positive")
        if interval_hours <= 0:
            raise ValueError("interval_hours must be positive")
        self.jitter = jitter

        self.it_load_kw = it_load_kw
        self.utilisation = utilisation
        self.pue_base = pue_base
        self.pue_temp_coeff = pue_temp_coeff
        self.temp_setpoint = temp_setpoint
        self.tau_cooling_hours = tau_cooling_hours
        self.tau_mass_hours = tau_mass_hours
        self.alpha = alpha
        self.interval_hours = interval_hours

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
            pue = self.pue_base + self.pue_temp_coeff * max(0.0, t_eff - self.temp_setpoint)

            # Total facility power = IT load × utilisation × PUE
            load = self.it_load_kw * self.utilisation * pue

            jittered_load = load + predictable_jitter(t_amb, self.jitter, 2)

            loads.append(jittered_load)

        return loads

    # ------------------------------------------------------------------ #
    # Convenience                                                          #
    # ------------------------------------------------------------------ #

    @property
    def it_power_kw(self) -> float:
        """Active IT power draw independent of cooling (kW)."""
        return self.it_load_kw * self.utilisation

    @property
    def pue_atemp_setpoint(self) -> float:
        """PUE when ambient is exactly at the free-cooling setpoint."""
        return self.pue_base

    def steady_state_load(self, temperature: float) -> float:
        """Facility power (kW) after infinitely long exposure to *temperature*.

        Useful for sanity-checking parameters without running a full
        simulation.
        """
        pue = self.pue_base + self.pue_temp_coeff * max(0.0, temperature - self.temp_setpoint)
        return self.it_load_kw * self.utilisation * pue
