from __future__ import annotations

from functools import cached_property
import math
from typing import Literal, Sequence
from lib.time_util import get_interval_hours
from lib.types import DailyProfile, TimeSlice

class WeatherSimulator:
    interval: TimeSlice = "30m"
    clamp: bool = False

    def __init__(self, interval: TimeSlice = "30m", clamp: bool = False) -> None:
        self.interval = interval
        self.clamp = clamp

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fit(profile: DailyProfile) -> tuple[float, float, float]:
        """Return least-squares sinusoidal coefficients (a, b, c).

        Derivation: the four anchor observations form an overdetermined
        linear system in (a, b, c). The closed-form least-squares solution
        is:

            a = mean(T_night, T_morning, T_afternoon, T_evening)
            b = (T_night    − T_afternoon) / 2
            c = (T_morning  − T_evening)   / 2
        """
        a = (profile.night + profile.morning + profile.afternoon + profile.evening) / 4.0
        b = (profile.night - profile.afternoon) / 2.0
        c = (profile.morning - profile.evening) / 2.0
        return a, b, c

    def _evaluate(self, t: float, a: float, b: float, c: float) -> float:
        """Evaluate T(t) = a + b·cos(2πt/24) + c·sin(2πt/24)."""
        angle = 2.0 * math.pi * t / 24.0
        return a + b * math.cos(angle) + c * math.sin(angle)

    @cached_property
    def interval_hours(self) -> float:
        return get_interval_hours(self.interval)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def simulate_day(self, profile: DailyProfile) -> list[float]:
        a, b, c = self._fit(profile)
        steps = round(24.0 / self.interval_hours)
        series: list[float] = []

        for i in range(steps):
            t = i * self.interval_hours
            temp = self._evaluate(t, a, b, c)

            if self.clamp:
                if profile.t_min is not None:
                    temp = max(temp, profile.t_min)
                if profile.t_max is not None:
                    temp = min(temp, profile.t_max)

            series.append(temp)

        return series

    def simulate(self, profiles: Sequence[DailyProfile]) -> list[float]:
        if not profiles:
            raise ValueError("profiles must not be empty")

        result: list[float] = []
        for profile in profiles:
            result.extend(self.simulate_day(profile))
        return result
