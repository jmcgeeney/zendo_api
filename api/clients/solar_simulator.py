from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

from api.db.client import DatabaseClient
from api.simulators.solar import SolarSimulator
from lib.types import TimeSlice
from lib.time_util import get_interval_hours, get_downsample_factor


def _date_to_datetime(d: date) -> datetime:
    """Return midnight UTC for *d* as a timezone-naive datetime."""
    return datetime.combine(d, time.min)


def _downsample(values: list[float], factor: int) -> list[float]:
    if factor == 1:
        return list(values)
    result: list[float] = []
    for i in range(0, len(values), factor):
        chunk = values[i : i + factor]
        result.append(sum(chunk) / len(chunk))
    return result


class SolarSimulatorClient:
    def __init__(
        self,
        db: DatabaseClient,
        installed_capacity_kw: float,
        performance_ratio: float = 0.80,
        temp_coefficient: float = -0.004,
        noct_delta_t: float = 25.0,
    ) -> None:
        self.db = db
        self.installed_capacity_kw = installed_capacity_kw
        self.performance_ratio = performance_ratio
        self.temp_coefficient = temp_coefficient
        self.noct_delta_t = noct_delta_t

    def get_solar_series(
        self,
        lat: float,
        lon: float,
        start_date: date,
        end_date: date,
        time_slice: TimeSlice = "30m",
    ) -> list[float]:
        if end_date < start_date:
            raise ValueError(
                f"end_date ({end_date}) must not be before start_date ({start_date})"
            )

        start_dt = _date_to_datetime(start_date)
        # Include the full final day up to and including 23:45.
        end_dt = _date_to_datetime(end_date + timedelta(days=1)) - timedelta(minutes=15)

        # ------------------------------------------------------------------ #
        # Fetch irradiance                                                    #
        # ------------------------------------------------------------------ #
        irradiance_rows = self.db.get_irradiance_series(
            lat=lat, lon=lon, start=start_dt, end=end_dt
        )
        if not irradiance_rows:
            raise ValueError(
                f"No irradiance data found for lat={lat}, lon={lon} "
                f"between {start_dt} and {end_dt}"
            )

        irradiance_15m = [row.irradiance for row in irradiance_rows]

        # ------------------------------------------------------------------ #
        # Fetch temperature (best-effort)                                      #
        # ------------------------------------------------------------------ #
        temperature_rows = self.db.get_temperature_series(
            lat=lat, lon=lon, start=start_dt, end=end_dt
        )
        temperature_15m: list[float] | None = (
            [row.temperature for row in temperature_rows]
            if temperature_rows
            else None
        )

        # ------------------------------------------------------------------ #
        # Downsample to requested resolution                                   #
        # ------------------------------------------------------------------ #
        factor = get_downsample_factor(time_slice)
        irradiance = _downsample(irradiance_15m, factor)
        temperatures: list[float] | None = (
            _downsample(temperature_15m, factor) if temperature_15m is not None else None
        )

        # Guard: if lengths diverge after downsampling (e.g. the temperature
        # table has gaps), disable temperature derating rather than crashing.
        if temperatures is not None and len(temperatures) != len(irradiance):
            temperatures = None

        # ------------------------------------------------------------------ #
        # Simulate                                                             #
        # ------------------------------------------------------------------ #
        simulator = SolarSimulator(
            installed_capacity_kw=self.installed_capacity_kw,
            performance_ratio=self.performance_ratio,
            temp_coefficient=self.temp_coefficient,
            noct_delta_t=self.noct_delta_t,
            interval_hours=get_interval_hours(time_slice),
        )

        return simulator.simulate(irradiance, temperatures)
