from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

from api.db.client import DatabaseClient
from api.simulators.solar import SolarSimulator
from lib.time_util import date_to_datetime


def _map_weather_temps(
    weather_rows: list,
    irradiance_rows: list,
) -> list[float] | None:
    """Map hourly weather temperatures onto 15-minute irradiance timestamps.

    Each irradiance timestamp is floored to its containing hour and looked up
    in the weather rows.  Returns ``None`` if any hour is missing so the
    caller can fall back to no derating rather than crashing.
    """
    if not weather_rows:
        return None
    temp_by_hour: dict[datetime, float] = {
        r.timestamp.replace(minute=0, second=0, microsecond=0): r.temperature
        for r in weather_rows
    }
    result: list[float] = []
    for row in irradiance_rows:
        hour_ts = row.timestamp.replace(minute=0, second=0, microsecond=0)
        if hour_ts not in temp_by_hour:
            return None  # gap — disable derating
        result.append(temp_by_hour[hour_ts])
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
    ) -> list[float]:
        if end_date < start_date:
            raise ValueError(
                f"end_date ({end_date}) must not be before start_date ({start_date})"
            )

        start_dt = date_to_datetime(start_date)
        # Include the full final day up to and including 23:45.
        end_dt = date_to_datetime(end_date + timedelta(days=1))

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

        irradiance = [row.irradiance for row in irradiance_rows]

        # ------------------------------------------------------------------ #
        # Fetch temperature from weather table (best-effort, enables derating) #
        # ------------------------------------------------------------------ #
        weather_rows = self.db.get_weather_series(
            lat=lat, lon=lon, start=start_dt, end=end_dt
        )
        temperatures = _map_weather_temps(weather_rows, irradiance_rows)

        # Guard: lengths should match after parallel downsampling, but be safe.
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
        )

        return simulator.simulate(irradiance, temperatures)
