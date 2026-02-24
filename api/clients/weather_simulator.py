from __future__ import annotations

from datetime import date, timedelta
from lib.time_util import get_interval_hours
from lib.types import TimeSlice

from api.clients.openweather import OpenWeatherClient
from api.simulators.weather import DailyProfile, WeatherSimulator


def _parse_day_summary(response: dict) -> DailyProfile:
    temp = response["temperature"]
    return DailyProfile(
        morning=temp["morning"],
        afternoon=temp["afternoon"],
        evening=temp["evening"],
        night=temp["night"],
        t_min=temp.get("min"),
        t_max=temp.get("max"),
    )


class WeatherSimulatorClient:
    def __init__(
        self,
        openweather: OpenWeatherClient,
        tz: str | None = None,
    ) -> None:
        self.openweather = openweather
        self.tz = tz

    def get_temperature_series(
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

        simulator = WeatherSimulator(interval=time_slice, clamp=True)

        # Collect daily profiles, one API call per day.
        profiles: list[DailyProfile] = []
        current = start_date
        while current <= end_date:
            response = self.openweather.get_day_summary(
                lat=lat,
                lon=lon,
                day=current,
                tz=self.tz,
            )
            profiles.append(_parse_day_summary(response))
            current += timedelta(days=1)

        return simulator.simulate(profiles)
