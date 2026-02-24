"""Client for the OpenWeatherMap API.

Docs: https://openweathermap.org/api
"""

from datetime import date, datetime
from typing import Literal, Optional, Union

import httpx

BASE_URL = "https://api.openweathermap.org"

Units = Literal["standard", "metric", "imperial"]


class OpenWeatherError(Exception):
    """Raised when the OpenWeatherMap API returns an error response."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f"OpenWeather API error {status_code}: {message}")


class OpenWeatherClient:
    def __init__(
        self,
        api_key: str,
        units: Units = "metric",
        timeout: float = 10.0,
    ) -> None:
        self.api_key = api_key
        self.units = units
        self._client = httpx.Client(
            base_url=BASE_URL,
            timeout=timeout,
            params={"appid": api_key, "units": units},
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, **params) -> dict:
        """Perform a GET request and return the parsed JSON body.

        Raises:
            OpenWeatherError: on any non-2xx HTTP status.
        """
        response = self._client.get(path, params={k: v for k, v in params.items() if v is not None})
        if not response.is_success:
            detail = response.json().get("message", response.text)
            raise OpenWeatherError(response.status_code, detail)
        return response.json()

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._client.close()

    def __enter__(self) -> "OpenWeatherClient":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Current weather
    # ------------------------------------------------------------------

    def get_current_weather(self, lat: float, lon: float) -> dict:
        return self._get("/data/2.5/weather", lat=lat, lon=lon)

    # ------------------------------------------------------------------
    # One Call 3.0 — requires "One Call by Call" subscription
    # ------------------------------------------------------------------

    def get_day_summary(
        self,
        lat: float,
        lon: float,
        day: date,
        tz: Optional[str] = None,
    ) -> dict:
        return self._get(
            "/data/3.0/onecall/day_summary",
            lat=lat,
            lon=lon,
            date=day.isoformat(),
            tz=tz,
        )

    def get_timemachine(
        self,
        lat: float,
        lon: float,
        dt: Union[datetime, int],
    ) -> dict:
        """Fetch historical weather for a specific point in time.

        Args:
            lat: Latitude of the location.
            lon: Longitude of the location.
            dt: The target time as a ``datetime`` (converted to a UTC Unix
                timestamp internally) or a raw Unix timestamp integer.

        Returns:
            The parsed JSON response from the Time Machine endpoint.
        """
        timestamp = int(dt.timestamp()) if isinstance(dt, datetime) else dt
        return self._get(
            "/data/3.0/onecall/timemachine",
            lat=lat,
            lon=lon,
            dt=timestamp,
        )

    def get_solar_irradiance(
        self,
        lat: float,
        lon: float,
        day: date,
        interval: str = "15m",
    ) -> dict:
        return self._get(
            "/energy/2.0/solar/interval_data",
            lat=lat,
            lon=lon,
            interval=interval,
            date=day.isoformat(),
        )