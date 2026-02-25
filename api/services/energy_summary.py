from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Optional

from api.db.client import DatabaseClient


class CustomerNotFoundError(Exception):
    def __init__(self, customer_id: int) -> None:
        self.customer_id = customer_id
        super().__init__(f"Customer {customer_id} not found")


@dataclass
class WeatherSummary:
    temperature: float | None
    feels_like: float | None = None
    description: str | None = None
    cloud_cover: float | None = None
    wind_speed: float | None = None


@dataclass
class Correlation:
    solar_irradiance_vs_production: float | None
    temperature_vs_consumption: float | None


@dataclass
class EnergySummary:
    customer_id: int
    date: date
    total_production_kwh: float
    total_consumption_kwh: float
    net_kwh: float
    weather_summary: WeatherSummary | None
    correlation: Correlation | None

class EnergySummaryService:

    def __init__(self, db: DatabaseClient | None = None) -> None:
        self._db = db or DatabaseClient()

    def get_energy_summary(self, customer_id: int, target_date: date) -> EnergySummary:
        customer = self._db.get_customer(customer_id)
        if customer is None:
            raise CustomerNotFoundError(customer_id)

        day_start = datetime.combine(target_date, time.min)
        day_end = day_start + timedelta(hours=23, minutes=45)

        # ------------------------------------------------------------------
        # Totals — sum kW readings over 15-min intervals → kWh (* 0.25 h)
        # ------------------------------------------------------------------
        production_rows = self._db.get_production_series(customer_id, day_start, day_end)
        consumption_rows = self._db.get_consumption_series(customer_id, day_start, day_end)

        total_production_kwh = round(sum(r.power for r in production_rows) * 0.25, 3)
        total_consumption_kwh = round(sum(r.power for r in consumption_rows) * 0.25, 3)
        net_kwh = round(total_production_kwh - total_consumption_kwh, 3)

        # ------------------------------------------------------------------
        # Weather summary from the stored weather time series
        # ------------------------------------------------------------------
        weather_summary: WeatherSummary | None = None
        weather_rows = self._db.get_weather_series(
            lat=customer.latitude,
            lon=customer.longitude,
            start=day_start,
            end=day_end,
        )
        if weather_rows:
            temps = [r.temperature for r in weather_rows]
            weather_summary = WeatherSummary(
                temperature=temps[-1],
                feels_like=weather_rows[-1].feels_like,
                description=weather_rows[-1].description,
                cloud_cover=weather_rows[-1].clouds,
                wind_speed=weather_rows[-1].wind_speed,
            )

        # ------------------------------------------------------------------
        # Correlation — latest Pearson row for the target date
        # ------------------------------------------------------------------
        correlation: Optional[Correlation] = None
        pearson_rows = self._db.get_pearson_series(customer_id, day_start, day_end)
        if pearson_rows:
            latest = pearson_rows[-1]
            correlation = Correlation(
                solar_irradiance_vs_production=latest.solar_irradiance_vs_production,
                temperature_vs_consumption=latest.temperature_vs_consumption,
            )

        return EnergySummary(
            customer_id=customer_id,
            date=target_date,
            total_production_kwh=total_production_kwh,
            total_consumption_kwh=total_consumption_kwh,
            net_kwh=net_kwh,
            weather_summary=weather_summary,
            correlation=correlation,
        )
