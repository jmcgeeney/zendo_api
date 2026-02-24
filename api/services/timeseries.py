from __future__ import annotations

from datetime import date, datetime, time
from lib.types import TimeSeriesPoint, HistoricalData


from api.db.client import DatabaseClient


class CustomerNotFoundError(Exception):

    def __init__(self, customer_id: int) -> None:
        self.customer_id = customer_id
        super().__init__(f"Customer {customer_id} not found")


class TimeSeriesService:

    def __init__(self, db: DatabaseClient | None = None) -> None:
        self._db = db or DatabaseClient()

    def get_historical_data(self, customer_id: int, day: date) -> HistoricalData:
        customer = self._db.get_customer(customer_id)
        if customer is None:
            raise CustomerNotFoundError(customer_id)

        start = datetime.combine(day, time.min)
        end = datetime.combine(day, time.max)

        production_rows = self._db.get_production_series(customer_id, start, end)
        consumption_rows = self._db.get_consumption_series(customer_id, start, end)
        weather_rows = self._db.get_weather_series(
            customer.latitude, customer.longitude, start, end
        )
        irradiance_rows = self._db.get_irradiance_series(
            customer.latitude, customer.longitude, start, end
        )
        correlation_rows = self._db.get_pearson_series(customer_id, start, end)

        return HistoricalData(
            customer_id=customer_id,
            date=day,
            production=[
                TimeSeriesPoint(timestamp=r.timestamp, value=r.power)
                for r in production_rows
            ],
            consumption=[
                TimeSeriesPoint(timestamp=r.timestamp, value=r.power)
                for r in consumption_rows
            ],
            temperature=[
                TimeSeriesPoint(timestamp=r.timestamp, value=r.temperature)
                for r in weather_rows
            ],
            irradiance=[
                TimeSeriesPoint(timestamp=r.timestamp, value=r.irradiance)
                for r in irradiance_rows
            ],
            correlation={
                "solar_irradiance_vs_production_correlation": correlation_rows[-1].solar_irradiance_vs_production,
                "temperature_vs_consumption_correlation": correlation_rows[-1].temperature_vs_consumption
            } if correlation_rows else None,
        )
