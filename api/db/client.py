from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from api.db.models import Consumption, Customer, Irradiance, Pearson, Production, Weather
from api.db.session import get_session


class DatabaseClient:
    """Typed interface for reading and writing Zendo time-series data.

    All methods open and close their own session using the shared
    :func:`~db.session.get_session` context manager, so no session
    management is required by the caller.
    """

    # ------------------------------------------------------------------
    # Customers
    # ------------------------------------------------------------------

    def add_customer(
        self,
        name: str,
        lat: float,
        lon: float,
    ) -> Customer:
        with get_session() as db:
            customer = Customer(name=name, latitude=lat, longitude=lon)
            db.add(customer)
            db.flush()  # populate customer_id before session closes
            db.expunge(customer)
        return customer

    def get_customer(self, customer_id: int) -> Customer | None:
        with get_session() as db:
            customer = db.get(Customer, customer_id)
            if customer is not None:
                db.expunge(customer)
        return customer

    def list_customers(self) -> list[Customer]:
        with get_session() as db:
            rows = db.execute(
                select(Customer).order_by(Customer.customer_id)
            ).scalars().all()
            for row in rows:
                db.expunge(row)
        return list(rows)

    # ------------------------------------------------------------------
    # irradiance
    # ------------------------------------------------------------------

    def upsert_irradiance(
        self,
        lat: float,
        lon: float,
        timestamp: datetime,
        irradiance: float,
    ) -> None:
        with get_session() as db:
            stmt = (
                sqlite_insert(Irradiance)
                .values(
                    latitude=lat,
                    longitude=lon,
                    timestamp=timestamp,
                    irradiance=irradiance,
                )
                .on_conflict_do_update(
                    index_elements=["latitude", "longitude", "timestamp"],
                    set_={"irradiance": irradiance},
                )
            )
            db.execute(stmt)

    def upsert_irradiance_bulk(self, rows: list[dict]) -> None:
        if not rows:
            return
        with get_session() as db:
            stmt = sqlite_insert(Irradiance).on_conflict_do_update(
                index_elements=["latitude", "longitude", "timestamp"],
                set_={"irradiance": sqlite_insert(Irradiance).excluded.irradiance},
            )
            db.execute(stmt, rows)

    def get_irradiance_series(
        self,
        lat: float,
        lon: float,
        start: datetime,
        end: datetime,
    ) -> list[Irradiance]:
        with get_session() as db:
            rows = db.execute(
                select(Irradiance)
                .where(
                    Irradiance.latitude == lat,
                    Irradiance.longitude == lon,
                    Irradiance.timestamp >= start,
                    Irradiance.timestamp <= end,
                )
                .order_by(Irradiance.timestamp)
            ).scalars().all()
            for row in rows:
                db.expunge(row)
        return list(rows)

    # ------------------------------------------------------------------
    # Consumption
    # ------------------------------------------------------------------

    def upsert_consumption(
        self,
        customer_id: int,
        timestamp: datetime,
        power: float,
    ) -> None:
        with get_session() as db:
            stmt = (
                sqlite_insert(Consumption)
                .values(customer_id=customer_id, timestamp=timestamp, power=power)
                .on_conflict_do_update(
                    index_elements=["customer_id", "timestamp"],
                    set_={"power": power},
                )
            )
            db.execute(stmt)

    def upsert_consumption_bulk(self, rows: list[dict]) -> None:
        if not rows:
            return
        with get_session() as db:
            stmt = sqlite_insert(Consumption).on_conflict_do_update(
                index_elements=["customer_id", "timestamp"],
                set_={"power": sqlite_insert(Consumption).excluded.power},
            )
            db.execute(stmt, rows)

    def get_consumption_series(
        self,
        customer_id: int,
        start: datetime,
        end: datetime,
    ) -> list[Consumption]:
        with get_session() as db:
            rows = db.execute(
                select(Consumption)
                .where(
                    Consumption.customer_id == customer_id,
                    Consumption.timestamp >= start,
                    Consumption.timestamp <= end,
                )
                .order_by(Consumption.timestamp)
            ).scalars().all()
            for row in rows:
                db.expunge(row)
        return list(rows)

    # ------------------------------------------------------------------
    # Production
    # ------------------------------------------------------------------

    def upsert_production(
        self,
        customer_id: int,
        timestamp: datetime,
        power: float,
    ) -> None:
        with get_session() as db:
            stmt = (
                sqlite_insert(Production)
                .values(customer_id=customer_id, timestamp=timestamp, power=power)
                .on_conflict_do_update(
                    index_elements=["customer_id", "timestamp"],
                    set_={"power": power},
                )
            )
            db.execute(stmt)

    def upsert_production_bulk(self, rows: list[dict]) -> None:
        if not rows:
            return
        with get_session() as db:
            stmt = sqlite_insert(Production).on_conflict_do_update(
                index_elements=["customer_id", "timestamp"],
                set_={"power": sqlite_insert(Production).excluded.power},
            )
            db.execute(stmt, rows)

    def get_production_series(
        self,
        customer_id: int,
        start: datetime,
        end: datetime,
    ) -> list[Production]:
        with get_session() as db:
            rows = db.execute(
                select(Production)
                .where(
                    Production.customer_id == customer_id,
                    Production.timestamp >= start,
                    Production.timestamp <= end,
                )
                .order_by(Production.timestamp)
            ).scalars().all()
            for row in rows:
                db.expunge(row)
        return list(rows)

    # ------------------------------------------------------------------
    # Pearson coefficients
    # ------------------------------------------------------------------

    def upsert_pearson_bulk(self, rows: list[dict]) -> None:
        if not rows:
            return
        with get_session() as db:
            stmt = sqlite_insert(Pearson).on_conflict_do_update(
                index_elements=["customer_id", "timestamp"],
                set_={
                    "solar_irradiance_vs_production": sqlite_insert(Pearson).excluded.solar_irradiance_vs_production,
                    "temperature_vs_consumption": sqlite_insert(Pearson).excluded.temperature_vs_consumption,
                },
            )
            db.execute(stmt, rows)

    def get_pearson_series(
        self,
        customer_id: int,
        start: datetime,
        end: datetime,
    ) -> list[Pearson]:
        with get_session() as db:
            rows = db.execute(
                select(Pearson)
                .where(
                    Pearson.customer_id == customer_id,
                    Pearson.timestamp >= start,
                    Pearson.timestamp <= end,
                )
                .order_by(Pearson.timestamp)
            ).scalars().all()
            for row in rows:
                db.expunge(row)
        return list(rows)

    # ------------------------------------------------------------------
    # Weather
    # ------------------------------------------------------------------

    def upsert_weather_bulk(self, rows: list[dict]) -> None:
        if not rows:
            return
        update_cols = {
            col: getattr(sqlite_insert(Weather).excluded, col)
            for col in (
                "temperature", "feels_like", "pressure", "humidity",
                "dew_point", "uvi", "clouds", "visibility",
                "wind_speed", "wind_degree", "description",
            )
        }
        with get_session() as db:
            stmt = sqlite_insert(Weather).on_conflict_do_update(
                index_elements=["latitude", "longitude", "timestamp"],
                set_=update_cols,
            )
            db.execute(stmt, rows)

    def get_last_weather_timestamp(
        self,
        lat: float,
        lon: float,
    ) -> datetime | None:
        with get_session() as db:
            row = db.execute(
                select(Weather.timestamp)
                .where(Weather.latitude == lat, Weather.longitude == lon)
                .order_by(Weather.timestamp.desc())
                .limit(1)
            ).scalar_one_or_none()
        return row

    def get_weather_series(
        self,
        lat: float,
        lon: float,
        start: datetime,
        end: datetime,
    ) -> list[Weather]:
        with get_session() as db:
            rows = db.execute(
                select(Weather)
                .where(
                    Weather.latitude == lat,
                    Weather.longitude == lon,
                    Weather.timestamp >= start,
                    Weather.timestamp <= end,
                )
                .order_by(Weather.timestamp)
            ).scalars().all()
            for row in rows:
                db.expunge(row)
        return list(rows)
