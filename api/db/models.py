from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, Integer, SmallInteger, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Reference
# ---------------------------------------------------------------------------


class Customer(Base):
    """A data-centre customer."""

    __tablename__ = "customers"

    customer_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)

    consumption: Mapped[list[Consumption]] = relationship(
        "Consumption", back_populates="customer", cascade="all, delete-orphan"
    )
    production: Mapped[list[Production]] = relationship(
        "Production", back_populates="customer", cascade="all, delete-orphan"
    )
    pearson: Mapped[list[Pearson]] = relationship(
        "Pearson", back_populates="customer", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Customer id={self.customer_id} name={self.name!r}>"


# ---------------------------------------------------------------------------
# Environmental time series
# ---------------------------------------------------------------------------


class Irradiance(Base):
    """15-minute solar irradiance readings indexed by location and time."""

    __tablename__ = "irradiance"

    latitude: Mapped[float] = mapped_column(Float, primary_key=True)
    longitude: Mapped[float] = mapped_column(Float, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    irradiance: Mapped[float] = mapped_column(Float, nullable=False)

    def __repr__(self) -> str:
        return (
            f"<Irradiance lat={self.latitude} lon={self.longitude}"
            f" ts={self.timestamp} val={self.irradiance}>"
        )


class Weather(Base):
    """Hourly weather observations indexed by location and time."""

    __tablename__ = "weather"

    latitude: Mapped[float] = mapped_column(Float, primary_key=True)
    longitude: Mapped[float] = mapped_column(Float, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, primary_key=True)

    temperature: Mapped[float] = mapped_column(Float, nullable=False)
    feels_like: Mapped[float] = mapped_column(Float, nullable=False)
    pressure: Mapped[int] = mapped_column(Integer, nullable=False)
    humidity: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    dew_point: Mapped[float] = mapped_column(Float, nullable=False)
    uvi: Mapped[float] = mapped_column(Float, nullable=False)
    clouds: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    visibility: Mapped[int] = mapped_column(Integer, nullable=False)
    wind_speed: Mapped[float] = mapped_column(Float, nullable=False)
    wind_degree: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    description: Mapped[str] = mapped_column(String(255), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<Weather lat={self.latitude} lon={self.longitude}"
            f" ts={self.timestamp} temp={self.temperature} desc={self.description!r}>"
        )


# ---------------------------------------------------------------------------
# Customer energy time series
# ---------------------------------------------------------------------------


class Consumption(Base):
    """15-minute energy consumption readings per customer."""

    __tablename__ = "consumption"

    customer_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("customers.customer_id", ondelete="CASCADE"),
        primary_key=True,
    )
    timestamp: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    power: Mapped[float] = mapped_column(Float, nullable=False)

    customer: Mapped[Customer] = relationship("Customer", back_populates="consumption")

    def __repr__(self) -> str:
        return f"<Consumption customer={self.customer_id} ts={self.timestamp} power={self.power}>"


class Production(Base):
    """15-minute energy production readings per customer."""

    __tablename__ = "production"

    customer_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("customers.customer_id", ondelete="CASCADE"),
        primary_key=True,
    )
    timestamp: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    power: Mapped[float] = mapped_column(Float, nullable=False)

    customer: Mapped[Customer] = relationship("Customer", back_populates="production")

    def __repr__(self) -> str:
        return f"<Production customer={self.customer_id} ts={self.timestamp} power={self.power}>"


# ---------------------------------------------------------------------------
# Correlation coefficients
# ---------------------------------------------------------------------------


class Pearson(Base):
    """Daily Pearson correlation coefficients per customer.

    ``timestamp`` marks the start of the calendar day the coefficients were
    computed for (midnight UTC-naive).

    Columns
    -------
    solar_irradiance_vs_production:
        Correlation between the irradiance time series and the AC power
        output series for the same day.
    temperature_vs_consumption:
        Correlation between the temperature time series and the energy
        consumption series for the same day.
    """

    __tablename__ = "pearson"

    customer_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("customers.customer_id", ondelete="CASCADE"),
        primary_key=True,
    )
    timestamp: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    solar_irradiance_vs_production: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    temperature_vs_consumption: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    customer: Mapped[Customer] = relationship("Customer", back_populates="pearson")

    def __repr__(self) -> str:
        return (
            f"<Pearson customer={self.customer_id} ts={self.timestamp}"
            f" irr_vs_prod={self.solar_irradiance_vs_production}"
            f" temp_vs_cons={self.temperature_vs_consumption}>"
        )
