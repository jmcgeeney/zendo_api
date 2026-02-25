from datetime import date, datetime
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.clients.openweather import OpenWeatherClient, OpenWeatherError
from api.config import settings
from api.services.energy_summary import (
    CustomerNotFoundError as EnergySummaryNotFoundError,
    EnergySummaryService,
)
from api.services.timeseries import CustomerNotFoundError, TimeSeriesService
from api.db.client import DatabaseClient

router = APIRouter()

_db = DatabaseClient()
_timeseries_service = TimeSeriesService()
_energy_summary_service = EnergySummaryService()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class CustomerResponse(BaseModel):
    customer_id: int
    name: str
    latitude: float
    longitude: float


class WeatherResponse(BaseModel):
    temperature: float
    feels_like: float
    description: str
    icon: str


class TimeSeriesPoint(BaseModel):
    timestamp: datetime
    value: float


class HistoricalDataResponse(BaseModel):
    customer_id: int
    date: date
    production: list[TimeSeriesPoint]
    consumption: list[TimeSeriesPoint]
    temperature: list[TimeSeriesPoint]
    irradiance: list[TimeSeriesPoint]
    correlation: Optional[Dict[str, Optional[float]]]


class WeatherSummaryResponse(BaseModel):
    temperature: Optional[float]
    feels_like: Optional[float]
    description: Optional[str]
    cloud_cover: Optional[float]
    wind_speed: Optional[float]


class CorrelationResponse(BaseModel):
    solar_irradiance_vs_production: Optional[float]
    temperature_vs_consumption: Optional[float]


class EnergySummaryResponse(BaseModel):
    customer_id: int
    date: date
    total_production_kwh: float
    total_consumption_kwh: float
    net_kwh: float
    weather_summary: Optional[WeatherSummaryResponse]
    correlation: Optional[CorrelationResponse]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/customers", response_model=list[CustomerResponse])
def list_customers():
    """Return all customers."""
    customers = _db.list_customers()
    return [
        CustomerResponse(
            customer_id=c.customer_id,
            name=c.name,
            latitude=c.latitude,
            longitude=c.longitude,
        )
        for c in customers
    ]


@router.get("/customers/{customer_id}/weather", response_model=WeatherResponse)
def customer_weather(customer_id: int):
    """Return current weather at the customer's location via OpenWeather."""
    customer = _db.get_customer(customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")

    ow = OpenWeatherClient(api_key=settings.OPENWEATHER_API_KEY)
    try:
        data = ow.get_current_weather(lat=customer.latitude, lon=customer.longitude)
    except OpenWeatherError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    finally:
        ow.close()

    return WeatherResponse(
        temperature=data["main"]["temp"],
        feels_like=data["main"]["feels_like"],
        description=data["weather"][0]["description"],
        icon=data["weather"][0]["icon"],
    )


@router.get("/customer/{customer_id}/energy-summary/{target_date}", response_model=EnergySummaryResponse)
def energy_summary(customer_id: int, target_date: date):
    """Return daily energy totals, weather summary, and latest Pearson correlations."""
    try:
        summary = _energy_summary_service.get_energy_summary(customer_id, target_date)
    except EnergySummaryNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return EnergySummaryResponse(
        customer_id=summary.customer_id,
        date=summary.date,
        total_production_kwh=summary.total_production_kwh,
        total_consumption_kwh=summary.total_consumption_kwh,
        net_kwh=summary.net_kwh,
        weather_summary=WeatherSummaryResponse(**vars(summary.weather_summary))
        if summary.weather_summary else None,
        correlation=CorrelationResponse(**vars(summary.correlation))
        if summary.correlation else None,
    )


@router.get("/customer/{customer_id}/historical-data/{date}", response_model=HistoricalDataResponse)
def historical_data(
    customer_id: int,
    date: date,
):
    """Return full-day time series for production, consumption, and temperature
    for a given customer and date.

    All series contain 15-minute interval readings covering the requested
    calendar day (midnight-to-midnight, inclusive).  ``correlation`` is
    reserved for a future implementation and is always ``null``.
    """
    try:
        data = _timeseries_service.get_historical_data(customer_id, date)
    except CustomerNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return HistoricalDataResponse(
        customer_id=data.customer_id,
        date=data.date,
        production=[TimeSeriesPoint(timestamp=p.timestamp, value=p.value) for p in data.production],
        consumption=[TimeSeriesPoint(timestamp=p.timestamp, value=p.value) for p in data.consumption],
        temperature=[TimeSeriesPoint(timestamp=p.timestamp, value=p.value) for p in data.temperature],
        irradiance=[TimeSeriesPoint(timestamp=p.timestamp, value=p.value) for p in data.irradiance],
        correlation=data.correlation,
    )
