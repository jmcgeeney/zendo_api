from dataclasses import dataclass
from datetime import datetime, date
from typing import Literal, Optional


TimeSlice = Literal["hourly", "30m", "15m"]

@dataclass
class DailyProfile:
    morning: float
    afternoon: float
    evening: float
    night: float
    t_min: Optional[float] = None
    t_max: Optional[float] = None


@dataclass
class TimeSeriesPoint:
    timestamp: datetime
    value: float


@dataclass
class HistoricalData:
    customer_id: int
    date: date
    production: list[TimeSeriesPoint]
    consumption: list[TimeSeriesPoint]
    temperature: list[TimeSeriesPoint]
    irradiance: list[TimeSeriesPoint]
    correlation: None  # not yet implemented