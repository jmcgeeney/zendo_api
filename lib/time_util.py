from datetime import date, datetime, timedelta, time
from lib.types import TimeInterval
from lib.constants import MINUTES_IN_HOUR, HOURS_IN_DAY

INTERVAL_MINUTES: dict[TimeInterval, int] = {
    "hourly": 60,
    "30m": 30,
    "15m": 15,
}

def interval_hours(time_slice: TimeInterval) -> float:
    return INTERVAL_MINUTES[time_slice] / MINUTES_IN_HOUR

def downsample_factor(time_slice: TimeInterval) -> int:
    return MINUTES_IN_HOUR / INTERVAL_MINUTES[time_slice]

def interval_minutes(time_slice: TimeInterval) -> int:
    return INTERVAL_MINUTES[time_slice]

def intervals_per_day(time_slice: TimeInterval) -> int:
    return int(HOURS_IN_DAY / interval_hours(time_slice))

def interval_timedelta(time_slice: TimeInterval) -> timedelta:
    return timedelta(minutes=interval_minutes(time_slice))

def date_to_datetime(d: date) -> datetime:
    """Return midnight UTC for *d* as a timezone-naive datetime."""
    return datetime.combine(d, time.min)

def day_window(day: date) -> tuple[datetime, datetime]:
    """Return (start, end) datetime pair covering every 15-min slot in *day*."""
    start = datetime.combine(day, time.min)
    end = start + timedelta(days=1)

    return start, end