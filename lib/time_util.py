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

def day_window(day: date, limit_to_now: bool = True) -> tuple[datetime, datetime]:
    """Return (start, end) datetime pair covering every 15-min slot in *day*."""
    start = datetime.combine(day, time.min)
    end = start + timedelta(days=1)

    if limit_to_now:
        now = datetime.now()
        if end > now:
            end = now

    return start, end

def timestamps_for_day(day: date, time_interval: TimeInterval) -> list[datetime]:
    start = datetime.combine(day, time.min)
    return [start + interval_timedelta(time_interval) * i for i in range(intervals_per_day(time_interval))]