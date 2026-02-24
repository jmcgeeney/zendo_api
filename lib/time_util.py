from lib.types import TimeSlice


INTERVAL_HOURS: dict[TimeSlice, float] = {
    "hourly": 1.0,
    "30m": 0.5,
    "15m": 0.25,
}

DOWNSAMPLE_FACTOR: dict[TimeSlice, int] = {
    "hourly": 4,   # 4 * 15m = 1h
    "30m": 2,      # 2 * 15m = 30m
    "15m": 1,      # no downsampling
}

def get_interval_hours(time_slice: TimeSlice) -> float:
    return INTERVAL_HOURS[time_slice]

def get_downsample_factor(time_slice: TimeSlice) -> int:
    return DOWNSAMPLE_FACTOR[time_slice]