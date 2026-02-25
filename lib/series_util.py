from __future__ import annotations
from copy import deepcopy
from typing import Callable, Optional, TypeVar

from lib.types import TimeSeriesPoint

Container = TypeVar("Any")

#TODO: This is slow, because we're going through the list 3 times. Update it to do all the work in one pass.
def interpolate_any(points: list[Container], getter: Callable[[Container], Optional[float]], setter: Callable[[Container, float], None]) -> list[Container]:
    points_copy = deepcopy(points)
    
    raw_points = [getter(p) for p in points_copy]
    interpolated_values = interpolate(raw_points)
    for p, value in zip(points_copy, interpolated_values):
        setter(p, value)
    return points_copy

def interpolate(points: list[Optional[float]]) -> list[float]:
    if not points:
        return []
    
    i = 0

    points_copy = points.copy() # avoid mutating the input list

    while i < len(points_copy):
        if points_copy[i] is not None:
            i += 1
            continue

        # Find the next non-None value to the right (if any)
        j = i + 1
        while j < len(points_copy) and points_copy[j] is None:
            j += 1

        left = points_copy[i - 1] if i > 0 else None
        right = points_copy[j] if j < len(points_copy) else None
        steps = j - i

        interpolated_values = interpolate_steps(left, right, steps)
        points_copy[i:j] = interpolated_values
        i = j

    return points_copy

def interpolate_steps(left: Optional[float], right: Optional[float], steps: int) -> list[float]:
    if left is None or right is None:
        return [left if left is not None else right] * steps

    step_size = (right - left) / (steps + 1)
    return [left + step_size * (i + 1) for i in range(steps)]

def interpolate_time_series(points: list[TimeSeriesPoint]) -> list[TimeSeriesPoint]:
    return interpolate_any(
        points,
        getter=lambda p: p.value,
        setter=lambda p, v: setattr(p, "value", v),
    )