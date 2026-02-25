def predictable_jitter(input: int, jitter_range: int = 10) -> int:
    return hash(input) % (jitter_range * 2 + 1) - jitter_range