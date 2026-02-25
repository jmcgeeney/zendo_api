from hashlib import sha256

def predictable_jitter(input: float, jitter_range: float = 10.0, round_to: int = 2) -> int:
    normalized_jitter = int(sha256(str(input).encode()).hexdigest(), 16) % (100 * 2 + 1) - 100

    return round(normalized_jitter * jitter_range / 100, round_to)