"""Sampling utilities for pipekit pipelines."""

import random
from typing import Any, Callable, List, Optional


def sample(
    data: List[Any],
    n: Optional[int] = None,
    fraction: Optional[float] = None,
    seed: Optional[int] = None,
) -> List[Any]:
    """Return a random sample of records from data.

    Specify either ``n`` (absolute count) or ``fraction`` (0.0–1.0).
    If both are given, ``n`` takes precedence.

    Args:
        data: Input list of records.
        n: Number of records to sample.
        fraction: Fraction of records to sample.
        seed: Optional random seed for reproducibility.

    Returns:
        A new list containing the sampled records.
    """
    if n is None and fraction is None:
        raise ValueError("Specify either 'n' or 'fraction'.")
    if fraction is not None and not (0.0 <= fraction <= 1.0):
        raise ValueError("'fraction' must be between 0.0 and 1.0.")

    rng = random.Random(seed)
    population = list(data)

    if n is None:
        n = max(0, round(len(population) * fraction))

    n = min(n, len(population))
    return rng.sample(population, n)


def sample_step(
    n: Optional[int] = None,
    fraction: Optional[float] = None,
    seed: Optional[int] = None,
) -> Callable[[List[Any]], List[Any]]:
    """Return a pipeline step that samples records.

    Example::

        pipeline = Pipeline([
            load_data,
            sample_step(fraction=0.1, seed=42),
            process,
        ])
    """
    def transform(data: List[Any]) -> List[Any]:
        return sample(data, n=n, fraction=fraction, seed=seed)

    transform.__name__ = "sample_step"
    return transform


def reservoir_sample(data: List[Any], k: int, seed: Optional[int] = None) -> List[Any]:
    """Sample exactly ``k`` records using reservoir sampling.

    Works correctly even for very large (or streaming) inputs.

    Args:
        data: Input iterable of records.
        k: Desired sample size.
        seed: Optional random seed.

    Returns:
        A list of up to ``k`` records.
    """
    if k < 0:
        raise ValueError("'k' must be non-negative.")

    rng = random.Random(seed)
    reservoir: List[Any] = []

    for i, item in enumerate(data):
        if len(reservoir) < k:
            reservoir.append(item)
        else:
            j = rng.randint(0, i)
            if j < k:
                reservoir[j] = item

    return reservoir
