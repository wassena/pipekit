"""Retry logic for pipeline steps."""

import time
from functools import wraps
from typing import Callable, Optional, Tuple, Type


def retry(
    max_attempts: int = 3,
    delay: float = 0.0,
    backoff: float = 1.0,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    on_failure: Optional[Callable] = None,
):
    """
    Decorator factory that retries a step function on failure.

    Args:
        max_attempts: Maximum number of attempts before raising.
        delay: Initial delay in seconds between attempts.
        backoff: Multiplier applied to delay after each attempt.
        exceptions: Tuple of exception types to catch and retry on.
        on_failure: Optional callback(attempt, exception) called on each failure.

    Returns:
        A decorator that wraps a step function with retry logic.

    Example:
        @retry(max_attempts=3, delay=0.5, backoff=2.0)
        def fetch_data(record):
            ...
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if on_failure is not None:
                        on_failure(attempt, exc)
                    if attempt < max_attempts:
                        if current_delay > 0:
                            time.sleep(current_delay)
                        current_delay *= backoff
            raise last_exc
        return wrapper
    return decorator
