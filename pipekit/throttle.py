"""Rate limiting and throttling utilities for pipeline steps."""

import time
import threading
from functools import wraps
from typing import Callable, Optional


def throttle(calls_per_second: float, *, burst: int = 1) -> Callable:
    """
    Decorator factory that rate-limits a step to at most `calls_per_second`.

    Args:
        calls_per_second: Maximum number of calls allowed per second.
        burst: Number of calls allowed to proceed immediately before throttling.

    Example::

        @throttle(2.0)
        def fetch(record):
            return requests.get(record["url"]).json()
    """
    if calls_per_second <= 0:
        raise ValueError("calls_per_second must be positive")
    if burst < 1:
        raise ValueError("burst must be at least 1")

    min_interval = 1.0 / calls_per_second
    lock = threading.Lock()
    last_called: list[Optional[float]] = [None]
    call_count: list[int] = [0]

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock:
                now = time.monotonic()
                if last_called[0] is None or call_count[0] < burst:
                    call_count[0] += 1
                else:
                    elapsed = now - last_called[0]
                    wait = min_interval - elapsed
                    if wait > 0:
                        time.sleep(wait)
                    call_count[0] = 1
                last_called[0] = time.monotonic()
            return func(*args, **kwargs)
        return wrapper
    return decorator


def debounce(wait: float) -> Callable:
    """
    Decorator that delays execution until `wait` seconds have passed since the
    last call. Useful for collapsing rapid successive calls.

    Args:
        wait: Seconds to wait after the last call before executing.
    """
    if wait < 0:
        raise ValueError("wait must be non-negative")

    def decorator(func: Callable) -> Callable:
        timer: list[Optional[threading.Timer]] = [None]
        result: list = [None]
        lock = threading.Lock()

        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock:
                if timer[0] is not None:
                    timer[0].cancel()

                def call():
                    result[0] = func(*args, **kwargs)

                t = threading.Timer(wait, call)
                timer[0] = t
                t.start()
                t.join()
            return result[0]
        return wrapper
    return decorator
