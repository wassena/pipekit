"""Simple caching utilities for pipeline steps."""

import hashlib
import json
import os
import pickle
from functools import wraps
from typing import Any, Callable, Optional


def _make_key(func_name: str, args: tuple, kwargs: dict) -> str:
    """Generate a cache key from function name and arguments."""
    try:
        payload = json.dumps({"fn": func_name, "args": args, "kwargs": kwargs}, sort_keys=True)
    except (TypeError, ValueError):
        payload = str((func_name, args, kwargs))
    return hashlib.md5(payload.encode()).hexdigest()


def cached_step(cache_dir: str = ".pipekit_cache", enabled: bool = True) -> Callable:
    """Decorator that caches the output of a pipeline step to disk.

    Args:
        cache_dir: Directory to store cached results.
        enabled: If False, caching is skipped entirely.

    Example:
        @cached_step(cache_dir=".cache")
        def my_step(data):
            return expensive_transform(data)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not enabled:
                return func(*args, **kwargs)

            os.makedirs(cache_dir, exist_ok=True)
            key = _make_key(func.__name__, args, kwargs)
            cache_path = os.path.join(cache_dir, f"{key}.pkl")

            if os.path.exists(cache_path):
                with open(cache_path, "rb") as f:
                    return pickle.load(f)

            result = func(*args, **kwargs)
            with open(cache_path, "wb") as f:
                pickle.dump(result, f)
            return result

        wrapper.cache_dir = cache_dir
        wrapper.clear_cache = lambda: _clear_cache(cache_dir, func.__name__)
        return wrapper
    return decorator


def _clear_cache(cache_dir: str, func_name: Optional[str] = None) -> int:
    """Remove cached files. Returns number of files deleted."""
    if not os.path.exists(cache_dir):
        return 0
    removed = 0
    for fname in os.listdir(cache_dir):
        if fname.endswith(".pkl"):
            os.remove(os.path.join(cache_dir, fname))
            removed += 1
    return removed


def clear_all_cache(cache_dir: str = ".pipekit_cache") -> int:
    """Clear all cached results in the given directory."""
    return _clear_cache(cache_dir)
