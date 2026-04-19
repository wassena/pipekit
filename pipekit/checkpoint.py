"""Checkpoint support for saving and resuming pipeline progress."""

import json
import os
import hashlib
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional


def _checkpoint_path(checkpoint_dir: str, name: str) -> Path:
    safe = hashlib.md5(name.encode()).hexdigest()[:12]
    return Path(checkpoint_dir) / f"{name}_{safe}.json"


def checkpoint(
    name: str,
    checkpoint_dir: str = ".checkpoints",
    overwrite: bool = False,
) -> Callable:
    """Decorator that saves step output to disk and reloads it on subsequent runs.

    Example::

        @checkpoint("clean_data")
        def clean(records):
            return [r for r in records if r.get("value")]
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(data: Any, *args, **kwargs) -> Any:
            os.makedirs(checkpoint_dir, exist_ok=True)
            path = _checkpoint_path(checkpoint_dir, name)

            if not overwrite and path.exists():
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)

            result = func(data, *args, **kwargs)

            with open(path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)

            return result

        wrapper.clear_checkpoint = lambda: _checkpoint_path(checkpoint_dir, name).unlink(missing_ok=True)  # noqa: E501
        return wrapper

    return decorator


def clear_checkpoints(checkpoint_dir: str = ".checkpoints") -> int:
    """Delete all checkpoint files in the given directory. Returns count removed."""
    removed = 0
    for p in Path(checkpoint_dir).glob("*.json"):
        p.unlink()
        removed += 1
    return removed
