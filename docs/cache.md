# Cache

The `pipekit.cache` module provides a simple disk-based caching decorator for pipeline steps. This is useful when a step performs an expensive computation and the input is unlikely to change between runs.

## `cached_step`

Decorates a function so its output is cached to disk based on input arguments.

```python
from pipekit.cache import cached_step

@cached_step(cache_dir=".cache")
def expensive_step(data):
    # simulate slow work
    return [x * 2 for x in data]

result = expensive_step([1, 2, 3])  # computed and cached
result = expensive_step([1, 2, 3])  # loaded from cache
```

### Parameters

| Parameter   | Type   | Default            | Description                          |
|-------------|--------|--------------------|--------------------------------------|
| `cache_dir` | `str`  | `".pipekit_cache"` | Directory to store `.pkl` cache files |
| `enabled`   | `bool` | `True`             | Set to `False` to disable caching    |

### Clearing the cache

Each decorated function exposes a `clear_cache()` method:

```python
expensive_step.clear_cache()
```

Or clear an entire cache directory:

```python
from pipekit.cache import clear_all_cache

clear_all_cache(".cache")
```

## Notes

- Cache keys are based on the function name and a JSON-serialised representation of arguments.
- Results are stored as pickle files — avoid caching untrusted data.
- Non-JSON-serialisable arguments fall back to `str()` for key generation.
