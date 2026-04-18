# Parallel Execution

`pipekit.parallel` provides thread-based parallel execution for pipeline steps that process lists of items.

## Functions

### `parallel_map(func, items, max_workers=4, timeout=None)`

Apply `func` to each item in `items` concurrently using a thread pool.

- Results are returned **in the same order** as the inputs.
- Exceptions raised by `func` are re-raised immediately.

```python
from pipekit.parallel import parallel_map

results = parallel_map(fetch_record, record_ids, max_workers=8)
```

### `parallel_step(func, max_workers=4, timeout=None)`

Wrap a function as a pipeline-compatible step that accepts a list and returns a list.

```python
from pipekit.parallel import parallel_step
from pipekit import Pipeline

enrich = parallel_step(fetch_metadata, max_workers=6)

pipeline = Pipeline([
    load_records,
    enrich,
    save_results,
])
```

## Notes

- Uses `ThreadPoolExecutor` — best suited for I/O-bound work (HTTP, disk, DB).
- For CPU-bound tasks consider `ProcessPoolExecutor` directly.
- `timeout` applies per-future via `as_completed`; a `TimeoutError` will propagate if exceeded.

## Example

```python
from pipekit.parallel import parallel_step
from pipekit import Pipeline, Step

def fetch(record):
    # simulate network call
    import time; time.sleep(0.1)
    return {**record, "enriched": True}

pipeline = Pipeline([
    Step(parallel_step(fetch, max_workers=10)),
])
```
