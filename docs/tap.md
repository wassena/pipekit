# `pipekit.tap` — Side-effect steps

The `tap` module provides helpers for inserting **observation-only** steps into
a pipeline.  Each helper calls a user-supplied function for its side-effect
(logging, metrics, debugging, …) and then returns the data **unchanged**.

---

## `tap(func)`

Wrap any callable as a pass-through pipeline step.

```python
from pipekit.tap import tap
from pipekit.pipeline import Pipeline

log_count = tap(lambda data: print(f"{len(data)} records in flight"))

pipeline = Pipeline([
    load,
    clean,
    log_count,   # <-- side-effect only, data flows through unchanged
    enrich,
    save,
])
```

---

## `tap_each(func)`

Apply *func* to **every element** of an iterable, then return the full list.
Useful for per-record metrics or audit logging.

```python
from pipekit.tap import tap_each

log_row = tap_each(lambda row: print("processing", row["id"]))

pipeline = Pipeline([load, log_row, transform, save])
```

---

## `tap_if(predicate, func)`

Call *func* only when `predicate(data)` is truthy.  The data is always
returned unchanged regardless of the predicate result.

```python
from pipekit.tap import tap_if

warn_empty = tap_if(
    lambda data: len(data) == 0,
    lambda data: print("WARNING: pipeline received empty dataset"),
)

pipeline = Pipeline([load, warn_empty, process, save])
```

---

## Combining taps with hooks

`tap` is intentionally simpler than `pipekit.hooks`.  Use `tap` when you only
need to *observe* the data at a point in the pipeline; use `hooks.before_after`
when you need to act both before and after a step executes.
