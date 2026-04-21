# Window

The `pipekit.window` module provides **sliding** and **tumbling** window helpers for processing ordered sequences in a pipeline.

## Functions

### `sliding_window(data, size, step=1)`

Yields overlapping windows of `size` elements, advancing by `step` each time.

```python
from pipekit.window import sliding_window

for w in sliding_window([1, 2, 3, 4, 5], size=3):
    print(w)
# [1, 2, 3]
# [2, 3, 4]
# [3, 4, 5]
```

### `tumbling_window(data, size)`

Yields **non-overlapping** windows of exactly `size` elements. Any trailing elements that do not fill a complete window are silently dropped.

```python
from pipekit.window import tumbling_window

for w in tumbling_window([1, 2, 3, 4, 5], size=2):
    print(w)
# [1, 2]
# [3, 4]
```

### `window_map(func, data, size, step=1)`

Applies `func` to every sliding window and returns a list of results. Ideal for use as a pipeline step.

```python
from pipekit.window import window_map
from pipekit.pipeline import Pipeline, Step

smooth = Step(lambda data: window_map(
    lambda w: sum(w) / len(w),
    data,
    size=3,
))

pipeline = Pipeline([smooth])
result = pipeline([10, 20, 30, 40, 50])
# [20.0, 30.0, 40.0]
```

## Errors

Both `sliding_window` and `tumbling_window` raise `ValueError` when `size < 1` or (for `sliding_window`) `step < 1`.

## Use cases

| Pattern | Function | Typical use |
|---------|----------|-------------|
| Moving average | `window_map` + `mean` | Time-series smoothing |
| Event grouping | `tumbling_window` | Fixed-size micro-batches |
| Overlap detection | `sliding_window` | Duplicate / near-duplicate checks |
