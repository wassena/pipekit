# Batch Processing

The `pipekit.batch` module provides utilities for processing large iterables in chunks.

## Functions

### `batch(items, size)`

Yields successive lists of length `size` from `items`. The final chunk may be smaller.

```python
from pipekit.batch import batch

for chunk in batch(range(10), 3):
    print(chunk)
# [0, 1, 2]
# [3, 4, 5]
# [6, 7, 8]
# [9]
```

### `process_batches(items, transform, size=100, on_error=None)`

Splits `items` into batches, applies `transform` to each, and returns a flat list of results.

- `transform`: `Callable[[List], List]` — receives a batch, returns a list.
- `on_error`: optional `Callable[[Exception, List], None]`. If provided, errors are passed to it instead of propagating.

```python
from pipekit.batch import process_batches

records = load_large_dataset()
cleaned = process_batches(records, clean_batch, size=50)
```

### `flat_map(items, transform)`

Applies `transform` to each item and flattens one level.

```python
from pipekit.batch import flat_map

result = flat_map([[1, 2], [3]], lambda x: x)
# [1, 2, 3]
```

## Integration with Pipeline

```python
from pipekit import Pipeline, Step
from pipekit.batch import process_batches

clean = Pipeline([Step(remove_nulls), Step(normalize)])
results = process_batches(raw_data, clean, size=100)
```
