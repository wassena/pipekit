# `pipekit.join` — Record joining

Combine two lists of records by a shared key field, similar to SQL `JOIN` operations.

## Functions

### `inner_join(left, right, on, suffixes=("_left", "_right"))`

Returns only records whose key appears in **both** `left` and `right`.

```python
from pipekit.join import inner_join

users = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
scores = [{"id": 1, "score": 95}]

result = inner_join(users, scores, on="id")
# [{"id": 1, "name": "Alice", "score": 95}]
```

### `left_join(left, right, on, suffixes=("_left", "_right"))`

Returns **all** records from `left`, enriched with matching fields from `right`.
Unmatched records receive `None` for every right-side field.

```python
from pipekit.join import left_join

result = left_join(users, scores, on="id")
# [
#   {"id": 1, "name": "Alice", "score": 95},
#   {"id": 2, "name": "Bob",   "score": None},
# ]
```

### `join_step(right, on, how="inner", suffixes=("_left", "_right"))`

Returns a **pipeline-compatible callable** that joins incoming data with a fixed
`right` dataset.

```python
from pipekit.pipeline import Pipeline
from pipekit.join import join_step

enrich_with_scores = join_step(scores, on="id", how="left")

pipeline = Pipeline([
    load_users,
    enrich_with_scores,
    save_results,
])
```

## Collision handling

When both sides share a field name (other than the join key), suffixes are
appended automatically:

```python
left  = [{"id": 1, "name": "Alice"}]
right = [{"id": 1, "name": "A"}]

result = inner_join(left, right, on="id", suffixes=("_l", "_r"))
# [{"id": 1, "name_l": "Alice", "name_r": "A"}]
```

## Notes

- Input lists are **never mutated**.
- One-to-many relationships are supported; each matching right record produces a
  separate output row.
- Only `"inner"` and `"left"` join types are supported by `join_step`; passing
  any other value raises `ValueError`.
