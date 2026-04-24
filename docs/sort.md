# sort

The `pipekit.sort` module provides step factories for sorting, ranking, and
selecting top records in a pipeline.

## Functions

### `sort_by(key, reverse=False)`

Sorts a list of records by a single field name or a callable key function.

```python
from pipekit.sort import sort_by

records = [
    {"name": "Carol", "age": 35},
    {"name": "Alice", "age": 30},
    {"name": "Bob",   "age": 25},
]

step = sort_by("age")
print(step(records))
# [{"name": "Bob", "age": 25}, {"name": "Alice", "age": 30}, ...]

step_desc = sort_by("age", reverse=True)
print(step_desc(records))
# [{"name": "Carol", "age": 35}, ...]
```

You can also pass a callable:

```python
step = sort_by(lambda r: r["name"].lower())
```

---

### `sort_by_multiple(keys)`

Sorts by several fields in priority order. Each entry in `keys` is either a
field name string or a `(field, reverse)` tuple.

```python
from pipekit.sort import sort_by_multiple

step = sort_by_multiple(["department", ("salary", True)])
result = step(employees)
# Sorted by department ascending, then salary descending within each department
```

---

### `top_n(n, key, reverse=True)`

Returns the top `n` records ranked by `key`. By default the *highest* values
rank first (`reverse=True`).

```python
from pipekit.sort import top_n

step = top_n(3, "score")
best_three = step(records)
```

Pass `reverse=False` to retrieve the *lowest* values instead:

```python
cheapest = top_n(5, "price", reverse=False)(products)
```

---

## Pipeline example

```python
from pipekit import Pipeline
from pipekit.sort import sort_by, top_n

pipeline = Pipeline([
    sort_by("score", reverse=True),
    top_n(10, "score"),
])

leaderboard = pipeline(records)
```
