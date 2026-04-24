# `pipekit.flatten`

Utilities for flattening nested lists and expanding list-valued fields in pipeline records.

---

## `flatten(data, depth=1)`

Flatten a nested iterable up to `depth` levels deep.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `data` | `Iterable` | — | The iterable to flatten. |
| `depth` | `int` | `1` | Levels to flatten. Pass `-1` for unlimited. |

```python
from pipekit.flatten import flatten

flatten([[1, 2], [3, [4, 5]]])           # [1, 2, 3, [4, 5]]
flatten([[1, 2], [3, [4, 5]]], depth=2)  # [1, 2, 3, 4, 5]
flatten([[1, [2, [3]]]], depth=-1)       # [1, 2, 3]
```

---

## `flatten_field(field, depth=1)`

Return a pipeline step that flattens a list-valued field **within** each record. The field is replaced with its flattened counterpart; all other fields are left untouched.

```python
from pipekit.flatten import flatten_field
from pipekit import Pipeline

pipeline = Pipeline([
    flatten_field("tags"),
])

records = [{"id": 1, "tags": [["python", "data"], ["etl"]]}]
pipeline(records)
# [{"id": 1, "tags": ["python", "data", "etl"]}]
```

---

## `flatten_records(field, depth=1)`

Return a pipeline step that **expands** a list-valued field so that each element in the list becomes a separate record. All other fields are copied to every new record.

This is useful when a single source record contains a one-to-many relationship that needs to be normalised.

```python
from pipekit.flatten import flatten_records
from pipekit import Pipeline

pipeline = Pipeline([
    flatten_records("tags"),
])

records = [
    {"id": 1, "tags": ["python", "data"]},
    {"id": 2, "tags": ["etl"]},
]
pipeline(records)
# [
#   {"id": 1, "tags": "python"},
#   {"id": 1, "tags": "data"},
#   {"id": 2, "tags": "etl"},
# ]
```

### Notes

- Records whose list field is **empty** produce **no output rows**.
- Neither `flatten_field` nor `flatten_records` mutates the original records.
- Both steps preserve all fields not named in `field`.
