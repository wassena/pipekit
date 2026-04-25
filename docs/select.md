# select

The `pipekit.select` module provides steps for **projecting** records — keeping
or dropping specific fields before passing data to the next stage.

## Functions

### `select_fields(fields, *, strict=False)`

Keep only the listed fields in every record.

```python
from pipekit.select import select_fields

records = [
    {"id": 1, "name": "Alice", "_internal": True},
    {"id": 2, "name": "Bob",   "_internal": False},
]

step = select_fields(["id", "name"])
print(step(records))
# [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
```

By default missing fields are silently skipped.  Pass `strict=True` to raise a
`KeyError` instead:

```python
select_fields(["id", "missing_col"], strict=True)(records)  # raises KeyError
```

---

### `exclude_fields(fields)`

Drop the listed fields and keep everything else.

```python
from pipekit.select import exclude_fields

records = [{"id": 1, "name": "Alice", "password": "s3cr3t"}]

step = exclude_fields(["password"])
print(step(records))
# [{"id": 1, "name": "Alice"}]
```

---

### `select_if(predicate)`

Keep only fields for which `predicate(field_name, value)` returns `True`.
Useful for dynamic projection logic.

```python
from pipekit.select import select_if

records = [{"a": 1, "b": None, "c": 3}]

# Drop fields whose value is None
step = select_if(lambda k, v: v is not None)
print(step(records))
# [{"a": 1, "c": 3}]
```

---

## Pipeline example

```python
from pipekit import Pipeline
from pipekit.select import exclude_fields, select_fields

pipeline = Pipeline([
    exclude_fields(["_raw", "_ts"]),
    select_fields(["id", "name", "score"]),
])

result = pipeline(raw_records)
```

## Notes

- All functions return **new** dicts; original records are never mutated.
- Steps accept any iterable and return a `list`.
