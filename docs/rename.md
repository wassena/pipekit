# rename

The `pipekit.rename` module provides pipeline steps for renaming record fields
in bulk — useful when normalising keys from external sources or preparing data
for downstream steps that expect specific field names.

## Functions

### `rename_fields(mapping, *, strict=False)`

Renames fields according to a `{old_name: new_name}` mapping.

```python
from pipekit.rename import rename_fields

records = [
    {"first_name": "Alice", "last_name": "Smith"},
    {"first_name": "Bob",   "last_name": "Jones"},
]

step = rename_fields({"first_name": "given", "last_name": "family"})
print(step(records))
# [{"given": "Alice", "family": "Smith"}, ...]
```

**Parameters**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `mapping` | `dict[str, str]` | — | Old → new field name pairs |
| `strict` | `bool` | `False` | Raise `KeyError` if a source field is missing |

---

### `prefix_fields(prefix, *, exclude=None)`

Prepends a string to every field name, optionally skipping listed fields.

```python
from pipekit.rename import prefix_fields

records = [{"id": 1, "score": 0.8, "label": "cat"}]

step = prefix_fields("model_", exclude=["id"])
print(step(records))
# [{"id": 1, "model_score": 0.8, "model_label": "cat"}]
```

---

### `suffix_fields(suffix, *, exclude=None)`

Appends a string to every field name, optionally skipping listed fields.

```python
from pipekit.rename import suffix_fields

records = [{"price": 9.99, "qty": 3, "id": 42}]

step = suffix_fields("_raw", exclude=["id"])
print(step(records))
# [{"price_raw": 9.99, "qty_raw": 3, "id": 42}]
```

---

## Pipeline example

```python
from pipekit.pipeline import Pipeline
from pipekit.rename import prefix_fields, rename_fields

pipeline = Pipeline([
    rename_fields({"firstname": "first_name", "lastname": "last_name"}),
    prefix_fields("user_", exclude=["id"]),
])

raw = [{"id": 1, "firstname": "Alice", "lastname": "Smith"}]
print(pipeline(raw))
# [{"id": 1, "user_first_name": "Alice", "user_last_name": "Smith"}]
```

## Notes

- All functions return **new** records; originals are never mutated.
- `rename_fields` silently skips missing fields unless `strict=True`.
- `prefix_fields` and `suffix_fields` accept an `exclude` list to protect
  identifier fields such as `id` or `timestamp`.
