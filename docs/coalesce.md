# coalesce

The `coalesce` module provides steps for filling missing or null field values
from ordered fallback sources — similar to SQL's `COALESCE` function.

## Functions

### `coalesce_field(field, *fallbacks, null_values=None)`

Returns a pipeline step that fills `field` with the first non-null value found
among `fallbacks` when the field's current value is considered null.

Each fallback can be:
- **A plain value** — used directly.
- **A string** — treated as a sibling field name; the field's value is read from the record.
- **A callable** — called with the record; the return value is used.

```python
from pipekit.coalesce import coalesce_field

# Fill from a sibling field, then a literal default
step = coalesce_field("display_name", "username", "Anonymous")

records = [
    {"display_name": None, "username": "alice"},
    {"display_name": None, "username": None},
    {"display_name": "Bob", "username": "bob"},
]

result = step(records)
# [
#   {"display_name": "alice",     "username": "alice"},
#   {"display_name": "Anonymous", "username": None},
#   {"display_name": "Bob",       "username": "bob"},
# ]
```

Custom null values:

```python
step = coalesce_field("score", 0, null_values=[None, "", -1])
```

---

### `coalesce_fields(fields, default=None, null_values=None)`

Returns a step that ensures every field in `fields` has a non-null value,
replacing any null with `default`.

```python
from pipekit.coalesce import coalesce_fields

step = coalesce_fields(["city", "country"], default="Unknown")

records = [{"city": None, "country": "DE"}, {"city": None, "country": None}]
result = step(records)
# [
#   {"city": "Unknown", "country": "DE"},
#   {"city": "Unknown", "country": "Unknown"},
# ]
```

---

## Pipeline usage

```python
from pipekit import Pipeline
from pipekit.coalesce import coalesce_field, coalesce_fields

pipeline = Pipeline([
    coalesce_field("name", "alias", "Unknown"),
    coalesce_fields(["age", "score"], default=0),
])

result = pipeline(records)
```
