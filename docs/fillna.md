# fillna

The `fillna` module provides helpers for handling missing (`None`) values in
record pipelines: fill them with a default, derive a replacement from the
record itself, or drop records that are incomplete.

## Functions

### `fillna_field(field, value, *, only_none=True)`

Returns a step that fills `None` values in a single field.

```python
from pipekit.fillna import fillna_field

records = [
    {"name": "alice", "score": None},
    {"name": "bob",   "score": 42},
]

fill_score = fillna_field("score", 0)
print(fill_score(records))
# [{"name": "alice", "score": 0}, {"name": "bob", "score": 42}]
```

Pass a **callable** to derive the fill value from the record:

```python
fill_score = fillna_field("score", lambda r: r["bonus"] * 2)
```

Set `only_none=False` to also replace falsy values (`0`, `""`, `False`):

```python
fill_score = fillna_field("score", -1, only_none=False)
```

---

### `fillna_fields(defaults, *, only_none=True)`

Fill several fields at once using a `{field: default}` mapping.

```python
from pipekit.fillna import fillna_fields

fill = fillna_fields({"score": 0, "grade": "N/A"})
result = fill(records)
```

---

### `dropna(fields=None)`

Remove records that contain `None` in any field (or in the specified fields).

```python
from pipekit.fillna import dropna

clean = dropna()                    # check every field
clean = dropna(fields=["score"])    # only check "score"
```

---

## Pipeline example

```python
from pipekit import Pipeline
from pipekit.fillna import fillna_field, fillna_fields, dropna

pipeline = Pipeline([
    fillna_field("region", "unknown"),
    fillna_fields({"score": 0, "active": False}),
    dropna(fields=["name"]),   # name must always be present
])

result = pipeline(records)
```
