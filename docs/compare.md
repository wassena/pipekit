# compare

Field-level boolean comparison steps for pipekit pipelines.

The `compare` module adds derived boolean fields to records by comparing a
field's value against a constant or against another field in the same record.

---

## `compare_field`

```python
from pipekit.compare import compare_field
```

Adds a boolean field indicating whether `record[field] <op> value`.

```python
step = compare_field("age", "ge", 18, output_field="is_adult")
records = [{"age": 20}, {"age": 15}]
result = step(records)
# [{"age": 20, "is_adult": True}, {"age": 15, "is_adult": False}]
```

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `field` | `str` | Source field to inspect. |
| `operator` | `str` | Comparison operator (see table below). |
| `value` | `Any` | Value to compare against. |
| `output_field` | `str \| None` | Destination field name (default: `<field>_<operator>`). |
| `missing_default` | `Any` | Substitute when the field is absent (default `None`). |

### Supported operators

| Operator | Meaning |
|---|---|
| `eq` | `==` |
| `ne` | `!=` |
| `lt` | `<` |
| `le` | `<=` |
| `gt` | `>` |
| `ge` | `>=` |
| `in` | `field in value` |
| `not_in` | `field not in value` |
| `contains` | `value in field` |
| `startswith` | `str(field).startswith(value)` |
| `endswith` | `str(field).endswith(value)` |

---

## `compare_fields`

```python
from pipekit.compare import compare_fields
```

Compares two fields within the same record and stores the boolean result.

```python
step = compare_fields("price", "lt", "budget", output_field="within_budget")
records = [{"price": 80, "budget": 100}, {"price": 120, "budget": 100}]
result = step(records)
# [{..., "within_budget": True}, {..., "within_budget": False}]
```

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `left` | `str` | Left-hand field name. |
| `operator` | `str` | One of `eq`, `ne`, `lt`, `le`, `gt`, `ge`. |
| `right` | `str` | Right-hand field name. |
| `output_field` | `str \| None` | Destination field (default: `<left>_vs_<right>`). |

---

## Pipeline example

```python
from pipekit import Pipeline
from pipekit.compare import compare_field, compare_fields

pipeline = Pipeline([
    compare_field("score", "ge", 50, output_field="passed"),
    compare_fields("actual", "le", "target", output_field="on_track"),
])

records = [
    {"score": 72, "actual": 90, "target": 100},
    {"score": 45, "actual": 110, "target": 100},
]
print(pipeline(records))
```
