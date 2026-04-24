# pivot

The `pipekit.pivot` module provides **pivot** and **melt** (unpivot) helpers for
transforming record-oriented data between wide and long formats.

---

## `pivot(records, index, column, value, agg=None)`

Convert long-form records into wide form by spreading unique values of `column`
into new fields.

| Parameter | Description |
|-----------|-------------|
| `records` | List of dicts |
| `index`   | Field that identifies each output row |
| `column`  | Field whose values become new column names |
| `value`   | Field whose values fill the new columns |
| `agg`     | Optional aggregation callable (e.g. `sum`, `max`) for duplicate cells |

```python
from pipekit.pivot import pivot

records = [
    {"name": "alice", "metric": "height", "val": 165},
    {"name": "alice", "metric": "weight", "val": 60},
    {"name": "bob",   "metric": "height", "val": 180},
    {"name": "bob",   "metric": "weight", "val": 80},
]

wide = pivot(records, index="name", column="metric", value="val")
# [
#   {"name": "alice", "height": 165, "weight": 60},
#   {"name": "bob",   "height": 180, "weight": 80},
# ]
```

When multiple source rows share the same `(index, column)` pair, pass an
aggregation function:

```python
pivot(records, index="dept", column="month", value="sales", agg=sum)
```

---

## `melt(records, id_fields, value_fields=None, column_name="variable", value_name="value")`

Unpivot wide records back into long form.

```python
from pipekit.pivot import melt

wide = [{"name": "alice", "height": 165, "weight": 60}]

long = melt(wide, id_fields=["name"])
# [
#   {"name": "alice", "variable": "height", "value": 165},
#   {"name": "alice", "variable": "weight", "value": 60},
# ]
```

Use `value_fields` to melt only a subset of columns:

```python
melt(wide, id_fields=["name"], value_fields=["height"])
```

---

## Pipeline step wrappers

Both functions have `_step` variants that return a callable suitable for use
inside a `Pipeline`:

```python
from pipekit.pipeline import Pipeline
from pipekit.pivot import pivot_step, melt_step

pipeline = Pipeline([
    pivot_step(index="name", column="metric", value="val", agg=sum),
])

result = pipeline(records)
```
