# `pipekit.interpolate`

Fill sparse or missing values in a sequence of records using a variety of
interpolation strategies.

## Functions

### `interpolate_field(field, strategy="ffill", missing=None)`

Returns a step that interpolates a single *field* across all records.

| Parameter  | Type  | Default  | Description                                              |
|------------|-------|----------|----------------------------------------------------------|
| `field`    | `str` | —        | The record key to interpolate.                           |
| `strategy` | `str` | `'ffill'`| One of `'ffill'`, `'bfill'`, or `'linear'`.             |
| `missing`  | `Any` | `None`   | Sentinel value treated as a missing entry.               |

```python
from pipekit.interpolate import interpolate_field

records = [
    {"ts": 1, "temp": 20.0},
    {"ts": 2, "temp": None},
    {"ts": 3, "temp": None},
    {"ts": 4, "temp": 26.0},
]

step = interpolate_field("temp", strategy="linear")
result = step(records)
# [{"ts": 1, "temp": 20.0},
#  {"ts": 2, "temp": 22.0},
#  {"ts": 3, "temp": 24.0},
#  {"ts": 4, "temp": 26.0}]
```

---

### `interpolate_step(fields, strategy="ffill", missing=None)`

Convenience wrapper that applies `interpolate_field` to **multiple fields** in
a single step.

```python
from pipekit.interpolate import interpolate_step

step = interpolate_step(["price", "volume"], strategy="ffill")
result = step(records)
```

---

## Strategies

| Name       | Description                                                      |
|------------|------------------------------------------------------------------|
| `ffill`    | Propagate the last known value forward.                          |
| `bfill`    | Propagate the next known value backward.                         |
| `linear`   | Linearly interpolate between the nearest non-missing neighbours. |

> **Note:** `linear` requires numeric field values. Leading or trailing gaps
> with no anchor on one side fall back to the available anchor value.

---

## Pipeline usage

```python
from pipekit import Pipeline
from pipekit.interpolate import interpolate_step
from pipekit.io import load_csv, save_json

pipeline = Pipeline([
    load_csv("sensor_readings.csv"),
    interpolate_step(["temperature", "humidity"], strategy="linear"),
    save_json("cleaned_readings.json"),
])

pipeline()
```
