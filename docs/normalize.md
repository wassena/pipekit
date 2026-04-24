# `pipekit.normalize`

Utilities for normalizing and scaling numeric fields across pipeline records.

---

## `normalize_field(field, method="minmax", minimum=None, maximum=None)`

Returns a pipeline step that normalizes a numeric field across **all** records
passed to it.

| Method | Description |
|--------|-------------|
| `minmax` | Scales values to the `[0, 1]` range. |
| `zscore` | Standardizes values to zero mean and unit variance. |

```python
from pipekit.normalize import normalize_field

records = [
    {"name": "alice", "score": 40},
    {"name": "bob",   "score": 80},
    {"name": "carol", "score": 100},
]

step = normalize_field("score")          # minmax by default
print(step(records))
# [{"name": "alice", "score": 0.0},
#  {"name": "bob",   "score": 0.667},
#  {"name": "carol", "score": 1.0}]
```

You can supply pre-computed bounds to normalize against a known range:

```python
step = normalize_field("score", minimum=0, maximum=100)
```

---

## `clamp_field(field, lo, hi)`

Returns a step that clamps a numeric field so every value falls within
`[lo, hi]`.  Values below `lo` become `lo`; values above `hi` become `hi`.

```python
from pipekit.normalize import clamp_field

records = [{"temp": -10}, {"temp": 22}, {"temp": 150}]
print(clamp_field("temp", 0, 100)(records))
# [{"temp": 0}, {"temp": 22}, {"temp": 100}]
```

---

## `round_field(field, decimals=2)`

Returns a step that rounds a numeric field to the given number of decimal
places.

```python
from pipekit.normalize import round_field

records = [{"ratio": 0.123456}]
print(round_field("ratio", decimals=3)(records))
# [{"ratio": 0.123}]
```

---

## Composing with `Pipeline`

```python
from pipekit import Pipeline
from pipekit.normalize import normalize_field, clamp_field, round_field

pipeline = Pipeline([
    clamp_field("value", 0, 1000),
    normalize_field("value"),
    round_field("value", decimals=4),
])

result = pipeline(raw_records)
```
