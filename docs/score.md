# `pipekit.score`

The `score` module provides lightweight helpers for assigning numeric scores
and ranks to records in a pipeline. All functions return step-compatible
callables that accept and return a list of dicts.

---

## `score_field(weights, output_field="score", missing=0.0, normalise=False)`

Compute a **weighted sum** of named fields for each record.

| Parameter | Type | Description |
|---|---|---|
| `weights` | `dict[str, float]` | Field name → weight mapping |
| `output_field` | `str` | Destination field for the computed score |
| `missing` | `float` | Value substituted when a field is absent or `None` |
| `normalise` | `bool` | Divide result by sum of absolute weights |

```python
from pipekit.score import score_field

records = [
    {"relevance": 0.9, "freshness": 0.6, "popularity": 0.4},
    {"relevance": 0.5, "freshness": 0.8, "popularity": 0.7},
]

step = score_field(
    weights={"relevance": 3.0, "freshness": 1.5, "popularity": 1.0},
    output_field="rank_score",
    normalise=True,
)

result = step(records)
# result[0]["rank_score"] ≈ 0.745
```

---

## `score_by(func, output_field="score")`

Assign a score using an **arbitrary callable**. The function receives the
full record dict and should return a `float`.

```python
from pipekit.score import score_by

step = score_by(
    lambda r: r["price"] * r["qty"] * (1 - r.get("discount", 0)),
    output_field="revenue",
)
```

---

## `rank_by(field, output_field="rank", ascending=False, start=1)`

Add a **dense rank** field based on the values of `field`. Records sharing
the same value receive the same rank; no gaps are introduced.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `field` | `str` | — | Field to rank by |
| `output_field` | `str` | `"rank"` | Destination field |
| `ascending` | `bool` | `False` | Lower value → lower rank when `True` |
| `start` | `int` | `1` | First rank number |

```python
from pipekit.score import rank_by

records = [
    {"name": "Alice", "score": 88},
    {"name": "Bob",   "score": 95},
    {"name": "Carol", "score": 88},
]

step = rank_by("score")  # descending by default
result = step(records)
# Alice → rank 2, Bob → rank 1, Carol → rank 2
```

---

## Pipeline example

```python
from pipekit import Pipeline
from pipekit.score import score_field, rank_by

pipeline = Pipeline([
    score_field(
        weights={"quality": 2.0, "speed": 1.0, "cost": -1.5},
        output_field="composite",
        normalise=True,
    ),
    rank_by("composite", output_field="position"),
])

results = pipeline(records)
```
