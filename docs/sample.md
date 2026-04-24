# Sample

The `pipekit.sample` module provides utilities for drawing random subsets of
records from a pipeline — useful for prototyping, debugging, or reducing load
during development.

---

## `sample(data, *, n=None, fraction=None, seed=None)`

Return a random sample from `data`.  Specify **either** `n` (absolute count)
or `fraction` (proportion between 0.0 and 1.0).

```python
from pipekit.sample import sample

records = list(range(1000))
subset  = sample(records, fraction=0.05, seed=42)  # ~50 records
```

| Parameter  | Type    | Description                                      |
|------------|---------|--------------------------------------------------|
| `data`     | list    | Input records                                    |
| `n`        | int     | Exact number of records to return                |
| `fraction` | float   | Proportion of records to return (0.0 – 1.0)      |
| `seed`     | int     | Random seed for reproducibility (optional)       |

---

## `sample_step(*, n=None, fraction=None, seed=None)`

Factory that returns a pipeline-compatible step wrapping `sample`.

```python
from pipekit import Pipeline
from pipekit.sample import sample_step
from pipekit.io import load_json, save_json

pipeline = Pipeline([
    load_json("data/records.json"),
    sample_step(fraction=0.1, seed=0),
    save_json("data/sample.json"),
])

pipeline()
```

---

## `reservoir_sample(data, k, *, seed=None)`

Classic reservoir sampling algorithm — draws exactly `k` items in a single
pass.  Suitable for very large or lazily-evaluated inputs.

```python
from pipekit.sample import reservoir_sample

big_stream = range(10_000_000)
subset = reservoir_sample(big_stream, k=500, seed=7)
```

> **Tip:** Unlike `sample()`, `reservoir_sample()` accepts any iterable, not
> just a list.

---

## Reproducibility

All three helpers accept an optional `seed` argument.  Passing the same seed
guarantees identical output across runs, which is handy for regression tests
or shareable notebooks.
