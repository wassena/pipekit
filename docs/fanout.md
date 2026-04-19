# fanout

The `fanout` module lets you **broadcast** a single input to multiple steps and collect their outputs — useful for computing several derived values from the same data in one pass.

## Functions

### `fanout(*steps)`

Returns a step that passes `data` to every step and returns a **list** of results.

```python
from pipekit.fanout import fanout, merge
from pipekit.pipeline import Pipeline

def count(records): return len(records)
def total(records): return sum(r["value"] for r in records)
def maximum(records): return max(r["value"] for r in records)

pipe = Pipeline([
    fanout(count, total, maximum),
    merge(lambda r: {"count": r[0], "total": r[1], "max": r[2]}),
])

result = pipe([{"value": 1}, {"value": 3}, {"value": 2}])
# {"count": 3, "total": 6, "max": 3}
```

### `fanout_dict(**named_steps)`

Like `fanout` but returns a **dict** keyed by the keyword argument names.

```python
from pipekit.fanout import fanout_dict

analyse = fanout_dict(count=count, total=total, maximum=maximum)
result = analyse(records)
# {"count": 3, "total": 6, "maximum": 3}
```

### `merge(combiner=None)`

Consumes the list produced by `fanout` and reduces it with `combiner`.  
When `combiner` is omitted the list is passed through unchanged.

```python
from pipekit.fanout import fanout, merge

step = merge(lambda results: results[0] + results[1])
step([10, 5])  # 15
```

## Combining with other pipekit features

`fanout` composes naturally with `retry`, `cache`, and `parallel_map`:

```python
from pipekit.fanout import fanout_dict
from pipekit.retry import retry

robust_analyse = fanout_dict(
    count=count,
    enriched=retry(enrich, attempts=3),
)
```
