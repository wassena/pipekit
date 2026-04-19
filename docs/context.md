# Context

`PipelineContext` lets you pass metadata and accumulate shared state across pipeline steps without threading extra arguments through every function manually.

## Basic Usage

```python
from pipekit.context import PipelineContext, with_context

ctx = PipelineContext(run_id="abc123", env="prod")

@with_context(ctx)
def count_records(data, context):
    context.set("record_count", len(data))
    return data

@with_context(ctx)
def tag_records(data, context):
    run_id = context.metadata["run_id"]
    return [{**r, "run_id": run_id} for r in data]
```

## API

### `PipelineContext(**metadata)`

Create a context with optional read-only metadata.

```python
ctx = PipelineContext(run_id="x", env="staging")
ctx.metadata  # {"run_id": "x", "env": "staging"}
```

### `ctx.set(key, value)`

Store a mutable value in the context.

### `ctx.get(key, default=None)`

Retrieve a value, with an optional default.

### `ctx.has(key) -> bool`

Check whether a key has been set.

### `ctx.all() -> dict`

Return a copy of all stored values.

### `with_context(ctx)`

Decorator that wraps a two-argument step `(data, context)` into a single-argument step `(data)` compatible with `Pipeline`.

```python
from pipekit.pipeline import Pipeline

pipeline = Pipeline([count_records, tag_records])
result = pipeline(records)
print(ctx.get("record_count"))
```

## Notes

- Metadata passed to the constructor is read-only (changes to the returned dict do not affect the context).
- The mutable store (`set`/`get`) is shared across all steps that receive the same context instance.
