# typecast

The `typecast` module provides helpers for converting record field values to
specific Python types inside a pipeline.

## Functions

### `cast_field(field, target, *, strict=True, default=None)`

Returns a step function that casts a single field to `target` for every record.

| Parameter | Description |
|-----------|-------------|
| `field`   | Key to cast. |
| `target`  | Any callable type — `int`, `float`, `str`, `bool`, etc. |
| `strict`  | If `True` (default) raise `CastError` when the cast fails. If `False` leave the original value. |
| `default` | Value used when `field` is absent from a record. |

```python
from pipekit.typecast import cast_field

records = [{"age": "28"}, {"age": "34"}]
step = cast_field("age", int)
print(step(records))
# [{"age": 28}, {"age": 34}]
```

### `cast_fields(schema, *, strict=True)`

Casts multiple fields in one step using a `{field: type}` mapping.

```python
from pipekit.typecast import cast_fields

records = [{"age": "28", "score": "7.5", "name": "Ada"}]
step = cast_fields({"age": int, "score": float})
print(step(records))
# [{"age": 28, "score": 7.5, "name": "Ada"}]
```

### `cast_step(field, target, *, strict=True)`

Alias for `cast_field` — named to match the `*_step` convention used elsewhere
in pipekit.

## Errors

`CastError` (subclass of `ValueError`) is raised when a value cannot be
converted and `strict=True`.

```python
from pipekit.typecast import cast_field, CastError

try:
    cast_field("x", int)([{"x": "oops"}])
except CastError as e:
    print(e)
```

## Pipeline example

```python
from pipekit import Pipeline
from pipekit.io import load_csv, save_json
from pipekit.typecast import cast_fields

pipeline = Pipeline([
    load_csv("data/input.csv"),
    cast_fields({"age": int, "salary": float}),
    save_json("data/output.json"),
])

pipeline()
```
