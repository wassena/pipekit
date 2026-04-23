# Schema

`pipekit.schema` provides lightweight schema validation and type coercion for
dictionary records flowing through a pipeline.

## Functions

### `apply_schema(schema, *, coerce=False, allow_extra=True)`

Returns a **single-record** step that checks every field in `schema`.

| Parameter     | Type          | Default | Description                                      |
|---------------|---------------|---------|--------------------------------------------------|
| `schema`      | `dict[str, type]` | —   | Field name → expected Python type mapping        |
| `coerce`      | `bool`        | `False` | Cast values to the expected type when possible   |
| `allow_extra` | `bool`        | `True`  | Allow fields not listed in the schema            |

Raises `SchemaError` on validation failure.

### `schema_step(schema, *, coerce=False, allow_extra=True)`

Same options as `apply_schema`, but the returned step accepts a **list of
records** and validates each one.

## Example

```python
from pipekit import Pipeline
from pipekit.schema import schema_step

SCHEMA = {"id": int, "name": str, "score": float}

pipeline = Pipeline([
    schema_step(SCHEMA, coerce=True),
])

records = [
    {"id": "1", "name": "Alice", "score": "9.5"},
    {"id": "2", "name": "Bob",   "score": "8.0"},
]

result = pipeline(records)
print(result)
# [{'id': 1, 'name': 'Alice', 'score': 9.5},
#  {'id': 2, 'name': 'Bob',   'score': 8.0}]
```

## Error handling

```python
from pipekit.schema import apply_schema, SchemaError

validate = apply_schema({"age": int})

try:
    validate({"age": "not-a-number"})
except SchemaError as exc:
    print(f"Validation failed: {exc}")
```
