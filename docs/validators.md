# Validators

The `pipekit.validators` module provides composable validation helpers that can be used as pipeline steps. Each validator returns a callable that accepts a record (dict) and either returns it unchanged or raises an error.

## Usage

```python
from pipekit import Pipeline, Step, validate_fields, validate_type, validate_range, validate_one_of

pipeline = Pipeline([
    Step(validate_fields(["name", "age", "status"])),
    Step(validate_type("age", int)),
    Step(validate_range("age", min_val=0, max_val=150)),
    Step(validate_one_of("status", ["active", "inactive"])),
])

record = {"name": "Alice", "age": 30, "status": "active"}
result = pipeline(record)
```

## API

### `validate_fields(required: List[str])`
Raises `ValueError` if any of the listed fields are absent from the record.

### `validate_type(field: str, expected_type: type)`
Raises `TypeError` if the field is present but not an instance of `expected_type`.

### `validate_range(field: str, min_val=None, max_val=None)`
Raises `ValueError` if the numeric field falls outside `[min_val, max_val]`. Either bound is optional.

### `validate_one_of(field: str, choices: List[Any])`
Raises `ValueError` if the field value is not in the provided `choices` list.

## Notes

- All validators are **non-mutating**: they return the original record dict unchanged on success.
- Missing fields are silently skipped by `validate_type`, `validate_range`, and `validate_one_of` — only `validate_fields` enforces presence.
- Validators can be freely mixed with transforms from `pipekit.transforms`.
