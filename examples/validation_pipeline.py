"""Example: using validators in a pipeline to clean and validate records."""
from pipekit import (
    Pipeline,
    Step,
    map_field,
    rename_field,
    validate_fields,
    validate_type,
    validate_range,
    validate_one_of,
)

# Build a pipeline that validates and normalises user records
pipeline = Pipeline(
    [
        Step(validate_fields(["name", "age", "role"]), name="require_fields"),
        Step(validate_type("age", int), name="age_is_int"),
        Step(validate_range("age", min_val=18, max_val=120), name="age_in_range"),
        Step(validate_one_of("role", ["admin", "editor", "viewer"]), name="valid_role"),
        Step(map_field("name", str.strip), name="strip_name"),
        Step(rename_field("role", "user_role"), name="rename_role"),
    ]
)

records = [
    {"name": "  Alice ", "age": 34, "role": "admin"},
    {"name": "Bob", "age": 22, "role": "viewer"},
]

for record in records:
    result = pipeline(record)
    print(result)

# Expected output:
# {'name': 'Alice', 'age': 34, 'user_role': 'admin'}
# {'name': 'Bob', 'age': 22, 'user_role': 'viewer'}
