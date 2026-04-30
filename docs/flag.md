# flag

The `flag` module lets you annotate records with boolean flags based on field
values or whole-record predicates — without removing any records from the
pipeline.

## Functions

### `flag_field(field, predicate, *, flag_as="flagged", overwrite=True)`

Adds a boolean flag derived from a single field's value.

```python
from pipekit.flag import flag_field

records = [
    {"product": "A", "stock": 0},
    {"product": "B", "stock": 42},
]

mark_oos = flag_field("stock", lambda v: v == 0, flag_as="out_of_stock")
result = mark_oos(records)
# [{"product": "A", "stock": 0, "out_of_stock": True},
#  {"product": "B", "stock": 42, "out_of_stock": False}]
```

Set `overwrite=False` to preserve an existing flag field if already present.

---

### `flag_if(predicate, *, flag_as="flagged", overwrite=True)`

Adds a boolean flag evaluated against the **whole record**.

```python
from pipekit.flag import flag_if

mark_vip = flag_if(
    lambda r: r["orders"] > 10 and r["spend"] > 500,
    flag_as="vip",
)
```

---

### `flag_compare(field, op, value, *, flag_as="flagged")`

Sugar for simple numeric comparisons. Supported operators:
`">"`, `">="`, `"<"`, `"<="`, `"=="`, `"!="`.

```python
from pipekit.flag import flag_compare

mark_expensive = flag_compare("price", ">=", 100, flag_as="expensive")
```

If the comparison raises a `TypeError` (e.g. comparing a string to a number)
the flag is set to `False` rather than raising.

---

## Pipeline example

```python
from pipekit import Pipeline
from pipekit.flag import flag_compare, flag_if
from pipekit.io import load_json, save_json

pipeline = Pipeline([
    load_json("products.json"),
    flag_compare("stock", "==", 0, flag_as="out_of_stock"),
    flag_compare("price", ">=", 200, flag_as="premium"),
    flag_if(lambda r: r["out_of_stock"] and r["premium"], flag_as="priority_restock"),
    save_json("products_flagged.json"),
])

pipeline()
```

## Notes

- All functions return **new** dicts; original records are never mutated.
- Flags are plain `bool` values (`True` / `False`).
- Flags can be consumed downstream by `filter_field`, `split`, `bucket_by_predicate`, etc.
