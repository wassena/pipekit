# `pipekit.enrich` — Field Enrichment

The `enrich` module provides utilities for adding or updating fields on pipeline records using derived values, multi-field mappings, or constants.

---

## `enrich_field(field, func, *, overwrite=True)`

Add or update a single field on every record using a callable that receives the full record.

```python
from pipekit.enrich import enrich_field

records = [{"price": 10, "qty": 3}, {"price": 5, "qty": 8}]

add_total = enrich_field("total", lambda r: r["price"] * r["qty"])
result = add_total(records)
# [{"price": 10, "qty": 3, "total": 30}, {"price": 5, "qty": 8, "total": 40}]
```

Set `overwrite=False` to leave existing values untouched:

```python
tag = enrich_field("source", lambda r: "inferred", overwrite=False)
```

---

## `enrich_from(mapping, *, overwrite=True)`

Enrich multiple fields in a single pass using a dict of `{field: func}` pairs.

```python
from pipekit.enrich import enrich_from

records = [{"first": "Ada", "last": "Lovelace"}]

step = enrich_from({
    "full_name": lambda r: f"{r['first']} {r['last']}",
    "initials": lambda r: f"{r['first'][0]}.{r['last'][0]}.",
})
result = step(records)
# [{"first": "Ada", "last": "Lovelace", "full_name": "Ada Lovelace", "initials": "A.L."}]
```

---

## `enrich_constant(field, value, *, overwrite=True)`

Stamp every record with the same constant value — useful for tagging records with a version, source label, or run ID.

```python
from pipekit.enrich import enrich_constant

from pipekit.pipeline import Pipeline

pipeline = Pipeline([
    load_records,
    enrich_constant("pipeline_version", "v2.1"),
    enrich_constant("processed", True),
    save_records,
])
```

---

## Composing enrichment steps

All three helpers return plain callables compatible with `Pipeline`:

```python
from pipekit.pipeline import Pipeline
from pipekit.enrich import enrich_field, enrich_constant

pipeline = Pipeline([
    load_orders,
    enrich_field("discount", lambda r: r["total"] * 0.1 if r["vip"] else 0),
    enrich_field("final_price", lambda r: r["total"] - r["discount"]),
    enrich_constant("currency", "USD"),
    save_orders,
])

result = pipeline([])
```

> **Note:** Original records are never mutated — each step returns new dicts.
