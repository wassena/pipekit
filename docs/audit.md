# `pipekit.audit` — Audit Trail

The `audit` module lets you attach a running change-log to every record as it
moves through a pipeline.  The log is stored under the special `_audit` key
and can be stripped before final output.

---

## Functions

### `audit_field(field, *, label=None, include_timestamp=False)`

Captures the *current* value of `field` and appends an entry to `_audit`.

```python
from pipekit.audit import audit_field

step = audit_field("price", label="price_before_tax")
result = step([{"price": 9.99}])
# result[0]["_audit"] == [{"field": "price_before_tax", "value": 9.99}]
```

### `audit_step(label, *, fields=None, include_timestamp=False)`

Snapshots an entire record (or a subset of fields) at a named pipeline stage.

```python
from pipekit.audit import audit_step

step = audit_step("after_enrich", fields=["score", "grade"])
```

### `strip_audit(records)`

Removes `_audit` from every record.  Call this as the final step before
saving output.

```python
from pipekit.audit import strip_audit

clean = strip_audit(enriched_records)
```

### `get_audit_log(record)`

Convenience helper — returns the audit list for a single record, or `[]`.

---

## Pipeline example

```python
from pipekit.pipeline import Pipeline
from pipekit.audit import audit_field, audit_step, strip_audit
from pipekit.transforms import map_field

def apply_tax(records):
    return map_field("price", lambda p: round(p * 1.2, 2))(records)

pipeline = Pipeline([
    audit_field("price", label="original_price"),
    apply_tax,
    audit_step("after_tax", fields=["price"]),
    strip_audit,
])

result = pipeline([{"product": "widget", "price": 10.0}])
print(result)  # [{"product": "widget", "price": 12.0}]
```

---

## Notes

- Audit entries are **appended** — calling `audit_field` twice on the same
  field produces two log entries, giving you a full history.
- The `_audit` key is intentionally excluded from `audit_step` snapshots to
  avoid recursive nesting.
- Set `include_timestamp=True` on any function to attach a UTC epoch float
  (`time.time()`) to each entry.
