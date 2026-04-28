# format

The `pipekit.format` module provides step factories for formatting field
values into human-readable strings without mutating the original records.

## format_field

Build a new (or overwrite an existing) field by interpolating other fields
from the same record using a Python format string.

```python
from pipekit.format import format_field
from pipekit import Pipeline

pipeline = Pipeline([
    format_field("full_name", "{first} {last}"),
    format_field("label",     "User #{id}: {full_name}"),
])

records = [{"id": 1, "first": "Jane", "last": "Doe"}]
print(pipeline(records))
# [{'id': 1, 'first': 'Jane', 'last': 'Doe',
#   'full_name': 'Jane Doe', 'label': 'User #1: Jane Doe'}]
```

### Parameters

| Parameter | Type  | Default | Description |
|-----------|-------|---------|-------------|
| `field`   | `str` | —       | Destination field name. |
| `template`| `str` | —       | Python format string referencing other field names. |
| `missing` | `str` | `""`    | Replacement value for keys absent from the record. |

---

## format_number

Apply a numeric format specifier (e.g. `".2f"`, `",d"`) to a field,
converting the value to a formatted string in-place.

```python
from pipekit.format import format_number

step = format_number("price", ".2f")
print(step([{"price": 9.5}]))
# [{'price': '9.50'}]

step2 = format_number("count", ",d")
print(step2([{"count": 1_000_000}]))
# [{'count': '1,000,000'}]
```

### Parameters

| Parameter  | Type            | Default | Description |
|------------|-----------------|---------|-------------|
| `field`    | `str`           | —       | Field to format. |
| `fmt`      | `str`           | —       | Format spec, e.g. `".2f"`. |
| `on_error` | `Any` or `None` | `None`  | Fallback when the value cannot be formatted. If `None` the original value is kept. |

---

## format_date

Convert a `datetime.date` or `datetime.datetime` stored in a field to a
formatted string using `strftime`.

```python
from datetime import date
from pipekit.format import format_date

step = format_date("created", "%d %b %Y")
print(step([{"created": date(2024, 6, 15)}]))
# [{'created': '15 Jun 2024'}]
```

### Parameters

| Parameter  | Type            | Default | Description |
|------------|-----------------|---------|-------------|
| `field`    | `str`           | —       | Field containing the date/datetime object. |
| `fmt`      | `str`           | —       | `strftime`-compatible format string. |
| `on_error` | `Any` or `None` | `None`  | Fallback when formatting fails. |

---

## Combining formatters in a pipeline

```python
from datetime import date
from pipekit import Pipeline
from pipekit.format import format_date, format_field, format_number

pipeline = Pipeline([
    format_date("order_date", "%d/%m/%Y"),
    format_number("total", ".2f"),
    format_field("summary", "Order on {order_date} — £{total}"),
])

records = [{"order_date": date(2024, 3, 1), "total": 149.9}]
print(pipeline(records))
# [{'order_date': '01/03/2024', 'total': '149.90',
#   'summary': 'Order on 01/03/2024 — £149.90'}]
```
