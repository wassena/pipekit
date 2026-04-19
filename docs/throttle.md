# Throttle

The `pipekit.throttle` module provides rate-limiting decorators for pipeline steps that call external APIs or other resources with usage constraints.

## `throttle(calls_per_second, *, burst=1)`

Limits how frequently a step may be called.

```python
from pipekit.throttle import throttle

@throttle(5.0)          # max 5 calls per second
def fetch(record):
    response = requests.get(record["url"])
    return {**record, "body": response.text}
```

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `calls_per_second` | `float` | Maximum call rate. Must be positive. |
| `burst` | `int` | Calls allowed to proceed immediately before throttling kicks in. Default `1`. |

### Burst mode

Set `burst` to allow a short burst of calls before rate limiting applies:

```python
@throttle(2.0, burst=5)
def enrich(record):
    ...
```

The first 5 calls proceed immediately; subsequent calls are spaced at least 0.5 s apart.

## `debounce(wait)`

Delays execution until `wait` seconds have elapsed since the last call. Useful when a step may be triggered in rapid succession but only the final invocation matters.

```python
from pipekit.throttle import debounce

@debounce(0.3)
def save_preview(record):
    write_to_disk(record)
    return record
```

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `wait` | `float` | Seconds to wait after the last call. Must be non-negative. |

## Using with Pipeline

```python
from pipekit import Pipeline
from pipekit.throttle import throttle

pipeline = Pipeline([
    parse_record,
    throttle(10.0)(call_api),
    save_result,
])

for record in records:
    pipeline(record)
```
