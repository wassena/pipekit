# Hooks

The `pipekit.hooks` module provides lightweight decorators that attach lifecycle
behaviour to any step function without changing its signature.

## `before_after`

Run callbacks immediately before and/or after a step executes.

```python
from pipekit.hooks import before_after

def log_input(data):
    print("in :", data)

def log_output(result):
    print("out:", result)

@before_after(before=log_input, after=log_output)
def enrich(data):
    return {**data, "enriched": True}

enrich({"id": 1})
# in : {'id': 1}
# out: {'id': 1, 'enriched': True}
```

Either argument is optional — pass only the one you need.

## `on_error`

Provide a fallback when a step raises an exception.

```python
from pipekit.hooks import on_error

@on_error(lambda exc, data: {**data, "error": str(exc)})
def parse(data):
    return {**data, "value": int(data["raw"])}

parse({"raw": "not-a-number"})
# {'raw': 'not-a-number', 'error': "invalid literal for int() ..."}
```

If the handler itself raises, that exception propagates normally.

## `timed`

Measure wall-clock time for each invocation.

```python
from pipekit.hooks import timed

@timed
def slow_step(data):
    import time; time.sleep(0.1)
    return data

slow_step({})
print(f"took {slow_step.last_duration:.3f}s")
# took 0.100s
```

## Combining hooks

Decorators compose naturally — apply them in any order:

```python
from pipekit.hooks import before_after, timed, on_error

@timed
@on_error(lambda exc, data: data)
@before_after(before=lambda d: print("start"))
def my_step(data):
    return {**data, "processed": True}
```
