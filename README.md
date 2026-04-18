# pipekit

Lightweight Python library for composing local data transformation pipelines with minimal config.

---

## Installation

```bash
pip install pipekit
```

---

## Usage

Build a pipeline by chaining transformation steps together:

```python
from pipekit import Pipeline, step

@step
def parse(data):
    return [line.strip() for line in data]

@step
def filter_empty(data):
    return [line for line in data if line]

@step
def uppercase(data):
    return [line.upper() for line in data]

pipeline = Pipeline([parse, filter_empty, uppercase])

result = pipeline.run(["  hello  ", "", "  world  "])
print(result)  # ['HELLO', 'WORLD']
```

Pipelines can also be composed together:

```python
from pipekit import Pipeline

clean = Pipeline([parse, filter_empty])
transform = Pipeline([uppercase])

full_pipeline = clean | transform
result = full_pipeline.run(raw_data)
```

---

## Features

- Simple decorator-based step definitions
- Composable pipelines via `|` operator
- No external dependencies
- Easy to test individual steps in isolation

---

## License

MIT © pipekit contributors