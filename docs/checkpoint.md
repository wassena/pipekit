# Checkpoint

The `checkpoint` decorator saves a step's output to disk so that re-running a pipeline skips expensive steps automatically.

## Basic Usage

```python
from pipekit.checkpoint import checkpoint

@checkpoint("cleaned")
def clean(records):
    return [r for r in records if r.get("value") is not None]
```

On the first run the function executes normally and the result is written to `.checkpoints/cleaned_<hash>.json`. On every subsequent run the saved result is returned immediately without calling the function.

## Options

| Parameter | Default | Description |
|---|---|---|
| `name` | required | Logical name for the checkpoint |
| `checkpoint_dir` | `".checkpoints"` | Directory to store checkpoint files |
| `overwrite` | `False` | Always re-run and overwrite existing checkpoint |

## Clearing Checkpoints

```python
# Clear a single step's checkpoint
clean.clear_checkpoint()

# Clear all checkpoints in a directory
from pipekit.checkpoint import clear_checkpoints
clear_checkpoints(".checkpoints")
```

## Pipeline Example

```python
from pipekit.pipeline import Pipeline
from pipekit.checkpoint import checkpoint

@checkpoint("loaded")
def load(path): ...

@checkpoint("enriched")
def enrich(records): ...

pipeline = Pipeline([load, enrich])
```

Resume long pipelines from where they left off by simply re-running the script.
