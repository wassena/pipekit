"""pipekit — Lightweight Python library for composing local data transformation pipelines."""

from pipekit.pipeline import Pipeline, Step
from pipekit.transforms import map_field, filter_field, rename_field
from pipekit.validators import validate_fields, validate_type, validate_range
from pipekit.io import load_json, save_json, load_csv, save_csv, load_text, save_text
from pipekit.batch import batch, process_batches, flat_map
from pipekit.cache import cached_step
from pipekit.retry import retry
from pipekit.parallel import parallel_map, parallel_step
from pipekit.hooks import before_after
from pipekit.context import PipelineContext
from pipekit.throttle import throttle, debounce
from pipekit.checkpoint import checkpoint
from pipekit.fanout import fanout, fanout_dict, merge
from pipekit.tap import tap, tap_each, tap_if
from pipekit.splitter import split, route
from pipekit.window import sliding_window, tumbling_window, window_map
from pipekit.dedupe import dedupe, dedupe_field
from pipekit.schema import apply_schema, schema_step, SchemaError

__all__ = [
    # core
    "Pipeline",
    "Step",
    # transforms
    "map_field",
    "filter_field",
    "rename_field",
    # validators
    "validate_fields",
    "validate_type",
    "validate_range",
    # schema
    "apply_schema",
    "schema_step",
    "SchemaError",
    # io
    "load_json",
    "save_json",
    "load_csv",
    "save_csv",
    "load_text",
    "save_text",
    # batch
    "batch",
    "process_batches",
    "flat_map",
    # cache
    "cached_step",
    # retry
    "retry",
    # parallel
    "parallel_map",
    "parallel_step",
    # hooks
    "before_after",
    # context
    "PipelineContext",
    # throttle
    "throttle",
    "debounce",
    # checkpoint
    "checkpoint",
    # fanout
    "fanout",
    "fanout_dict",
    "merge",
    # tap
    "tap",
    "tap_each",
    "tap_if",
    # splitter
    "split",
    "route",
    # window
    "sliding_window",
    "tumbling_window",
    "window_map",
    # dedupe
    "dedupe",
    "dedupe_field",
]
