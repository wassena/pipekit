"""pipekit – Lightweight Python library for composing local data transformation pipelines."""

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
from pipekit.flatten import flatten, flatten_field, flatten_records
from pipekit.diff import diff_records, field_diff, diff_step
from pipekit.aggregate import group_by, aggregate, count_by
from pipekit.enrich import enrich_field, enrich_from, enrich_constant
from pipekit.sample import sample, sample_step, reservoir_sample
from pipekit.normalize import normalize_field, clamp_field, round_field
from pipekit.pivot import pivot, melt, pivot_step, melt_step
from pipekit.sort import sort_by, sort_by_multiple, top_n
from pipekit.join import inner_join, left_join, full_join, join_step
from pipekit.truncate import take, drop, slice_records
from pipekit.coalesce import coalesce_field, coalesce_fields
from pipekit.typecast import cast_field, cast_fields, CastError
from pipekit.fillna import fillna_field, fillna_fields, dropna
from pipekit.mask import mask_field, redact_pattern, drop_fields
from pipekit.rename import rename_fields, prefix_fields, suffix_fields
from pipekit.select import select_fields, exclude_fields, select_if
from pipekit.limit import take_while, drop_while, limit_by
from pipekit.audit import audit_field, audit_step, strip_audit
from pipekit.interpolate import interpolate_field
from pipekit.format import format_field, format_number, format_date
from pipekit.expression import where, expr_field
from pipekit.bucket import bucket_by_thresholds, bucket_by_predicate, collect_buckets
from pipekit.flag import flag_field, flag_if, flag_compare
from pipekit.score import score_field, score_by, rank_by
from pipekit.compare import compare_field, compare_fields

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
    # schema
    "apply_schema",
    "schema_step",
    "SchemaError",
    # flatten
    "flatten",
    "flatten_field",
    "flatten_records",
    # diff
    "diff_records",
    "field_diff",
    "diff_step",
    # aggregate
    "group_by",
    "aggregate",
    "count_by",
    # enrich
    "enrich_field",
    "enrich_from",
    "enrich_constant",
    # sample
    "sample",
    "sample_step",
    "reservoir_sample",
    # normalize
    "normalize_field",
    "clamp_field",
    "round_field",
    # pivot
    "pivot",
    "melt",
    "pivot_step",
    "melt_step",
    # sort
    "sort_by",
    "sort_by_multiple",
    "top_n",
    # join
    "inner_join",
    "left_join",
    "full_join",
    "join_step",
    # truncate
    "take",
    "drop",
    "slice_records",
    # coalesce
    "coalesce_field",
    "coalesce_fields",
    # typecast
    "cast_field",
    "cast_fields",
    "CastError",
    # fillna
    "fillna_field",
    "fillna_fields",
    "dropna",
    # mask
    "mask_field",
    "redact_pattern",
    "drop_fields",
    # rename
    "rename_fields",
    "prefix_fields",
    "suffix_fields",
    # select
    "select_fields",
    "exclude_fields",
    "select_if",
    # limit
    "take_while",
    "drop_while",
    "limit_by",
    # audit
    "audit_field",
    "audit_step",
    "strip_audit",
    # interpolate
    "interpolate_field",
    # format
    "format_field",
    "format_number",
    "format_date",
    # expression
    "where",
    "expr_field",
    # bucket
    "bucket_by_thresholds",
    "bucket_by_predicate",
    "collect_buckets",
    # flag
    "flag_field",
    "flag_if",
    "flag_compare",
    # score
    "score_field",
    "score_by",
    "rank_by",
    # compare
    "compare_field",
    "compare_fields",
]
