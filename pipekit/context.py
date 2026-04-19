"""Pipeline execution context for passing metadata between steps."""

from typing import Any


class PipelineContext:
    """Carries metadata and shared state across pipeline steps.

    Example::

        ctx = PipelineContext(run_id="abc123", env="prod")
        ctx.set("record_count", 42)
        print(ctx.get("record_count"))  # 42
    """

    def __init__(self, **metadata: Any) -> None:
        self._metadata = dict(metadata)
        self._store: dict[str, Any] = {}

    @property
    def metadata(self) -> dict[str, Any]:
        """Immutable metadata passed at construction time."""
        return dict(self._metadata)

    def set(self, key: str, value: Any) -> None:
        """Store a value in the context."""
        self._store[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from the context."""
        return self._store.get(key, default)

    def has(self, key: str) -> bool:
        """Check whether a key exists in the context store."""
        return key in self._store

    def all(self) -> dict[str, Any]:
        """Return a copy of the entire context store."""
        return dict(self._store)

    def __repr__(self) -> str:
        return f"PipelineContext(metadata={self._metadata}, store={self._store})"


def with_context(ctx: PipelineContext):
    """Decorator that injects a PipelineContext as the second argument to a step.

    Example::

        ctx = PipelineContext(run_id="x")

        @with_context(ctx)
        def my_step(data, context):
            context.set("count", len(data))
            return data
    """
    def decorator(func):
        def wrapper(data):
            return func(data, ctx)
        wrapper.__name__ = func.__name__
        wrapper.__wrapped__ = func
        return wrapper
    return decorator
