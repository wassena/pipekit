from typing import Any, Callable, Iterable, List


class Step:
    """A single transformation step in a pipeline."""

    def __init__(self, func: Callable, name: str = None):
        self.func = func
        self.name = name or func.__name__

    def __call__(self, data: Any) -> Any:
        return self.func(data)

    def __repr__(self) -> str:
        return f"Step(name={self.name!r})"


class Pipeline:
    """Composes a sequence of transformation steps applied to data."""

    def __init__(self, steps: List[Step] = None):
        self._steps: List[Step] = steps or []

    def pipe(self, func: Callable, name: str = None) -> "Pipeline":
        """Add a transformation step and return self for chaining."""
        self._steps.append(Step(func, name=name))
        return self

    def run(self, data: Any) -> Any:
        """Execute all steps sequentially on the input data."""
        result = data
        for step in self._steps:
            result = step(result)
        return result

    def run_each(self, items: Iterable[Any]) -> List[Any]:
        """Run the pipeline on each item in an iterable."""
        return [self.run(item) for item in items]

    @property
    def steps(self) -> List[Step]:
        return list(self._steps)

    def __len__(self) -> int:
        return len(self._steps)

    def __repr__(self) -> str:
        names = [s.name for s in self._steps]
        return f"Pipeline(steps={names})"
