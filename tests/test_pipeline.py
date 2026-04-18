import pytest
from pipekit import Pipeline, Step


def double(x):
    return x * 2


def add_one(x):
    return x + 1


def to_upper(s):
    return s.upper()


class TestStep:
    def test_step_calls_func(self):
        step = Step(double)
        assert step(5) == 10

    def test_step_default_name(self):
        step = Step(double)
        assert step.name == "double"

    def test_step_custom_name(self):
        step = Step(double, name="my_step")
        assert step.name == "my_step"

    def test_step_repr(self):
        step = Step(double)
        assert "double" in repr(step)


class TestPipeline:
    def test_empty_pipeline_returns_input(self):
        p = Pipeline()
        assert p.run(42) == 42

    def test_single_step(self):
        p = Pipeline().pipe(double)
        assert p.run(3) == 6

    def test_multiple_steps_chained(self):
        p = Pipeline().pipe(double).pipe(add_one)
        assert p.run(4) == 9  # 4*2 + 1

    def test_pipe_returns_pipeline(self):
        p = Pipeline()
        result = p.pipe(double)
        assert result is p

    def test_run_each(self):
        p = Pipeline().pipe(double)
        assert p.run_each([1, 2, 3]) == [2, 4, 6]

    def test_steps_property_is_copy(self):
        p = Pipeline().pipe(double)
        steps = p.steps
        steps.clear()
        assert len(p) == 1

    def test_len(self):
        p = Pipeline().pipe(double).pipe(add_one)
        assert len(p) == 2

    def test_string_pipeline(self):
        p = Pipeline().pipe(to_upper)
        assert p.run("hello") == "HELLO"

    def test_repr_contains_step_names(self):
        p = Pipeline().pipe(double).pipe(add_one)
        r = repr(p)
        assert "double" in r
        assert "add_one" in r

    def test_run_each_empty(self):
        p = Pipeline().pipe(double)
        assert p.run_each([]) == []
