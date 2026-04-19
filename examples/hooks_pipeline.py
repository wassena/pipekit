"""Example: using lifecycle hooks in a pipeline."""

from pipekit.pipeline import Pipeline, Step
from pipekit.hooks import before_after, on_error, timed

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log(label):
    def _inner(data):
        print(f"[{label}] {data}")
    return _inner


def _fallback(exc, data):
    print(f"[error] {exc} — returning data unchanged")
    return data


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

@timed
@before_after(before=_log("load:in"), after=_log("load:out"))
def load(data):
    """Simulate loading extra fields."""
    return {**data, "loaded": True}


@on_error(_fallback)
def risky_transform(data):
    """May fail when 'value' is missing."""
    return {**data, "doubled": data["value"] * 2}


@timed
def summarise(data):
    return {k: v for k, v in data.items() if k != "loaded"}


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

pipeline = Pipeline([
    Step(load),
    Step(risky_transform),
    Step(summarise),
])


if __name__ == "__main__":
    good = {"id": 1, "value": 21}
    print("=== good input ===")
    result = pipeline(good)
    print("result :", result)
    print(f"load took   {load.last_duration*1000:.1f} ms")
    print(f"summary took {summarise.last_duration*1000:.1f} ms")

    print()
    bad = {"id": 2}          # 'value' key missing — on_error kicks in
    print("=== bad input ===")
    result = pipeline(bad)
    print("result :", result)
