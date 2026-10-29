"""Crash-safety tests for the weekly refresh. Run from the repo root (no project env needed; nothing here touches the network):

  uv run --no-project --with pytest --with pyarrow --with numpy --with pandas --with duckdb==1.5.6 --with pymysql pytest search/refresh/tests -q

`kill(step_index)` makes genstate.hook raise Killed at the Nth durable step: an exception stops the process at the same point SIGKILL
would (no cleanup code runs after it in the code under test: they have no except/finally around the steps), and the on-disk state is what a
killed run leaves behind.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
for sub in ("build", "refresh", "data", "deploy"):
    sys.path.insert(0, str(ROOT / sub))

import genstate  # noqa: E402


class Killed(BaseException):
    pass


@pytest.fixture
def steps(monkeypatch):
    """steps.record(): list collecting every hook call; steps.kill_at(i): raise Killed at the i-th (0-based) call."""

    class S:
        def __init__(self):
            self.calls: list[str] = []

        def record(self):
            self.calls.clear()
            monkeypatch.setattr(genstate, "hook", lambda step: self.calls.append(step))

        def kill_at(self, i: int):
            self.calls.clear()

            def h(step):
                if len(self.calls) == i:
                    self.calls.append(step)
                    raise Killed(step)
                self.calls.append(step)

            monkeypatch.setattr(genstate, "hook", h)

        def off(self):
            monkeypatch.setattr(genstate, "hook", lambda step: None)

    return S()
