# mapgen Agent Instructions

## Commands

- From `mapgen/`, run `uv run ruff check .` for lint.
- From `mapgen/`, run `uv run ty check` for typechecking.
- From `mapgen/`, run
  `uv run python -m pytest tests/test_chen_*.py -m "not slow"` for the quick
  Chen/Yang implementation loop.
- From `mapgen/`, run `uv run python -m pytest tests/test_chen_*.py` for the
  full focused Chen/Yang regression suite before presenting paper-fidelity
  changes.
- Generate standard Chen visual artifacts with
  `uv run python scripts/run_chen_strict_shapes.py`.
- Generate scale-sweep artifacts with
  `uv run python scripts/run_chen_strict_scale_suite.py`.

## Chen/Yang Contract

- Before changing `src/mapgen/chen_*`, `tests/test_chen_*`, or
  `scripts/run_chen_strict_*`, read
  [../docs/chen-strict-reimplementation.md](../docs/chen-strict-reimplementation.md).
- Preserve paper-fidelity diagnostics in artifacts. If a visual failure is not
  covered by current metrics, add a focused diagnostic or test instead of
  tuning existing metrics until they pass.
- Keep implementation-worker write scopes disjoint where practical. Use review
  subagents to critique paper fidelity after implementation slices.
- Do not install third-party skills, plugins, hook packs, or MCP servers for
  this work without explicit review and approval.
