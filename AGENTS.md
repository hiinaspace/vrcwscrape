# vrcwscrape Agent Instructions

## Repository Defaults

- Use `uv` for Python work in this repository.
- For new Python project scaffolding, set up `ruff format`, `ruff check`, and
  `ty` typechecking as pre-commit hooks.
- Prefer narrow, verified changes over broad rewrites. Keep unrelated generated
  artifacts and local worktree changes intact.
- The user is an experienced SWE. Be direct about uncertain assumptions,
  especially in domains outside normal backend/systems work.

## Chen/Yang Layout Work

- The focused Chen 2024/Yang 2013 reimplementation is a paper-fidelity project,
  not a metric-only optimization loop.
- Before editing `mapgen/src/mapgen/chen_*`, `mapgen/tests/test_chen_*`, or the
  Chen artifact scripts, read [docs/chen-strict-reimplementation.md](docs/chen-strict-reimplementation.md).
- Do not declare a Chen/Yang implementation wave complete while the contract doc
  has an active `Blocking` paper-fidelity gap that is neither assigned to a
  follow-up worker nor explicitly deferred with a reason.
- For long Chen/Yang work, keep the main thread focused on orchestration and use
  subagents for bounded implementation, source-audit, test, and review slices.
