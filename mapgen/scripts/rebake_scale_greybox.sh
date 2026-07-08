#!/usr/bin/env bash
# Re-bake idx0 + idx12 WITH the greybox export (the scale test skipped it) and
# the final aspect-cap code, then app-export each to a global-frame viewer dir.
# Reuses the existing per-island inputs. Compute-bound; run in background.
set -u
cd "$(dirname "$0")/.."
ROOT="artifacts/r1/scale"
LOG="$ROOT/rebake_greybox.log"
: > "$LOG"
stamp() { date '+%H:%M:%S'; }

bake() {
  local idx="$1" target="$2"
  local d="$ROOT/idx${idx}"
  echo "[$(stamp)] === idx$idx hybrid+greybox (target $target) ===" | tee -a "$LOG"
  uv run python scripts/run_r1_hybrid.py \
      --in-dir "$d/inputs" --out-dir "$d/hybrid" \
      --greybox-out "$d/greybox" --total-target "$target" --n-cores 6 \
      >>"$LOG" 2>&1
  echo "[$(stamp)] idx$idx hybrid rc=$?" | tee -a "$LOG"
  echo "[$(stamp)] === idx$idx app-export -> $ROOT/app/idx${idx} ===" | tee -a "$LOG"
  uv run python scripts/run_r1_app_export.py \
      --greybox-dir "$d/greybox" --out-dir "$ROOT/app/idx${idx}" \
      >>"$LOG" 2>&1
  echo "[$(stamp)] idx$idx app-export rc=$?" | tee -a "$LOG"
}

echo "[$(stamp)] REBAKE START" | tee -a "$LOG"
bake 0  2200
bake 12 8800
echo "[$(stamp)] REBAKE DONE" | tee -a "$LOG"
