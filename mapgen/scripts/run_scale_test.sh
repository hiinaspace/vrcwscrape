#!/usr/bin/env bash
# Scale test: bake the two large islands never laid out (idx 0, idx 12).
# idx4 baseline = target 1200 @ 12312 pts. Scale target by point ratio to hold
# ~constant worlds-per-district. Compute (not quota) bound; run in background.
set -u
cd "$(dirname "$0")/.."
ROOT="artifacts/r1/scale"
mkdir -p "$ROOT"
LOG="$ROOT/scale_test.log"
: > "$LOG"

stamp() { date '+%H:%M:%S'; }
run_island() {
  local idx="$1" target="$2" ncores="$3"
  local d="$ROOT/idx${idx}"
  mkdir -p "$d"
  echo "[$(stamp)] ===== ISLAND $idx  target=$target ncores=$ncores =====" | tee -a "$LOG"

  echo "[$(stamp)] inputs idx$idx ..." | tee -a "$LOG"
  local t0=$SECONDS
  uv run python scripts/run_r1_island_inputs.py --island-index "$idx" \
      --out-dir "$d/inputs" >>"$LOG" 2>&1
  local rc=$?
  echo "[$(stamp)] inputs idx$idx rc=$rc  ($((SECONDS-t0))s)" | tee -a "$LOG"
  [ $rc -ne 0 ] && { echo "[$(stamp)] INPUTS FAILED idx$idx, skipping hybrid" | tee -a "$LOG"; return; }

  echo "[$(stamp)] hybrid idx$idx ..." | tee -a "$LOG"
  t0=$SECONDS
  uv run python scripts/run_r1_hybrid.py --in-dir "$d/inputs" \
      --out-dir "$d/hybrid" --total-target "$target" --n-cores "$ncores" >>"$LOG" 2>&1
  rc=$?
  echo "[$(stamp)] hybrid idx$idx rc=$rc  ($((SECONDS-t0))s)" | tee -a "$LOG"
}

echo "[$(stamp)] SCALE TEST START" | tee -a "$LOG"
run_island 0  2200 6
run_island 12 8800 6
echo "[$(stamp)] SCALE TEST DONE" | tee -a "$LOG"
