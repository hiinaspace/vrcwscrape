#!/usr/bin/env bash
# One flock'd GPU job per model on sayu. Usage: run_gpu.sh MODEL [extra embed.py args]
set -u
D=$HOME/code/vrcwscrape-search-data
PY=$HOME/code/sample-map/.venv/bin/python   # existing torch+cu130+sentence-transformers venv (not modified)
m=$1; shift
if pgrep -x -f 'vrserver|monado-service|vrmonitor' >/dev/null || pgrep -a 'vrserver|monado' >/dev/null; then
  echo "VR runtime up; skipping $m"; exit 3; fi
cd $D
export UV_CACHE_DIR=$D/uvcache LD_LIBRARY_PATH=/run/opengl-driver/lib HF_HOME=$D/hf
exec flock /tmp/sayu-gpu.lock timeout 2h uv run --no-project --python $PY --with pandas --with pyarrow python embed.py --model $m --parquet worlds-2026-10-20.parquet \
  --queries queries.parquet --out-dir work "$@"
