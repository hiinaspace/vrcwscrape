#!/usr/bin/env python3
"""Encode thumbnails for shas that are not yet in the pack state.

  thumbs_delta.py --parquet PUBLIC.parquet --state STATE_THUMBS --images /images --out new.parquet [--workers N] [--max-new N]

State: `--state` is the generation-pointer dir of build/thumbstate.py (CURRENT -> gen-N/); the old flat layout is adopted as gen-0.
Reads `<images>/<aa>/<bb>/<sha256>.png` (read-only) and encodes exactly like data/encode_stream.py (Pillow AVIF, 256 px wide, q50, speed 4).
Output new.parquet: sha256, avif, width, height. Missing/undecodable shas are listed in new.parquet.missing.txt and simply get no thumbnail
this week (they are retried next run).
"""
import argparse
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "build"))
from encode_stream import encode  # noqa: E402  (same encoder settings as the original 193k-image run)
import thumbstate  # noqa: E402


def one(args):
    sha, root = args
    p = Path(root) / sha[:2] / sha[2:4] / f"{sha}.png"
    try:
        data = p.read_bytes()
    except OSError:
        return sha, None, "missing"
    try:
        return sha, encode(data), None
    except Exception as e:  # noqa: BLE001
        return sha, None, repr(e)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--state", required=True)
    ap.add_argument("--images", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--max-new", type=int, default=30000)
    a = ap.parse_args()
    t0 = time.time()
    used = {s for s in pq.read_table(a.parquet, columns=["thumb_sha256"])["thumb_sha256"].to_pylist() if s}
    st = thumbstate.load(Path(a.state), verify_hashes=False)  # CURRENT generation, sizes validated (pack_thumbs re-checks the hashes); raises on a bad state
    known = set(st.known) if st else set()
    new = sorted(used - known)
    print(f"thumbs_delta: {len(used)} shas in use, {len(known)} in state, {len(new)} new", flush=True)
    if len(new) > a.max_new:
        print(f"ERROR: {len(new)} new shas > --max-new {a.max_new}", file=sys.stderr)
        return 3
    rows, bad = {}, []
    nbytes = 0
    with ProcessPoolExecutor(a.workers) as pool:
        for sha, res, err in pool.map(one, [(s, a.images) for s in new], chunksize=8):
            if err:
                bad.append(f"{sha}\t{err}")
            else:
                rows[sha] = res
                nbytes += len(res[0])
    shas = sorted(rows)
    t = pa.table({"sha256": shas, "avif": [rows[s][0] for s in shas], "width": pa.array([rows[s][1] for s in shas], pa.int16()),
                  "height": pa.array([rows[s][2] for s in shas], pa.int16())})
    a.out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(t, a.out, compression="none")
    a.out.with_name(a.out.name + ".missing.txt").write_text("\n".join(bad) + ("\n" if bad else ""))
    print(f"thumbs_delta: encoded {len(rows)}, {len(bad)} missing/errors, {nbytes / 1e6:.1f} MB avif, {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
