"""Top-k nearest neighbours from embeddings -> sharded static files.

usage: similar.py <embeddings.npy> <ids.parquet> <world_order.parquet> <out_data_dir> [--k 20]

  embeddings.npy   float32 (m, d), row-aligned with ids.parquet (column world_id)
  world_order.parquet  written by build_index.py (work/world_order.parquet): world_id in index order
Neighbours are stored as world *indices* (positions in popularity order) so the client resolves
them through the meta chunks it already fetches. Shards: sim/<i>.bin covers world idx
[i*SHARD, (i+1)*SHARD); worlds without an embedding are simply absent (UI hides the list).
Shard layout (gzip): u32 m | u32 idx[m] | k u8 plane-shuffled u32 nbr[m*k] (k from sim/index.json).
Memory: blocked matmul (QB x CH float32), never materialises the full similarity matrix.
"""

from __future__ import annotations

import argparse
import gzip
import json
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

SHARD = 4096
QB = 512
CH = 16384


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("emb")
    ap.add_argument("ids")
    ap.add_argument("order")
    ap.add_argument("out")
    ap.add_argument("--k", type=int, default=20)
    a = ap.parse_args()
    t0 = time.time()
    emb = np.load(a.emb, mmap_mode="r")
    ids = pq.read_table(a.ids, columns=["world_id"])["world_id"].to_pylist()
    assert len(ids) == emb.shape[0], (len(ids), emb.shape)
    pos = {w: i for i, w in enumerate(pq.read_table(a.order)["world_id"].to_pylist())}
    rows = np.array([r for r, w in enumerate(ids) if w in pos], dtype=np.int64)
    widx = np.array([pos[ids[r]] for r in rows], dtype=np.int64)
    m = len(rows)
    k = min(a.k, m - 1)
    print(f"{m}/{len(ids)} embedded worlds are in the index; d={emb.shape[1]} k={k}")

    def block(lo, hi):
        x = np.asarray(emb[rows[lo:hi]], dtype=np.float32)
        x /= np.maximum(np.linalg.norm(x, axis=1, keepdims=True), 1e-9)
        return x

    nbr = np.zeros((m, k), dtype=np.int64)
    for qs in range(0, m, QB):
        qe = min(m, qs + QB)
        q = block(qs, qe)
        bs = np.full((qe - qs, k), -2.0, dtype=np.float32)
        bi = np.zeros((qe - qs, k), dtype=np.int64)
        for cs in range(0, m, CH):
            ce = min(m, cs + CH)
            s = q @ block(cs, ce).T
            if cs <= qe and qs < ce:  # mask self
                r = np.arange(max(qs, cs), min(qe, ce))
                s[r - qs, r - cs] = -3.0
            kk = min(k, ce - cs)
            part = np.argpartition(-s, kk - 1, axis=1)[:, :kk]
            ps = np.take_along_axis(s, part, 1)
            cs_all = np.concatenate([bs, ps], 1)
            ci_all = np.concatenate([bi, part + cs], 1)
            top = np.argpartition(-cs_all, k - 1, axis=1)[:, :k]
            bs = np.take_along_axis(cs_all, top, 1)
            bi = np.take_along_axis(ci_all, top, 1)
        o = np.argsort(-bs, axis=1)
        nbr[qs:qe] = np.take_along_axis(bi, o, 1)
        if (qs // QB) % 8 == 0:
            print(f"  {qe}/{m} {time.time()-t0:.0f}s", flush=True)
    nbr_w = widx[nbr]  # neighbour -> world index
    out = Path(a.out) / "sim"
    out.mkdir(parents=True, exist_ok=True)
    for f in out.glob("*.bin"):
        f.unlink()
    srt = np.argsort(widx)
    shards = {}
    for r in srt:
        shards.setdefault(int(widx[r]) // SHARD, []).append(r)
    total = 0
    biggest = 0
    for s, rs in shards.items():
        rs = np.array(rs)
        idx = widx[rs].astype("<u4")
        nb = nbr_w[rs].astype("<u4")
        planes = nb.reshape(-1).view(np.uint8).reshape(-1, 4).T.tobytes()
        raw = np.uint32(len(rs)).tobytes() + idx.tobytes() + planes
        data = gzip.compress(raw, 9, mtime=0)
        (out / f"{s}.bin").write_bytes(data)
        total += len(data)
        biggest = max(biggest, len(data))
    (out / "index.json").write_text(json.dumps({"k": k, "shard": SHARD, "shards": sorted(shards), "count": m}))
    print(json.dumps({"embedded": m, "shards": len(shards), "total_bytes": total, "largest": biggest, "seconds": round(time.time() - t0, 1)}))


if __name__ == "__main__":
    main()
