"""Opt-in browser semantic search assets (static-similarity-mrl-multilingual-v1, 256-d).

usage: semantic.py <emb/static dir> <emb/ids.parquet (full row order)> <world_order.parquet> <out_data_dir>
Writes <out>/sem/{meta.json,tokens.<i>.bin,scale.bin,tokenizer.bin,vecs.bin}; vecs.bin = packed sign bits
[n,32] in *index order* (public worlds only). Everything gzip'd except meta.json; token chunks <= 25 MiB.
"""

import gzip
import json
import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

src, ids_p, order_p, out = Path(sys.argv[1]), sys.argv[2], sys.argv[3], Path(sys.argv[4]) / "sem"
out.mkdir(parents=True, exist_ok=True)
meta = json.load(open(src / "static_meta.json"))
full = {w: i for i, w in enumerate(pq.read_table(ids_p, columns=["world_id"])["world_id"].to_pylist())}
order = pq.read_table(order_p)["world_id"].to_pylist()
bins = np.load(src / "static_full_256_bin.npy", mmap_mode="r")
rows = np.array([full[w] for w in order])
vecs = np.ascontiguousarray(bins[rows])
assert vecs.shape == (len(order), 32), vecs.shape


def w(name, data):
    (out / name).write_bytes(gzip.compress(data, 6, mtime=0))


w("vecs.bin", vecs.tobytes())
for i in range(meta["table_chunks"]):
    w(f"tokens.{i}.bin", (src / f"static_tokens_int8.{i}.bin").read_bytes())
w("scale.bin", (src / "static_tokens_scale.f16").read_bytes())
w("tokenizer.bin", (src / "static_tokenizer.json").read_bytes())
(out / "meta.json").write_text(json.dumps({"vocab": meta["vocab"], "dim": meta["dim"], "chunks": meta["table_chunks"], "rowsPerChunk": meta["rows_per_chunk"], "n": len(order)}))
tot = sum(f.stat().st_size for f in out.iterdir())
print(f"sem assets: {tot/1e6:.1f} MB; largest {max(f.stat().st_size for f in out.iterdir())/1e6:.1f} MB")
