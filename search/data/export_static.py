#!/usr/bin/env python3
"""Export the static-similarity-mrl-multilingual-v1 query embedder for browser use + 256-d world vectors.

  export_static.py --emb-work DIR --out-dir DIR

Outputs in out-dir:
  static_tokens_int8.{0..3}.bin   int8 [V,256] row-major, split by rows (first 256 MRL dims of the token table)
  static_tokens_scale.f16  float16 [V] per-row scale (value = int8 * scale)
  static_tokenizer.json    HF tokenizers file (WordPiece; mBERT vocab)
  static_meta.json         vocab size, dim, pooling description, sizes, verification result
  static_{short,full}_256_int8.npy  int8 [N,256] world vectors (global scale in meta) 
  static_{short,full}_256_bin.npy   uint8 [N,32] packed sign bits
Query-time algorithm: ids = tokenizer(q) (no CLS/SEP in the model: verified below) ; v = mean(table[ids]*scale) ;
v /= |v| ; score = v . doc_int8 (or Hamming on sign bits).
"""
import argparse
import gzip
import json
from pathlib import Path

import numpy as np
from sentence_transformers import SentenceTransformer

ap = argparse.ArgumentParser()
ap.add_argument("--emb-work", required=True)
ap.add_argument("--out-dir", required=True)
a = ap.parse_args()
out = Path(a.out_dir)
out.mkdir(parents=True, exist_ok=True)
m = SentenceTransformer("sentence-transformers/static-similarity-mrl-multilingual-v1", device="cpu")
se = m[0]
W = se.embedding.weight.detach().numpy().astype(np.float32)  # [V,1024]
V = W.shape[0]
D = 256
T = W[:, :D]
scale = np.abs(T).max(1) / 127.0
scale[scale == 0] = 1.0
q8 = np.clip(np.round(T / scale[:, None]), -127, 127).astype(np.int8)
# Cloudflare Pages caps files at 25 MiB and the whole int8 table is 25.85 MiB: ship 4 row-chunks.
NCH = 4
rows_per = -(-V // NCH)
for i in range(NCH):
    (out / f"static_tokens_int8.{i}.bin").write_bytes(q8[i * rows_per:(i + 1) * rows_per].tobytes())
(out / "static_tokens_int8.bin").unlink(missing_ok=True)
(out / "static_tokens_scale.f16").write_bytes(scale.astype(np.float16).tobytes())
tok = se.tokenizer
tok.save(str(out / "static_tokenizer.json")) if hasattr(tok, "save") else None

# verify: numpy re-implementation vs the library, truncated to 256 + renormalised
tests = ["horror escape room", "ホラー脱出", "Traditional Japanese House Cat Cafe", "cozy night cafe 夜のカフェ", "vrchat avatar world"]
ref = m.encode(tests, normalize_embeddings=False)[:, :D]
ref /= np.linalg.norm(ref, axis=1, keepdims=True)
deq = q8.astype(np.float32) * scale.astype(np.float16).astype(np.float32)[:, None]
errs, enc_info = [], None
for t, r in zip(tests, ref):
    ids = tok.encode(t).ids if hasattr(tok, "encode") else None
    v = deq[ids].mean(0)
    v /= np.linalg.norm(v)
    errs.append(float(v @ r))
meta = {"vocab": V, "dim": D, "pooling": "mean over token ids from tokenizer.encode (as the library does), then L2 normalise",
        "cos_vs_library_256": errs, "tokenizer_ids_example": {tests[0]: tok.encode(tests[0]).ids},
        "int8_bytes": q8.nbytes, "table_chunks": NCH, "rows_per_chunk": rows_per, "note": "tokenizer.encode adds [CLS]=101/[SEP]=102 and the library mean includes them; do the same in JS", "scale_bytes": scale.size * 2}
print(meta)
import os
meta["tokenizer_json_bytes"] = os.path.getsize(out / "static_tokenizer.json")
meta["table_gzip_bytes"] = len(gzip.compress(q8.tobytes(), 6))
meta["tokenizer_json_gzip_bytes"] = len(gzip.compress((out / "static_tokenizer.json").read_bytes(), 6))

for kind in ("short", "full"):
    x = np.load(Path(a.emb_work) / f"static_{kind}.npy")[:, :D].astype(np.float32)
    x /= np.linalg.norm(x, axis=1, keepdims=True) + 1e-9
    s = float(np.percentile(np.abs(x[:20000]), 99.9))
    xi = np.clip(np.round(x / s * 127), -127, 127).astype(np.int8)
    np.save(out / f"static_{kind}_256_int8.npy", xi)
    np.save(out / f"static_{kind}_256_bin.npy", np.packbits(x > 0, axis=1))
    meta[f"{kind}_int8_scale"] = s  # value = int8/127*scale
json.dump(meta, open(out / "static_meta.json", "w"), indent=1)
print(json.dumps(meta, indent=1))
