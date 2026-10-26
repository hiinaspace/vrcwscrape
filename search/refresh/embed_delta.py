#!/usr/bin/env python3
"""Incremental embeddings for the weekly refresh.

  embed_delta.py --parquet PUBLIC.parquet --state STATE_EMB --out OUT [--threads N] [--max-embed N]

* Text of every world = data/common.py full_text(); its sha1 (salted with the model tag) decides whether a stored vector is still valid.
* Only new/changed worlds are embedded: embeddinggemma-2, CPU fp32 (bf16 on CPU is 2.4x slower), 768-d, L2-normalised, stored as float16.
  Worlds that left the parquet are dropped from the store.
* The browser's static model (static-similarity-mrl-multilingual-v1) sign bits (first 256 dims, 32 bytes/world) are computed for the same
  new/changed worlds and cached alongside.
* State (STATE_EMB): ids.parquet (world_id, text_sha), gemma2_full_f16.npy, static_full_256_bin.npy, static/ (constant model assets).
  OUT gets ids.parquet / gemma2_full_f16.npy (row order = the parquet) and static/ with the new static_full_256_bin.npy, ready for
  build/similar.py and build/semantic.py. The state is replaced atomically only after everything succeeded.
"""
import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data"))
from common import full_text  # noqa: E402

TAG = "gemma2-v1"
GEMMA = ("google/embeddinggemma-2", "914f7f89142e33e77833254d9c9b90c3cef7303b", "Document")
STATIC = "sentence-transformers/static-similarity-mrl-multilingual-v1"


def text_sha(t: str) -> str:
    return hashlib.sha1((TAG + "\0" + t).encode("utf-8")).hexdigest()


def texts_of(df: pd.DataFrame) -> list[str]:
    return [full_text(n, t, a, d) for n, t, a, d in zip(df["name"], df["tags"], df["author_name"], df["description"])]


def encode_sorted(model, texts, bs, label, **kw):
    order = np.argsort([len(t) for t in texts])[::-1]
    t0 = time.time()
    out = None
    done = 0
    for lo in range(0, len(texts), 512):
        idx = order[lo : lo + 512]
        e = model.encode([texts[i] for i in idx], batch_size=bs, normalize_embeddings=True, convert_to_numpy=True, show_progress_bar=False, **kw)
        if out is None:
            out = np.empty((len(texts), e.shape[1]), dtype=np.float32)
        out[idx] = e
        done += len(idx)
        print(f"  {label}: {done}/{len(texts)} {time.time() - t0:.0f}s", flush=True)
    return out


def static_bits(model, texts) -> np.ndarray:
    e = model.encode(texts, batch_size=256, normalize_embeddings=False, convert_to_numpy=True, show_progress_bar=False)[:, :256]
    return np.packbits(e > 0, axis=1)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--state", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--threads", type=int, default=int(os.environ.get("OMP_NUM_THREADS", "8")))
    ap.add_argument("--max-embed", type=int, default=8000, help="abort when more worlds than this need embedding (seed mismatch / model change)")
    ap.add_argument("--batch", type=int, default=16)
    a = ap.parse_args()
    state, out = Path(a.state), Path(a.out)
    t0 = time.time()
    df = pd.read_parquet(a.parquet, columns=["world_id", "name", "description", "author_name", "tags"])
    ids = df["world_id"].tolist()
    texts = texts_of(df)
    shas = [text_sha(t) for t in texts]
    old = {}
    old_vec = old_bits = None
    if (state / "ids.parquet").is_file():
        o = pd.read_parquet(state / "ids.parquet")
        old_vec = np.load(state / "gemma2_full_f16.npy", mmap_mode="r")
        old_bits = np.load(state / "static_full_256_bin.npy", mmap_mode="r")
        assert len(o) == len(old_vec) == len(old_bits), (len(o), len(old_vec), len(old_bits))
        old = {w: (i, s) for i, (w, s) in enumerate(zip(o["world_id"], o["text_sha"]))}
    n = len(ids)
    vec = np.zeros((n, 768), dtype=np.float16)
    bits = np.zeros((n, 32), dtype=np.uint8)
    need = []
    for i, (w, s) in enumerate(zip(ids, shas)):
        j = old.get(w)
        if j is not None and j[1] == s:
            vec[i] = old_vec[j[0]]
            bits[i] = old_bits[j[0]]
        else:
            need.append(i)
    kept = n - len(need)
    gone = len(set(old) - set(ids))
    print(f"embed_delta: {n} worlds, {kept} reused, {len(need)} to embed, {gone} dropped from store", flush=True)
    if len(need) > a.max_embed:
        print(f"ERROR: {len(need)} worlds need embedding (> --max-embed {a.max_embed}); refusing (CPU budget). Run attended with a higher cap.", file=sys.stderr)
        return 3
    if need:
        import torch
        from sentence_transformers import SentenceTransformer

        torch.set_num_threads(a.threads)
        ntexts = [texts[i] for i in need]
        sm = SentenceTransformer(STATIC, device="cpu", trust_remote_code=False)
        nb = static_bits(sm, ntexts)
        print(f"static bits for {len(need)} worlds {time.time() - t0:.0f}s", flush=True)
        hf_id, rev, doc_prompt = GEMMA
        model = SentenceTransformer(hf_id, device="cpu", trust_remote_code=False, revision=rev)
        model.max_seq_length = min(512, model.max_seq_length or 512)
        emb = encode_sorted(model, ntexts, a.batch, "gemma2", prompt_name=doc_prompt)
        vec[need] = emb.astype(np.float16)
        bits[need] = nb
    # write OUT, then swap the state
    shutil.rmtree(out, ignore_errors=True)
    (out / "static").mkdir(parents=True)
    pd.DataFrame({"world_id": ids, "text_sha": shas}).to_parquet(out / "ids.parquet")
    np.save(out / "gemma2_full_f16.npy", vec)
    sa = state / "static"
    for f in sa.iterdir() if sa.is_dir() else []:
        if f.name != "static_full_256_bin.npy":
            shutil.copyfile(f, out / "static" / f.name)
    np.save(out / "static" / "static_full_256_bin.npy", bits)
    np.save(out / "static_full_256_bin.npy", bits)
    state.mkdir(parents=True, exist_ok=True)
    tmp = state.with_name(state.name + ".new")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    for f in ("ids.parquet", "gemma2_full_f16.npy", "static_full_256_bin.npy"):
        shutil.copyfile(out / f, tmp / f)
    if sa.is_dir():
        shutil.copytree(sa, tmp / "static")
    bak = state.with_name(state.name + ".old")
    shutil.rmtree(bak, ignore_errors=True)
    os.rename(state, bak)
    os.rename(tmp, state)
    shutil.rmtree(bak, ignore_errors=True)
    print(json.dumps({"worlds": n, "reused": kept, "embedded": len(need), "dropped": gone, "seconds": round(time.time() - t0, 1)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
