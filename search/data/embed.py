#!/usr/bin/env python3
"""Embed all worlds (full text + short text) and eval queries with one model.

  embed.py --model qwen3 --parquet worlds.parquet --queries queries.parquet --out-dir OUT [--device cuda]

Writes OUT/<model>_full.npy, OUT/<model>_short.npy (float16, L2-normalised, N x D, row order =
OUT/ids.parquet world_id order), OUT/<model>_queries.npy. Docs sorted by length for batching.
"""

import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from sentence_transformers import SentenceTransformer

from common import MODELS, full_text, short_text


def encode(model, texts, prompt_name, bs, label):
    order = np.argsort([len(t) for t in texts])[::-1]
    sorted_texts = [texts[i] for i in order]
    t0 = time.time()
    kw = {"prompt_name": prompt_name} if prompt_name else {}
    emb = model.encode(sorted_texts, batch_size=bs, normalize_embeddings=True, convert_to_numpy=True,
                       show_progress_bar=False, **kw)
    out = np.empty_like(emb)
    out[order] = emb
    print(f"{label}: {len(texts)} texts, {time.time()-t0:.0f}s", flush=True)
    return out.astype(np.float16)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True, choices=MODELS)
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--queries")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--device", default="cuda")
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--max-seq", type=int, default=512)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--skip-full", action="store_true")
    a = ap.parse_args()
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    hf_id, kw, doc_p, q_p, dtype = MODELS[a.model]
    df = pd.read_parquet(a.parquet, columns=["world_id", "name", "description", "author_name", "tags"])
    if a.limit:
        df = df.head(a.limit)
    df[["world_id"]].to_parquet(out / "ids.parquet")
    model_kwargs = {"torch_dtype": getattr(torch, dtype)} if a.device == "cuda" else {}
    model = SentenceTransformer(hf_id, device=a.device, model_kwargs=model_kwargs, trust_remote_code=False)
    try:
        model.max_seq_length = min(a.max_seq, model.max_seq_length or a.max_seq)
    except AttributeError:  # StaticEmbedding has no seq limit
        pass
    print(a.model, "dim", model.get_sentence_embedding_dimension(), "max_seq", model.max_seq_length, flush=True)
    if a.queries:
        q = pd.read_parquet(a.queries)
        np.save(out / f"{a.model}_queries.npy", encode(model, q["query"].tolist(), q_p, a.batch, "queries"))
    shorts = [short_text(n, t) for n, t in zip(df["name"], df["tags"])]
    np.save(out / f"{a.model}_short.npy", encode(model, shorts, doc_p, a.batch, "short"))
    if not a.skip_full:
        fulls = [full_text(n, t, au, d) for n, t, au, d in
                 zip(df["name"], df["tags"], df["author_name"], df["description"])]
        np.save(out / f"{a.model}_full.npy", encode(model, fulls, doc_p, a.batch, "full"))
    print("done", flush=True)


if __name__ == "__main__":
    main()
