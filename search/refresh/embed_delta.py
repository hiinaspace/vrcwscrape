#!/usr/bin/env python3
"""Incremental embeddings for the weekly refresh.

  embed_delta.py --parquet PUBLIC.parquet --state STATE_EMB --out OUT [--threads N] [--max-embed N] [--new-models]

* Text of every world = data/common.py full_text(); its sha1 (salted with the model tag) decides whether a stored vector is still valid.
* Only new/changed worlds are embedded: embeddinggemma-2, CPU fp32 (bf16 on CPU is 2.4x slower), 768-d, L2-normalised, stored as float16.
* The store is a SUPERSET: worlds that are absent from this week's parquet keep their vectors (a bad extract week must not make the next
  good week re-embed 30k worlds behind --max-embed); the build gets exactly the rows it asks for, in parquet order.
* The browser's static model (static-similarity-mrl-multilingual-v1) sign bits (first 256 dims, 32 bytes/world) are computed for the same
  new/changed worlds and cached alongside.
* Both models are pinned to explicit HF revisions (GEMMA, STATIC_REV). meta.json of every store generation records tag + both model ids and
  revisions; a store whose meta differs from the code's is refused (rc 4: its vectors are in other coordinates) unless --new-models, which
  starts an empty store (attended: the cap must be raised too). HF_HUB_OFFLINE=1 is set when the local HF cache holds both revisions.
* State (STATE_EMB): CURRENT -> gen-<N>/{ids.parquet (world_id, text_sha), gemma2_full_f16.npy, static_full_256_bin.npy, static/ (constant
  model assets), meta.json} (build/genstate.py). A new generation is written completely, then CURRENT is replaced atomically; a kill at any
  point leaves the previous generation current. The old flat layout is adopted as gen-0 (renames, resumable).
  OUT gets ids.parquet / gemma2_full_f16.npy (row order = the parquet), meta.json and static/ with the new static_full_256_bin.npy, ready
  for build/similar.py and build/semantic.py.
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
STATIC_REV = "b68f4122911bcffcd6e1f695f2d99cd6788972d8"  # the snapshot in the seeded state's HF cache (2026-10)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "build"))
import genstate  # noqa: E402
from genstate import StateError  # noqa: E402


def model_meta() -> dict:
    return {"tag": TAG, "gemma": [GEMMA[0], GEMMA[1]], "static": [STATIC, STATIC_REV]}


def hf_cached(model: str, rev: str) -> bool:
    hub = Path(os.environ.get("HF_HUB_CACHE") or Path(os.environ.get("HF_HOME") or Path.home() / ".cache/huggingface") / "hub")
    return (hub / ("models--" + model.replace("/", "--")) / "snapshots" / rev).is_dir()


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


def compute(ntexts: list[str], threads: int, batch: int) -> tuple[np.ndarray, np.ndarray]:
    """(gemma embeddings float32, static sign bits) for the given texts."""
    if hf_cached(STATIC, STATIC_REV) and hf_cached(GEMMA[0], GEMMA[1]):
        os.environ["HF_HUB_OFFLINE"] = "1"  # warm cache: an HF outage must not add timeouts to the run
    import torch
    from sentence_transformers import SentenceTransformer

    t0 = time.time()
    torch.set_num_threads(threads)
    sm = SentenceTransformer(STATIC, device="cpu", trust_remote_code=False, revision=STATIC_REV, model_kwargs={"dtype": torch.float32})
    nb = static_bits(sm, ntexts)
    print(f"static bits for {len(ntexts)} worlds {time.time() - t0:.0f}s", flush=True)
    hf_id, rev, doc_prompt = GEMMA
    # float32 explicitly: the checkpoint is bf16 and sentence-transformers keeps that dtype on CPU, which is ~3.5x slower (3.7 vs 13 worlds/s on the i5-10500 nodes)
    model = SentenceTransformer(hf_id, device="cpu", trust_remote_code=False, revision=rev, model_kwargs={"dtype": torch.float32})
    model.max_seq_length = min(512, model.max_seq_length or 512)
    return encode_sorted(model, ntexts, batch, "gemma2", prompt_name=doc_prompt), nb


class Store:
    def __init__(self, path: Path):
        self.path = path
        self.ids_df = pd.read_parquet(path / "ids.parquet")
        self.vec = np.load(path / "gemma2_full_f16.npy", mmap_mode="r")
        self.bits = np.load(path / "static_full_256_bin.npy", mmap_mode="r")
        mp = path / "meta.json"
        self.meta = json.loads(mp.read_text()) if mp.is_file() else None
        n = len(self.ids_df)
        if not (n == len(self.vec) == len(self.bits)) or self.vec.shape[1:] != (768,) or self.bits.shape[1:] != (32,) or self.ids_df["world_id"].duplicated().any():
            raise StateError(f"{path}: inconsistent store (ids {n}, vec {self.vec.shape}, bits {self.bits.shape})")
        if not (path / "static").is_dir():
            raise StateError(f"{path}: static model assets missing")


def load_store(state: Path) -> Store | None:
    def finish(g0: Path) -> None:  # adopting a flat/seeded store: stamp the models it is assumed to come from
        if not (g0 / "meta.json").is_file():
            genstate.write_durable(g0 / "meta.json", json.dumps(model_meta()).encode())

    if state.is_dir():
        flat = [(f, f) for f in ("ids.parquet", "gemma2_full_f16.npy", "static_full_256_bin.npy", "static")]
        genstate.migrate(state, flat, finish=finish)
    g = genstate.resolve(state)
    return Store(g) if g else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--state", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--threads", type=int, default=int(os.environ.get("EMBED_THREADS", "8")))
    ap.add_argument("--max-embed", type=int, default=8000, help="abort when more worlds than this need embedding (seed mismatch / model change)")
    ap.add_argument("--batch", type=int, default=8)
    ap.add_argument("--new-models", action="store_true", help="ignore a store made with other model revisions (starts empty; raise --max-embed)")
    a = ap.parse_args()
    state, out = Path(a.state), Path(a.out)
    t0 = time.time()
    df = pd.read_parquet(a.parquet, columns=["world_id", "name", "description", "author_name", "tags"])
    ids = df["world_id"].tolist()
    texts = texts_of(df)
    shas = [text_sha(t) for t in texts]
    store = load_store(state)
    if store and store.meta != model_meta():
        if not a.new_models:
            print(f"ERROR: store {store.path} was made with {store.meta}, code pins {model_meta()}; its vectors are in other coordinates. "
                  "Run attended with --new-models and a higher --max-embed to start a new store.", file=sys.stderr)
            return 4
        print("embed_delta: --new-models: ignoring the stored vectors", flush=True)
        store = None
    old = {w: (i, s) for i, (w, s) in enumerate(zip(store.ids_df["world_id"], store.ids_df["text_sha"]))} if store else {}
    n = len(ids)
    need = [i for i, (w, s) in enumerate(zip(ids, shas)) if (old.get(w) or (None, None))[1] != s]
    kept = n - len(need)
    absent = len(set(old) - set(ids))
    print(f"embed_delta: {n} worlds, {kept} reused, {len(need)} to embed, {absent} in the store but not in this parquet (kept)", flush=True)
    if len(need) > a.max_embed:
        print(f"ERROR: {len(need)} worlds need embedding (> --max-embed {a.max_embed}); refusing (CPU budget). Run attended with a higher cap.", file=sys.stderr)
        return 3
    # the new store = the old one (every row kept) with changed rows replaced in place and new worlds appended
    sids = list(store.ids_df["world_id"]) if store else []
    sshas = list(store.ids_df["text_sha"]) if store else []
    svec = np.array(store.vec) if store else np.zeros((0, 768), dtype=np.float16)
    sbits = np.array(store.bits) if store else np.zeros((0, 32), dtype=np.uint8)
    sdir = store.path / "static" if store else None
    store_path = store.path if store else None
    store = None  # drop the mmaps before anything is removed
    pos = {w: i for i, w in enumerate(sids)}
    if need:
        emb, nb = compute([texts[i] for i in need], a.threads, a.batch)
        emb16 = emb.astype(np.float16)
        app = []
        for k, i in enumerate(need):
            j = pos.get(ids[i])
            if j is None:
                pos[ids[i]] = len(sids) + len(app)
                app.append(k)
            else:
                svec[j], sbits[j], sshas[j] = emb16[k], nb[k], shas[i]
        if app:
            svec = np.concatenate([svec, emb16[app]])
            sbits = np.concatenate([sbits, nb[app]])
            sids += [ids[need[k]] for k in app]
            sshas += [shas[need[k]] for k in app]
    idx = np.fromiter((pos[w] for w in ids), dtype=np.int64, count=n)
    vec, bits = svec[idx], sbits[idx]
    # OUT for the build (row order = the parquet)
    shutil.rmtree(out, ignore_errors=True)
    (out / "static").mkdir(parents=True)
    pd.DataFrame({"world_id": ids, "text_sha": shas}).to_parquet(out / "ids.parquet")
    np.save(out / "gemma2_full_f16.npy", vec)
    for f in sdir.iterdir() if sdir else []:
        if f.name != "static_full_256_bin.npy":
            shutil.copyfile(f, out / "static" / f.name)
    np.save(out / "static" / "static_full_256_bin.npy", bits)
    np.save(out / "static_full_256_bin.npy", bits)
    (out / "meta.json").write_text(json.dumps(model_meta()))
    if need or store_path is None:  # nothing to store otherwise: the current generation already holds every row
        def populate(tmp: Path) -> None:
            genstate.hook("write-ids")
            pd.DataFrame({"world_id": sids, "text_sha": sshas}).to_parquet(tmp / "ids.parquet")
            genstate.hook("write-vec")
            np.save(tmp / "gemma2_full_f16.npy", svec)
            genstate.hook("write-bits")
            np.save(tmp / "static_full_256_bin.npy", sbits)
            genstate.hook("link-static")
            (tmp / "static").mkdir()
            for f in sdir.iterdir() if sdir else []:  # constant model assets: shared with the previous generation by hard link
                genstate.link_or_copy(f, tmp / "static" / f.name)
            genstate.hook("write-meta")
            genstate.write_durable(tmp / "meta.json", json.dumps(model_meta()).encode())

        genstate.publish(state, populate)
    print(json.dumps({"worlds": n, "reused": kept, "embedded": len(need), "store_rows": len(svec), "seconds": round(time.time() - t0, 1)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
