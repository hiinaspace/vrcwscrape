#!/usr/bin/env python3
"""One-off: seed the refresh state volume from the hand-built 2026-10-20 snapshot (run on cc-0, then rsync to chirashi).

  seed_state.py emb    --data /work/projects/vrcwscrape-data --full worlds-2026-10-20.parquet --out SEED/emb
  seed_state.py thumbs --dist <live dist dir> --by-sha thumbs/thumbs-by-sha.parquet --out SEED/thumbs
  seed_state.py ids    --public worlds-2026-10-20-public-v2.parquet --out SEED/deployed-ids.parquet

emb:    state vectors/static bits = the stored GPU vectors keyed by the sha1 of the same full_text the new run computes.
thumbs: reconstructs the sha -> blob table of the live packs by matching each blob's bytes to thumbs-by-sha (exact), and copies the packs
        unchanged (so they stay byte-identical to what is live).
"""
import argparse
import gzip
import hashlib
import json
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "build"))
import genstate  # noqa: E402
from embed_delta import model_meta, text_sha, texts_of  # noqa: E402


def planes_to_u32(buf: bytes) -> np.ndarray:
    cnt = len(buf) // 4
    a = np.frombuffer(buf, dtype=np.uint8).reshape(4, cnt).astype(np.uint32)
    return a[0] | (a[1] << 8) | (a[2] << 16) | (a[3] << 24)


def seed_emb(a):
    d = Path(a.data)
    out = Path(a.out) / "gen-0"  # generation layout (build/genstate.py); CURRENT is written last
    (out / "static").mkdir(parents=True, exist_ok=True)
    ids = pd.read_parquet(d / "emb/ids.parquet")["world_id"].tolist()
    full = pd.read_parquet(d / a.full, columns=["world_id", "name", "description", "author_name", "tags"]).set_index("world_id")
    df = full.loc[ids].reset_index()
    shas = [text_sha(t) for t in texts_of(df)]
    vec = np.load(d / "emb/gemma2_full_f16.npy")
    bits = np.load(d / "emb/static/static_full_256_bin.npy")
    assert len(ids) == len(vec) == len(bits)
    pd.DataFrame({"world_id": ids, "text_sha": shas}).to_parquet(out / "ids.parquet")
    np.save(out / "gemma2_full_f16.npy", vec)
    np.save(out / "static_full_256_bin.npy", bits)
    for f in (d / "emb/static").iterdir():
        if f.name.startswith(("static_tokens", "static_tokenizer", "static_meta")):
            shutil.copyfile(f, out / "static" / f.name)
    (out / "meta.json").write_text(json.dumps(model_meta()))
    genstate.fsync_tree(out)
    genstate.write_pointer(Path(a.out), "gen-0")
    print("emb seeded:", len(ids), "worlds")


def seed_thumbs(a):
    dist, out = Path(a.dist), Path(a.out) / "gen-0"  # generation layout (build/thumbstate.py); CURRENT is written last
    out.mkdir(parents=True, exist_ok=True)
    gz = lambda p: gzip.decompress(Path(p).read_bytes())  # noqa: E731
    hdr = gz(dist / "data/thumbs_index.bin")
    assert hdr[:4] == b"THM2"
    n, nb, np_, ws, bs = np.frombuffer(hdr[4:24], dtype="<u4")
    packs = [tuple(np.frombuffer(hdr[24 + 8 * k : 32 + 8 * k], dtype="<u4")) for k in range(np_)]
    lens = np.concatenate([planes_to_u32(gz(dist / f"data/th/b{k}.bin")) for k in range(-(-int(nb) // int(bs)))]).astype(np.int64)
    assert len(lens) == nb
    # sha by avif content hash
    by = pq.read_table(a.by_sha, columns=["sha256", "avif"])
    h2sha: dict[bytes, list[str]] = {}  # identical AVIF bytes can belong to several PNG shas: each sha gets one of the identical blobs
    for s, b in zip(by["sha256"].to_pylist(), by["avif"].to_pylist()):
        h2sha.setdefault(hashlib.sha1(b).digest(), []).append(s)
    rows, plist = [], []
    for k, (first, nbytes) in enumerate(packs):
        end = int(packs[k + 1][0]) if k + 1 < len(packs) else int(nb)
        src = dist / "thumbs" / f"p-{k:03d}.bin"
        data = src.read_bytes()
        assert len(data) == nbytes, (k, len(data), nbytes)
        shutil.copyfile(src, out / src.name)
        o = 0
        for b in range(int(first), end):
            ln = int(lens[b])
            cands = h2sha.get(hashlib.sha1(data[o : o + ln]).digest())
            assert cands, f"blob {b} not in thumbs-by-sha"
            sha = cands.pop(0)
            rows.append((sha, b, ln))
            o += ln
        assert o == nbytes
        plist.append({"first": int(first), "bytes": int(nbytes), "hash": hashlib.sha256(data).hexdigest()[:16], "kind": "base"})
    pq.write_table(pa.table({"sha256": [r[0] for r in rows], "blob": pa.array([r[1] for r in rows], pa.int64()), "len": pa.array([r[2] for r in rows], pa.int64())}),
                   out / "blobs.parquet")
    (out / "packs.json").write_text(json.dumps({"packs": plist}))
    genstate.fsync_tree(out)
    genstate.write_pointer(Path(a.out), "gen-0")
    print("shas without a blob:", sum(len(v) for v in h2sha.values()))
    print("thumbs seeded:", len(rows), "blobs,", len(plist), "packs")


def seed_ids(a):
    t = pq.read_table(a.public, columns=["world_id"])
    pq.write_table(t, a.out)
    print("ids:", t.num_rows)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["emb", "thumbs", "ids"])
    ap.add_argument("--data")
    ap.add_argument("--full")
    ap.add_argument("--dist")
    ap.add_argument("--by-sha")
    ap.add_argument("--public")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    {"emb": seed_emb, "thumbs": seed_thumbs, "ids": seed_ids}[a.what](a)
