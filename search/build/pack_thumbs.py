"""Pack self-hosted AVIF thumbnails into <=25 MiB range-addressable files + a small lazy index.

usage: pack_thumbs.py <thumbs.parquet> <world_order.parquet> <out_dir> [--manifest data/manifest.json]
                      [--pack-mib 24] [--base ../thumbs/] [--index-dir DIR]

  <thumbs.parquet>      world_id, sha256, avif (binary), width, height -- one row per world with an image (several worlds may share a sha)
  <world_order.parquet> world_id in site ordinal order (written by build_index.py as work/world_order.parquet)
Writes   <out_dir>/p-000.bin, p-001.bin ...   concatenated AVIFs, unique images only, in popularity order of first use
         <index-dir>/thumbs_index.bin          gz; world ordinal -> blob id, blob lengths, pack table (see Thumbs in web/src/thumbs.ts)
and sets manifest["thumbs"] = {base, packs, count, blobs, bytes, index}. `base` is resolved by the browser relative to data/ unless absolute
(set it to e.g. https://thumbs.example.com/v1/ when the packs are hosted elsewhere; that host must allow CORS + Range).

Index (all little endian; every file gzip'd and magic-sniffed like the other payloads):
  <index-dir>/thumbs_index.bin  "THM2" u32 nWorlds u32 nBlobs u32 nPacks u32 WS u32 BS ; nPacks x (u32 firstBlob, u32 bytes);
                                ceil(nWorlds/WS) x u32 maxBlobBefore (-1 as 0xFFFFFFFF); ceil(nBlobs/BS) x u32 offsetOfFirstBlobInItsPack
  <index-dir>/th/w<k>.bin       world shard k (WS worlds): u32 codes as 4 byte planes. 0 = next fresh blob, 1 = no thumbnail,
                                c >= 2 = an already-used blob: (maxSeen - (c - 2))
  <index-dir>/th/b<k>.bin       blob shard k (BS blobs): u32 lengths as 4 byte planes (rare AVIFs exceed 64 KiB)
Blobs are stored back to back inside a pack in blob-id order; a blob's offset is the prefix sum of lengths from the pack's first blob
(reset to 0 at each pack boundary). Blob ids are assigned in order of first use by world ordinal (= popularity), so the popular worlds
live in the first shards/packs and a result page touches only a handful of tiny shards.
"""

from __future__ import annotations

import argparse
import gzip
import json
import struct
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq


def planes(a: np.ndarray) -> bytes:
    a = np.ascontiguousarray(a.astype("<" + a.dtype.str[1:]))
    return a.view(np.uint8).reshape(len(a), a.dtype.itemsize).T.tobytes()


WS = 4096  # worlds per world shard (default; --ws)
BS = 2048  # blobs per blob shard (default; --bs)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("thumbs")
    ap.add_argument("order")
    ap.add_argument("out")
    ap.add_argument("--manifest", default=None)
    ap.add_argument("--pack-mib", type=float, default=24.0)
    ap.add_argument("--base", default="../thumbs/")
    ap.add_argument("--index-dir", default=None)
    ap.add_argument("--ws", type=int, default=WS)
    ap.add_argument("--bs", type=int, default=BS)
    a = ap.parse_args()
    WS_, BS_ = a.ws, a.bs
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    idx_dir = Path(a.index_dir) if a.index_dir else out
    limit = int(a.pack_mib * 1048576)

    order = pq.read_table(a.order, columns=["world_id"])["world_id"].to_pylist()
    ord_of = {w: i for i, w in enumerate(order)}
    n = len(order)

    # world -> sha (first row wins), sha -> avif bytes (unique)
    pf = pq.ParquetFile(a.thumbs)
    sha_of: dict[int, str] = {}
    avif: dict[str, bytes] = {}
    for batch in pf.iter_batches(batch_size=4096, columns=["world_id", "sha256", "avif"]):
        for w, s, b in zip(batch["world_id"].to_pylist(), batch["sha256"].to_pylist(), batch["avif"].to_pylist()):
            i = ord_of.get(w)
            if i is None or s is None or not b:
                continue
            sha_of[i] = s
            if s not in avif:
                avif[s] = b

    code = np.ones(n, dtype=np.uint32)  # 1 = none
    blob_of: dict[str, int] = {}
    lens: list[int] = []
    packs: list[list[int]] = []  # [firstBlob, bytes]
    cur = bytearray()
    cur_first = 0
    pack_no = 0

    def flush() -> None:
        nonlocal cur, cur_first, pack_no
        if not lens or (packs and packs[-1][0] == cur_first) or not cur:
            return
        (out / f"p-{pack_no:03d}.bin").write_bytes(cur)
        packs.append([cur_first, len(cur)])
        pack_no += 1
        cur = bytearray()
        cur_first = len(lens)

    maxseen = -1
    for i in range(n):
        s = sha_of.get(i)
        if s is None:
            continue
        b = blob_of.get(s)
        if b is None:
            data = avif[s]
            if len(cur) + len(data) > limit:
                flush()
            b = len(lens)
            blob_of[s] = b
            lens.append(len(data))
            cur += data
            code[i] = 0
            maxseen = b
        else:
            code[i] = 2 + (maxseen - b)
    flush()

    nb_ = len(lens)
    pack_first = [f for f, _ in packs]
    # offset of each blob inside its pack
    off = np.zeros(nb_, dtype=np.int64)
    for k, (f, _) in enumerate(packs):
        end = packs[k + 1][0] if k + 1 < len(packs) else nb_
        if end > f:
            off[f:end] = np.concatenate([[0], np.cumsum(lens[f : end - 1])]) if end - f > 1 else 0
    nws = -(-n // WS_)
    nbs = -(-nb_ // BS_)
    max_before = np.zeros(nws, dtype=np.uint32)
    fresh = (code == 0).astype(np.int64)
    cum = np.cumsum(fresh)  # cum[i] = fresh blobs up to and including i
    for k in range(nws):
        j = k * WS_
        max_before[k] = (int(cum[j - 1]) - 1) & 0xFFFFFFFF if j > 0 else 0xFFFFFFFF
    hdr = b"THM2" + struct.pack("<IIIII", n, nb_, len(packs), WS_, BS_) + b"".join(struct.pack("<II", f, nb) for f, nb in packs)
    hdr += max_before.astype("<u4").tobytes() + np.array([off[k * BS_] for k in range(nbs)], dtype="<u4").tobytes()
    idx_dir.mkdir(parents=True, exist_ok=True)
    (idx_dir / "th").mkdir(exist_ok=True)
    (idx_dir / "thumbs_index.bin").write_bytes(gzip.compress(hdr, 9, mtime=0))
    shard_bytes = (idx_dir / "thumbs_index.bin").stat().st_size
    for k in range(nws):
        d = gzip.compress(planes(code[k * WS_ : (k + 1) * WS_]), 9, mtime=0)
        (idx_dir / "th" / f"w{k}.bin").write_bytes(d)
        shard_bytes += len(d)
    for k in range(nbs):
        d = gzip.compress(planes(np.array(lens[k * BS_ : (k + 1) * BS_], dtype=np.uint32)), 9, mtime=0)
        (idx_dir / "th" / f"b{k}.bin").write_bytes(d)
        shard_bytes += len(d)
    total = sum(nb for _, nb in packs)
    info = {
        "base": a.base, "packs": len(packs), "count": int((code != 1).sum()), "blobs": len(lens), "bytes": total,
        "index": "thumbs_index.bin",
    }
    if a.manifest:
        mp = Path(a.manifest)
        m = json.loads(mp.read_text())
        m["thumbs"] = info
        m["hasThumb"] = True
        mp.write_text(json.dumps(m, ensure_ascii=False, separators=(",", ":")))
    print(json.dumps({**info, "avg_bytes": round(total / max(1, len(lens))), "index_gz_bytes_total": shard_bytes, "index_files": 1 + nws + nbs,
                      "max_pack": max(nb for _, nb in packs) if packs else 0}))


if __name__ == "__main__":
    main()
