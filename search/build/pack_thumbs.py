"""Pack self-hosted AVIF thumbnails into <=25 MiB range-addressable files + a small lazy index.

usage: pack_thumbs.py <thumbs.parquet> <world_order.parquet> <out_dir> [--manifest data/manifest.json]
                      [--pack-mib 24] [--base ../thumbs/] [--index-dir DIR]

  <thumbs.parquet>      world_id, sha256, avif (binary), width, height -- one row per world with an image (several worlds may share a sha)
  <world_order.parquet> world_id in site ordinal order (written by build_index.py as work/world_order.parquet)
Writes   <out_dir>/p-000.bin, p-001.bin ...   concatenated AVIFs, unique images only, in popularity order of first use
         <index-dir>/thumbs_index.bin          gz; world ordinal -> blob id, blob lengths, pack table (see Thumbs in web/src/thumbs.ts)
and sets manifest["thumbs"] = {base, packs, count, blobs, bytes, index}. `base` is resolved by the browser relative to data/ unless absolute
(set it to e.g. https://thumbs.example.com/v1/ when the packs are hosted elsewhere; that host must allow CORS + Range).

Index layout (little endian, then gzip):
  "THM1" u32 nWorlds u32 nBlobs u32 nPacks ; nPacks x (u32 firstBlob, u32 bytes)
  worldCode: nWorlds x u32 as 4 byte planes: 0 = next fresh blob, 1 = no thumbnail, k>=2 = existing blob (maxSeen - (k-2))
  blobLen:   nBlobs x u16 as 2 byte planes
Offsets inside a pack are the prefix sum of blobLen from the pack's first blob (blobs are stored back to back).
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("thumbs")
    ap.add_argument("order")
    ap.add_argument("out")
    ap.add_argument("--manifest", default=None)
    ap.add_argument("--pack-mib", type=float, default=24.0)
    ap.add_argument("--base", default="../thumbs/")
    ap.add_argument("--index-dir", default=None)
    a = ap.parse_args()
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
            if len(data) > 0xFFFF:
                raise SystemExit(f"thumbnail {s} is {len(data)} bytes; index stores u16 lengths")
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

    hdr = b"THM1" + struct.pack("<III", n, len(lens), len(packs)) + b"".join(struct.pack("<II", f, nb) for f, nb in packs)
    body = hdr + planes(code) + planes(np.array(lens, dtype=np.uint16))
    idx_dir.mkdir(parents=True, exist_ok=True)
    (idx_dir / "thumbs_index.bin").write_bytes(gzip.compress(body, 9, mtime=0))
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
    gz = (idx_dir / "thumbs_index.bin").stat().st_size
    print(json.dumps({**info, "avg_bytes": round(total / max(1, len(lens))), "index_gz_bytes": gz, "max_pack": max(nb for _, nb in packs) if packs else 0}))


if __name__ == "__main__":
    main()
