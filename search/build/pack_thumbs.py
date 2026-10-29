"""Pack self-hosted AVIF thumbnails into <=25 MiB range-addressable files + a small lazy index.

Two modes.

Legacy / full (positional thumbs.parquet):
  pack_thumbs.py <thumbs.parquet> <world_order.parquet> <out_dir> [--manifest data/manifest.json] [--pack-mib 24] [--base ../thumbs/]
                 [--index-dir DIR]
  <thumbs.parquet>      world_id, sha256, avif (binary), width, height -- one row per world with an image (several worlds may share a sha)
  Blob ids are assigned in order of first use by world ordinal (= popularity), packs are written fresh.

Incremental (weekly refresh; `-` as the thumbs argument):
  pack_thumbs.py - <world_order.parquet> <out_dir> --state DIR --worlds <public.parquet> [--new new.parquet] [--repack] [--manifest ..]
  --worlds  parquet with world_id and thumb_sha256 (the build's world list)
  --new     sha256, avif, width, height for shas not yet in the state (thumbs_delta.py)
  --state   DIR/CURRENT -> DIR/gen-<N>/{p-NNN.bin (frozen), blobs.parquet (sha256, blob, len), packs.json} (build/thumbstate.py, genstate.py).
            Blob ids are frozen per sha and existing pack files are never rewritten: a new generation hard-links the old packs and adds NEW
            packs after the last one. When appended packs exceed --repack-frac (10%) of all pack bytes, or unreferenced blobs do, everything
            is repacked in popularity order (fresh ids) into a fresh generation. The only commit point is the atomic replace of CURRENT; a
            killed run leaves the previous generation current (the next run discards the half-written one). The state is validated on every
            load (pack sizes, pack hashes, blob table) and a mismatch fails the run; nothing is repaired in place.
  The state packs are the only store of AVIF bytes; a repack reads the surviving blobs back out of them.

Output (both modes)
  <out_dir>/p-000.bin, p-001.bin ...   concatenated AVIFs (incremental: copies of the state packs)
  <index-dir>/thumbs_index.bin          gz; header + pack table (see Thumbs in web/src/thumbs.ts)
  <index-dir>/th/w<k>.bin, th/b<k>.bin  world shards / blob shards
and sets manifest["thumbs"] = {base, packs, count, blobs, bytes, index, hashes}. `hashes[i]` is the first 16 hex chars of the sha256 of
pack i; the browser requests `p-NNN.bin?v=<hash>`, so an unchanged pack keeps its URL (and its browser cache) across builds.

Index (all little endian; every file gzip'd and magic-sniffed like the other payloads):
  thumbs_index.bin  "THM3" u32 nWorlds u32 nBlobs u32 nPacks u32 WS u32 BS ; nPacks x (u32 firstBlob, u32 bytes);
                    ceil(nBlobs/BS) x u32 offsetOfFirstBlobInItsPack
  th/w<k>.bin       world shard k (WS worlds): u32 codes as 4 byte planes. 0 = no thumbnail; c >= 1: blob = prev + unzigzag(c - 1), where
                    prev is the blob of the previous world WITH a thumbnail in this shard (0 at the shard start)
  th/b<k>.bin       blob shard k (BS blobs): u32 lengths as 4 byte planes (rare AVIFs exceed 64 KiB)
Blobs are stored back to back inside a pack in blob-id order; a blob's offset is the prefix sum of lengths from the pack's first blob
(reset to 0 at each pack boundary).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import struct
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

import genstate
import thumbstate
from thumbstate import pack_name, sha16


def planes(a: np.ndarray) -> bytes:
    a = np.ascontiguousarray(a.astype("<" + a.dtype.str[1:]))
    return a.view(np.uint8).reshape(len(a), a.dtype.itemsize).T.tobytes()


WS = 4096  # worlds per world shard (default; --ws)
BS = 2048  # blobs per blob shard (default; --bs)


class PackWriter:
    """Appends blobs to consecutive packs p-<first_no>.bin ...; blob ids continue from `first_blob`."""

    def __init__(self, out: Path, first_no: int, first_blob: int, limit: int):
        self.out, self.no, self.limit = out, first_no, limit
        self.blob = first_blob
        self.cur = bytearray()
        self.cur_first = first_blob
        self.packs: list[dict] = []
        self.rows: list[tuple[str, int, int]] = []  # sha, blob, len
        out.mkdir(parents=True, exist_ok=True)

    def flush(self) -> None:
        if not self.cur:
            return
        p = self.out / pack_name(self.no)
        genstate.hook("pack-write")
        genstate.write_durable(p, bytes(self.cur))
        self.packs.append({"first": self.cur_first, "bytes": len(self.cur), "hash": sha16(p)})
        self.no += 1
        self.cur = bytearray()
        self.cur_first = self.blob

    def add(self, sha: str, data: bytes) -> int:
        if self.cur and len(self.cur) + len(data) > self.limit:
            self.flush()
        b = self.blob
        self.blob += 1
        self.cur += data
        self.rows.append((sha, b, len(data)))
        return b


def write_index(idx_dir: Path, code_blob: np.ndarray, lens: list[int], packs: list[dict], ws: int, bs: int) -> int:
    """code_blob: int64 per world ordinal, -1 = none. Returns total index bytes."""
    n, nb = len(code_blob), len(lens)
    pack_first = [p["first"] for p in packs]
    off = np.zeros(nb, dtype=np.int64)
    for k, p in enumerate(packs):
        end = packs[k + 1]["first"] if k + 1 < len(packs) else nb
        if end - p["first"] > 1:
            off[p["first"] : end] = np.concatenate([[0], np.cumsum(lens[p["first"] : end - 1])])
    nws, nbs = -(-n // ws), -(-nb // bs)
    hdr = b"THM3" + struct.pack("<IIIII", n, nb, len(packs), ws, bs) + b"".join(struct.pack("<II", f, p["bytes"]) for f, p in zip(pack_first, packs))
    hdr += np.array([off[k * bs] for k in range(nbs)], dtype="<u4").tobytes()
    idx_dir.mkdir(parents=True, exist_ok=True)
    th = idx_dir / "th"
    if th.exists():
        shutil.rmtree(th)
    th.mkdir()
    (idx_dir / "thumbs_index.bin").write_bytes(gzip.compress(hdr, 9, mtime=0))
    total = (idx_dir / "thumbs_index.bin").stat().st_size
    for k in range(nws):
        seg = code_blob[k * ws : (k + 1) * ws]
        codes = np.zeros(len(seg), dtype=np.uint32)
        prev = 0
        for i, b in enumerate(seg.tolist()):
            if b < 0:
                continue
            d = b - prev
            codes[i] = ((d << 1) ^ (d >> 63)) + 1
            prev = b
        data = gzip.compress(planes(codes), 9, mtime=0)
        (th / f"w{k}.bin").write_bytes(data)
        total += len(data)
    for k in range(nbs):
        data = gzip.compress(planes(np.array(lens[k * bs : (k + 1) * bs], dtype=np.uint32)), 9, mtime=0)
        (th / f"b{k}.bin").write_bytes(data)
        total += len(data)
    return total


def first_use_order(sha_of: dict[int, str]):
    seen = set()
    for i in sorted(sha_of):
        s = sha_of[i]
        if s not in seen:
            seen.add(s)
            yield s


def run_incremental(a, order: list[str], out: Path, idx_dir: Path, limit: int) -> dict:
    state = Path(a.state)
    state.mkdir(parents=True, exist_ok=True)
    ord_of = {w: i for i, w in enumerate(order)}
    wt = pq.read_table(a.worlds, columns=["world_id", "thumb_sha256"])
    sha_of: dict[int, str] = {}
    for w, s in zip(wt["world_id"].to_pylist(), wt["thumb_sha256"].to_pylist()):
        i = ord_of.get(w)
        if i is not None and s:
            sha_of[i] = s
    new_avif: dict[str, bytes] = {}
    if a.new and Path(a.new).is_file():
        t = pq.read_table(a.new, columns=["sha256", "avif"])
        new_avif = dict(zip(t["sha256"].to_pylist(), t["avif"].to_pylist()))

    st = thumbstate.load(state)  # validates (sizes + pack hashes); raises StateError on any inconsistency
    known: dict[str, tuple[int, int]] = st.known if st else {}
    packs: list[dict] = st.packs if st else []
    used = set(sha_of.values())
    add = [s for s in first_use_order(sha_of) if s not in known and s in new_avif]
    add_bytes = sum(len(new_avif[s]) for s in add)
    total_bytes = sum(p["bytes"] for p in packs) + add_bytes
    app_bytes = sum(p["bytes"] for p in packs if p.get("kind") == "append") + add_bytes
    orphan = sum(ln for s, (b, ln) in known.items() if s not in used)
    why = None
    if not packs:
        why = "no state"
    elif a.repack:
        why = "--repack"
    elif total_bytes and app_bytes / total_bytes > a.repack_frac:
        why = f"appended packs are {app_bytes / total_bytes:.1%} of bytes"
    elif total_bytes and orphan / total_bytes > a.repack_frac:
        why = f"unreferenced blobs are {orphan / total_bytes:.1%} of bytes"
    info = {"mode": "repack" if why else "append", "why": why, "new_blobs": len(add), "new_bytes": add_bytes,
            "appended_bytes": app_bytes, "orphan_bytes": orphan, "missing": len(used - set(known) - set(new_avif))}
    if why:
        # fresh ids in popularity order; surviving blobs are read back out of the current generation's (validated) packs
        old_off: dict[int, tuple[int, int]] = {}  # blob -> (pack index, offset)
        lens = {b: ln for b, ln in known.values()}
        for k, p in enumerate(packs):
            end = packs[k + 1]["first"] if k + 1 < len(packs) else max(lens, default=-1) + 1
            o = 0
            for b in range(p["first"], end):
                old_off[b] = (k, o)
                o += lens.get(b, 0)

        def fill(tmp: Path):
            wr = PackWriter(tmp, 0, 0, limit)
            handles: dict[int, object] = {}
            for s in first_use_order(sha_of):
                if s in new_avif:
                    data = new_avif[s]
                elif s in known:
                    b, ln = known[s]
                    k, o = old_off[b]
                    f = handles.get(k) or handles.setdefault(k, open(st.path / pack_name(k), "rb"))
                    f.seek(o)
                    data = f.read(ln)
                    assert len(data) == ln
                else:
                    continue
                wr.add(s, data)
            wr.flush()
            for f in handles.values():
                f.close()
            for p in wr.packs:
                p["kind"] = "base"
            return wr.packs, wr.rows

        gen, packs, rows = thumbstate.write_generation(state, fill)  # the old generation stays untouched until CURRENT moves
    elif add:
        def fill(tmp: Path):
            for k in range(len(packs)):
                genstate.link_or_copy(st.path / pack_name(k), tmp / pack_name(k))
            # ids continue after the highest id ever assigned (unreferenced blobs keep their ids until the next repack)
            wr = PackWriter(tmp, len(packs), max(b for b, _ in known.values()) + 1, limit)
            for s in add:
                wr.add(s, new_avif[s])
            wr.flush()
            for p in wr.packs:
                p["kind"] = "append"
            return packs + wr.packs, [(s, b, ln) for s, (b, ln) in known.items()] + wr.rows

        gen, packs, rows = thumbstate.write_generation(state, fill)
    else:
        gen, rows = st.path, sorted(((s, b, ln) for s, (b, ln) in known.items()), key=lambda r: r[1])

    blob_of = {s: b for s, b, _ in rows}
    nblobs = (rows[-1][1] + 1) if rows else 0
    lens_by_blob = [0] * nblobs
    for _, b, ln in rows:
        lens_by_blob[b] = ln
    n = len(order)
    code = np.full(n, -1, dtype=np.int64)
    for i, s in sha_of.items():
        b = blob_of.get(s)
        if b is not None:
            code[i] = b
    # copy packs into the build output
    out.mkdir(parents=True, exist_ok=True)
    for k in range(len(packs)):
        dst = out / f"p-{k:03d}.bin"
        try:
            os.link(gen / dst.name, dst)
        except OSError:
            shutil.copyfile(gen / dst.name, dst)
    idx_bytes = write_index(idx_dir, code, lens_by_blob, packs, a.ws, a.bs)
    info.update({"packs": len(packs), "count": int((code >= 0).sum()), "blobs": nblobs, "bytes": sum(p["bytes"] for p in packs),
                 "hashes": [p["hash"] for p in packs], "index_gz_bytes_total": idx_bytes})
    return info


def run_legacy(a, order: list[str], out: Path, idx_dir: Path, limit: int) -> dict:
    ord_of = {w: i for i, w in enumerate(order)}
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
    wr = PackWriter(out, 0, 0, limit)
    blob_of: dict[str, int] = {}
    for s in first_use_order(sha_of):
        blob_of[s] = wr.add(s, avif[s])
    wr.flush()
    code = np.full(len(order), -1, dtype=np.int64)
    for i, s in sha_of.items():
        code[i] = blob_of[s]
    lens = [0] * wr.blob
    for _, b, ln in wr.rows:
        lens[b] = ln
    idx_bytes = write_index(idx_dir, code, lens, wr.packs, a.ws, a.bs)
    return {"packs": len(wr.packs), "count": int((code >= 0).sum()), "blobs": len(lens), "bytes": sum(p["bytes"] for p in wr.packs),
            "hashes": [p["hash"] for p in wr.packs], "index_gz_bytes_total": idx_bytes}


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
    ap.add_argument("--state", default=None, help="incremental mode: state dir")
    ap.add_argument("--worlds", default=None, help="incremental mode: parquet with world_id, thumb_sha256")
    ap.add_argument("--new", default=None, help="incremental mode: parquet of newly encoded shas")
    ap.add_argument("--repack", action="store_true")
    ap.add_argument("--repack-frac", type=float, default=0.10)
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    idx_dir = Path(a.index_dir) if a.index_dir else out
    limit = int(a.pack_mib * 1048576)
    order = pq.read_table(a.order, columns=["world_id"])["world_id"].to_pylist()
    if a.state:
        assert a.worlds, "--state needs --worlds"
        res = run_incremental(a, order, out, idx_dir, limit)
    else:
        res = run_legacy(a, order, out, idx_dir, limit)
    info = {"base": a.base, "packs": res["packs"], "count": res["count"], "blobs": res["blobs"], "bytes": res["bytes"],
            "index": "thumbs_index.bin", "hashes": res["hashes"]}
    if a.manifest:
        mp = Path(a.manifest)
        m = json.loads(mp.read_text())
        m["thumbs"] = info
        m["hasThumb"] = True
        mp.write_text(json.dumps(m, ensure_ascii=False, separators=(",", ":")))
    print(json.dumps({**{k: v for k, v in res.items() if k != "hashes"}, "avg_bytes": round(res["bytes"] / max(1, res["blobs"]))}))


if __name__ == "__main__":
    main()
