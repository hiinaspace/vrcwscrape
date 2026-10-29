"""Thumbnail pack state as immutable generations (see genstate.py): STATE/CURRENT -> gen-<N>/{blobs.parquet, packs.json, p-NNN.bin ...}.

Packs are immutable files. A new generation hard-links the packs it shares with the previous one (append mode: all of them) instead of copying
(NFS: copying 700 MB per week is not OK; both generations live in one dataset), and writes only the new packs. A repack writes a fresh set.
blobs.parquet (sha256, blob, len) and packs.json ({"packs": [{first, bytes, hash, kind}]}) are written in the same generation, so the blob table
and the pack bytes it indexes can never be paired wrongly: the pair only becomes visible when CURRENT moves.

Every reader goes through load(), which validates the generation and raises StateError (never repairs): blob ids dense 0..N-1, every pack's
blobs sum to its recorded byte size, the file has exactly that size, and (verify_hashes) its sha256[:16] equals the recorded hash.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import genstate
from genstate import StateError

__all__ = ["StateError", "Gen", "load", "pack_name", "sha16", "write_generation"]


def pack_name(k: int) -> str:
    return f"p-{k:03d}.bin"


def sha16(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


@dataclass
class Gen:
    path: Path
    packs: list[dict]
    known: dict[str, tuple[int, int]]  # sha -> (blob, len)


def _flat_moves(state: Path) -> list[tuple[str, str]]:
    moves = [("blobs.parquet", "blobs.parquet"), ("packs.json", "packs.json")]
    sp = state / "packs"
    if sp.is_dir():
        moves += [(f"packs/{f.name}", f.name) for f in sorted(sp.glob("p-*.bin"))]
    return moves


def resolve(state: Path) -> Path | None:
    """Current generation dir, adopting the old flat layout (packs/p-*.bin, blobs.parquet, packs.json) as gen-0 on first sight."""
    state = Path(state)
    if state.is_dir():
        genstate.migrate(state, _flat_moves(state), extra_dirs=("packs",))
    return genstate.resolve(state)


def load(state: Path, verify_hashes: bool = True) -> Gen | None:
    g = resolve(state)
    if g is None:
        return None
    try:
        packs = json.loads((g / "packs.json").read_text())["packs"]
        bt = pq.read_table(g / "blobs.parquet")
    except (OSError, ValueError, KeyError) as e:
        raise StateError(f"{g}: unreadable generation: {e!r}") from e
    sha, blob, ln = bt["sha256"].to_pylist(), bt["blob"].to_pylist(), bt["len"].to_pylist()
    known = {s: (b, n) for s, b, n in zip(sha, blob, ln)}
    if len(known) != len(sha) or sorted(b for b, _ in known.values()) != list(range(len(known))):
        raise StateError(f"{g}: blob table is not a dense 0..N-1 sha->blob map")
    lens = np.zeros(len(known), dtype=np.int64)
    for b, n in known.values():
        lens[b] = n
    firsts = [p["first"] for p in packs]
    if packs and (firsts[0] != 0 or firsts != sorted(set(firsts))):
        raise StateError(f"{g}: pack first-blob ids are not increasing from 0: {firsts[:5]}")
    for k, p in enumerate(packs):
        end = packs[k + 1]["first"] if k + 1 < len(packs) else len(lens)
        if not p["first"] < end:
            raise StateError(f"{g}: pack {k} covers no blobs")
        f = g / pack_name(k)
        if not f.is_file():
            raise StateError(f"{f} missing")
        want = int(lens[p["first"] : end].sum())
        have = f.stat().st_size
        if not (want == have == p["bytes"]):
            raise StateError(f"{f}: blobs span {want} bytes, file has {have}, packs.json says {p['bytes']}")
        if verify_hashes and sha16(f) != p["hash"]:
            raise StateError(f"{f}: sha256 does not match packs.json ({p['hash']})")
    if known and not packs:
        raise StateError(f"{g}: blob table without packs")
    return Gen(g, packs, known)


def write_generation(state: Path, fill) -> tuple[Path, list[dict], list[tuple[str, int, int]]]:
    """Publish a generation. `fill(tmp)` writes/links the pack files into the new generation dir and returns (packs, rows[sha, blob, len]);
    this adds blobs.parquet + packs.json. Returns (generation dir, packs, rows sorted by blob id)."""
    out: dict = {}

    def populate(tmp: Path) -> None:
        packs, rows = fill(tmp)
        rows = sorted(rows, key=lambda r: r[1])
        out["packs"], out["rows"] = packs, rows
        genstate.hook("blobs-write")
        bt = pa.table({"sha256": [r[0] for r in rows], "blob": pa.array([r[1] for r in rows], pa.int64()), "len": pa.array([r[2] for r in rows], pa.int64())})
        pq.write_table(bt, tmp / "blobs.parquet")
        genstate.hook("packs-json-write")
        genstate.write_durable(tmp / "packs.json", json.dumps({"packs": packs}).encode())

    gen = genstate.publish(Path(state), populate)
    return gen, out["packs"], out["rows"]
