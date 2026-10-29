"""pack_thumbs incremental state: generation pointer, kill injection at every durable step, validation, migration."""
import argparse
import json
import os
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import Killed

import genstate
import pack_thumbs
import thumbstate
from thumbstate import StateError, pack_name

LIMIT = 4000  # bytes per pack: a few packs from a handful of fake AVIFs


def avif(tag: str, n: int = 900) -> bytes:
    return (b"\0\0\0\x1cftypavif" + tag.encode() * n)[:n]


class Env:
    """A fake world: ground-truth bytes per sha, a state dir, and a way to run one 'week' of pack_thumbs.run_incremental."""

    def __init__(self, root: Path):
        self.root = root
        self.state = root / "state"
        self.truth: dict[str, bytes] = {}

    def week(self, worlds: list[tuple[str, str]], new: dict[str, bytes] | None = None, repack: bool = False, tag: str = "w", repack_frac: float = 0.10):
        """worlds: (world_id, thumb sha) in popularity order. new: shas to add (default: all shas the state does not know)."""
        for s, b in (new or {}).items():
            self.truth[s] = b
        r = self.root / tag
        shutil.rmtree(r, ignore_errors=True)
        r.mkdir(parents=True)
        pq.write_table(pa.table({"world_id": [w for w, _ in worlds], "thumb_sha256": [s for _, s in worlds]}), r / "worlds.parquet")
        if new:
            pq.write_table(pa.table({"sha256": list(new), "avif": list(new.values())}), r / "new.parquet")
        a = argparse.Namespace(state=str(self.state), worlds=str(r / "worlds.parquet"), new=str(r / "new.parquet") if new else None, repack=repack,
                               repack_frac=repack_frac, ws=4096, bs=2048)
        return pack_thumbs.run_incremental(a, [w for w, _ in worlds], r / "out", r / "idx", LIMIT)

    def snapshot(self) -> dict:
        """content of the current generation, independent of its number"""
        g = thumbstate.load(self.state)
        bt = pq.read_table(g.path / "blobs.parquet").to_pylist()
        return {"blobs": sorted((r["sha256"], r["blob"], r["len"]) for r in bt), "packs": g.packs,
                "bytes": [(g.path / pack_name(k)).read_bytes() for k in range(len(g.packs))]}

    def assert_maps_to_truth(self):
        """every sha in the state resolves (by pack/offset/len) to exactly its original bytes"""
        g = thumbstate.load(self.state)
        if g is None:  # a run killed before its first generation: no state, which the next run handles as a fresh start
            return
        lens = {b: ln for b, ln in g.known.values()}
        for sha, (b, ln) in g.known.items():
            k = max(i for i, p in enumerate(g.packs) if p["first"] <= b)
            off = sum(lens[x] for x in range(g.packs[k]["first"], b))
            data = (g.path / pack_name(k)).read_bytes()[off : off + ln]
            assert data == self.truth[sha], f"{sha} maps to the wrong bytes"


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def worlds_of(shas: list[str]) -> list[tuple[str, str]]:
    return [(f"wrld_{i:03d}", s) for i, s in enumerate(shas)]


def gen_dirs(state: Path) -> list[str]:
    return sorted(d.name for d in state.iterdir() if d.is_dir())


def test_generations_share_packs_and_gc(env):
    shas = [f"{i:02x}" * 32 for i in range(12)]
    env.week(worlds_of(shas[:8]), {s: avif(s) for s in shas[:8]})
    assert (env.state / "CURRENT").read_text() == "gen-0\n"
    env.week(worlds_of(shas[:10]), {s: avif(s) for s in shas[8:10]}, repack_frac=0.99)
    assert (env.state / "CURRENT").read_text() == "gen-1\n"
    g0, g1 = env.state / "gen-0", env.state / "gen-1"
    assert os.stat(g0 / "p-000.bin").st_ino == os.stat(g1 / "p-000.bin").st_ino  # unchanged packs are hard links, not copies
    env.week(worlds_of(shas[:12]), {s: avif(s) for s in shas[10:12]}, repack_frac=0.99)
    assert gen_dirs(env.state) == ["gen-1", "gen-2"]  # CURRENT and the one before it
    env.assert_maps_to_truth()
    # nothing new -> no new generation
    env.week(worlds_of(shas[:12]), None, repack_frac=0.99)
    assert (env.state / "CURRENT").read_text() == "gen-2\n"


def reference(tmp_path, setup, run):
    """state after setup + an uninterrupted run, as comparable content"""
    e = Env(tmp_path / "ref")
    setup(e)
    run(e)
    return e.snapshot()


def kill_matrix(tmp_path, steps, setup, run, rerun=None):
    """For every durable step of run(): copy the post-setup state, kill there, check the state is loadable and truthful, rerun cleanly and
    compare with the uninterrupted result. Returns the step names seen."""
    steps.record()
    e0 = Env(tmp_path / "probe")
    setup(e0)
    steps.record()
    run(e0)
    seen = list(steps.calls)
    want = e0.snapshot()
    assert seen, "no hook points: the test would prove nothing"
    for i in range(len(seen)):
        steps.off()
        e = Env(tmp_path / f"k{i}")
        setup(e)
        e.truth = dict(e0.truth)
        steps.kill_at(i)
        with pytest.raises(Killed):
            run(e)
        steps.off()
        thumbstate.load(e.state)  # the state a killed run leaves behind is valid (old or new generation) ...
        e.assert_maps_to_truth()  # ... and truthful
        (rerun or run)(e)
        assert e.snapshot() == want, f"after a kill at step {i} ({seen[i]}) the rerun differs from an uninterrupted run"
        e.assert_maps_to_truth()
    return seen


SHAS = [f"{i:02x}" * 32 for i in range(14)]


def setup_base(e: Env):  # two generations, so the run under test has something to garbage-collect
    e.week(worlds_of(SHAS[:6]), {s: avif(s) for s in SHAS[:6]})
    e.week(worlds_of(SHAS[:8]), {s: avif(s) for s in SHAS[6:8]}, repack_frac=0.99)


def test_kill_every_step_append(tmp_path, steps):
    def run(e):
        e.week(worlds_of(SHAS[:11]), {s: avif(s) for s in SHAS[8:11]}, repack_frac=0.99)

    seen = kill_matrix(tmp_path, steps, setup_base, run)
    assert {"pack-write", "blobs-write", "packs-json-write", "gen-rename", "pointer-replace", "gc"} <= set(seen)


def test_kill_every_step_repack(tmp_path, steps):
    def run(e):  # popularity changed: reversed order + a forced repack
        e.week(list(reversed(worlds_of(SHAS[:11]))), {s: avif(s) for s in SHAS[8:11]}, repack=True)

    seen = kill_matrix(tmp_path, steps, setup_base, run)
    assert "pack-write" in seen and "pointer-replace" in seen


def test_swap_scenario_cannot_map_to_wrong_sha(tmp_path, steps):
    """Codex #1: two equal-length thumbnails A, B are reordered by a repack. The old code could leave new pack bytes with the old
    sha->offset table; every kill point here must leave a state in which each sha still maps to its own bytes, also after the next repack."""
    a, b = "aa" * 32, "bb" * 32
    A, B = avif("A", 700), avif("B", 700)
    assert len(A) == len(B)

    def setup(e):
        e.week(worlds_of([a, b]), {a: A, b: B})

    def repack_swapped(e):
        e.week(worlds_of([b, a]), None, repack=True)

    def repack_again(e):
        e.week(worlds_of([b, a]), None, repack=True)

    steps.record()
    e0 = Env(tmp_path / "p")
    setup(e0)
    steps.record()
    repack_swapped(e0)
    n = len(steps.calls)
    for i in range(n):
        steps.off()
        e = Env(tmp_path / f"s{i}")
        setup(e)
        steps.kill_at(i)
        with pytest.raises(Killed):
            repack_swapped(e)
        steps.off()
        e.truth = {a: A, b: B}
        e.assert_maps_to_truth()  # right after the kill
        repack_again(e)  # next week's (forced) repack reads the surviving state
        e.assert_maps_to_truth()
        g = thumbstate.load(e.state)
        assert g.known[a][0] != g.known[b][0]


def test_corrupt_state_fails_loudly(env):
    shas = SHAS[:8]
    env.week(worlds_of(shas), {s: avif(s) for s in shas})
    p = env.state / "gen-0" / "p-000.bin"
    good = p.read_bytes()
    p.write_bytes(good[:-1] + bytes([good[-1] ^ 1]))  # same size, different byte: only the hash catches it
    with pytest.raises(StateError, match="sha256"):
        thumbstate.load(env.state)
    with pytest.raises(StateError):
        env.week(worlds_of(shas), None)
    p.write_bytes(good[:-5])  # truncated: a blob lies beyond the pack
    with pytest.raises(StateError, match="blobs span"):
        thumbstate.load(env.state, verify_hashes=False)
    p.write_bytes(good)
    thumbstate.load(env.state)
    # a blob table that has rows no pack covers (the old two-file hole)
    bt = pq.read_table(env.state / "gen-0" / "blobs.parquet").to_pylist()
    bt.append({"sha256": "ff" * 32, "blob": len(bt), "len": 10})
    pq.write_table(pa.Table.from_pylist(bt), env.state / "gen-0" / "blobs.parquet")
    with pytest.raises(StateError):
        thumbstate.load(env.state)
    # a pointer to nothing
    (env.state / "CURRENT").write_text("gen-9\n")
    with pytest.raises(StateError, match="not a generation"):
        thumbstate.load(env.state)


def flatten(state: Path) -> None:
    """turn the current generation back into the pre-pointer layout (packs/p-*.bin, blobs.parquet, packs.json)"""
    g = genstate.resolve(state)
    (state / "packs").mkdir()
    for f in g.iterdir():
        os.rename(f, (state / "packs" / f.name) if f.name.endswith(".bin") else (state / f.name))
    g.rmdir()
    for d in state.glob("gen-*"):  # the old layout never had generation dirs
        shutil.rmtree(d)
    (state / "CURRENT").unlink()


def test_migrate_flat_layout_resumable(tmp_path, steps):
    def setup(e):
        setup_base(e)
        flatten(e.state)

    def run(e):  # first run after the deploy: adopt, then an ordinary append week
        e.week(worlds_of(SHAS[:10]), {s: avif(s) for s in SHAS[8:10]}, repack_frac=0.99)

    seen = kill_matrix(tmp_path, steps, setup, run)
    assert seen.count("migrate-move") >= 5
    # adoption alone copies nothing: same inodes as the flat files had
    e = Env(tmp_path / "inode")
    setup_base(e)
    flatten(e.state)
    ino = os.stat(e.state / "packs" / "p-000.bin").st_ino
    thumbstate.load(e.state)
    assert os.stat(e.state / "gen-0" / "p-000.bin").st_ino == ino and not (e.state / "packs").exists()


def test_fresh_start_kill_between_rename_and_pointer(tmp_path, steps):
    def run(e):
        e.week(worlds_of(SHAS[:6]), {s: avif(s) for s in SHAS[:6]})

    seen = kill_matrix(tmp_path, steps, lambda e: None, run)
    assert "pointer-tmp" in seen


def test_repack_reads_current_generation_and_keeps_old_until_pointer_moves(env, steps):
    env.week(worlds_of(SHAS[:8]), {s: avif(s) for s in SHAS[:8]})
    before = env.snapshot()
    steps.kill_at(0)  # first durable step of the repack
    with pytest.raises(Killed):
        env.week(list(reversed(worlds_of(SHAS[:8]))), None, repack=True)
    steps.off()
    assert env.snapshot() == before and (env.state / "gen-0").is_dir()


def test_rerun_into_the_same_out_dir_never_writes_through_a_hard_link(env):
    """found by the real-data smoke: out/p-NNN.bin is a hard link to a state pack; a second build into the same out dir used to
    copyfile() over it and rewrote the (immutable, shared) state pack in place"""
    shas = SHAS[:8]
    env.week(worlds_of(shas), {s: avif(s) for s in shas})
    old = {f.name: f.read_bytes() for f in (env.state / "gen-0").glob("p-*.bin")}
    out = env.root / "shared-out"
    out.mkdir()
    for name in old:
        os.link(env.state / "gen-0" / name, out / name)  # what the previous build left behind
    a = argparse.Namespace(state=str(env.state), worlds=str(env.root / "w" / "worlds.parquet"), new=None, repack=True, repack_frac=0.1, ws=4096, bs=2048)
    pack_thumbs.run_incremental(a, [w for w, _ in reversed(worlds_of(shas))], out, env.root / "idx", LIMIT)  # reversed popularity: the repack changes every pack
    assert {f.name: f.read_bytes() for f in (env.state / "gen-0").glob("p-*.bin")} == old  # untouched, although the repack re-created the same names
    cur = genstate.resolve(env.state)
    assert cur.name == "gen-1" and (out / "p-000.bin").read_bytes() == (cur / "p-000.bin").read_bytes()
    env.assert_maps_to_truth()
