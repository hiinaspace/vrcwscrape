"""embed_delta: superset store, generation pointer, kill injection, model pins."""
import hashlib
import json
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from conftest import Killed

import embed_delta
import genstate

N_STATIC = 5


def fake_compute(ntexts, threads, batch):
    """deterministic stand-in for the two models: vector/bits are a function of the text"""
    emb = np.zeros((len(ntexts), 768), dtype=np.float32)
    nb = np.zeros((len(ntexts), 32), dtype=np.uint8)
    for k, t in enumerate(ntexts):
        h = np.frombuffer(hashlib.sha256(t.encode()).digest(), dtype=np.uint8)
        emb[k, :32] = h / 255.0
        nb[k] = h
    return emb, nb


class Env:
    def __init__(self, root: Path, monkeypatch):
        self.root = root
        self.state = root / "emb"
        self.state.mkdir(parents=True)
        self.embedded = 0
        monkeypatch.setattr(embed_delta, "compute", self.compute)

    def compute(self, ntexts, threads, batch):
        self.embedded += len(ntexts)
        return fake_compute(ntexts, threads, batch)

    def seed_static(self):
        """like seed_state.py emb: gen-0 with the constant model assets (here an empty store) + CURRENT"""

        def populate(tmp):
            pd.DataFrame({"world_id": pd.Series([], dtype=str), "text_sha": pd.Series([], dtype=str)}).to_parquet(tmp / "ids.parquet")
            np.save(tmp / "gemma2_full_f16.npy", np.zeros((0, 768), np.float16))
            np.save(tmp / "static_full_256_bin.npy", np.zeros((0, 32), np.uint8))
            (tmp / "static").mkdir()
            for k in range(N_STATIC):
                (tmp / "static" / f"static_tokens_int8.{k}.bin").write_bytes(bytes([k]) * 100)
            (tmp / "meta.json").write_text(json.dumps(embed_delta.model_meta()))

        genstate.publish(self.state, populate)

    def run(self, worlds: list[tuple[str, str]], tag="a", max_embed=8000, extra=()):
        """worlds: (world_id, name)"""
        p = self.root / f"{tag}.parquet"
        pd.DataFrame({"world_id": [w for w, _ in worlds], "name": [n for _, n in worlds], "description": ["d"] * len(worlds),
                      "author_name": ["a"] * len(worlds), "tags": [["x"]] * len(worlds)}).to_parquet(p)
        out = self.root / f"out-{tag}"
        old = sys.argv
        sys.argv = ["embed_delta", "--parquet", str(p), "--state", str(self.state), "--out", str(out), "--max-embed", str(max_embed), *extra]
        try:
            rc = embed_delta.main()
        finally:
            sys.argv = old
        return rc, out

    def snapshot(self) -> dict:
        g = genstate.resolve(self.state)
        ids = pd.read_parquet(g / "ids.parquet")
        return {"ids": ids.to_dict("list"), "vec": np.load(g / "gemma2_full_f16.npy").tobytes(), "bits": np.load(g / "static_full_256_bin.npy").tobytes(),
                "static": sorted((f.name, f.read_bytes()) for f in (g / "static").iterdir()), "meta": json.loads((g / "meta.json").read_text())}


def W(lo, hi, suffix=""):
    return [(f"wrld_{i:03d}", f"world {i}{suffix}") for i in range(lo, hi)]


@pytest.fixture
def env(tmp_path, monkeypatch):
    e = Env(tmp_path, monkeypatch)
    e.seed_static()
    return e


def test_superset_store_and_reuse(env):
    assert env.run(W(0, 30))[0] == 0
    assert env.embedded == 30
    # a bad extract week: 10 worlds missing. They are kept in the store ...
    env.embedded = 0
    rc, out = env.run(W(0, 20), tag="b")
    assert rc == 0 and env.embedded == 0
    assert len(pd.read_parquet(out / "ids.parquet")) == 20 and np.load(out / "gemma2_full_f16.npy").shape == (20, 768)
    assert len(env.snapshot()["ids"]["world_id"]) == 30
    # ... so when they are back nothing needs embedding, even with a zero cap
    env.embedded = 0
    rc, out = env.run(W(0, 30), tag="c", max_embed=0)
    assert rc == 0 and env.embedded == 0
    assert np.load(out / "gemma2_full_f16.npy").shape == (30, 768)


def test_changed_text_replaced_in_place_and_out_follows_parquet_order(env):
    env.run(W(0, 10))
    env.embedded = 0
    rc, out = env.run(list(reversed(W(0, 12, "")[:10])) + W(10, 12), tag="b")
    assert env.embedded == 2
    ids = pd.read_parquet(out / "ids.parquet")["world_id"].tolist()
    assert ids[:10] == [f"wrld_{i:03d}" for i in reversed(range(10))]
    v = np.load(out / "gemma2_full_f16.npy")
    want, _ = fake_compute([embed_delta.texts_of(pd.DataFrame({"name": ["world 9"], "tags": [["x"]], "author_name": ["a"], "description": ["d"]}))[0]], 1, 1)
    assert np.array_equal(v[0], want[0].astype(np.float16))
    env.embedded = 0
    env.run([(w, n + "!") if w == "wrld_003" else (w, n) for w, n in W(0, 12)], tag="c")
    assert env.embedded == 1
    assert len(env.snapshot()["ids"]["world_id"]) == 12  # replaced in place, not appended


def test_static_assets_travel_with_every_generation_and_out(env):
    _, out = env.run(W(0, 5))
    g = genstate.resolve(env.state)
    assert sorted(f.name for f in (g / "static").iterdir()) == [f"static_tokens_int8.{k}.bin" for k in range(N_STATIC)]
    assert (out / "static" / "static_full_256_bin.npy").is_file() and (out / "static" / "static_tokens_int8.0.bin").is_file()
    assert (out / "meta.json").is_file()


def test_cap_still_applies(env):
    env.run(W(0, 5))
    rc, _ = env.run(W(0, 40), tag="b", max_embed=10)
    assert rc == 3


@pytest.mark.parametrize("flat", [False, True])
def test_kill_every_step(tmp_path, monkeypatch, steps, flat):
    """kill at each durable step of a week with new + changed worlds; the state is always a valid generation, and the rerun ends identical
    to an uninterrupted run. flat=True starts from the pre-pointer layout (migration steps included)."""

    def mk(name):
        e = Env(tmp_path / name, monkeypatch)
        steps.off()
        e.seed_static()
        e.run(W(0, 20))
        if flat:  # old layout: files directly in emb/
            g = genstate.resolve(e.state)
            for f in g.iterdir():
                if f.name != "meta.json":
                    shutil.move(str(f), e.state / f.name)
            shutil.rmtree(e.state / "gen-0")
            (e.state / "CURRENT").unlink()
            # the old layout kept constant assets in static/ only
        return e

    week = W(0, 24) + []
    week[3] = ("wrld_003", "world 3 changed")
    probe = mk("probe")
    steps.record()
    probe.run(week, tag="w")
    seen = list(steps.calls)
    steps.off()
    want = probe.snapshot()
    assert {"write-ids", "write-vec", "write-bits", "link-static", "write-meta", "pointer-replace"} <= set(seen)
    if flat:
        assert "migrate-move" in seen
    for i in range(len(seen)):
        e = mk(f"k{i}")
        steps.kill_at(i)
        with pytest.raises(Killed):
            e.run(week, tag="w")
        steps.off()
        # the state left behind loads, and a worlds-sized rerun re-embeds at most this week's delta (4 new + 1 changed), never the whole store
        before = e.embedded
        rc, out = e.run(week, tag="r")
        assert rc == 0
        assert e.embedded - before <= 5,   f"kill at {seen[i]}: {e.embedded - before} worlds re-embedded"
        assert e.snapshot() == want, f"kill at step {i} ({seen[i]}): rerun differs from an uninterrupted run"


def test_model_pin_mismatch_is_refused(env, capsys):
    env.run(W(0, 5))
    g = genstate.resolve(env.state)
    meta = json.loads((g / "meta.json").read_text())
    meta["static"][1] = "0" * 40
    (g / "meta.json").write_text(json.dumps(meta))
    rc, _ = env.run(W(0, 5), tag="b")
    assert rc == 4 and "other coordinates" in capsys.readouterr().err
    # attended override starts an empty store
    rc, _ = env.run(W(0, 5), tag="c", extra=["--new-models"])
    assert rc == 0 and json.loads((genstate.resolve(env.state) / "meta.json").read_text()) == embed_delta.model_meta()


def test_pins_are_explicit():
    m = embed_delta.model_meta()
    assert len(m["gemma"][1]) == 40 and len(m["static"][1]) == 40


def test_offline_flag_only_when_both_revisions_cached(tmp_path, monkeypatch):
    hub = tmp_path / "hub"
    monkeypatch.setenv("HF_HUB_CACHE", str(hub))
    assert not embed_delta.hf_cached(embed_delta.STATIC, embed_delta.STATIC_REV)
    (hub / "models--sentence-transformers--static-similarity-mrl-multilingual-v1/snapshots" / embed_delta.STATIC_REV).mkdir(parents=True)
    assert embed_delta.hf_cached(embed_delta.STATIC, embed_delta.STATIC_REV)
    assert not embed_delta.hf_cached(*embed_delta.GEMMA[:2])
