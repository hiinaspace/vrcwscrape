"""history_extract: asof anchored to the Dolt commit time; history-base generations + kill injection."""
import json
import shutil
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import Killed

import genstate
import history_extract as H


def js(name, created: list[str]):
    return json.dumps({"name": name, "versions": [{"version": i + 1, "created_at": c, "file": {"sizeInBytes": 10}, "deleted": False} for i, c in enumerate(created)]})


class Src:
    def __init__(self, files: dict, metrics: list):
        self.files, self.m = files, metrics

    def file_list(self):
        return [(f, w, t) for f, (w, t, _) in self.files.items()]

    def pages(self, batch=1000):
        for f, (w, _, j) in sorted(self.files.items()):
            yield f, w, j

    def by_ids(self, ids, batch=200):
        for f in ids:
            w, _, j = self.files[f]
            yield f, w, j

    def metrics(self, since=None):
        yield from (r for r in self.m if since is None or r[1] > since)


NAME = "World - 2022.3.6f1_1_standalonewindows_x"
T = datetime
BASE_FILES = {
    "file_a": ("wrld_a", T(2026, 10, 1), js(NAME, ["2026-07-28T23:57:00Z", "2026-05-01T00:00:00Z"])),
    "file_b": ("wrld_b", T(2026, 10, 1), js(NAME, ["2026-10-20T10:00:00Z"])),
}
METRICS = [("wrld_a", T(2026, 10, 26, 23, 55), 5, 1, 0), ("wrld_b", T(2026, 10, 20), 1, 1, 0)]
HEAD_T = T(2026, 10, 27, 0, 22, 9)


@pytest.fixture
def pub(tmp_path):
    p = tmp_path / "public.parquet"
    pq.write_table(pa.table({"world_id": ["wrld_a", "wrld_b"]}), p)
    return p


def summary(out):
    return {r["world_id"]: r for r in pq.read_table(Path(out) / "summary.parquet").to_pylist()}


def test_asof_is_the_commit_time_and_metrics_max_is_kept_apart(tmp_path, pub):
    H.run(Src(BASE_FILES, METRICS), str(pub), str(tmp_path / "o1"), head=("abc", HEAD_T))
    a = json.loads((tmp_path / "o1/asof.json").read_text())
    assert a["asof"] == "2026-10-27 00:22:09" and a["metrics_max"] == "2026-10-26 23:55:00" and a["asof_source"] == "dolt-commit"
    # the upload at 07-28 23:57 is 90d + 22 min before the commit: outside uploads_90d, although it is inside a window anchored at the last metric
    assert summary(tmp_path / "o1")["wrld_a"]["uploads_90d"] == 0
    H.run(Src(BASE_FILES, METRICS), str(pub), str(tmp_path / "o2"), head=("abc", None))  # commit time unknown: fall back to the newest metric
    a2 = json.loads((tmp_path / "o2/asof.json").read_text())
    assert a2["asof"] == a2["metrics_max"] == "2026-10-26 23:55:00" and a2["asof_source"] == "metrics-max"
    assert summary(tmp_path / "o2")["wrld_a"]["uploads_90d"] == 1
    assert str(a["asof"])[:10] == "2026-10-27"  # what build_index puts in manifest.history.asof == the snapshot day


def test_asof_accepts_iso_strings_and_aware_times():
    assert H.as_naive_utc("2026-10-27T00:22:09Z") == T(2026, 10, 27, 0, 22, 9)
    assert H.as_naive_utc(None) is None


def week2_files():
    f = dict(BASE_FILES)
    f["file_b"] = ("wrld_b", T(2026, 10, 27), js(NAME, ["2026-10-20T10:00:00Z", "2026-10-26T10:00:00Z"]))  # changed
    f["file_c"] = ("wrld_a", T(2026, 10, 27), js(NAME, ["2026-10-25T10:00:00Z"]))  # new
    return f


def base_content(base: Path):
    g = genstate.resolve(base)
    return {f: pq.read_table(g / f).to_pylist() for f in H.BASE_FILES}


def setup(root: Path, pub: Path, flat=False):
    base = root / "history-base"
    H.run(Src(BASE_FILES, METRICS), str(pub), str(root / "o0"), base=str(base), head=("abc", HEAD_T))
    if flat:
        g = genstate.resolve(base)
        for f in g.iterdir():
            shutil.move(str(f), base / f.name)
        shutil.rmtree(g)
        (base / "CURRENT").unlink()
    return base


@pytest.mark.parametrize("flat", [False, True])
def test_kill_every_step_of_the_base_swap(tmp_path, pub, steps, flat):
    m2 = METRICS + [("wrld_b", T(2026, 10, 27, 0, 10), 9, 2, 1)]
    head2 = ("def", T(2026, 10, 28))

    def run(base, out):
        H.run(Src(week2_files(), m2), str(pub), str(out), base=str(base), head=head2)

    probe = setup(tmp_path / "probe", pub, flat)
    steps.record()
    run(probe, tmp_path / "probe/o")
    seen = list(steps.calls)
    steps.off()
    want, want_sum = base_content(probe), summary(tmp_path / "probe/o")
    assert {"base-files", "base-rows", "base-metrics", "pointer-replace"} <= set(seen)
    for i in range(len(seen)):
        r = tmp_path / f"k{i}"
        base = setup(r, pub, flat)
        steps.kill_at(i)
        with pytest.raises(Killed):
            run(base, r / "o")
        steps.off()
        run(base, r / "o2")  # next week: finds a valid base (old or new generation), never silently falls back or crashes
        assert json.loads((r / "o2/asof.json").read_text())["mode"] == "incremental", f"kill at {seen[i]}: lost the base"
        assert base_content(base) == want and summary(r / "o2") == want_sum, f"kill at {seen[i]}"


def test_incomplete_base_fails_loudly(tmp_path, pub):
    base = setup(tmp_path, pub)
    (genstate.resolve(base) / "rows.parquet").unlink()
    with pytest.raises(genstate.StateError, match="incomplete"):
        H.run(Src(BASE_FILES, METRICS), str(pub), str(tmp_path / "o"), base=str(base), head=("abc", HEAD_T))
