"""Deploy baseline: pending-deploy intent record, reconcile against the live version, promote atomicity, preflight, tolerance."""
import json
import shutil
from datetime import datetime, timedelta, timezone

import pytest
from conftest import Killed

import baseline
import genstate

A, B = 1111, 2222  # manifest versions: A is deployed.json's, B is the candidate
LAUNCH = baseline.LAUNCH_VERSION


def mk_state(tmp_path, deployed=A, ids=b"ids-A"):
    s = tmp_path / "state"
    s.mkdir(parents=True, exist_ok=True)
    (s / "deployed-ids.parquet").write_bytes(ids)
    if deployed is not None:
        (s / "deployed.json").write_text(json.dumps({"version": deployed, "commit": "a" * 32, "commitTime": "2026-10-20T00:00:00Z", "deployedAt": "2026-10-21T00:00:00Z"}))
    return s


def ids_file(tmp_path, content=b"ids-B"):
    p = tmp_path / "cand.parquet"
    p.write_bytes(content)
    return p


META_B = {"version": B, "commit": "b" * 32, "commitTime": "2026-10-27T00:22:09Z", "run": "r2", "deployedAt": "2026-10-28T00:00:00Z"}


def test_normal_week_pending_then_promote(tmp_path):
    s = mk_state(tmp_path)
    baseline.write_pending(s, ids_file(tmp_path), META_B)
    assert baseline.reconcile(s, A, say=lambda *_: None)["version"] == A  # live not yet at B: pending dropped
    assert not (s / "pending-deploy").exists() and (s / "deployed-ids.parquet").read_bytes() == b"ids-A"
    baseline.write_pending(s, ids_file(tmp_path), META_B)
    d = baseline.promote(s)
    assert d["version"] == B and (s / "deployed-ids.parquet").read_bytes() == b"ids-B" and baseline.deployed(s)["commit"] == "b" * 32
    assert not (s / "pending-deploy").exists()


def test_previous_run_deployed_and_died_is_promoted_at_start(tmp_path):
    s = mk_state(tmp_path)
    baseline.write_pending(s, ids_file(tmp_path), META_B)  # ... wrangler ran, process killed before verify
    d = baseline.reconcile(s, B, say=lambda *_: None)
    assert d["version"] == B and (s / "deployed-ids.parquet").read_bytes() == b"ids-B" and not (s / "pending-deploy").exists()


def test_unknown_live_version_fails_loudly(tmp_path):
    s = mk_state(tmp_path)
    with pytest.raises(baseline.UnknownLive, match="unknown version 9999"):
        baseline.reconcile(s, 9999, say=lambda *_: None)
    baseline.write_pending(s, ids_file(tmp_path), META_B)
    with pytest.raises(baseline.UnknownLive, match="unknown version 9999"):
        baseline.reconcile(s, 9999, say=lambda *_: None)
    assert (s / "pending-deploy").exists()  # nothing destroyed on a mismatch


def test_launch_build_migration_only_when_absent_and_exact(tmp_path):
    s = mk_state(tmp_path, deployed=None)
    with pytest.raises(baseline.UnknownLive):
        baseline.reconcile(s, LAUNCH + 1, say=lambda *_: None)
    d = baseline.reconcile(s, LAUNCH, {"snapshot": "2026-10-20"}, say=lambda *_: None)
    assert d["version"] == LAUNCH and d["commit"] is None and d["commitTime"] == "2026-10-20T00:00:00Z" and d["deployedAt"] == baseline.LAUNCH_DEPLOYED_AT
    # present: never overwritten, a different live version is an error even if it is the launch version
    s2 = mk_state(tmp_path / "x", deployed=A)
    with pytest.raises(baseline.UnknownLive):
        baseline.reconcile(s2, LAUNCH, say=lambda *_: None)
    # no ids to compare against: nothing to seed
    s3 = tmp_path / "empty"
    s3.mkdir()
    with pytest.raises(baseline.UnknownLive):
        baseline.reconcile(s3, LAUNCH, say=lambda *_: None)


def test_kill_every_step_of_pending_and_promote(tmp_path, steps):
    """write_pending happens before wrangler (live is still A), promote after the live manifest showed B. A kill at any step of either:
    the next start ends with deployed = A (kill before the record was complete, nothing promoted) or B (kill in promote), identical to an
    uninterrupted run in the second case, and never with ids of one build paired with the json of the other."""

    def run(s):
        baseline.write_pending(s, ids_file(s.parent), META_B)
        baseline.promote(s)

    def state_of(s):
        return (s / "deployed-ids.parquet").read_bytes(), json.loads((s / "deployed.json").read_text()), (s / "pending-deploy").exists()

    probe = mk_state(tmp_path / "probe")
    steps.record()
    run(probe)
    seen = list(steps.calls)
    steps.off()
    want = state_of(probe)
    assert {"pending-ids", "pending-meta", "pending-rename", "promote-ids", "promote-json", "promote-drop"} <= set(seen)
    for i, name in enumerate(seen):
        s = mk_state(tmp_path / f"k{i}")
        steps.kill_at(i)
        with pytest.raises(Killed):
            run(s)
        steps.off()
        if name.startswith("pending"):
            baseline.reconcile(s, A, say=lambda *_: None)  # deploy never started
            ids, d, pend = state_of(s)
            assert (ids, d["version"], pend) == (b"ids-A", A, False), f"kill at {name}"
            baseline.write_pending(s, ids_file(tmp_path), META_B)  # and the next deploy can proceed (stale .tmp cleaned)
        else:
            baseline.reconcile(s, B, say=lambda *_: None)
            assert state_of(s) == want, f"kill at {name}"


def test_promote_killed_between_its_two_replaces_is_completed_by_reconcile(tmp_path, steps):
    s = mk_state(tmp_path)
    baseline.write_pending(s, ids_file(tmp_path), META_B)
    steps.kill_on("promote-json")
    with pytest.raises(Killed):
        baseline.promote(s)
    steps.off()
    assert (s / "deployed-ids.parquet").read_bytes() == b"ids-B" and baseline.deployed(s)["version"] == A and (s / "pending-deploy").exists()
    assert baseline.reconcile(s, B, say=lambda *_: None)["version"] == B and not (s / "pending-deploy").exists()


NOW = datetime(2026, 11, 1, 10, 0, tzinfo=timezone.utc)


def dj(commit="a" * 32, commit_time="2026-10-27T00:22:09Z"):
    return {"version": A, "commit": commit, "commitTime": commit_time, "deployedAt": "2026-10-28T00:00:00Z"}


def test_preflight_unchanged_input_is_a_noop_until_stale():
    with pytest.raises(baseline.NoNewData, match="no new data; live data age 5."):
        baseline.preflight(dj(), {"hash": "a" * 32}, NOW)
    baseline.preflight(dj(), {"hash": "b" * 32}, NOW)  # new commit: proceed
    baseline.preflight(None, {"hash": "a" * 32}, NOW)
    baseline.preflight(dj(commit=None), {"hash": "a" * 32}, NOW)  # launch build has no commit: always proceed
    with pytest.raises(baseline.DataStale, match="no new Dolt commit for 10 days"):
        baseline.preflight(dj(), {"hash": "a" * 32}, NOW + timedelta(days=5))
    baseline.preflight(dj(), {"hash": "b" * 32}, NOW + timedelta(days=30))  # stale only matters while the input is unchanged
    with pytest.raises(baseline.NoNewData):  # exactly 9 days is still fine
        baseline.preflight(dj(), {"hash": "a" * 32}, datetime(2026, 11, 5, 0, 22, 9, tzinfo=timezone.utc))


def test_weeks_since_rounds_up_and_caps():
    d = lambda days: baseline.weeks_since(dj(), datetime(2026, 10, 28, tzinfo=timezone.utc) + timedelta(days=days))  # noqa: E731
    assert [d(0), d(1), d(7), d(7.1), d(14), d(21.5), d(28), d(90)] == [1, 1, 1, 2, 2, 4, 4, 4]
    assert baseline.weeks_since(None) == 1


# ---- accepted drift baseline, resumable cleanup, incomplete pending


def drift_file(tmp_path, content=b"extract-B"):
    p = tmp_path / "ex-public-v2.parquet"
    p.write_bytes(content)
    return p


def pend_b(s, tmp_path):
    baseline.write_pending(s, ids_file(tmp_path), META_B, drift_src=drift_file(tmp_path))


def leftovers(s):
    return sorted(p.name for p in s.iterdir() if p.name.startswith("pending-deploy"))


def test_accepted_baseline_advances_only_on_promote(tmp_path):
    s = mk_state(tmp_path)
    assert baseline.accepted_file(s) is None
    pend_b(s, tmp_path)
    assert baseline.accepted_file(s) is None  # a written (or failed, or dry) run never advances it
    baseline.reconcile(s, A, say=lambda *_: None)  # the deploy never happened: pending dropped
    assert baseline.accepted_file(s) is None
    pend_b(s, tmp_path)
    baseline.promote(s)
    assert baseline.accepted_file(s).read_bytes() == b"extract-B"


def test_kill_every_step_of_cleanup_and_accepted(tmp_path, steps):
    def prep(name):
        s = mk_state(tmp_path / name)
        pend_b(s, tmp_path / name)
        return s

    probe = prep("probe")
    steps.record()
    baseline.promote(probe)
    seen = list(steps.calls)
    steps.off()
    assert {"accepted-copy", "retire-rename", "cleanup-unlink", "cleanup-rmdir", "pointer-replace"} <= set(seen)
    assert seen.count("cleanup-unlink") >= 3  # ids, drift file, meta: a kill after each deletion is exercised
    for i, name in enumerate(seen):
        s = prep(f"k{i}")
        steps.kill_at(i)
        with pytest.raises(Killed):
            baseline.promote(s)
        steps.off()
        assert baseline.reconcile(s, B, say=lambda *_: None)["version"] == B, f"kill at {name}"
        assert leftovers(s) == [] and (s / "deployed-ids.parquet").read_bytes() == b"ids-B" and baseline.accepted_file(s).read_bytes() == b"extract-B"
        baseline.write_pending(s, ids_file(tmp_path), {**META_B, "version": 3333})  # the next deploy can proceed


def test_kill_inside_drop_cleanup_with_live_still_at_a(tmp_path, steps):
    probe = mk_state(tmp_path / "probe")
    pend_b(probe, tmp_path / "probe")
    steps.record()
    baseline.reconcile(probe, A, say=lambda *_: None)
    seen = list(steps.calls)
    steps.off()
    assert "cleanup-unlink" in seen
    for i, name in enumerate(seen):
        s = mk_state(tmp_path / f"d{i}")
        pend_b(s, tmp_path / f"d{i}")
        steps.kill_at(i)
        with pytest.raises(Killed):
            baseline.reconcile(s, A, say=lambda *_: None)
        steps.off()
        assert baseline.reconcile(s, A, say=lambda *_: None)["version"] == A, f"kill at {name}"
        assert leftovers(s) == [] and baseline.accepted_file(s) is None and (s / "deployed-ids.parquet").read_bytes() == b"ids-A"


def test_incomplete_pending_dir(tmp_path):
    s = mk_state(tmp_path)
    (s / "pending-deploy").mkdir()
    (s / "pending-deploy" / "meta.json").write_text(json.dumps(META_B))  # ids.parquet missing
    with pytest.raises(baseline.UnknownLive, match="incomplete"):
        baseline.reconcile(s, B, say=lambda *_: None)  # live moved: cannot tell what happened -> loud
    assert baseline.reconcile(s, A, say=lambda *_: None)["version"] == A  # never reached wrangler: discarded
    assert leftovers(s) == []


def test_write_pending_precedes_wrangler_and_meta_is_written_last():
    from pathlib import Path

    src = (Path(baseline.__file__).parent / "refresh.py").read_text()
    assert src.index("baseline.write_pending(") < src.index('"pages", "deploy"') < src.index("baseline.promote(STATE)")
    w = Path(baseline.__file__).read_text()
    body = w[w.index("def write_pending") : w.index("def pending_complete")]
    assert body.index('"pending-meta"') > body.index("ids.parquet") and body.index("os.rename(tmp, pend)") > body.index('"pending-meta"')
