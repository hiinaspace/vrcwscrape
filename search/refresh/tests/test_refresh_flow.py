"""refresh.py window handoff and early exits, with the cluster/db/venv steps mocked."""
import json
import os
import tempfile
import time
from pathlib import Path

import pytest

_ROOT = Path(tempfile.mkdtemp(prefix="refresh-flow-"))
os.environ.update({"STATE": str(_ROOT / "state"), "WORK": str(_ROOT / "work"), "IMAGES": str(_ROOT / "images"), "FLAG": str(_ROOT / "flag"),
                   "DATABASE_URL": "mysql://u:p@h:3306/db", "LIVE_URL": "http://live.invalid"})
(_ROOT / "images").mkdir()

import baseline  # noqa: E402
import refresh as R  # noqa: E402

HEAD = {"hash": "a" * 32, "time": "2026-10-27T00:22:09Z"}
LIVE = 1111


@pytest.fixture
def rig(monkeypatch):
    log: list[str] = []
    st = R.STATE
    for d in list(st.iterdir()):
        if d.name != "runs":
            import shutil

            shutil.rmtree(d, ignore_errors=True) if d.is_dir() else d.unlink()
    (st / "deployed-ids.parquet").write_bytes(b"ids")
    (st / "deployed.json").write_text(json.dumps({"version": LIVE, "commit": "a" * 32, "commitTime": "2026-10-27T00:22:09Z", "deployedAt": "2026-10-28T00:00:00Z"}))
    R._flag_held = False
    R._keeper_stop.clear()
    R.FLAG.write_text("0\n")
    flag_at = {}

    def flag_value() -> int:
        return int(R.FLAG.read_text())

    monkeypatch.setattr(baseline, "fetch_live", lambda url, tries=3: {"version": LIVE, "snapshot": "2026-10-27"})
    monkeypatch.setattr(R, "prep_deps", lambda: log.append("deps"))
    monkeypatch.setattr(R, "restore_hf", lambda: None)
    monkeypatch.setattr(R, "pin_head", lambda: dict(HEAD))
    monkeypatch.setattr(R, "pods", lambda sel=None: [])
    monkeypatch.setattr(R, "kube", lambda *a, **k: {"status": {"startTime": "2026-10-27T09:00:00Z", "conditions": []}})

    def restart(reason):
        log.append(f"restart_dolt flag_held={flag_value() > time.time()}")

    monkeypatch.setattr(R, "restart_dolt", restart)
    real_clear = R.flag_clear

    def clear():
        log.append(f"flag_clear (held={R._flag_held})")
        real_clear()

    monkeypatch.setattr(R, "flag_clear", clear)
    real_open = R.flag_open

    def opn():
        log.append("flag_open")
        real_open()

    monkeypatch.setattr(R, "flag_open", opn)
    return log, flag_value


def test_flag_is_the_first_thing_and_unchanged_head_ends_the_run_without_a_restart(rig):
    log, flag_value = rig
    assert R.main() == 0
    assert log[0] == "flag_open"
    assert not any(l.startswith("restart_dolt") for l in log), log  # nothing was read from Dolt: no heap to free
    assert flag_value() == 0 and "flag_clear (held=True)" in log
    assert not (R.STATE / "extract/new").exists()
    assert json.loads((R.STATE / "runs" / f"{R.RUN_ID}.json").read_text())["noop"] is True


def test_stale_unchanged_head_fails_the_job(rig):
    log, flag_value = rig
    dj = json.loads((R.STATE / "deployed.json").read_text())
    dj["commitTime"] = "2026-10-01T00:00:00Z"  # same commit, much older than 9 days
    (R.STATE / "deployed.json").write_text(json.dumps(dj))
    assert R.main() == 1
    assert flag_value() == 0 and not any(l.startswith("restart_dolt") for l in log)


def test_unknown_live_version_fails_loudly_and_releases_the_flag(rig, monkeypatch):
    log, flag_value = rig
    monkeypatch.setattr(baseline, "fetch_live", lambda url, tries=3: {"version": 9999})
    assert R.main() == 1
    assert log[0] == "flag_open" and flag_value() == 0
    assert "deps" not in log  # failed before anything expensive


def test_flag_held_through_the_post_extract_dolt_restart(rig, monkeypatch):
    log, flag_value = rig
    monkeypatch.setattr(R, "pin_head", lambda: {"hash": "c" * 32, "time": "2026-11-01T00:22:09Z"})  # a new commit

    def boom(*a, **k):
        raise RuntimeError("etl exploded")  # extract fails after the pin: the finally path

    monkeypatch.setattr(R, "py", boom)
    assert R.main() == 1
    i_restart = [i for i, l in enumerate(log) if l.startswith("restart_dolt")]
    assert i_restart, log
    assert all("flag_held=True" in log[i] for i in i_restart), log  # the scraper is still locked out while dolt-0 is deleted
    assert log.index("flag_clear (held=True)") > max(i_restart) and flag_value() == 0
