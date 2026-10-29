"""finalize.py (refresh mode): snapshot and history.asof are the same commit date, or the build fails."""
import json
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import finalize


def dist(tmp_path, hist_asof):
    d = tmp_path / "dist"
    (d / "data").mkdir(parents=True)
    (d / "data/manifest.json").write_text(json.dumps({"n": 3, "hasThumb": True, "snapshot": "2026-10-20", "history": {"asof": hist_asof}}))
    (d / "about.html").write_text("data @@SNAPSHOT@@ history @@HISTORY@@ n=@@COUNT@@ @@COMMIT@@")
    pq.write_table(pa.table({"last_scrape_time": pa.array([None], pa.timestamp("us"))}), tmp_path / "p.parquet")
    return d


def run(monkeypatch, d, tmp_path):
    monkeypatch.setattr(sys, "argv", ["finalize.py", str(d), str(tmp_path / "p.parquet"), "--asof", "2026-10-27T00:22:09Z", "--commit", "c" * 32])
    return finalize.main()


def test_consistent_dates_pass(tmp_path, monkeypatch):
    d = dist(tmp_path, "2026-10-27")
    assert run(monkeypatch, d, tmp_path) == 0
    m = json.loads((d / "data/manifest.json").read_text())
    assert m["snapshot"] == m["history"]["asof"] == "2026-10-27" and m["refresh"]["commitTime"] == "2026-10-27T00:22:09Z"
    assert "history 2026-10-27" in (d / "about.html").read_text()


def test_history_asof_from_another_day_fails_the_build(tmp_path, monkeypatch):
    d = dist(tmp_path, "2026-10-26")  # the pre-fix behaviour: newest metrics time just before midnight
    with pytest.raises(SystemExit, match="history.asof"):
        run(monkeypatch, d, tmp_path)
