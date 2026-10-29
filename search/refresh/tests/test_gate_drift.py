"""gate.py: extract drift check (column empty rates, changed-text fraction)."""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import gate


def table(n=1000, blank_desc=0.05, change=0, blank_fav=0.0):
    return pa.table({
        "world_id": [f"wrld_{i}" for i in range(n)],
        "name": [f"name {i}" for i in range(n)],
        "description": ["" if i < n * blank_desc else f"desc {i}{' changed' if i >= n - change else ''}" for i in range(n)],
        "tags": [["a", "b"] for _ in range(n)],
        "author_name": ["au"] * n,
        "capacity": pa.array([16] * n, pa.int32()),
        "favorites": pa.array([0 if i < n * blank_fav else 5 for i in range(n)], pa.int32()),
        "visits": pa.array([100] * n, pa.int32()),
    })


@pytest.fixture(autouse=True)
def clear():
    gate.results.clear()


def run(tmp_path, cur, prev):
    pq.write_table(cur, tmp_path / "cur.parquet")
    pq.write_table(prev, tmp_path / "prev.parquet")
    gate.check_extract_drift(tmp_path / "cur.parquet", tmp_path / "prev.parquet")
    return {r["check"]: r for r in gate.results}


def test_normal_week_passes(tmp_path):
    r = run(tmp_path, table(change=15, blank_desc=0.051), table())
    assert all(x["ok"] for x in r.values()), r


def test_blanked_column_fails_with_numbers(tmp_path):
    r = run(tmp_path, table(blank_fav=0.20), table(blank_fav=0.05))
    x = r["extract drift: empty/null rates"]
    assert not x["ok"] and "favorites +15.00pp" in x["detail"]
    r = run(tmp_path, table(blank_desc=0.30), table())
    assert not r["extract drift: empty/null rates"]["ok"]


def test_mass_text_change_fails(tmp_path):
    r = run(tmp_path, table(change=400), table())
    x = r["extract drift: text changed"]
    assert not x["ok"] and "40.00%" in x["detail"]


def test_no_previous_extract_is_not_a_failure(tmp_path):
    pq.write_table(table(), tmp_path / "cur.parquet")
    gate.check_extract_drift(tmp_path / "cur.parquet", tmp_path / "missing.parquet")
    assert gate.results[0]["ok"]
