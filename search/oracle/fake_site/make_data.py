"""Create the fake site's 6k-row sample (parquet + data.json) from the v2 public parquet.

Usage: uv run python fake_site/make_data.py <v2 parquet> [--n 6000]
The sample parquet is committed (truth for the fake); data.json is generated (git-ignored).
Descriptions are cut to 300 chars to keep the sample small (the fake and the oracle both see the cut).
"""

import json
import sys
from pathlib import Path

import duckdb

here = Path(__file__).parent
n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 6000
con = duckdb.connect()
src = sys.argv[1]
con.execute(
    f"copy (select * replace (substr(description, 1, 300) as description) from read_parquet('{src}') "
    f"where name is not null order by hash(world_id) limit {n}) "
    f"to '{here / 'sample.parquet'}' (format parquet, compression zstd)"
)
rows = con.execute(
    f"""select world_id id, name, author_id, author_name author, capacity cap,
        strftime(updated_at, '%Y-%m-%dT%H:%M:%S') updated, strftime(created_at, '%Y-%m-%dT%H:%M:%S') created,
        tags, favorites fav, visits, pc_size_mb pc, quest_size_mb quest, description as "desc", platforms,
        recent_visits_per_day rv
        from read_parquet('{here / "sample.parquet"}')"""
).fetchall()
cols = [
    "id",
    "name",
    "author_id",
    "author",
    "cap",
    "updated",
    "created",
    "tags",
    "fav",
    "visits",
    "pc",
    "quest",
    "desc",
    "platforms",
    "rv",
]
data = [dict(zip(cols, r)) for r in rows]
(here / "data.json").write_text(json.dumps(data, ensure_ascii=False))
print(len(data), "rows")
import os

(here / "sem.bin").write_bytes(
    os.urandom(2 * 1024 * 1024)
)  # stands in for the semantic model download
