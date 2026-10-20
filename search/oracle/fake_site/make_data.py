"""Create the fake site's 2k-row sample (parquet + data.json) from a full parquet.

Usage: uv run python fake_site/make_data.py <parquet>
"""

import json
import sys
from pathlib import Path

import duckdb

here = Path(__file__).parent
con = duckdb.connect()
src = sys.argv[1]
con.execute(
    f"copy (select * from read_parquet('{src}') where name is not null order by hash(world_id) "
    f"limit 2000) to '{here / 'sample.parquet'}' (format parquet, compression zstd)"
)
rows = con.execute(
    f"""select world_id id, name, author_id, author_name author, capacity cap,
        strftime(updated_at, '%Y-%m-%dT%H:%M:%S') updated, strftime(created_at, '%Y-%m-%dT%H:%M:%S') created,
        tags, favorites, visits, pc_size_mb pc, quest_size_mb quest, description
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
]
data = [dict(zip(cols, r)) for r in rows]
(here / "data.json").write_text(json.dumps(data, ensure_ascii=False))
print(len(data), "rows")
