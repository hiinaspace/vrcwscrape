#!/usr/bin/env python3
"""Drop excluded worlds/authors (deploy/excluded_ids.txt) from the public parquet.

usage: filter_parquet.py IN.parquet EXCLUDED.txt OUT.parquet
Writes OUT only when something is removed. Last stdout line = number of rows removed (the caller keys on it).
"""

import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def load(p: str) -> tuple[set[str], set[str]]:
    w, a = set(), set()
    for line in open(p, encoding="utf-8"):
        s = line.split("#", 1)[0].strip()
        if not s:
            continue
        if s.startswith("wrld_"):
            w.add(s)
        elif s.startswith("usr_"):
            a.add(s)
        else:
            w.add("wrld_" + s)
            a.add("usr_" + s)
    return w, a


def main() -> int:
    src, excl, out = sys.argv[1:4]
    w, a = load(excl)
    t = pq.read_table(src)
    drop = pc.or_(pc.is_in(t["world_id"], value_set=pa.array(sorted(w), type=t["world_id"].type)),
                  pc.is_in(t["author_id"], value_set=pa.array(sorted(a), type=t["author_id"].type)))
    n = int(pc.sum(drop).as_py() or 0)
    if n:
        pq.write_table(t.filter(pc.invert(drop)), out)
    print(n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
