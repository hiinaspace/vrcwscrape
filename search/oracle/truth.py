"""DuckDB ground truth for facet counts on random filter combos.

Usage: uv run python truth.py <parquet> [--out gold/facets.jsonl] [--n 200] [--seed 7]
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

from common import connect, facet_truth


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("parquet")
    ap.add_argument("--out", default="gold/facets.jsonl")
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    con = connect(a.parquet)
    lo, hi = con.execute("select min(updated_at), max(updated_at) from w").fetchone()
    sizes = con.execute(
        "select quantile_cont(x, [0.1,0.25,0.5,0.75,0.9]) from "
        "(select least(coalesce(pc_size_mb,1e9), coalesce(quest_size_mb,1e9)) x from w where "
        "pc_size_mb is not null or quest_size_mb is not null)"
    ).fetchone()[0]
    top_tags = [
        r[0]
        for r in con.execute(
            "select t from (select unnest(tagset) t from w) group by t having count(*) >= 20 "
            "order by count(*) desc, t limit 60"
        ).fetchall()
    ]
    authors = [
        r[0]
        for r in con.execute(
            "select author_id from w group by author_id having count(*) between 5 and 300 "
            "order by author_id limit 5000"
        ).fetchall()
    ]
    caps = [4, 8, 16, 24, 32, 40, 64, 80]
    out = []
    for _ in range(a.n):
        f: dict = {}
        dims = rnd.sample(["cap", "size", "platform", "date", "tags", "author"], rnd.randint(1, 4))
        if "cap" in dims:
            kind = rnd.choice(["min", "max", "both"])
            if kind in ("min", "both"):
                f["cap_min"] = rnd.choice(caps)
            if kind in ("max", "both"):
                f["cap_max"] = rnd.choice([c for c in caps if c >= f.get("cap_min", 0)])
        if "size" in dims:
            f["size_max_mb"] = round(rnd.choice(sizes), 1)
        if "platform" in dims:
            f["platform"] = rnd.choice(["pc", "quest", "both"])
        if "date" in dims:
            kind = rnd.choice(["after", "before", "both"])
            d1 = lo + (hi - lo) * rnd.random()
            d2 = d1 + (hi - d1) * rnd.random()
            if kind in ("after", "both"):
                f["after"] = d1.strftime("%Y-%m-%d")
            if kind in ("before", "both"):
                f["before"] = (d2 if kind == "both" else d1).strftime("%Y-%m-%d")
        if "tags" in dims:
            f["tags"] = rnd.sample(top_tags[:30], rnd.choice([1, 1, 2]))
        if "author" in dims and authors:
            f["author"] = rnd.choice(authors)
        out.append({"filters": f, "truth": facet_truth(con, f)})
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    with open(a.out, "w") as fh:
        fh.writelines(json.dumps(o, ensure_ascii=False) + "\n" for o in out)
    nz = sum(1 for o in out if o["truth"]["total"] > 0)
    print(f"wrote {len(out)} combos ({nz} non-empty) to {a.out}; updated_at range {lo}..{hi}")


if __name__ == "__main__":
    main()
