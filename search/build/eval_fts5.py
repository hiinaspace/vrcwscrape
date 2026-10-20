"""Design (a) measurement: SQLite FTS5 (trigram + unicode61) sizes and page-read traces.

usage: eval_fts5.py <worlds.parquet> <work_dir>
Prints JSON. I/O is measured with strace (pread64 distinct 4 KiB pages), which is what an
HTTP-Range VFS (sql.js-httpvfs / wa-sqlite) would have to fetch (lower bound: no read-ahead).
"""

import json
import os
import re
import sqlite3
import apsw
import sys
import time

import pyarrow.parquet as pq
import sqlite3  # noqa

pq_path, work = sys.argv[1], sys.argv[2]
os.makedirs(work, exist_ok=True)
t = pq.read_table(pq_path).to_pydict()
n = len(t["world_id"])
order = sorted(range(n), key=lambda i: (-(t["visits"][i] or 0), -(t["favorites"][i] or 0), t["world_id"][i]))
rows = []
for r, i in enumerate(order):
    tags = " ".join(re.sub(r"^(author_tag_|system_)", "", x) for x in (t["tags"][i] or []))
    rows.append((r, t["name"][i] or "", t["author_name"][i] or "", tags, (t["description"][i] or "")[:1000]))


def build(name, ddl, cols):
    p = os.path.join(work, name + ".db")
    if os.path.exists(p):
        os.remove(p)
    c = sqlite3.connect(p)
    c.execute("pragma page_size=4096")
    t0 = time.time()
    c.execute(ddl)
    ph = ",".join("?" * (len(cols) + 1))
    c.executemany(f"insert into f(rowid,{','.join(cols)}) values ({ph})", [(r[0], *[r[1 + k] for k in range(len(cols))]) for r in rows])
    c.execute("insert into f(f) values('optimize')")
    c.commit()
    c.execute("vacuum")
    c.close()
    return p, os.path.getsize(p), time.time() - t0


res = {"worlds": n}
variants = {
    "trigram_title_author_tags": ("create virtual table f using fts5(title,author,tags,tokenize='trigram',columnsize=0)", ["title", "author", "tags"]),
    "trigram_all_with_desc": ("create virtual table f using fts5(title,author,tags,descr,tokenize='trigram',columnsize=0)", ["title", "author", "tags", "descr"]),
    "unicode61_prefix_all": ("create virtual table f using fts5(title,author,tags,descr,tokenize='unicode61',prefix='2 3')", ["title", "author", "tags", "descr"]),
}
paths = {}
for k, (ddl, cols) in variants.items():
    p, sz, secs = build(k, ddl, cols)
    paths[k] = p
    res[k] = {"bytes": sz, "build_s": round(secs, 1)}
    print(k, sz, flush=True)

QS = [
    ("trigram_all_with_desc", "chill", "latin word 'chill'"),
    ("trigram_all_with_desc", "ミラー", "katakana substring"),
    ("trigram_all_with_desc", "睡眠", "kanji 2-char (trigram cannot match <3 chars: falls to scan)"),
    ("trigram_all_with_desc", "night club", "two words"),
    ("unicode61_prefix_all", "chill*", "prefix word"),
    ("unicode61_prefix_all", "cozy hangout*", "two words, prefix last"),
]

class TFile(apsw.VFSFile):
    def __init__(self, name, flags):
        super().__init__(vfs_base, name, flags)

    def xRead(self, amount, offset):
        LOG.append((offset, amount))
        return super().xRead(amount, offset)


class TVfs(apsw.VFS):
    def __init__(self):
        super().__init__("trace", "")

    def xOpen(self, name, flags):
        return TFile(name, flags)


vfs_base = ""
LOG: list = []
_tvfs = TVfs()
res["queries"] = []
for var, q, note in QS:
    for mode in ("rowid_order", "bm25"):
        c = apsw.Connection(paths[var], flags=apsw.SQLITE_OPEN_READONLY, vfs="trace")
        c.pragma("cache_size", 0)
        LOG.clear()
        t0 = time.time()
        order = "rank" if mode == "bm25" else "rowid"
        r = c.execute(f"select rowid from f where f match ? order by {order} limit 50", (q,)).fetchall()
        ms = (time.time() - t0) * 1000
        pages = set()
        for off, ln in LOG:
            for pg in range(off // 4096, (off + ln - 1) // 4096 + 1):
                pages.add(pg)
        pl = sorted(pages)
        runs = sum(1 for i, p in enumerate(pl) if i == 0 or p != pl[i - 1] + 1)
        res["queries"].append({"variant": var, "q": q, "note": note, "mode": mode, "hits_returned": len(r), "local_ms": round(ms, 1), "distinct_4k_pages": len(pl), "bytes": len(pl) * 4096, "contiguous_runs": runs, "reads": len(LOG)})
        c.close()
print(json.dumps(res, indent=1))
