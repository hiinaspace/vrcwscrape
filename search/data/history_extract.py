#!/usr/bin/env python3
"""Extract package-upload history + metrics from the DoltHub mirror clone.

Usage: history_extract.py <dolt-bin> <clone-dir> <public-v2.parquet> <out-dir> [--port 13307]
Serves the clone with `dolt sql-server` (read-only use), pulls raw rows in pages,
parses JSON in Python, writes uploads/metrics/summary parquets with DuckDB.
Run with: uv run --with duckdb --with pymysql --with pyarrow python history_extract.py ...
"""
import json, re, subprocess, sys, time, os
import duckdb, pymysql, pyarrow as pa, pyarrow.parquet as pq
from datetime import datetime, timezone

dolt, clone, public_pq, out = sys.argv[1:5]
port = int(sys.argv[6]) if len(sys.argv) > 6 and sys.argv[5] == "--port" else 13307
os.makedirs(out, exist_ok=True)
DOT = "․"
PLAT = {"android": "quest", "standalonewindows": "pc", "ios": "ios"}

srv = subprocess.Popen(["nice", "-n10", dolt, "sql-server", "-H", "127.0.0.1", "-P", str(port)],
                       cwd=clone, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    for _ in range(60):
        try:
            cn = pymysql.connect(host="127.0.0.1", port=port, user="root", database=os.path.basename(clone) if False else None, autocommit=True)
            break
        except Exception:
            time.sleep(1)
    cur = cn.cursor()
    cur.execute("show databases"); dbs = [r[0] for r in cur.fetchall()]
    db = [d for d in dbs if d not in ("information_schema", "mysql")][0]
    cur.execute(f"use `{db}`")
    cur.execute("select dolt_hashof('HEAD'), max(date) from dolt_log"); head = cur.fetchone()
    print("HEAD", head, flush=True)

    public = duckdb.sql(f"select world_id from read_parquet('{public_pq}')").fetchall()
    pubset = {r[0] for r in public}

    def parse_name(n):
        n = (n or "").replace(DOT, ".")
        m = re.search(r"-\s*([0-9]{4}\.[0-9]+\.[0-9]+[a-z0-9]*)_\d+_([a-z]+)_", n)
        if not m:
            m2 = re.search(r"([0-9]{4}\.[0-9]+\.[0-9]+[a-z0-9]*)", n)
            uv = m2.group(1) if m2 else None
            pl = next((v for k, v in PLAT.items() if k in n.lower()), "other")
            return pl, uv
        return PLAT.get(m.group(2), "other"), m.group(1)

    rows = []; bad = {"bad_json": 0, "no_created_at": 0, "nonpublic_skipped": 0}
    cur2 = pymysql.cursors.SSCursor(cn)
    cur2.execute("select world_id, file_metadata from file_metadata where file_type='UNITY_PACKAGE' and scrape_status='SUCCESS'")
    n = 0
    for wid, js in cur2:
        n += 1
        if wid not in pubset:
            bad["nonpublic_skipped"] += 1; continue
        try:
            d = json.loads(js) if isinstance(js, (str, bytes)) else js
        except Exception:
            bad["bad_json"] += 1; continue
        if not d: bad["bad_json"] += 1; continue
        pl, uv = parse_name(d.get("name"))
        for v in d.get("versions") or []:
            ca = v.get("created_at")
            if not ca:
                bad["no_created_at"] += 1; continue
            sz = (v.get("file") or {}).get("sizeInBytes")
            rows.append((wid, pl, uv, int(v.get("version")), ca, int(sz) if sz is not None else None, bool(v.get("deleted"))))
    cur2.close()
    print("package rows scanned", n, "version rows", len(rows), bad, flush=True)
    t = pa.table({
        "world_id": [r[0] for r in rows], "platform": [r[1] for r in rows], "unity_version": [r[2] for r in rows],
        "version": pa.array([r[3] for r in rows], pa.int32()), "created_raw": [r[4] for r in rows],
        "size_bytes": pa.array([r[5] for r in rows], pa.int64()), "deleted": [r[6] for r in rows]})
    con = duckdb.connect()
    con.register("raw", t)
    con.execute(f"""copy (select world_id, platform, unity_version, version,
        cast(try_cast(created_raw as timestamptz) at time zone 'UTC' as timestamp) as created_at, size_bytes, deleted
        from raw where try_cast(created_raw as timestamptz) >= timestamptz '2015-01-01' order by world_id, created_at, platform)
        to '{out}/uploads.parquet' (format parquet, compression zstd)""")

    bad["epoch_or_unparseable_dropped"] = len(rows) - con.execute(f"select count(*) from '{out}/uploads.parquet'").fetchone()[0]
    # metrics: stream to parquet
    cur3 = pymysql.cursors.SSCursor(cn)
    cur3.execute("select world_id, scrape_time, visits, favorites, occupants from world_metrics")
    w = None; buf = []; total = 0
    sch = pa.schema([("world_id", pa.string()), ("t", pa.timestamp("us")), ("visits", pa.int64()),
                     ("favorites", pa.int64()), ("occupants", pa.int64())])
    w = pq.ParquetWriter(f"{out}/metrics.unsorted.parquet", sch, compression="zstd")
    def flush():
        global buf
        if buf:
            c = list(zip(*buf))
            w.write_table(pa.table({s.name: pa.array(c[i], s.type) for i, s in enumerate(sch)}))
            buf = []
    for r in cur3:
        if r[0] in pubset:
            buf.append(r); total += 1
            if len(buf) >= 200000: flush()
    flush(); w.close(); cur3.close()
    print("metrics rows public", total, flush=True)
    con.execute(f"""copy (select * from read_parquet('{out}/metrics.unsorted.parquet') order by world_id, t)
        to '{out}/metrics.parquet' (format parquet, compression zstd)""")
    os.remove(f"{out}/metrics.unsorted.parquet")

    # summary: events = per-world uploads collapsed within 10 min (gap-based chaining)
    asof = con.execute(f"select max(t) from '{out}/metrics.parquet'").fetchone()[0]  # data time; working tree runs ahead of the HEAD commit
    con.execute(f"""create temp table ev as
      with u as (select world_id, created_at, lag(created_at) over (partition by world_id order by created_at) p
                 from read_parquet('{out}/uploads.parquet')),
      m as (select world_id, created_at, case when p is null or created_at - p > interval 10 minute then 1 else 0 end s from u),
      g as (select world_id, created_at, sum(s) over (partition by world_id order by created_at) gid from m)
      select world_id, min(created_at) t from g group by world_id, gid""")
    con.execute(f"""copy (select p.world_id, min(e.t) first_upload, max(e.t) last_upload,
        count(e.t)::int uploads_total,
        count(*) filter (where e.t >= timestamp '{asof:%Y-%m-%d %H:%M:%S}' - interval 30 day)::int uploads_30d,
        count(*) filter (where e.t >= timestamp '{asof:%Y-%m-%d %H:%M:%S}' - interval 90 day)::int uploads_90d,
        count(*) filter (where e.t >= timestamp '{asof:%Y-%m-%d %H:%M:%S}' - interval 365 day)::int uploads_365d
      from (select world_id, file_row_number r from read_parquet('{public_pq}', file_row_number=true)) p left join ev e using (world_id)
      group by p.world_id, p.r order by p.r) to '{out}/summary.tmp.parquet'""")
    con.execute(f"copy (select * from read_parquet('{out}/summary.tmp.parquet')) to '{out}/summary.parquet' (format parquet, compression zstd)")
    os.remove(f"{out}/summary.tmp.parquet")
    json.dump({"head": head[0], "head_date": str(head[1]), "asof": str(asof), **bad}, open(f"{out}/asof.json", "w"))
finally:
    srv.terminate()
