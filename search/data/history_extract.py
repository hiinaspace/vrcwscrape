#!/usr/bin/env python3
"""Extract package-upload history + metrics from Dolt (SQL server) and write uploads/metrics/summary parquets.

Usage
  history_extract.py --database-url mysql://user:pw@host:3306/db [--base DIR] [--full] <public-v2.parquet> <out-dir>
  history_extract.py <dolt-bin> <clone-dir> <public-v2.parquet> <out-dir> [--port 13307]      (legacy: spawns a local dolt sql-server)

Incremental (the weekly k8s refresh): --base DIR keeps a per-file_id table of the parsed upload rows (rows.parquet), the file list with each
file's last_scrape_time (files.parquet) and all world_metrics rows (metrics.parquet), on the state volume.
  * full mode (no base, --full, or env HISTORY_FULL=1): keyset pages over file_metadata (a single streaming scan of the JSON column
    OOM-kills dolt-sql-server at 8 GiB; ~15 GB of JSON in all, ~23 min). Writes the base.
  * incremental mode: one cheap query lists (file_id, world_id, last_scrape_time) of every UNITY_PACKAGE/SUCCESS file (no JSON); files whose
    time differs from the base (or are new) are fetched by primary key in batches of 200 and re-parsed; files that left the list are dropped.
    world_metrics (append-only) is fetched from (base max time - 1 day) and merged on (world_id, t).
Both modes then write exactly the same outputs: uploads.parquet, metrics.parquet, summary.parquet, asof.json (filtered to the public worlds).
Upload rows of non-public worlds are kept in the base (a world can turn public later) but never written out.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import timedelta
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

DOT = "․"
PLAT = {"android": "quest", "standalonewindows": "pc", "ios": "ios"}
ROW_SCHEMA = pa.schema([("file_id", pa.string()), ("world_id", pa.string()), ("platform", pa.string()), ("unity_version", pa.string()),
                        ("version", pa.int32()), ("created_raw", pa.string()), ("size_bytes", pa.int64()), ("deleted", pa.bool_())])
FILE_SCHEMA = pa.schema([("file_id", pa.string()), ("world_id", pa.string()), ("last_scrape_time", pa.timestamp("us"))])
MET_SCHEMA = pa.schema([("world_id", pa.string()), ("t", pa.timestamp("us")), ("visits", pa.int64()),
                        ("favorites", pa.int64()), ("occupants", pa.int64())])
T0 = time.time()


def log(msg):
    print(f"[history {time.time() - T0:6.0f}s] {msg}", flush=True)


def parse_name(n):
    n = (n or "").replace(DOT, ".")
    m = re.search(r"-\s*([0-9]{4}\.[0-9]+\.[0-9]+[a-z0-9]*)_\d+_([a-z]+)_", n)
    if not m:
        m2 = re.search(r"([0-9]{4}\.[0-9]+\.[0-9]+[a-z0-9]*)", n)
        uv = m2.group(1) if m2 else None
        pl = next((v for k, v in PLAT.items() if k in n.lower()), "other")
        return pl, uv
    return PLAT.get(m.group(2), "other"), m.group(1)


def parse_file(fid, wid, js, bad):
    try:
        d = json.loads(js) if isinstance(js, (str, bytes)) else js
    except Exception:
        bad["bad_json"] += 1
        return []
    if not d:
        bad["bad_json"] += 1
        return []
    pl, uv = parse_name(d.get("name"))
    out = []
    for v in d.get("versions") or []:
        ca = v.get("created_at")
        if not ca:
            bad["no_created_at"] += 1
            continue
        sz = (v.get("file") or {}).get("sizeInBytes")
        out.append((fid, wid, pl, uv, int(v.get("version")), ca, int(sz) if sz is not None else None, bool(v.get("deleted"))))
    return out


def table(rows, schema):
    cols = list(zip(*rows)) if rows else [[] for _ in schema]
    return pa.table({f.name: pa.array(cols[i], f.type) for i, f in enumerate(schema)}, schema=schema)


class SqlSource:
    def __init__(self, cn):
        self.cn = cn

    def file_list(self):
        cur = self.cn.cursor()
        cur.execute("select file_id, world_id, last_scrape_time from file_metadata where file_type='UNITY_PACKAGE' and scrape_status='SUCCESS'")
        r = cur.fetchall()
        cur.close()
        return r

    def pages(self, batch=1000):
        cur = self.cn.cursor()
        last = ""
        while True:
            cur.execute("select file_id, world_id, file_metadata from file_metadata where file_id > %s and file_type='UNITY_PACKAGE'"
                        " and scrape_status='SUCCESS' order by file_id limit %s", (last, batch))
            page = cur.fetchall()
            if not page:
                break
            last = page[-1][0]
            yield from page
        cur.close()

    def by_ids(self, ids, batch=200):
        cur = self.cn.cursor()
        for i in range(0, len(ids), batch):
            part = ids[i:i + batch]
            cur.execute("select file_id, world_id, file_metadata from file_metadata where file_id in (%s)" % ",".join(["%s"] * len(part)), part)
            yield from cur.fetchall()
        cur.close()

    def metrics(self, since=None):
        import pymysql
        cur = pymysql.cursors.SSCursor(self.cn)
        if since is None:
            cur.execute("select world_id, scrape_time, visits, favorites, occupants from world_metrics")
        else:
            cur.execute("select world_id, scrape_time, visits, favorites, occupants from world_metrics where scrape_time > %s", (since,))
        yield from cur
        cur.close()


def write_rows(parsed, path):
    """parsed: iterator of row tuples -> parquet in chunks"""
    w = pq.ParquetWriter(path, ROW_SCHEMA, compression="zstd")
    buf, n = [], 0
    for r in parsed:
        buf.append(r)
        n += 1
        if len(buf) >= 200000:
            w.write_table(table(buf, ROW_SCHEMA))
            buf = []
    if buf:
        w.write_table(table(buf, ROW_SCHEMA))
    w.close()
    return n


def run(src, public_pq, out, base=None, full=False, head=("unknown", None)):
    os.makedirs(out, exist_ok=True)
    work = os.path.join(out, "_work")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    bad = {"bad_json": 0, "no_created_at": 0}
    con = duckdb.connect()
    have_base = bool(base) and all(os.path.isfile(os.path.join(base, f)) for f in ("files.parquet", "rows.parquet", "metrics.parquet"))
    mode = "full" if (full or not have_base) else "incremental"
    log(f"mode {mode}")

    files = src.file_list()
    log(f"{len(files)} package files listed")
    ftab = table([(a, b, c) for a, b, c in files], FILE_SCHEMA)
    rows_pq = os.path.join(work, "rows.parquet")
    if mode == "full":
        def gen():
            n = 0
            for fid, wid, js in src.pages():
                n += 1
                if n % 20000 == 0:
                    log(f"  paged {n} files")
                yield from parse_file(fid, wid, js, bad)
        n = write_rows(gen(), rows_pq)
        log(f"full scan: {n} version rows")
        changed = deleted = None
    else:
        bf = pq.read_table(os.path.join(base, "files.parquet"))
        bt = dict(zip(bf["file_id"].to_pylist(), bf["last_scrape_time"].to_pylist()))
        now = {fid: t for fid, _w, t in files}
        changed = sorted(fid for fid, t in now.items() if bt.get(fid) != t)
        deleted = sorted(set(bt) - set(now))
        log(f"{len(changed)} changed/new files, {len(deleted)} gone, {len(now) - len(changed)} unchanged")
        got = []
        for k, (fid, wid, js) in enumerate(src.by_ids(changed)):
            got.extend(parse_file(fid, wid, js, bad))
            if k and k % 2000 == 0:
                log(f"  fetched {k}/{len(changed)}")
        new_pq = os.path.join(work, "new_rows.parquet")
        pq.write_table(table(got, ROW_SCHEMA), new_pq, compression="zstd")
        drop = pa.table({"file_id": pa.array(changed + deleted, pa.string())})
        con.register("drop_ids", drop)
        con.execute(f"""copy (select b.* from read_parquet('{os.path.join(base, "rows.parquet")}') b
                         where b.file_id not in (select file_id from drop_ids)
                         union all select * from read_parquet('{new_pq}')) to '{rows_pq}' (format parquet, compression zstd)""")
        log(f"re-parsed {len(got)} version rows for the changed files")

    # metrics (append-only table): all rows, or the tail since the base's newest time
    met_pq = os.path.join(work, "metrics_all.parquet")
    if mode == "full":
        w = pq.ParquetWriter(met_pq, MET_SCHEMA, compression="zstd")
        buf, total = [], 0
        for r in src.metrics():
            buf.append(r)
            total += 1
            if len(buf) >= 200000:
                w.write_table(table(buf, MET_SCHEMA))
                buf = []
        if buf:
            w.write_table(table(buf, MET_SCHEMA))
        w.close()
        log(f"metrics: {total} rows streamed")
    else:
        base_met = os.path.join(base, "metrics.parquet")
        since = con.execute(f"select max(t) from read_parquet('{base_met}')").fetchone()[0]
        since = since - timedelta(days=1)
        tail = [r for r in src.metrics(since)]
        tail_pq = os.path.join(work, "metrics_tail.parquet")
        pq.write_table(table(tail, MET_SCHEMA), tail_pq)
        con.execute(f"""copy (select b.* from read_parquet('{base_met}') b
                         where not exists (select 1 from read_parquet('{tail_pq}') n where n.world_id = b.world_id and n.t = b.t)
                         union all select * from read_parquet('{tail_pq}')) to '{met_pq}' (format parquet, compression zstd)""")
        log(f"metrics: {len(tail)} rows since {since}")

    # ---- outputs (same as the original full extract)
    pub = f"(select world_id from read_parquet('{public_pq}'))"
    con.execute(f"""create temp table pubrows as select * from read_parquet('{rows_pq}') where world_id in {pub}""")
    con.execute(f"""copy (select world_id, platform, unity_version, version,
        cast(try_cast(created_raw as timestamptz) at time zone 'UTC' as timestamp) as created_at, size_bytes, deleted
        from pubrows where try_cast(created_raw as timestamptz) >= timestamptz '2015-01-01' order by world_id, created_at, platform)
        to '{out}/uploads.parquet' (format parquet, compression zstd)""")
    n_pub = con.execute("select count(*) from pubrows").fetchone()[0]
    bad["epoch_or_unparseable_dropped"] = n_pub - con.execute(f"select count(*) from '{out}/uploads.parquet'").fetchone()[0]
    bad["nonpublic_files"] = con.execute(f"select count(*) from read_parquet('{rows_pq}') where world_id not in {pub}").fetchone()[0]
    con.execute(f"""copy (select * from read_parquet('{met_pq}') where world_id in {pub} order by world_id, t)
        to '{out}/metrics.parquet' (format parquet, compression zstd)""")
    log("uploads + metrics written")
    asof = con.execute(f"select max(t) from '{out}/metrics.parquet'").fetchone()[0]
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
    json.dump({"head": head[0], "head_date": str(head[1]), "asof": str(asof), "mode": mode,
               "changed_files": None if changed is None else len(changed), "deleted_files": None if deleted is None else len(deleted), **bad},
              open(f"{out}/asof.json", "w"))

    if base:  # replace the base atomically (3 files)
        new = base.rstrip("/") + ".new"
        shutil.rmtree(new, ignore_errors=True)
        os.makedirs(new)
        pq.write_table(ftab, os.path.join(new, "files.parquet"), compression="zstd")
        shutil.copyfile(rows_pq, os.path.join(new, "rows.parquet"))
        shutil.copyfile(met_pq, os.path.join(new, "metrics.parquet"))
        old = base.rstrip("/") + ".old"
        shutil.rmtree(old, ignore_errors=True)
        if os.path.isdir(base):
            os.rename(base, old)
        os.rename(new, base)
        shutil.rmtree(old, ignore_errors=True)
        log("base updated")
    shutil.rmtree(work, ignore_errors=True)
    log("done")


def main():
    import pymysql
    args = sys.argv[1:]
    base = None
    full = os.environ.get("HISTORY_FULL", "") not in ("", "0")
    while "--full" in args:
        args.remove("--full")
        full = True
    if "--base" in args:
        i = args.index("--base")
        base = args[i + 1]
        del args[i:i + 2]
    srv = None
    try:
        if args[0] == "--database-url":
            u = urlparse(args[1])
            public_pq, out = args[2:4]
            cn = pymysql.connect(host=u.hostname, port=u.port or 3306, user=u.username, password=u.password or "",
                                 database=u.path.lstrip("/"), autocommit=True, read_timeout=3600)
            h = u.path.lstrip("/").split("/", 1)[-1]
            cur = cn.cursor()
            cur.execute("select commit_hash, date from dolt_log where commit_hash = %s", (h,))
            head = cur.fetchone() or (h, None)
        else:
            dolt, clone, public_pq, out = args[:4]
            port = int(args[5]) if len(args) > 5 and args[4] == "--port" else 13307
            srv = subprocess.Popen(["nice", "-n10", dolt, "sql-server", "-H", "127.0.0.1", "-P", str(port)], cwd=clone,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for _ in range(60):
                try:
                    cn = pymysql.connect(host="127.0.0.1", port=port, user="root", autocommit=True)
                    break
                except Exception:
                    time.sleep(1)
            cur = cn.cursor()
            cur.execute("show databases")
            db = [d for (d,) in cur.fetchall() if d not in ("information_schema", "mysql")][0]
            cur.execute(f"use `{db}`")
            cur.execute("select dolt_hashof('HEAD'), max(date) from dolt_log")
            head = cur.fetchone()
        log(f"HEAD {head}")
        run(SqlSource(cn), public_pq, out, base=base, full=full, head=head)
    finally:
        if srv:
            srv.terminate()


if __name__ == "__main__":
    main()
