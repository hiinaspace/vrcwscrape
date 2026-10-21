"""Synthetic round-6 history for the fake site: uploads/metrics/summary parquets in ./history (git-ignored),
plus history.json (lazily fetched on panel open) and recent.json (eager id -> [last_upload_ms, uploads_90d]).
usage: uv run python make_history.py sample.parquet ; then VRCW_HISTORY_DIR=$PWD/history for run.py."""
import csv, json, random, sys
from datetime import datetime, timedelta
from pathlib import Path
import duckdb

ASOF = datetime(2026, 10, 20, 12, 0)
out = Path("history"); out.mkdir(exist_ok=True)
con = duckdb.connect()
ids = [r[0] for r in con.execute(f"select world_id from read_parquet(\x27{sys.argv[1]}\x27)").fetchall()]
rnd = random.Random(5)
up, mt, summ, hist, recent = [], [], [], {}, {}
for k, wid in enumerate(ids):
    n = 1 + int(rnd.paretovariate(0.9)) % 400 if rnd.random() < 0.6 else 1 + rnd.randint(0, 3)
    if k < 3: n = 300 + 40 * k
    t = ASOF - timedelta(days=rnd.randint(0, 1500))
    evs = []
    for _ in range(n):
        evs.append(t); t -= timedelta(hours=1, days=rnd.random() * (3 if n > 50 else 60))
    evs.sort()
    for e in evs:
        plats = ["pc"] + (["quest"] if rnd.random() < 0.4 else [])
        for p in plats:
            c = e + timedelta(minutes=rnd.randint(0, 6) if p == "quest" else 0)
            up.append((wid, p, "2022.3.22f1", len(up), c.isoformat(sep=" "), rnd.randint(10**6, 10**8), False))
    evs = [e for e in evs if e <= ASOF]
    for j in range(rnd.choice([0, 0, 3, 12])):
        mt.append((wid, (datetime(2025, 6, 1) + timedelta(days=j * 20)).isoformat(sep=" "), 100 * j + rnd.randint(0, 50), j, ""))
    hist[wid] = {"events": [e.isoformat() for e in evs]}
    recent[wid] = [int(evs[-1].timestamp() * 1000), sum(1 for e in evs if (ASOF - e).days < 90)] if evs else [0, 0]
    summ.append((wid, evs[0], evs[-1], len(evs), sum(1 for e in evs if (ASOF - e).days < 30), sum(1 for e in evs if (ASOF - e).days < 90), sum(1 for e in evs if (ASOF - e).days < 365)))
def w(name, rows, cols):
    with open(out / (name + ".csv"), "w", newline="") as f:
        csv.writer(f).writerows(rows)
    con.execute(f"copy (select * from read_csv(\x27{out}/{name}.csv\x27, header=false, columns={cols}, nullstr=\x27\x27)) to \x27{out}/{name}.parquet\x27 (format parquet)")
    (out / (name + ".csv")).unlink()
w("uploads", up, "{\x27world_id\x27:\x27VARCHAR\x27,\x27platform\x27:\x27VARCHAR\x27,\x27unity_version\x27:\x27VARCHAR\x27,\x27version\x27:\x27INT\x27,\x27created_at\x27:\x27TIMESTAMP\x27,\x27size_bytes\x27:\x27BIGINT\x27,\x27deleted\x27:\x27BOOLEAN\x27}")
w("metrics", [(a, b, c, d, "") for a, b, c, d, _ in mt], "{\x27world_id\x27:\x27VARCHAR\x27,\x27t\x27:\x27TIMESTAMP\x27,\x27visits\x27:\x27INT\x27,\x27favorites\x27:\x27INT\x27,\x27occupants\x27:\x27INT\x27}")
w("summary", [(a, b.isoformat(sep=" "), c.isoformat(sep=" "), *r) for a, b, c, *r in summ], "{\x27world_id\x27:\x27VARCHAR\x27,\x27first_upload\x27:\x27TIMESTAMP\x27,\x27last_upload\x27:\x27TIMESTAMP\x27,\x27uploads_total\x27:\x27INT\x27,\x27uploads_30d\x27:\x27INT\x27,\x27uploads_90d\x27:\x27INT\x27,\x27uploads_365d\x27:\x27INT\x27}")
(out / "asof.txt").write_text("as_of: 2026-10-20\n")
json.dump({wid: {"events": h["events"], "spark": [m[2] for m in mt if m[0] == wid]} for wid, h in hist.items()}, open("history.json", "w"))
json.dump(recent, open("recent.json", "w"))
print(len(ids), "worlds", len(up), "upload rows")
