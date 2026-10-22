"""History shards + core columns from the DATA builder's parquets (uploads / metrics / summary).

usage (standalone): build_history.py <history_dir> <world_order.parquet> <out_data_dir>
build_index.py calls `core_columns()` (last_upload, uploads_90d) and `write_shards()` when --history is given.

Per-world record (before gzip; shards are consecutive world ordinals, shard k = ordinals starts[k]..starts[k+1]-1):
  flags u8 (bit0 uploads, bit1 metrics)
  uploads: v events(true total; one event = package rows of a world chained within 10 minutes, any platform) v firstDay v nOld [v m0, nOld x (v events, v pcKB, v questKB)] v nKept nKept x (v dDay, u8 mask, nPresent x v sizeKB+1)
           mask: bits0-3 = platforms present (pc, quest, ios, other), bit4 = every row of the event deleted; sizes follow in platform order (0 = size unknown)
           events beyond CAP_ALL keep only the newest KEEP_RECENT individually; older events become per-month bins (event count + last known size per platform)
  metrics: v snapsTotal v n n x (v dDay, zigzag dVisits, zigzag dFav)   (<= MAX_PTS points, one per day, thinned evenly)
Days are days since 2010-01-01 (same as core.bin created/updated); months are months since 2010-01.
"""

from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

EPOCH = np.datetime64("2010-01-01", "D")
CAP_ALL = 200
KEEP_RECENT = 150
MAX_PTS = 90
SHARD_GZ_TARGET = 100_000
PLAT = {"pc": 0, "quest": 1, "ios": 2, "other": 3}


def days(ts: pa.ChunkedArray) -> np.ndarray:
    a = ts.cast(pa.timestamp("s"), safe=False).to_numpy(zero_copy_only=False)
    d = (a.astype("datetime64[D]") - EPOCH).astype(np.int64)
    return np.where(np.isnat(a), -1, np.clip(d, 0, 65535))


def core_columns(history_dir: str, ids: list[str]) -> tuple[np.ndarray, np.ndarray]:
    """last_upload (u16 days, 0 = none) and uploads_90d (u16) in `ids` order"""
    t = pq.read_table(Path(history_dir) / "summary.parquet")
    pos = {w: i for i, w in enumerate(t["world_id"].to_pylist())}
    lu = days(t["last_upload"])
    u90 = np.minimum(np.array(t["uploads_90d"].to_numpy(zero_copy_only=False), dtype=np.int64), 65535)
    last = np.zeros(len(ids), dtype=np.uint16)
    n90 = np.zeros(len(ids), dtype=np.uint16)
    for i, w in enumerate(ids):
        j = pos.get(w)
        if j is not None and lu[j] >= 0:
            last[i], n90[i] = lu[j], u90[j]
    return last, n90


def vi(out: bytearray, n: int) -> None:
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)


def zz(out: bytearray, n: int) -> None:
    vi(out, (n << 1) if n >= 0 else ((-n << 1) - 1))


M_EPOCH = int(EPOCH.astype("datetime64[M]").astype(np.int64))  # 480 (months from 1970 to 2010-01)


def month_idx(days_arr: np.ndarray) -> np.ndarray:
    m = (EPOCH + days_arr.astype("timedelta64[D]")).astype("datetime64[M]").astype(np.int64)
    return m - M_EPOCH


def enc_world(up: dict | None, mt: dict | None, summ: tuple | None) -> bytes:
    out = bytearray()
    out.append((1 if up else 0) | (2 if mt else 0))
    if up:
        day, mask, dele, kb = up["day"], up["mask"], up["del"], up["kb"]
        n = len(day)
        vi(out, n)
        vi(out, int(day[0]))
        keep = n if n <= CAP_ALL else KEEP_RECENT
        old = n - keep
        if old:
            mo = month_idx(day[:old])
            m0 = int(mo[0])
            span = int(mo[-1]) - m0 + 1
            cnt = np.bincount(mo - m0, minlength=span)
            last = [[0] * span, [0] * span]
            for k in range(old):
                for p in (0, 1):
                    if kb[p][k] > 0:
                        last[p][int(mo[k]) - m0] = int(kb[p][k])
            vi(out, span)
            vi(out, m0)
            for k in range(span):
                vi(out, int(cnt[k]))
                vi(out, last[0][k])
                vi(out, last[1][k])
        else:
            vi(out, 0)
        vi(out, keep)
        prev = int(day[old])
        vi(out, prev)
        for k in range(old, n):
            vi(out, int(day[k]) - prev)
            prev = int(day[k])
            m = int(mask[k]) | (16 if dele[k] else 0)
            out.append(m)
            for p in range(4):
                if m & (1 << p):
                    vi(out, int(kb[p][k]))
    if mt:
        day, vis, fav = mt["day"], mt["vis"], mt["fav"]
        vi(out, mt["total"])
        n = len(day)
        vi(out, n)
        pd = pv = pf = 0
        for k in range(n):
            vi(out, int(day[k]) - pd)
            zz(out, int(vis[k]) - pv)
            zz(out, int(fav[k]) - pf)
            pd, pv, pf = int(day[k]), int(vis[k]), int(fav[k])
    return bytes(out)


def summ_ev(sm: dict, w: str) -> int:
    return int(sm[w][0]) if w in sm else -1


GAP_S = 600


def collapse(day: np.ndarray, t: np.ndarray, plat: np.ndarray, size: np.ndarray, dele: np.ndarray) -> dict:
    """package rows (sorted by time) -> upload events: consecutive rows <= 10 min apart (any platform) are one event"""
    new = np.concatenate([[True], np.diff(t) > GAP_S])
    ev = np.cumsum(new) - 1
    n = int(ev[-1]) + 1
    first = np.flatnonzero(new)
    mask = np.zeros(n, dtype=np.uint8)
    alld = np.ones(n, dtype=bool)
    kb = [np.zeros(n, dtype=np.int64) for _ in range(4)]
    for i in range(len(t)):
        e = ev[i]
        p = int(plat[i]) & 3
        mask[e] |= 1 << p
        if not dele[i]:
            alld[e] = False
        if size[i] > 0:
            kb[p][e] = max(kb[p][e], int(size[i]) // 1024 + 1)
    return {"day": day[first], "mask": mask, "del": alld, "kb": kb}


def group(ids: np.ndarray):
    """world id array -> dict world_id -> (start, end) over a sorted copy; returns (order, uniq, starts)"""
    order = np.argsort(ids, kind="stable")
    s = ids[order]
    uniq, st = np.unique(s, return_index=True)
    en = np.append(st[1:], len(s))
    return order, {u: (a, b) for u, a, b in zip(uniq.tolist(), st.tolist(), en.tolist())}


def write_shards(history_dir: str, ids: list[str], out_root: Path, writer=None) -> dict:
    hd = Path(history_dir)
    ut = pq.read_table(hd / "uploads.parquet")
    u_id = np.array(ut["world_id"].to_pylist(), dtype=object)
    u_t = ut["created_at"].cast(pa.timestamp("s"), safe=False).to_numpy(zero_copy_only=False).astype("int64")
    u_day = days(ut["created_at"])
    u_plat = np.array([PLAT.get(p, 3) for p in ut["platform"].to_pylist()], dtype=np.int8)
    u_size = np.array([-1 if x is None else x for x in ut["size_bytes"].to_pylist()], dtype=np.int64)
    u_del = np.array(ut["deleted"].fill_null(False).to_numpy(zero_copy_only=False), dtype=bool)
    ordr = np.lexsort((u_plat, u_t, u_id))
    u_id, u_t, u_day, u_plat, u_size, u_del = (a[ordr] for a in (u_id, u_t, u_day, u_plat, u_size, u_del))
    uniq, ust = np.unique(u_id, return_index=True)
    uen = np.append(ust[1:], len(u_id))
    ug = {w: (a, b) for w, a, b in zip(uniq.tolist(), ust.tolist(), uen.tolist())}

    mt = pq.read_table(hd / "metrics.parquet")
    m_id = np.array(mt["world_id"].to_pylist(), dtype=object)
    m_t = mt["t"].cast(pa.timestamp("s"), safe=False).to_numpy(zero_copy_only=False).astype("int64")
    m_day = days(mt["t"])
    m_vis = np.array([0 if x is None else x for x in mt["visits"].to_pylist()], dtype=np.int64)
    m_fav = np.array([0 if x is None else x for x in mt["favorites"].to_pylist()], dtype=np.int64)
    mo = np.lexsort((m_t, m_id))
    m_id, m_day, m_vis, m_fav = (a[mo] for a in (m_id, m_day, m_vis, m_fav))
    muniq, mst = np.unique(m_id, return_index=True)
    men = np.append(mst[1:], len(m_id))
    mg = {w: (a, b) for w, a, b in zip(muniq.tolist(), mst.tolist(), men.tolist())}

    st = pq.read_table(hd / "summary.parquet")
    sm = {w: (a,) for w, a in zip(st["world_id"].to_pylist(), st["uploads_total"].to_pylist())}

    recs: list[bytes] = []
    stats = {"uploads_rows": [], "uploads_capped": 0, "rows_raw": 0, "event_mismatch": 0}
    for w in ids:
        up = mtd = None
        if w in ug:
            a, b = ug[w]
            if u_day[a:b].min() >= 0:
                dd = u_day[a:b]
            else:
                dd = np.maximum(u_day[a:b], 0)
            up = collapse(dd, u_t[a:b], u_plat[a:b], u_size[a:b], u_del[a:b])
            stats["uploads_rows"].append(len(up["day"]))
            stats["rows_raw"] += b - a
            if summ_ev(sm, w) != len(up["day"]):
                stats["event_mismatch"] += 1
            if len(up["day"]) > CAP_ALL:
                stats["uploads_capped"] += 1
        if w in mg:
            a, b = mg[w]
            d, v, f = m_day[a:b], m_vis[a:b], m_fav[a:b]
            keep = np.flatnonzero(np.append(d[1:] != d[:-1], True) & (d >= 0))  # last snapshot of each day
            total = b - a
            if len(keep) > MAX_PTS:
                keep = keep[np.unique(np.linspace(0, len(keep) - 1, MAX_PTS).round().astype(int))]
            if len(keep):
                mtd = {"day": d[keep], "vis": v[keep], "fav": f[keep], "total": total}
        recs.append(enc_world(up, mtd, sm.get(w)) if (up or mtd) else b"\x00")

    # pack consecutive ordinals into shards of <= SHARD_GZ_TARGET gzip bytes
    shards: list[tuple[int, bytes]] = []

    def blob(lo: int, hi: int) -> bytes:
        head = bytearray()
        for r in recs[lo:hi]:
            vi(head, len(r))
        return gzip.compress(bytes(head) + b"".join(recs[lo:hi]), 9, mtime=0)

    def emit(lo: int, hi: int) -> None:
        data = blob(lo, hi)
        if len(data) > SHARD_GZ_TARGET and hi - lo > 1:
            mid = (lo + hi) // 2
            emit(lo, mid)
            emit(mid, hi)
        else:
            shards.append((lo, data))

    n = len(recs)
    lo, rawsum, ratio = 0, 0, 0.35
    for i in range(n):
        rawsum += len(recs[i])
        if rawsum * ratio >= SHARD_GZ_TARGET * 0.95 or i == n - 1:
            hi = i + 1
            data_len_before = len(shards)
            emit(lo, hi)
            tot_gz = sum(len(d) for _, d in shards[data_len_before:])
            ratio = max(0.05, min(0.8, tot_gz / max(1, rawsum)))
            lo, rawsum = hi, 0
    sizes = [len(d) for _, d in shards]
    for k, (_, d) in enumerate(shards):
        p = out_root / "hist" / f"{k}.bin"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(d)
        if writer is not None:
            writer.files[f"hist/{k}.bin"] = (0, len(d))
    rows = np.array(stats["uploads_rows"] or [0])
    info = {
        "shards": len(shards),
        "starts": [s for s, _ in shards],
        "capAll": CAP_ALL,
        "keepRecent": KEEP_RECENT,
        "maxPts": MAX_PTS,
        "metricsFrom": "2025-06",
    }
    summary = {
        "worlds_with_uploads": int(len(rows)),
        "rows_raw": stats["rows_raw"], "event_mismatch_vs_summary": stats["event_mismatch"], "events_p50": float(np.percentile(rows, 50)),
        "events_p99": float(np.percentile(rows, 99)),
        "events_max": int(rows.max()),
        "worlds_capped": stats["uploads_capped"],
        "shard_bytes_max": max(sizes),
        "shard_bytes_median": int(np.median(sizes)),
        "shard_bytes_total": sum(sizes),
        "shard_count": len(shards),
        "largest_record_raw": max(len(r) for r in recs),
    }
    return {"info": info, "stats": summary}


if __name__ == "__main__":
    hd, order, out = sys.argv[1:4]
    ids = pq.read_table(order)["world_id"].to_pylist()
    r = write_shards(hd, ids, Path(out))
    print(json.dumps(r["stats"], indent=1))
