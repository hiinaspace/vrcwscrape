"""parquet -> static search assets.

usage: build_index.py <worlds.parquet> <out_data_dir> [--work DIR]
Writes <out_data_dir>/{manifest.json,core.bin,tags_core.bin,tags_all.bin,tagidx/,fts/,meta/,card/,authors/,ids/}
Every payload is gzip'd (magic-sniffed by the client; extension .bin regardless).
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import time
from array import array
from collections import defaultdict
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import tok

EPOCH = np.datetime64("2010-01-01", "D")
N_FTS_SHARDS = 192
META_CHUNK = 32
CARD_CHUNK = 64
TAG_SHARDS = 64
STOP_FRAC = 0.05  # desc-only stopword threshold (fraction of docs)
MIN_CORE_TAG = 20
N_FLAG_TAGS = 8
DESC_INDEX_CHARS = 1000
DESC_STORE_CHARS = 700
ROW_DESC = 160


class Out:
    def __init__(self, root: Path):
        self.root = root
        self.files: dict[str, tuple[int, int]] = {}

    def write(self, rel: str, raw: bytes, gz: bool = True) -> None:
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        data = gzip.compress(raw, 9, mtime=0) if gz else raw
        p.write_bytes(data)
        self.files[rel] = (len(raw), len(data))

    def json(self, rel: str, obj, gz: bool = True) -> None:
        self.write(rel, json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode(), gz)


def varint_vec(v: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    v = v.astype(np.uint64)
    nb = np.ones(len(v), dtype=np.int64)
    for k in range(1, 5):
        nb += v >= (1 << (7 * k))
    pos = np.cumsum(nb) - nb
    out = np.empty(int(nb.sum()), dtype=np.uint8)
    for k in range(5):
        sel = nb > k
        if not sel.any():
            break
        b = ((v[sel] >> np.uint64(7 * k)) & np.uint64(0x7F)).astype(np.uint8)
        b |= np.where(nb[sel] > k + 1, 0x80, 0).astype(np.uint8)
        out[pos[sel] + k] = b
    return out, pos


def varint(n: int) -> bytes:
    out = bytearray()
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def planes(a: np.ndarray) -> bytes:
    """byte-plane shuffle (little endian) to help gzip"""
    a = np.ascontiguousarray(a.astype("<" + a.dtype.str[1:]))
    return a.view(np.uint8).reshape(len(a), a.dtype.itemsize).T.tobytes()


def load(path: str) -> dict:
    t = pq.read_table(path)
    cols = set(t.column_names)
    if "scrape_status" in cols:
        t = t.filter(pa.compute.equal(t["scrape_status"], "SUCCESS"))
    n = t.num_rows

    def col(name, default=None):
        if name in cols:
            return t[name].to_pylist()
        return [default] * n

    def num(name):
        return np.array([x if x is not None else 0 for x in col(name)], dtype=np.int64)

    d: dict = {"n": n}
    d["id"] = col("world_id")
    d["name"] = [x or "" for x in col("name", "")]
    d["desc"] = [x or "" for x in col("description", "")]
    d["author_id"] = [x or "" for x in col("author_id", "")]
    d["author_name"] = [x or "" for x in col("author_name", "")]
    d["capacity"] = np.clip(num("capacity"), 0, 255)
    d["visits"] = num("visits")
    d["fav"] = num("favorites")
    d["heat"] = num("heat")
    d["pop"] = num("popularity")
    for k, c in (("pc", "pc_size_bytes"), ("quest", "quest_size_bytes")):
        if c in cols:
            vals = t[c].to_pylist()
        elif k + "_size_mb" in cols:
            vals = [None if x is None else int(x * 1048576) for x in t[k + "_size_mb"].to_pylist()]
        else:
            vals = [None] * n
        d[k] = np.array([0xFFFFFFFF if (x is None or x <= 0) else min(-(-x // 1024), 0xFFFFFFFE) for x in vals], dtype=np.int64)
        d[k + "_lo"] = np.array([0 if (x is None or x <= 0) else (x - 1) % 1024 for x in vals], dtype=np.int64)
    for k, c in (("created", "created_at"), ("updated", "updated_at")):
        if c in cols:
            arr = t[c].cast(pa.timestamp("s"), safe=False).to_numpy(zero_copy_only=False)
            days = (arr.astype("datetime64[D]") - EPOCH).astype(np.int64)
            days = np.where(np.isnat(arr), 0, days)
            d[k] = np.clip(days, 0, 65535)
        else:
            d[k] = np.zeros(n, dtype=np.int64)
    d["tags"] = [list(x) if x else [] for x in col("tags", [])]
    # thumbnails are self-hosted (build/pack_thumbs.py); the VRChat CDN URLs in the parquet are never emitted
    d["thumb"] = None
    d["recent"] = None
    if "recent_visits_per_day" in cols:  # v2 parquet: recent visits/day (null = unknown), basis metrics|popularity_fallback
        rv = t["recent_visits_per_day"].to_pylist()
        d["recent"] = np.array([np.nan if x is None else x for x in rv], dtype=np.float64)
        d["recent_metrics"] = np.array(
            [b == "metrics" and x is not None for b, x in zip(col("recent_basis", "metrics"), rv)], dtype=bool
        )
        wd = [x for x in col("recent_window_days") if x is not None]
        d["recent_window"] = float(np.median(wd)) if wd else None
    if "platforms" in cols:  # fresh export: list of platform strings
        pl = t["platforms"].to_pylist()
        hasq = np.array([any("android" in str(p).lower() or "quest" in str(p).lower() for p in (x or [])) for x in pl])
        hasp = np.array([any("windows" in str(p).lower() or "pc" in str(p).lower() for p in (x or [])) for x in pl])
        d["has_pc"], d["has_quest"] = hasp, hasq
    else:
        d["has_pc"], d["has_quest"] = d["pc"] != 0xFFFFFFFF, d["quest"] != 0xFFFFFFFF
    return d


HIDDEN_PREFIXES = ("admin_", "adm_", "feature_", "debug_")


def tag_class(raw: str) -> str:
    """hidden | status | cw | user (mirrored in web/src/engine.ts)"""
    if raw in ("system_approved", "system_labs"):
        return "status"
    if raw.startswith(HIDDEN_PREFIXES) or raw.startswith("system_"):
        return "hidden"
    if raw.startswith("content_"):
        return "cw"
    return "user"


def canon_tag(raw: str) -> str:
    if raw.startswith("author_tag_"):
        return raw[len("author_tag_") :]
    return raw


TREND_SCALE = 1024  # u16 steps per decade of (1 + recent visits/day)


def trend_column(d: dict) -> tuple[np.ndarray, dict]:
    """u16 trending key per world (already in popularity order) + manifest info.

    v2 parquet: round(1024 * log10(1 + recent_visits_per_day)); rows without a measurement (basis popularity_fallback)
    get the median recent rate of measured worlds with the same VRChat `popularity` value (so they interleave sensibly).
    Without recent columns (old parquet): popularity-only key, documented as popularity_fallback.
    """
    pop = d["pop"].astype(np.int64)
    n = len(pop)
    if d.get("recent") is None:
        key = np.clip(pop * 5000 + d["heat"].astype(np.int64) * 500, 0, 65535)
        return key.astype(np.uint16), {"basis": "popularity_fallback", "windowDays": None, "scale": 0, "measured": 0}
    rec = d["recent"].copy()
    meas = d["recent_metrics"] & ~np.isnan(rec)
    med = {}
    for pv in np.unique(pop):
        sel = meas & (pop == pv)
        if sel.sum() >= 20:
            med[int(pv)] = float(np.median(rec[sel]))
    overall = float(np.median(rec[meas])) if meas.any() else 0.0
    fill = np.array([med.get(int(pv), overall) for pv in pop])
    v = np.where(meas, rec, fill)
    key = np.clip(np.round(TREND_SCALE * np.log10(1.0 + np.maximum(v, 0))), 0, 65535)
    return key.astype(np.uint16), {
        "basis": "recent_visits_per_day",
        "windowDays": d.get("recent_window"),
        "scale": TREND_SCALE,
        "measured": int(meas.sum()),
        "fallbackRows": int(n - meas.sum()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("parquet")
    ap.add_argument("out")
    ap.add_argument("--work", default=None)
    a = ap.parse_args()
    T0 = time.time()
    out = Out(Path(a.out))
    work = Path(a.work) if a.work else Path(a.out).parent / "work"
    work.mkdir(parents=True, exist_ok=True)

    d = load(a.parquet)
    n = d["n"]
    # popularity order: visits desc, favorites desc, id
    order = sorted(range(n), key=lambda i: (-int(d["visits"][i]), -int(d["fav"][i]), d["id"][i]))
    order = np.array(order, dtype=np.int64)
    for k, v in list(d.items()):
        if isinstance(v, np.ndarray):
            d[k] = v[order]
        elif isinstance(v, list):
            d[k] = [v[i] for i in order]
    ids_sorted = d["id"]
    pq.write_table(pa.table({"world_id": ids_sorted}), work / "world_order.parquet")
    print(f"loaded {n} worlds in {time.time()-T0:.1f}s")

    # ---- tags
    cnt: dict[str, int] = defaultdict(int)
    canon_lists = []
    for tl in d["tags"]:
        cs = sorted({canon_tag(x) for x in tl})
        canon_lists.append(cs)
        for c in cs:
            cnt[c] += 1
    all_tags = sorted(cnt.items(), key=lambda kv: (-kv[1], kv[0]))
    min_core = max(MIN_CORE_TAG, n // 5000)
    core = [(k, c) for k, c in all_tags if c >= min_core][:65000]
    core_id = {k: i for i, (k, _) in enumerate(core)}
    # UI-only tag groups: case-fold + simple plural merge (facetCounts.tags / tag filters stay raw for the oracle contract)
    lower_set = {k.lower() for k, _ in all_tags}

    def gkey(k: str) -> str:
        lo = k.lower()
        return lo[:-1] if len(lo) >= 4 and lo.endswith("s") and lo[:-1] in lower_set else lo

    grp = defaultdict(list)
    for k, _ in core:
        if tag_class(k) == "user":
            grp[gkey(k)].append(k)
    tag_groups = {g: v for g, v in grp.items() if len(v) > 1}
    tagcnt = np.zeros(n, dtype=np.uint8)
    tagids = array("H")
    flags = np.zeros(n, dtype=np.uint8)
    all_id = {k: i for i, (k, _) in enumerate(all_tags)}
    rare_cnt = np.zeros(n, dtype=np.uint8)
    rare_ids = array('H')
    for i, cs in enumerate(canon_lists):
        k = 0
        for c in cs:
            if c in core_id:
                ci = core_id[c]
                if ci < N_FLAG_TAGS:
                    flags[i] |= 1 << ci
                else:
                    tagids.append(ci)
                    k += 1
            else:
                rare_ids.append(all_id[c])
                rare_cnt[i] += 1
        tagcnt[i] = min(k, 255)
    out.json("tags_core.bin", [[k, c] for k, c in core])
    out.json("tags_all.bin", [[k, c] for k, c in all_tags])
    out.write("tags_rare.bin", rare_cnt.tobytes() + planes(np.frombuffer(rare_ids, dtype=np.uint16)))

    # ---- core columns
    sections = []
    blob = bytearray()

    def add(name, arr, dtype, kind="plain"):
        nonlocal blob
        arr = np.asarray(arr).astype(dtype)
        raw = arr.tobytes() if arr.dtype.itemsize == 1 else planes(arr)
        sections.append({"name": name, "dtype": np.dtype(dtype).name, "len": len(arr), "off": len(blob), "bytes": len(raw), "kind": kind})
        blob += raw

    # exact byte sizes for fractional size_max_mb: bytes = (K-1)*1024 + lo + 1 where K = stored KiB (ceil); lazy file
    out.write("sizelo.bin", planes(d["pc_lo"].astype(np.uint16)) + planes(d["quest_lo"].astype(np.uint16)))
    add("cap", d["capacity"], np.uint8)
    add("pc", d["pc"], np.uint32)
    add("quest", d["quest"], np.uint32)
    add("created", d["created"], np.uint16)
    add("updated", d["updated"], np.uint16)
    vis = d["visits"]
    dv = np.concatenate([[vis[0]], vis[:-1] - vis[1:]]) if n else vis
    add("visits_delta", dv, np.uint32, "delta_desc")
    add("fav", np.minimum(d["fav"], 0xFFFFFFFF), np.uint32)
    trend_key, trend_info = trend_column(d)
    add("trend", trend_key, np.uint16)
    add("plat", d["has_pc"].astype(np.uint8) | (d["has_quest"].astype(np.uint8) << 1), np.uint8)
    add("flags", flags, np.uint8)
    add("tagcnt", tagcnt, np.uint8)
    add("tagids", np.frombuffer(tagids, dtype=np.uint16), np.uint16)
    out.write("core.bin", bytes(blob))

    # ---- full text
    t1 = time.time()
    tid: dict[str, int] = {}
    A_t, A_d, A_m = array("I"), array("I"), array("B")

    def emit(term: str, doc: int, mask: int) -> None:
        i = tid.get(term)
        if i is None:
            i = tid[term] = len(tid)
        A_t.append(i)
        A_d.append(doc)
        A_m.append(mask)

    for i in range(n):
        for tm in tok.tokens(d["name"][i], True):
            emit(tm, i, tok.F_TITLE)
        tk = tok.title_key(d["name"][i])
        if tk:
            emit(tok.TITLE_MARK + tk, i, tok.F_TITLE)
        for tm in tok.tokens(d["author_name"][i], True):
            emit(tm, i, tok.F_AUTHOR)
        ak = tok.title_key(d["author_name"][i])
        if ak:
            emit(tok.AUTHOR_MARK + ak, i, tok.F_AUTHOR)
        pcs = tok.pieces(d["name"][i])
        for a_, b_ in zip(pcs, pcs[1:]):
            emit(tok.PAIR_MARK + a_ + " " + b_, i, tok.F_TITLE)
        for c in canon_lists[i]:
            cl = tag_class(c)
            if cl in ("hidden", "status"):
                continue
            for tm in tok.tokens(c, True):
                emit(tm, i, tok.F_TAG)
            if cl == "user":
                tk_ = tok.title_key(c)
                if tk_:
                    emit(tok.TAG_MARK + tk_, i, tok.F_TAG)
        for tm in tok.tokens(d["desc"][i][:DESC_INDEX_CHARS], False):
            emit(tm, i, tok.F_DESC)
    print(f"tokenized: {len(tid)} terms, {len(A_t)} raw postings, {time.time()-t1:.1f}s")

    T = np.frombuffer(A_t, dtype=np.uint32).astype(np.uint64)
    D = np.frombuffer(A_d, dtype=np.uint32)
    M = np.frombuffer(A_m, dtype=np.uint8)
    key = (T << np.uint64(32)) | D.astype(np.uint64)
    o = np.argsort(key, kind="stable")
    key, M = key[o], M[o]
    del T, D, o
    starts = np.concatenate([[True], key[1:] != key[:-1]]) if len(key) else np.zeros(0, bool)
    idx = np.flatnonzero(starts)
    M = np.bitwise_or.reduceat(M, idx)
    key = key[idx]
    T = (key >> np.uint64(32)).astype(np.int64)
    D = (key & np.uint64(0xFFFFFFFF)).astype(np.int64)
    del key
    nterms = len(tid)
    # stopwords: terms frequent in descriptions
    dfd = np.bincount(T[(M & 8) > 0], minlength=nterms)
    stop_ids = np.flatnonzero(dfd > STOP_FRAC * n)
    isstop = np.zeros(nterms, dtype=bool)
    isstop[stop_ids] = True
    M = np.where(isstop[T], M & ~np.uint8(8), M)
    keep = M > 0
    T, D, M = T[keep], D[keep], M[keep]
    terms = [None] * nterms
    for s, i in tid.items():
        terms[i] = s
    stop_terms = sorted(terms[i] for i in stop_ids)
    print(f"stopwords (desc-dropped): {len(stop_terms)}; postings kept {len(T)}")
    # group boundaries (T is sorted ascending already)
    gstart = np.flatnonzero(np.concatenate([[True], T[1:] != T[:-1]]))
    gend = np.concatenate([gstart[1:], [len(T)]])
    gterm = T[gstart]
    delta = D.copy()
    delta[1:] -= D[:-1]
    delta[gstart] = D[gstart]
    vals = (delta.astype(np.uint64) << np.uint64(4)) | M.astype(np.uint64)
    vb, vpos = varint_vec(vals)
    vpos = np.append(vpos, len(vb))
    per_shard = defaultdict(list)
    for gi, ti in enumerate(gterm):
        per_shard[tok.shard_of(terms[ti], N_FTS_SHARDS)].append(gi)
    fts_sizes = []
    for s in range(N_FTS_SHARDS):
        gis = per_shard.get(s, [])
        gis.sort(key=lambda g: tok.u16_key(terms[gterm[g]]))
        tb = [terms[gterm[g]].encode() for g in gis]
        hdr = bytearray(varint(len(gis)))
        for b in tb:
            hdr += varint(len(b))
        for b in tb:
            hdr += b
        for g in gis:
            hdr += varint(int(gend[g] - gstart[g]))
        for g in gis:
            hdr += varint(int(vpos[gend[g]] - vpos[gstart[g]]))
        body = b"".join(vb[vpos[gstart[g]] : vpos[gend[g]]].tobytes() for g in gis)
        out.write(f"fts/{s}.bin", bytes(hdr) + body)
        fts_sizes.append(len(gis))
    print(f"fts built in {time.time()-t1:.1f}s")

    # ---- meta + card chunks
    # no hotlinking: thumbnails come from pack_thumbs.py (manifest.thumbs)

    def strip(s, p):
        return s[len(p) :] if s.startswith(p) else s

    for k in range(0, n, META_CHUNK):
        r = range(k, min(n, k + META_CHUNK))
        m = {
            "id": [strip(ids_sorted[i], "wrld_") for i in r],
            "n": [d["name"][i] for i in r],
            "an": [d["author_name"][i] for i in r],
            "au": [strip(d["author_id"][i], "usr_") for i in r],
            # row-sized card data: first 160 chars of the description + visible tags, so a result row needs no card chunk
            "d": [d["desc"][i][:ROW_DESC].replace("\n", " ") for i in r],
            "t": [[x for x in canon_lists[i] if tag_class(x) != "hidden"] for i in r],
        }
        out.json(f"meta/{k // META_CHUNK}.bin", m)
    for k in range(0, n, CARD_CHUNK):
        r = range(k, min(n, k + CARD_CHUNK))
        out.json(
            f"card/{k // CARD_CHUNK}.bin",
            {"d": [d["desc"][i][:DESC_STORE_CHARS] for i in r], "t": [canon_lists[i] for i in r], "hp": [[int(d["heat"][i]), int(d["pop"][i])] for i in r]},
        )

    # ---- author + id shards
    au = defaultdict(lambda: defaultdict(list))
    ids = defaultdict(dict)
    for i in range(n):
        a_ = strip(d["author_id"][i], "usr_")
        au[a_[:2]][a_].append(i)
        w = strip(ids_sorted[i], "wrld_")
        ids[w[:2]][w] = i
    for s, m in au.items():
        out.json(f"authors/{s}.bin", m)
    for s, m in ids.items():
        out.json(f"ids/{s}.bin", m)

    manifest = {
        "version": int(time.time()),
        "n": n,
        "epoch": "2010-01-01",
        "core": {"file": "core.bin", "columns": sections},
        "ftsShards": N_FTS_SHARDS,
        "stop": stop_terms,
        "metaChunk": META_CHUNK,
        "cardChunk": CARD_CHUNK,
        "nCoreTags": len(core),
        "minCoreTag": min_core,
        "flagTags": N_FLAG_TAGS,
        "sizeUnit": 1024,
        "thumb": None,
        "hasThumb": False,
        "thumbs": None,  # filled in by pack_thumbs.py
        "trend": trend_info,
        "tagGroups": tag_groups,
        "cw": [[k, c] for k, c in all_tags if tag_class(k) == "cw"],
        "date_max": int(max(d["updated"].max(), d["created"].max())) if n else 0,
    }
    out.json("manifest.json", manifest, gz=False)
    stats = {
        "worlds": n,
        "build_seconds": round(time.time() - T0, 1),
        "total_bytes": sum(v[1] for v in out.files.values()),
        "largest": max(out.files.items(), key=lambda kv: kv[1][1]),
        "by_dir": {},
    }
    for rel, (raw, gz) in out.files.items():
        top = rel.split("/")[0]
        e = stats["by_dir"].setdefault(top, {"files": 0, "gz": 0, "raw": 0})
        e["files"] += 1
        e["gz"] += gz
        e["raw"] += raw
    (work / "index_stats.json").write_text(json.dumps(stats, indent=1))
    print(json.dumps(stats, indent=1))


if __name__ == "__main__":
    main()
