#!/usr/bin/env python3
"""Automatic gate before a deploy. Any failure -> exit 1 (the refresh then does not deploy and the Job fails).

  gate.py --dist D --public PUBLIC.parquet --full FULL.parquet --excluded excluded_ids.txt --deployed-ids IDS.parquet
          --thumb-state STATE --live-url URL --search <checkout>/search [--deployed-json F] [--extract-dir STATE/extract] [--report out.json]
Run with the build venv (pyarrow, numpy). The browser oracle (search/oracle) is NOT part of the job; run it attended, see deploy/README.md.

Checks
  1. deploy/check.py preflight (Pages limits, no private / excluded ids, image hosts, stamps, gitleaks).
  2. web engine tests (web/test/run.mjs: engine + query) against dist/data, with the real packs.
  3. thumbnail pack integrity, independent of the builder: decode the new THM3 index and, for a sample of worlds, check blob id / length against the
     state's sha->blob table and that the bytes in the pack are an AVIF.
  4. week over week against the live manifest and the last deployed id list: public count +-3% per week since deployed.json's time (1..4 weeks),
     >=99% of last deployed public worlds retained unless they turned non-public, thumbnail coverage >= min(99%, live coverage - 0.5pp),
     neighbour coverage 100%, data as-of newer than live, manifest.snapshot == manifest.history.asof.
  5. extract drift (extract/current vs extract/previous): null/empty rate of the user-visible columns up by <= 2pp, < 25% of worlds with changed text.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import random
import subprocess
import sys
import urllib.request
from pathlib import Path

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "build"))
import baseline  # noqa: E402
import thumbstate  # noqa: E402

results: list[dict] = []


def rec(name: str, ok: bool, detail: str = "") -> bool:
    results.append({"check": name, "ok": ok, "detail": detail})
    print(f"  {'ok  ' if ok else 'FAIL'}  {name}: {detail}", flush=True)
    return ok


def planes(buf: bytes) -> np.ndarray:
    cnt = len(buf) // 4
    a = np.frombuffer(buf, dtype=np.uint8).reshape(4, cnt).astype(np.uint32)
    return a[0] | (a[1] << 8) | (a[2] << 16) | (a[3] << 24)


def gz(p: Path) -> bytes:
    b = p.read_bytes()
    return gzip.decompress(b) if b[:2] == b"\x1f\x8b" else b


def check_thumbs(dist: Path, public: Path, state: Path, man: dict) -> None:
    th = man.get("thumbs")
    if not th:
        rec("thumb integrity", False, "manifest.thumbs missing")
        return
    hdr = gz(dist / "data" / th["index"])
    if hdr[:4] != b"THM3":
        rec("thumb integrity", False, f"index magic {hdr[:4]!r}")
        return
    n, nb, npk, ws, bs = (int(x) for x in np.frombuffer(hdr[4:24], dtype="<u4"))
    packs = [tuple(int(x) for x in np.frombuffer(hdr[24 + 8 * k : 32 + 8 * k], dtype="<u4")) for k in range(npk)]
    nbs = -(-nb // bs)
    bsbase = np.frombuffer(hdr[24 + 8 * npk : 24 + 8 * npk + 4 * nbs], dtype="<u4")
    lens = np.concatenate([planes(gz(dist / "data/th" / f"b{k}.bin")) for k in range(nbs)]).astype(np.int64)
    # decode all world shards
    blob = np.full(n, -1, dtype=np.int64)
    for k in range(-(-n // ws)):
        c = planes(gz(dist / "data/th" / f"w{k}.bin")).astype(np.int64)
        prev = 0
        for i, v in enumerate(c.tolist()):
            if v == 0:
                continue
            z = v - 1
            prev += (z >> 1) ^ -(z & 1)
            blob[k * ws + i] = prev
    # worlds in index order -> sha
    order = [json.loads(gz(p)) for p in sorted((dist / "data/ids").glob("*.bin"))]
    ord_of = {}
    for d in order:
        ord_of.update({"wrld_" + k: v for k, v in d.items()})
    t = pq.read_table(public, columns=["world_id", "thumb_sha256"])
    sha_of = {w: s for w, s in zip(t["world_id"].to_pylist(), t["thumb_sha256"].to_pylist())}
    try:
        st = thumbstate.load(state)  # CURRENT generation, fully validated incl. pack hashes
    except thumbstate.StateError as e:
        rec("thumb integrity", False, f"thumb state invalid: {e}")
        return
    if st is None:
        rec("thumb integrity", False, f"no thumb state in {state}")
        return
    tab = st.known
    if [p["hash"] for p in st.packs] != list(th.get("hashes") or []):
        rec("thumb integrity", False, "manifest pack hashes != the state generation's packs.json")
        return
    bad = []
    ids = sorted(ord_of)
    random.Random(7).shuffle(ids)
    handles: dict[int, object] = {}
    checked = 0
    for w in ids[:3000]:
        o = ord_of[w]
        s = sha_of.get(w)
        b = int(blob[o])
        if s is None or s not in tab:
            if b != -1:
                bad.append((w, "has a blob but no sha/table entry"))
            continue
        tb, tl = tab[s]
        if b != tb or lens[b] != tl:
            bad.append((w, f"blob {b}/{lens[b]} != table {tb}/{tl}"))
            continue
        pk = max(i for i, (f, _) in enumerate(packs) if f <= b)
        # offset = prefix sum from max(first blob of pack, first blob of its blob shard)
        start = max(packs[pk][0], (b // bs) * bs)
        base = int(bsbase[b // bs]) if packs[pk][0] <= (b // bs) * bs else 0
        off = base + int(lens[start:b].sum())
        f = handles.get(pk) or handles.setdefault(pk, open(dist / "thumbs" / f"p-{pk:03d}.bin", "rb"))
        f.seek(off)
        data = f.read(int(lens[b]))
        if len(data) != lens[b] or data[4:12] not in (b"ftypavif", b"ftypavis"):
            bad.append((w, f"pack {pk} off {off} not an AVIF ({data[4:12]!r})"))
        else:
            checked += 1
    for f in handles.values():
        f.close()
    hashes = th.get("hashes") or []
    import hashlib

    hbad = [k for k in range(npk) if hashlib.sha256((dist / "thumbs" / f"p-{k:03d}.bin").read_bytes()).hexdigest()[:16] != hashes[k]] if len(hashes) == npk else ["count"]
    rec("thumb integrity", not bad and not hbad, f"{checked} sampled worlds map to the right AVIF bytes; {len(bad)} bad {bad[:2]}; pack hash mismatches {hbad[:3]}")


DRIFT_COLS = ["name", "description", "tags", "author_name", "capacity", "favorites", "visits"]
DRIFT_MAX_RISE = 0.02  # null/empty rate of a user-visible column may rise by at most 2pp week over week
DRIFT_MAX_CHANGED = 0.25  # fraction of worlds present in both extracts whose text (name, description, tags, author) changed; observed ~1-2% / week


def empty_rates(t) -> dict[str, float]:
    out = {}
    for c in DRIFT_COLS:
        col = t[c]
        if str(col.type).endswith("string"):
            bad = pc.equal(pc.utf8_trim_whitespace(col), "")
        elif str(col.type).startswith("list"):
            bad = pc.equal(pc.list_value_length(col), 0)
        else:  # numeric: 0 (a blanked scrape shows up as null or 0)
            bad = pc.equal(col, 0)
        bad = pc.fill_null(bad, True)  # null counts as empty
        out[c] = pc.sum(pc.cast(bad, "int64")).as_py() / max(1, len(col))
    return out


def text_keys(t) -> dict[str, int]:
    cols = [t[c].to_pylist() for c in ("name", "description", "tags", "author_name")]
    return {w: hash((n, d, tuple(tg or ()), au)) for w, n, d, tg, au in zip(t["world_id"].to_pylist(), *cols)}


def check_extract_drift(cur: Path, prev: Path) -> None:
    if not prev.is_file() or not cur.is_file():
        rec("extract drift", True, f"no previous extract to compare ({prev})")
        return
    cols = ["world_id", *DRIFT_COLS, "author_name"]
    c, p = pq.read_table(cur, columns=list(dict.fromkeys(cols))), pq.read_table(prev, columns=list(dict.fromkeys(cols)))
    rc, rp = empty_rates(c), empty_rates(p)
    worst = max(DRIFT_COLS, key=lambda k: rc[k] - rp[k])
    rise = {k: rc[k] - rp[k] for k in DRIFT_COLS}
    print("  empty rates (previous -> current): " + ", ".join(f"{k} {rp[k]:.2%}->{rc[k]:.2%}" for k in DRIFT_COLS), flush=True)
    rec("extract drift: empty/null rates", all(v <= DRIFT_MAX_RISE for v in rise.values()), f"worst {worst} {rise[worst] * 100:+.2f}pp (limit +{DRIFT_MAX_RISE * 100:.0f}pp)")
    kc, kp = text_keys(c), text_keys(p)
    both = kc.keys() & kp.keys()
    changed = sum(1 for w in both if kc[w] != kp[w]) / max(1, len(both))
    rec("extract drift: text changed", changed < DRIFT_MAX_CHANGED, f"{changed:.2%} of {len(both)} worlds in both extracts (limit {DRIFT_MAX_CHANGED:.0%})")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dist", type=Path, required=True)
    ap.add_argument("--public", type=Path, required=True)
    ap.add_argument("--full", type=Path, required=True)
    ap.add_argument("--excluded", type=Path, required=True)
    ap.add_argument("--deployed-ids", type=Path, required=True)
    ap.add_argument("--thumb-state", type=Path, required=True)
    ap.add_argument("--live-url", required=True)
    ap.add_argument("--search", type=Path, required=True)
    ap.add_argument("--deployed-json", type=Path)
    ap.add_argument("--extract-dir", type=Path)
    ap.add_argument("--report", type=Path)
    a = ap.parse_args()
    dist = a.dist.resolve()
    man = json.loads((dist / "data/manifest.json").read_text())

    print("-- 1. preflight (deploy/check.py)", flush=True)
    r = subprocess.run([sys.executable, str(a.search / "deploy/check.py"), str(dist), "--full-parquet", str(a.full), "--public-parquet", str(a.public),
                        "--excluded", str(a.excluded)], text=True, capture_output=True)
    print(r.stdout[-6000:], r.stderr[-1500:], flush=True)
    rec("deploy/check.py preflight", r.returncode == 0, f"rc={r.returncode}")

    print("-- 2. web engine tests", flush=True)
    env = {**os.environ, "VRCW_DATA": str(dist / "data"), "VRCW_THUMBS": str(dist)}
    r = subprocess.run(["node", "test/run.mjs", "test/engine.test.ts", "test/query.test.ts"], cwd=a.search / "web", env=env, text=True, capture_output=True)
    tail = (r.stdout + r.stderr)[-2500:]
    print(tail, flush=True)
    rec("web/test engine+query", r.returncode == 0, f"rc={r.returncode}")

    print("-- 3. thumbnail integrity", flush=True)
    check_thumbs(dist, a.public, a.thumb_state, man)

    print("-- 4. week over week", flush=True)
    n = man["n"]
    th = man.get("thumbs") or {}
    cov = (th.get("count", 0) / n) if n else 0
    try:
        sim = json.loads(gz(dist / "data/sim/index.json"))
        rec("neighbour coverage 100%", sim.get("count") == n, f"{sim.get('count')}/{n}")
    except Exception as e:  # noqa: BLE001
        rec("neighbour coverage 100%", False, f"sim/index.json unreadable: {e}")
    try:
        req = urllib.request.Request(a.live_url.rstrip("/") + f"/data/manifest.json?cb={os.getpid()}", headers={"User-Agent": "vrcw-search-refresh/1"})  # Cloudflare 403s urllib's default UA
        with urllib.request.urlopen(req, timeout=30) as resp:
            live = json.load(resp)
    except Exception as e:  # noqa: BLE001
        live = None
        rec("live manifest readable", False, str(e))
    rec("manifest.snapshot == history.asof", not man.get("history") or (man["history"].get("asof") or "")[:10] == (man.get("snapshot") or "")[:10],
        f"snapshot {man.get('snapshot')} history.asof {(man.get('history') or {}).get('asof')}")
    if live:
        lth = live.get("thumbs") or {}
        live_cov = (lth.get("count", 0) / live["n"]) if live.get("n") else 0
        need_cov = min(0.99, live_cov - 0.005)
        rec("thumbnail coverage >= min(99%, live - 0.5pp)", cov >= need_cov, f"{th.get('count', 0)}/{n} = {cov:.2%}; live {lth.get('count', 0)}/{live['n']} = {live_cov:.2%}; need {need_cov:.2%}")
        dj = baseline.read_json(a.deployed_json) if a.deployed_json else None
        weeks = baseline.weeks_since(dj)
        tol = 0.03 * weeks
        d = (n - live["n"]) / live["n"]
        rec(f"public world count within +-{tol:.0%} of live", abs(d) <= tol, f"{n} vs live {live['n']} ({d:+.2%}); {weeks} week(s) since deployed.json {dj and dj.get('deployedAt')}")
        if dj and live["version"] != dj.get("version"):
            rec("deployed.json describes the live site", False, f"live {live['version']} != deployed.json {dj.get('version')}")
        new_t = (man.get("refresh") or {}).get("commitTime") or man.get("snapshot")
        live_t = (live.get("refresh") or {}).get("commitTime") or ((live.get("snapshot") or "")[:10] + "T23:59:59Z")  # a bare date is the newest scrape's day
        rec("data as-of newer than live", bool(new_t) and new_t > live_t, f"{new_t} vs live {live_t}")
        rec("manifest.version differs from live", man["version"] != live["version"], f"{man['version']} vs {live['version']}")
    if a.deployed_ids.is_file():
        prev = set(pq.read_table(a.deployed_ids, columns=["world_id"])["world_id"].to_pylist())
        now = set(pq.read_table(a.public, columns=["world_id"])["world_id"].to_pylist())
        t = pq.read_table(a.full, columns=["world_id", "release_status"])
        st = dict(zip(t["world_id"].to_pylist(), pc.utf8_lower(t["release_status"]).to_pylist()))
        lost = prev - now
        turned_private = {w for w in lost if st.get(w) not in (None, "public")}
        excl = set()
        for line in a.excluded.read_text(encoding="utf-8").splitlines():
            v = line.split("#", 1)[0].strip()
            if v.startswith("wrld_"):
                excl.add(v)
        unexplained = lost - turned_private - excl
        kept = 1 - len(unexplained) / max(1, len(prev - turned_private - excl))
        rec(">= 99% of last deployed public worlds retained", kept >= 0.99,
            f"{len(prev)} last week, {len(lost)} gone ({len(turned_private)} now non-public, {len(unexplained)} unexplained) -> {kept:.2%} retained")
    else:
        rec(">= 99% of last deployed public worlds retained", False, f"no {a.deployed_ids} (seed it from the live build)")

    print("-- 5. extract drift", flush=True)
    if a.extract_dir:
        check_extract_drift(a.extract_dir / "current" / "public-v2.parquet", a.extract_dir / "previous" / "public-v2.parquet")

    ok = all(x["ok"] for x in results)
    if a.report:
        a.report.write_text(json.dumps({"ok": ok, "results": results}, indent=1))
    print(f"GATE {'PASSED' if ok else 'FAILED'}: " + ", ".join(f"{x['check']}={'ok' if x['ok'] else 'FAIL'}" for x in results))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
