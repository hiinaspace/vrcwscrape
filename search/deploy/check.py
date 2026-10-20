#!/usr/bin/env python3
"""Preflight for a Cloudflare Pages deploy of search/dist.

usage: uv run --project search/build python search/deploy/check.py DIST [--full-parquet P] [--public-parquet P] [--skip-gitleaks]

Fails (exit 1) on: file count >= 20,000, any file > 25 MiB, image-host references (vrchat.cloud etc.) in code/manifest,
private world ids in the world-id tables, unstamped placeholders in about.html, missing _headers/about.html/index.html,
gitleaks findings in the raw files. Warns on: image-host URLs inside user description text, private ids mentioned in text,
third-party hosts in code, gitleaks findings inside decompressed user text.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

MAX_FILES = 20_000
MAX_BYTES = 25 * 1024 * 1024
MAX_HEADER_RULES = 100
DATA = Path("/work/projects/vrcwscrape-data")

UUID = re.compile(rb"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
IMG_HOSTS = re.compile(
    rb"(?:api|files|assets|images|cdn)?\.?vrchat\.cloud|assets\.vrchat\.com|d348imysud55la\.cloudfront\.net|vrchat-prod-images",
    re.I,
)
URL = re.compile(rb"https?://([A-Za-z0-9.-]+)")
OK_HOSTS = {"vrchat.com", "www.vrchat.com", "www.w3.org", "w3.org"}
CODE_EXT = {".js", ".css", ".html", ".json", ".txt", ".svg", ".map"}

errors: list[str] = []
warns: list[str] = []


def err(msg: str) -> None:
    errors.append(msg)
    print(f"  FAIL  {msg}")


def warn(msg: str) -> None:
    warns.append(msg)
    print(f"  warn  {msg}")


def ok(msg: str) -> None:
    print(f"  ok    {msg}")


def read_maybe_gz(p: Path) -> bytes:
    b = p.read_bytes()
    if b[:2] == b"\x1f\x8b":
        try:
            return gzip.decompress(b)
        except Exception:
            return b
    return b


def is_data_chunk(rel: str) -> bool:
    return rel.startswith(("data/meta/", "data/card/", "data/authors/", "data/fts/"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("dist", type=Path)
    ap.add_argument("--full-parquet", type=Path, default=DATA / "worlds-2026-10-20.parquet")
    ap.add_argument("--public-parquet", type=Path, default=DATA / "worlds-2026-10-20-public-v2.parquet")
    ap.add_argument("--excluded", type=Path, default=Path(__file__).resolve().parent / "excluded_ids.txt")
    ap.add_argument("--skip-gitleaks", action="store_true")
    a = ap.parse_args()
    dist: Path = a.dist.resolve()
    if not dist.is_dir():
        print(f"not a directory: {dist}")
        return 2

    files = sorted(p for p in dist.rglob("*") if p.is_file())
    rels = {p: p.relative_to(dist).as_posix() for p in files}
    sizes = {p: p.stat().st_size for p in files}
    total = sum(sizes.values())

    print(f"== {dist}")
    print("-- limits")
    if len(files) >= MAX_FILES:
        err(f"{len(files)} files (Pages limit is 20,000)")
    else:
        ok(f"{len(files)} files < {MAX_FILES}")
    big = [(sizes[p], rels[p]) for p in files if sizes[p] > MAX_BYTES]
    if big:
        for s, r in big:
            err(f"{r} is {s/2**20:.1f} MiB (> 25 MiB)")
    else:
        top = max(files, key=lambda p: sizes[p])
        ok(f"largest file {rels[top]} = {sizes[top]/2**20:.2f} MiB (<= 25 MiB)")

    print("-- required files")
    for need in ("index.html", "about.html", "about.css", "_headers", "data/manifest.json"):
        if not (dist / need).is_file():
            err(f"missing {need}")
    about = dist / "about.html"
    if about.is_file():
        t = about.read_text(encoding="utf-8")
        if "@@" in t:
            err("about.html still has @@placeholders@@ (run finalize.py)")
        else:
            m = re.search(r"Snapshot: newest scrape <strong>([^<]+)</strong>", t)
            ok(f"about.html stamped (snapshot {m.group(1) if m else '?'})")
        if "<!--" in t:
            err("about.html still has THUMBS markers (run finalize.py)")
        has_thumb_claim = "re-encoded previews" in t
        mj = dist / "data/manifest.json"
        if mj.is_file() and has_thumb_claim != bool(json.loads(mj.read_text()).get("hasThumb")):
            err(f"about.html thumbnail claim ({has_thumb_claim}) disagrees with manifest.hasThumb")
        if "mailto:hiina@hiina.space" not in t:
            err("about.html has no takedown contact")
    if (dist / "data/sem").is_dir():
        lic = dist / "data/sem/LICENSE.txt"
        if not lic.is_file() or "Apache License" not in lic.read_text() or "CHANGES MADE" not in lic.read_text():
            err("data/sem/LICENSE.txt missing or incomplete (Apache-2.0 text + notice)")
        else:
            ok("data/sem/LICENSE.txt present (Apache-2.0 text + changes notice)")
    idx = dist / "index.html"
    if idx.is_file() and "about.html" not in idx.read_text(encoding="utf-8"):
        err("index.html has no link to about.html")
    hdr = dist / "_headers"
    if hdr.is_file():
        lines = hdr.read_text().splitlines()
        rules = [ln for ln in lines if ln and not ln[0].isspace() and not ln.startswith("#")]
        longest = max((len(ln) for ln in lines), default=0)
        if len(rules) > MAX_HEADER_RULES or longest > 2000:
            err(f"_headers: {len(rules)} rules (max 100), longest line {longest} (max 2000)")
        else:
            ok(f"_headers: {len(rules)} rules, longest line {longest}")
        if "Content-Security-Policy" not in hdr.read_text():
            err("_headers has no CSP")
    man = {}
    mp = dist / "data/manifest.json"
    if mp.is_file():
        man = json.loads(mp.read_text())

    print("-- image hosts / third parties")
    img_code, img_text, hosts = [], Counter(), Counter()
    for p in files:
        r = rels[p]
        if r.startswith("thumbs/") or p.suffix == ".avif":
            continue
        if sizes[p] > 40 * 2**20:
            continue
        b = read_maybe_gz(p) if (p.suffix == ".bin" or p.suffix == ".json") else p.read_bytes()
        if p.suffix not in CODE_EXT and p.suffix != ".bin":
            continue
        n = len(IMG_HOSTS.findall(b))
        if n:
            (img_text if is_data_chunk(r) else img_code).append((r, n))  # type: ignore[arg-type]
        if p.suffix in CODE_EXT and not is_data_chunk(r):
            for h in URL.findall(b):
                hosts[h.decode().lower()] += 1
    if img_code:
        for r, n in img_code:
            err(f"{r}: {n} reference(s) to vrchat image hosts")
    else:
        ok("no vrchat.cloud / image-host reference in code, manifest, or index files")
    if img_text:
        warn(
            f"{sum(n for _, n in img_text)} image-host URLs inside user description text in "
            f"{len(img_text)} chunk files (text only; the site never loads them)"
        )
    bad_hosts = {h: c for h, c in hosts.items() if h not in OK_HOSTS}
    if bad_hosts:
        warn(f"other hosts named in code/manifest: {dict(bad_hosts)}")
    else:
        ok(f"hosts named in code: {dict(hosts) or 'none'}")

    print("-- private worlds")
    t = pq.read_table(a.full_parquet, columns=["world_id", "release_status"])
    st = pc.utf8_lower(t["release_status"])
    priv = {s[5:] if s.startswith("wrld_") else s for s in t["world_id"].filter(pc.not_equal(st, "public")).to_pylist()}
    pub = {s[5:] if s.startswith("wrld_") else s for s in t["world_id"].filter(pc.equal(st, "public")).to_pylist()}
    privb = {s.encode() for s in priv}
    print(f"        {len(priv):,} non-public ids and {len(pub):,} public ids in {a.full_parquet.name}")
    ex_w, ex_a = set(), set()
    if a.excluded.is_file():
        for line in a.excluded.read_text(encoding="utf-8").splitlines():
            v = line.split("#", 1)[0].strip()
            if not v:
                continue
            bare = v.split("_", 1)[1] if v.startswith(("wrld_", "usr_")) else v
            if not v.startswith("usr_"):
                ex_w.add(bare)
            if not v.startswith("wrld_"):
                ex_a.add(bare)
    ex_hits: set[str] = set()
    ex_text: Counter[str] = Counter()
    pub -= ex_w  # excluded worlds must be absent, so they are not expected by the scanner sanity check
    seen_pub: set[bytes] = set()
    leak_tables: set[str] = set()
    leak_text: Counter[str] = Counter()
    for p in files:
        r = rels[p]
        if not (r.startswith("data/") and p.suffix == ".bin"):
            continue
        b = read_maybe_gz(p)
        if ex_w or ex_a:
            if r.startswith(("data/ids/", "data/authors/", "data/meta/")):
                doc = json.loads(b)
                keys = set(doc) if r.startswith(("data/ids/", "data/authors/")) else set(doc["id"]) | set(doc["au"])
                ex_hits |= keys & (ex_w | ex_a)
            else:
                for e in ex_w | ex_a:
                    if e.encode() in b:
                        ex_text[r.split("/")[1]] += 1
        found = set(UUID.findall(b))
        if not found:
            continue
        if r.startswith("data/ids/"):
            seen_pub |= found
            leak_tables |= {u.decode() for u in found & privb}
        elif r.startswith("data/meta/"):  # only the "id" array is a world-id table; uuid-shaped user tags are reported separately
            ids_here = {x.encode() for x in json.loads(b)["id"]}
            leak_tables |= {u.decode() for u in ids_here & privb}
            for u in (found - ids_here) & privb:
                leak_text["meta-tags"] += 1
        else:
            for u in found & privb:
                leak_text[r.split("/")[1]] += 1
    if a.excluded.is_file():
        if ex_hits:
            err(f"{len(ex_hits)} EXCLUDED world/author ids present in id/author/meta tables, e.g. {sorted(ex_hits)[:3]}")
        else:
            ok(f"none of the {len(ex_w) + len(ex_a)} excluded ids ({a.excluded.name}) present in id/author/meta tables")
        if ex_text:
            warn(f"excluded ids mentioned in other data files (text of other worlds): {dict(ex_text)}")
    if leak_tables:
        err(f"{len(leak_tables)} NON-PUBLIC world ids in the id/meta tables, e.g. {sorted(leak_tables)[:3]}")
    else:
        ok("no non-public world id in data/ids or data/meta")
    if leak_text:
        warn(f"uuid-shaped strings equal to non-public world ids outside the id tables (user tags / description text; counts of occurrences): {dict(leak_text)}")
    id_seen = sum(1 for u in seen_pub if u.decode() in pub)
    n = man.get("n")
    if id_seen < 0.99 * len(pub):
        err(f"scanner saw only {id_seen:,}/{len(pub):,} public ids in data/ids (it cannot vouch for the check)")
    else:
        ok(f"scanner sanity: {id_seen:,} of {len(pub):,} public ids found in data/ids")
    extra = {u.decode() for u in seen_pub} - pub
    if extra:
        err(f"{len(extra)} ids in data/ids are not public ids of the full parquet, e.g. {sorted(extra)[:3]}")
    if a.public_parquet.is_file():
        npub = pq.ParquetFile(a.public_parquet).metadata.num_rows
        if n != npub:
            err(f"manifest n={n} != rows in {a.public_parquet.name} ({npub})")
        else:
            ok(f"manifest n={n:,} == rows in {a.public_parquet.name}")

    print("-- thumbnails")
    packs = sorted(p for p in files if rels[p].startswith("thumbs/p-"))
    if packs:
        ok(
            f"{len(packs)} packs, {sum(sizes[p] for p in packs)/2**20:.1f} MiB, "
            f"largest {max(sizes[p] for p in packs)/2**20:.2f} MiB; manifest.thumbs={'set' if man.get('thumbs') else 'MISSING'}"
        )
        if not man.get("thumbs"):
            err("thumb packs present but manifest.thumbs is empty")
    else:
        warn("no thumbnail packs in this dist (site will show placeholders)")

    print("-- secrets (gitleaks)")
    gl = shutil.which("gitleaks")
    if a.skip_gitleaks:
        warn("gitleaks skipped by flag")
    elif not gl:
        err("gitleaks not found on PATH")
    else:
        r = subprocess.run(
            [gl, "dir", str(dist), "--no-banner", "--redact", "--exit-code", "7", "--log-level", "error"],
            capture_output=True,
            text=True,
        )
        if r.returncode == 0:
            ok("gitleaks: no leaks in raw files")
        else:
            err(f"gitleaks rc={r.returncode} on raw files:\n{r.stdout[-800:]}{r.stderr[-400:]}")
        with tempfile.TemporaryDirectory(dir=dist.parent, prefix=".chk-") as td:
            n_dec = 0
            for p in files:
                r_ = rels[p]
                if p.suffix == ".bin" and r_.startswith("data/") and sizes[p] < 8 * 2**20:
                    b = p.read_bytes()
                    if b[:2] == b"\x1f\x8b":
                        q = Path(td) / (r_.replace("/", "_") + ".txt")
                        try:
                            q.write_bytes(gzip.decompress(b))
                            n_dec += 1
                        except Exception:
                            pass
            rep = Path(td).parent / ".chk-gitleaks.json"
            r2 = subprocess.run(
                [gl, "dir", td, "--no-banner", "--redact", "--exit-code", "7", "--log-level", "error", "-f", "json", "-r", str(rep)],
                capture_output=True,
                text=True,
            )
            if r2.returncode == 0:
                ok(f"gitleaks: no leaks in {n_dec} decompressed data files")
            else:
                try:
                    c = Counter(x["RuleID"] for x in json.loads(rep.read_text()))
                except Exception:
                    c = Counter({"unknown": 1})
                warn(f"gitleaks matched user text in decompressed data (names/descriptions, values redacted; review the rule mix): {dict(c)}")
            rep.unlink(missing_ok=True)

    print("-- summary")
    by_top = Counter()
    cnt_top = Counter()
    for p in files:
        top = rels[p].split("/")[0] if "/" in rels[p] else "(root)"
        by_top[top] += sizes[p]
        cnt_top[top] += 1
    for k in sorted(by_top, key=lambda k: -by_top[k]):
        print(f"        {k:<10} {cnt_top[k]:>6} files {by_top[k]/2**20:>9.1f} MiB")
    print(f"        TOTAL      {len(files):>6} files {total/2**20:>9.1f} MiB   (Pages: no total cap; 20,000 files, 25 MiB/file)")
    print(f"        result: {len(errors)} failure(s), {len(warns)} warning(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
