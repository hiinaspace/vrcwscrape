"""Run the oracle harness against a site.

Usage:
  uv run python run.py --url http://host:port [--parquet P] [--gold gold] [--dist DIR]
                       [--label NAME] [--build-time-s N] [--no-shots]
"""

from __future__ import annotations

import argparse
import math
import re
import json
import os
import random
import statistics
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import numpy as np
from playwright.sync_api import sync_playwright

from common import DEFAULT_PARQUET, HERE, connect, script_of, tag_class

MiB = 1024 * 1024
TARGETS = {
    "cold_first_results_s": 1.5,
    "first_view_bytes": 3 * MiB,
    "warm_p95_ms": 100.0,
    "facet_exact_rate": 1.0,
    "recall_at_10": 0.9,
    "max_file_bytes": 25 * MiB,
    "total_bytes": 200 * 1000 * 1000,
    "max_files": 20000,
}
LIB_DIR = Path.home() / ".cache/chromium-libs/usr/lib/x86_64-linux-gnu"
SHOT_VIEWPORTS = {"desktop": (1440, 900), "mobile": (390, 844)}


def launch(p):
    env = dict(os.environ)
    if LIB_DIR.exists():  # user-space copies of Chromium's shared libs (no root on cc-0)
        env["LD_LIBRARY_PATH"] = f"{LIB_DIR}:{env.get('LD_LIBRARY_PATH', '')}"
    return p.chromium.launch(env=env)


def site_url(base: str, **params) -> str:
    params = {k: v for k, v in params.items() if v not in (None, "")}
    return base.rstrip("/") + "/" + (("?" + urlencode(params)) if params else "")


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))]


# ---------------------------------------------------------------- cold start
def cold_run(browser, url: str) -> dict:
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    page = ctx.new_page()
    cdp = ctx.new_cdp_session(page)
    cdp.send("Network.enable")
    cdp.send("Network.setCacheDisabled", {"cacheDisabled": False})
    state = {"bytes": 0, "reqs": 0, "third_bytes": 0, "third_reqs": 0}
    urls: dict[str, str] = {}
    origin = urlsplit(url)._replace(path="", query="", fragment="").geturl()

    def on_req(e):
        u = e["request"]["url"]
        urls[e["requestId"]] = u
        if u.startswith(origin) or u.startswith(("data:", "blob:")):
            state["reqs"] += 1
        else:
            state["third_reqs"] += 1

    def on_done(e):
        u = urls.get(e["requestId"], "")
        n = e.get("encodedDataLength", 0)
        if u.startswith(origin) or u.startswith(("data:", "blob:")):
            state["bytes"] += n
        else:
            state["third_bytes"] += n

    cdp.on("Network.requestWillBeSent", on_req)
    cdp.on("Network.loadingFinished", on_done)
    t0 = time.perf_counter()
    err = None
    try:
        page.goto(url, wait_until="commit")
        page.wait_for_selector('[data-testid="result-item"]', timeout=30000)
        t = time.perf_counter() - t0
        try:
            page.wait_for_load_state("networkidle", timeout=15000)  # 500 ms with no requests
        except Exception:  # noqa: BLE001
            pass
    except Exception as e:  # noqa: BLE001
        t, err = None, str(e)[:200]
    out = {
        "first_results_s": t,
        "bytes": state["bytes"],
        "requests": state["reqs"],
        "third_party_bytes": state["third_bytes"],
        "third_party_requests": state["third_reqs"],
        "error": err,
    }
    ctx.close()
    return out


def cold_metrics(browser, base: str, q: str, reps: int = 3) -> dict:
    runs = [cold_run(browser, site_url(base, q=q)) for _ in range(reps)]
    ok = [r for r in runs if r["first_results_s"] is not None]
    if not ok:
        return {"runs": runs, "error": runs[0]["error"]}
    return {
        "runs": runs,
        "first_results_s": statistics.median(r["first_results_s"] for r in ok),
        "bytes": int(statistics.median(r["bytes"] for r in ok)),
        "requests": int(statistics.median(r["requests"] for r in ok)),
        "third_party_bytes": int(statistics.median(r["third_party_bytes"] for r in ok)),
        "third_party_requests": int(statistics.median(r["third_party_requests"] for r in ok)),
    }


# ---------------------------------------------------------------- in-page batch
QUERY_JS = """
async (items) => {
  await window.vrcwSearchReady;
  const out = [];
  for (const it of items) {
    const t0 = performance.now();
    let r = null, err = null;
    try { r = await window.vrcwSearch.query(it); } catch (e) { err = String(e); }
    out.push({wall: performance.now() - t0, ms: r && r.ms, ids: r && r.ids, total: r && r.total,
              facetCounts: r && r.facetCounts, err});
  }
  return out;
}
"""
SIMILAR_JS = """
async (items) => {
  await window.vrcwSearchReady;
  const out = [];
  for (const [id, k] of items) {
    try { out.push(await window.vrcwSearch.similar(id, k)); } catch (e) { out.push([]); }
  }
  return out;
}
"""


def chunked(page, js, items, n=200):
    res = []
    for i in range(0, len(items), n):
        res += page.evaluate(js, items[i : i + n])
    return res


def load_jsonl(p: Path) -> list[dict]:
    return [json.loads(line) for line in open(p) if line.strip()]


def warm_and_recall(page, gold: list[dict], facets: list[dict], parquet: str) -> dict:
    con = connect(parquet)
    tag_ids = {}
    for t in {g["tag"] for g in gold if g.get("tag")}:
        tag_ids[t] = {
            r[0]
            for r in con.execute(
                "select world_id from w where list_contains(tagset, ?)", [t]
            ).fetchall()
        }
    items = [{"q": g["q"], "filters": {}, "sort": "relevance", "limit": 10} for g in gold]
    chunked(page, QUERY_JS, items[:25])  # warm-up pass, discarded
    res = chunked(page, QUERY_JS, items)
    fitems = [{"q": "", "filters": f["filters"], "sort": "relevance", "limit": 10} for f in facets]
    fres = chunked(page, QUERY_JS, fitems)
    walls = [r["wall"] for r in res if r["err"] is None] + [
        r["wall"] for r in fres if r["err"] is None
    ]
    errors = sum(1 for r in res + fres if r["err"])
    cats: dict[str, dict] = {}
    misses = []
    for g, r in zip(gold, res):
        c = cats.setdefault(g["cat"], {"n": 0, "hit": 0, "target_hit": 0, "prec": []})
        c["n"] += 1
        top = r["ids"] or []
        exp = tag_ids[g["tag"]] if g.get("tag") else set(g["expected"])
        hit = any(i in exp for i in top)
        c["hit"] += hit
        c["target_hit"] += g["target"] in top
        c["prec"].append(sum(i in exp for i in top) / max(1, len(top)) if top else 0.0)
        if not hit and len(misses) < 15:
            misses.append({"cat": g["cat"], "q": g["q"], "err": r["err"], "top3": top[:3]})
    for k, c in cats.items():
        c["graded_by"] = "precision" if k == "tag_word" else "recall"
        c["recall_at_10"] = c["hit"] / c["n"]
        c["target_at_10"] = c["target_hit"] / c["n"]
        c["precision_at_10"] = sum(c.pop("prec")) / c["n"]
    rc = [c for c in cats.values() if c["graded_by"] == "recall"]
    n = sum(c["n"] for c in rc)
    overall = sum(c["hit"] for c in rc) / max(1, n)
    return {
        "warm": {
            "n": len(walls),
            "p50_ms": pct(walls, 0.5),
            "p95_ms": pct(walls, 0.95),
            "max_ms": max(walls) if walls else None,
            "errors": errors,
        },
        "recall": {"overall": overall, "by_category": cats, "n": n, "sample_misses": misses},
        "fres": fres,
    }


def facet_eval(facets: list[dict], fres: list[dict]) -> dict:
    exact = 0
    bad = []
    cap_present = 0
    for f, r in zip(facets, fres):
        t = f["truth"]
        fc = r.get("facetCounts") or {}
        reasons = []
        if r["err"] or r["total"] is None:
            reasons.append(f"error {r['err']}")
        else:
            if r["total"] != t["total"]:
                reasons.append(f"total {r['total']} != {t['total']}")
            plat = fc.get("platform")
            if not isinstance(plat, dict):
                reasons.append("facetCounts.platform missing")
            else:
                for k, v in t["platform"].items():
                    if plat.get(k) != v:
                        reasons.append(f"platform.{k} {plat.get(k)} != {v}")
            tg = fc.get("tags")
            if not isinstance(tg, dict):
                reasons.append("facetCounts.tags missing")
            else:
                for k, v in t["tags"].items():
                    if tg.get(k) != v:
                        reasons.append(f"tag {k} {tg.get(k)} != {v}")
            cap = fc.get("capacity")
            if isinstance(cap, dict):
                cap_present += 1
        if not reasons:
            exact += 1
        elif len(bad) < 8:
            bad.append({"filters": f["filters"], "reasons": reasons[:4]})
    return {
        "n": len(facets),
        "exact": exact,
        "exact_rate": exact / max(1, len(facets)),
        "capacity_facet_present": cap_present,
        "sample_failures": bad,
    }


# ---------------------------------------------------------------- similar sanity
def similar_eval(page, parquet: str, k: int = 10, want: int = 200, seed: int = 3) -> dict:
    con = connect(parquet)
    rows = con.execute(
        "select world_id, name, author_id, tagset, coalesce(visits,0) from w order by world_id"
    ).fetchall()
    rows = [(a, b, c, [t for t in d if tag_class(t) == "user"], e) for a, b, c, d, e in rows]
    info = {r[0]: r for r in rows}
    rnd = random.Random(seed)
    cand = sorted(rows, key=lambda r: -r[4])[:30000]
    rnd.shuffle(cand)
    cand = cand[: max(want * 8, 400)]
    res = chunked(page, SIMILAR_JS, [[r[0], k] for r in cand], n=100)
    usable = [(r, nb) for r, nb in zip(cand, res) if nb]
    coverage = len(usable) / max(1, len(res))
    usable = usable[:want]
    ids = list(info)

    def jac(a, b):
        a, b = set(a), set(b)
        return len(a & b) / len(a | b) if (a | b) else 0.0

    def score(src, nbs):
        nbs = [info[i] for i in nbs if i in info and i != src[0]]
        if not nbs:
            return None
        return (
            statistics.mean(jac(src[3], n[3]) for n in nbs),
            statistics.mean(n[2] == src[2] for n in nbs),
            statistics.mean(script_of(n[1]) == script_of(src[1]) for n in nbs),
        )

    sims = [score(r, nb) for r, nb in usable]
    sims = [s for s in sims if s]
    base = [score(r, rnd.sample(ids, k)) for r, _ in usable]
    base = [s for s in base if s]
    if not sims:
        return {"error": "similar() returned no neighbors for any sampled world", "tried": len(res)}
    m = lambda xs, i: statistics.mean(x[i] for x in xs)
    out = {"n_worlds": len(sims), "tried": len(res), "coverage_of_popular_sample": coverage, "k": k}
    for i, name in enumerate(["tag_jaccard", "same_author", "same_script"]):
        out[name] = {"neighbors": m(sims, i), "random": m(base, i)}
    out["verdict_pass"] = bool(
        out["tag_jaccard"]["neighbors"] > 1.5 * out["tag_jaccard"]["random"]
        and out["same_author"]["neighbors"] >= out["same_author"]["random"]
        and out["same_script"]["neighbors"] > out["same_script"]["random"]
    )
    return out


# ---------------------------------------------------------------- semantic (optional)
SEM_LOAD_JS = """
async () => {
  await window.vrcwSearchReady;
  const s = window.vrcwSearch.semantic;
  if (!s) return {present: false};
  const t0 = performance.now();
  if (typeof s.load === "function") await s.load();
  else if (typeof s === "function") await s("warmup", 1);
  return {present: true, load_ms: performance.now() - t0};
}
"""
SEM_Q_JS = """
async (items) => {
  const s = window.vrcwSearch.semantic;
  const f = typeof s === "function" ? s : (q, k) => s.query(q, k);
  const out = [];
  for (const q of items) {
    const t0 = performance.now();
    let ids = null, err = null;
    try { const r = await f(q, 10); ids = Array.isArray(r) ? r : (r && r.ids) || null; }
    catch (e) { err = String(e); }
    out.push({ms: performance.now() - t0, ids, err});
  }
  return out;
}
"""
SEM_QUERIES = "/work/projects/vrcwscrape-data/emb/queries.parquet"
SEM_OFFLINE_RECALL = 0.202


def semantic_eval(browser, base: str) -> dict:
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    page = ctx.new_page()
    cdp = ctx.new_cdp_session(page)
    cdp.send("Network.enable")
    page.goto(site_url(base), wait_until="load")
    page.wait_for_function("window.vrcwSearchReady !== undefined", timeout=30000)
    present = page.evaluate(
        "(async () => { await window.vrcwSearchReady; return !!window.vrcwSearch.semantic; })()"
    )
    if not present:
        ctx.close()
        return {"skipped": "window.vrcwSearch.semantic not present"}
    state = {"bytes": 0}
    cdp.on(
        "Network.loadingFinished",
        lambda e: state.__setitem__("bytes", state["bytes"] + e.get("encodedDataLength", 0)),
    )
    page.set_default_timeout(600000)
    ld = page.evaluate(SEM_LOAD_JS)
    dl = state["bytes"]
    rows = duckdb_rows(SEM_QUERIES)
    res = chunked(page, SEM_Q_JS, [q for _, q in rows], n=50)
    lat = [r["ms"] for r in res if r["err"] is None]
    hits = sum(1 for (wid, _), r in zip(rows, res) if r["ids"] and wid in r["ids"][:10])
    ctx.close()
    return {
        "n": len(rows),
        "download_bytes": dl,
        "load_s": ld["load_ms"] / 1000,
        "p50_ms": pct(lat, 0.5),
        "p95_ms": pct(lat, 0.95),
        "errors": sum(1 for r in res if r["err"]),
        "recall_at_10": hits / max(1, len(rows)),
        "offline_recall_at_10": SEM_OFFLINE_RECALL,
    }


def duckdb_rows(path: str) -> list[tuple[str, str]]:
    import duckdb

    return (
        duckdb.connect()
        .execute(
            "select world_id, query from read_parquet(?) where set = 'cjk2en' order by world_id",
            [path],
        )
        .fetchall()
    )


# ---------------------------------------------------------------- popularity / author / content-default
POP_QUERIES = (
    "horror club avatar chill pool game shrine japan sleep dance music anime quest pvp puzzle "
    "cafe forest space city beach cozy fantasy escape maze parkour movie bar home night castle"
).split()


def _top_ids(page, items):
    return [r["ids"] or [] for r in chunked(page, QUERY_JS, items)]


def popularity_eval(page, parquet: str) -> dict:
    """Median log10(visits+1) of the site's top 10 vs p75 of all DuckDB text matches (title/tag ILIKE)."""
    con = connect(parquet)
    vis = dict(con.execute("select world_id, coalesce(visits,0) from w").fetchall())
    tops = _top_ids(
        page, [{"q": q, "filters": {}, "sort": "relevance", "limit": 10} for q in POP_QUERIES]
    )
    rows, passed = [], 0
    for q, top in zip(POP_QUERIES, tops):
        like = "%" + q.replace("%", "") + "%"
        match = [
            math.log10(v + 1)
            for (v,) in con.execute(
                "select coalesce(visits,0) from w where name ilike ? or "
                "list_bool_or(list_transform(tagset, t -> t ilike ?))",
                [like, like],
            ).fetchall()
        ]
        if len(match) < 20 or not top:
            rows.append({"q": q, "skipped": f"matches={len(match)} top={len(top)}"})
            continue
        p75 = float(np.percentile(match, 75))
        med = statistics.median(math.log10(vis.get(i, 0) + 1) for i in top)
        ok = med >= p75
        passed += ok
        rows.append(
            {
                "q": q,
                "top10_median_log10": med,
                "match_p75_log10": p75,
                "n_match": len(match),
                "pass": ok,
            }
        )
    graded = [r for r in rows if "pass" in r]
    return {"n": len(graded), "passed": passed, "rate": passed / max(1, len(graded)), "rows": rows}


def author_eval(page, parquet: str, n: int = 30, seed: int = 11) -> dict:
    con = connect(parquet)
    cands = con.execute(
        "select author_name, count(*) c from w where author_name is not null and length(author_name) >= 3 "
        "group by author_name having count(*) between 3 and 150 order by author_name"
    ).fetchall()
    rnd = random.Random(seed)
    picks = rnd.sample(cands, min(n, len(cands)))
    authored = {
        nm: {
            r[0]
            for r in con.execute("select world_id from w where author_name = ?", [nm]).fetchall()
        }
        for nm, _ in picks
    }
    tops = _top_ids(
        page, [{"q": nm, "filters": {}, "sort": "relevance", "limit": 10} for nm, _ in picks]
    )
    shares = []
    for (nm, c), top in zip(picks, tops):
        denom = min(10, c)
        shares.append(
            {"author": nm, "worlds": c, "share": sum(i in authored[nm] for i in top[:10]) / denom}
        )
    mean = statistics.mean(x["share"] for x in shares) if shares else 0.0
    return {
        "n": len(shares),
        "mean_share": mean,
        "frac_ge_0_7": sum(x["share"] >= 0.7 for x in shares) / max(1, len(shares)),
        "worst": sorted(shares, key=lambda x: x["share"])[:5],
    }


DESIGN_MD = HERE.parent / "DESIGN.md"
CONTENT_DOC_RE = re.compile(
    r"show_?(adult|nsfw|content|all)|nsfw|hidden by default|hide_?adult", re.I
)


DOM_IDS_JS = """
() => [...document.querySelectorAll('[data-testid="result-item"]')].slice(0, 50).map(e => {
  const m = e.outerHTML.match(/wrld_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/);
  return m ? m[0] : null;
})
"""


def content_default_eval(page, parquet: str, mode: str, base: str, browser) -> dict:
    """UI-only default (DESIGN.md round 4): `?sort=published` renders no content_sex/adult worlds; `&adult=1` shows some."""
    if mode == "off":
        return {"skipped": "--content-default off"}
    con = connect(parquet)
    bad_ids = {
        r[0]
        for r in con.execute(
            "select world_id from w where list_contains(tagset,'content_sex') or list_contains(tagset,'content_adult')"
        ).fetchall()
    }

    def rendered(params: dict) -> list[str | None]:
        ctx = browser.new_context(
            viewport={"width": 1440, "height": 900}
        )  # fresh: no stored adult toggle
        pg = ctx.new_page()
        try:
            pg.goto(site_url(base, **params), wait_until="load")
            pg.wait_for_selector('[data-testid="result-item"]', timeout=20000)
            pg.wait_for_timeout(500)
            return pg.evaluate(DOM_IDS_JS)
        finally:
            ctx.close()

    default = rendered({"sort": "published"})
    adult = rendered({"sort": "published", "adult": "1"})
    unparsed = sum(1 for i in default if i is None)
    d_flag = [i for i in default if i in bad_ids]
    a_flag = [i for i in adult if i in bad_ids]
    return {
        "n_default": len(default),
        "flagged_default": len(d_flag),
        "flagged_default_ids": d_flag[:5],
        "n_adult": len(adult),
        "flagged_adult": len(a_flag),
        "unparsed_ids": unparsed,
    }


# ---------------------------------------------------------------- dist audit
def dist_audit(dist: Path) -> dict:
    files = [(p, p.stat().st_size) for p in dist.rglob("*") if p.is_file()]
    big = max(files, key=lambda x: x[1]) if files else (None, 0)
    return {
        "files": len(files),
        "total_bytes": sum(s for _, s in files),
        "largest_file": str(big[0].relative_to(dist)) if big[0] else None,
        "largest_bytes": big[1],
        "top5": [(str(p.relative_to(dist)), s) for p, s in sorted(files, key=lambda x: -x[1])[:5]],
    }


# ---------------------------------------------------------------- screenshots
def screenshots(browser, base: str, parquet: str, outdir: Path) -> dict:
    con = connect(parquet)
    wid = con.execute(
        "select world_id from w order by coalesce(visits,0) desc, world_id limit 1"
    ).fetchone()[0]
    views = [
        ("home", {}),
        ("q_horror", {"q": "horror"}),
        ("q_jinja", {"q": "神社"}),
        ("facets_quest_cap16_2026", {"platform": "quest", "cap_min": 16, "after": "2026-01-01"}),
        ("world_card", {"world": wid}),
        ("similar_panel", {"world": wid}),
    ]
    status = {}
    for vp_name, (w, h) in SHOT_VIEWPORTS.items():
        ctx = browser.new_context(viewport={"width": w, "height": h})
        for name, params in views:
            page = ctx.new_page()
            fn = f"{name}_{w}x{h}.png"
            try:
                page.goto(site_url(base, **params), wait_until="load", timeout=30000)
                page.evaluate("window.vrcwSearchReady")
                sel = {"world_card": "world-card", "similar_panel": "similar-list"}.get(
                    name, "result-item"
                )
                page.wait_for_selector(f'[data-testid="{sel}"]', timeout=15000)
                if name == "similar_panel":
                    page.locator('[data-testid="similar-list"]').first.scroll_into_view_if_needed()
                page.wait_for_timeout(700)  # thumbnails / fonts
                page.screenshot(path=str(outdir / fn))
                status[fn] = "ok"
            except Exception as e:  # noqa: BLE001
                status[fn] = f"FAILED: {str(e)[:120]}"
                try:
                    page.screenshot(path=str(outdir / ("FAILED_" + fn)))
                except Exception:  # noqa: BLE001
                    pass
            page.close()
        ctx.close()
    return status


# ---------------------------------------------------------------- main
def verdicts(m: dict) -> list[tuple[str, str, str, bool | None]]:
    rows = []

    def add(name, value, target, ok):
        rows.append((name, value, target, ok))

    for key, label in [
        ("cold_empty", "cold first results, empty query"),
        ("cold_query", "cold first results, query"),
    ]:
        c = m.get(key, {})
        v = c.get("first_results_s")
        add(
            label,
            f"{v:.2f} s" if v is not None else f"ERROR {c.get('error')}",
            "<= 1.5 s",
            v is not None and v <= TARGETS["cold_first_results_s"],
        )
    c = m.get("cold_empty", {})
    b = c.get("bytes")
    add(
        "first-view transfer (empty query)",
        (
            f"{b / MiB:.2f} MiB / {c.get('requests')} req same-origin; third-party (not graded) "
            f"{c.get('third_party_bytes', 0) / MiB:.2f} MiB / {c.get('third_party_requests')} req"
        )
        if b is not None
        else "n/a",
        "same-origin <= 3 MiB",
        b is not None and b <= TARGETS["first_view_bytes"],
    )
    c = m.get("cold_query", {})
    b = c.get("bytes")
    add(
        "first-view transfer (query)",
        (
            f"{b / MiB:.2f} MiB / {c.get('requests')} req same-origin; third-party (not graded) "
            f"{c.get('third_party_bytes', 0) / MiB:.2f} MiB / {c.get('third_party_requests')} req"
        )
        if b is not None
        else "n/a",
        "same-origin <= 3 MiB",
        b is not None and b <= TARGETS["first_view_bytes"],
    )
    w = m.get("warm", {})
    add(
        "warm query latency p50 / p95 (wall)",
        f"{w.get('p50_ms', 0):.1f} / {w.get('p95_ms', 0):.1f} ms (n={w.get('n')}, errors={w.get('errors')})",
        "p95 <= 100 ms",
        w.get("p95_ms") is not None
        and w["p95_ms"] <= TARGETS["warm_p95_ms"]
        and w.get("errors") == 0,
    )
    r = m.get("recall", {})
    cats = r.get("by_category", {})
    add(
        "FTS recall@10 overall",
        f"{r.get('overall', 0):.3f} (n={r.get('n')})",
        ">= 0.90 recall overall/per category (tag_word: precision >= 0.70)",
        r.get("overall", 0) >= 0.9
        and all(
            c["recall_at_10"] >= 0.9 if c["graded_by"] == "recall" else c["precision_at_10"] >= 0.7
            for c in cats.values()
        ),
    )
    for k, c in sorted(cats.items()):
        if c["graded_by"] == "precision":
            add(
                f"  precision@10 {k} (share of top 10 carrying the tag)",
                f"{c['precision_at_10']:.3f} (n={c['n']})",
                ">= 0.70",
                c["precision_at_10"] >= 0.7,
            )
            continue
        add(
            f"  recall@10 {k}",
            f"{c['recall_at_10']:.3f} (target-id@10 {c['target_at_10']:.3f}, p@10 {c['precision_at_10']:.2f}, n={c['n']})",
            ">= 0.90",
            c["recall_at_10"] >= 0.9,
        )
    f = m.get("facets", {})
    add(
        "facet counts exact vs DuckDB",
        f"{f.get('exact')}/{f.get('n')} ({f.get('exact_rate', 0):.3f})",
        "100%",
        f.get("exact_rate") == 1.0,
    )
    s = m.get("similar", {})
    if "error" in s:
        add("similar-worlds sanity", s["error"], "> random", False)
    elif s:
        add(
            "similar-worlds sanity",
            f"tag-Jaccard {s['tag_jaccard']['neighbors']:.3f} vs {s['tag_jaccard']['random']:.3f} random; "
            f"same-author {s['same_author']['neighbors']:.3f} vs {s['same_author']['random']:.3f}; "
            f"same-script {s['same_script']['neighbors']:.3f} vs {s['same_script']['random']:.3f}; "
            f"coverage {s['coverage_of_popular_sample']:.2f}",
            "jaccard >1.5x, author >=, script > random",
            s["verdict_pass"],
        )
    pp = m.get("popularity", {})
    if "rate" in pp:
        add(
            "popularity sanity (top-10 median >= match p75)",
            f"{pp['passed']}/{pp['n']} queries ({pp['rate']:.2f})",
            ">= 0.80",
            pp["rate"] >= 0.8,
        )
    elif pp:
        add("popularity sanity", pp.get("error", "?"), ">= 0.80", False)
    au = m.get("authors", {})
    if "mean_share" in au:
        add(
            "author-name query top-10 share",
            f"mean {au['mean_share']:.2f}; {au['frac_ge_0_7']:.2f} of {au['n']} authors >= 0.7",
            "mean >= 0.70",
            au["mean_share"] >= 0.7,
        )
    elif au:
        add("author-name query", au.get("error", "?"), "mean >= 0.70", False)
    cd = m.get("content_default", {})
    if "flagged_default" in cd:
        add(
            "adult default hidden in UI (?sort=published, rendered items)",
            f"{cd['flagged_default']} flagged of {cd['n_default']}; world ids unparsed from DOM: {cd['unparsed_ids']}",
            "0 flagged, ids readable",
            cd["flagged_default"] == 0 and cd["n_default"] > 0 and cd["unparsed_ids"] == 0,
        )
        add(
            "adult=1 reveals flagged worlds",
            f"{cd['flagged_adult']} flagged of {cd['n_adult']}",
            ">= 1",
            cd["flagged_adult"] >= 1,
        )
    elif "error" in cd:
        add("content default check", cd["error"], "0 flagged", False)
    elif cd:
        add("content default check", "skipped: " + cd["skipped"], "optional", None)
    sm = m.get("semantic", {})
    if "recall_at_10" in sm:
        add(
            "semantic cjk2en recall@10 (report only)",
            f"{sm['recall_at_10']:.3f}, contaminated (query text is part of the indexed doc); "
            f"offline leak-free estimate 0.20 in search/data/EMBED_EVAL.md",
            "report only",
            None,
        )
        add(
            "semantic model download",
            f"{sm['download_bytes'] / MiB:.1f} MiB in {sm['load_s']:.1f} s",
            "<= 40 MiB",
            sm["download_bytes"] <= 40 * MiB,
        )
        add(
            "semantic query latency p50 / p95",
            f"{sm['p50_ms']:.0f} / {sm['p95_ms']:.0f} ms (n={sm['n']}, errors={sm['errors']})",
            "p95 <= 100 ms",
            sm["p95_ms"] is not None and sm["p95_ms"] <= 100 and sm["errors"] == 0,
        )
    elif "error" in sm:
        add("semantic check", sm["error"], "no errors", False)
    elif sm:
        add("semantic cjk2en", "skipped: " + sm["skipped"], "optional", None)
    d = m.get("dist")
    if d:
        add(
            "dist largest file",
            f"{d['largest_file']} {d['largest_bytes'] / MiB:.2f} MiB",
            "<= 25 MiB",
            d["largest_bytes"] <= TARGETS["max_file_bytes"],
        )
        add(
            "dist total size / files",
            f"{d['total_bytes'] / 1e6:.1f} MB / {d['files']} files",
            "< 200 MB, < 20k files",
            d["total_bytes"] < TARGETS["total_bytes"] and d["files"] < TARGETS["max_files"],
        )
    if m.get("build_time_s") is not None:
        add("build time end to end", f"{m['build_time_s']:.0f} s", "(reported only)", None)
    return rows


def write_summary(m: dict, rows, path: Path) -> None:
    lines = []
    if m.get("label"):
        lines += [f"> **{m['label']}**", ""]
    lines += [
        f"# Oracle run {m['timestamp']}",
        "",
        f"- site: {m['url']}",
        f"- parquet: {m['parquet']}",
        f"- gold: {m['gold_dir']} ({m['n_gold']} queries, {m['n_facet_combos']} facet combos)",
        "- network: loopback/LAN as seen from the harness host (not a throttled link)",
        "",
        "| metric | value | target | verdict |",
        "|---|---|---|---|",
    ]
    for name, val, tgt, ok in rows:
        lines.append(
            f"| {name} | {val} | {tgt} | {'PASS' if ok else 'FAIL' if ok is False else '-'} |"
        )
    graded = [ok for *_, ok in rows if ok is not None]
    lines += [
        "",
        f"**Overall: {'PASS' if all(graded) else 'FAIL'}** ({sum(graded)}/{len(graded)} checks)",
        "",
    ]
    sh = m.get("screenshots")
    if sh:
        bad = {k: v for k, v in sh.items() if v != "ok"}
        lines += [
            f"Screenshots: {len(sh) - len(bad)}/{len(sh)} ok" + (f"; failed: {bad}" if bad else "")
        ]
    if m["recall"]["sample_misses"]:
        lines += ["", "Sample recall misses:"] + [
            f"- {x}" for x in m["recall"]["sample_misses"][:8]
        ]
    if m["facets"]["sample_failures"]:
        lines += ["", "Sample facet mismatches:"] + [
            f"- {x}" for x in m["facets"]["sample_failures"][:5]
        ]
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--parquet", default=DEFAULT_PARQUET)
    ap.add_argument("--gold", default=str(HERE / "gold"))
    ap.add_argument("--dist")
    ap.add_argument("--label", default="")
    ap.add_argument("--build-time-s", type=float)
    ap.add_argument("--content-default", choices=["auto", "on", "off"], default="auto")
    ap.add_argument("--no-shots", action="store_true")
    ap.add_argument("--runs-dir", default=str(HERE / "runs"))
    a = ap.parse_args()
    gold = load_jsonl(Path(a.gold) / "queries.jsonl")
    facets = load_jsonl(Path(a.gold) / "facets.jsonl")
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    outdir = Path(a.runs_dir) / ts
    outdir.mkdir(parents=True, exist_ok=True)
    m: dict = {
        "timestamp": ts,
        "url": a.url,
        "parquet": a.parquet,
        "gold_dir": a.gold,
        "label": a.label,
        "n_gold": len(gold),
        "n_facet_combos": len(facets),
        "build_time_s": a.build_time_s,
    }
    with sync_playwright() as p:
        browser = launch(p)
        try:
            m["cold_empty"] = cold_metrics(browser, a.url, "")
            m["cold_query"] = cold_metrics(browser, a.url, "horror")
            ctx = browser.new_context(viewport={"width": 1440, "height": 900})
            page = ctx.new_page()
            page.goto(site_url(a.url), wait_until="load")
            page.wait_for_function("window.vrcwSearchReady !== undefined", timeout=30000)
            page.evaluate("window.vrcwSearchReady")
            wr = warm_and_recall(page, gold, facets, a.parquet)
            fres = wr.pop("fres")
            m["warm"], m["recall"] = wr["warm"], wr["recall"]
            m["facets"] = facet_eval(facets, fres)
            try:
                m["similar"] = similar_eval(page, a.parquet)
            except Exception as e:  # noqa: BLE001
                m["similar"] = {"error": f"harness error: {e}"[:300]}
            for key, fn in (
                ("popularity", lambda: popularity_eval(page, a.parquet)),
                ("authors", lambda: author_eval(page, a.parquet)),
                (
                    "content_default",
                    lambda: content_default_eval(
                        page, a.parquet, a.content_default, a.url, browser
                    ),
                ),
            ):
                try:
                    m[key] = fn()
                except Exception as e:  # noqa: BLE001
                    m[key] = {"error": f"harness error: {e}"[:300]}
            ctx.close()
            try:
                m["semantic"] = semantic_eval(browser, a.url)
            except Exception as e:  # noqa: BLE001
                m["semantic"] = {"error": f"harness error: {e}"[:300]}
            if not a.no_shots:
                m["screenshots"] = screenshots(browser, a.url, a.parquet, outdir)
        finally:
            browser.close()
    if a.dist:
        m["dist"] = dist_audit(Path(a.dist))
    rows = verdicts(m)
    m["verdicts"] = [{"metric": n, "value": v, "target": t, "pass": ok} for n, v, t, ok in rows]
    (outdir / "metrics.json").write_text(json.dumps(m, indent=1, ensure_ascii=False, default=str))
    write_summary(m, rows, outdir / "summary.md")
    print((outdir / "summary.md").read_text())
    print(f"run dir: {outdir}")


if __name__ == "__main__":
    main()
