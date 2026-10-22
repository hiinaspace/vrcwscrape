"""Shared semantics (mirrors SEMANTICS.md) and data loading for the oracle."""

from __future__ import annotations

import glob
import json
import os
import re
from pathlib import Path
from urllib.parse import urlencode

import duckdb

HERE = Path(__file__).parent
DEFAULT_PARQUET = "/work/projects/vrcwscrape-data/worlds-2026-10-20-public.parquet"
CJK_RE = re.compile(r"[぀-ヿ㐀-䶿一-鿿가-힯]")
KANA_RE = re.compile(r"[぀-ヿ]")
HANGUL_RE = re.compile(r"[가-힯]")
HAN_RE = re.compile(r"[㐀-䶿一-鿿]")
CJK_RUN_RE = re.compile(r"[぀-ヿ㐀-䶿一-鿿가-힯]+")


def connect(parquet: str) -> duckdb.DuckDBPyConnection:
    """DuckDB with view `w` holding the parquet plus derived columns (see SEMANTICS.md)."""
    con = duckdb.connect()
    cols = {
        r[0] for r in con.execute(f"describe select * from read_parquet('{parquet}')").fetchall()
    }
    # platform membership: the export's `platforms` list when present, else non-NULL package size
    has_pc = "list_contains(platforms, 'pc')" if "platforms" in cols else "pc_size_mb is not null"
    has_q = (
        "list_contains(platforms, 'quest')" if "platforms" in cols else "quest_size_mb is not null"
    )
    # round 6b: "updated" = max(updated_at, newest package upload); raw value kept as updated_at_raw
    hs = Path(os.environ.get("VRCW_HISTORY_DIR", "/work/projects/vrcwscrape-data/history")) / "summary.parquet"
    if hs.exists() and "updated_at" in cols and os.environ.get("VRCW_RAW_UPDATED") != "1":
        con.execute(
            f"create view base as select p.* replace (greatest(p.updated_at, s.last_upload::timestamp) as updated_at), p.updated_at as updated_at_raw "
            f"from read_parquet('{parquet}') p left join read_parquet('{hs}') s using (world_id)"
        )
        src = "base"
    else:
        src = f"read_parquet('{parquet}')"
    con.execute(
        f"""
        create view w as select *,
          coalesce({has_pc}, false) as has_pc,
          coalesce({has_q}, false) as has_quest,
          list_sort(list_distinct(list_transform(coalesce(tags, []),
              t -> regexp_replace(t, '^author_tag_', '')))) as tagset
        from {src}
        """
    )
    return con


def sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def where_sql(f: dict) -> str:
    """Filter semantics -> SQL boolean expression over view `w`."""
    c = ["true"]
    if f.get("cap_min") is not None:
        c.append(f"capacity >= {int(f['cap_min'])}")
    if f.get("cap_max") is not None:
        c.append(f"capacity <= {int(f['cap_max'])}")
    plat = f.get("platform")
    if plat == "pc":
        c.append("has_pc")
    elif plat == "quest":
        c.append("has_quest")
    elif plat == "both":
        c.append("(has_pc and has_quest)")
    if f.get("size_max_mb") is not None:
        x = float(f["size_max_mb"])
        if plat == "pc":
            c.append(f"pc_size_mb <= {x}")
        elif plat == "quest":
            c.append(f"quest_size_mb <= {x}")
        elif plat == "both":
            c.append(f"(pc_size_mb <= {x} and quest_size_mb <= {x})")
        else:
            c.append(f"least(coalesce(pc_size_mb, 1e18), coalesce(quest_size_mb, 1e18)) <= {x}")
    for k, col in (("after", "updated_at"), ("pub_after", "created_at")):  # inclusive calendar days
        if f.get(k):
            c.append(f"{col} >= timestamp {sql_str(f[k] + ' 00:00:00')}")
    if f.get("pub_before"):
        c.append(
            f"created_at < timestamp {sql_str(f['pub_before'] + ' 00:00:00')} + interval 1 day"
        )
    if f.get("platform_ex") == "pc":
        c.append("not has_pc")
    elif f.get("platform_ex") == "quest":
        c.append("not has_quest")
    if f.get("before"):
        c.append(f"updated_at < timestamp {sql_str(f['before'] + ' 00:00:00')} + interval 1 day")
    for st in f.get("status") or []:
        c.append(f"list_contains(tagset, {sql_str('system_' + st)})")
    for cw in f.get("cw") or []:
        c.append(f"list_contains(tagset, {sql_str('content_' + cw)})")
    for cw in f.get("cw_exclude") or []:
        c.append(f"not list_contains(tagset, {sql_str('content_' + cw)})")
    for st in f.get("status_ex") or []:
        c.append(f"not list_contains(tagset, {sql_str('system_' + st)})")
    for cw in f.get("cw_ex") or []:  # round-5 alias of cw_exclude
        c.append(f"not list_contains(tagset, {sql_str('content_' + cw)})")
    for t in f.get("tags") or []:
        c.append(f"list_contains(tagset, {sql_str(t)})")
    for t in f.get("tags_ex") or []:
        c.append(f"not list_contains(tagset, {sql_str(t)})")
    if f.get("author"):
        c.append(f"author_id = {sql_str(f['author'])}")
    return " and ".join(c)


def facet_truth(con, f: dict, top_tags: int = 20) -> dict:
    wh = where_sql(f)
    total, pc, quest, both = con.execute(
        f"select count(*), count(*) filter (has_pc), count(*) filter (has_quest), "
        f"count(*) filter (has_pc and has_quest) from w where {wh}"
    ).fetchone()
    tags = con.execute(
        f"select t, count(*) n from (select unnest(tagset) t from w where {wh}) "
        f"group by t order by n desc, t asc limit {top_tags}"
    ).fetchall()
    cap = con.execute(
        f"""select case when capacity <= 8 then '1-8' when capacity <= 16 then '9-16'
            when capacity <= 32 then '17-32' when capacity <= 64 then '33-64' else '65+' end b,
            count(*) from w where {wh} and capacity is not null group by b"""
    ).fetchall()
    return {
        "total": total,
        "platform": {"pc": pc, "quest": quest, "both": both},
        "tags": {t: n for t, n in tags},
        "capacity": {b: n for b, n in cap},
    }


def script_of(name: str) -> str:
    """'cjk' if the name contains any kana/han/hangul, else 'latin'."""
    return "cjk" if CJK_RE.search(name or "") else "latin"


HIDDEN_PREFIXES = ("admin_", "adm_", "feature_", "debug_", "system_")


def tag_class(t: str) -> str:
    """Mirror of the site's tag classes (DESIGN.md): hidden | status | cw | user."""
    if t in ("system_approved", "system_labs"):
        return "status"
    if t.startswith(HIDDEN_PREFIXES):
        return "hidden"
    if t.startswith("content_"):
        return "cw"
    return "user"


# ---------------------------------------------------------------- shared browser helpers
MiB = 1024 * 1024
LIB_DIR = Path.home() / ".cache/chromium-libs/usr/lib/x86_64-linux-gnu"
# Firefox cannot live in ~/.cache on cc-0 (home is ~100% full); see README "Firefox on cc-0".
FIREFOX_ROOT = Path(os.environ.get("VRCW_PW_BROWSERS", "/work/projects/vrcwscrape-pw-browsers"))


def launch(p):
    env = dict(os.environ)
    if LIB_DIR.exists():  # user-space copies of Chromium's shared libs (no root on cc-0)
        env["LD_LIBRARY_PATH"] = f"{LIB_DIR}:{env.get('LD_LIBRARY_PATH', '')}"
    return p.chromium.launch(env=env)


def launch_firefox(p):
    """Playwright Firefox from FIREFOX_ROOT (or the default cache); None if not installed."""
    exes = glob.glob(str(FIREFOX_ROOT / "firefox-*/firefox/firefox"))
    env = dict(os.environ)
    libs = [str(FIREFOX_ROOT / "fflibs/usr/lib/x86_64-linux-gnu")] if exes else []
    if exes:
        libs.insert(0, str(Path(exes[0]).parent))
    if LIB_DIR.exists():
        libs.append(str(LIB_DIR))
    if libs:
        env["LD_LIBRARY_PATH"] = ":".join(libs + [env.get("LD_LIBRARY_PATH", "")])
    try:
        return p.firefox.launch(env=env, executable_path=exes[0] if exes else None)
    except Exception:  # noqa: BLE001
        return None


def site_url(base: str, **params) -> str:
    params = {k: v for k, v in params.items() if v not in (None, "")}
    return base.rstrip("/") + "/" + (("?" + urlencode(params)) if params else "")


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))]


def load_jsonl(p: Path) -> list[dict]:
    return [json.loads(line) for line in open(p) if line.strip()]


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
WID_RE_JS = "/wrld_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/"


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
