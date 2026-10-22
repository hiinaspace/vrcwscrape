"""Round-6 checks: aligned percentile bars, upload/metrics history in the detail panel,
recently-updated filter and "most updated" sort.

URL / DOM / hook only. Test ids are resolved from candidate lists (SEL6; first = DESIGN.md round 6, update
when DESIGN lands). Truth: /work/projects/vrcwscrape-data/history/{uploads,metrics,summary}.parquet
(override with VRCW_HISTORY_DIR) and the public-v2 parquet. Every check returns a dict
(`{"skipped": why}`, `{"error": why}` or measurements); verdict_rows() turns them into graded rows.

Heavy-tail rule (coordinator, 2026-10-21): the summary line's total and last-upload are graded exactly for every
sampled world; individual `upload-tick`s are graded only for worlds with total <= TICK_MAX (60, env
VRCW_TICK_MAX); for heavier worlds the per-month density counts must sum to the true total.
"""

from __future__ import annotations

import os
import random
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from common import connect, site_url
from r5 import css, dom_ids, loc, open_site, req_bytes, SEL

HIST_DIR = Path(os.environ.get("VRCW_HISTORY_DIR", "/work/projects/vrcwscrape-data/history"))
TICK_MAX = int(os.environ.get("VRCW_TICK_MAX", "60"))
BAR_NAMES = ["cap", "pc", "quest", "visits", "fav"]  # DESIGN.md round 6: bar-cap|pc|quest|visits|fav
SEL.update(
    {
        "hist_panel": ["history-panel"],
        "hist_summary": ["history-summary"],
        "tick": ["upload-tick"],
        "month": ["upload-month"],
        "spark": ["visits-spark"],
        "spark_pt": ["visits-spark-point"],  # unused: the sparkline exposes data-n
    }
)
SORT_CANDIDATES = ["uploads"]  # Sort "uploads" = most updated (uploads_90d desc)
FILTER_KEY_CANDIDATES = ["recent_days"]  # Filters.recent_days: 7|30|90 days before the newest upload in the data
ASOF_FILES = ["asof.txt", "ASOF", "as_of.txt", "asof.json", "meta.json", "MANIFEST.json", "README.md"]


def have_history() -> str | None:
    miss = [f for f in ("uploads.parquet", "summary.parquet") if not (HIST_DIR / f).exists()]
    return f"history files missing in {HIST_DIR}: {miss}" if miss else None


def hcon(v2_parquet: str):
    con = connect(v2_parquet)
    con.execute(f"create view up as select * from read_parquet('{HIST_DIR}/uploads.parquet')")
    con.execute(f"create view sm as select * from read_parquet('{HIST_DIR}/summary.parquet')")
    if (HIST_DIR / "metrics.parquet").exists():
        con.execute(f"create view mt as select * from read_parquet('{HIST_DIR}/metrics.parquet')")
    return con


def asof_date(con) -> tuple[date, str]:
    if os.environ.get("VRCW_ASOF"):
        return date.fromisoformat(os.environ["VRCW_ASOF"]), "env VRCW_ASOF"
    try:
        import json

        j = json.loads((HIST_DIR / "asof.json").read_text())
        return date.fromisoformat(str(j["asof"])[:10]), "history/asof.json (reported; the filter rule is relative to the newest upload)"
    except Exception:  # noqa: BLE001
        pass
    for f in ASOF_FILES:
        p = HIST_DIR / f
        if p.exists():
            mm = re.search(r"as[-_ ]?of\D{0,20}(\d{4}-\d{2}-\d{2})", p.read_text(), re.I)
            if mm:
                return date.fromisoformat(mm.group(1)), f"history/{f}"
    d = con.execute("select max(last_upload)::date from sm").fetchone()[0]
    return d, "ASSUMED max(last_upload) (no documented as-of file found)"


# ---------------------------------------------------------------- 1+2. bars
BARS_JS = """
(a) => {
  const rows = [...document.querySelectorAll(a.row)].slice(0, a.limit);
  const WID = %s;
  return rows.map(r => {
    const rr = r.getBoundingClientRect();
    const m = (r.getAttribute('data-id') || r.outerHTML).match(WID);
    const bars = {};
    for (const t of r.querySelectorAll('[data-testid^="bar-"]')) {
      const name = t.getAttribute('data-testid').slice(4);
      if (name.startsWith('fill-')) continue;
      const trk = t.querySelector('.bar') || t;
      const b = trk.getBoundingClientRect();
      const fillEl = trk.querySelector('i') || trk.firstElementChild;
      let frac = null;
      const attr = t.getAttribute('data-pct') ?? t.getAttribute('aria-valuenow') ?? (fillEl && fillEl.getAttribute('data-pct'));
      if (fillEl && /%%$/.test(fillEl.style.width || '')) frac = parseFloat(fillEl.style.width);
      else if (attr !== null && attr !== undefined) { frac = parseFloat(attr); if (frac <= 1.0 && !/%%/.test(attr) && t.getAttribute('data-pct-scale') !== '100') frac = frac * 100; }
      else if (fillEl && b.width > 0) frac = 100 * fillEl.getBoundingClientRect().width / b.width;
      const tip = [t, trk, fillEl].map(e => e && (e.getAttribute('title') || e.getAttribute('data-tip') || e.getAttribute('aria-label') || '')).filter(Boolean).join(' | ');
      bars[name] = {left: b.left, width: b.width, right: b.right, frac, tip, none: t.classList.contains('none'), text: t.innerText || ''};
    }
    return {id: m ? m[0] : null, left: rr.left, right: rr.right, top: rr.top, bars};
  });
}
""" % "/wrld_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/"


def read_bars(page, limit: int) -> list[dict]:
    return page.evaluate(BARS_JS, {"row": css("result_item"), "limit": limit})


def alignment_eval(browser, base: str) -> dict:
    out = {}
    for label, vp in (("desktop", (1440, 900)), ("mobile", (390, 844))):
        ctx, page, _, errs = open_site(browser, base, viewport=vp)
        try:
            page.wait_for_selector(css("result_item"), timeout=20000)
            page.wait_for_timeout(800)
            rows = read_bars(page, 15)
            names = sorted({n for r in rows for n in r["bars"]})
            if not rows or not names:
                out[label] = {"error": f"{len(rows)} rows, bar tracks found: {names}"}
                continue
            bad, worst = [], 0.0
            for n in names:
                got = [r["bars"][n] for r in rows if n in r["bars"]]
                if len(got) < 2:
                    continue
                # mobile may stack rows (or bars) but a column must stay aligned across rows
                for key in ("left", "width"):
                    # compare relative to the row's own left edge (rows may be indented identically)
                    vals = [(b[key] - (r["left"] if key == "left" else 0)) for r, b in ((r, r["bars"][n]) for r in rows if n in r["bars"])]
                    spread = max(vals) - min(vals)
                    worst = max(worst, spread)
                    if spread > 1.0:
                        bad.append(f"{n}.{key} spread {spread:.1f}px")
                over = [r["id"] for r in rows if n in r["bars"] and (r["bars"][n]["right"] > r["right"] + 1 or r["bars"][n]["left"] < r["left"] - 1)]
                if over:
                    bad.append(f"{n} overflows row in {len(over)} rows")
            out[label] = {"rows": len(rows), "names": names, "worst_px": worst, "bad": bad, "pass": not bad, "page_errors": errs[:2]}
        finally:
            ctx.close()
    return out


def true_percentiles(con) -> dict[str, dict]:
    """bar name -> sorted values among worlds that have the value (DESIGN: cap > 0; pc/quest only with that package)."""
    cols = {r[0] for r in con.execute("describe w").fetchall()}
    fav = next((c for c in ("favorites", "favourites", "fav") if c in cols), None)
    spec = {"cap": "capacity", "pc": "pc_size_mb", "quest": "quest_size_mb", "visits": "visits", "fav": fav}
    out = {}
    for name, c in spec.items():
        if not c:
            continue
        cond = "capacity > 0" if name == "cap" else f"{c} is not null"
        out[name] = {"col": c, "vals": [r[0] for r in con.execute(f"select {c} from w where {cond} order by 1").fetchall()]}
    return out


def midrank(vals: list, v: float) -> float:
    import bisect

    lo, hi = bisect.bisect_left(vals, v), bisect.bisect_right(vals, v)
    return 100 * (lo + (hi - lo) / 2) / len(vals)


def percentile_eval(browser, base: str, con, n_sample: int = 50, seed: int = 61) -> dict:
    rnd = random.Random(seed)
    ctx, page, _, _ = open_site(browser, base)
    try:
        rows: dict[str, dict] = {}
        for sort in (None, "published", "visits"):
            page.goto(site_url(base, sort=sort), wait_until="load")
            page.wait_for_function("window.vrcwSearchReady !== undefined")
            page.evaluate("window.vrcwSearchReady")
            page.wait_for_selector(css("result_item"), timeout=20000)
            from r5 import collect_dom_ids

            collect_dom_ids(page, 120)
            for r in read_bars(page, 400):
                if r["id"] and r["bars"]:
                    rows.setdefault(r["id"], r)
        if not rows:
            return {"error": "no rendered rows with bar tracks"}
        ids = rnd.sample(sorted(rows), min(n_sample, len(rows)))
        tr = true_percentiles(con)
        cols = sorted({t["col"] for t in tr.values()})
        vals = {
            r[0]: dict(zip(cols, r[1:]))
            for r in con.execute(
                f"select world_id, {', '.join(cols)} from w where world_id in (%s)" % ",".join("'%s'" % i for i in ids)
            ).fetchall()
        }
        checked, bad, tipbad, nofrac = 0, [], [], 0
        for i in ids:
            for name, b in rows[i]["bars"].items():
                if name not in tr or i not in vals or b.get("none"):
                    continue
                v = vals[i].get(tr[name]["col"])
                if v is None or (name == "cap" and v < 1):
                    continue
                if b["frac"] is None:
                    nofrac += 1
                    continue
                checked += 1
                tp = midrank(tr[name]["vals"], v)
                # the site draws round(pct) with a 2% floor, so the comparison allows +-1 point plus that rounding
                if abs(b["frac"] - tp) > 1.5 and not (b["frac"] == 2 and tp < 3):
                    bad.append(f"{i[-8:]} {name} fill {b['frac']:.1f} vs true mid-rank {tp:.1f}")
                if not re.search(r"\d", b["tip"] or ""):
                    tipbad.append(f"{i[-8:]} {name}: tip={b['tip']!r}")
        return {"worlds": len(ids), "bars_checked": checked, "no_fill_value": nofrac, "bad": len(bad), "samples": bad[:5], "tip_bad": len(tipbad), "tip_samples": tipbad[:4], "pass": checked > 0 and not bad and not tipbad}
    finally:
        ctx.close()


# ---------------------------------------------------------------- 3. history
def sample_worlds(con, n: int = 30, seed: int = 71) -> list[dict]:
    # sampled by upload events (summary.uploads_total; > TICK_MAX events -> monthly bars)
    rnd = random.Random(seed)
    top = con.execute("select world_id, uploads_total from sm order by uploads_total desc limit 3").fetchall()
    have_m = "mt" in {r[0] for r in con.execute("show tables").fetchall()} or _has_view(con, "mt")
    def pick(where, k):
        q = f"select world_id from sm where {where}"
        r = [x[0] for x in con.execute(q).fetchall()]
        return rnd.sample(r, min(k, len(r)))
    heavy = pick("uploads_total >= 20", 9)
    mid = pick("uploads_total between 5 and 19", 8)
    light = pick("uploads_total between 1 and 4", 10)
    # make sure both sides of the 60-row threshold and the 200-row truncation are covered
    heavy += pick("uploads_total between 56 and 64", 2)
    heavy += pick("world_id in (select world_id from up group by 1 having count(*) > 200)", 3)
    seen, out = set(), []
    for w, tag in [(t[0], "top3") for t in top] + [(w, "heavy") for w in heavy] + [(w, "mid") for w in mid] + [(w, "light") for w in light]:
        if w not in seen:
            seen.add(w)
            out.append({"id": w, "kind": tag})
    return out[:n]


def _has_view(con, name):
    try:
        con.execute(f"select 1 from {name} limit 1")
        return True
    except duckdb.Error:
        return False


def true_events(con, wid: str) -> list:
    ts = [r[0] for r in con.execute("select created_at from up where world_id=? order by created_at", [wid]).fetchall()]
    ev = []
    for t in ts:  # chain rule: >10 min after the previous upload starts a new event
        if not ev or t - ev[-1] > timedelta(minutes=10):
            ev.append(t)
        else:
            ev[-1] = t
    return ev


def parse_total(text: str) -> int | None:
    m = re.search(r"([\d,]+)\s+(?:package\s+)?(?:uploads?|updates?|versions?|events?)", text, re.I)
    return int(m.group(1).replace(",", "")) if m else None


def last_ok(text: str, last: datetime, now: datetime) -> tuple[bool, str]:
    t = text.lower()
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", t)
    if m:
        return date(*map(int, m.groups())) == last.date(), f"abs {m.group(0)}"
    if re.search(r"\b(today|just now)\b", t):
        return (now - last).days < 1, "today"
    if "yesterday" in t:
        return 1 <= (now - last).days <= 2, "yesterday"
    m = re.search(r"(\d+)\s*(day|week|month|year|hour|minute)s?\s+ago", t)
    if m:
        k, unit = int(m.group(1)), m.group(2)
        days = (now - last).total_seconds() / 86400
        exp = {"minute": k / 1440, "hour": k / 24, "day": k, "week": 7 * k, "month": 30.44 * k, "year": 365.25 * k}[unit]
        tol = {"minute": 1, "hour": 1, "day": 1.01, "week": 7, "month": 16, "year": 183}[unit]
        return abs(days - exp) <= tol, f"rel {m.group(0)}"
    return False, "no parsable last-upload"


def count_spark_points(page) -> int | None:
    n = page.evaluate("(s) => { const e = document.querySelector(s); return e ? parseInt(e.getAttribute('data-n')) : null; }", css("spark"))
    if n is not None and n == n:
        return n
    return page.evaluate(
        """(a) => {
        const s = document.querySelector(a.spark); if (!s) return null;
        const pts = s.querySelectorAll(a.pt); if (pts.length) return pts.length;
        const pl = s.querySelector('polyline'); if (pl) return pl.getAttribute('points').trim().split(/\\s+/).filter(Boolean).length;
        if (s.dataset.points) return parseInt(s.dataset.points);
        const p = s.querySelector('path'); if (p) return (p.getAttribute('d').match(/[ML]/g) || []).length;
        return 0; }""",
        {"spark": css("spark"), "pt": css("spark_pt")},
    )


def history_eval(browser, base: str, con) -> dict:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    # no history fetch before a panel opens (home view incl. scroll and a query)
    ctx, page, reqs, _ = open_site(browser, base)
    page.wait_for_selector(css("result_item"), timeout=20000)
    page.mouse.wheel(0, 4000)
    page.wait_for_timeout(1000)
    pre = [r.url for r in reqs if re.search(r"/hist/|histor", r.url, re.I)]
    ctx.close()
    sample = sample_worlds(con)
    metrics_rows = {}
    if _has_view(con, "mt"):
        metrics_rows = dict(con.execute("select world_id, count(*) from mt group by 1").fetchall())
    rows, bytes_by = [], {}
    for s in sample:
        wid = s["id"]
        ev = true_events(con, wid)
        total, last = len(ev), (ev[-1] if ev else None)
        nrows = con.execute("select count(*) from up where world_id=?", [wid]).fetchone()[0]
        ctx, page, reqs, _ = open_site(browser, base, world=wid)
        row = {"id": wid, "kind": s["kind"], "true_total": total, "package_rows": nrows}
        try:
            page.wait_for_selector(css("hist_summary"), timeout=15000)
            page.wait_for_timeout(500)
            start = len(reqs)
            txt = loc(page, "hist_summary").first.inner_text()
            row["summary"] = txt[:120]
            row["hist_bytes"] = req_bytes([r for r in reqs if re.search(r"/hist/|histor", r.url, re.I)])
            row["hist_reqs"] = len([r for r in reqs if re.search(r"/hist/|histor", r.url, re.I)])
            gt = parse_total(txt)
            row["total_ok"] = gt == total
            row["summary_events"] = con.execute("select uploads_total from sm where world_id=?", [wid]).fetchone()[0]
            if last is not None:
                tl = page.evaluate("(s) => [...document.querySelector(s).querySelectorAll('[title]')].map(e => e.title).join(' ')", css("hist_summary"))
                row["last_ok"], row["last_how"] = last_ok(tl + " " + txt, last, now)
            nt = loc(page, "tick").count()
            row["ticks"] = nt
            # every stacked segment of a month carries the month's total in its <title> ("2026-09: 5 package uploads"): count each month once
            msum = page.evaluate(
                """(sel) => { const e=[...document.querySelectorAll(sel)]; if(!e.length) return null; const mo = {};
                  if (e.every(x => x.getAttribute('data-count') !== null)) return e.reduce((a,x)=>a+parseInt(x.getAttribute('data-count')),0);
                  for (const x of e) { const t=(x.querySelector('title')||{}).textContent||''; const m=t.match(/^(\\d{4}-\\d{2}):\\s*(\\d+)/); if (m) mo[m[1]]=parseInt(m[2]); }
                  return Object.values(mo).reduce((a,b)=>a+b,0);}""",
                css("month"),
            )
            row["month_sum"] = msum
            ut = row["summary_events"]  # round 6b: ticks / month bars count upload EVENTS = summary.uploads_total
            row["total_ok"] = row["total_ok"] and gt == ut
            ev_attr = page.evaluate("(s) => { const e = document.querySelector(s); return e && e.getAttribute('data-events') !== null ? parseInt(e.getAttribute('data-events')) : null; }", css("hist_panel"))
            row["data_events"] = ev_attr
            if ev_attr is not None and ev_attr != ut:
                row["total_ok"] = False
            if ut <= TICK_MAX:
                row["detail_ok"] = nt == ut
            else:
                row["detail_ok"] = msum == ut
            sp = count_spark_points(page)
            mr = metrics_rows.get(wid, 0)
            row["spark_pts"], row["metrics_rows"] = sp, mr
            row["spark_ok"] = (sp is not None and 0 < sp <= mr) if mr else sp in (0, None)
        except Exception as e:  # noqa: BLE001
            row["error"] = f"{type(e).__name__}: {str(e)[:120]}"
        finally:
            ctx.close()
        rows.append(row)
    ok = lambda k: sum(1 for r in rows if r.get(k) is True)
    n = len(rows)
    errs = [r for r in rows if "error" in r]
    hb = [r["hist_bytes"] for r in rows if "hist_bytes" in r]
    return {
        "n": n, "errors": len(errs), "error_samples": [(r["id"][-8:], r["error"]) for r in errs[:3]],
        "total_ok": ok("total_ok"), "last_ok": ok("last_ok"), "detail_ok": ok("detail_ok"), "spark_ok": ok("spark_ok"),
        "tick_threshold": TICK_MAX,
        "pre_open_history_requests": pre,
        "hist_bytes_median": sorted(hb)[len(hb) // 2] if hb else None,
        "hist_bytes_top3": [(r["id"][-8:], r["true_total"], r.get("hist_bytes")) for r in rows if r["kind"] == "top3"],
        "hist_bytes_max": max(hb) if hb else None,
        "bad_rows": [r for r in rows if "error" in r or not (r.get("total_ok") and r.get("last_ok", True) and r.get("detail_ok") and r.get("spark_ok"))][:5],
    }


# ---------------------------------------------------------------- 4. recently updated / most updated
def hook_query(page, **kw):
    return page.evaluate("async (a) => { await window.vrcwSearchReady; const r = await window.vrcwSearch.query(a); return {total: r.total, ids: r.ids}; }", kw)


def recent_eval(browser, base: str, con) -> dict:
    asof, how = asof_date(con)
    ctx, page, _, _ = open_site(browser, base)
    try:
        base_total = hook_query(page, q="", filters={}, limit=1)["total"]
        key = None
        for k in ([os.environ["VRCW_RECENT_KEY"]] if os.environ.get("VRCW_RECENT_KEY") else FILTER_KEY_CANDIDATES):
            try:
                if hook_query(page, q="", filters={k: 7}, limit=1)["total"] != base_total:
                    key = k
                    break
            except Exception:  # noqa: BLE001
                pass
        res = {"asof": str(asof), "asof_source": how, "filter_key": key, "counts": {}}
        if key is None:
            res["error"] = f"no recent-days filter key accepted by the hook (tried {FILTER_KEY_CANDIDATES}; set VRCW_RECENT_KEY)"
        else:
            for n in (7, 30, 90):
                # DESIGN: newest package upload within N days of the newest upload in the data (calendar days)
                truth = con.execute("select count(*) from sm where last_upload::date >= (select max(last_upload)::date from sm) - ?::int", [n]).fetchone()[0]
                got = hook_query(page, q="", filters={key: n}, limit=1)["total"]
                res["counts"][n] = {"truth": truth, "got": got, "ok": got == truth}
        # UI presets (when test ids exist) must show the same count in the URL-driven hook state
        skey = None
        p99 = con.execute("select quantile_disc(uploads_90d, 0.99) from sm").fetchone()[0]
        res["p99_uploads_90d"] = p99
        for s in ([os.environ["VRCW_SORT_MOST"]] if os.environ.get("VRCW_SORT_MOST") else SORT_CANDIDATES):
            try:
                r = hook_query(page, q="", filters={}, sort=s, limit=20)
                if r["ids"]:
                    vs = [x[0] for x in con.execute("select uploads_90d from sm where world_id in (%s)" % ",".join("'%s'" % i for i in r["ids"])).fetchall()]
                    if len(vs) == len(r["ids"]) and min(vs) >= (p99 or 0) and (max(vs) - min(vs) > 0 or (p99 or 0) > 0):
                        skey = s
                        res["top20_min_uploads_90d"] = min(vs)
                        break
                    res.setdefault("sort_tried", {})[s] = min(vs) if vs else None
            except Exception:  # noqa: BLE001
                pass
        res["sort_key"] = skey
        res["sort_ok"] = skey is not None
        return res
    finally:
        ctx.close()


# ---------------------------------------------------------------- verdict rows + driver
def verdict_rows(m: dict) -> list[tuple[str, str, str, bool | None]]:
    rows = []

    def add(name, value, target, ok):
        rows.append((name, value, target, ok))

    def skip(key, name, target):
        d = m.get(key)
        if not d:
            return True
        if "skipped" in d:
            add(name, "skipped: " + d["skipped"], target, None)
            return True
        if "error" in d:
            add(name, d["error"], target, False)
            return True
        return False

    if not skip("r6_align", "bars aligned across first 15 rows (desktop/mobile)", "+-1 px, no overflow"):
        for lab, d in m["r6_align"].items():
            add(f"bar tracks aligned, {lab}", d.get("error") or f"{d['rows']} rows, bars {d['names']}, worst spread {d['worst_px']:.1f}px" + (f"; {d['bad'][:3]}" if d.get("bad") else ""), "<= 1 px, no overflow", d.get("pass", False))
    if not skip("r6_pct", "bar fill = percentile rank", "+-1 point"):
        d = m["r6_pct"]
        add(f"bar fill vs true percentile rank ({d['worlds']} worlds)", f"{d['bars_checked']} bars, {d['bad']} off" + (f"; {d['samples'][:2]}" if d["bad"] else "") + (f"; {d['no_fill_value']} bars exposed no value" if d["no_fill_value"] else ""), "+-1 point", d["bad"] == 0 and d["bars_checked"] > 0)
        add("bar tooltips contain the numeric value", f"{d['tip_bad']} missing" + (f"; {d['tip_samples'][:2]}" if d["tip_bad"] else ""), "0 missing", d["tip_bad"] == 0 and d["bars_checked"] > 0)
    if not skip("r6_hist", "history panel", "exact"):
        d = m["r6_hist"]
        n = d["n"]
        add(f"history summary total == distinct upload events ({n} worlds, incl. top-3 heaviest)", f"{d['total_ok']}/{n}" + (f"; errors {d['error_samples']}" if d["errors"] else ""), "exact", d["total_ok"] == n)
        add("history summary last-upload date", f"{d['last_ok']}/{n}", "exact", d["last_ok"] == n)
        add(f"upload ticks == events (summary.uploads_total <= {d['tick_threshold']}); heavier: upload-month counts sum to uploads_total", f"{d['detail_ok']}/{n}", "exact", d["detail_ok"] == n)
        add("sparkline points in (0, metrics rows]", f"{d['spark_ok']}/{n}", "all", d["spark_ok"] == n)
        add("no history fetch before a detail panel opens", f"{len(d['pre_open_history_requests'])} requests", "0", not d["pre_open_history_requests"])
        add("history bytes per panel open (report only)", f"median {d['hist_bytes_median']} B, max {d['hist_bytes_max']} B; top-3 heaviest (id,events,bytes) {d['hist_bytes_top3']}", "report only", None)
        if d.get("bad_rows"):
            add("history sample failures (report only)", str([{k: r.get(k) for k in ('id', 'true_total', 'summary', 'ticks', 'month_sum', 'error')} for r in d["bad_rows"][:3]])[:400], "-", None)
    if not skip("r6_recent", "recently-updated filter / most-updated sort", "exact"):
        d = m["r6_recent"]
        add(f"recently updated 7/30/90 d counts vs DuckDB (rule: within N days of newest upload; data as-of {d['asof']}: {d['asof_source']})", ", ".join(f"{n}d {c['got']}/{c['truth']}" for n, c in d["counts"].items()), "exact", all(c["ok"] for c in d["counts"].values()))
        add(f"'most updated' top 20 have uploads_90d >= p99 ({d['p99_uploads_90d']})", f"sort={d['sort_key']}, min {d.get('top20_min_uploads_90d')}" + ("" if d["sort_ok"] else f"; tried {d.get('sort_tried')}"), ">= p99", d["sort_ok"])
    return rows


def run_all(playwright, browser, base: str, parquet: str, outdir: Path, a) -> dict:
    m: dict = {}

    def guard(key, fn):
        try:
            m[key] = fn()
        except Exception as e:  # noqa: BLE001
            m[key] = {"error": f"harness error: {type(e).__name__}: {str(e)[:300]}"}

    con = connect(parquet)
    guard("r6_align", lambda: alignment_eval(browser, base))
    guard("r6_pct", lambda: percentile_eval(browser, base, con))
    why = have_history()
    if why:
        for k in ("r6_hist", "r6_recent"):
            m[k] = {"skipped": why}
        return m
    hc = hcon(parquet)
    guard("r6_hist", lambda: history_eval(browser, base, hc))
    guard("r6_recent", lambda: recent_eval(browser, base, hc))
    return m
