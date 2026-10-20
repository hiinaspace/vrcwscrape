"""Round-5 checks: real-UI semantic toggle, boolean query truth, include/exclude facets, trending
default, self-hosted thumbnails, nothing-hidden-by-default, owner screenshots.

Everything talks to the site through its URL / DOM test ids / `window.vrcwSearch` contract only.
Test ids are resolved from candidate lists (SEL) so the harness survives renames; the first
candidate is the one DESIGN.md documents. Every check returns a dict (never raises): `{"skipped": why}`,
`{"error": why}` or its measurements; `verdict_rows()` turns them into graded rows for run.py.
"""

from __future__ import annotations

import hashlib
import os
import random
import re
import statistics
import time
from pathlib import Path
from urllib.parse import urlsplit

import bool_truth as bt
from common import (
    QUERY_JS,
    WID_RE_JS,
    MiB,
    connect,
    facet_eval,
    facet_truth,
    launch_firefox,
    site_url,
    where_sql,
)

SEL = {
    "search_input": ["search-input"],
    "sem_toggle": ["sem-toggle", "semantic-toggle"],
    "sem_enable": ["sem-enable", "sem-confirm", "sem-download"],
    "sem_item": ["semantic-item"],
    "sem_state": ["sem-state", "sem-status", "sem-note"],
    "result_item": ["result-item"],
    "results": ["results"],
    "pill": ["pill", "query-pill", "active-pill"],
    "detail": ["detail-panel", "world-detail", "world-card"],
    "detail_close": ["detail-close", "world-close"],
    "hist_date": ["date-hist", "hist-date", "date-histogram", "uploads-histogram", "facet-date"],
    "load_more": ["load-more", "more-results"],
}
VRCHAT_HOST_RE = re.compile(r"(^|\.)vrchat\.(cloud|com|net)$", re.IGNORECASE)
SEM_QUERY = "cozy cabin in the snow"
SEM_TIMEOUT_S = int(os.environ.get("VRCW_SEM_TIMEOUT_S", "180"))  # selftest shortens it
ADULT_TAGS = ("content_sex", "content_adult")


def css(key: str) -> str:
    return ", ".join(f'[data-testid="{c}"]' for c in SEL[key])


def loc(page, key: str):
    return page.locator(css(key))


def vrchat_requests(urls: list[str]) -> list[str]:
    bad = []
    for u in urls:
        try:
            if VRCHAT_HOST_RE.search(urlsplit(u).hostname or ""):
                bad.append(u)
        except ValueError:
            pass
    return bad


def dom_ids(page, limit: int = 400) -> list[str | None]:
    return page.evaluate(
        f"""(lim) => [...document.querySelectorAll({css("result_item")!r})].slice(0, lim).map(e => {{
          const m = (e.getAttribute('data-id') || e.outerHTML).match({WID_RE_JS});
          return m ? m[0] : null; }})""",
        limit,
    )


def collect_dom_ids(page, want: int, max_rounds: int = 25) -> list[str | None]:
    """Result ids rendered in the DOM; scrolls / clicks a load-more control to reach `want`."""
    last = -1
    for _ in range(max_rounds):
        ids = dom_ids(page, want)
        if len(ids) >= want or len(ids) == last:
            break
        last = len(ids)
        more = loc(page, "load_more")
        if not more.count():  # no test id: a visible "Show more" / "Load more" button
            more = page.get_by_role("button", name=re.compile("(show|load) more", re.I))
        if more.count() and more.first.is_visible():
            more.first.click()
        page.mouse.wheel(0, 6000)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(350)
    return dom_ids(page, want)


def open_first_result(page) -> None:
    """Open a result's detail panel the way a user does: the UI only listens on the row's `?world=` links."""
    links = page.locator(css("result_item") + ' a[href^="?world="]')
    (links.nth(1) if links.count() > 1 else links.first).click()  # nth(1) = the title link
    # `detail-panel` is an always-present hidden <aside>; the open state is a visible world-card inside it
    page.wait_for_selector(
        '[data-testid="world-card"]:visible, [data-testid="detail-panel"]:not([hidden])',
        timeout=10000,
    )


def open_site(browser, base: str, viewport=(1440, 900), **params):
    ctx = browser.new_context(viewport={"width": viewport[0], "height": viewport[1]})
    page = ctx.new_page()
    reqs: list = []
    page.on("request", lambda r: reqs.append(r))
    errors: list[str] = []
    page.on("pageerror", lambda e: errors.append(str(e)[:200]))
    page.goto(site_url(base, **params), wait_until="load", timeout=45000)
    page.wait_for_function("window.vrcwSearchReady !== undefined", timeout=45000)
    page.evaluate("window.vrcwSearchReady")
    return ctx, page, reqs, errors


def req_bytes(reqs: list, start: int = 0) -> int:
    n = 0
    for r in reqs[start:]:
        try:
            s = r.sizes()
            n += max(0, s["responseBodySize"]) + max(0, s["responseHeadersSize"])
        except Exception:  # noqa: BLE001
            pass
    return n


# ---------------------------------------------------------------- 1. semantic toggle via the real UI
ENABLED_JS = """
(sels) => {
  const t = document.querySelector(sels.toggle);
  const st = sels.state ? [...document.querySelectorAll(sels.state)] : [];
  const o = {toggle_present: !!t, toggle_visible: false, checked: null, aria: null, cls: '', data: '',
             state_text: st.map(e => (e.innerText || e.textContent || '').trim()).join(' | ').slice(0, 160)};
  if (t) {
    const r = t.getBoundingClientRect();
    o.toggle_visible = r.width > 0 && r.height > 0;
    o.checked = t.checked === undefined ? null : t.checked;
    o.aria = t.getAttribute('aria-checked') || t.getAttribute('aria-pressed');
    o.cls = t.className && t.className.baseVal === undefined ? t.className : '';
    o.data = t.getAttribute('data-state') || t.getAttribute('data-enabled') || '';
  }
  return o;
}
"""
ON_RE = re.compile(r"\b(on|enabled|ready|active|loaded|true)\b", re.IGNORECASE)


def semantic_ui_case(browser, base: str, query: str) -> dict:
    t_all = time.perf_counter()
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    page = ctx.new_page()
    reqs: list = []
    page.on("request", lambda r: reqs.append(r))
    dialogs: list[str] = []
    page.on("dialog", lambda d: (dialogs.append(d.message[:100]), d.accept()))
    errors: list[str] = []
    page.on("pageerror", lambda e: errors.append(str(e)[:200]))
    out: dict = {"query": query}
    try:
        page.goto(site_url(base), wait_until="load", timeout=45000)
        page.wait_for_selector(css("result_item"), timeout=45000)
        if query:
            page.locator(css("search_input")).first.fill(query)
            page.wait_for_timeout(700)
        tog = loc(page, "sem_toggle")
        if not tog.count():
            return {**out, "fail": "no sem-toggle element in the DOM"}
        body0 = hashlib.md5(page.inner_text("body").encode()).hexdigest()
        start = len(reqs)
        tog.first.scroll_into_view_if_needed()
        tog.first.click(timeout=5000)
        page.wait_for_timeout(600)
        out["dom_changed_after_toggle_click"] = (
            hashlib.md5(page.inner_text("body").encode()).hexdigest() != body0
        )
        enable = loc(page, "sem_enable")
        if enable.count() and enable.first.is_visible():
            enable.first.click(timeout=5000)
            out["clicked_enable"] = True
        t0 = time.perf_counter()
        if query:
            try:
                page.wait_for_selector(css("sem_item"), timeout=SEM_TIMEOUT_S * 1000)
            except Exception:  # noqa: BLE001
                pass
            out["semantic_items"] = loc(page, "sem_item").count()
        else:
            page.wait_for_timeout(1500)
            deadline = time.time() + SEM_TIMEOUT_S
            while time.time() < deadline:
                st = page.evaluate(
                    ENABLED_JS, {"toggle": css("sem_toggle"), "state": css("sem_state")}
                )
                if _enabled(st):
                    break
                page.wait_for_timeout(500)
        out["wait_s"] = round(time.perf_counter() - t0, 2)
        out["state"] = page.evaluate(
            ENABLED_JS, {"toggle": css("sem_toggle"), "state": css("sem_state")}
        )
        out["enabled_visible"] = _enabled(out["state"])
        out["bytes"] = req_bytes(reqs, start)
        out["native_dialogs"] = dialogs
        out["page_errors"] = errors[:3]
        out["total_s"] = round(time.perf_counter() - t_all, 2)
        if query:
            out["pass"] = out["semantic_items"] >= 1 and out["dom_changed_after_toggle_click"]
        else:
            out["pass"] = bool(out["enabled_visible"]) and out["dom_changed_after_toggle_click"]
    except Exception as e:  # noqa: BLE001
        out["fail"] = f"{type(e).__name__}: {str(e)[:200]}"
        out["pass"] = False
    finally:
        ctx.close()
    return out


def _enabled(st: dict) -> bool:
    if not st.get("toggle_present") and not st.get("state_text"):
        return False
    if st.get("checked") or st.get("aria") == "true" or ON_RE.search(st.get("data") or ""):
        return st.get("toggle_visible", False) or bool(st.get("state_text"))
    if re.search(r"\b(on|active|enabled)\b", st.get("cls") or "", re.IGNORECASE):
        return True
    return bool(st.get("state_text") and ON_RE.search(st["state_text"]))


def semantic_ui(playwright, chromium, base: str) -> dict:
    res: dict = {}
    engines = [("chromium", chromium)]
    ff = launch_firefox(playwright)
    if ff is not None:
        engines.append(("firefox", ff))
    else:
        res["firefox"] = {"skipped": "firefox not installed (see README)"}
    for name, br in engines:
        res[name] = {
            "with_query": semantic_ui_case(br, base, SEM_QUERY),
            "empty_query": semantic_ui_case(br, base, ""),
        }
    if ff is not None:
        ff.close()
    return res


# ---------------------------------------------------------------- 2. boolean queries
def build_bool_truth(parquet: str):
    con = connect(parquet)
    t0 = time.time()
    stats = bt.build_docs(con)
    qs = bt.generate(con, stats)
    return con, qs, time.time() - t0


def _within(total: int, truth: int) -> bool:
    return abs(total - truth) <= max(0.03 * truth, 2 if truth < 100 else 0)


def boolean_eval(page, con, queries: list[dict]) -> dict:
    """Grade the site's `ids`/`total` for each generated query against DuckDB truth (see SEMANTICS.md).

    precision (gated, 1.0): every returned id satisfies the lenient superset P (positive terms may
    prefix-match; negated terms must be absent as whole words).
    total (gated): inside the sandwich [min mode count, max mode count] +-3%; "tight" when also within
    +-3% of the documented rule (L or LG), reported as its own graded row.
    """
    items = [{"q": q["q"], "filters": {}, "limit": 50} for q in queries]
    res = []
    for i in range(0, len(items), 20):
        res += page.evaluate(QUERY_JS, items[i : i + 20])
    rows, fails = [], []
    for q, r in zip(queries, res):
        ids = r["ids"] or []
        node = q["node"]
        ok_p = bt.member_ids(con, node, "P", ids)
        ok_doc = bt.member_ids(con, node, "LG", ids)
        ok_s = bt.member_ids(con, node, "S", ids)
        n = bt.counts(con, node)
        total = r["total"]
        lo, hi = min(n.values()), max(n.values())
        sandwich = total is not None and lo * 0.97 - 2 <= total <= hi * 1.03 + 2
        tight = [m for m in ("L", "LG") if total is not None and _within(total, n[m])]
        row = {
            "form": q["form"],
            "q": q["q"],
            "gate": q["gate"],
            "returned": len(ids),
            "precision_P": len(ok_p) / len(ids) if ids else 0.0,
            "precision_doc": len(ok_doc) / len(ids) if ids else 0.0,
            "precision_S": len(ok_s) / len(ids) if ids else 0.0,
            "total": total,
            "truth": n,
            "sandwich": sandwich,
            "tight": bool(tight),
            "err": r["err"],
        }
        row["ok"] = bool(
            not r["err"]
            and ids
            and row["precision_P"] == 1.0
            and sandwich
            and len(ids) >= min(50, lo)
        )
        rows.append(row)
        if q["gate"] and not row["ok"]:
            fails.append(row)
    gated = [r for r in rows if r["gate"]]
    by_form: dict[str, dict] = {}
    for r in rows:
        f = by_form.setdefault(r["form"], {"n": 0, "ok": 0, "tight": 0})
        f["n"] += 1
        f["ok"] += r["ok"]
        f["tight"] += r["tight"]
    return {
        "n": len(rows),
        "n_gated": len(gated),
        "ok_gated": sum(r["ok"] for r in gated),
        "tight_gated": sum(r["tight"] for r in gated),
        "precision_P_min": min((r["precision_P"] for r in gated), default=None),
        "precision_doc_mean": statistics.mean(r["precision_doc"] for r in gated) if gated else None,
        "strict_precision_mean": statistics.mean(r["precision_S"] for r in gated)
        if gated
        else None,
        "by_form": by_form,
        "failures": fails[:12],
        "rows": rows,
    }


def boolean_ui_agreement(
    browser, base: str, queries: list[dict], hook_rows: list[dict], n: int = 6
):
    """Type queries in the real search box; the first 10 rendered ids must overlap the hook's >= 8."""
    out = []
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    page = ctx.new_page()
    try:
        page.goto(site_url(base), wait_until="load", timeout=45000)
        page.wait_for_selector(css("result_item"), timeout=45000)
        step = max(1, len(queries) // n)
        for q, row in list(zip(queries, hook_rows))[::step][:n]:
            box = page.locator(css("search_input")).first
            box.fill(q["q"])
            page.wait_for_timeout(1200)
            ids = [i for i in dom_ids(page, 10) if i]
            hook = (
                page.evaluate(QUERY_JS, [{"q": q["q"], "filters": {}, "limit": 10}])[0]["ids"] or []
            )
            out.append(
                {
                    "q": q["q"],
                    "dom": len(ids),
                    "overlap": len(set(ids) & set(hook)),
                    "hook": len(hook),
                }
            )
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {str(e)[:200]}"}
    finally:
        ctx.close()
    ok = all(o["overlap"] >= min(8, o["hook"]) for o in out) and bool(out)
    return {"rows": out, "ok": ok}


# ---------------------------------------------------------------- 3. include/exclude facets
def month_index(col: str) -> str:
    return f"((year({col}) - 2010) * 12 + month({col}) - 1)"


def hist_truth(con, f: dict, which: str) -> dict[int, int]:
    """Worlds per month (index since 2010-01) with every filter EXCEPT the histogram's own range."""
    g = dict(f)
    for k in ("pub_after", "pub_before") if which == "created" else ("after", "before"):
        g.pop(k, None)
    col = "created_at" if which == "created" else "updated_at"
    rows = con.execute(
        f"select {month_index(col)} m, count(*) from w where {where_sql(g)} and {col} is not null group by m"
    ).fetchall()
    return {int(m): int(c) for m, c in rows}


def hist_diff(got, truth: dict[int, int]) -> str | None:
    if not isinstance(got, dict) or "m0" not in got or "counts" not in got:
        return "missing"
    have = {got["m0"] + i: c for i, c in enumerate(got["counts"]) if c}
    want = {m: c for m, c in truth.items() if c}
    if have != want:
        bad = sorted(set(have) | set(want), key=int)
        for m in bad:
            if have.get(m, 0) != want.get(m, 0):
                return f"month {m}: {have.get(m, 0)} != {want.get(m, 0)}"
    return None


def cap_hist_diff(got, con, f: dict) -> str | None:
    g = {k: v for k, v in f.items() if k not in ("cap_min", "cap_max")}
    truth = dict(
        con.execute(
            f"select capacity, count(*) from w where {where_sql(g)} and capacity is not null group by 1"
        ).fetchall()
    )
    if not isinstance(got, list):
        return "missing"
    have = {i: c for i, c in enumerate(got) if c}
    want = {int(k): int(v) for k, v in truth.items() if 0 <= k < len(got)}
    if have != want:
        for k in sorted(set(have) | set(want)):
            if have.get(k, 0) != want.get(k, 0):
                return f"capacity {k}: {have.get(k, 0)} != {want.get(k, 0)}"
    return None


def facet_ex_combos(con, n: int = 40, seed: int = 29) -> list[dict]:
    rnd = random.Random(seed)
    nrows = con.execute("select count(*) from w").fetchone()[0]
    floor = max(6, int(300 * nrows / 198000))
    tags = [
        r[0]
        for r in con.execute(
            f"select t from (select unnest(tagset) t from w) group by t having count(*) >= {floor} "
            "order by count(*) desc, t limit 60"
        ).fetchall()
        if not t_hidden(r[0])
    ]
    cws = [
        r[0][len("content_") :]
        for r in con.execute(
            "select t from (select unnest(tagset) t from w) where t like 'content\\_%' escape '\\' "
            f"group by t having count(*) >= {max(5, floor // 2)} order by count(*) desc limit 8"
        ).fetchall()
    ]
    lo, hi = con.execute("select min(created_at), max(created_at) from w").fetchone()
    out = []
    for i in range(n):
        f: dict = {}
        kinds = rnd.sample(
            [
                "tags_ex",
                "status_ex",
                "cw_exclude",
                "platform_ex",
                "tags",
                "cw",
                "status",
                "plat",
                "cap",
                "pub",
            ],
            rnd.randint(1, 4),
        )
        if "tags_ex" in kinds or i % 5 == 0:
            f["tags_ex"] = rnd.sample(
                tags[: max(3, min(40, len(tags)))], min(len(tags), rnd.choice([1, 1, 2, 3]))
            )
        if "status_ex" in kinds:
            f["status_ex"] = rnd.choice([["approved"], ["labs"], ["approved", "labs"]])
        if "cw_exclude" in kinds and cws:
            f["cw_exclude"] = rnd.sample(cws, min(len(cws), rnd.choice([1, 2, 3])))
        if "platform_ex" in kinds:
            f["platform_ex"] = rnd.choice(["pc", "quest"])
        if "tags" in kinds and tags:
            inc = rnd.sample(tags[: min(30, len(tags))], 1)
            f["tags"] = [t for t in inc if t not in f.get("tags_ex", [])]
        if "cw" in kinds and cws and "cw_exclude" not in f:
            f["cw"] = rnd.sample(cws, 1)
        if "status" in kinds and "status_ex" not in f:
            f["status"] = [rnd.choice(["approved", "labs"])]
        if "plat" in kinds and "platform_ex" not in f:
            f["platform"] = rnd.choice(["pc", "quest", "both"])
        if "cap" in kinds:
            f["cap_min"] = rnd.choice([4, 8, 16, 32])
        if "pub" in kinds:
            d1 = lo + (hi - lo) * rnd.random()
            d2 = d1 + (hi - d1) * rnd.random()
            f["pub_after"], f["pub_before"] = d1.strftime("%Y-%m-%d"), d2.strftime("%Y-%m-%d")
        out.append({"filters": f, "truth": facet_truth(con, f)})
    return out


def t_hidden(t: str) -> bool:
    return t.startswith(("admin_", "adm_", "feature_", "debug_", "system_", "content_"))


def facet_ex_eval(page, con, combos: list[dict]) -> dict:
    items = [{"q": "", "filters": c["filters"], "limit": 10} for c in combos]
    res = []
    for i in range(0, len(items), 20):
        res += page.evaluate(QUERY_JS, items[i : i + 20])
    out = facet_eval(combos, res)
    hist_bad, checked = [], 0
    for c, r in zip(combos, res):
        fc = r.get("facetCounts") or {}
        for key, which in (("months", "created"), ("updatedMonths", "updated")):
            d = hist_diff(fc.get(key), hist_truth(con, c["filters"], which))
            checked += 1
            if d:
                hist_bad.append({"filters": c["filters"], "hist": key, "diff": d})
        d = cap_hist_diff(fc.get("capacityHist"), con, c["filters"])
        checked += 1
        if d:
            hist_bad.append({"filters": c["filters"], "hist": "capacityHist", "diff": d})
    out["hist_checked"] = checked
    out["hist_bad"] = len(hist_bad)
    out["hist_sample_failures"] = hist_bad[:5]
    return out


# ---------------------------------------------------------------- 4. trending default
def trending_eval(page, base: str, browser, con, parquet: str) -> dict:
    cols = {r[0] for r in con.execute("describe w").fetchall()}
    if "recent_visits_per_day" not in cols:
        return {"skipped": "parquet has no recent_visits_per_day (need the v2 parquet)"}
    (p95,) = con.execute("select quantile_cont(recent_visits_per_day, 0.95) from w").fetchone()
    rv = dict(con.execute("select world_id, recent_visits_per_day from w").fetchall())

    def med(ids):
        ids = [i for i in ids if i]
        return statistics.median((rv.get(i) or 0.0) for i in ids) if ids else None, len(ids)

    hook = (
        page.evaluate(QUERY_JS, [{"q": "", "filters": {}, "sort": "trending", "limit": 20}])[0][
            "ids"
        ]
        or []
    )
    ctx, pg, _, _ = open_site(browser, base)
    try:
        pg.wait_for_selector(css("result_item"), timeout=30000)
        pg.wait_for_timeout(500)
        dom = dom_ids(pg, 20)
    finally:
        ctx.close()
    m_hook, n_hook = med(hook)
    m_dom, n_dom = med(dom)
    return {
        "p95_all": p95,
        "median_top20_hook": m_hook,
        "n_hook": n_hook,
        "median_top20_dom": m_dom,
        "n_dom": n_dom,
        "pass": bool(m_hook is not None and m_hook >= p95 and m_dom is not None and m_dom >= p95),
    }


# ---------------------------------------------------------------- 5. thumbnails
THUMB_JS = """
async (sel) => {
  const imgs = [...document.querySelectorAll(sel + ' img')];
  await Promise.all(imgs.map(i => i.decode ? i.decode().catch(() => null) : null));
  return imgs.map(i => ({src: (i.currentSrc || i.src || '').slice(0, 80), w: i.naturalWidth, h: i.naturalHeight,
                         complete: i.complete}));
}
"""


def thumbs_eval(browser, base: str, parquet: str, wid: str) -> dict:
    out: dict = {}
    # first view + results page: no VRChat hosts, thumbnail bytes
    ctx, page, reqs, errors = open_site(browser, base)
    try:
        page.wait_for_selector(css("result_item"), timeout=30000)
        for _ in range(4):
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(500)
        page.wait_for_timeout(1500)
        out["first_view_vrchat_requests"] = vrchat_requests([r.url for r in reqs])[:5]
        out["first_view_requests"] = len(reqs)
        n_before = len(reqs)
        # detail panel: UI click path
        page.evaluate("window.scrollTo(0, 0)")
        open_first_result(page)
        page.wait_for_timeout(2000)
        out["detail_panel_vrchat_requests"] = vrchat_requests([r.url for r in reqs[n_before:]])[:5]
        out["detail_has_official_link"] = bool(
            page.locator('a[href*="vrchat.com/home/world"]').count()
        )
        out["detail_requests"] = len(reqs) - n_before
        out["errors"] = errors[:3]
    except Exception as e:  # noqa: BLE001
        out["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    finally:
        ctx.close()
    # sampled thumbnails across several result pages
    seen: dict[str, dict] = {}
    page_bytes = []
    for params in (
        {},
        {"q": "horror"},
        {"sort": "published"},
        {"q": "avatar"},
        {"q": "club"},
        {"q": "pool"},
    ):
        if len(seen) >= 100:
            break
        ctx, page, reqs, _ = open_site(browser, base, **params)
        try:
            page.wait_for_selector(css("result_item"), timeout=30000)
            for _ in range(5):
                page.mouse.wheel(0, 2500)
                page.wait_for_timeout(400)
            page.wait_for_timeout(2000)
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:  # noqa: BLE001
                pass
            try:  # packed thumbs are filled in lazily (IntersectionObserver + object URL)
                page.wait_for_function(
                    f"document.querySelector({css('result_item')!r} + ' img') !== null",
                    timeout=6000,
                )
            except Exception:  # noqa: BLE001
                pass
            th = page.evaluate(THUMB_JS, css("result_item"))
            for t in th:
                seen.setdefault(t["src"] + str(len(seen)), t)
            thumb_req = [r for r in reqs if "thumb" in r.url]
            page_bytes.append(
                {
                    "params": params,
                    "items": len(th),
                    "thumb_requests": len(thumb_req),
                    "thumb_bytes": req_bytes(thumb_req),
                    "all_bytes": req_bytes(reqs),
                }
            )
        except Exception as e:  # noqa: BLE001
            page_bytes.append({"params": params, "error": str(e)[:120]})
        finally:
            ctx.close()
    imgs = list(seen.values())[:100]
    real = [
        t
        for t in imgs
        if t["src"].startswith(("blob:", "data:image/avif", "data:image/webp"))
        or "/thumbs/" in t["src"]
    ]
    out["pages"] = page_bytes
    if not real:
        out["decode"] = {
            "skipped": "no thumbs packed: the first result rows contain no <img>, only placeholders "
            "(thumbUrl null / no thumbs in the build)"
        }
        out["no_thumbs_packed"] = True
    else:
        ok = sum(1 for t in real if t["w"] > 0)
        out["decode"] = {
            "sampled": len(real),
            "decoded": ok,
            "rate": ok / len(real),
            "foreign_src": [t["src"] for t in imgs if t not in real][:3],
        }
    return out


# ---------------------------------------------------------------- 6. nothing hidden by default
def default_visibility_eval(page, browser, base: str, con, want: int = 200) -> dict:
    """`?sort=published`, no other params: rendered ids == the engine's top ids; adult-tagged worlds show."""
    adult = {
        r[0]
        for r in con.execute(
            "select world_id from w where list_contains(tagset,'content_sex') or list_contains(tagset,'content_adult')"
        ).fetchall()
    }
    hook = page.evaluate(QUERY_JS, [{"q": "", "filters": {}, "sort": "published", "limit": want}])[
        0
    ]
    hook_ids = hook["ids"] or []
    truth_ids = [
        r[0]
        for r in con.execute(
            f"select world_id from w order by created_at desc nulls last, world_id limit {want}"
        ).fetchall()
    ]
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    pg = ctx.new_page()
    try:
        pg.goto(site_url(base, sort="published"), wait_until="load", timeout=45000)
        pg.wait_for_selector(css("result_item"), timeout=30000)
        pg.wait_for_timeout(600)
        dom = collect_dom_ids(pg, want)
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {str(e)[:200]}"}
    finally:
        ctx.close()
    n = len(dom)
    dom_set = set(i for i in dom if i)
    expected = set(hook_ids[:n])
    return {
        "dom_n": n,
        "unparsed_ids": sum(1 for i in dom if i is None),
        "dom_vs_engine_top_n_overlap": len(dom_set & expected) / max(1, n),
        "dom_vs_duckdb_published_overlap": len(dom_set & set(truth_ids[:n])) / max(1, n),
        "adult_in_dom": len(dom_set & adult),
        "adult_expected_in_top_n": len(expected & adult),
        "adult_in_hook_top_200": len(set(hook_ids) & adult),
        "pass": bool(
            n >= 20
            and len(dom_set & expected) / max(1, n) >= 0.98
            and len(dom_set & adult) >= len(expected & adult)
            and len(set(hook_ids) & adult) >= 1
        ),
    }


# ---------------------------------------------------------------- 7. owner screenshots
def _shot(page, path: Path) -> str:
    page.wait_for_timeout(900)
    page.screenshot(path=str(path))
    return "ok"


def owner_screenshots(browser, base: str, parquet: str, outdir: Path) -> dict:
    status: dict[str, str] = {}
    for vp, (w, h) in {"desktop": (1440, 900), "mobile": (390, 844)}.items():
        ctx = browser.new_context(viewport={"width": w, "height": h})
        views = [
            ("r5_zero_state", {}, None),
            ("r5_query_pills_lake_-hangout", {"q": "lake -hangout"}, None),
            ("r5_detail_panel", {}, "detail"),
            ("r5_histogram_brushed", {}, "brush"),
        ]
        for name, params, action in views:
            fn = f"{name}_{w}x{h}.png"
            page = ctx.new_page()
            try:
                page.goto(site_url(base, **params), wait_until="load", timeout=45000)
                page.evaluate("window.vrcwSearchReady")
                page.wait_for_selector(css("result_item"), timeout=30000)
                if name.startswith("r5_query_pills"):
                    try:
                        page.wait_for_selector(css("pill"), timeout=4000)
                    except Exception:  # noqa: BLE001
                        status[fn + ":pills"] = "no pill element found (testid pill)"
                if action == "detail":
                    open_first_result(page)
                elif action == "brush":
                    hist = loc(page, "hist_date")
                    if not (
                        hist.count() and hist.first.is_visible()
                    ):  # mobile: filters live in a drawer
                        opener = page.get_by_role(
                            "button", name=re.compile("filter", re.IGNORECASE)
                        )
                        if opener.count():
                            opener.first.click()
                            page.wait_for_timeout(500)
                    hist.first.scroll_into_view_if_needed()
                    box = hist.first.bounding_box()
                    if not box:
                        raise RuntimeError("date histogram not visible")
                    before_url = page.url.split("?", 1)[-1][:160]
                    y = box["y"] + box["height"] * 0.5
                    page.mouse.move(box["x"] + box["width"] * 0.55, y)
                    page.mouse.down()
                    page.mouse.move(box["x"] + box["width"] * 0.8, y, steps=8)
                    page.mouse.up()
                    page.wait_for_timeout(900)
                    after = page.url.split("?", 1)[-1][:160]
                    status[fn + ":url_after_brush"] = after
                    if after == before_url:
                        raise RuntimeError("brushing the date histogram did not change the URL")
                status[fn] = _shot(page, outdir / fn)
            except Exception as e:  # noqa: BLE001
                status[fn] = f"FAILED: {str(e)[:120]}"
                try:
                    page.screenshot(path=str(outdir / ("FAILED_" + fn)))
                except Exception:  # noqa: BLE001
                    pass
            page.close()
        ctx.close()
    return status


# ---------------------------------------------------------------- verdict rows
def verdict_rows(m: dict) -> list[tuple[str, str, str, bool | None]]:
    rows: list = []

    def add(name, value, target, ok):
        rows.append((name, value, target, ok))

    s = m.get("semantic_ui")
    if s:
        for eng in ("chromium", "firefox"):
            e = s.get(eng)
            if not e:
                continue
            if "skipped" in e:
                add(f"semantic UI ({eng})", "skipped: " + e["skipped"], "optional", None)
                continue
            for case, label in (
                ("with_query", "query -> semantic-item"),
                ("empty_query", "empty query -> enabled state"),
            ):
                c = e[case]
                val = (
                    f"{c.get('semantic_items', '-')} items, state={c.get('enabled_visible')}, "
                    f"dom_changed={c.get('dom_changed_after_toggle_click')}, "
                    f"{c.get('bytes', 0) / MiB:.1f} MiB in {c.get('wait_s', '?')} s"
                    + (f", native dialogs {c['native_dialogs']}" if c.get("native_dialogs") else "")
                    + (f" FAIL: {c['fail']}" if c.get("fail") else "")
                )
                add(
                    f"semantic UI ({eng}) {label}",
                    val,
                    "click works in the real UI",
                    bool(c.get("pass")),
                )
    b = m.get("boolean")
    if b:
        if "error" in b:
            add("boolean queries", b["error"], "precision 1.0", False)
        else:
            add(
                "boolean: top-50 precision (every result satisfies the condition)",
                f"min {b['precision_P_min']:.3f}; documented-rule precision mean {b['precision_doc_mean']:.3f}; "
                f"strict-word mean {b['strict_precision_mean']:.3f}",
                "1.000 vs lenient superset P",
                b["precision_P_min"] == 1.0,
            )
            add(
                "boolean: result total vs DuckDB (inside the strict..lenient sandwich +-3%)",
                f"{b['ok_gated']}/{b['n_gated']} queries ok",
                "all gated queries",
                b["ok_gated"] == b["n_gated"],
            )
            add(
                "boolean: total within +-3% of the documented rule (tight)",
                f"{b['tight_gated']}/{b['n_gated']} queries",
                ">= 90%",
                b["tight_gated"] >= 0.9 * b["n_gated"],
            )
            for f, v in sorted(b["by_form"].items()):
                add(
                    f"  form {f}",
                    f"{v['ok']}/{v['n']} ok, {v['tight']} tight",
                    "all ok",
                    v["ok"] == v["n"],
                )
    u = m.get("boolean_ui")
    if u:
        if "error" in u:
            add("boolean via typed UI vs hook", u["error"], "top-10 overlap >= 8", False)
        else:
            add(
                "boolean via typed UI vs hook",
                f"{len(u['rows'])} queries",
                "top-10 overlap >= 8",
                u["ok"],
            )
    f = m.get("facets_ex")
    if f:
        if "error" in f:
            add("include/exclude facet counts", f["error"], "100%", False)
        else:
            add(
                "include/exclude facet counts exact vs DuckDB (tags_ex/status_ex/cw_exclude/platform_ex/pub range)",
                f"{f['exact']}/{f['n']}",
                "100%",
                f["exact_rate"] == 1.0,
            )
            add(
                "histogram facets exact (months, updatedMonths, capacityHist ignore their own range)",
                f"{f['hist_checked'] - f['hist_bad']}/{f['hist_checked']}"
                + (
                    f"; e.g. {f['hist_sample_failures'][0]['hist']}: {f['hist_sample_failures'][0]['diff']}"
                    if f["hist_bad"]
                    else ""
                ),
                "100%",
                f["hist_bad"] == 0,
            )
    t = m.get("trending")
    if t:
        if "skipped" in t:
            add("trending default sort", "skipped: " + t["skipped"], "median top-20 >= p95", None)
        else:
            add(
                "trending default (empty query): median recent_visits/day of top 20 vs p95 of all",
                f"hook {t['median_top20_hook']:.1f}, rendered {t['median_top20_dom']:.1f} vs p95 {t['p95_all']:.1f}",
                ">= p95",
                t["pass"],
            )
    th = m.get("thumbs")
    if th:
        if "error" in th:
            add("thumbnails", th["error"], "no VRChat hosts", False)
        else:
            add(
                "no VRChat-host requests (first view / detail panel)",
                f"{len(th.get('first_view_vrchat_requests', []))} / {len(th.get('detail_panel_vrchat_requests', []))}"
                + (
                    "; detail has official-world link"
                    if th.get("detail_has_official_link")
                    else "; no official-world link"
                ),
                "0 / 0",
                not th.get("first_view_vrchat_requests")
                and not th.get("detail_panel_vrchat_requests"),
            )
            d = th["decode"]
            if "skipped" in d:
                add(
                    "sampled thumbnails decode",
                    "skipped: " + d["skipped"],
                    ">= 99% naturalWidth > 0",
                    None,
                )
            else:
                add(
                    "sampled thumbnails decode",
                    f"{d['decoded']}/{d['sampled']}",
                    ">= 99%",
                    d["rate"] >= 0.99,
                )
            pb = [p for p in th["pages"] if "thumb_bytes" in p]
            if pb:
                add(
                    "thumbnail bytes per results page (report only)",
                    "; ".join(
                        f"{p['params'] or 'home'}: {p['thumb_bytes'] / 1024:.0f} KiB/{p['items']} items"
                        for p in pb[:4]
                    ),
                    "report only",
                    None,
                )
    d = m.get("default_visibility")
    if d:
        if "error" in d:
            add("nothing hidden by default (?sort=published)", d["error"], "adult shown", False)
        else:
            add(
                "nothing hidden by default (?sort=published, no params)",
                f"{d['dom_n']} rendered, overlap with engine {d['dom_vs_engine_top_n_overlap']:.2f}, "
                f"adult-tagged shown {d['adult_in_dom']} (engine top-n has {d['adult_expected_in_top_n']}, "
                f"top-200 has {d['adult_in_hook_top_200']})",
                "rendered == engine top-n incl. adult",
                d["pass"],
            )
    return rows


def run_all(playwright, browser, base: str, parquet: str, outdir: Path, page, a) -> dict:
    """Execute every round-5 check and return the metrics dict (keys used by verdict_rows)."""
    m: dict = {}

    def guard(key, fn):
        try:
            m[key] = fn()
        except Exception as e:  # noqa: BLE001
            m[key] = {"error": f"harness error: {type(e).__name__}: {str(e)[:300]}"}

    if not a.no_semantic_ui:
        guard("semantic_ui", lambda: semantic_ui(playwright, browser, base))
    con = connect(parquet)
    try:
        bcon, queries, secs = build_bool_truth(parquet)
        m["boolean_truth_build_s"] = secs
        guard("boolean", lambda: boolean_eval(page, bcon, queries))
        if "rows" in m["boolean"]:
            guard(
                "boolean_ui",
                lambda: boolean_ui_agreement(browser, base, queries, m["boolean"]["rows"]),
            )
    except Exception as e:  # noqa: BLE001
        m["boolean"] = {"error": f"harness error: {type(e).__name__}: {str(e)[:300]}"}
    guard("facets_ex", lambda: facet_ex_eval(page, con, facet_ex_combos(con)))
    guard("trending", lambda: trending_eval(page, base, browser, con, parquet))
    wid = con.execute("select world_id from w order by coalesce(visits,0) desc limit 1").fetchone()[
        0
    ]
    guard("thumbs", lambda: thumbs_eval(browser, base, parquet, wid))
    guard("default_visibility", lambda: default_visibility_eval(page, browser, base, con))
    if not a.no_shots:
        guard("screenshots_r5", lambda: owner_screenshots(browser, base, parquet, outdir))
    return m
