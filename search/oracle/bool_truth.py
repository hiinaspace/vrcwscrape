"""DuckDB ground truth for boolean text queries (round 5 query language).

A query is built as a small AST (tuples) that renders to BOTH the site's syntax and a DuckDB
boolean expression, so truth never depends on parsing the site's string. See SEMANTICS.md
("Boolean query truth") for the exact semantics and the documented tokenizer differences.

AST nodes: ("term", w) ("phrase", "w1 w2") ("tag", x) ("cw", x) ("title", w)
           ("cap", ">=16"|"8..32") ("upd", ">=2025") ("visits", ">1000") ("plat", "quest")
           ("size", "<50") ("and", [..]) ("or", [..], sep) ("not", node)
Modes (see SEMANTICS.md "Boolean query truth"):
  S  = strict: whole-word matches, exact raw tag, adjacent phrase
  L  = documented site rule: only the LAST plain term of the query prefix-expands (positive or negated),
       tags case-insensitive, phrase = adjacent anywhere OR all words in the description
  LG = L with tags case/plural-grouped (`tag:x` also matches xs / x-without-s)
  P  = lenient superset used for precision: every positive term prefix-matches, negated terms strict, tags grouped
  N  = negated terms prefix-match, positives strict (lower bound partner of P)
  PP = every term prefix-matches
"""

from __future__ import annotations

import random
import re
import unicodedata
from collections import Counter

import regex

from common import sql_str

MODES = ("S", "L", "LG", "P", "N", "PP")

CJK = "々-〇぀-ヿㇰ-ㇿ㐀-䶿一-鿿豈-﫿가-힯\U00020000-\U0002fa1f"
CJK_ANY = regex.compile(f"[{CJK}]")
_SPLIT = regex.compile(rf"([{CJK}]+)|((?:(?![{CJK}])[\p{{L}}\p{{N}}\p{{M}}])+)")
DESC_CHARS = 1000
HIDDEN_PREFIXES = ("admin_", "adm_", "feature_", "debug_", "system_")


def norm_text(s: str | None) -> str:
    """NFKC + lowercase, runs of latin words and CJK runs separated by single spaces."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s).lower()
    return " ".join(m.group(0) for m in _SPLIT.finditer(s))


def is_cjk(term: str) -> bool:
    return bool(CJK_ANY.search(term))


def build_docs(con, parquet_view: str = "w") -> dict:
    """Create DuckDB table `docs` (world_id, blob, taglow, cwset, rawtags) and return word stats.

    blob = ' ' + title + ' ## ' + author + ' ## ' + tag words (' # '-joined per tag) + ' ## ' + desc[:1000] + ' '
    where each part is norm_text(...). Hidden/status tags are not text (the site does not index them);
    content_* tags are (as words: `content horror`).
    """
    import pyarrow as pa

    rows = con.execute(
        f"select world_id, name, author_name, description, tagset from {parquet_view}"
    ).fetchall()
    ids, blobs, taglows, cwsets, raws = [], [], [], [], []
    df: Counter[str] = Counter()
    ddf: Counter[str] = Counter()
    for wid, name, author, desc, tagset in rows:
        tagset = tagset or []
        text_tags = [t for t in tagset if not t.startswith(HIDDEN_PREFIXES)]
        parts = [
            norm_text(name),
            norm_text(author),
            " # ".join(norm_text(t) for t in text_tags),
            norm_text((desc or "")[:DESC_CHARS]),
        ]
        blob = " " + " ## ".join(parts) + " "
        ids.append(wid)
        blobs.append(blob)
        taglows.append(sorted({t.lower() for t in tagset if not t.startswith(HIDDEN_PREFIXES)}))
        cwsets.append([t for t in tagset if t.startswith("content_")])
        raws.append(list(tagset))
        words = set(blob.split(" ")) - {"", "#", "##"}
        df.update(words)
        ddf.update(set(parts[3].split(" ")) - {""})
    tbl = pa.table(
        {"world_id": ids, "blob": blobs, "taglow": taglows, "cwset": cwsets, "rawtags": raws}
    )
    con.register("docs_arrow", tbl)
    con.execute(
        "create or replace table docs as select d.world_id, d.blob, d.taglow, d.cwset, d.rawtags, "
        "w.capacity, w.updated_at, w.created_at, w.visits, w.has_pc, w.has_quest, w.pc_size_mb, "
        "w.quest_size_mb, w.name, w.author_name from docs_arrow d join w using (world_id)"
    )
    return {"n": len(ids), "df": df, "ddf": ddf}


# ---------------------------------------------------------------- AST -> site string
def render(node, top: bool = True) -> str:
    k = node[0]
    if k == "term":
        return node[1]
    if k == "phrase":
        return f'"{node[1]}"'
    if k in ("tag", "cw", "title", "cap", "upd", "visits", "plat", "size"):
        name = {"upd": "updated", "plat": "platform"}.get(k, k)
        return f"{name}:{node[1]}"
    if k == "not":
        inner = node[1]
        if inner[0] in ("and", "or"):
            return f"-({render(inner, True)})"
        return "-" + render(inner, False)
    if k == "and":
        return " ".join(render(c, False) for c in node[1])
    if k == "or":
        sep = f" {node[2]} "
        s = sep.join(render(c, False) for c in node[1])
        inline = len(node) > 3 and node[3]  # `a b | c` (OR binds tighter than the space-AND)
        return s if (top or inline) else f"({s})"
    raise ValueError(k)


# ---------------------------------------------------------------- AST -> DuckDB
def _num(col: str, spec: str) -> str:
    m = re.fullmatch(r"(>=|<=|>|<|=)?(\d+(?:\.\d+)?)", spec)
    if m:
        return f"coalesce({col} {m.group(1) or '='} {m.group(2)}, false)"
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\.\.(\d+(?:\.\d+)?)", spec)
    if m:
        return f"coalesce({col} between {m.group(1)} and {m.group(2)}, false)"
    raise ValueError(spec)


def _upd(spec: str) -> str:
    m = re.fullmatch(r"(>=|<=|>|<)?(\d{4})", spec)
    y = int(m.group(2))
    op = m.group(1) or "="
    if op == "=":
        return f"coalesce(updated_at >= timestamp '{y}-01-01' and updated_at < timestamp '{y + 1}-01-01', false)"
    if op == ">=":
        return f"coalesce(updated_at >= timestamp '{y}-01-01', false)"
    if op == ">":
        return f"coalesce(updated_at >= timestamp '{y + 1}-01-01', false)"
    if op == "<=":
        return f"coalesce(updated_at < timestamp '{y + 1}-01-01', false)"
    return f"coalesce(updated_at < timestamp '{y}-01-01', false)"


def last_term(node):
    """The last plain `term` leaf in query-text order (the one the site prefix-expands), or None."""
    found = None

    def walk(n):
        nonlocal found
        if n[0] == "term":
            found = n
        elif n[0] == "not":
            walk(n[1])
        elif n[0] in ("and", "or"):
            for c in n[1]:
                walk(c)

    walk(node)
    return found


def _pre(mode: str, neg: bool, leaf, last) -> bool:
    if mode in ("L", "LG"):
        return leaf is last
    return mode == "PP" or (mode == "P" and not neg) or (mode == "N" and neg)


def to_sql(node, mode: str = "S", neg: bool = False, last=None) -> str:
    k = node[0]
    lenient = mode in ("L", "LG", "P", "PP")

    def sfx(prefix: bool) -> str:
        return "" if prefix else " "

    if k == "term":
        t = node[1]
        if is_cjk(t):
            return f"contains(blob, {sql_str(norm_text(t))})"
        return f"contains(blob, {sql_str(' ' + norm_text(t) + sfx(_pre(mode, neg, node, last)))})"
    if k == "phrase":
        t = norm_text(node[1])
        adj = f"contains(blob, {sql_str(' ' + t + ' ')})"
        if not lenient:
            return adj
        allw = " and ".join(
            f"contains(split_part(blob, ' ## ', 4), {sql_str(' ' + w + ' ')})" for w in t.split(" ")
        )
        return f"({adj} or ({allw}))"
    if k == "title":
        t = norm_text(node[1])
        part = "(split_part(blob, ' ## ', 1) || ' ')"
        if is_cjk(t):
            return f"contains({part}, {sql_str(t)})"
        return f"contains({part}, {sql_str(' ' + t + sfx(_pre(mode, neg, node, last)))})"
    if k == "tag":
        x = node[1]
        if mode == "S":
            return f"list_contains(rawtags, {sql_str(x)})"
        if mode in ("L", "N"):
            return f"list_contains(taglow, {sql_str(x)})"
        xs = {x, x + "s"} | ({x[:-1]} if x.endswith("s") and len(x) > 3 else set())
        return "(" + " or ".join(f"list_contains(taglow, {sql_str(v)})" for v in sorted(xs)) + ")"
    if k == "cw":
        return f"list_contains(cwset, {sql_str('content_' + node[1])})"
    if k == "cap":
        return _num("capacity", node[1])
    if k == "visits":
        return _num("visits", node[1])
    if k == "upd":
        return _upd(node[1])
    if k == "plat":
        return "has_quest" if node[1] == "quest" else "has_pc"
    if k == "size":
        m = re.fullmatch(r"(>=|<=|>|<)?(\d+(?:\.\d+)?)", node[1])
        col = "least(coalesce(pc_size_mb, 1e18), coalesce(quest_size_mb, 1e18))"
        return f"({col} {m.group(1) or '='} {m.group(2)})"
    if k == "and":
        return "(" + " and ".join(to_sql(c, mode, neg, last) for c in node[1]) + ")"
    if k == "or":
        return "(" + " or ".join(to_sql(c, mode, neg, last) for c in node[1]) + ")"
    if k == "not":
        return f"(not {to_sql(node[1], mode, True, last)})"
    raise ValueError(k)


def counts(con, node) -> dict[str, int]:
    """One scan: result count under every mode."""
    last = last_term(node)
    sel = ", ".join(f"count(*) filter (where {to_sql(node, m, False, last)})" for m in MODES)
    return dict(zip(MODES, con.execute(f"select {sel} from docs").fetchone()))


def count(con, node, mode: str) -> int:
    return con.execute(
        f"select count(*) from docs where {to_sql(node, mode, False, last_term(node))}"
    ).fetchone()[0]


def member_ids(con, node, mode: str, ids: list[str]) -> set[str]:
    if not ids:
        return set()
    lst = ",".join(sql_str(i) for i in ids)
    rows = con.execute(
        f"select world_id from docs where world_id in ({lst}) and "
        f"{to_sql(node, mode, False, last_term(node))}"
    ).fetchall()
    return {r[0] for r in rows}


# ---------------------------------------------------------------- generation
STOPLIKE = set(
    [
        "content",
        "this",
        "that",
        "with",
        "from",
        "your",
        "have",
        "will",
        "here",
        "there",
        "world",
        "worlds",
        "about",
        "just",
        "play",
        "only",
        "more",
        "some",
        "into",
        "than",
        "then",
        "they",
        "them",
        "what",
        "when",
        "where",
        "which",
        "also",
        "been",
        "were",
        "make",
        "made",
        "like",
        "very",
    ]
)


def candidate_terms(stats: dict, lo: int | None = None, hi: int | None = None) -> list[str]:
    n = stats["n"]
    lo = lo if lo is not None else max(8, int(300 * n / 198000))
    hi = hi if hi is not None else max(60, int(12000 * n / 198000))
    out = []
    for w, c in stats["df"].items():
        if not (lo <= c <= hi) or not re.fullmatch(r"[a-z]{4,10}", w) or w in STOPLIKE:
            continue
        if stats["ddf"].get(w, 0) / n > 0.02:  # description stopword candidates: avoid
            continue
        out.append(w)
    return sorted(out, key=lambda w: (-stats["df"][w], w))


def _masks(con, terms: list[str]) -> dict[str, object]:
    import numpy as np

    sel = ", ".join(
        f"contains(blob, {sql_str(' ' + t + ' ')}) as t{i}" for i, t in enumerate(terms)
    )
    cols = con.execute(
        f"select {sel} from docs using sample reservoir(60000 rows) repeatable (1)"
    ).fetchnumpy()
    return {t: np.asarray(cols[f"t{i}"], dtype=bool) for i, t in enumerate(terms)}


def generate(
    con, stats: dict, seed: int = 17, per_form: int = 5, min_hits: int | None = None
) -> list[dict]:
    """~per_form queries for each of 8 required forms plus a handful of extras (sizes scale with n)."""
    n_docs = stats["n"]
    scale = min(1.0, n_docs / 198000)
    min_hits = min_hits if min_hits is not None else max(15, int(80 * scale))
    c_lo = max(8, int(300 * scale))
    rnd = random.Random(seed)
    cands = candidate_terms(stats)
    pool = cands[:70] + rnd.sample(cands[70:], min(30, max(0, len(cands) - 70)))
    masks = _masks(con, pool)
    # term pairs (a, b) with a meaningful overlap: P(b|a) in [0.1, 0.6]
    pairs = []
    for a in pool:
        for b in pool:
            if a == b:
                continue
            na = masks[a].sum()
            if na < max(25, 100 * scale):
                continue
            r = (masks[a] & masks[b]).sum() / na
            if 0.1 <= r <= 0.6:
                pairs.append((a, b))
    rnd.shuffle(pairs)
    tags = con.execute(
        "select t, count(*) c from (select unnest(taglow) t from docs) group by t "
        f"having count(*) between {c_lo} and {max(100, int(40000 * scale))} order by c desc"
    ).fetchall()
    rawlow = {
        r[0]
        for r in con.execute(
            "select t from (select unnest(rawtags) t from docs) where regexp_matches(t, '^[a-z0-9]{3,}$') "
            "group by t having count(*) >= " + str(c_lo)
        ).fetchall()
    }
    tag_names = [t for t, _ in tags if t in rawlow and not t.startswith(("content_", "author_tag"))]
    cw_names = [
        r[0][len("content_") :]
        for r in con.execute(
            "select t from (select unnest(cwset) t from docs) group by t having count(*) >= "
            + str(max(12, int(500 * scale)))
            + " order by count(*) desc limit 8"
        ).fetchall()
    ]
    out: list[dict] = []

    def add(form: str, node, require_neg_effect=None, gate: bool = True) -> bool:
        cs = counts(con, node)
        s, p = cs["S"], cs["P"]
        if s < min_hits or p > 120000 * scale + 50:
            return False
        if require_neg_effect is not None:
            base = count(con, require_neg_effect, "S")
            if base - s < 0.05 * base:  # the negation must actually remove something
                return False
        out.append(
            {
                "form": form,
                "q": render(node),
                "node": node,
                "n_S": s,
                "n_P": p,
                "n_LG": cs["LG"],
                "gate": gate,
            }
        )
        return True

    # 1. a -b
    k = 0
    for a, b in pairs:
        if k >= per_form:
            break
        node = ("and", [("term", a), ("not", ("term", b))])
        k += add("a -b", node, ("term", a))
    # 2. a OR b
    k = 0
    for a, b in pairs[::3]:
        if k >= per_form:
            break
        k += add("a OR b", ("or", [("term", a), ("term", b)], "OR" if k % 2 == 0 else "|"))
    # 3. (a | b) -c
    k = 0
    trip = []
    for (a, b), (_, c) in zip(pairs, pairs[7:]):
        if len({a, b, c}) == 3:
            trip.append((a, b, c))
    for a, b, c in trip:
        if k >= per_form:
            break
        pos = ("or", [("term", a), ("term", b)], "|" if k % 2 == 0 else "OR")
        k += add("(a | b) -c", ("and", [pos, ("not", ("term", c))]), pos)
    # 3b. a b | c  (OR binds tighter than the space-AND: a AND (b OR c))
    k = 0
    for (a, b), (_, c) in zip(pairs[1::2], pairs[4::5]):
        if k >= per_form or len({a, b, c}) < 3:
            continue
        k += add("a b | c", ("and", [("term", a), ("or", [("term", b), ("term", c)], "|", True)]))
    # 4. tag:x -tag:y
    k, tries = 0, 0
    while k < per_form and tries < 400 and len(tag_names) > 4:
        tries += 1
        x, y = rnd.sample(tag_names[:60], 2)
        node = ("and", [("tag", x), ("not", ("tag", y))])
        k += add("tag:x -tag:y", node, ("tag", x))
    # 5. -cw:horror tag:japan
    k, tries = 0, 0
    while k < per_form and tries < 400 and cw_names:
        tries += 1
        c, x = rnd.choice(cw_names), rnd.choice(tag_names[:60])
        node = ("and", [("not", ("cw", c)), ("tag", x)])
        k += add("-cw:c tag:x", node, ("tag", x))
    # 6. cap:>=16 a
    k, tries = 0, 0
    while k < per_form and tries < 200:
        tries += 1
        a = rnd.choice(pool[:60])
        spec = rnd.choice([">=16", ">=32", "<=8", ">=24", ">=64"])
        k += add("cap:>=16 a", ("and", [("cap", spec), ("term", a)]))
    # 7. updated:>=2025 a
    k, tries = 0, 0
    while k < per_form and tries < 200:
        tries += 1
        a = rnd.choice(pool[:60])
        spec = rnd.choice([">=2025", ">=2026", "<=2023", ">=2024"])
        k += add("updated:>=2025 a", ("and", [("upd", spec), ("term", a)]))
    # 8. "two words"  (adjacent title/description word pairs seen in the data)
    rows = con.execute(
        "select name from docs where name is not null using sample reservoir(20000 rows) repeatable (5)"
    ).fetchall()
    pc: Counter[str] = Counter()
    for (nm,) in rows:
        w = norm_text(nm).split(" ")
        pc.update(
            f"{x} {y}"
            for x, y in zip(w, w[1:])
            if re.fullmatch(r"[a-z]{3,}", x) and re.fullmatch(r"[a-z]{3,}", y)
        )
    phr = [
        p
        for p, c in pc.most_common(120)
        if c >= (4 if scale >= 1 else 2)
        and all(stats["ddf"].get(x, 0) / n_docs <= 0.02 for x in p.split(" "))
    ]
    rnd.shuffle(phr)
    k = 0
    for ph in phr:
        if k >= per_form:
            break
        k += add('"two words"', ("phrase", ph))
    # extras: cap range, visits, platform, title:, size (size is not gated: min-over-platforms semantics)
    ex = [
        ("cap:8..32 a", ("and", [("cap", "8..32"), ("term", pool[2])]), True),
        ("visits:>1000 a", ("and", [("visits", ">1000"), ("term", pool[3])]), True),
        ("platform:quest a", ("and", [("plat", "quest"), ("term", pool[4])]), True),
        ("title:a -b", ("and", [("title", pool[5]), ("not", ("term", pool[6]))]), True),
        ("size:<50 a", ("and", [("size", "<50"), ("term", pool[7])]), False),
    ]
    for form, node, gate in ex:
        add(form, node, gate=gate)
    # CJK substring (needs enough CJK titles): reported, gated like others
    cjk = con.execute(
        "select name from docs where regexp_matches(name, '[぀-ヿ一-鿿]{2,}') "
        "using sample reservoir(3000 rows) repeatable (9)"
    ).fetchall()
    cc: Counter[str] = Counter()
    for (nm,) in cjk:
        for m in regex.finditer(rf"[{CJK}]{{2,}}", unicodedata.normalize("NFKC", nm).lower()):
            s = m.group(0)
            cc.update(s[i : i + 2] for i in range(len(s) - 1))
    for bg, c in cc.most_common(40):
        if c >= max(3, 8 * scale) and len([o for o in out if o["form"].startswith("CJK")]) < 3:
            add("CJK a -b", ("and", [("term", bg), ("not", ("term", pool[1]))]), ("term", bg))
    for o in out:
        o["q"] = render(o["node"])
    return out
