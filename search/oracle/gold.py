"""Build a deterministic gold query set from the parquet itself.

Usage: uv run python gold.py <parquet> [--out gold] [--seed 1] [--n 300]
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

from common import CJK_RE, CJK_RUN_RE, HAN_RE, HANGUL_RE, KANA_RE, connect, tag_class


STOP = {
    "this",
    "that",
    "with",
    "from",
    "your",
    "have",
    "will",
    "they",
    "them",
    "more",
    "into",
    "only",
    "also",
    "just",
    "some",
    "world",
    "worlds",
    "vrchat",
    "none",
    "test",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("parquet")
    ap.add_argument("--out", default="gold")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--n", type=int, default=300)
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    con = connect(a.parquet)
    rows = con.execute(
        "select world_id, name, author_id, author_name, tagset, coalesce(visits,0) "
        "from w where name is not null and length(trim(name)) > 0 order by world_id"
    ).fetchall()
    n = len(rows)
    names_lc = [r[1].lower() for r in rows]
    vis_sorted = sorted(r[5] for r in rows)
    pop_cut = vis_sorted[int(n * 0.7)]
    popular = [i for i in range(n) if rows[i][5] >= pop_cut]
    longtail = [i for i in range(n) if rows[i][5] < pop_cut]

    def pick() -> int:
        return rnd.choice(popular if rnd.random() < 0.8 else longtail)

    def matches(sub: str) -> list[int]:
        s = sub.lower()
        return [i for i in range(n) if s in names_lc[i]]

    def ids(idx: list[int]) -> list[str]:
        return [rows[i][0] for i in idx]

    out: list[dict] = []
    seen: set[tuple[str, str]] = set()

    def add(cat: str, q: str, expected: list[str], target: str) -> bool:
        if (cat, q) in seen or not q.strip() or not expected:
            return False
        seen.add((cat, q))
        out.append({"cat": cat, "q": q, "expected": expected[:200], "target": target})
        return True

    q_exact = int(a.n * 0.2)
    q_part = int(a.n * 0.2)
    q_cjk = int(a.n * 0.27)
    q_tag = int(a.n * 0.15)
    quota = {
        "exact_title": q_exact,
        "partial_title": q_part,
        "cjk_title": q_cjk,
        "tag_word": q_tag,
        "author_name": a.n - q_exact - q_part - q_cjk - q_tag,
    }
    cnt = dict.fromkeys(quota, 0)

    by_name: dict[str, list[int]] = {}
    for i in range(n):
        by_name.setdefault(names_lc[i].strip(), []).append(i)
    tries = 0
    while cnt["exact_title"] < quota["exact_title"] and tries < 20000:
        tries += 1
        i = pick()
        nm = rows[i][1].strip()
        if len(nm) < 4 or len(by_name[nm.lower()]) > 20:
            continue
        if add("exact_title", nm, ids(by_name[nm.lower()]), rows[i][0]):
            cnt["exact_title"] += 1

    tries = 0
    while cnt["partial_title"] < quota["partial_title"] and tries < 20000:
        tries += 1
        i = pick()
        words = rows[i][1].split()
        if len(words) < 4 or CJK_RE.search(rows[i][1]):
            continue
        k = rnd.choice([2, 3])
        s = rnd.randrange(0, len(words) - k + 1)
        sub = " ".join(words[s : s + k])
        if len(sub) < 7:
            continue
        m = matches(sub)
        if 1 <= len(m) <= 20 and add("partial_title", sub, ids(m), rows[i][0]):
            cnt["partial_title"] += 1

    lang_quota = {"ja": int(q_cjk * 0.6), "ko": int(q_cjk * 0.2)}
    lang_quota["zh"] = q_cjk - lang_quota["ja"] - lang_quota["ko"]
    got = dict.fromkeys(lang_quota, 0)
    cjk_pool = [i for i in range(n) if CJK_RE.search(rows[i][1])]
    pool_pop = [i for i in cjk_pool if rows[i][5] >= pop_cut]
    tries = 0
    while cnt["cjk_title"] < q_cjk and tries < 40000:
        tries += 1
        i = rnd.choice(pool_pop if rnd.random() < 0.8 and pool_pop else cjk_pool)
        nm = rows[i][1]
        if KANA_RE.search(nm):
            lang = "ja"
        elif HANGUL_RE.search(nm):
            lang = "ko"
        elif HAN_RE.search(nm):
            lang = "zh"
        else:
            continue
        if got[lang] >= lang_quota[lang] and tries < 30000:
            continue  # spill over only late, if a language pool is exhausted
        runs = [r for r in CJK_RUN_RE.findall(nm) if len(r) >= 2]
        if not runs:
            continue
        run = rnd.choice(runs)
        L = min(rnd.choice([2, 3, 4]), len(run))
        s = rnd.randrange(0, len(run) - L + 1)
        sub = run[s : s + L]
        m = matches(sub)
        if 1 <= len(m) <= 30 and add("cjk_title", sub, ids(m), rows[i][0]):
            got[lang] += 1
            cnt["cjk_title"] += 1
            out[-1]["lang"] = lang

    tagcount: dict[str, int] = {}
    tag_ids: dict[str, list[str]] = {}
    for r in rows:
        for t in r[4]:
            tagcount[t] = tagcount.get(t, 0) + 1
            tag_ids.setdefault(t, []).append(r[0])
    cand_tags = sorted(
        t
        for t, c in tagcount.items()
        if c >= 20
        and len(t) >= 4
        and t not in STOP
        and t.isascii()
        and t.replace("_", "").isalnum()
        and tag_class(t) == "user"
    )
    rnd.shuffle(cand_tags)
    for t in cand_tags:
        if cnt["tag_word"] >= quota["tag_word"]:
            break
        if add("tag_word", t.replace("_", " "), tag_ids[t], tag_ids[t][0]):
            out[-1]["tag"] = t
            cnt["tag_word"] += 1

    by_author: dict[str, list[int]] = {}
    for i in range(n):
        if rows[i][3] and len(rows[i][3]) >= 3:
            by_author.setdefault(rows[i][3], []).append(i)
    authors = sorted(k for k, v in by_author.items() if 2 <= len(v) <= 200)
    rnd.shuffle(authors)
    authors.sort(key=lambda k: 0 if any(rows[i][5] >= pop_cut for i in by_author[k]) else 1)
    pool = authors[: quota["author_name"] * 3]
    rnd.shuffle(pool)
    for au in pool:
        if cnt["author_name"] >= quota["author_name"]:
            break
        if add("author_name", au, ids(by_author[au]), ids(by_author[au])[0]):
            cnt["author_name"] += 1

    outdir = Path(a.out)
    outdir.mkdir(parents=True, exist_ok=True)
    with open(outdir / "queries.jsonl", "w") as fh:
        fh.writelines(json.dumps(q, ensure_ascii=False) + "\n" for q in out)
    print(f"wrote {len(out)} queries to {outdir}/queries.jsonl: {cnt}")


if __name__ == "__main__":
    main()
