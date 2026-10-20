#!/usr/bin/env python3
"""Embedding eval: bilingual retrieval + 'similar worlds' sanity.

  eval_embed.py make-queries --parquet worlds.parquet --out queries.parquet
  eval_embed.py run --emb-dir DIR --queries queries.parquet --parquet worlds.parquet --models qwen3,gemma2,static --md EMBED_EVAL.md

Retrieval sets (index = title+tags-only embeddings of ALL worlds; query = held-out description text):
  cjk2en : source world has CJK title AND an English description (ascii>=0.9, >=40 chars); query = English description.
  jp2en  : source world has ASCII title and a description with CJK text (>=20 chars); query = that description.
  en2en  : control: ASCII title + English description; query = description.
Variants: float, int8, binary (sign, Hamming/ip on +-1), and MRL-256 truncations of each.
"""

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from common import MODELS, ascii_ratio, clean_tags, has_cjk, script_of

N_PER_SET = 400


def make_queries(parquet: str, out: str, seed: int = 0):
    df = pd.read_parquet(parquet, columns=["world_id", "name", "description", "tags"])
    d = df["description"].fillna("").map(lambda s: re.sub(r"\s+", " ", s).strip())
    nm = df["name"].fillna("")
    ar = d.map(ascii_ratio)
    cjk_name = nm.map(has_cjk)
    sets = {
        "cjk2en": (cjk_name & (ar >= 0.9) & (d.str.len() >= 40)),
        "jp2en": ((~cjk_name) & (nm.map(ascii_ratio) >= 0.9) & d.map(has_cjk) & (d.str.len() >= 20)),
        "en2en": ((~cjk_name) & (nm.str.len() >= 4) & (ar >= 0.95) & (d.str.len() >= 60)),
    }
    rows = []
    rng = np.random.default_rng(seed)
    for k, m in sets.items():
        idx = np.flatnonzero(m.to_numpy())
        print(k, "eligible", len(idx))
        pick = rng.choice(idx, size=min(N_PER_SET, len(idx)), replace=False)
        for i in pick:
            rows.append({"set": k, "world_id": df.world_id.iat[i], "query": d.iat[i][:300]})
    pd.DataFrame(rows).to_parquet(out)
    print("wrote", len(rows))


def quantize_int8(x: np.ndarray) -> np.ndarray:
    """Global-scale symmetric int8 (what a shipped table would use)."""
    s = np.percentile(np.abs(x[:20000]), 99.9)
    return np.clip(np.round(x / s * 127), -127, 127).astype(np.int8)


def prep(x: np.ndarray, variant: str, dim: int):
    """-> (docs matrix as float32 chunks-provider, query transform)."""
    return x


def score_ranks(docs: np.ndarray, qs: np.ndarray, truth: np.ndarray, kind: str) -> np.ndarray:
    """Return rank (1-based) of truth doc per query. docs: (N,D) in the variant's domain."""
    N = docs.shape[0]
    ranks = np.empty(len(qs), np.int32)
    tscore = np.empty(len(qs), np.float32)
    # score truth first, then count docs scoring strictly greater (ties pessimistic-neutral: +ties/2 ignored)
    qf = qs.astype(np.float32)
    for i in range(len(qs)):
        tscore[i] = float(docs[truth[i]].astype(np.float32) @ qf[i])
    greater = np.zeros(len(qs), np.int64)
    CH = 20000
    for s in range(0, N, CH):
        blk = docs[s:s + CH].astype(np.float32)
        sc = blk @ qf.T  # (CH, Q)
        greater += (sc > tscore[None, :]).sum(0)
    return greater + 1


def run_variants(docs_f: np.ndarray, qs_f: np.ndarray, truth, dim_full: int):
    res = {}
    for tag, sl in (("full", slice(None)), ("mrl256", slice(0, 256))):
        if tag == "mrl256" and dim_full <= 256:
            continue
        d = docs_f[:, sl].astype(np.float32)
        q = qs_f[:, sl].astype(np.float32)
        if tag == "mrl256":  # re-normalise after truncation
            d /= np.linalg.norm(d, axis=1, keepdims=True) + 1e-9
            q /= np.linalg.norm(q, axis=1, keepdims=True) + 1e-9
        res[f"{tag}-float"] = score_ranks(d, q, truth, "f")
        di = quantize_int8(d)
        res[f"{tag}-int8"] = score_ranks(di, q, truth, "i8")  # float query vs int8 docs
        db = np.where(d > 0, 1, -1).astype(np.int8)
        qb = np.where(q > 0, 1, -1).astype(np.float32)
        res[f"{tag}-binary"] = score_ranks(db, qb, truth, "bin")  # sign vs sign == Hamming
        res[f"{tag}-binary(float q)"] = score_ranks(db, q, truth, "binasym")
    return res


def similar_sanity(full: np.ndarray, df: pd.DataFrame, n=500, k=10, seed=1):
    rng = np.random.default_rng(seed)
    ids = rng.choice(len(df), n, replace=False)
    X = full.astype(np.float32)
    tags = [set(clean_tags(t)) for t in df["tags"]]
    auth = df["author_id"].fillna("").to_numpy()
    scr = np.array([script_of(s) for s in df["name"].fillna("")])
    sim = X[ids] @ X.T
    sim[np.arange(n), ids] = -9
    nb = np.argpartition(-sim, k, axis=1)[:, :k]
    rn = rng.integers(0, len(df), size=(n, k))

    def stats(nbrs):
        jac, sa, ss = [], [], []
        for q, row in zip(ids, nbrs):
            tq = tags[q]
            for j in row:
                u = tq | tags[j]
                jac.append(len(tq & tags[j]) / len(u) if u else 0.0)
                sa.append(auth[q] == auth[j] and auth[q] != "")
                ss.append(scr[q] == scr[j])
        return np.mean(jac), np.mean(sa), np.mean(ss)

    return stats(nb), stats(rn)


def pct(x):
    return f"{100*x:.1f}"


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("make-queries")
    m.add_argument("--parquet", required=True)
    m.add_argument("--out", required=True)
    r = sub.add_parser("run")
    r.add_argument("--emb-dir", required=True)
    r.add_argument("--queries", required=True)
    r.add_argument("--parquet", required=True)
    r.add_argument("--models", required=True)
    r.add_argument("--md", required=True)
    a = ap.parse_args()
    if a.cmd == "make-queries":
        return make_queries(a.parquet, a.out)
    emb = Path(a.emb_dir)
    q = pd.read_parquet(a.queries)
    df = pd.read_parquet(a.parquet, columns=["world_id", "name", "author_id", "tags"])
    ids = pd.read_parquet(emb / "ids.parquet")["world_id"]
    assert (ids.to_numpy() == df["world_id"].to_numpy()).all(), "row order mismatch"
    pos = {w: i for i, w in enumerate(ids)}
    truth = q["world_id"].map(pos).to_numpy()
    lines = ["# Embedding eval", "",
             f"Index: {len(ids):,} worlds, title+tags-only embeddings. Query sets (n per set): "
             + ", ".join(f"{k}={v}" for k, v in q["set"].value_counts().items()) + ".", ""]
    rows, sims = [], []
    for mk in a.models.split(","):
        short = np.load(emb / f"{mk}_short.npy")
        qe = np.load(emb / f"{mk}_queries.npy")
        for sname in q["set"].unique():
            msk = (q["set"] == sname).to_numpy()
            res = run_variants(short, qe[msk], truth[msk], short.shape[1])
            for var, rk in res.items():
                rows.append((mk, sname, var, (rk <= 10).mean(), np.where(rk <= 100, 1 / rk, 0).mean()))
        fp = emb / f"{mk}_full.npy"
        if fp.exists():
            (nj, ns, nsc), (rj, rs, rsc) = similar_sanity(np.load(fp), df)
            sims.append((mk, nj, rj, ns, rs, nsc, rsc))
        print(mk, "done", file=sys.stderr)
    t = pd.DataFrame(rows, columns=["model", "set", "variant", "recall@10", "mrr@100"])
    for sname in t["set"].unique():
        lines += [f"## Retrieval: {sname}  (recall@10 % / MRR@100 x100)", "",
                  "| variant | " + " | ".join(a.models.split(",")) + " |",
                  "|---|" + "---|" * len(a.models.split(","))]
        sub_t = t[t["set"] == sname]
        for var in dict.fromkeys(sub_t["variant"]):
            cells = []
            for mk in a.models.split(","):
                x = sub_t[(sub_t.model == mk) & (sub_t.variant == var)]
                cells.append(f"{pct(x['recall@10'].iat[0])} / {pct(x['mrr@100'].iat[0])}" if len(x) else "-")
            lines.append(f"| {var} | " + " | ".join(cells) + " |")
        lines.append("")
    lines += ["## Similar-worlds sanity (500 sampled worlds, top-10 neighbours on full-text embeddings)", "",
              "| model | tag Jaccard (nbr / random) | same author % (nbr / random) | same title script % (nbr / random) |",
              "|---|---|---|---|"]
    for mk, nj, rj, ns, rs, nsc, rsc in sims:
        lines.append(f"| {mk} | {nj:.3f} / {rj:.3f} | {pct(ns)} / {pct(rs)} | {pct(nsc)} / {pct(rsc)} |")
    Path(a.md).write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
