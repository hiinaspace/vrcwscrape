"""Stage D: cluster worlds in embedding space and label clusters with an LLM.

Recipe (BERTopic-style): UMAP-reduce to a low-dim space for density, HDBSCAN to
find clusters, then ask ollama (gemma4) for a short human label per cluster from
its representative world titles/tags.

Writes clusters.parquet (world_id, cluster_id) and cluster_labels.json.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import numpy as np
import polars as pl

from mapgen.common import DEFAULT_LLM_MODEL, l2_normalize, ollama_generate

LABEL_PROMPT = """You are labeling a cluster of VRChat worlds for a map of \
"neighborhoods". Below are representative world titles (some Japanese) with their \
tags. Give a SHORT neighborhood label (2-4 words, Title Case) and a broad category.

Worlds:
{samples}

Return a single JSON object with keys "label" and "category"."""

# JSON schema passed to ollama's structured output so the response is always a
# single well-formed object (no markdown fences / arrays to parse around).
LABEL_SCHEMA = {
    "type": "object",
    "properties": {
        "label": {"type": "string"},
        "category": {"type": "string"},
    },
    "required": ["label", "category"],
}


def _reduce_for_clustering(emb: np.ndarray, dim: int, seed: int) -> np.ndarray:
    import umap

    return umap.UMAP(
        n_components=dim,
        n_neighbors=30,
        min_dist=0.0,
        metric="euclidean",
        random_state=seed,
    ).fit_transform(emb)


def _representative_idx(emb: np.ndarray, members: np.ndarray, k: int) -> list[int]:
    centroid = emb[members].mean(axis=0)
    sims = emb[members] @ centroid
    order = np.argsort(-sims)[:k]
    return members[order].tolist()


def _parse_label(raw: str) -> dict[str, str]:
    """Extract {label, category}. With the schema set this is trivial, but stay
    robust to markdown fences / arrays in case a model ignores it."""
    text = raw.strip()
    text = re.sub(r"^```(?:json)?", "", text).strip()
    text = re.sub(r"```$", "", text).strip()

    obj: object = None
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{[^{}]*\}", text, re.DOTALL)
        if m:
            try:
                obj = json.loads(m.group(0))
            except json.JSONDecodeError:
                obj = None
    if isinstance(obj, list) and obj:
        obj = obj[0]
    if isinstance(obj, dict):
        return {
            "label": str(obj.get("label", "")).strip() or "Unlabelled",
            "category": str(obj.get("category", "")).strip() or "misc",
        }
    return {"label": text[:40] or "Unlabelled", "category": "misc"}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--embeddings", type=Path, required=True)
    ap.add_argument("--meta", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--umap-dim", type=int, default=10)
    ap.add_argument("--min-cluster-size", type=int, default=None)
    ap.add_argument(
        "--min-samples",
        type=int,
        default=5,
        help="HDBSCAN min_samples; lower => fewer noise points",
    )
    ap.add_argument(
        "--selection",
        choices=("eom", "leaf"),
        default="leaf",
        help="HDBSCAN cluster_selection_method. 'eom' (excess of mass) can "
        "collapse to a few giant clusters when the density tree has a dominant "
        "parent; 'leaf' takes the fine leaf clusters and is robust + better for "
        "the hierarchy.",
    )
    ap.add_argument("--samples-per-cluster", type=int, default=25)
    ap.add_argument("--llm-model", default=DEFAULT_LLM_MODEL)
    ap.add_argument("--ollama-url", default=None)
    ap.add_argument("--no-label", action="store_true", help="skip LLM labeling")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    import hdbscan

    emb = l2_normalize(np.load(args.embeddings))
    meta = pl.read_parquet(args.meta)
    names = meta["name"].to_list()
    texts = meta["text"].to_list()
    n = emb.shape[0]

    mcs = args.min_cluster_size or max(25, n // 400)
    print(f"Clustering {n:,} worlds (umap-dim={args.umap_dim}, min_cluster_size={mcs})")

    low = _reduce_for_clustering(emb, args.umap_dim, args.seed)
    labels = hdbscan.HDBSCAN(
        min_cluster_size=mcs,
        min_samples=args.min_samples,
        metric="euclidean",
        cluster_selection_method=args.selection,
    ).fit_predict(low)

    cluster_ids = sorted(int(c) for c in set(labels) if c != -1)
    n_noise = int((labels == -1).sum())
    print(f"  -> {len(cluster_ids)} clusters, {n_noise:,} noise points")

    meta.select("world_id").with_columns(
        cluster_id=pl.Series(labels.astype("int32"))
    ).write_parquet(args.out_dir / "clusters.parquet")

    labels_out: dict[str, dict] = {}
    exemplars: dict[int, list[str]] = {}
    for cid in cluster_ids:
        members = np.where(labels == cid)[0]
        rep = _representative_idx(emb, members, args.samples_per_cluster)
        exemplars[cid] = [names[i] for i in rep]
        entry: dict = {"size": int(members.size)}
        if not args.no_label:
            samples = "\n".join(f"- {names[i]}: {texts[i][:120]}" for i in rep)
            raw = ollama_generate(
                LABEL_PROMPT.format(samples=samples),
                model=args.llm_model,
                base_url=args.ollama_url,
                fmt=LABEL_SCHEMA,
            )
            entry.update(_parse_label(raw))
        else:
            entry.update({"label": f"cluster {cid}", "category": "misc"})
        labels_out[str(cid)] = entry
        print(f"  cluster {cid:>3} (n={members.size:>5}): {entry.get('label')}")

    (args.out_dir / "cluster_labels.json").write_text(
        json.dumps(labels_out, ensure_ascii=False, indent=2)
    )
    _write_exemplars(
        args.out_dir / "cluster_exemplars.md", labels_out, exemplars, n_noise, n
    )
    print(
        f"  wrote clusters.parquet + cluster_labels.json + exemplars to {args.out_dir}"
    )


def _write_exemplars(
    path: Path,
    labels_out: dict[str, dict],
    exemplars: dict[int, list[str]],
    n_noise: int,
    n_total: int,
    top: int = 15,
) -> None:
    """Human-readable per-cluster exemplar list (largest first) for judging
    cluster quality by reading titles rather than squinting at the map."""
    rows = sorted(labels_out.items(), key=lambda kv: kv[1].get("size", 0), reverse=True)
    lines = [
        "# Cluster exemplars\n",
        f"{len(rows)} clusters, {n_noise:,}/{n_total:,} "
        f"({100 * n_noise / n_total:.0f}%) noise\n",
    ]
    for cid_str, entry in rows:
        cid = int(cid_str)
        lines.append(
            f"## {entry.get('label')} "
            f"_(n={entry.get('size')}, {entry.get('category')})_"
        )
        lines.extend(f"- {name}" for name in exemplars.get(cid, [])[:top])
        lines.append("")
    path.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
