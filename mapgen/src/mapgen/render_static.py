"""Stage E: render the labeled interactive map + a nearest-neighbor spot-check.

Produces (per DR variant) a standalone datamapplot HTML, plus nn_report.md: for
sampled worlds, the top-k nearest neighbors by embedding cosine, and a
cross-lingual probe (e.g. "shrine" should surface 神社 worlds). Eyeballing these
is the GO/NO-GO gate before any app gets built.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import polars as pl

from mapgen.common import DEFAULT_EMBED_MODEL, OllamaEmbedder, l2_normalize

CROSS_LINGUAL_QUERIES = [
    "shrine",
    "神社",
    "anime",
    "horror",
    "night club",
    "cozy bedroom to sleep",
    "寝る",
    "forest nature",
]


def _label_array(clusters: pl.DataFrame, labels_json: dict, n: int) -> np.ndarray:
    cid = clusters["cluster_id"].to_numpy()
    out = np.full(n, "Unlabelled", dtype=object)
    for i in range(n):
        c = int(cid[i])
        if c != -1 and str(c) in labels_json:
            out[i] = labels_json[str(c)].get("label", "Unlabelled")
    return out.astype(str)


def _render_map(coords, labels, hover, title, sub_title, out_path: Path) -> None:
    import datamapplot

    fig = datamapplot.create_interactive_plot(
        coords,
        labels,
        hover_text=hover,
        title=title,
        sub_title=sub_title,
    )
    fig.save(str(out_path))
    print(f"  wrote {out_path}")


def _nn_report(
    emb: np.ndarray,
    names: list[str],
    texts: list[str],
    model: str,
    ollama_url: str | None,
    out_path: Path,
    k: int = 8,
    n_samples: int = 12,
) -> None:
    rng = np.random.default_rng(0)
    lines = ["# Nearest-neighbor spot-check\n"]

    sample_idx = rng.choice(len(names), size=min(n_samples, len(names)), replace=False)
    lines.append("## Sampled worlds -> nearest by embedding\n")
    for i in sample_idx:
        sims = emb @ emb[i]
        nn = np.argsort(-sims)[1 : k + 1]
        lines.append(f"**{names[i]}** — _{texts[i][:80].strip()}_")
        for j in nn:
            lines.append(f"  - {sims[j]:.3f}  {names[j]}")
        lines.append("")

    lines.append("## Cross-lingual probe (query -> nearest worlds)\n")
    embedder = OllamaEmbedder(model=model, base_url=ollama_url)
    qvecs = l2_normalize(embedder.embed(CROSS_LINGUAL_QUERIES, show_progress=False))
    for q, qv in zip(CROSS_LINGUAL_QUERIES, qvecs, strict=True):
        sims = emb @ qv
        nn = np.argsort(-sims)[:k]
        lines.append(f"**query: `{q}`**")
        for j in nn:
            lines.append(f"  - {sims[j]:.3f}  {names[j]}")
        lines.append("")

    out_path.write_text("\n".join(lines))
    print(f"  wrote {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--embeddings", type=Path, required=True)
    ap.add_argument("--meta", type=Path, required=True)
    ap.add_argument("--clusters", type=Path, required=True)
    ap.add_argument("--labels", type=Path, required=True, help="cluster_labels.json")
    ap.add_argument(
        "--coords-dir", type=Path, required=True, help="dir with coords_*.parquet"
    )
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--model", default=DEFAULT_EMBED_MODEL)
    ap.add_argument("--ollama-url", default=None)
    ap.add_argument("--skip-nn", action="store_true")
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    import json

    meta = pl.read_parquet(args.meta)
    names = meta["name"].to_list()
    texts = meta["text"].to_list()
    n = meta.height

    # Align clusters to meta row order by world_id.
    clusters = (
        meta.select("world_id")
        .join(pl.read_parquet(args.clusters), on="world_id", how="left")
        .with_columns(pl.col("cluster_id").fill_null(-1))
    )
    labels_json = json.loads(Path(args.labels).read_text())
    label_arr = _label_array(clusters, labels_json, n)
    hover = [f"{nm}" for nm in names]

    for coords_path in sorted(args.coords_dir.glob("coords_*.parquet")):
        variant = coords_path.stem.replace("coords_", "")
        cdf = meta.select("world_id").join(
            pl.read_parquet(coords_path), on="world_id", how="left"
        )
        coords = cdf.select("x", "y").to_numpy().astype("float32")
        n_labeled = int((label_arr != "Unlabelled").sum())
        _render_map(
            coords,
            label_arr,
            hover,
            title="VRChat Worlds Latent Map",
            sub_title=f"{n:,} worlds — DR: {variant} — {n_labeled:,} labeled",
            out_path=args.out_dir / f"map_{variant}.html",
        )

    if not args.skip_nn:
        emb = l2_normalize(np.load(args.embeddings))
        _nn_report(
            emb,
            names,
            texts,
            args.model,
            args.ollama_url,
            args.out_dir / "nn_report.md",
        )


if __name__ == "__main__":
    main()
