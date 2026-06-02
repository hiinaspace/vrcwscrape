"""Legibility pass: turn the DR scatter into a labeled "neighborhoods" map.

For each DR variant:
  * assign every point a region (HDBSCAN noise -> nearest cluster centroid in
    embedding space), so the map has no gray "unassigned" field;
  * optionally relax the 2D points to reduce overlap (KDTree repulsion);
  * build region polygons by buffer-union of each cluster's *core* points
    (organic blobs; a fragmented cluster yields a MultiPolygon, so we can label
    each island separately at an interior point -> no labels floating in voids);
  * render a high-DPI PNG for evaluation and export regions.geojson for deck.gl.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.patheffects as pe  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import polars as pl  # noqa: E402
import shapely  # noqa: E402
from scipy.spatial import KDTree  # noqa: E402
from shapely import MultiPoint, MultiPolygon, Polygon  # noqa: E402

from mapgen.common import l2_normalize  # noqa: E402


def _assign_regions(emb: np.ndarray, cluster_id: np.ndarray) -> np.ndarray:
    """Every point -> a cluster id; noise (-1) goes to the nearest cluster
    centroid in (normalized) embedding space (cosine)."""
    region = cluster_id.copy()
    cids = sorted(int(c) for c in set(cluster_id.tolist()) if c != -1)
    if not cids:
        return region
    centroids = np.stack([emb[cluster_id == c].mean(axis=0) for c in cids])
    centroids = l2_normalize(centroids)
    noise = np.where(cluster_id == -1)[0]
    if noise.size:
        sims = emb[noise] @ centroids.T
        region[noise] = np.array(cids)[sims.argmax(axis=1)]
    return region


def _relax(xy: np.ndarray, iters: int, radius: float, strength: float) -> np.ndarray:
    """Nudge near-overlapping points apart (a few repulsion iterations)."""
    xy = xy.astype(np.float64).copy()
    for _ in range(iters):
        pairs = KDTree(xy).query_pairs(radius, output_type="ndarray")
        if len(pairs) == 0:
            break
        d = xy[pairs[:, 0]] - xy[pairs[:, 1]]
        dist = np.linalg.norm(d, axis=1, keepdims=True) + 1e-9
        push = ((radius - dist) / dist) * d * strength
        disp = np.zeros_like(xy)
        np.add.at(disp, pairs[:, 0], push)
        np.add.at(disp, pairs[:, 1], -push)
        xy += disp
    return xy


def _median_nn(xy: np.ndarray, sample: int = 4000) -> float:
    idx = np.random.default_rng(0).choice(
        len(xy), size=min(sample, len(xy)), replace=False
    )
    dist, _ = KDTree(xy).query(xy[idx], k=2)
    return float(np.median(dist[:, 1]))


def _region_polys(
    xy: np.ndarray, cluster_id: np.ndarray, cids: list[int], buffer_r: float
) -> dict[int, Polygon | MultiPolygon]:
    """Buffer-union each cluster's core points into an organic region polygon."""
    polys: dict[int, Polygon | MultiPolygon] = {}
    for c in cids:
        pts = xy[cluster_id == c]
        if len(pts) < 3:
            continue
        geom = MultiPoint(pts).buffer(buffer_r).simplify(buffer_r * 0.25)
        if not geom.is_empty:
            polys[c] = geom
    return polys


def _distinct_colors(n: int) -> np.ndarray:
    hues = np.linspace(0, 1, n, endpoint=False)
    rng = np.random.default_rng(7)
    rng.shuffle(hues)  # adjacent regions get contrasting hues
    return plt.get_cmap("hsv")(hues)


def _iter_polygons(geom: Polygon | MultiPolygon):
    if isinstance(geom, MultiPolygon):
        yield from geom.geoms
    else:
        yield geom


def _render_png(
    xy: np.ndarray,
    region: np.ndarray,
    polys: dict[int, Polygon | MultiPolygon],
    labels: dict[int, str],
    sizes: dict[int, int],
    out_png: Path,
) -> None:
    cids = sorted(polys)
    color_by_cid = {
        c: col for c, col in zip(cids, _distinct_colors(len(cids)), strict=True)
    }
    max_size = max(sizes.values()) if sizes else 1

    fig, ax = plt.subplots(figsize=(26, 26), dpi=140)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    point_colors = np.array(
        [color_by_cid.get(int(c), (0.6, 0.6, 0.6, 1.0)) for c in region]
    )
    ax.scatter(xy[:, 0], xy[:, 1], s=3.0, c=point_colors, alpha=0.55, linewidths=0)

    # region fills (skip tiny speck fragments to reduce clutter)
    for c, geom in polys.items():
        col = color_by_cid[c]
        biggest = max(_iter_polygons(geom), key=lambda p: p.area).area
        for poly in _iter_polygons(geom):
            if poly.area < 0.02 * biggest:
                continue
            ax.fill(*poly.exterior.xy, color=col, alpha=0.18, zorder=1)

    # one label per region, on its largest island, sized by member count
    for c, geom in polys.items():
        poly = max(_iter_polygons(geom), key=lambda p: p.area)
        pt = poly.representative_point()
        fs = 9 + 15 * (sizes.get(c, 1) / max_size) ** 0.5
        txt = ax.text(
            pt.x,
            pt.y,
            labels.get(c, str(c)),
            fontsize=min(fs, 26),
            color="#111111",
            ha="center",
            va="center",
            weight="bold",
            zorder=3,
        )
        txt.set_path_effects([pe.withStroke(linewidth=4, foreground="white")])

    ax.set_aspect("equal")
    ax.axis("off")
    fig.tight_layout(pad=0)
    fig.savefig(out_png, facecolor="white", bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_png}")


def _export_geojson(
    polys: dict[int, Polygon | MultiPolygon],
    labels: dict[int, str],
    sizes: dict[int, int],
    out_path: Path,
) -> None:
    feats = []
    for c, geom in polys.items():
        feats.append(
            {
                "type": "Feature",
                "properties": {
                    "region_id": c,
                    "label": labels.get(c, str(c)),
                    "size": sizes.get(c, 0),
                },
                "geometry": json.loads(shapely.to_geojson(geom)),
            }
        )
    out_path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))
    print(f"  wrote {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--embeddings", type=Path, required=True)
    ap.add_argument("--meta", type=Path, required=True)
    ap.add_argument("--clusters", type=Path, required=True)
    ap.add_argument("--labels", type=Path, required=True)
    ap.add_argument("--coords-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--relax-iters", type=int, default=8)
    ap.add_argument("--buffer-scale", type=float, default=2.5)
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    emb = l2_normalize(np.load(args.embeddings))
    meta = pl.read_parquet(args.meta)
    labels_json = json.loads(Path(args.labels).read_text())
    labels = {int(k): v.get("label", k) for k, v in labels_json.items()}

    clusters = (
        meta.select("world_id")
        .join(pl.read_parquet(args.clusters), on="world_id", how="left")
        .with_columns(pl.col("cluster_id").fill_null(-1))
    )
    cluster_id = clusters["cluster_id"].to_numpy()
    region = _assign_regions(emb, cluster_id)
    cids = sorted(int(c) for c in set(cluster_id.tolist()) if c != -1)
    sizes = {c: int((region == c).sum()) for c in cids}

    for coords_path in sorted(args.coords_dir.glob("coords_*.parquet")):
        variant = coords_path.stem.replace("coords_", "")
        cdf = meta.select("world_id").join(
            pl.read_parquet(coords_path), on="world_id", how="left"
        )
        xy = cdf.select("x", "y").to_numpy().astype("float64")

        nn = _median_nn(xy)
        if args.relax_iters:
            xy = _relax(xy, args.relax_iters, radius=nn * 2.0, strength=0.3)
        polys = _region_polys(xy, cluster_id, cids, buffer_r=nn * args.buffer_scale)
        print(f"{variant}: {len(polys)} region polygons (median nn={nn:.3f})")

        _render_png(
            xy, region, polys, labels, sizes, args.out_dir / f"regions_{variant}.png"
        )
        _export_geojson(
            polys, labels, sizes, args.out_dir / f"regions_{variant}.geojson"
        )


if __name__ == "__main__":
    main()
