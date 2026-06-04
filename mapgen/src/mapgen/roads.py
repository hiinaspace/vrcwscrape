"""Generate virtual road networks for the static latent-map app.

This is a visual/cartographic layer, not a semantic labeler. It turns existing
map coordinates and hierarchy ids into a looped street graph:

* local roads inside each subregion from KMeans junctions over world positions;
* arterial roads inside each island from subregion centroids;
* Delaunay/MST connectivity plus short extra edges for loops;
* a light PCA-grid snap to make dense areas read more like neighborhoods.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
import shapely
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import minimum_spanning_tree
from scipy.spatial import Delaunay, QhullError, cKDTree
from sklearn.cluster import MiniBatchKMeans


@dataclass(frozen=True)
class RoadFeature:
    coords: list[tuple[float, float]]
    kind: str
    region: int
    subregion: int
    world_count: int
    weight: float


def _level_numbers(columns: list[str]) -> list[int]:
    out = []
    for col in columns:
        if col.startswith("l") and col.endswith("_sid"):
            with suppress(ValueError):
                out.append(int(col[1:-4]))
    return sorted(out)


def _median_nn(xy: np.ndarray) -> float:
    if len(xy) < 2:
        return 0.0
    d, _ = cKDTree(xy).query(xy, k=2, workers=-1)
    return float(np.median(d[:, 1]))


def _pca_axes(xy: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    center = xy.mean(axis=0)
    if len(xy) < 3:
        return center, np.eye(2)
    cov = np.cov((xy - center).T)
    vals, vecs = np.linalg.eigh(cov)
    order = np.argsort(vals)[::-1]
    axes = vecs[:, order]
    if np.linalg.det(axes) < 0:
        axes[:, 1] *= -1
    return center, axes


def _straighten_nodes(
    source_xy: np.ndarray,
    nodes: np.ndarray,
    *,
    strength: float,
    grid_scale: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if len(nodes) < 3 or strength <= 0:
        center, axes = _pca_axes(source_xy)
        return nodes, center, axes
    center, axes = _pca_axes(source_xy)
    local = (nodes - center) @ axes
    spacing = _median_nn(nodes) * grid_scale
    if spacing <= 1e-9:
        return nodes, center, axes
    snapped = np.round(local / spacing) * spacing
    local2 = local * (1.0 - strength) + snapped * strength
    return center + local2 @ axes.T, center, axes


def _delaunay_edges(xy: np.ndarray) -> set[tuple[int, int]]:
    n = len(xy)
    if n < 2:
        return set()
    if n == 2:
        return {(0, 1)}
    try:
        tri = Delaunay(xy)
    except QhullError:
        order = np.argsort(xy[:, 0] + xy[:, 1] * 1e-6)
        return {
            tuple(sorted((int(a), int(b))))
            for a, b in zip(order[:-1], order[1:], strict=False)
        }
    edges: set[tuple[int, int]] = set()
    for simplex in tri.simplices:
        for a, b in (
            (simplex[0], simplex[1]),
            (simplex[1], simplex[2]),
            (simplex[2], simplex[0]),
        ):
            if a != b:
                edges.add(tuple(sorted((int(a), int(b)))))
    return edges


def _mst_edges(xy: np.ndarray, edges: set[tuple[int, int]]) -> set[tuple[int, int]]:
    n = len(xy)
    if n < 2:
        return set()
    if not edges:
        return {(i, i + 1) for i in range(n - 1)}
    rows = []
    cols = []
    data = []
    for a, b in edges:
        d = float(np.linalg.norm(xy[a] - xy[b]))
        rows.extend([a, b])
        cols.extend([b, a])
        data.extend([d, d])
    graph = coo_matrix((data, (rows, cols)), shape=(n, n)).tocsr()
    mst = minimum_spanning_tree(graph).tocoo()
    out = {
        tuple(sorted((int(a), int(b))))
        for a, b in zip(mst.row.tolist(), mst.col.tolist(), strict=True)
        if a != b
    }
    if len(out) >= n - 1:
        return out

    # Delaunay can fragment after degenerate fallback; bridge components by nearest
    # component centroids so every local network remains navigable.
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for a, b in out:
        union(a, b)
    while len({find(i) for i in range(n)}) > 1:
        comps: dict[int, list[int]] = defaultdict(list)
        for i in range(n):
            comps[find(i)].append(i)
        best: tuple[float, int, int] | None = None
        keys = list(comps)
        for i, ca in enumerate(keys):
            ia = comps[ca]
            for cb in keys[i + 1 :]:
                ib = comps[cb]
                d2 = ((xy[ia, None, :] - xy[ib][None, :, :]) ** 2).sum(axis=2)
                pos = np.unravel_index(int(d2.argmin()), d2.shape)
                candidate = (float(d2[pos]), ia[pos[0]], ib[pos[1]])
                if best is None or candidate[0] < best[0]:
                    best = candidate
        if best is None:
            break
        _, a, b = best
        out.add(tuple(sorted((a, b))))
        union(a, b)
    return out


def _looped_graph(
    xy: np.ndarray,
    *,
    loop_factor: float,
    max_edge_scale: float,
    max_edge_quantile: float,
) -> set[tuple[int, int]]:
    candidates = _delaunay_edges(xy)
    selected = _mst_edges(xy, candidates)
    if not candidates:
        return selected
    lengths = np.array([np.linalg.norm(xy[a] - xy[b]) for a, b in candidates])
    nn = _median_nn(xy)
    q = float(np.quantile(lengths, max_edge_quantile)) if len(lengths) else math.inf
    max_len = max(q, nn * max_edge_scale) if nn > 0 else q
    extras = [
        (float(np.linalg.norm(xy[a] - xy[b])), a, b)
        for a, b in candidates
        if (a, b) not in selected and np.linalg.norm(xy[a] - xy[b]) <= max_len
    ]
    extras.sort()
    target = max(0, int(round(len(xy) * loop_factor)))
    for _dist, a, b in extras[:target]:
        selected.add((a, b))
    return selected


def _line_coords(
    a: np.ndarray,
    b: np.ndarray,
    *,
    center: np.ndarray,
    axes: np.ndarray,
    orthogonal: float,
    salt: int,
) -> list[tuple[float, float]]:
    if orthogonal <= 0:
        return [(float(a[0]), float(a[1])), (float(b[0]), float(b[1]))]
    la = (a - center) @ axes
    lb = (b - center) @ axes
    delta = np.abs(lb - la)
    if max(delta) <= 1e-9 or min(delta) / max(delta) < orthogonal:
        return [(float(a[0]), float(a[1])), (float(b[0]), float(b[1]))]
    bend_local = np.array([lb[0], la[1]]) if salt % 2 else np.array([la[0], lb[1]])
    bend = center + bend_local @ axes.T
    return [
        (float(a[0]), float(a[1])),
        (float(bend[0]), float(bend[1])),
        (float(b[0]), float(b[1])),
    ]


def _edge_features(
    nodes: np.ndarray,
    edges: set[tuple[int, int]],
    *,
    source_xy: np.ndarray,
    kind: str,
    region: int,
    subregion: int,
    world_count: int,
    grid_strength: float,
    grid_scale: float,
    orthogonal: float,
) -> list[RoadFeature]:
    straight, center, axes = _straighten_nodes(
        source_xy, nodes, strength=grid_strength, grid_scale=grid_scale
    )
    out = []
    for a, b in sorted(edges):
        coords = _line_coords(
            straight[a],
            straight[b],
            center=center,
            axes=axes,
            orthogonal=orthogonal,
            salt=a * 1009 + b,
        )
        length = sum(
            math.dist(coords[i], coords[i + 1]) for i in range(len(coords) - 1)
        )
        if length <= 1e-9:
            continue
        out.append(
            RoadFeature(
                coords=coords,
                kind=kind,
                region=region,
                subregion=subregion,
                world_count=world_count,
                weight=length,
            )
        )
    return out


def _kmeans_nodes(xy: np.ndarray, k: int, seed: int) -> np.ndarray:
    if k >= len(xy):
        return xy.copy()
    km = MiniBatchKMeans(
        n_clusters=k,
        random_state=seed,
        batch_size=min(8192, max(1024, len(xy))),
        n_init="auto",
        reassignment_ratio=0.01,
    )
    km.fit(xy)
    return km.cluster_centers_.astype(np.float64)


def _local_roads(
    points: pl.DataFrame,
    *,
    top_col: str,
    sub_col: str,
    target_worlds: int,
    min_worlds: int,
    min_nodes: int,
    max_nodes: int,
    loop_factor: float,
    max_edge_scale: float,
    max_edge_quantile: float,
    grid_strength: float,
    grid_scale: float,
    orthogonal: float,
    seed: int,
) -> list[RoadFeature]:
    features: list[RoadFeature] = []
    for row in (
        points.group_by([top_col, sub_col])
        .len()
        .filter(pl.col("len") >= min_worlds)
        .sort("len", descending=True)
        .iter_rows(named=True)
    ):
        region = int(row[top_col])
        subregion = int(row[sub_col])
        n = int(row["len"])
        xy = (
            points.filter((pl.col(top_col) == region) & (pl.col(sub_col) == subregion))
            .select("x", "y")
            .to_numpy()
            .astype(np.float64)
        )
        k = int(round(n / max(target_worlds, 1)))
        k = min(max_nodes, max(min_nodes, k), max(2, min(max_nodes, len(xy))))
        if k < 2:
            continue
        nodes = _kmeans_nodes(xy, k, seed + subregion * 17)
        edges = _looped_graph(
            nodes,
            loop_factor=loop_factor,
            max_edge_scale=max_edge_scale,
            max_edge_quantile=max_edge_quantile,
        )
        features.extend(
            _edge_features(
                nodes,
                edges,
                source_xy=xy,
                kind="local",
                region=region,
                subregion=subregion,
                world_count=n,
                grid_strength=grid_strength,
                grid_scale=grid_scale,
                orthogonal=orthogonal,
            )
        )
    return features


def _arterial_roads(
    points: pl.DataFrame,
    *,
    top_col: str,
    sub_col: str,
    min_subregions: int,
    loop_factor: float,
    max_edge_scale: float,
    max_edge_quantile: float,
    grid_strength: float,
    grid_scale: float,
    seed: int,
) -> list[RoadFeature]:
    del seed
    features: list[RoadFeature] = []
    grouped = (
        points.group_by([top_col, sub_col])
        .agg(
            pl.col("x").mean().alias("x"),
            pl.col("y").mean().alias("y"),
            pl.len().alias("n"),
        )
        .sort("n", descending=True)
    )
    for row in (
        grouped.group_by(top_col)
        .len()
        .filter(pl.col("len") >= min_subregions)
        .iter_rows(named=True)
    ):
        region = int(row[top_col])
        sub = grouped.filter(pl.col(top_col) == region)
        xy = sub.select("x", "y").to_numpy().astype(np.float64)
        source_xy = (
            points.filter(pl.col(top_col) == region)
            .select("x", "y")
            .to_numpy()
            .astype(np.float64)
        )
        edges = _looped_graph(
            xy,
            loop_factor=loop_factor,
            max_edge_scale=max_edge_scale,
            max_edge_quantile=max_edge_quantile,
        )
        features.extend(
            _edge_features(
                xy,
                edges,
                source_xy=source_xy,
                kind="arterial",
                region=region,
                subregion=-1,
                world_count=int(sub["n"].sum()),
                grid_strength=grid_strength,
                grid_scale=grid_scale,
                orthogonal=0.0,
            )
        )
    return features


def _write_geojson(features: list[RoadFeature], out_path: Path) -> None:
    feats = []
    for i, road in enumerate(features):
        line = shapely.LineString(road.coords)
        if line.is_empty or line.length <= 0:
            continue
        feats.append(
            {
                "type": "Feature",
                "properties": {
                    "id": i,
                    "kind": road.kind,
                    "region": int(road.region),
                    "subregion": int(road.subregion),
                    "world_count": int(road.world_count),
                    "weight": float(road.weight),
                },
                "geometry": json.loads(shapely.to_geojson(line)),
            }
        )
    out_path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))
    counts = defaultdict(int)
    for f in feats:
        counts[f["properties"]["kind"]] += 1
    print(
        f"wrote {out_path} ({len(feats):,} roads; "
        + ", ".join(f"{k}={v:,}" for k, v in sorted(counts.items()))
        + ")"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--points", type=Path, required=True, help="app_points.parquet")
    ap.add_argument("--out", type=Path, required=True, help="roads.geojson")
    ap.add_argument("--top-level", type=int, default=None)
    ap.add_argument("--sub-level", type=int, default=None)
    ap.add_argument("--local-target-worlds", type=int, default=360)
    ap.add_argument("--local-min-worlds", type=int, default=180)
    ap.add_argument("--local-min-nodes", type=int, default=3)
    ap.add_argument("--local-max-nodes", type=int, default=56)
    ap.add_argument("--local-loop-factor", type=float, default=0.42)
    ap.add_argument("--local-max-edge-scale", type=float, default=2.2)
    ap.add_argument("--local-max-edge-quantile", type=float, default=0.68)
    ap.add_argument("--arterial-min-subregions", type=int, default=4)
    ap.add_argument("--arterial-loop-factor", type=float, default=0.25)
    ap.add_argument("--arterial-max-edge-scale", type=float, default=2.5)
    ap.add_argument("--arterial-max-edge-quantile", type=float, default=0.72)
    ap.add_argument("--grid-strength", type=float, default=0.55)
    ap.add_argument("--arterial-grid-strength", type=float, default=0.25)
    ap.add_argument("--grid-scale", type=float, default=0.95)
    ap.add_argument(
        "--orthogonal",
        type=float,
        default=0.35,
        help="minimum minor/major axis ratio before local edges become L-shaped",
    )
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    points = pl.read_parquet(args.points)
    levels = _level_numbers(points.columns)
    if not levels:
        raise SystemExit("app_points has no l{n}_sid hierarchy columns")
    top_level = args.top_level if args.top_level is not None else levels[-1]
    sub_level = args.sub_level if args.sub_level is not None else levels[-2]
    top_col = f"l{top_level}_sid"
    sub_col = f"l{sub_level}_sid"
    missing = [c for c in ("x", "y", top_col, sub_col) if c not in points.columns]
    if missing:
        raise SystemExit(f"app_points missing required columns: {missing}")
    points = points.select("x", "y", top_col, sub_col).drop_nulls()
    print(
        f"roads from {points.height:,} points; top={top_col}, sub={sub_col}; "
        f"subregions={points.select(sub_col).n_unique()}"
    )

    features = []
    features.extend(
        _arterial_roads(
            points,
            top_col=top_col,
            sub_col=sub_col,
            min_subregions=args.arterial_min_subregions,
            loop_factor=args.arterial_loop_factor,
            max_edge_scale=args.arterial_max_edge_scale,
            max_edge_quantile=args.arterial_max_edge_quantile,
            grid_strength=args.arterial_grid_strength,
            grid_scale=args.grid_scale,
            seed=args.seed,
        )
    )
    features.extend(
        _local_roads(
            points,
            top_col=top_col,
            sub_col=sub_col,
            target_worlds=args.local_target_worlds,
            min_worlds=args.local_min_worlds,
            min_nodes=args.local_min_nodes,
            max_nodes=args.local_max_nodes,
            loop_factor=args.local_loop_factor,
            max_edge_scale=args.local_max_edge_scale,
            max_edge_quantile=args.local_max_edge_quantile,
            grid_strength=args.grid_strength,
            grid_scale=args.grid_scale,
            orthogonal=args.orthogonal,
            seed=args.seed,
        )
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    _write_geojson(features, args.out)


if __name__ == "__main__":
    main()
