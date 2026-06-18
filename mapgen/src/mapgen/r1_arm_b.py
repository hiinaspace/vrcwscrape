"""R1 probe – Arm B: least-cost arterial baseline (Galin 2010-style).

Reusable logic called by scripts/run_r1_arm_b_baseline.py.

Coordinate convention: island frame, x = x0 + col*cell, y = y0 + row*cell
(cell centres are at x0 + (col+0.5)*cell, same as r1_inputs.py raster_coords).
All GeoJSON outputs use island-frame coordinates (no geographic CRS).

Pipeline steps
--------------
1. Density peaks  – skimage.feature.peak_local_max with tunable separation and
   threshold.
2. Cost field     – base + w_slope * norm_slope + w_river * river_indicator;
   outside-island cells set to np.inf.
3. Arterial paths – Delaunay triangulation over peaks → least-cost path per
   edge (skimage.graph.route_through_array); pruned by relative-cost ratio
   (beta-skeleton threshold).
4. Polygonize     – union arterials + boundary exterior ring → district
   polygons via shapely.ops.polygonize; degenerate polygons
   (area < min_district_area) merged into largest neighbour.
5. World counts   – point-in-polygon from island_points.parquet (polars).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import shapely
import shapely.geometry as sg
from scipy.spatial import Delaunay
from shapely.ops import polygonize, unary_union
from skimage.feature import peak_local_max
from skimage.graph import route_through_array

# ---------------------------------------------------------------------------
# Cost field weights (defaults; can be overridden from the script)
# ---------------------------------------------------------------------------

DEFAULT_COST_BASE: float = 1.0
DEFAULT_W_SLOPE: float = 8.0  # normalized slope contribution
DEFAULT_W_RIVER: float = 6.0  # p99 flow-accum crossing penalty (bridge)

# Peak detection defaults
DEFAULT_PEAK_MIN_DISTANCE_UNITS: float = 10.0  # island-frame units
DEFAULT_PEAK_THRESHOLD_FRAC: float = 0.03  # fraction of density max

# Network pruning: drop a Delaunay edge if its least-cost path cost ratio
# vs the best alternative through the network exceeds this factor.
# Set to np.inf to keep all edges.
DEFAULT_BETA_RATIO: float = 1.2

# District cleanup: polygons smaller than this (island units²) are merged
# into a neighbour.
DEFAULT_MIN_DISTRICT_AREA: float = 20.0

# Arterial simplification tolerance (island units)
DEFAULT_SIMPLIFY_TOLERANCE: float = 1.5


# ---------------------------------------------------------------------------
# 1. Density peaks
# ---------------------------------------------------------------------------


def find_density_peaks(
    density: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    *,
    min_distance_units: float = DEFAULT_PEAK_MIN_DISTANCE_UNITS,
    threshold_frac: float = DEFAULT_PEAK_THRESHOLD_FRAC,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (peak_xs, peak_ys, peak_densities) in island-frame coords.

    Parameters
    ----------
    density : (nrows, ncols) float32 density raster.
    x0, y0, cell : raster affine (cell centres at x0+(col+0.5)*cell).
    min_distance_units : minimum separation in island-frame units.
    threshold_frac : density threshold = threshold_frac * density.max().
    """
    min_dist_cells = max(1, int(round(min_distance_units / cell)))
    threshold_abs = float(density.max()) * threshold_frac

    coords = peak_local_max(
        density, min_distance=min_dist_cells, threshold_abs=threshold_abs
    )
    # coords shape (n, 2) → (row, col)
    peak_xs = x0 + (coords[:, 1] + 0.5) * cell
    peak_ys = y0 + (coords[:, 0] + 0.5) * cell
    peak_ds = density[coords[:, 0], coords[:, 1]]
    return peak_xs, peak_ys, peak_ds


# ---------------------------------------------------------------------------
# 2. Cost field
# ---------------------------------------------------------------------------


def build_cost_field(
    slope: np.ndarray,
    flow_accum: np.ndarray,
    mask: np.ndarray,
    *,
    base: float = DEFAULT_COST_BASE,
    w_slope: float = DEFAULT_W_SLOPE,
    w_river: float = DEFAULT_W_RIVER,
) -> np.ndarray:
    """Build per-cell cost raster for least-cost path routing.

    cost = base + w_slope * norm_slope + w_river * river_indicator
    Cells outside mask → np.inf (impassable).

    Parameters
    ----------
    slope : (nrows, ncols) slope magnitude (already gradient of height_carved).
    flow_accum : (nrows, ncols) D8 flow accumulation.
    mask : (nrows, ncols) bool, True inside island.
    """
    # Normalize slope to [0,1] over inside-island cells
    slope_inside = slope[mask]
    slope_max = float(slope_inside.max()) if slope_inside.size > 0 else 1.0
    if slope_max <= 0:
        slope_max = 1.0
    norm_slope = (slope / slope_max).astype(np.float64)

    # River indicator: flow_accum > p99 of inside-island values
    fa_inside = flow_accum[mask]
    river_threshold = (
        float(np.percentile(fa_inside, 99)) if fa_inside.size > 0 else np.inf
    )
    river_indicator = (flow_accum >= river_threshold).astype(np.float64)

    cost = base + w_slope * norm_slope + w_river * river_indicator
    cost[~mask] = np.inf
    return cost.astype(np.float64)


# ---------------------------------------------------------------------------
# 3. Arterial paths
# ---------------------------------------------------------------------------


def _xy_to_rowcol(
    xs: np.ndarray,
    ys: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    nrows: int,
    ncols: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Convert island-frame coords to integer row/col indices (clipped to grid)."""
    cols = np.clip(((xs - x0) / cell - 0.5).round().astype(int), 0, ncols - 1)
    rows = np.clip(((ys - y0) / cell - 0.5).round().astype(int), 0, nrows - 1)
    return rows, cols


def _rowcol_to_xy(
    rows: np.ndarray,
    cols: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Convert row/col indices back to island-frame cell-centre coords."""
    xs = x0 + (cols + 0.5) * cell
    ys = y0 + (rows + 0.5) * cell
    return xs, ys


def compute_arterials(
    peak_xs: np.ndarray,
    peak_ys: np.ndarray,
    cost: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    *,
    beta_ratio: float = DEFAULT_BETA_RATIO,
    simplify_tolerance: float = DEFAULT_SIMPLIFY_TOLERANCE,
) -> tuple[list[sg.LineString], list[dict[str, Any]]]:
    """Compute arterial network as island-frame LineStrings.

    Steps:
    1. Delaunay triangulation over peaks.
    2. Least-cost path (route_through_array) per Delaunay edge.
    3. Prune edges whose path cost ratio vs best alternative > beta_ratio.
       (simple relative-cost filter; keeps-all is also valid for a probe)
    4. Merge overlapping cell paths (natural dedup from integer raster).
    5. Simplify and return.

    Returns
    -------
    lines : list of simplified LineStrings in island frame.
    edge_records : list of dicts with edge metadata (for GeoJSON properties).
    """
    nrows, ncols = cost.shape
    n = len(peak_xs)

    if n < 2:
        return [], []

    pts = np.column_stack([peak_xs, peak_ys])

    # Delaunay triangulation
    if n == 2:
        # Only one possible edge
        edge_set = {(0, 1)}
    else:
        tri = Delaunay(pts)
        edge_set: set[tuple[int, int]] = set()
        for simplex in tri.simplices:
            for i in range(3):
                a, b = sorted((simplex[i], simplex[(i + 1) % 3]))
                edge_set.add((a, b))

    peak_rows, peak_cols = _xy_to_rowcol(peak_xs, peak_ys, x0, y0, cell, nrows, ncols)

    # Least-cost path per edge
    edge_list = sorted(edge_set)
    edge_paths: list[list[tuple[int, int]]] = []
    edge_costs: list[float] = []

    for i_a, i_b in edge_list:
        ra, ca = int(peak_rows[i_a]), int(peak_cols[i_a])
        rb, cb = int(peak_rows[i_b]), int(peak_cols[i_b])
        path, path_cost = route_through_array(
            cost, (ra, ca), (rb, cb), fully_connected=True, geometric=True
        )
        edge_paths.append(path)
        edge_costs.append(float(path_cost))

    # Beta-ratio pruning: for each edge check whether removing it still leaves
    # both endpoints mutually reachable (through other paths) at a cost within
    # beta_ratio of the direct path.  Simple approximation: sort edges by cost;
    # build a running "shortcut cost" dict for each pair via union-find of
    # reached pairs.  For the probe we use a straightforward pairwise check:
    # after tentatively removing edge (a,b), check if any chain of other edges
    # connects a to b with cost <= beta_ratio * cost(a,b).
    kept = _prune_edges(edge_list, edge_costs, beta_ratio)

    # Collect cell paths for kept edges and build LineStrings
    lines: list[sg.LineString] = []
    edge_records: list[dict[str, Any]] = []

    for idx, (i_a, i_b) in enumerate(edge_list):
        if not kept[idx]:
            continue
        path = edge_paths[idx]
        cost_val = edge_costs[idx]

        rows_p = [p[0] for p in path]
        cols_p = [p[1] for p in path]
        xs_p, ys_p = _rowcol_to_xy(np.array(rows_p), np.array(cols_p), x0, y0, cell)
        coords = list(zip(xs_p.tolist(), ys_p.tolist(), strict=True))
        if len(coords) < 2:
            continue

        line = sg.LineString(coords)
        if simplify_tolerance > 0:
            line = line.simplify(simplify_tolerance, preserve_topology=False)
        if line.is_empty or line.length < cell:
            continue

        length = float(line.length)
        lines.append(line)
        edge_records.append(
            {
                "peak_a": int(i_a),
                "peak_b": int(i_b),
                "path_cost": round(cost_val, 4),
                "length": round(length, 3),
            }
        )

    return lines, edge_records


def _prune_edges(
    edge_list: list[tuple[int, int]],
    edge_costs: list[float],
    beta_ratio: float,
) -> list[bool]:
    """Return boolean kept-mask for each edge.

    Strategy: for each edge (a,b), compute the shortest alternative path cost
    from a to b through the remaining graph.  Keep the edge if either:
    - no alternative path exists, OR
    - alternative cost > beta_ratio * direct_cost.

    Uses Dijkstra over the weighted graph of all other edges.
    """
    if beta_ratio <= 0 or beta_ratio == np.inf:
        return [True] * len(edge_list)

    import heapq

    # Build adjacency for all edges
    all_nodes: set[int] = set()
    for a, b in edge_list:
        all_nodes.add(a)
        all_nodes.add(b)
    node_list = sorted(all_nodes)

    def _dijkstra_without(skip_idx: int, src: int, dst: int) -> float:
        adj: dict[int, list[tuple[float, int]]] = {n: [] for n in node_list}
        for k, (a, b) in enumerate(edge_list):
            if k == skip_idx:
                continue
            adj[a].append((edge_costs[k], b))
            adj[b].append((edge_costs[k], a))
        dist: dict[int, float] = {n: np.inf for n in node_list}
        dist[src] = 0.0
        heap: list[tuple[float, int]] = [(0.0, src)]
        while heap:
            d, u = heapq.heappop(heap)
            if d > dist[u]:
                continue
            if u == dst:
                break
            for w, v in adj[u]:
                nd = d + w
                if nd < dist[v]:
                    dist[v] = nd
                    heapq.heappush(heap, (nd, v))
        return dist[dst]

    kept = []
    for idx, (i_a, i_b) in enumerate(edge_list):
        direct = edge_costs[idx]
        alt = _dijkstra_without(idx, i_a, i_b)
        # Keep if no alternative or alternative is more expensive than threshold
        keep = alt > beta_ratio * direct
        kept.append(keep)
    return kept


# ---------------------------------------------------------------------------
# 4. District polygonization
# ---------------------------------------------------------------------------


def polygonize_districts(
    arterial_lines: list[sg.LineString],
    boundary: sg.Polygon,
    *,
    min_district_area: float = DEFAULT_MIN_DISTRICT_AREA,
) -> list[sg.Polygon]:
    """Build district polygons from arterials + boundary exterior.

    Steps:
    1. Collect boundary exterior ring as a LinearRing / LineString.
    2. Union all arterials + boundary exterior into a planar graph.
    3. polygonize → set of face polygons.
    4. Keep only polygons that lie within the boundary.
    5. Merge degenerate polygons (area < min_district_area) into the
       adjacent district with the longest shared boundary.

    Returns list of district Polygons (sorted by area descending).
    """
    # Build the set of lines to polygonize: arterials + boundary ring
    boundary_ring = sg.LineString(list(boundary.exterior.coords))

    all_lines: list[sg.LineString] = list(arterial_lines) + [boundary_ring]
    merged = unary_union(all_lines)

    # polygonize
    raw_polys = list(polygonize(merged))

    # Filter to those inside the boundary
    inside = [p for p in raw_polys if boundary.contains(p) or boundary.covers(p)]
    if not inside:
        # Fall back: if polygonize gives no covered polys, try intersection
        inside = [
            p.intersection(boundary)
            for p in raw_polys
            if not p.intersection(boundary).is_empty
        ]
        inside = [p for p in inside if p.geom_type == "Polygon"]

    if not inside:
        return [boundary]

    # Merge degenerate polygons
    districts = _merge_slivers(inside, min_district_area)
    districts.sort(key=lambda p: p.area, reverse=True)
    return districts


def _merge_slivers(
    polys: list[sg.Polygon],
    min_area: float,
) -> list[sg.Polygon]:
    """Iteratively merge polygons with area < min_area into largest neighbor.

    A 'neighbor' is a polygon that shares more than a point with the sliver
    (i.e. shared boundary length > 0).
    """
    polys_list = list(polys)

    changed = True
    while changed:
        changed = False
        slivers = [i for i, p in enumerate(polys_list) if p.area < min_area]
        if not slivers:
            break
        # Merge smallest sliver first
        slivers.sort(key=lambda i: polys_list[i].area)
        sliver_idx = slivers[0]
        sliver = polys_list[sliver_idx]

        best_neighbor = None
        best_shared_len = 0.0
        for j, other in enumerate(polys_list):
            if j == sliver_idx:
                continue
            shared = sliver.boundary.intersection(other.boundary)
            if shared.is_empty:
                continue
            shared_len = shared.length if hasattr(shared, "length") else 0.0
            if shared_len > best_shared_len:
                best_shared_len = shared_len
                best_neighbor = j

        if best_neighbor is None:
            # No neighbor with shared boundary; remove sliver anyway
            polys_list.pop(sliver_idx)
        else:
            merged = polys_list[sliver_idx].union(polys_list[best_neighbor])
            # Ensure result is a single polygon
            if merged.geom_type == "MultiPolygon":
                merged = max(merged.geoms, key=lambda p: p.area)
            # Replace neighbor; remove sliver
            hi, lo = sorted([sliver_idx, best_neighbor], reverse=True)
            polys_list.pop(hi)
            polys_list.pop(lo)
            polys_list.append(merged)
        changed = True

    return polys_list


# ---------------------------------------------------------------------------
# 5. World count assignment
# ---------------------------------------------------------------------------


def assign_worlds_to_districts(
    districts: list[sg.Polygon],
    points: pl.DataFrame,
) -> list[int]:
    """Return per-district world count (same order as districts).

    Uses polars for the point table; point coords are columns 'x', 'y'.
    """
    xs = points["x"].to_numpy()
    ys = points["y"].to_numpy()
    counts = []
    for poly in districts:
        inside = shapely.contains_xy(poly, xs, ys)
        counts.append(int(inside.sum()))
    return counts


# ---------------------------------------------------------------------------
# GeoJSON serialization helpers
# ---------------------------------------------------------------------------


def peaks_to_geojson(
    peak_xs: np.ndarray,
    peak_ys: np.ndarray,
    peak_ds: np.ndarray,
) -> dict[str, Any]:
    features = []
    for i in range(len(peak_xs)):
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(peak_xs[i]), float(peak_ys[i])],
                },
                "properties": {
                    "peak_id": i,
                    "density": float(peak_ds[i]),
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def arterials_to_geojson(
    lines: list[sg.LineString],
    edge_records: list[dict[str, Any]],
) -> dict[str, Any]:
    features = []
    for line, rec in zip(lines, edge_records, strict=True):
        features.append(
            {
                "type": "Feature",
                "geometry": sg.mapping(line),
                "properties": rec,
            }
        )
    return {"type": "FeatureCollection", "features": features}


def districts_to_geojson(
    districts: list[sg.Polygon],
    world_counts: list[int],
) -> dict[str, Any]:
    features = []
    for i, (poly, wc) in enumerate(zip(districts, world_counts, strict=True)):
        features.append(
            {
                "type": "Feature",
                "geometry": sg.mapping(poly),
                "properties": {
                    "district_id": i,
                    "area": round(float(poly.area), 3),
                    "world_count": wc,
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------


def render_arm_b_png(
    density: np.ndarray,
    height_carved: np.ndarray,
    peak_xs: np.ndarray,
    peak_ys: np.ndarray,
    arterial_lines: list[sg.LineString],
    districts: list[sg.Polygon],
    boundary: sg.Polygon,
    out_path: Path,
    x0: float,
    y0: float,
    cell: float,
) -> None:
    """Render hillshade backdrop + arterials + district boundaries + peaks."""
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    from matplotlib.colors import LightSource

    nrows, ncols = height_carved.shape

    fig, ax = plt.subplots(figsize=(12, 8))

    # Density backdrop (inferno, semi-transparent)
    extent: tuple[float, float, float, float] = (
        x0,
        x0 + ncols * cell,
        y0,
        y0 + nrows * cell,
    )
    ax.imshow(
        density,
        origin="lower",
        extent=extent,
        cmap="inferno",
        alpha=0.55,
        aspect="equal",
        zorder=0,
    )

    # Hillshade overlay
    ls = LightSource(azdeg=315, altdeg=45)
    hs = ls.hillshade(height_carved.astype(np.float64), vert_exag=4.0)
    ax.imshow(
        hs,
        origin="lower",
        extent=extent,
        cmap="gray",
        alpha=0.35,
        aspect="equal",
        zorder=1,
    )

    # Island boundary
    bx, by = boundary.exterior.xy
    ax.plot(bx, by, color="black", lw=1.5, zorder=5)

    # District boundaries (thin, light)
    for poly in districts:
        dx, dy = poly.exterior.xy
        ax.plot(dx, dy, color="#666666", lw=0.8, alpha=0.8, zorder=4)

    # Arterials (dark red)
    for line in arterial_lines:
        if line.geom_type == "LineString":
            lx, ly = line.xy
            ax.plot(lx, ly, color="#8b0000", lw=1.8, zorder=6, solid_capstyle="round")
        elif line.geom_type == "MultiLineString":
            for part in line.geoms:
                lx, ly = part.xy
                ax.plot(
                    lx,
                    ly,
                    color="#8b0000",
                    lw=1.8,
                    zorder=6,
                    solid_capstyle="round",
                )

    # Peaks
    ax.scatter(
        peak_xs,
        peak_ys,
        s=60,
        c="#ff4400",
        edgecolors="white",
        linewidths=0.8,
        zorder=7,
    )

    n_districts = len(districts)
    n_arterials = len(arterial_lines)
    n_peaks = len(peak_xs)
    ax.set_title(
        f"R1 Arm B: least-cost arterial baseline  "
        f"({n_peaks} peaks, {n_arterials} arterials, {n_districts} districts)",
        fontsize=11,
    )
    ax.set_xlabel("island x (units)")
    ax.set_ylabel("island y (units)")
    ax.set_aspect("equal")

    # Legend
    legend_handles = [
        mpatches.Patch(color="#8b0000", label="Arterials"),
        mpatches.Patch(color="#666666", label="Districts"),
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor="#ff4400",
            markersize=8,
            label="Peaks",
        ),
    ]
    ax.legend(handles=legend_handles, loc="upper left", fontsize=9)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Island mask reconstruction (from boundary polygon)
# ---------------------------------------------------------------------------


def boundary_mask(
    boundary: sg.Polygon,
    x0: float,
    y0: float,
    cell: float,
    nrows: int,
    ncols: int,
) -> np.ndarray:
    """Reconstruct boolean island mask from boundary polygon and raster params."""
    cols = np.arange(ncols)
    rows = np.arange(nrows)
    Xc = x0 + (cols + 0.5) * cell
    Yc = y0 + (rows + 0.5) * cell
    XX, YY = np.meshgrid(Xc, Yc)
    pts_x = XX.ravel()
    pts_y = YY.ravel()
    inside = shapely.contains_xy(boundary, pts_x, pts_y)
    return inside.reshape(nrows, ncols)


# ---------------------------------------------------------------------------
# High-level entry point
# ---------------------------------------------------------------------------


def run_arm_b(
    in_dir: Path,
    out_dir: Path,
    *,
    peak_min_distance_units: float = DEFAULT_PEAK_MIN_DISTANCE_UNITS,
    peak_threshold_frac: float = DEFAULT_PEAK_THRESHOLD_FRAC,
    cost_base: float = DEFAULT_COST_BASE,
    w_slope: float = DEFAULT_W_SLOPE,
    w_river: float = DEFAULT_W_RIVER,
    beta_ratio: float = DEFAULT_BETA_RATIO,
    simplify_tolerance: float = DEFAULT_SIMPLIFY_TOLERANCE,
    min_district_area: float = DEFAULT_MIN_DISTRICT_AREA,
) -> dict[str, Any]:
    """Run full Arm B pipeline; return manifest dict."""
    out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------
    print("Loading inputs…")
    fields = np.load(in_dir / "fields.npz")
    density = fields["density"]
    height_carved = fields["height_carved"]
    flow_accum = fields["flow_accum"]
    slope = fields["slope"]
    x0 = float(fields["x0"])
    y0 = float(fields["y0"])
    cell = float(fields["cell"])
    nrows, ncols = density.shape

    with (in_dir / "island_boundary.geojson").open() as f:
        bnd_gj = json.load(f)
    boundary: sg.Polygon = sg.shape(bnd_gj["features"][0]["geometry"])

    points = pl.read_parquet(in_dir / "island_points.parquet")

    # ------------------------------------------------------------------
    # Island mask
    # ------------------------------------------------------------------
    print("Building island mask…")
    mask = boundary_mask(boundary, x0, y0, cell, nrows, ncols)

    # ------------------------------------------------------------------
    # 1. Peaks
    # ------------------------------------------------------------------
    print("Finding density peaks…")
    peak_xs, peak_ys, peak_ds = find_density_peaks(
        density,
        x0,
        y0,
        cell,
        min_distance_units=peak_min_distance_units,
        threshold_frac=peak_threshold_frac,
    )
    n_peaks = len(peak_xs)
    print(f"  {n_peaks} peaks found")

    # ------------------------------------------------------------------
    # 2. Cost field
    # ------------------------------------------------------------------
    print("Building cost field…")
    cost = build_cost_field(
        slope,
        flow_accum,
        mask,
        base=cost_base,
        w_slope=w_slope,
        w_river=w_river,
    )

    # ------------------------------------------------------------------
    # 3. Arterial paths
    # ------------------------------------------------------------------
    print("Computing arterial network…")
    arterial_lines, edge_records = compute_arterials(
        peak_xs,
        peak_ys,
        cost,
        x0,
        y0,
        cell,
        beta_ratio=beta_ratio,
        simplify_tolerance=simplify_tolerance,
    )
    n_arterials = len(arterial_lines)
    total_arterial_length = sum(ln.length for ln in arterial_lines)
    print(
        f"  {n_arterials} arterial segments, total length={total_arterial_length:.1f}"
    )

    # ------------------------------------------------------------------
    # 4. Districts
    # ------------------------------------------------------------------
    print("Polygonizing districts…")
    districts = polygonize_districts(
        arterial_lines,
        boundary,
        min_district_area=min_district_area,
    )
    n_districts = len(districts)
    world_counts = assign_worlds_to_districts(districts, points)
    print(f"  {n_districts} districts, world counts: {world_counts}")

    # ------------------------------------------------------------------
    # 5. District area stats
    # ------------------------------------------------------------------
    areas = [float(d.area) for d in districts]
    area_arr = np.array(areas)
    area_stats: dict[str, float] = {
        "min": float(area_arr.min()),
        "max": float(area_arr.max()),
        "mean": float(area_arr.mean()),
        "median": float(np.median(area_arr)),
        "p25": float(np.percentile(area_arr, 25)),
        "p75": float(np.percentile(area_arr, 75)),
    }

    # ------------------------------------------------------------------
    # Write GeoJSON outputs
    # ------------------------------------------------------------------
    print("Writing GeoJSON…")
    peaks_gj = peaks_to_geojson(peak_xs, peak_ys, peak_ds)
    with (out_dir / "peaks.geojson").open("w") as f:
        json.dump(peaks_gj, f, indent=2)

    arterials_gj = arterials_to_geojson(arterial_lines, edge_records)
    with (out_dir / "arterials.geojson").open("w") as f:
        json.dump(arterials_gj, f, indent=2)

    districts_gj = districts_to_geojson(districts, world_counts)
    with (out_dir / "districts.geojson").open("w") as f:
        json.dump(districts_gj, f, indent=2)

    # ------------------------------------------------------------------
    # Render PNG
    # ------------------------------------------------------------------
    print("Rendering arm_b.png…")
    render_arm_b_png(
        density,
        height_carved,
        peak_xs,
        peak_ys,
        arterial_lines,
        districts,
        boundary,
        out_dir / "arm_b.png",
        x0,
        y0,
        cell,
    )

    # ------------------------------------------------------------------
    # Manifest
    # ------------------------------------------------------------------
    manifest: dict[str, Any] = {
        "arm": "B",
        "description": "Galin 2010-style least-cost arterial baseline",
        "params": {
            "peak_min_distance_units": peak_min_distance_units,
            "peak_threshold_frac": peak_threshold_frac,
            "cost_base": cost_base,
            "w_slope": w_slope,
            "w_river": w_river,
            "beta_ratio": beta_ratio,
            "simplify_tolerance": simplify_tolerance,
            "min_district_area": min_district_area,
        },
        "cost_field_note": (
            "cost = base + w_slope*norm_slope + w_river*(flow_accum>p99); "
            "outside island = inf"
        ),
        "results": {
            "n_peaks": n_peaks,
            "n_arterials": n_arterials,
            "total_arterial_length": round(total_arterial_length, 2),
            "n_districts": n_districts,
            "district_area_stats": area_stats,
            "district_world_counts": world_counts,
        },
        "outputs": [
            "peaks.geojson",
            "arterials.geojson",
            "districts.geojson",
            "arm_b.png",
            "arm_b_manifest.json",
        ],
    }
    with (out_dir / "arm_b_manifest.json").open("w") as f:
        json.dump(manifest, f, indent=2)
    print("Wrote arm_b_manifest.json")

    return manifest
