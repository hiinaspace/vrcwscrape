"""R1 probe – cross-arm comparison metrics and side-by-side renders.

Coordinate convention: island frame, x = x0 + col*cell, y = y0 + row*cell
(cell centres at x0 + (col+0.5)*cell, same as r1_inputs.py raster_coords).

Layouts compared
----------------
- Arm A / baseline
- Arm A / regional
- Arm A / regional_fine
- Arm A / regional_strong
- Arm B (least-cost arterial baseline)
- KMeans (l0 clusters from island_points.parquet, vectorized from raster)

Metrics (per layout)
--------------------
- district_count
- district area: median, CV, min, max
- density–area Spearman correlation (negative = good: dense cores → small districts)
- world-count balance: CV of per-district world counts
- shape: convexity median + min, compactness (4πA/P²) median
- arterial metrics (Arm A / B only): total length, terrain alignment fraction,
  river crossing count
- world_displacement: N/A in R1 (no worlds are moved)

Outputs
-------
- comparison.json
- comparison.md
- compare_main.png   (3 panels: Arm A regional_strong | Arm B | KMeans)
- compare_contact.png (6 panels: all 4 Arm A configs + Arm B + KMeans)
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import shapely
import shapely.geometry as sg
from matplotlib.colors import LightSource
from scipy.spatial import KDTree
from scipy.stats import spearmanr
from skimage.measure import find_contours

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

Districts = list[sg.Polygon]
Arterials = list[sg.LineString]

# ---------------------------------------------------------------------------
# Raster / geometry helpers
# ---------------------------------------------------------------------------


def _raster_mask(
    boundary: sg.Polygon,
    x0: float,
    y0: float,
    cell: float,
    nrows: int,
    ncols: int,
) -> np.ndarray:
    """Boolean mask of raster cells whose centre lies inside boundary."""
    cols = np.arange(ncols)
    rows = np.arange(nrows)
    Xc = x0 + (cols + 0.5) * cell
    Yc = y0 + (rows + 0.5) * cell
    XX, YY = np.meshgrid(Xc, Yc)
    inside = shapely.contains_xy(boundary, XX.ravel(), YY.ravel())
    return inside.reshape(nrows, ncols)


def _label_raster_by_nearest_l0(
    l0_ids: np.ndarray,
    l0_xs: np.ndarray,
    l0_ys: np.ndarray,
    mask: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    nrows: int,
    ncols: int,
) -> tuple[np.ndarray, dict[int, int]]:
    """Label every inside-mask raster cell with the l0_id of the nearest world.

    Returns
    -------
    label_grid : (nrows, ncols) int array; 0 = outside island.
    id_to_label : mapping from l0_id integer to contiguous label integer.
    """
    # Grid cell centres for inside-mask cells
    rows_inside, cols_inside = np.where(mask)
    xs_inside = x0 + (cols_inside + 0.5) * cell
    ys_inside = y0 + (rows_inside + 0.5) * cell

    pts = np.column_stack([xs_inside, ys_inside])
    world_pts = np.column_stack([l0_xs, l0_ys])

    tree = KDTree(world_pts)
    _, nearest_idx = tree.query(pts, k=1, workers=-1)
    nearest_l0 = l0_ids[nearest_idx]

    # Map l0_id → contiguous label (1-based so 0 = outside)
    unique_ids = np.unique(nearest_l0)
    id_to_label = {int(uid): i + 1 for i, uid in enumerate(unique_ids)}

    label_grid = np.zeros((nrows, ncols), dtype=np.int32)
    labels = np.array([id_to_label[int(lid)] for lid in nearest_l0], dtype=np.int32)
    label_grid[rows_inside, cols_inside] = labels

    return label_grid, id_to_label


def _vectorize_label(
    label_grid: np.ndarray,
    label_val: int,
    x0: float,
    y0: float,
    cell: float,
    boundary: sg.Polygon,
) -> sg.Polygon | sg.MultiPolygon | None:
    """Convert a single label region to a (Multi)Polygon.

    Uses marching squares via skimage.find_contours on the binary mask, then
    converts contour (row, col) coords to island frame and clips to boundary.
    """
    mask_bin = (label_grid == label_val).astype(float)
    contours = find_contours(mask_bin, 0.5)
    if not contours:
        return None

    polys: list[sg.Polygon] = []
    for contour in contours:
        # contour: (N, 2) in (row, col) pixel coordinates
        # Convert to island frame: x = x0 + (col + 0.5) * cell, same for y
        xs = x0 + (contour[:, 1] + 0.5) * cell
        ys = y0 + (contour[:, 0] + 0.5) * cell
        coords = list(zip(xs.tolist(), ys.tolist(), strict=False))
        if len(coords) < 4:
            continue
        p = sg.Polygon(coords)
        if not p.is_valid:
            p = p.buffer(0)
        if p.is_empty:
            continue
        polys.append(p)

    if not polys:
        return None

    # Separate outer rings (positive area) from holes (negative orientation)
    # skimage find_contours: largest contour = outer ring, inner = holes
    outer = max(polys, key=lambda p: p.area)
    smaller = [p for p in polys if p is not outer]

    if not smaller:
        result: sg.Polygon | sg.MultiPolygon = outer
    else:
        # Attempt to punch holes; fall back to union otherwise
        geom = outer
        for hole in smaller:
            try:
                diff = geom.difference(hole)
                if not diff.is_empty and diff.area > 0:
                    geom = diff
            except Exception:  # noqa: BLE001
                pass
        result = geom

    # Clip to island boundary
    clipped = result.intersection(boundary)
    if clipped.is_empty:
        return None
    return clipped  # type: ignore[return-value]


def _build_kmeans_districts(
    points: pl.DataFrame,
    boundary: sg.Polygon,
    mask: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    nrows: int,
    ncols: int,
) -> tuple[Districts, dict[int, str], dict[int, int]]:
    """Build KMeans l0 districts from island_points.parquet.

    Returns
    -------
    districts : list of Polygons (one per unique l0_id)
    id_to_name : l0_id → l0_name
    id_to_world_count : l0_id → world_count (recomputed by point-in-polygon)
    """
    l0_ids = points["l0_id"].to_numpy()
    l0_xs = points["x"].to_numpy()
    l0_ys = points["y"].to_numpy()
    l0_names_series = points["l0_id", "l0_name"].unique()

    id_to_name: dict[int, str] = {}
    for row in l0_names_series.iter_rows():
        id_to_name[int(row[0])] = str(row[1])

    label_grid, id_to_label = _label_raster_by_nearest_l0(
        l0_ids, l0_xs, l0_ys, mask, x0, y0, cell, nrows, ncols
    )
    label_to_id = {v: k for k, v in id_to_label.items()}

    districts: Districts = []
    district_l0_ids: list[int] = []

    for label_val, l0_id in sorted(label_to_id.items()):
        poly = _vectorize_label(label_grid, label_val, x0, y0, cell, boundary)
        if poly is None:
            continue
        if poly.geom_type not in ("Polygon", "MultiPolygon"):
            continue
        # Normalize to a Polygon for area access (use largest if MultiPolygon)
        if poly.geom_type == "MultiPolygon":
            poly = max(poly.geoms, key=lambda p: p.area)  # type: ignore[union-attr]
        districts.append(poly)
        district_l0_ids.append(l0_id)

    # Recompute world counts by point-in-polygon
    xs_all = points["x"].to_numpy()
    ys_all = points["y"].to_numpy()
    id_to_world_count: dict[int, int] = {}
    for poly, l0_id in zip(districts, district_l0_ids, strict=True):
        inside = shapely.contains_xy(poly, xs_all, ys_all)
        id_to_world_count[l0_id] = int(inside.sum())

    return districts, id_to_name, id_to_world_count


# ---------------------------------------------------------------------------
# Density statistics per district
# ---------------------------------------------------------------------------


def _district_mean_density(
    poly: sg.Polygon,
    density: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
) -> float:
    """Mean density raster value for cells inside poly."""
    nrows, ncols = density.shape
    xmin, ymin, xmax, ymax = poly.bounds
    # Restrict to bounding box for efficiency
    col0 = max(0, int((xmin - x0) / cell))
    col1 = min(ncols, int(math.ceil((xmax - x0) / cell)) + 1)
    row0 = max(0, int((ymin - y0) / cell))
    row1 = min(nrows, int(math.ceil((ymax - y0) / cell)) + 1)

    cols = np.arange(col0, col1)
    rows = np.arange(row0, row1)
    if cols.size == 0 or rows.size == 0:
        return 0.0

    CC, RR = np.meshgrid(cols, rows)
    xs = x0 + (CC + 0.5) * cell
    ys = y0 + (RR + 0.5) * cell

    inside = shapely.contains_xy(poly, xs.ravel(), ys.ravel())
    vals = density[RR.ravel(), CC.ravel()][inside]
    if vals.size == 0:
        return 0.0
    return float(vals.mean())


# ---------------------------------------------------------------------------
# Metric computations
# ---------------------------------------------------------------------------


def _cv(arr: np.ndarray) -> float:
    """Coefficient of variation (std / mean); 0 if mean == 0."""
    m = float(arr.mean())
    if m == 0:
        return 0.0
    return float(arr.std()) / m


def _convexity(poly: sg.Polygon) -> float:
    """area / convex_hull.area; 1.0 = convex."""
    ch = poly.convex_hull
    if ch.area == 0:
        return 0.0
    return min(1.0, poly.area / ch.area)


def _compactness(poly: sg.Polygon) -> float:
    """4π * area / perimeter²; 1.0 = circle."""
    p = poly.exterior.length
    if p == 0:
        return 0.0
    return min(1.0, 4 * math.pi * poly.area / (p * p))


def _shape_metrics(districts: Districts) -> dict[str, float]:
    convexities = [_convexity(d) for d in districts]
    compactnesses = [_compactness(d) for d in districts]
    return {
        "convexity_median": float(np.median(convexities)),
        "convexity_min": float(np.min(convexities)),
        "compactness_median": float(np.median(compactnesses)),
    }


def _area_metrics(districts: Districts) -> dict[str, float]:
    areas = np.array([d.area for d in districts])
    return {
        "median": float(np.median(areas)),
        "cv": round(_cv(areas), 4),
        "min": float(areas.min()),
        "max": float(areas.max()),
    }


def _density_area_correlation(
    districts: Districts,
    density: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
) -> dict[str, Any]:
    if len(districts) < 3:
        return {"spearman_r": None, "spearman_p": None, "n": len(districts)}
    mean_dens = [_district_mean_density(d, density, x0, y0, cell) for d in districts]
    areas = [d.area for d in districts]
    r, p = spearmanr(mean_dens, areas)
    return {
        "spearman_r": round(float(r), 4),
        "spearman_p": round(float(p), 4),
        "n": len(districts),
    }


def _world_count_balance(world_counts: list[int]) -> float:
    arr = np.array(world_counts, dtype=float)
    if arr.sum() == 0:
        return 0.0
    return round(_cv(arr), 4)


def _arterial_metrics(
    arterials: Arterials,
    slope: np.ndarray,
    flow_accum: np.ndarray,
    mask: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    slope_p50_threshold: float,
    river_threshold: float,
) -> dict[str, Any]:
    """Compute total length, terrain alignment fraction, river crossings."""
    if not arterials:
        return {
            "total_length": 0.0,
            "terrain_alignment_frac": None,
            "terrain_alignment_slope_threshold": round(slope_p50_threshold, 6),
            "river_crossings": 0,
            "river_threshold_flow_accum": round(river_threshold, 2),
        }

    total_length = sum(a.length for a in arterials)

    # Sample arterials at cell resolution along their length
    nrows, ncols = slope.shape

    low_slope_len = 0.0
    river_crossing_count = 0

    for art in arterials:
        # Sample points along the arterial at cell/2 intervals
        step = cell / 2.0
        if step <= 0:
            step = 0.1
        n_samples = max(2, int(art.length / step) + 1)
        fracs = np.linspace(0, 1, n_samples)
        # shapely interpolate by fraction
        pts = [art.interpolate(f, normalized=True) for f in fracs]
        xs = np.array([p.x for p in pts])
        ys = np.array([p.y for p in pts])

        # Convert to raster row/col
        cols = np.clip(((xs - x0) / cell - 0.5).round().astype(int), 0, ncols - 1)
        rows = np.clip(((ys - y0) / cell - 0.5).round().astype(int), 0, nrows - 1)

        seg_len = art.length / max(1, n_samples - 1)

        prev_river = False
        for r, c in zip(rows.tolist(), cols.tolist(), strict=False):
            slope_val = float(slope[r, c])
            fa_val = float(flow_accum[r, c])

            if slope_val <= slope_p50_threshold:
                low_slope_len += seg_len

            is_river = fa_val >= river_threshold
            if is_river and not prev_river:
                river_crossing_count += 1
            prev_river = is_river

    terrain_frac = low_slope_len / total_length if total_length > 0 else 0.0

    return {
        "total_length": round(total_length, 2),
        "terrain_alignment_frac": round(terrain_frac, 4),
        "terrain_alignment_slope_threshold": round(slope_p50_threshold, 6),
        "river_crossings": river_crossing_count,
        "river_threshold_flow_accum": round(river_threshold, 2),
    }


# ---------------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------------


def _load_districts_geojson(path: Path) -> Districts:
    with path.open() as f:
        gj = json.load(f)
    result: Districts = []
    for feat in gj["features"]:
        g = sg.shape(feat["geometry"])
        if g.geom_type == "Polygon":
            result.append(g)
        elif g.geom_type == "MultiPolygon":
            # Use largest part
            result.append(max(g.geoms, key=lambda p: p.area))
    return result


def _load_arterials_geojson(path: Path) -> Arterials:
    with path.open() as f:
        gj = json.load(f)
    result: Arterials = []
    for feat in gj["features"]:
        g = sg.shape(feat["geometry"])
        if g.geom_type == "LineString":
            result.append(g)
        elif g.geom_type == "MultiLineString":
            for part in g.geoms:
                result.append(part)
    return result


def _load_world_counts_from_geojson(path: Path) -> list[int]:
    with path.open() as f:
        gj = json.load(f)
    return [int(feat["properties"].get("world_count", 0)) for feat in gj["features"]]


# ---------------------------------------------------------------------------
# Full metrics for one layout
# ---------------------------------------------------------------------------


def compute_layout_metrics(
    label: str,
    districts: Districts,
    arterials: Arterials | None,
    world_counts: list[int],
    density: np.ndarray,
    slope: np.ndarray,
    flow_accum: np.ndarray,
    mask: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    slope_p50_threshold: float,
    river_threshold: float,
) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "layout": label,
        "district_count": len(districts),
        "district_area": _area_metrics(districts),
        "density_area_corr": _density_area_correlation(
            districts, density, x0, y0, cell
        ),
        "world_count_balance_cv": _world_count_balance(world_counts),
        "shape": _shape_metrics(districts),
        "world_displacement": "N/A (R1: no worlds moved)",
    }

    if arterials is not None:
        metrics["arterials"] = _arterial_metrics(
            arterials,
            slope,
            flow_accum,
            mask,
            x0,
            y0,
            cell,
            slope_p50_threshold,
            river_threshold,
        )
    else:
        metrics["arterials"] = "N/A (KMeans has no arterials)"

    return metrics


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------

# Colour palette for layouts in contact sheet
_LAYOUT_COLOURS = {
    "arm_a_baseline": "#2166ac",
    "arm_a_regional": "#1a9850",
    "arm_a_regional_fine": "#d73027",
    "arm_a_regional_strong": "#7570b3",
    "arm_b": "#8b0000",
    "kmeans": "#4d4d4d",
}


def _render_density_backdrop(
    ax: Any,
    density: np.ndarray,
    height_carved: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    nrows: int,
    ncols: int,
) -> None:
    extent = (x0, x0 + ncols * cell, y0, y0 + nrows * cell)
    ax.imshow(
        density,
        origin="lower",
        extent=extent,
        cmap="inferno",
        alpha=0.5,
        aspect="equal",
        zorder=0,
    )
    ls = LightSource(azdeg=315, altdeg=45)
    hs = ls.hillshade(height_carved.astype(np.float64), vert_exag=4.0)
    ax.imshow(
        hs,
        origin="lower",
        extent=extent,
        cmap="gray",
        alpha=0.3,
        aspect="equal",
        zorder=1,
    )


def _render_boundary(ax: Any, boundary: sg.Polygon, lw: float = 1.5) -> None:
    bx, by = boundary.exterior.xy
    ax.plot(bx, by, color="black", lw=lw, zorder=8)


def _render_districts(
    ax: Any,
    districts: Districts,
    edge_color: str = "#333333",
    fill_alpha: float = 0.08,
    fill_color: str = "#aaaaaa",
    lw: float = 0.8,
) -> None:
    for poly in districts:
        if poly.is_empty:
            continue
        try:
            rings = [poly.exterior] + list(poly.interiors)
        except AttributeError:
            continue
        for ring in rings:
            xs, ys = ring.xy
            ax.fill(xs, ys, alpha=fill_alpha, color=fill_color, zorder=2)
            ax.plot(xs, ys, color=edge_color, lw=lw, alpha=0.9, zorder=3)


def _render_arterials(
    ax: Any,
    arterials: Arterials,
    color: str = "#8b0000",
    lw: float = 1.4,
) -> None:
    for art in arterials:
        if art.geom_type == "LineString":
            xs, ys = art.xy
            ax.plot(xs, ys, color=color, lw=lw, zorder=6, solid_capstyle="round")


def _set_extent(ax: Any, boundary: sg.Polygon, pad: float = 2.0) -> None:
    minx, miny, maxx, maxy = boundary.bounds
    ax.set_xlim(minx - pad, maxx + pad)
    ax.set_ylim(miny - pad, maxy + pad)
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])


def render_main_png(
    density: np.ndarray,
    height_carved: np.ndarray,
    boundary: sg.Polygon,
    arm_a_regional_strong_districts: Districts,
    arm_a_regional_strong_arterials: Arterials,
    arm_b_districts: Districts,
    arm_b_arterials: Arterials,
    kmeans_districts: Districts,
    x0: float,
    y0: float,
    cell: float,
    out_path: Path,
) -> None:
    """3-panel side-by-side: Arm A regional_strong | Arm B | KMeans."""
    nrows, ncols = density.shape

    fig, axes = plt.subplots(1, 3, figsize=(20, 7))

    panels = [
        (
            axes[0],
            "Arm A / regional_strong",
            arm_a_regional_strong_districts,
            arm_a_regional_strong_arterials,
            "#7570b3",
        ),
        (axes[1], "Arm B (least-cost)", arm_b_districts, arm_b_arterials, "#8b0000"),
        (axes[2], "KMeans l0 districts", kmeans_districts, None, "#4d4d4d"),
    ]

    for ax, title, dists, arts, colour in panels:
        _render_density_backdrop(ax, density, height_carved, x0, y0, cell, nrows, ncols)
        _render_boundary(ax, boundary)
        _render_districts(
            ax, dists, edge_color=colour, fill_color=colour, fill_alpha=0.07
        )
        if arts is not None:
            _render_arterials(ax, arts, color=colour)
        _set_extent(ax, boundary, pad=2.0)
        ax.set_title(
            f"{title}\n({len(dists)} districts"
            + (f", {len(arts)} arterials" if arts else "")
            + ")",
            fontsize=10,
        )

    fig.suptitle(
        "R1 Regional Probe – Cross-Arm Comparison (density backdrop + hillshade)",
        fontsize=12,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def render_contact_png(
    density: np.ndarray,
    height_carved: np.ndarray,
    boundary: sg.Polygon,
    layouts: list[tuple[str, Districts, Arterials | None, str]],
    x0: float,
    y0: float,
    cell: float,
    out_path: Path,
) -> None:
    """6-panel contact sheet: all 4 Arm A configs + Arm B + KMeans."""
    nrows, ncols = density.shape

    n = len(layouts)
    ncols_grid = 3
    nrows_grid = math.ceil(n / ncols_grid)

    fig, axes = plt.subplots(nrows_grid, ncols_grid, figsize=(18, 5 * nrows_grid))
    axes_flat = axes.ravel() if hasattr(axes, "ravel") else [axes]

    for i, (label, dists, arts, colour) in enumerate(layouts):
        ax = axes_flat[i]
        _render_density_backdrop(ax, density, height_carved, x0, y0, cell, nrows, ncols)
        _render_boundary(ax, boundary)
        _render_districts(
            ax, dists, edge_color=colour, fill_color=colour, fill_alpha=0.07
        )
        if arts is not None:
            _render_arterials(ax, arts, color=colour)
        _set_extent(ax, boundary, pad=2.0)
        art_str = f", {len(arts)} arts" if arts else ""
        ax.set_title(f"{label}\n({len(dists)} districts{art_str})", fontsize=9)

    # Hide any unused axes
    for j in range(len(layouts), len(axes_flat)):
        axes_flat[j].set_visible(False)

    fig.suptitle("R1 Regional Probe – All Layouts Contact Sheet", fontsize=12)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Markdown table generation
# ---------------------------------------------------------------------------


def _fmt(val: Any, decimals: int = 3) -> str:
    if val is None:
        return "—"
    if isinstance(val, float):
        return f"{val:.{decimals}f}"
    return str(val)


def _make_markdown_table(
    results: list[dict[str, Any]], slope_p50: float, river_thr: float
) -> str:
    lines: list[str] = []
    lines.append("# R1 Cross-Arm Comparison Metrics\n")
    lines.append(
        f"*Note: slope_p50 threshold = {slope_p50:.6f} (island-frame); "
        f"river flow_accum threshold = {river_thr:.1f}. "
        "World displacement is N/A in R1 (no worlds are moved).*\n"
    )

    # Main metrics table
    header = [
        "Layout",
        "Districts",
        "Area median",
        "Area CV",
        "Area min",
        "Area max",
        "Dens-Area ρ",
        "ρ p-value",
        "WC-CV",
        "Convexity med",
        "Convexity min",
        "Compact med",
    ]
    lines.append("## District Shape and Balance Metrics\n")
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join(["---"] * len(header)) + "|")

    for r in results:
        da = r["district_area"]
        dc = r["density_area_corr"]
        sh = r["shape"]
        row = [
            r["layout"],
            str(r["district_count"]),
            _fmt(da["median"], 1),
            _fmt(da["cv"], 3),
            _fmt(da["min"], 1),
            _fmt(da["max"], 1),
            _fmt(dc["spearman_r"], 3) if dc["spearman_r"] is not None else "—",
            _fmt(dc["spearman_p"], 4) if dc["spearman_p"] is not None else "—",
            _fmt(r["world_count_balance_cv"], 3),
            _fmt(sh["convexity_median"], 3),
            _fmt(sh["convexity_min"], 3),
            _fmt(sh["compactness_median"], 3),
        ]
        lines.append("| " + " | ".join(row) + " |")

    lines.append("")

    # Arterial metrics table
    lines.append("## Arterial Metrics\n")
    art_header = [
        "Layout",
        "Total length",
        "Terrain align frac",
        "River crossings",
    ]
    lines.append("| " + " | ".join(art_header) + " |")
    lines.append("|" + "|".join(["---"] * len(art_header)) + "|")

    for r in results:
        am = r["arterials"]
        if isinstance(am, str):
            row = [r["layout"], "N/A", "N/A", "N/A"]
        else:
            row = [
                r["layout"],
                _fmt(am["total_length"], 1),
                _fmt(am.get("terrain_alignment_frac"), 3),
                str(am.get("river_crossings", "—")),
            ]
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# High-level entry point
# ---------------------------------------------------------------------------


def run_compare(
    in_dir: Path,
    arm_a_dir: Path,
    arm_b_dir: Path,
    out_dir: Path,
) -> dict[str, Any]:
    """Run the full R1 cross-arm comparison pipeline.

    Parameters
    ----------
    in_dir : Path to r1/inputs (fields.npz, island_boundary.geojson,
             island_points.parquet).
    arm_a_dir : Path to r1/arm_a (contains subdirs baseline/, regional/, etc.).
    arm_b_dir : Path to r1/arm_b.
    out_dir : Output directory; created if missing.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Load shared inputs
    # ------------------------------------------------------------------
    print("Loading fields.npz …")
    fields = np.load(in_dir / "fields.npz")
    density: np.ndarray = fields["density"].astype(np.float32)
    height_carved: np.ndarray = fields["height_carved"].astype(np.float32)
    flow_accum: np.ndarray = fields["flow_accum"].astype(np.float32)
    slope: np.ndarray = fields["slope"].astype(np.float32)
    x0 = float(fields["x0"])
    y0 = float(fields["y0"])
    cell = float(fields["cell"])
    nrows, ncols = density.shape

    print("Loading island_boundary.geojson …")
    with (in_dir / "island_boundary.geojson").open() as f:
        bnd_gj = json.load(f)
    boundary: sg.Polygon = sg.shape(bnd_gj["features"][0]["geometry"])

    print("Loading island_points.parquet …")
    points = pl.read_parquet(in_dir / "island_points.parquet")

    # ------------------------------------------------------------------
    # Island mask + slope/flow thresholds
    # ------------------------------------------------------------------
    print("Building island mask …")
    mask = _raster_mask(boundary, x0, y0, cell, nrows, ncols)

    slope_inside = slope[mask].astype(float)
    slope_p50_threshold = float(np.percentile(slope_inside, 50))
    fa_inside = flow_accum[mask].astype(float)
    river_threshold = float(np.percentile(fa_inside, 99))

    print(f"  slope p50 threshold = {slope_p50_threshold:.6f}")
    print(f"  river flow_accum p99 threshold = {river_threshold:.1f}")

    # ------------------------------------------------------------------
    # KMeans districts
    # ------------------------------------------------------------------
    print("Building KMeans l0 districts …")
    kmeans_districts, km_id_to_name, km_id_to_count = _build_kmeans_districts(
        points, boundary, mask, x0, y0, cell, nrows, ncols
    )
    km_world_counts = list(km_id_to_count.values())
    l0_clusters_with_50plus = sum(1 for c in km_id_to_count.values() if c >= 50)
    total_l0_on_island = len(km_id_to_count)
    print(
        f"  {total_l0_on_island} l0 clusters on island, "
        f"{l0_clusters_with_50plus} have ≥50 worlds"
    )

    # Check stored vs recomputed world counts for Arm A/B (just log; no assert)
    anomalies: list[str] = []

    # ------------------------------------------------------------------
    # Arm A configs
    # ------------------------------------------------------------------
    arm_a_configs = ["baseline", "regional", "regional_fine", "regional_strong"]
    arm_a_layouts: dict[str, tuple[Districts, Arterials, list[int]]] = {}

    for cfg in arm_a_configs:
        cfg_dir = arm_a_dir / cfg
        dists = _load_districts_geojson(cfg_dir / "districts.geojson")
        arts = _load_arterials_geojson(cfg_dir / "arterials.geojson")
        stored_wc = _load_world_counts_from_geojson(cfg_dir / "districts.geojson")

        # Recompute world counts by point-in-polygon
        xs_all = points["x"].to_numpy()
        ys_all = points["y"].to_numpy()
        recomp_wc = [int(shapely.contains_xy(d, xs_all, ys_all).sum()) for d in dists]
        stored_total = sum(stored_wc)
        recomp_total = sum(recomp_wc)
        if abs(stored_total - recomp_total) > 10:
            msg = (
                f"arm_a/{cfg}: stored world_count total={stored_total} "
                f"vs recomputed={recomp_total} (diff={recomp_total - stored_total})"
            )
            anomalies.append(msg)
            print(f"  ANOMALY: {msg}")
        else:
            print(
                f"  arm_a/{cfg}: {len(dists)} districts, "
                f"world_count stored={stored_total} recomp={recomp_total}"
            )
        arm_a_layouts[cfg] = (dists, arts, recomp_wc)

    # ------------------------------------------------------------------
    # Arm B
    # ------------------------------------------------------------------
    arm_b_districts = _load_districts_geojson(arm_b_dir / "districts.geojson")
    arm_b_arterials = _load_arterials_geojson(arm_b_dir / "arterials.geojson")
    arm_b_stored_wc = _load_world_counts_from_geojson(arm_b_dir / "districts.geojson")

    xs_all = points["x"].to_numpy()
    ys_all = points["y"].to_numpy()
    arm_b_recomp_wc = [
        int(shapely.contains_xy(d, xs_all, ys_all).sum()) for d in arm_b_districts
    ]
    stored_total = sum(arm_b_stored_wc)
    recomp_total = sum(arm_b_recomp_wc)
    if abs(stored_total - recomp_total) > 10:
        msg = (
            f"arm_b: stored world_count total={stored_total} "
            f"vs recomputed={recomp_total} (diff={recomp_total - stored_total})"
        )
        anomalies.append(msg)
        print(f"  ANOMALY: {msg}")
    else:
        print(
            f"  arm_b: {len(arm_b_districts)} districts, "
            f"world_count stored={stored_total} recomp={recomp_total}"
        )

    # ------------------------------------------------------------------
    # Compute all metrics
    # ------------------------------------------------------------------
    print("Computing metrics …")
    all_metrics: list[dict[str, Any]] = []

    for cfg in arm_a_configs:
        dists, arts, wc = arm_a_layouts[cfg]
        m = compute_layout_metrics(
            label=f"arm_a_{cfg}",
            districts=dists,
            arterials=arts,
            world_counts=wc,
            density=density,
            slope=slope,
            flow_accum=flow_accum,
            mask=mask,
            x0=x0,
            y0=y0,
            cell=cell,
            slope_p50_threshold=slope_p50_threshold,
            river_threshold=river_threshold,
        )
        all_metrics.append(m)

    m_b = compute_layout_metrics(
        label="arm_b",
        districts=arm_b_districts,
        arterials=arm_b_arterials,
        world_counts=arm_b_recomp_wc,
        density=density,
        slope=slope,
        flow_accum=flow_accum,
        mask=mask,
        x0=x0,
        y0=y0,
        cell=cell,
        slope_p50_threshold=slope_p50_threshold,
        river_threshold=river_threshold,
    )
    all_metrics.append(m_b)

    m_km = compute_layout_metrics(
        label="kmeans_l0",
        districts=kmeans_districts,
        arterials=None,
        world_counts=km_world_counts,
        density=density,
        slope=slope,
        flow_accum=flow_accum,
        mask=mask,
        x0=x0,
        y0=y0,
        cell=cell,
        slope_p50_threshold=slope_p50_threshold,
        river_threshold=river_threshold,
    )
    all_metrics.append(m_km)

    # ------------------------------------------------------------------
    # Build comparison output structure
    # ------------------------------------------------------------------
    comparison: dict[str, Any] = {
        "island_summary": {
            "total_worlds": int(points.shape[0]),
            "l0_clusters_on_island": total_l0_on_island,
            "l0_clusters_ge50_worlds": l0_clusters_with_50plus,
            "island_area_units2": round(boundary.area, 2),
        },
        "thresholds": {
            "slope_p50_inside_mask": round(float(slope_p50_threshold), 6),
            "river_flow_accum_p99_inside_mask": round(float(river_threshold), 2),
        },
        "layouts": all_metrics,
        "anomalies": anomalies,
        "note_world_displacement": "N/A in R1 — no worlds are repositioned",
    }

    # ------------------------------------------------------------------
    # Write comparison.json
    # ------------------------------------------------------------------
    with (out_dir / "comparison.json").open("w") as f:
        json.dump(comparison, f, indent=2)
    print("Wrote comparison.json")

    # ------------------------------------------------------------------
    # Write comparison.md
    # ------------------------------------------------------------------
    md = _make_markdown_table(all_metrics, slope_p50_threshold, river_threshold)
    # Prepend island summary
    prefix = (
        f"**Island**: {total_l0_on_island} l0 clusters, "
        f"{l0_clusters_with_50plus} with ≥50 worlds, "
        f"{int(points.shape[0])} total worlds, area ≈ {boundary.area:.0f} units²\n\n"
    )
    if anomalies:
        prefix += "**Anomalies**:\n" + "\n".join(f"- {a}" for a in anomalies) + "\n\n"

    with (out_dir / "comparison.md").open("w") as f:
        f.write(prefix + md)
    print("Wrote comparison.md")

    # ------------------------------------------------------------------
    # Renders
    # ------------------------------------------------------------------
    print("Rendering compare_main.png …")
    arm_a_rs_dists, arm_a_rs_arts, _ = arm_a_layouts["regional_strong"]
    render_main_png(
        density=density,
        height_carved=height_carved,
        boundary=boundary,
        arm_a_regional_strong_districts=arm_a_rs_dists,
        arm_a_regional_strong_arterials=arm_a_rs_arts,
        arm_b_districts=arm_b_districts,
        arm_b_arterials=arm_b_arterials,
        kmeans_districts=kmeans_districts,
        x0=x0,
        y0=y0,
        cell=cell,
        out_path=out_dir / "compare_main.png",
    )
    print("Wrote compare_main.png")

    print("Rendering compare_contact.png …")

    def _a(cfg: str) -> tuple[Districts, Arterials]:
        return arm_a_layouts[cfg][0], arm_a_layouts[cfg][1]

    contact_layouts: list[tuple[str, Districts, Arterials | None, str]] = [
        ("Arm A / baseline", *_a("baseline"), "#2166ac"),
        ("Arm A / regional", *_a("regional"), "#1a9850"),
        ("Arm A / regional_fine", *_a("regional_fine"), "#d73027"),
        ("Arm A / regional_strong", *_a("regional_strong"), "#7570b3"),
        ("Arm B (least-cost)", arm_b_districts, arm_b_arterials, "#8b0000"),
        ("KMeans l0", kmeans_districts, None, "#4d4d4d"),
    ]
    render_contact_png(
        density=density,
        height_carved=height_carved,
        boundary=boundary,
        layouts=contact_layouts,
        x0=x0,
        y0=y0,
        cell=cell,
        out_path=out_dir / "compare_contact.png",
    )
    print("Wrote compare_contact.png")

    return comparison
