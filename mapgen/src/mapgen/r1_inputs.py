"""R1 probe – island selection and input layer generation.

Reusable logic called by scripts/run_r1_island_inputs.py.  Does not touch
any chen_* files.

Island frame convention (interface contract for downstream R1 slices):
    island_xy = (world_xy - offset) * scale
where offset = (xmin, ymin) of the island polygon bbox (source coords) and
scale is chosen so the longer bbox side = 200.0.

All raster outputs use the same shape and the affine: x = x0 + col*cell.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import shapely
import shapely.affinity
import shapely.geometry as sg
from scipy.ndimage import gaussian_filter

# ---------------------------------------------------------------------------
# Polygon stats helpers
# ---------------------------------------------------------------------------

LAND_LONG_SIDE = 200.0  # island frame bbox long side, in island units


def load_land_polygons(land_geojson: Path) -> list[sg.Polygon]:
    """Return the list of individual Polygon geometries from land.geojson."""
    with land_geojson.open() as f:
        gj = json.load(f)
    geoms: list[sg.Polygon] = []
    for feat in gj["features"]:
        g = sg.shape(feat["geometry"])
        if g.geom_type == "MultiPolygon":
            geoms.extend(list(g.geoms))
        elif g.geom_type == "Polygon":
            geoms.append(g)
    return geoms


def _density_contrast(
    xs: np.ndarray,
    ys: np.ndarray,
    poly: sg.Polygon,
    *,
    grid_cells: int = 80,
    sigma: float = 1.5,
) -> tuple[float, float, float]:
    """Return (p50, p90, contrast=p90/p50) of smoothed density over non-zero cells."""
    xmin, ymin, xmax, ymax = poly.bounds
    cell = max(xmax - xmin, ymax - ymin) / grid_cells
    nx = max(int((xmax - xmin) / cell) + 1, 2)
    ny = max(int((ymax - ymin) / cell) + 1, 2)
    H, _, _ = np.histogram2d(
        xs,
        ys,
        bins=[nx, ny],
        range=[[xmin, xmax], [ymin, ymax]],
    )
    H_s = gaussian_filter(H, sigma=sigma)
    nz = H_s[H_s > 0]
    if len(nz) < 2:
        return 0.0, 0.0, 0.0
    p50 = float(np.percentile(nz, 50))
    p90 = float(np.percentile(nz, 90))
    contrast = p90 / max(p50, 1e-9)
    return p50, p90, contrast


def compute_polygon_stats(
    polys: list[sg.Polygon],
    df: pl.DataFrame,
) -> list[dict[str, Any]]:
    """Compute per-polygon selection stats.

    Returns a list of dicts with keys:
        index, area, point_count, density, nz_p50, nz_p90, contrast
    """
    xs = df["x"].to_numpy()
    ys = df["y"].to_numpy()

    rows: list[dict[str, Any]] = []
    for i, poly in enumerate(polys):
        in_mask = shapely.contains_xy(poly, xs, ys)
        n = int(in_mask.sum())
        area = float(poly.area)
        density = n / area if area > 0 else 0.0
        if n >= 5:
            px, py = xs[in_mask], ys[in_mask]
            p50, p90, contrast = _density_contrast(px, py, poly)
        else:
            p50, p90, contrast = 0.0, 0.0, 0.0
        rows.append(
            {
                "index": i,
                "area": round(area, 4),
                "point_count": n,
                "density_per_unit2": round(density, 2),
                "nz_p50": round(p50, 3),
                "nz_p90": round(p90, 3),
                "contrast_p90_p50": round(contrast, 3),
            }
        )
    return rows


def select_island(
    stats: list[dict[str, Any]],
    *,
    min_points: int = 3000,
    max_points: int = 15000,
    override_index: int | None = None,
) -> tuple[int, str]:
    """Choose island index and return (index, justification_string).

    Selection: among polygons with min_points <= n <= max_points, pick the one
    with the highest core/fringe contrast (nz_p90/nz_p50 density ratio).
    """
    if override_index is not None:
        justification = f"user override --island-index {override_index}"
        return override_index, justification

    candidates = [s for s in stats if min_points <= s["point_count"] <= max_points]
    if not candidates:
        raise ValueError(
            f"No polygon has {min_points}–{max_points} points. "
            "Use --island-index to override."
        )
    best = max(candidates, key=lambda s: s["contrast_p90_p50"])
    idx = best["index"]
    justification = (
        f"Polygon {idx} has {best['point_count']} worlds (in target range "
        f"{min_points}–{max_points}), area={best['area']:.2f}, "
        f"contrast(p90/p50)={best['contrast_p90_p50']:.2f} — highest "
        "core/fringe contrast among mid-size candidates."
    )
    return idx, justification


# ---------------------------------------------------------------------------
# Island frame transform
# ---------------------------------------------------------------------------


def island_transform(poly: sg.Polygon) -> tuple[float, float, float]:
    """Compute (offset_x, offset_y, scale) for island frame.

    offset = bbox min; scale so longer bbox side = LAND_LONG_SIDE.
    """
    xmin, ymin, xmax, ymax = poly.bounds
    w = xmax - xmin
    h = ymax - ymin
    scale = LAND_LONG_SIDE / max(w, h)
    return float(xmin), float(ymin), float(scale)


def to_island_frame(
    xs: np.ndarray,
    ys: np.ndarray,
    offset_x: float,
    offset_y: float,
    scale: float,
) -> tuple[np.ndarray, np.ndarray]:
    return (xs - offset_x) * scale, (ys - offset_y) * scale


def polygon_to_island_frame(
    poly: sg.Polygon,
    offset_x: float,
    offset_y: float,
    scale: float,
) -> sg.Polygon:
    """Map a Shapely Polygon into island frame coords."""
    return shapely.affinity.affine_transform(
        poly, [scale, 0, 0, scale, -offset_x * scale, -offset_y * scale]
    )


# ---------------------------------------------------------------------------
# Boundary preparation
# ---------------------------------------------------------------------------


def prepare_boundary(
    poly: sg.Polygon,
    offset_x: float,
    offset_y: float,
    scale: float,
    *,
    target_vertices: int = 100,
) -> tuple[sg.Polygon, int, float]:
    """Clean, transform, and simplify to a budget vertex count.

    Returns (simplified_polygon_in_island_frame, vertex_count, tolerance_used).
    """
    # Take largest exterior ring only (drop holes)
    if poly.geom_type == "MultiPolygon":
        poly = max(poly.geoms, key=lambda p: p.area)
    cleaned = sg.Polygon(poly.exterior)  # exterior ring only, holes dropped
    cleaned = cleaned.buffer(0)  # validity fix

    # Transform to island frame
    cleaned_f = polygon_to_island_frame(cleaned, offset_x, offset_y, scale)

    # Simplify until vertex count <= target_vertices
    n_orig = len(cleaned_f.exterior.coords)
    tol = 0.0
    result = cleaned_f
    if n_orig > target_vertices:
        # Binary search for tolerance
        lo, hi = 0.0, 20.0
        for _ in range(40):
            mid = (lo + hi) / 2
            s = cleaned_f.simplify(mid, preserve_topology=True)
            if len(s.exterior.coords) <= target_vertices:
                hi = mid
                result = s
            else:
                lo = mid
        tol = hi
        result = cleaned_f.simplify(tol, preserve_topology=True)
    n_final = len(result.exterior.coords)
    return result, n_final, tol


# ---------------------------------------------------------------------------
# Raster grid
# ---------------------------------------------------------------------------


def make_raster_grid(
    poly_frame: sg.Polygon,
    *,
    long_side_cells: int = 384,
) -> tuple[float, float, float, int, int]:
    """Compute (x0, y0, cell, ncols, nrows) for the raster grid.

    x = x0 + col*cell, y = y0 + row*cell (row 0 = bottom, like a geographic raster).
    Grid covers the polygon bbox with one half-cell margin on each side.
    """
    xmin, ymin, xmax, ymax = poly_frame.bounds
    w = xmax - xmin
    h = ymax - ymin
    cell = max(w, h) / long_side_cells
    # add half-cell margin
    ncols = int(np.ceil(w / cell)) + 2
    nrows = int(np.ceil(h / cell)) + 2
    x0 = xmin - (ncols * cell - w) / 2
    y0 = ymin - (nrows * cell - h) / 2
    return float(x0), float(y0), float(cell), ncols, nrows


def raster_coords(
    x0: float, y0: float, cell: float, ncols: int, nrows: int
) -> tuple[np.ndarray, np.ndarray]:
    """Return (X, Y) meshgrid of cell-center coords, shape (nrows, ncols)."""
    cx = x0 + (np.arange(ncols) + 0.5) * cell
    cy = y0 + (np.arange(nrows) + 0.5) * cell
    return np.meshgrid(cx, cy)


def island_mask(
    poly_frame: sg.Polygon,
    X: np.ndarray,
    Y: np.ndarray,
) -> np.ndarray:
    """Boolean mask of cells whose center falls inside the polygon.

    Shape: (nrows, ncols).
    """
    pts_x = X.ravel()
    pts_y = Y.ravel()
    inside = shapely.contains_xy(poly_frame, pts_x, pts_y)
    return inside.reshape(X.shape)


# ---------------------------------------------------------------------------
# Density raster
# ---------------------------------------------------------------------------


def compute_density(
    px: np.ndarray,
    py: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    ncols: int,
    nrows: int,
    mask: np.ndarray,
    *,
    bandwidth: float | None = None,
) -> tuple[np.ndarray, float]:
    """Return (density_raster, bandwidth_used).

    Uses histogram binning + Gaussian smoothing.  bandwidth is in island-frame
    units; default = 3*cell (smooth but core/fringe structure survives).
    """
    if bandwidth is None:
        bandwidth = 3.0 * cell
    sigma_cells = bandwidth / cell

    H, _, _ = np.histogram2d(
        px,
        py,
        bins=[ncols, nrows],
        range=[[x0, x0 + ncols * cell], [y0, y0 + nrows * cell]],
    )
    # H is (ncols, nrows); transpose to (nrows, ncols) for row=y, col=x
    H = H.T.astype(np.float32)
    H_s = gaussian_filter(H, sigma=sigma_cells)
    H_s[~mask] = 0.0
    return H_s, float(bandwidth)


# ---------------------------------------------------------------------------
# D8 flow accumulation
# ---------------------------------------------------------------------------


def _d8_flow_accum(dem: np.ndarray) -> np.ndarray:
    """Simple D8 flow accumulation over a DEM (nrows x ncols).

    Each cell drains to its steepest-descent neighbour (8-connected).  Flats
    and pits are resolved by adding a small epsilon ramp proportional to
    distance from the DEM boundary (cells at the boundary drain outward).
    Returns an integer accumulation array of the same shape.
    """
    nrows, ncols = dem.shape

    # Epsilon ramp: add a tiny slope toward the boundary so every interior
    # flat/pit eventually drains out.  distance_transform_edt gives the
    # nearest-zero distance; boundary = 0, interior increases inward.
    from scipy.ndimage import distance_transform_edt

    interior_mask = np.ones_like(dem, dtype=bool)
    interior_mask[0, :] = False
    interior_mask[-1, :] = False
    interior_mask[:, 0] = False
    interior_mask[:, -1] = False
    dist = distance_transform_edt(interior_mask).astype(np.float32)
    max_dist = dist.max()
    if max_dist > 0:
        dist /= max_dist
    eps = 1e-4
    dem_eps = dem.astype(np.float64) + eps * dist  # nudge interior up slightly

    # D8 neighbour offsets (dr, dc) and diagonal distance weights
    neighbors = [
        (-1, -1),
        (-1, 0),
        (-1, 1),
        (0, -1),
        (0, 1),
        (1, -1),
        (1, 0),
        (1, 1),
    ]

    # Build receiver array: receiver[r,c] = (r2, c2) of steepest descent
    # We store as flat index for speed.
    flat_size = nrows * ncols
    flat_dem = dem_eps.ravel()
    receiver = np.arange(flat_size, dtype=np.int64)  # default: self (pit)

    for dr, dc in neighbors:
        # Shift dem: shifted[r,c] = dem[r+dr, c+dc]
        sr1, sr2 = max(dr, 0), min(nrows, nrows + dr)
        sc1, sc2 = max(dc, 0), min(ncols, ncols + dc)
        tr1, tr2 = max(-dr, 0), min(nrows, nrows - dr)
        tc1, tc2 = max(-dc, 0), min(ncols, ncols - dc)
        src_flat = (
            (np.arange(tr1, tr2)[:, None] * ncols + np.arange(tc1, tc2)[None, :])
            .ravel()
            .astype(np.int64)
        )
        nbr_flat = (
            (np.arange(sr1, sr2)[:, None] * ncols + np.arange(sc1, sc2)[None, :])
            .ravel()
            .astype(np.int64)
        )
        # Compare drop to neighbour vs drop to current receiver (absolute diff,
        # not normalized by distance — simple and consistent across all offsets).
        cur_diff = flat_dem[src_flat] - flat_dem[receiver[src_flat]]
        nbr_diff = flat_dem[src_flat] - flat_dem[nbr_flat]
        update_mask = nbr_diff > cur_diff
        receiver[src_flat[update_mask]] = nbr_flat[update_mask]

    # Count in-degrees (= flow accumulation after one pass via topological sort)
    # Use iterative accumulation: sort cells from highest to lowest elevation,
    # pass each cell's count to its receiver.
    accum = np.ones(flat_size, dtype=np.int64)
    order = np.argsort(-flat_dem)  # highest first
    for idx in order:
        rec = receiver[idx]
        if rec != idx:
            accum[rec] += accum[idx]
    return accum.reshape(nrows, ncols)


# ---------------------------------------------------------------------------
# Terrain derivation
# ---------------------------------------------------------------------------


def compute_fields(
    density: np.ndarray,
    mask: np.ndarray,
    *,
    carve_k: float = 0.12,
    height_smooth_sigma_cells: float = 6.0,
) -> dict[str, np.ndarray]:
    """Derive height, flow_accum, height_carved, slope from density raster.

    Parameters
    ----------
    density : (nrows, ncols) float32, masked to 0 outside island
    mask    : (nrows, ncols) bool, True inside island
    carve_k : carving strength; height_carved = height - k*norm(log1p(accum))
    height_smooth_sigma_cells : extra Gaussian smoothing applied to the height
        channel only. The density raster keeps its sharp core/fringe structure,
        but a heightfield at that bandwidth is bumpy at the KDE scale and D8
        drainage fragments into short disconnected streams; a smoother height
        organizes the flow into coherent valleys reaching the sea.

    Returns dict with keys: height, flow_accum, height_carved, slope.
    """
    # height = normalized extra-smoothed density, 0 outside
    h = density.astype(np.float64)
    if height_smooth_sigma_cells > 0:
        h = gaussian_filter(h, sigma=height_smooth_sigma_cells)
    dmax = h[mask].max() if mask.any() else 1.0
    if dmax > 0:
        h /= dmax
    h[~mask] = 0.0
    height = h.astype(np.float32)

    # flow accumulation (D8 over height; rivers flow from high density to low/sea)
    accum = _d8_flow_accum(height)
    accum = accum.astype(np.float32)
    # Zero outside island: exterior cells are permanent sinks (sea); their
    # accumulated counts are not meaningful and would flood the display.
    accum[~mask] = 0.0

    # carve height
    log_accum = np.log1p(accum)
    la_max = log_accum[mask].max() if mask.any() else 1.0
    log_accum_norm = log_accum / la_max if la_max > 0 else log_accum
    h_carved = height - carve_k * log_accum_norm
    h_carved = np.clip(h_carved, 0.0, None)
    h_carved[~mask] = 0.0
    h_carved = h_carved.astype(np.float32)

    # slope = gradient magnitude of carved height (per-cell island-frame units)
    gy, gx = np.gradient(h_carved.astype(np.float64))
    slope = np.sqrt(gx**2 + gy**2).astype(np.float32)
    slope[~mask] = 0.0

    return {
        "height": height,
        "flow_accum": accum,
        "height_carved": h_carved,
        "slope": slope,
    }


# ---------------------------------------------------------------------------
# Quicklook rendering helpers
# ---------------------------------------------------------------------------


def render_density_png(
    density: np.ndarray,
    out_path: Path,
    *,
    cmap: str = "inferno",
) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.imshow(density, origin="lower", cmap=cmap, aspect="equal")
    ax.set_title("Density (KDE)")
    ax.axis("off")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def render_terrain_png(
    height_carved: np.ndarray,
    flow_accum: np.ndarray,
    out_path: Path,
    *,
    island_mask_arr: np.ndarray | None = None,
) -> None:
    """Hillshade of height_carved with top-decile flow_accum drainage overlay.

    Parameters
    ----------
    island_mask_arr : optional bool array matching height_carved shape; when
        provided, drainage overlay is restricted to inside-island cells.
    """
    import matplotlib.pyplot as plt
    from matplotlib.colors import LightSource

    ls = LightSource(azdeg=315, altdeg=45)
    hs = ls.hillshade(height_carved.astype(np.float64), vert_exag=3.0)

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.imshow(hs, origin="lower", cmap="gray", aspect="equal")

    # Overlay top-1% flow accumulation as blue drainage lines (inside island).
    # Using p99 (not p90) because all island cells have accum > 0 in D8; the
    # p90 threshold would show ~10% of cells, not meaningful channel lines.
    fa = flow_accum.copy()
    if island_mask_arr is not None:
        fa[~island_mask_arr] = 0.0
    nonzero_fa = fa[fa > 0]
    if len(nonzero_fa) > 0:
        threshold = float(np.percentile(nonzero_fa, 99))
        drainage = fa >= threshold
        overlay = np.zeros((*fa.shape, 4), dtype=np.float32)
        overlay[drainage] = [0.2, 0.4, 1.0, 0.7]  # blue, semi-transparent
        ax.imshow(overlay, origin="lower", aspect="equal")

    ax.set_title("Terrain (hillshade + drainage)")
    ax.axis("off")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def render_points_png(
    px: np.ndarray,
    py: np.ndarray,
    boundary: sg.Polygon,
    out_path: Path,
) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 8))

    # Boundary outline
    bx, by = boundary.exterior.xy
    ax.plot(bx, by, "k-", lw=1.5, zorder=2)

    # Scatter
    ax.scatter(px, py, s=0.5, alpha=0.3, c="steelblue", zorder=1, rasterized=True)

    ax.set_aspect("equal")
    ax.set_title(f"Island worlds (n={len(px):,})")
    ax.axis("off")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
