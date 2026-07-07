#!/usr/bin/env python
"""R1 stage 3 — full hybrid assembly: Chen/R2 in every macro-block.

Assembles the whole-island macro+micro hybrid from
``docs/large-scale-growth-research.md`` (staged build plan, step 3): the stage-1
macro arterial hierarchy (highways/majors + macro-blocks) with per-block
density-calibrated Chen/R2 *local* fabric run inside EVERY macro-block.

Connectivity (arterial<->local T-junctions) is now WIRED (stage-3 connectivity
slices in ``mapgen.r1_connect``): per-block Chen boundary "gates" are snapped
onto the macro arterial/ring network (``snap_gates_to_macro``), summarized
(``connectivity_metrics``), and fused with the local street fabric into one
``networkx`` graph (``build_unified_street_graph`` /
``graph_connectivity_summary``). Perimeter-flagged local streets (duplicates of
the arterial/ring/coast geometry they run along) are skipped when rendering;
realized T-junctions are drawn as markers.

Calibration (LOCKED design decision; see the doc): a SINGLE GLOBAL district mass
``M = total_mass / total_target`` is applied as ``max_parcel_mass`` to every
block (dense blocks therefore split into more districts; district size stays
consistent island-wide), with a global absolute geometric floor
``min_parcel_area = island_area / (total_target * 4)`` so the mass gate, not
geometry, governs termination.

Outputs to mapgen/artifacts/r1/hybrid/ (or --out-dir):
  hybrid_overview.png    — full island: density backdrop + boundary + all Chen
                           district edges (faint) + Chen streets (perimeter
                           duplicates skipped) + macro arterials (tier-colored,
                           on top) + T-junction markers + world points.
  hybrid_core{1..3}.png  — zooms into the top-3 density peaks, same layers but
                           fabric/arterials thicker so local-meets-arterial reads.
  hybrid_seam.png        — zoom into the non-core macro-block with the median
                           T-junction count: the money shot for judging whether
                           local fabric actually plugs into the arterial grid.
  hybrid_junctions.geojson — every snapped SeamJunction as a Point feature.
  hybrid_manifest.json   — totals, calibration, per-block district counts,
                           failure count, runtime, invariant pass rate,
                           connectivity metrics + unified-graph summary.

Run from mapgen/::
    uv run python scripts/run_r1_hybrid.py
    uv run python scripts/run_r1_hybrid.py --total-target 400
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.patches as mpatches  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import polars as pl  # noqa: E402
import shapely  # noqa: E402
import shapely.geometry as sg  # noqa: E402
from shapely.ops import unary_union  # noqa: E402

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "src") not in sys.path:
    sys.path.insert(0, str(_REPO / "src"))

from mapgen.r1_arm_a import IslandFields, build_density_field  # noqa: E402
from mapgen.r1_arm_b import find_density_peaks  # noqa: E402
from mapgen.r1_compare import (  # noqa: E402
    _render_boundary,
    _render_density_backdrop,
    _set_extent,
)
from mapgen.r1_connect import (  # noqa: E402
    Gate,
    SeamJunction,
    build_unified_street_graph,
    connectivity_metrics,
    densify_gates,
    graph_connectivity_summary,
    snap_gates_to_macro,
)
from mapgen.r1_lots import (  # noqa: E402
    DEFAULT_INSET,
    DEFAULT_MIN_LOT_AREA_FRAC,
    Lot,
    MassingConfig,
    assign_worlds_to_districts,
    build_lots,
    classify_typology,
    count_sliver_reassignments,
    displacement_stats,
    select_member_points,
    top_landmark_ids,
)
from mapgen.r1_macro import (  # noqa: E402
    MacroEdge,
    MacroLayer,
    MacroParams,
    assign_nearest_nucleus,
    build_macro_layer,
    load_boundary,
    nucleus_specs_to_geojson,
)
from mapgen.r1_mesh import DEFAULT_METERS_PER_UNIT  # noqa: E402
from mapgen.r1_seam import (  # noqa: E402
    DEFAULT_RETRY_SEEDS,
    chen_in_block,
)
from mapgen.r1_zoom import load_points_with_labels, zoom_window  # noqa: E402

_DEFAULT_IN = _REPO / "artifacts/r1/inputs"
_DEFAULT_OUT = _REPO / "artifacts/r1/hybrid"

# Tier styling, matching run_r1_macro (highway red, major blue, local teal).
_TIER_STYLE: dict[int, dict[str, Any]] = {
    2: {"color": "#b30000", "label": "Highway (city–city)"},
    1: {"color": "#1f5fb0", "label": "Major (≥ town)"},
    0: {"color": "#138d75", "label": "Local (incl. village)"},
}


class BlockResult:
    """Per-block Chen outcome tagged with the macro-block id.

    A thin record (not a dataclass to keep it local): the districts and streets
    produced inside macro-block ``block_id``, the seed used, whether Chen
    succeeded (vs the single-district fallback), the cheap invariant flags, and
    (stage-3 connectivity) the boundary "gates" and index-aligned
    street-perimeter flags used to stitch this block's local streets onto the
    macro network (``mapgen.r1_connect``). ``n_connectors`` (stage-4,
    optional) is how many of ``gates``/``streets`` are
    :func:`mapgen.r1_connect.densify_gates` connectors rather than organic
    Chen output (``0`` when densification is off).
    """

    __slots__ = (
        "block_id",
        "districts",
        "streets",
        "failed",
        "seed_used",
        "district_count",
        "geometry_valid_pass",
        "paper_invariant_pass",
        "seconds",
        "gates",
        "street_perimeter_flags",
        "n_connectors",
    )

    def __init__(
        self,
        *,
        block_id: int,
        districts: list[sg.Polygon],
        streets: list[sg.LineString],
        failed: bool,
        seed_used: int | str,
        district_count: int,
        geometry_valid_pass: bool | None,
        paper_invariant_pass: bool | None,
        seconds: float,
        gates: list[Gate],
        street_perimeter_flags: list[bool],
        n_connectors: int = 0,
    ) -> None:
        self.block_id = block_id
        self.districts = districts
        self.streets = streets
        self.failed = failed
        self.seed_used = seed_used
        self.district_count = district_count
        self.geometry_valid_pass = geometry_valid_pass
        self.paper_invariant_pass = paper_invariant_pass
        self.seconds = seconds
        self.gates = gates
        self.street_perimeter_flags = street_perimeter_flags
        self.n_connectors = n_connectors


def run_all_blocks(
    macro_blocks: list[sg.Polygon],
    fields: IslandFields,
    *,
    max_parcel_mass: float,
    min_parcel_area: float,
    seeds: tuple[int, ...] = DEFAULT_RETRY_SEEDS,
    max_gate_spacing: float | None = None,
) -> list[BlockResult]:
    """Run Chen/R2 inside every macro-block with the global calibration.

    Sequential (the user is OK with long runtimes). On total Chen failure for a
    block (every seed raised), fall back to leaving that block as a SINGLE
    district (the block polygon itself) and record it as failed, rather than
    aborting the whole run. Prints per-block progress.

    ``max_gate_spacing`` (stage-4, optional; ``None`` is OFF by default) runs
    :func:`mapgen.r1_connect.densify_gates` on every successful (non-fallback)
    block right here, while ``res.generated.layout`` is still in scope:
    connector streets are appended to that block's ``streets`` (so they render
    as local streets and become unified-graph edges) and connector gates are
    appended to ``gates`` — both BEFORE the caller's ``snap_gates_to_macro``
    call, exactly like organically-extracted gates. A fallback block (no Chen
    layout) is never densified — it has no interior corner graph to route
    through.
    """
    results: list[BlockResult] = []
    n = len(macro_blocks)
    for i, block in enumerate(macro_blocks):
        t0 = time.perf_counter()
        res = chen_in_block(
            block,
            fields,
            max_parcel_mass=max_parcel_mass,
            min_parcel_area=min_parcel_area,
            seeds=seeds,
        )
        seconds = time.perf_counter() - t0
        if res.generated is None:
            # Fallback: the block itself is one district; no local streets, so
            # no gates either (a single-district block has no interior street
            # to snap onto the macro network).
            districts: list[sg.Polygon] = [block]
            streets: list[sg.LineString] = []
            failed = True
            district_count = 1
            geom_pass: bool | None = None
            inv_pass: bool | None = None
            seed_used: int | str = "all_failed"
            gates: list[Gate] = []
            perimeter_flags: list[bool] = []
            n_connectors = 0
            print(
                f"  block {i + 1}/{n}: FAILED Chen (fallback 1 district) "
                f"{seconds:.1f}s",
                flush=True,
            )
        else:
            districts = res.districts
            streets = list(res.streets)
            failed = False
            district_count = res.info["district_count"]
            geom_pass = res.info.get("geometry_valid_pass")
            inv_pass = res.info.get("paper_invariant_pass")
            seed_used = res.info["seed_used"]
            gates = list(res.gates)
            perimeter_flags = list(res.street_perimeter_flags)
            n_connectors = 0
            if max_gate_spacing is not None:
                connectors, connector_gates = densify_gates(
                    res.generated.layout,
                    block,
                    gates,
                    max_gate_spacing=max_gate_spacing,
                )
                streets.extend(connectors)
                perimeter_flags.extend([False] * len(connectors))
                gates.extend(connector_gates)
                n_connectors = len(connectors)
            suffix = (
                f", +{n_connectors} connectors" if max_gate_spacing is not None else ""
            )
            print(
                f"  block {i + 1}/{n}: {district_count} districts "
                f"(seed {seed_used}) {seconds:.1f}s{suffix}",
                flush=True,
            )
        results.append(
            BlockResult(
                block_id=i,
                districts=districts,
                streets=streets,
                failed=failed,
                seed_used=seed_used,
                district_count=district_count,
                geometry_valid_pass=geom_pass,
                paper_invariant_pass=inv_pass,
                seconds=round(seconds, 2),
                gates=gates,
                street_perimeter_flags=perimeter_flags,
                n_connectors=n_connectors,
            )
        )
    return results


def _color_lookup(l0_ids: np.ndarray) -> dict[int, tuple[float, float, float, float]]:
    unique = sorted({int(v) for v in l0_ids})
    cmap = plt.get_cmap("tab20")
    return {uid: cmap(i % 20) for i, uid in enumerate(unique)}


def _draw_layers(
    ax: Any,
    *,
    fields: IslandFields,
    boundary: sg.Polygon,
    results: list[BlockResult],
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    core_polys: list[sg.Polygon],
    lp_xs: np.ndarray,
    lp_ys: np.ndarray,
    lp_l0: np.ndarray,
    colors: dict[int, tuple[float, float, float, float]],
    junctions: list[SeamJunction],
    district_lw: float,
    district_alpha: float,
    street_lw: float,
    arterial_lw: dict[int, float],
    ring_lw: float,
    point_size: float,
    point_alpha: float,
    junction_size: float,
    plaza_polys: list[sg.Polygon] | None = None,
) -> None:
    """Draw the shared hybrid layer stack on ``ax`` (backdrop -> junctions)."""
    density = fields.density
    nrows, ncols = density.shape
    x0, y0, cell = fields.x0, fields.y0, fields.cell

    _render_density_backdrop(
        ax, density, fields.height_carved, x0, y0, cell, nrows, ncols
    )
    _render_boundary(ax, boundary)

    # World points: tiny, low alpha, colored by l0 cluster. Callers pass
    # PLAZA-MASKED coordinate arrays (see ``run_hybrid``): a world whose raw
    # coordinates fall inside a plaza disc is re-assigned to the nearest
    # non-plaza district by the greybox export (slice P), so drawing its raw
    # position inside the park would misrepresent the fabric.
    pt_colors = [colors[int(i)] for i in lp_l0]
    ax.scatter(
        lp_xs,
        lp_ys,
        s=point_size,
        c=pt_colors,
        alpha=point_alpha,
        linewidths=0.0,
        zorder=2,
    )

    # Plaza park discs (slice P): filled light green ABOVE the world dots
    # (which are masked out of plazas anyway) but BELOW district edges and
    # Chen streets, so any fabric wrongly landing inside a plaza stays
    # visible to the visual gate rather than being painted over.
    for poly in plaza_polys or []:
        if poly.is_empty:
            continue
        px, py = poly.exterior.xy
        ax.fill(px, py, color="#a9dfbf", alpha=0.75, lw=0.0, zorder=2.5)
        ax.plot(
            px,
            py,
            color="#1e8449",
            lw=ring_lw * 0.8,
            alpha=0.95,
            zorder=9,
            solid_capstyle="round",
        )

    # All Chen district edges: very thin, low alpha.
    for res in results:
        for poly in res.districts:
            if poly.is_empty:
                continue
            for ring in [poly.exterior, *list(poly.interiors)]:
                rx, ry = ring.xy
                ax.plot(
                    rx,
                    ry,
                    color="#555555",
                    lw=district_lw,
                    alpha=district_alpha,
                    zorder=3,
                )

    # Chen streets: thin black. Perimeter-flagged streets are skipped — they
    # coincide with the block-boundary ring (arterial/ring/coast geometry
    # already drawn), so drawing them too is a doubled-line artifact (see
    # mapgen.r1_connect.street_perimeter_flags).
    for res in results:
        flags = res.street_perimeter_flags or [False] * len(res.streets)
        for line, is_perimeter in zip(res.streets, flags, strict=True):
            if is_perimeter:
                continue
            if line.geom_type != "LineString":
                continue
            lx, ly = line.xy
            ax.plot(lx, ly, color="#1a1a1a", lw=street_lw, alpha=0.85, zorder=4)

    # Macro arterials on top, tier-colored (locals, then majors, then highways).
    for tier in (0, 1, 2):
        if tier not in _TIER_STYLE or tier not in arterial_lw:
            continue
        style = _TIER_STYLE[tier]
        for line, rec in zip(arterial_lines, edges, strict=True):
            if rec.tier != tier or line.geom_type != "LineString":
                continue
            lx, ly = line.xy
            ax.plot(
                lx,
                ly,
                color=style["color"],
                lw=arterial_lw[tier],
                alpha=0.95,
                zorder=5 + tier,
                solid_capstyle="round",
            )

    # Core ring roads on top: dashed dark-orange downtown rings.
    for poly in core_polys:
        if poly.is_empty:
            continue
        rx, ry = poly.exterior.xy
        ax.plot(
            rx,
            ry,
            color="#d35400",
            lw=ring_lw,
            ls=(0, (5, 2)),
            alpha=0.95,
            zorder=9,
            solid_capstyle="round",
        )

    # Realized arterial/ring T-junctions: light dot, dark edge, above
    # everything else. Coast/unmatched gates are skipped — they aren't a
    # local-fabric<->macro-network junction (see mapgen.r1_connect.SeamJunction).
    jx = [j.x for j in junctions if j.kind in ("arterial", "ring")]
    jy = [j.y for j in junctions if j.kind in ("arterial", "ring")]
    if jx:
        ax.scatter(
            jx,
            jy,
            s=junction_size,
            c="#fdfefe",
            edgecolors="#1a1a1a",
            linewidths=0.6,
            zorder=10,
            alpha=0.95,
        )


def _legend_handles() -> list[Any]:
    return [
        mpatches.Patch(color=_TIER_STYLE[2]["color"], label=_TIER_STYLE[2]["label"]),
        mpatches.Patch(color=_TIER_STYLE[1]["color"], label=_TIER_STYLE[1]["label"]),
        mpatches.Patch(color=_TIER_STYLE[0]["color"], label=_TIER_STYLE[0]["label"]),
        plt.Line2D(
            [0], [0], color="#d35400", lw=2.0, ls=(0, (5, 2)), label="Core ring road"
        ),
        mpatches.Patch(color="#a9dfbf", label="Plaza (park)"),
        plt.Line2D([0], [0], color="#1a1a1a", lw=1.2, label="Chen street (local)"),
        plt.Line2D([0], [0], color="#555555", lw=0.8, label="Chen district edge"),
        plt.Line2D(
            [0],
            [0],
            marker="o",
            linestyle="None",
            markerfacecolor="#fdfefe",
            markeredgecolor="#1a1a1a",
            markeredgewidth=0.8,
            markersize=6,
            label="arterial↔local T-junction",
        ),
    ]


def render_overview(
    *,
    fields: IslandFields,
    boundary: sg.Polygon,
    results: list[BlockResult],
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    core_polys: list[sg.Polygon],
    lp_xs: np.ndarray,
    lp_ys: np.ndarray,
    lp_l0: np.ndarray,
    colors: dict[int, tuple[float, float, float, float]],
    junctions: list[SeamJunction],
    plaza_polys: list[sg.Polygon] | None = None,
    total_districts: int,
    total_streets: int,
    out_path: Path,
    dpi: int,
) -> None:
    """Full-island hybrid overview render."""
    fig, ax = plt.subplots(figsize=(18, 13))
    _draw_layers(
        ax,
        fields=fields,
        boundary=boundary,
        results=results,
        arterial_lines=arterial_lines,
        edges=edges,
        core_polys=core_polys,
        lp_xs=lp_xs,
        lp_ys=lp_ys,
        lp_l0=lp_l0,
        colors=colors,
        junctions=junctions,
        plaza_polys=plaza_polys,
        district_lw=0.25,
        district_alpha=0.35,
        street_lw=0.4,
        arterial_lw={2: 2.6, 1: 1.2, 0: 0.6},
        ring_lw=1.6,
        point_size=1.2,
        point_alpha=0.18,
        junction_size=5.0,
    )
    _set_extent(ax, boundary, pad=2.0)
    n_hwy = sum(1 for e in edges if e.tier == 2)
    n_major = sum(1 for e in edges if e.tier == 1)
    n_local = sum(1 for e in edges if e.tier == 0)
    ax.set_title(
        "R1 hybrid — macro arterials + core ring-roads + per-block Chen fabric\n"
        f"{len(results)} macro-blocks, {len(core_polys)} core rings, "
        f"{total_districts} districts, {total_streets} Chen streets, "
        f"{n_hwy} highways, {n_major} majors, {n_local} locals",
        fontsize=13,
    )
    ax.legend(handles=_legend_handles(), loc="upper left", fontsize=9, framealpha=0.9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def render_core(
    *,
    fields: IslandFields,
    boundary: sg.Polygon,
    results: list[BlockResult],
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    core_polys: list[sg.Polygon],
    lp_xs: np.ndarray,
    lp_ys: np.ndarray,
    lp_l0: np.ndarray,
    colors: dict[int, tuple[float, float, float, float]],
    junctions: list[SeamJunction],
    plaza_polys: list[sg.Polygon] | None = None,
    window: tuple[float, float, float, float],
    rank: int,
    cx: float,
    cy: float,
    out_path: Path,
    dpi: int,
) -> None:
    """Dense-core zoom render (thicker fabric/arterials than the overview)."""
    fig, ax = plt.subplots(figsize=(11, 11))
    _draw_layers(
        ax,
        fields=fields,
        boundary=boundary,
        results=results,
        arterial_lines=arterial_lines,
        edges=edges,
        core_polys=core_polys,
        lp_xs=lp_xs,
        lp_ys=lp_ys,
        lp_l0=lp_l0,
        colors=colors,
        junctions=junctions,
        plaza_polys=plaza_polys,
        district_lw=0.6,
        district_alpha=0.7,
        street_lw=1.1,
        arterial_lw={2: 3.6, 1: 2.2, 0: 1.3},
        ring_lw=2.8,
        point_size=14.0,
        point_alpha=0.85,
        junction_size=26.0,
    )
    ax.set_xlim(window[0], window[2])
    ax.set_ylim(window[1], window[3])
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(
        f"hybrid dense core #{rank} @ ({cx:.0f}, {cy:.0f}) — "
        "local Chen fabric meeting macro arterials",
        fontsize=12,
    )
    ax.legend(handles=_legend_handles(), loc="upper left", fontsize=8, framealpha=0.9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def render_seam(
    *,
    fields: IslandFields,
    boundary: sg.Polygon,
    results: list[BlockResult],
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    core_polys: list[sg.Polygon],
    lp_xs: np.ndarray,
    lp_ys: np.ndarray,
    lp_l0: np.ndarray,
    colors: dict[int, tuple[float, float, float, float]],
    junctions: list[SeamJunction],
    plaza_polys: list[sg.Polygon] | None = None,
    window: tuple[float, float, float, float],
    block_id: int,
    tjunction_count: int,
    out_path: Path,
    dpi: int,
) -> None:
    """Seam money-shot: the median-T-junction non-core block, thickly styled."""
    fig, ax = plt.subplots(figsize=(11, 11))
    _draw_layers(
        ax,
        fields=fields,
        boundary=boundary,
        results=results,
        arterial_lines=arterial_lines,
        edges=edges,
        core_polys=core_polys,
        lp_xs=lp_xs,
        lp_ys=lp_ys,
        lp_l0=lp_l0,
        colors=colors,
        junctions=junctions,
        plaza_polys=plaza_polys,
        district_lw=0.7,
        district_alpha=0.75,
        street_lw=1.3,
        arterial_lw={2: 3.8, 1: 2.4, 0: 1.5},
        ring_lw=3.0,
        point_size=16.0,
        point_alpha=0.85,
        junction_size=50.0,
    )
    ax.set_xlim(window[0], window[2])
    ax.set_ylim(window[1], window[3])
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(
        f"hybrid seam — macro-block {block_id} ({tjunction_count} T-junctions, "
        "median among non-core blocks) — local fabric plugging into arterials",
        fontsize=12,
    )
    ax.legend(handles=_legend_handles(), loc="upper left", fontsize=8, framealpha=0.9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def junctions_to_geojson(junctions: list[SeamJunction]) -> dict[str, Any]:
    """Serialize every snapped :class:`SeamJunction` as a Point feature."""
    features = []
    for j in junctions:
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [j.x, j.y]},
                "properties": {
                    "block_id": j.block_id,
                    "kind": j.kind,
                    "tier": j.tier,
                    "macro_index": j.macro_index,
                    "distance": j.distance,
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _is_core_block(block: sg.Polygon, core_polys: list[sg.Polygon]) -> bool:
    """True if ``block``'s representative point lands inside a detected core.

    Core interiors are polygonized as their own macro-block (ring boundary as
    a block edge), so a representative-point-in-polygon test is exact for the
    normal case; ``buffer(1e-6)`` absorbs float noise on the shared boundary.
    """
    if not core_polys:
        return False
    point = block.representative_point()
    return any(point.within(poly.buffer(1e-6)) for poly in core_polys)


def _select_seam_block(
    macro_blocks: list[sg.Polygon],
    core_polys: list[sg.Polygon],
    tjunctions_by_block: dict[int, int],
) -> tuple[int, int]:
    """Pick the non-core block with the median T-junction count.

    Ranks (count, block_id) pairs over every non-core block (0 for blocks
    with no realized junction) and takes the element at ``len // 2`` — for an
    odd-length ranking that IS the median value; for an even length it is the
    deterministic upper-middle element (still block_id tie-broken). Falls back
    to ranking over ALL blocks if every block is core (degenerate small
    islands), so a seam render always exists.

    Returns ``(block_id, tjunction_count)``.
    """
    non_core_ids = [
        i
        for i, block in enumerate(macro_blocks)
        if not _is_core_block(block, core_polys)
    ]
    candidate_ids = non_core_ids if non_core_ids else list(range(len(macro_blocks)))
    ranked = sorted((tjunctions_by_block.get(bid, 0), bid) for bid in candidate_ids)
    count, block_id = ranked[len(ranked) // 2]
    return block_id, count


# ---------------------------------------------------------------------------
# Stage G0 — greybox geometry + lots export (docs/greybox-plan.md).
#
# Off by default (``--greybox-out`` is ``None``): nothing below this comment
# runs, and nothing above it changes, so the pre-G0 hybrid output stays
# byte-identical when the flag is omitted.
# ---------------------------------------------------------------------------

_GREYBOX_TIER_NAME: dict[int, str] = {2: "highway", 1: "major", 0: "local"}


def _feature_collection(
    records: list[tuple[int, Any, dict[str, Any]]],
) -> dict[str, Any]:
    """Build a GeoJSON FeatureCollection from ``(id, geometry, properties)``.

    Every feature's ``properties`` gets an explicit ``"id"`` entry (in
    addition to whatever semantic properties the caller supplies), and the
    feature list is sorted by it -- so every greybox export is stable
    regardless of any upstream construction-order subtlety, per the stage-G0
    determinism contract (same inputs/seed -> byte-identical export).
    """
    features = []
    for feature_id, geom, props in records:
        features.append(
            {
                "type": "Feature",
                "geometry": sg.mapping(geom),
                "properties": {"id": feature_id, **props},
            }
        )
    features.sort(key=lambda feat: feat["properties"]["id"])
    return {"type": "FeatureCollection", "features": features}


def _greybox_island_geojson(boundary: sg.Polygon) -> dict[str, Any]:
    """Single-feature island boundary FeatureCollection."""
    return _feature_collection([(0, boundary, {})])


def _greybox_arterials_geojson(
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    ring_lines: list[sg.LineString],
) -> dict[str, Any]:
    """Clipped arterials (``tier``: highway/major/local) + core rings (``ring``)."""
    records: list[tuple[int, Any, dict[str, Any]]] = []
    feature_id = 0
    for line, rec in zip(arterial_lines, edges, strict=True):
        tier_name = _GREYBOX_TIER_NAME.get(rec.tier, str(rec.tier))
        records.append((feature_id, line, {"tier": tier_name}))
        feature_id += 1
    for line in ring_lines:
        records.append((feature_id, line, {"tier": "ring"}))
        feature_id += 1
    return _feature_collection(records)


def _greybox_blocks_geojson(macro_blocks: list[sg.Polygon]) -> dict[str, Any]:
    """Macro-block polygons, ``block_id`` == the export's own feature id."""
    return _feature_collection(
        [(i, poly, {"block_id": i}) for i, poly in enumerate(macro_blocks)]
    )


def _district_typology_tier(
    poly: sg.Polygon,
    world_count: int,
    massing: MassingConfig,
    meters_per_unit: float,
) -> str:
    """Area-only typology tier for a WHOLE district (``"detached"``/``"row"``,
    ``"park"`` for a zero-world district) -- ignores ``landmark_ids`` (a
    per-WORLD override, see :func:`mapgen.r1_lots.classify_typology`), so this
    is the base tier every occupied lot in the district shares before any
    landmark override. Used only for ``districts.geojson``'s audit-facing
    ``typology`` property; ``lots.parquet``'s per-lot ``typology`` column
    (``Lot.typology``) is the authoritative per-world value."""
    if world_count <= 0:
        return "park"
    return classify_typology(
        poly.area, world_count, "", frozenset(), massing, meters_per_unit
    )


def _greybox_districts_geojson(
    district_records: list[tuple[int, int, sg.Polygon, int, int | None, float | None]],
    massing: MassingConfig,
    meters_per_unit: float,
    plaza_ids: frozenset[int] = frozenset(),
) -> dict[str, Any]:
    """``district_records``: ``(district_id, block_id, polygon, world_count,
    nucleus_id, nucleus_dist)``.

    ``typology`` is the district's AREA-ONLY tier (:func:`_district_typology_tier`)
    -- the classification map viewers/audit see at a glance; a district's
    individual lots can still classify ``"landmark"`` per world (see
    ``lots.parquet``'s own ``typology`` column). ``nucleus_id``/``nucleus_dist``
    (S2 interface, :func:`mapgen.r1_macro.assign_nearest_nucleus`) are ``null``
    when no cores were detected (``nuclei`` empty). A ``plaza_ids`` member
    (slice P, :func:`plaza_district_ids`) is FORCED ``kind="park"`` --
    belt-and-suspenders on top of the world_count==0 path, which already
    holds for plaza districts because :func:`assign_worlds_excluding` never
    assigns worlds to them (:func:`export_greybox` asserts it).
    """
    records: list[tuple[int, Any, dict[str, Any]]] = []
    for (
        district_id,
        block_id,
        poly,
        world_count,
        nucleus_id,
        nucleus_dist,
    ) in district_records:
        is_park = world_count <= 0 or district_id in plaza_ids
        kind = "park" if is_park else "fabric"
        records.append(
            (
                district_id,
                poly,
                {
                    "block_id": block_id,
                    "district_id": district_id,
                    "kind": kind,
                    "world_count": world_count,
                    "typology": _district_typology_tier(
                        poly, world_count, massing, meters_per_unit
                    ),
                    "nucleus_id": nucleus_id,
                    "nucleus_dist": nucleus_dist,
                },
            )
        )
    return _feature_collection(records)


def _greybox_streets_geojson(results: list[BlockResult]) -> dict[str, Any]:
    """Per-block Chen streets, excluding perimeter-duplicate paths."""
    records: list[tuple[int, Any, dict[str, Any]]] = []
    feature_id = 0
    for res in results:
        flags = res.street_perimeter_flags or [False] * len(res.streets)
        for line, is_perimeter in zip(res.streets, flags, strict=True):
            if is_perimeter or line.geom_type != "LineString":
                continue
            records.append((feature_id, line, {"block_id": res.block_id}))
            feature_id += 1
    return _feature_collection(records)


def plaza_district_ids(
    district_polys: list[sg.Polygon],
    plaza_polys: list[sg.Polygon],
    *,
    cover_frac: float = 0.5,
) -> frozenset[int]:
    """District indices that are plaza-derived (slice P).

    A district counts as plaza-derived when at least ``cover_frac`` of its
    area lies inside ANY single ``plaza_polys`` member. Plazas are protected
    macro-block faces (see ``mapgen.r1_macro.build_macro_blocks_with_cores``),
    so the expectation is ~1 district per plaza -- but Chen may subdivide a
    plaza block further, so any number of districts can match one plaza.
    Zero-area districts never match. Deterministic: a plain index scan with
    a bounds pre-check, no set iteration order in the result (frozenset of
    ints).
    """
    ids: set[int] = set()
    for district_id, poly in enumerate(district_polys):
        if poly.is_empty or poly.area <= 0.0:
            continue
        dminx, dminy, dmaxx, dmaxy = poly.bounds
        for plaza in plaza_polys:
            pminx, pminy, pmaxx, pmaxy = plaza.bounds
            if dminx > pmaxx or dmaxx < pminx or dminy > pmaxy or dmaxy < pminy:
                continue
            if poly.intersection(plaza).area >= cover_frac * poly.area:
                ids.add(district_id)
                break
    return frozenset(ids)


def assign_worlds_excluding(
    points: pl.DataFrame,
    district_polys: list[sg.Polygon],
    excluded_ids: frozenset[int],
) -> tuple[dict[int, list[int]], list[str]]:
    """``assign_worlds_to_districts`` with some districts removed as candidates.

    Same return shape as :func:`mapgen.r1_lots.assign_worlds_to_districts`
    (assignment keyed by GLOBAL district index over ALL of ``district_polys``,
    plus the per-row direct/snapped kinds), but districts in ``excluded_ids``
    (slice P: the plaza-derived ones) are never candidates: they always come
    back with an empty row list, and a world whose coordinates fall inside
    one is "snapped" to the nearest non-excluded district instead of
    direct-assigning into it. Assignment semantics for every other world are
    unchanged -- district polygons are disjoint, so removing candidates
    cannot alter another world's direct match. With nothing excluded this is
    exactly ``assign_worlds_to_districts``.
    """
    if not excluded_ids:
        return assign_worlds_to_districts(points, district_polys)
    candidate_ids = [i for i in range(len(district_polys)) if i not in excluded_ids]
    sub_assignment, assigned_kind = assign_worlds_to_districts(
        points, [district_polys[i] for i in candidate_ids]
    )
    assignment: dict[int, list[int]] = {i: [] for i in range(len(district_polys))}
    for sub_idx, rows in sub_assignment.items():
        assignment[candidate_ids[sub_idx]] = rows
    return assignment, assigned_kind


def export_greybox(
    out_dir: Path,
    in_dir: Path,
    *,
    boundary: sg.Polygon,
    layer: MacroLayer,
    results: list[BlockResult],
    points: pl.DataFrame,
    inset: float = DEFAULT_INSET,
    min_lot_area_frac: float = DEFAULT_MIN_LOT_AREA_FRAC,
    massing: MassingConfig | None = None,
    meters_per_unit: float = DEFAULT_METERS_PER_UNIT,
) -> dict[str, Any]:
    """Write the Stage-G0 greybox export (geometry + lots) to ``out_dir``.

    Assembles a GLOBAL district list by walking ``results`` in ``block_id``
    order (already the order ``run_all_blocks`` built them in) and, within a
    block, in that block's own ``districts`` list order -- both deterministic
    given fixed inputs/seeds, so the resulting ``district_id`` (its position
    in that walk) is stable across runs. Every world in ``points`` is then
    assigned to exactly one NON-plaza district (:func:`assign_worlds_excluding`
    over ``mapgen.r1_lots.assign_worlds_to_districts`` -- direct
    point-in-polygon or nearest-snap, with the plaza-derived districts of
    :func:`plaza_district_ids` removed from the candidate set, slice P), and
    every district with at least one world gets its lots tessellated
    (``mapgen.r1_lots.build_lots``); a district with zero worlds -- which
    every plaza district is, asserted below -- is exported as ``kind="park"``
    with no lots, per ``docs/greybox-plan.md`` Stage G0.

    ``massing`` (default ``MassingConfig()``) + ``meters_per_unit`` drive the
    wave-2 typology/height/footprint model (``docs/wave2-plan.md``);
    ``landmark_ids`` (the top ``massing.landmark_count`` worlds by visits,
    island-wide -- :func:`mapgen.r1_lots.top_landmark_ids`) is computed once
    here from ``points`` and threaded into every district's ``build_lots``
    call.

    Writes ``island.geojson``, ``arterials.geojson`` (clipped arterials +
    core rings), ``blocks.geojson``, ``nuclei.geojson`` (ranked settlement
    nuclei -- anchor points with ``rank``/``mass``/``label``/``is_major``, see
    :func:`mapgen.r1_macro.nucleus_specs_to_geojson`, S2),
    ``districts.geojson`` (now with a ``typology`` property per district --
    the AREA-ONLY tier, see :func:`_district_typology_tier` -- plus
    ``nucleus_id``/``nucleus_dist``, the nearest-nucleus assignment from
    :func:`mapgen.r1_macro.assign_nearest_nucleus`), ``streets.geojson``
    (perimeter-duplicate paths excluded, reusing the connectivity wave's
    ``street_perimeter_flags``), ``lots.parquet`` (now with
    ``lot_x``/``lot_y``/``kind``/``displacement``/``typology`` -- see
    ``mapgen.r1_lots.Lot``, ``kind="greenspace"`` rows carry ``world_id=""`` --
    plus a ``lot_id`` column: the row's index in this function's own
    deterministic district-walk BUILD order, assigned before the world_id sort
    below so downstream readers (``run_r1_app_export.py``) can use it directly
    as a stable identifier instead of re-minting one via a different sort --
    and its district's ``nucleus_id``/``nucleus_dist``, same as
    ``districts.geojson``), and ``greybox_manifest.json``
    (``displacement_stats``/``fallback_districts`` keys and
    ``counts.n_greenspace``/``counts.n_fallback``/``counts.n_nuclei``/
    ``counts.n_major_nuclei``, one shared ``fallback_stats`` dict threaded
    across every district's ``build_lots`` call). Returns the manifest dict.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    if massing is None:
        massing = MassingConfig()

    district_entries: list[tuple[int, sg.Polygon]] = []  # (block_id, polygon)
    for res in results:
        for poly in res.districts:
            district_entries.append((res.block_id, poly))
    district_polys = [poly for _block_id, poly in district_entries]

    # Slice P: plaza-derived districts are excluded from the assignment
    # candidate set, so a world whose raw coordinates fall inside a plaza
    # disc snaps to the nearest NON-plaza district instead of turning the
    # plaza into subdivided fabric (the S3 "no extra plumbing needed"
    # assumption this replaces).
    plaza_ids = plaza_district_ids(district_polys, layer.plaza_polys)
    assignment, assigned_kind = assign_worlds_excluding(
        points, district_polys, plaza_ids
    )
    n_direct = sum(1 for kind in assigned_kind if kind == "direct")
    n_snapped = sum(1 for kind in assigned_kind if kind == "snapped")

    # Nearest-nucleus assignment (S2 interface): one (nucleus_id, nucleus_dist)
    # pair per district, aligned to district_polys / district_entries order
    # (district_id IS that index -- see the enumerate() below), so
    # nucleus_by_district[district_id] below is a direct lookup.
    nucleus_by_district = assign_nearest_nucleus(district_polys, layer.nuclei)

    landmark_ids = top_landmark_ids(points, massing.landmark_count)

    all_lots: list[Lot] = []
    district_records: list[
        tuple[int, int, sg.Polygon, int, int | None, float | None]
    ] = []
    n_park = 0
    # Shared across every district's build_lots call (docs/lots-wave-plan.md
    # slice L2) so the manifest reports ONE island-wide fallback tally rather
    # than only the last district's.
    fallback_stats: dict[str, Any] = {}
    for district_id, (block_id, poly) in enumerate(district_entries):
        row_indices = assignment.get(district_id, [])
        world_count = len(row_indices)
        # Slice P invariant: a plaza district can NEVER hold worlds --
        # assign_worlds_excluding structurally guarantees it, and this assert
        # keeps any future assignment change from silently regressing plazas
        # back into fabric.
        assert district_id not in plaza_ids or world_count == 0, (
            f"plaza district {district_id} was assigned {world_count} worlds; "
            "plaza districts must stay zero-world parks (slice P)"
        )
        nucleus_id, nucleus_dist = nucleus_by_district[district_id]
        district_records.append(
            (district_id, block_id, poly, world_count, nucleus_id, nucleus_dist)
        )
        if world_count == 0:
            n_park += 1
            continue
        member = select_member_points(points, row_indices, assigned_kind)
        all_lots.extend(
            build_lots(
                poly,
                district_id,
                member,
                inset=inset,
                min_lot_area_frac=min_lot_area_frac,
                massing=massing,
                meters_per_unit=meters_per_unit,
                landmark_ids=landmark_ids,
                fallback_stats=fallback_stats,
            )
        )
    n_sliver = count_sliver_reassignments(all_lots)
    n_greenspace = sum(1 for lot in all_lots if lot.kind == "greenspace")

    # Fallback-rate acceptance check (docs/lots-wave-plan.md): the street-
    # fronting subdivision path is meant to be the norm, with the Voronoi
    # fallback (:func:`build_lots`) absorbing rare geometry pathologies --
    # not a large fraction of all districts. Mirrors the affine-roundtrip
    # acceptance check in ``scripts/run_r1_app_export.py`` (a loud but
    # non-fatal signal; does not raise/exit nonzero).
    n_districts = len(district_entries)
    n_fallback = fallback_stats.get("n_fallback", 0)
    fallback_threshold = max(2, 0.02 * n_districts)
    fallback_ok = n_fallback <= fallback_threshold
    if not fallback_ok:
        print(
            f"!!! WARNING: {n_fallback}/{n_districts} districts fell back to "
            f"Voronoi tessellation (threshold {fallback_threshold:.1f}) -- "
            "street-fronting subdivision is failing far more often than "
            "expected; see greybox_manifest.json's fallback_districts for "
            "reasons. !!!"
        )

    with (out_dir / "island.geojson").open("w") as f:
        json.dump(_greybox_island_geojson(boundary), f, indent=2, sort_keys=True)
    with (out_dir / "arterials.geojson").open("w") as f:
        json.dump(
            _greybox_arterials_geojson(
                layer.arterial_lines, layer.edges, layer.ring_lines
            ),
            f,
            indent=2,
            sort_keys=True,
        )
    with (out_dir / "blocks.geojson").open("w") as f:
        json.dump(_greybox_blocks_geojson(layer.blocks), f, indent=2, sort_keys=True)
    with (out_dir / "nuclei.geojson").open("w") as f:
        json.dump(nucleus_specs_to_geojson(layer.nuclei), f, indent=2, sort_keys=True)
    with (out_dir / "districts.geojson").open("w") as f:
        json.dump(
            _greybox_districts_geojson(
                district_records, massing, meters_per_unit, plaza_ids
            ),
            f,
            indent=2,
            sort_keys=True,
        )
    with (out_dir / "streets.geojson").open("w") as f:
        json.dump(_greybox_streets_geojson(results), f, indent=2, sort_keys=True)

    # lot_id: row index in the district-walk BUILD order (all_lots' append
    # order from the district loop above -- itself deterministic given fixed
    # inputs/seeds, see this function's docstring), assigned BEFORE the
    # world_id sort below so it stays stable across reruns rather than being
    # re-derived from a different sort key downstream. Paired with each Lot
    # via enumerate() before sorting so it travels along with its lot.
    all_lots_with_id = list(enumerate(all_lots))
    # lots.parquet — sorted by world_id so the table is deterministic
    # independent of district/build_lots internal ordering (lot_id, above,
    # is what preserves the build-order identity through this sort).
    all_lots_with_id.sort(key=lambda pair: pair[1].world_id)
    lot_ids = [pair[0] for pair in all_lots_with_id]
    all_lots = [pair[1] for pair in all_lots_with_id]

    # Per-district nucleus_id/nucleus_dist (S2 interface), propagated onto
    # every lot of that district -- both None when `layer.nuclei` is empty
    # (no cores detected).
    nucleus_by_district_id: dict[int, tuple[int | None, float | None]] = {
        district_id: (nucleus_id, nucleus_dist)
        for district_id, _block_id, _poly, _world_count, nucleus_id, nucleus_dist in (
            district_records
        )
    }

    lots_df = pl.DataFrame(
        {
            "world_id": [lot.world_id for lot in all_lots],
            "district_id": [lot.district_id for lot in all_lots],
            "footprint_wkb": [lot.footprint.wkb for lot in all_lots],
            "lot_wkb": [lot.lot.wkb for lot in all_lots],
            "height": [lot.height for lot in all_lots],
            # Explicit Utf8: island_points.parquet has no ``name`` column today
            # (see mapgen.r1_lots module docstring), so every Lot.name is
            # None -- without an explicit dtype polars would otherwise infer
            # an all-null column as its own Null dtype, which downstream
            # parquet readers may not expect for a "should be a string" column.
            "name": pl.Series("name", [lot.name for lot in all_lots], dtype=pl.Utf8),
            "visits": [lot.visits for lot in all_lots],
            "x": [lot.x for lot in all_lots],
            "y": [lot.y for lot in all_lots],
            "assigned": [lot.assigned for lot in all_lots],
            "lot_x": [lot.lot_x for lot in all_lots],
            "lot_y": [lot.lot_y for lot in all_lots],
            "kind": [lot.kind for lot in all_lots],
            "displacement": [lot.displacement for lot in all_lots],
            "typology": [lot.typology for lot in all_lots],
            "lot_id": lot_ids,
            # Explicit dtype for the same reason as "name" above: with no
            # nuclei detected every value is None, which polars would
            # otherwise infer as its own Null dtype.
            "nucleus_id": pl.Series(
                "nucleus_id",
                [nucleus_by_district_id[lot.district_id][0] for lot in all_lots],
                dtype=pl.Int64,
            ),
            "nucleus_dist": pl.Series(
                "nucleus_dist",
                [nucleus_by_district_id[lot.district_id][1] for lot in all_lots],
                dtype=pl.Float64,
            ),
        }
    )
    lots_df.write_parquet(out_dir / "lots.parquet")

    island_frame: dict[str, Any] = {}
    inputs_manifest_path = in_dir / "inputs_manifest.json"
    if inputs_manifest_path.exists():
        inputs_manifest = json.loads(inputs_manifest_path.read_text())
        island_frame = inputs_manifest.get("island_frame", {})

    n_streets_exported = sum(
        1
        for res in results
        for is_perimeter in (res.street_perimeter_flags or [False] * len(res.streets))
        if not is_perimeter
    )

    manifest: dict[str, Any] = {
        "stage": "G0 (greybox geometry + lots export)",
        "description": (
            "Island/arterial/block/district/street geometry + per-district "
            "street-fronting lot subdivision (falling back to per-world "
            "Voronoi tessellation on subdivision failure, see 'n_fallback') "
            "and building footprints, exported from the hybrid pipeline for "
            "the G1 mesh bake / track-W 2D site view (docs/greybox-plan.md, "
            "docs/lots-wave-plan.md)"
        ),
        "counts": {
            "n_worlds": points.height,
            "n_direct": n_direct,
            "n_snapped": n_snapped,
            "n_blocks": len(layer.blocks),
            "n_arterials": len(layer.arterial_lines),
            "n_ring_roads": len(layer.ring_lines),
            "n_nuclei": len(layer.nuclei),
            "n_major_nuclei": sum(1 for n in layer.nuclei if n.is_major),
            "n_streets_exported": n_streets_exported,
            "n_districts": len(district_entries),
            "n_park_districts": n_park,
            # Plaza-derived park districts (slice P): a subset of
            # n_park_districts -- excluded from world assignment and forced
            # kind="park", see plaza_district_ids/assign_worlds_excluding.
            "n_plaza_districts": len(plaza_ids),
            "n_fabric_districts": len(district_entries) - n_park,
            "n_lots": len(all_lots),
            "n_sliver_reassignments": n_sliver,
            "n_greenspace": n_greenspace,
            "n_fallback": fallback_stats.get("n_fallback", 0),
            # districts that reached N lots via the shape-floor relaxation ladder
            # (subdivision-robustness slice) instead of a full Voronoi fallback --
            # a soft-degradation count, distinct from the hard n_fallback above.
            "n_relaxed": fallback_stats.get("n_relaxed", 0),
            "typology_counts": {
                typ: sum(
                    1 for lot in all_lots if lot.kind == "lot" and lot.typology == typ
                )
                for typ in ("detached", "row", "landmark")
            },
        },
        "lot_config": {
            "inset": inset,
            "min_lot_area_frac": min_lot_area_frac,
            "meters_per_unit": meters_per_unit,
            "massing": asdict(massing),
            "n_landmark_ids": len(landmark_ids),
        },
        "displacement_stats": displacement_stats(all_lots),
        "fallback_districts": fallback_stats.get("fallback_districts", []),
        "acceptance": {"fallback_ok": fallback_ok},
        "island_frame": island_frame,
        "coordinate_note": (
            "Planar geometry in every greybox export file (island/arterials/"
            "blocks/districts/streets, and lots.parquet's x/y/lot_x/lot_y/"
            "displacement/footprint_wkb/lot_wkb) is in ISLAND UNITS -- the "
            "same frame as island_points.parquet / hybrid_manifest.json -- "
            "NOT meters. 'displacement' is an island-unit DISTANCE (Euclidean, "
            "same units as x/y), so 'displacement_stats' below is also island "
            "units; multiply by the island_frame scale's reciprocal (or see "
            "run_r1_app_export.py's affine) to get app units, or by the G1 "
            "mesh bake's --meters-per-unit for meters. lots.parquet's "
            "'height' column IS already in meters (MassingConfig constants "
            "are meter constants, docs/wave2-plan.md 'The massing model'); "
            "G1's mesh bake applies --meters-per-unit to the planar geometry "
            "only, not to height. This export's own 'meters_per_unit' "
            "(lot_config below) is the SAME value G1 must be run with -- the "
            "massing model's setbacks are meter-denominated and convert to "
            "island units using it, so a mismatched G1 --meters-per-unit "
            "would silently mis-scale the footprint/setback geometry."
        ),
        "outputs": [
            "island.geojson",
            "arterials.geojson",
            "blocks.geojson",
            "nuclei.geojson",
            "districts.geojson",
            "streets.geojson",
            "lots.parquet",
            "greybox_manifest.json",
        ],
    }
    with (out_dir / "greybox_manifest.json").open("w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    return manifest


def run_hybrid(
    in_dir: Path,
    out_dir: Path,
    *,
    total_target: int,
    n_cores: int = 3,
    dpi: int = 240,
    max_gate_spacing: float | None = None,
    greybox_out: Path | None = None,
    greybox_inset: float = DEFAULT_INSET,
    greybox_min_lot_area_frac: float = DEFAULT_MIN_LOT_AREA_FRAC,
    greybox_massing: MassingConfig | None = None,
    greybox_meters_per_unit: float = DEFAULT_METERS_PER_UNIT,
) -> dict[str, Any]:
    """Full hybrid assembly: macro layer -> per-block Chen -> assemble -> render.

    ``max_gate_spacing`` is stage-4 gate densification
    (``mapgen.r1_connect.densify_gates``, wired in ``run_all_blocks``);
    ``None`` (the default) leaves it OFF, so the connectivity numbers are
    byte-identical to the pre-stage-4 hybrid.

    ``greybox_out`` (stage G0, optional; ``None`` is OFF by default) writes
    the geometry + lots export (:func:`export_greybox`) to that directory
    alongside the existing PNG/junctions/manifest outputs, once assembly is
    done. Leaving it ``None`` runs exactly the pre-G0 code path -- this
    parameter and the ``greybox_*`` knobs are read nowhere else in this
    function.
    """
    t_start = time.perf_counter()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Building stage-1 macro layer (shared build_macro_layer)…", flush=True)
    fields = IslandFields.from_npz(str(in_dir / "fields.npz"))
    boundary = load_boundary(in_dir)
    points = pl.read_parquet(in_dir / "island_points.parquet")
    params = MacroParams()
    # Stage 3.5b: the block set is polygonized over CLIPPED arterials + core
    # rings + boundary, and the drawn arterials are the clipped ones (no
    # convergence on a core summit).
    layer = build_macro_layer(fields, boundary, points, params)
    arterial_lines: list[sg.LineString] = layer.arterial_lines
    edges: list[MacroEdge] = layer.edges
    core_polys: list[sg.Polygon] = layer.core_polys
    macro_blocks: list[sg.Polygon] = layer.blocks
    n_blocks = len(macro_blocks)
    print(
        f"  {n_blocks} macro-blocks, {len(core_polys)} core rings, "
        f"{len(edges)} arterial segments",
        flush=True,
    )

    # Global calibration (LOCKED): single district mass M over every block.
    density_field = build_density_field(fields)
    total_mass = float(density_field.mass(boundary))
    island_area = float(boundary.area)
    global_m = total_mass / float(total_target)
    min_parcel_area = island_area / (float(total_target) * 4.0)
    print(
        f"  calibration: total_mass={total_mass:.2f}, island_area={island_area:.2f}, "
        f"M={global_m:.4f}, min_parcel_area={min_parcel_area:.4f}",
        flush=True,
    )

    print(f"Running Chen/R2 in {n_blocks} macro-blocks (global M)…", flush=True)
    results = run_all_blocks(
        macro_blocks,
        fields,
        max_parcel_mass=global_m,
        min_parcel_area=min_parcel_area,
        max_gate_spacing=max_gate_spacing,
    )

    total_districts = sum(r.district_count for r in results)
    total_streets = sum(len(r.streets) for r in results)
    n_failed = sum(1 for r in results if r.failed)
    n_connectors_added = sum(r.n_connectors for r in results)
    print(
        f"  assembled: {total_districts} districts, {total_streets} Chen streets, "
        f"{n_failed} blocks failed"
        + (
            f", {n_connectors_added} gate-densification connectors"
            if max_gate_spacing is not None
            else ""
        ),
        flush=True,
    )

    # Stage-3 connectivity: snap every block's boundary gates onto the macro
    # network, summarize the seam, and fuse everything into one graph.
    print("Snapping gates to macro network (arterial/ring/coast)…", flush=True)
    gates_by_block: dict[int, list[Gate]] = {r.block_id: r.gates for r in results}
    streets_by_block: dict[int, list[sg.LineString]] = {
        r.block_id: r.streets for r in results
    }
    perimeter_flags_by_block: dict[int, list[bool]] = {
        r.block_id: r.street_perimeter_flags for r in results
    }
    junctions: list[SeamJunction] = snap_gates_to_macro(
        gates_by_block, arterial_lines, edges, layer.ring_lines, boundary, tol=0.05
    )
    conn_metrics = connectivity_metrics(
        junctions, arterial_lines, edges, layer.ring_lines, macro_blocks
    )
    unified_graph = build_unified_street_graph(
        arterial_lines,
        edges,
        layer.ring_lines,
        junctions,
        streets_by_block,
        perimeter_flags_by_block,
    )
    graph_summary = graph_connectivity_summary(unified_graph)
    print(
        f"  connectivity: {conn_metrics['n_tjunctions']} T-junctions "
        f"({conn_metrics['n_arterial']} arterial, {conn_metrics['n_ring']} ring), "
        f"graph n_components={graph_summary['n_components']}, "
        f"largest_component_local_length_fraction="
        f"{graph_summary['largest_component_local_length_fraction']}",
        flush=True,
    )
    with (out_dir / "hybrid_junctions.geojson").open("w") as f:
        json.dump(junctions_to_geojson(junctions), f, indent=2)

    # Points (loaded once, reused across renders). Worlds whose raw
    # coordinates fall inside a plaza disc are masked out of the render
    # scatter (slice P): the greybox assignment excludes plaza districts, so
    # those worlds live in the nearest non-plaza district now -- drawing
    # their raw position inside the park would misrepresent the fabric (the
    # colors lookup still spans EVERY l0 cluster so palette assignment is
    # unchanged by the mask).
    lp = load_points_with_labels(str(in_dir / "island_points.parquet"))
    colors = _color_lookup(lp.l0_ids)
    if layer.plaza_polys:
        plaza_union = unary_union(layer.plaza_polys)
        keep = ~shapely.contains_xy(plaza_union, lp.xs, lp.ys)
        dot_xs, dot_ys, dot_l0 = lp.xs[keep], lp.ys[keep], lp.l0_ids[keep]
        print(
            f"  render mask: {int((~keep).sum())} world dots inside plaza "
            "discs hidden (re-assigned outward by the greybox exclusion)",
            flush=True,
        )
    else:
        dot_xs, dot_ys, dot_l0 = lp.xs, lp.ys, lp.l0_ids

    print("Rendering hybrid_overview.png…", flush=True)
    render_overview(
        fields=fields,
        boundary=boundary,
        results=results,
        arterial_lines=arterial_lines,
        edges=edges,
        core_polys=core_polys,
        lp_xs=dot_xs,
        lp_ys=dot_ys,
        lp_l0=dot_l0,
        colors=colors,
        junctions=junctions,
        plaza_polys=layer.plaza_polys,
        total_districts=total_districts,
        total_streets=total_streets,
        out_path=out_dir / "hybrid_overview.png",
        dpi=dpi,
    )

    # Dense-core zooms into the top density peaks.
    density = fields.density
    x0, y0, cell = fields.x0, fields.y0, fields.cell
    peak_xs, peak_ys, peak_ds = find_density_peaks(density, x0, y0, cell)
    order = np.argsort(peak_ds)[::-1]
    span = max(
        boundary.bounds[2] - boundary.bounds[0],
        boundary.bounds[3] - boundary.bounds[1],
    )
    half_span = span * 0.12
    core_paths: list[str] = []
    for rank, pidx in enumerate(order[:n_cores], start=1):
        cx, cy = float(peak_xs[pidx]), float(peak_ys[pidx])
        window = zoom_window(cx, cy, half_span)
        out_path = out_dir / f"hybrid_core{rank}.png"
        print(f"Rendering {out_path.name}…", flush=True)
        render_core(
            fields=fields,
            boundary=boundary,
            results=results,
            arterial_lines=arterial_lines,
            edges=edges,
            core_polys=core_polys,
            lp_xs=dot_xs,
            lp_ys=dot_ys,
            lp_l0=dot_l0,
            colors=colors,
            junctions=junctions,
            plaza_polys=layer.plaza_polys,
            window=window,
            rank=rank,
            cx=cx,
            cy=cy,
            out_path=out_path,
            dpi=dpi,
        )
        core_paths.append(str(out_path))

    # Seam money-shot: the non-core block with the median T-junction count.
    tjunctions_by_block: dict[int, int] = {}
    for j in junctions:
        if j.kind in ("arterial", "ring"):
            tjunctions_by_block[j.block_id] = tjunctions_by_block.get(j.block_id, 0) + 1
    seam_block_id, seam_tjunction_count = _select_seam_block(
        macro_blocks, core_polys, tjunctions_by_block
    )
    seam_block = macro_blocks[seam_block_id]
    minx, miny, maxx, maxy = seam_block.bounds
    pad_x = (maxx - minx) * 0.2
    pad_y = (maxy - miny) * 0.2
    seam_window = (minx - pad_x, miny - pad_y, maxx + pad_x, maxy + pad_y)
    seam_path = out_dir / "hybrid_seam.png"
    print(
        f"Rendering {seam_path.name}… (block {seam_block_id}, "
        f"{seam_tjunction_count} T-junctions)",
        flush=True,
    )
    render_seam(
        fields=fields,
        boundary=boundary,
        results=results,
        arterial_lines=arterial_lines,
        edges=edges,
        core_polys=core_polys,
        lp_xs=dot_xs,
        lp_ys=dot_ys,
        lp_l0=dot_l0,
        colors=colors,
        junctions=junctions,
        plaza_polys=layer.plaza_polys,
        window=seam_window,
        block_id=seam_block_id,
        tjunction_count=seam_tjunction_count,
        out_path=seam_path,
        dpi=dpi,
    )

    runtime_s = round(time.perf_counter() - t_start, 2)

    n_hwy = sum(1 for e in edges if e.tier == 2)
    n_major = sum(1 for e in edges if e.tier == 1)
    n_local = sum(1 for e in edges if e.tier == 0)

    # Invariant pass rate over the blocks where Chen actually ran (non-fallback).
    ran = [r for r in results if not r.failed]
    n_geom_pass = sum(1 for r in ran if r.geometry_valid_pass)
    n_inv_pass = sum(1 for r in ran if r.paper_invariant_pass)
    pass_rate = {
        "n_blocks_chen_ran": len(ran),
        "geometry_valid_pass": n_geom_pass,
        "paper_invariant_pass": n_inv_pass,
        "geometry_valid_pass_frac": round(n_geom_pass / len(ran), 4) if ran else None,
        "paper_invariant_pass_frac": round(n_inv_pass / len(ran), 4) if ran else None,
    }

    manifest: dict[str, Any] = {
        "stage": "3 (full hybrid assembly, stage-3.5b core ring-roads, "
        "stage-3 connectivity)",
        "description": (
            "Chen/R2 in every macro-block (global district mass) + macro "
            "arterials clipped to dense-core ring-roads (downtown blocks); "
            "arterial<->local junctions snapped and fused into one unified "
            "street graph (mapgen.r1_connect)"
        ),
        "macro_params": params.to_dict(),
        "total_target": total_target,
        "M": round(global_m, 6),
        "min_parcel_area": round(min_parcel_area, 6),
        "total_mass": round(total_mass, 4),
        "island_area": round(island_area, 4),
        "n_core_rings": len(core_polys),
        "n_macro_blocks": n_blocks,
        "n_blocks_failed": n_failed,
        "failed_block_ids": [r.block_id for r in results if r.failed],
        "total_districts": total_districts,
        "total_chen_streets": total_streets,
        "n_highway_arterials": n_hwy,
        "n_major_arterials": n_major,
        "n_local_arterials": n_local,
        # T-geo cleanup + slice-B functional-relabel observability counters.
        "tgeo_b_counters": {
            "n_corridors_merged": layer.n_corridors_merged,
            "n_endpoints_snapped": layer.n_endpoints_snapped,
            "n_dangles_pruned": layer.n_dangles_pruned,
            "n_tier_changed": layer.n_tier_changed,
            "n_disconnected_nucleus_pairs": layer.n_disconnected_nucleus_pairs,
        },
        "per_block_district_counts": [r.district_count for r in results],
        "per_block_seed_used": [r.seed_used for r in results],
        "invariant_pass_rate": pass_rate,
        "retry_seeds": list(DEFAULT_RETRY_SEEDS),
        "connectivity": {
            **conn_metrics,
            "graph": graph_summary,
            "seam_block_id": seam_block_id,
            "seam_block_tjunctions": seam_tjunction_count,
            "densification_enabled": max_gate_spacing is not None,
            "max_gate_spacing": max_gate_spacing,
            "n_connectors_added": n_connectors_added,
        },
        "runtime_seconds": runtime_s,
        "outputs": [
            "hybrid_overview.png",
            *[Path(p).name for p in core_paths],
            "hybrid_seam.png",
            "hybrid_junctions.geojson",
            "hybrid_manifest.json",
        ],
    }
    with (out_dir / "hybrid_manifest.json").open("w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote hybrid_manifest.json (runtime {runtime_s}s)", flush=True)

    if greybox_out is not None:
        print(f"Writing greybox export to {greybox_out}…", flush=True)
        greybox_manifest = export_greybox(
            greybox_out,
            in_dir,
            boundary=boundary,
            layer=layer,
            results=results,
            points=points,
            inset=greybox_inset,
            min_lot_area_frac=greybox_min_lot_area_frac,
            massing=greybox_massing,
            meters_per_unit=greybox_meters_per_unit,
        )
        counts = greybox_manifest["counts"]
        print(
            f"  greybox: {counts['n_worlds']} worlds "
            f"({counts['n_direct']} direct, {counts['n_snapped']} snapped), "
            f"{counts['n_districts']} districts "
            f"({counts['n_fabric_districts']} fabric, "
            f"{counts['n_park_districts']} park), "
            f"{counts['n_lots']} lots "
            f"({counts['n_sliver_reassignments']} sliver reassignments)",
            flush=True,
        )

    return manifest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="R1 stage 3: full hybrid assembly (Chen/R2 in every block)"
    )
    parser.add_argument("--in-dir", type=Path, default=_DEFAULT_IN)
    parser.add_argument("--out-dir", type=Path, default=_DEFAULT_OUT)
    parser.add_argument(
        "--total-target",
        type=int,
        default=1200,
        help="Island-wide district target; sets global M = total_mass / target",
    )
    parser.add_argument("--n-cores", type=int, default=3)
    parser.add_argument("--dpi", type=int, default=240)
    parser.add_argument(
        "--max-gate-spacing",
        type=float,
        default=None,
        help=(
            "Stage-4 boundary-run densification threshold (island units): a "
            "boundary run (gap between a block's gate stations) longer than "
            "this gets a connector street routed to the nearest interior "
            "local-street node (mapgen.r1_connect.densify_gates). Default "
            "None leaves densification OFF (byte-identical to the "
            "pre-stage-4 hybrid)."
        ),
    )
    parser.add_argument(
        "--greybox-out",
        type=Path,
        default=None,
        help=(
            "Stage-G0 geometry + lots export directory (docs/greybox-plan.md): "
            "island/arterials/blocks/districts/streets geojson + lots.parquet "
            "+ greybox_manifest.json. Default None writes nothing (the "
            "pre-G0 hybrid run is unaffected)."
        ),
    )
    parser.add_argument(
        "--greybox-inset",
        type=float,
        default=DEFAULT_INSET,
        help="Building footprint inward-buffer inset, island units (stage G0).",
    )
    parser.add_argument(
        "--greybox-min-lot-area-frac",
        type=float,
        default=DEFAULT_MIN_LOT_AREA_FRAC,
        help="Sliver-lot floor as a fraction of the median cell area (stage G0).",
    )
    parser.add_argument(
        "--meters-per-unit",
        type=float,
        default=DEFAULT_METERS_PER_UNIT,
        help=(
            "Island-units-to-meters scale (stage G0): must match the G1 mesh "
            "bake's --meters-per-unit -- the wave-2 massing model's setbacks "
            "(docs/wave2-plan.md) are meter-denominated and convert to island "
            "units using this value. Default matches "
            "mapgen.r1_mesh.DEFAULT_METERS_PER_UNIT."
        ),
    )
    args = parser.parse_args(argv)

    manifest = run_hybrid(
        args.in_dir,
        args.out_dir,
        total_target=args.total_target,
        n_cores=args.n_cores,
        dpi=args.dpi,
        max_gate_spacing=args.max_gate_spacing,
        greybox_out=args.greybox_out,
        greybox_inset=args.greybox_inset,
        greybox_min_lot_area_frac=args.greybox_min_lot_area_frac,
        greybox_meters_per_unit=args.meters_per_unit,
    )

    print("\n--- Hybrid stage 3 summary ---")
    print(f"  total_target:    {manifest['total_target']}")
    print(f"  M (district):    {manifest['M']}")
    print(f"  macro-blocks:    {manifest['n_macro_blocks']}")
    print(f"  blocks failed:   {manifest['n_blocks_failed']}")
    print(f"  total districts: {manifest['total_districts']}")
    print(f"  Chen streets:    {manifest['total_chen_streets']}")
    conn = manifest["connectivity"]
    print(f"  T-junctions:     {conn['n_tjunctions']} (graph: {conn['graph']})")
    print(
        f"  densification:   enabled={conn['densification_enabled']} "
        f"max_gate_spacing={conn['max_gate_spacing']} "
        f"n_connectors_added={conn['n_connectors_added']}"
    )
    print(f"  runtime:         {manifest['runtime_seconds']}s")
    print(f"\nOutputs in: {args.out_dir}")
    if args.greybox_out is not None:
        print(f"Greybox export in: {args.greybox_out}")
    print("Done.")


if __name__ == "__main__":
    main()
