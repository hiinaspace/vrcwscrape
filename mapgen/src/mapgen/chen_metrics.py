"""Chen 2024 Table 1 metrics and paper comparator.

This module computes the exact metric columns from Chen 2024 Table 1 (p.11) and
provides comparator helpers for regression testing against the paper's published
reference rows for Rect, Triangle, and Ellipse boundaries.

Paper references:
- Chen 2024 Section 4, Table 1, Fig. 4d (street classification)
- Chen 2024 Eq. 1 (irregularity), Sec. 4.1 (approx polygon / 135-degree rule)
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
from shapely import Polygon

from mapgen.chen_core import (
    ChenLayout,
    EdgeKey,
    StreetPath,
    chen_irregularity,
    normalized_edge,
    vertex_path_length,
)

# ---------------------------------------------------------------------------
# Paper Table 1 reference rows (verified from chen2024_urban_modeling.pdf p.11)
# Columns not directly in Table 1 (land_perimeter_km, land_area_ha) are
# included as provided context for normalization; the comparator normalises
# street lengths by land_perimeter_km.
# ---------------------------------------------------------------------------

#: Hardcoded paper Table 1 reference rows keyed by shape name.
#: Values are the exact numbers printed in the paper (1200dpi verification per
#: the plan document).  Units match Table 1 caption: km / ha / plain count /
#: degrees.
PAPER_TABLE1_ROWS: dict[str, dict[str, float | int | None]] = {
    "rect": {
        "land_perimeter_km": 6.085,
        "land_area_ha": 217.603,
        "min_parcel_area_ha": 1,
        "parcel_tri_count": 0,
        "parcel_quad_count": 116,
        "parcel_pent_count": 0,
        "parcel_hex_count": 0,
        "parcel_hept_count": 0,
        "parcel_other_count": 0,
        "parcel_total_count": 116,
        "irregularity_min": 0.0000,
        "irregularity_max": 0.0754,
        "irregularity_avg": 0.0097,
        "street_count": 9,
        "street_junction_count": 19,
        "street_end_count": 0,
        "street_length_total_km": 7.574,
        "street_length_avg_km": 0.842,
        "junction_angle_dev_from_90_avg": 0.42,
        "max_hierarchical_level": 7,
    },
    "triangle": {
        "land_perimeter_km": 5.042,
        "land_area_ha": 121.194,
        "min_parcel_area_ha": 1,
        "parcel_tri_count": 1,
        "parcel_quad_count": 102,
        "parcel_pent_count": 0,
        "parcel_hex_count": 0,
        "parcel_hept_count": 0,
        "parcel_other_count": 0,
        "parcel_total_count": 103,
        "irregularity_min": 0.0001,
        "irregularity_max": 0.1378,
        "irregularity_avg": 0.0274,
        "street_count": 8,
        "street_junction_count": 18,
        "street_end_count": 0,
        "street_length_total_km": 5.421,
        "street_length_avg_km": 0.678,
        "junction_angle_dev_from_90_avg": 2.94,
        "max_hierarchical_level": 8,
    },
    "ellipse": {
        "land_perimeter_km": 5.146,
        "land_area_ha": 195.3784,
        "min_parcel_area_ha": 1,
        "parcel_tri_count": 4,
        "parcel_quad_count": 124,
        "parcel_pent_count": 0,
        "parcel_hex_count": 0,
        "parcel_hept_count": 0,
        "parcel_other_count": 0,
        "parcel_total_count": 128,
        "irregularity_min": 0.0004,
        "irregularity_max": 0.1591,
        "irregularity_avg": 0.0370,
        "street_count": 9,
        "street_junction_count": 25,
        "street_end_count": 0,
        "street_length_total_km": 9.291,
        "street_length_avg_km": 1.032,
        "junction_angle_dev_from_90_avg": 7.04,
        "max_hierarchical_level": 8,
    },
}


# ---------------------------------------------------------------------------
# Boundary-street exclusion helpers
# ---------------------------------------------------------------------------


def _boundary_vertex_ids(layout: ChenLayout) -> frozenset[int]:
    """Return the set of on-boundary mesh vertex ids."""
    return frozenset(
        vertex_id
        for vertex_id, vertex in layout.mesh.vertices.items()
        if vertex.on_boundary
    )


def _street_is_boundary_loop(street: StreetPath, boundary_ids: frozenset[int]) -> bool:
    """Return True when every node of this street lies on the mesh boundary.

    Chen Table 1 caption: "excluding the user-specified street at the input
    land boundary".  A street composed entirely of boundary-ring vertices is
    the user-specified boundary loop.  Interior streets that merely touch the
    boundary at endpoints share boundary nodes but do not qualify because their
    intermediate nodes are interior.

    Rule: a street is a boundary-loop street iff every node in its node
    sequence is an on-boundary vertex.
    """
    return all(node in boundary_ids for node in street.nodes)


def _non_boundary_streets(layout: ChenLayout) -> list[StreetPath]:
    """Return streets excluding the boundary-loop street(s)."""
    boundary_ids = _boundary_vertex_ids(layout)
    return [
        street
        for street in layout.street_graph.streets
        if not _street_is_boundary_loop(street, boundary_ids)
    ]


# ---------------------------------------------------------------------------
# Street-length helpers
# ---------------------------------------------------------------------------


def _street_length(street: StreetPath, layout: ChenLayout) -> float:
    """Return the geometric length of a street in world units."""
    return sum(_edge_geometric_length(edge, layout) for edge in street.edges)


def _edge_geometric_length(edge: EdgeKey, layout: ChenLayout) -> float:
    """Return the geometric length of a corner-graph edge (may have sub-nodes)."""
    # Prefer the corner-graph path (which may contain intermediate mesh nodes)
    # so curved sides are measured accurately.
    path = layout.corner_graph.edge_paths.get(normalized_edge(*edge))
    if path is not None and len(path) >= 2:
        return float(vertex_path_length(layout.mesh, path))
    # Fallback: direct Euclidean distance between the two endpoint vertices.
    a, b = edge
    pa = layout.mesh.vertices[a].point
    pb = layout.mesh.vertices[b].point
    return math.hypot(pb[0] - pa[0], pb[1] - pa[1])


# ---------------------------------------------------------------------------
# Junction-angle deviation helpers
# ---------------------------------------------------------------------------


def _junction_angle_devs(layout: ChenLayout, streets: list[StreetPath]) -> list[float]:
    """Return per-junction-pair |angle - 90°| deviations in degrees.

    For each junction node (shared by >= 2 streets), for each pair of streets
    meeting there, compute the angle between the two street directions at that
    node and take |angle - 90°|.

    Semantic choices:
    - "Street direction at a junction" = direction from the junction node to
      its immediate neighbour along that street.  This matches the paper's
      intent of measuring grid-alignment at intersections.
    - For each unordered pair of streets at a junction, we compute one angle
      deviation.  We average over all junction-pair observations.
    - Only non-boundary streets are included (same exclusion as Table 1).
    - Junctions involving only one non-boundary street are skipped.
    """
    # Build a map: junction_node -> list of (direction_angle_rad,)
    vertex_map = layout.mesh.vertices

    # For each non-boundary street, note the outgoing direction from each
    # endpoint/junction node.
    node_to_directions: dict[int, list[float]] = {}
    for street in streets:
        nodes = street.nodes
        if len(nodes) < 2:
            continue
        # Direction from start node toward interior
        start_node = nodes[0]
        next_node = nodes[1]
        p0 = vertex_map[start_node].point
        p1 = vertex_map[next_node].point
        angle_start = math.atan2(p1[1] - p0[1], p1[0] - p0[0])
        node_to_directions.setdefault(start_node, []).append(angle_start)

        # Direction from end node toward interior (reversed)
        end_node = nodes[-1]
        prev_node = nodes[-2]
        pe = vertex_map[end_node].point
        pp = vertex_map[prev_node].point
        angle_end = math.atan2(pp[1] - pe[1], pp[0] - pe[0])
        node_to_directions.setdefault(end_node, []).append(angle_end)

        # For interior nodes that happen to be shared (degree-3+ in street
        # network), also record their direction; these are T-junctions where
        # the street passes through but another street also arrives.
        # These are captured already via both streets touching the same node.

    # The junctions dict in StreetGraph includes all nodes shared by >= 2
    # streets.  We use those plus the subset filter on non-boundary streets.
    non_boundary_junction_nodes: set[int] = set()
    for street in streets:
        for node in street.nodes:
            if node in layout.street_graph.junctions:
                non_boundary_junction_nodes.add(node)

    devs: list[float] = []
    for node in sorted(non_boundary_junction_nodes):
        directions = node_to_directions.get(node)
        if not directions or len(directions) < 2:
            continue
        for i in range(len(directions)):
            for j in range(i + 1, len(directions)):
                a0 = directions[i]
                a1 = directions[j]
                diff = abs(math.atan2(math.sin(a1 - a0), math.cos(a1 - a0)))
                # diff is in [0, π]; angle between lines is min(diff, π-diff)
                line_angle = min(diff, math.pi - diff)
                dev_deg = abs(math.degrees(line_angle) - 90.0)
                devs.append(dev_deg)
    return devs


# ---------------------------------------------------------------------------
# Approximate-polygon type counts
# ---------------------------------------------------------------------------


def _parcel_corner_count(parcel_id: int, layout: ChenLayout) -> int:
    """Return the number of corners in the approximate polygon for this parcel."""
    ring = layout.corner_graph.parcel_corner_rings.get(parcel_id)
    if ring is not None:
        return len(ring)
    # Fallback: use approx_points from parcel_approx_points
    approx = layout.parcel_graph.parcel_approx_points.get(parcel_id)
    if approx is not None:
        return len(approx)
    return len(layout.mesh.parcels[parcel_id].ring)


# ---------------------------------------------------------------------------
# Main metric function
# ---------------------------------------------------------------------------


def paper_table1_metrics(
    layout: ChenLayout,
    *,
    boundary: Polygon,
    max_hierarchical_level: int | None = None,
    generation_seconds: float | None = None,
    optimization_seconds: float | None = None,
) -> dict[str, float | int | None]:
    """Compute Chen 2024 Table 1 metric columns for a ChenLayout.

    Returns a dict with the exact column names used in Table 1 (p.11), plus
    passthrough fields for hierarchical level and timing.

    Boundary-street exclusion: any street whose nodes all lie on the mesh
    boundary ring is excluded from street_count, junction counts, and length
    stats.  This matches Table 1 caption: "excluding the user-specified street
    at the input land boundary."

    World units: length metrics are in world units as stored in the mesh
    (typically metres in generated layouts).  The paper uses km; callers that
    want km must divide by 1000.
    """
    parcel_ids = list(layout.mesh.parcels.keys())

    # --- approximate polygon corner counts ---
    tri_count = 0
    quad_count = 0
    pent_count = 0
    hex_count = 0
    hept_count = 0
    other_count = 0
    for parcel_id in parcel_ids:
        n = _parcel_corner_count(parcel_id, layout)
        if n == 3:
            tri_count += 1
        elif n == 4:
            quad_count += 1
        elif n == 5:
            pent_count += 1
        elif n == 6:
            hex_count += 1
        elif n == 7:
            hept_count += 1
        else:
            other_count += 1
    total_count = len(parcel_ids)
    quad_fraction = quad_count / total_count if total_count > 0 else 0.0

    # --- irregularity (Chen Eq. 1) ---
    irregularities = [
        chen_irregularity(layout.mesh.parcels[pid].geom) for pid in parcel_ids
    ]
    irr_min = float(min(irregularities)) if irregularities else 0.0
    irr_max = float(max(irregularities)) if irregularities else 0.0
    irr_avg = float(np.mean(irregularities)) if irregularities else 0.0

    # --- streets (excluding boundary loop) ---
    non_bdy_streets = _non_boundary_streets(layout)
    s_count = len(non_bdy_streets)

    # Junctions: nodes shared by >= 2 streets, but restrict to non-boundary
    # street nodes so boundary-only junctions don't inflate the count.
    non_bdy_nodes: set[int] = {node for s in non_bdy_streets for node in s.nodes}
    junction_count = sum(
        1
        for node, street_ids in layout.street_graph.junctions.items()
        if node in non_bdy_nodes
        # Only count junctions that appear in >= 2 non-boundary streets
        and sum(1 for s in non_bdy_streets if node in s.nodes) >= 2
    )

    # Street ends: degree-1 nodes in the non-boundary-street sub-network that
    # are also NOT on the mesh boundary (i.e., genuine cul-de-sacs / dead-ends).
    # Nodes where a street terminates at the land boundary are not cul-de-sacs
    # — they are entry/exit points.  Only interior dead-ends count.
    boundary_ids = _boundary_vertex_ids(layout)
    non_bdy_edges: set[EdgeKey] = {e for s in non_bdy_streets for e in s.edges}
    adj: dict[int, set[int]] = {}
    for a, b in non_bdy_edges:
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)
    end_count = sum(
        1 for node, nbrs in adj.items() if len(nbrs) == 1 and node not in boundary_ids
    )

    # --- street lengths ---
    lengths = [_street_length(s, layout) for s in non_bdy_streets]
    length_total = sum(lengths)
    length_avg = length_total / s_count if s_count > 0 else 0.0

    # --- junction angle deviation from 90° ---
    devs = _junction_angle_devs(layout, non_bdy_streets)
    junc_angle_dev = float(np.mean(devs)) if devs else 0.0

    return {
        # approximate polygon type counts
        "parcel_tri_count": tri_count,
        "parcel_quad_count": quad_count,
        "parcel_pent_count": pent_count,
        "parcel_hex_count": hex_count,
        "parcel_hept_count": hept_count,
        "parcel_other_count": other_count,
        "parcel_total_count": total_count,
        "parcel_quad_fraction": quad_fraction,
        # irregularity (Chen Eq. 1)
        "irregularity_min": irr_min,
        "irregularity_max": irr_max,
        "irregularity_avg": irr_avg,
        # streets (boundary-loop excluded)
        "street_count": s_count,
        "street_junction_count": junction_count,
        "street_end_count": end_count,
        # lengths in world units
        "street_length_total": length_total,
        "street_length_avg": length_avg,
        # angle quality
        "junction_angle_dev_from_90_avg": junc_angle_dev,
        # passthrough
        "max_hierarchical_level": max_hierarchical_level,
        "generation_seconds": generation_seconds,
        "optimization_seconds": optimization_seconds,
    }


# ---------------------------------------------------------------------------
# Paper comparator
# ---------------------------------------------------------------------------

#: Normalised quantity extractors for compare_to_paper.
#: Each entry: (key_in_our_metrics, computation_fn, paper_row_key)
#: computation_fn(our_metrics, paper_row) -> float | None


def _quad_fraction(our: dict, paper: dict) -> float | None:
    total = our.get("parcel_total_count", 0)
    if not total:
        return None
    return float(our["parcel_quad_count"]) / float(total)


def _paper_quad_fraction(paper: dict) -> float | None:
    total = paper.get("parcel_total_count", 0)
    if not total:
        return None
    quad = paper.get("parcel_quad_count", 0)
    return float(quad) / float(total)  # type: ignore[arg-type]


def compare_to_paper(metrics: dict[str, Any], shape: str) -> dict[str, Any]:
    """Compare our Table 1 metrics against the paper's published reference row.

    Compares only dimensionless/normalized quantities so world-unit scale
    differences do not inflate deltas.

    Returns a dict keyed by quantity name, each value being:
        {"ours": ..., "paper": ..., "delta": ...}

    ``shape`` must be one of "rect", "triangle", "ellipse".
    """
    if shape not in PAPER_TABLE1_ROWS:
        raise ValueError(f"Unknown shape {shape!r}; known: {sorted(PAPER_TABLE1_ROWS)}")
    paper = PAPER_TABLE1_ROWS[shape]
    result: dict[str, Any] = {}

    # -- quad fraction --
    our_total = metrics.get("parcel_total_count", 0) or 0
    our_quad = metrics.get("parcel_quad_count", 0) or 0
    ours_qf = (float(our_quad) / float(our_total)) if our_total else None
    paper_qf = _paper_quad_fraction(paper)
    result["quad_fraction"] = {
        "ours": ours_qf,
        "paper": paper_qf,
        "delta": (
            (ours_qf - paper_qf)
            if (ours_qf is not None and paper_qf is not None)
            else None
        ),
    }

    # -- tri / pent / hex fractions --
    for poly_type in ("tri", "pent", "hex", "hept"):
        our_cnt = metrics.get(f"parcel_{poly_type}_count", 0) or 0
        paper_cnt = paper.get(f"parcel_{poly_type}_count", 0) or 0
        ours_f = (float(our_cnt) / float(our_total)) if our_total else None
        paper_f = float(paper_cnt) / float(paper.get("parcel_total_count", 1) or 1)
        result[f"{poly_type}_fraction"] = {
            "ours": ours_f,
            "paper": paper_f,
            "delta": (ours_f - paper_f) if ours_f is not None else None,
        }

    # -- irregularity stats --
    for stat in ("min", "max", "avg"):
        ours_irr = metrics.get(f"irregularity_{stat}")
        paper_irr = paper.get(f"irregularity_{stat}")
        result[f"irregularity_{stat}"] = {
            "ours": ours_irr,
            "paper": paper_irr,
            "delta": (float(ours_irr) - float(paper_irr))
            if (ours_irr is not None and paper_irr is not None)
            else None,
        }

    # -- junction angle deviation from 90° --
    ours_jad = metrics.get("junction_angle_dev_from_90_avg")
    paper_jad = paper.get("junction_angle_dev_from_90_avg")
    result["junction_angle_dev_from_90_avg"] = {
        "ours": ours_jad,
        "paper": paper_jad,
        "delta": (float(ours_jad) - float(paper_jad))
        if (ours_jad is not None and paper_jad is not None)
        else None,
    }

    # -- streets per parcel --
    our_streets = metrics.get("street_count", 0) or 0
    ours_spp = (float(our_streets) / float(our_total)) if our_total else None
    paper_streets = paper.get("street_count", 0) or 0
    paper_total = paper.get("parcel_total_count", 1) or 1
    paper_spp = float(paper_streets) / float(paper_total)  # type: ignore[arg-type]
    result["streets_per_parcel"] = {
        "ours": ours_spp,
        "paper": paper_spp,
        "delta": (ours_spp - paper_spp) if ours_spp is not None else None,
    }

    # -- street length total normalized by boundary perimeter --
    # We accept street_length_total_km (paper) or street_length_total (world units)
    # for our metrics, and use land_perimeter_km from the paper row to normalize
    # the paper value.  For ours we normalize by the same ratio:
    # paper_norm = paper_km / perim_km.  For ours: if we know the boundary perimeter
    # in world units, we'd divide.  Since we don't require it here (comparator only
    # gets metrics dict and shape), we compute a dimensionless ratio using
    # street_count as a proxy for "total streets" and note the semantic choice.
    # Instead, we normalize street_length_total by parcel_total_count, yielding
    # avg-length-per-parcel as a dimensionless proxy (units cancel when comparing
    # paper vs ours IF both use the same world-unit convention, which they may not
    # for generated vs paper KM layouts).
    # For paper we use paper's own street_length_total_km / land_perimeter_km.
    paper_perim_km = paper.get("land_perimeter_km")
    paper_len_km = paper.get("street_length_total_km")
    paper_len_norm = (
        (float(paper_len_km) / float(paper_perim_km))  # type: ignore[arg-type]
        if (paper_perim_km and paper_len_km)
        else None
    )
    # For ours we can only produce a meaningful number if the caller passed
    # boundary_perimeter_world in metrics; otherwise leave as None.
    ours_perim = metrics.get("boundary_perimeter_world")
    our_len = metrics.get("street_length_total")
    ours_len_norm = (
        (float(our_len) / float(ours_perim))
        if (ours_perim and our_len is not None)
        else None
    )
    result["street_length_total_norm_by_perimeter"] = {
        "ours": ours_len_norm,
        "paper": paper_len_norm,
        "delta": (ours_len_norm - paper_len_norm)
        if (ours_len_norm is not None and paper_len_norm is not None)
        else None,
    }

    return result


# ---------------------------------------------------------------------------
# Paper fidelity bands
# ---------------------------------------------------------------------------


def paper_fidelity_bands(
    metrics: dict[str, Any],
    shape: str,
    *,
    cul_de_sac_avoidance: bool = True,
) -> dict[str, bool]:
    """Return loose pass-bands for quick regression gates.

    These thresholds are intentionally permissive — they are meant to catch
    gross failures (majority non-quads, high irregularity, many cul-de-sacs)
    rather than enforcing paper-exact values.

    Args:
        metrics: output of paper_table1_metrics()
        shape: one of "rect", "triangle", "ellipse" (used for future
            shape-specific thresholds; currently all shapes use the same bands)
        cul_de_sac_avoidance: when True, the street_end_count == 0 band is
            active.  When False (cul-de-sac avoidance disabled), the band is
            skipped and returns True unconditionally.

    Returns:
        dict[band_name, passed]
    """
    total = metrics.get("parcel_total_count", 0) or 0
    quad = metrics.get("parcel_quad_count", 0) or 0
    quad_fraction = float(quad) / float(total) if total else 0.0

    bands: dict[str, bool] = {
        "quad_fraction_gt_0.8": quad_fraction > 0.8,
        "irregularity_avg_lt_0.08": (metrics.get("irregularity_avg") or 0.0) < 0.08,
        "junction_angle_dev_lt_15": (
            (metrics.get("junction_angle_dev_from_90_avg") or 0.0) < 15.0
        ),
    }
    if cul_de_sac_avoidance:
        bands["street_end_count_eq_0"] = (metrics.get("street_end_count") or 0) == 0
    else:
        bands["street_end_count_eq_0"] = True  # band not active

    return bands
