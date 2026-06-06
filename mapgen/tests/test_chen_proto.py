from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

from shapely import LineString, Polygon

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mapgen.chen_proto import (  # noqa: E402
    ParcelRec,
    StreetRec,
    _chen_irregularity,
    _street_graph_from_parcels,
    _street_topology_metrics,
    build_chen_proto,
    generate_chen_layout,
)


def test_chen_irregularity_matches_simple_shapes() -> None:
    square = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    rectangle = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
    notched = Polygon(
        [(0, 0), (20, 0), (20, 10), (12, 10), (12, 6), (8, 6), (8, 10), (0, 10)]
    )

    assert math.isclose(_chen_irregularity(square), 0.0, abs_tol=1e-12)
    assert _chen_irregularity(rectangle) > 0.0
    assert _chen_irregularity(notched) > _chen_irregularity(rectangle)


def test_street_topology_metrics_detect_components_and_angles() -> None:
    nodes = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (20.0, 20.0), (30.0, 20.0)]
    street_edges = {(0, 1), (1, 2), (3, 4)}
    streets = [
        StreetRec(1, "street_access", LineString([nodes[0], nodes[1], nodes[2]])),
        StreetRec(2, "street_access", LineString([nodes[3], nodes[4]])),
    ]

    boundary = Polygon([(0, 0), (30, 0), (30, 30), (0, 30)])
    metrics = _street_topology_metrics(
        nodes, street_edges, streets, boundary, target_area=100.0
    )

    assert metrics["street_graph_component_count"] == 2
    assert metrics["street_graph_dead_end_count"] == 4
    assert metrics["street_isolated_edge_count"] == 1
    assert metrics["street_junction_angle_deviation_p95_deg"] == 0.0


def test_street_generation_reports_unreachable_group_repair() -> None:
    parcels: list[ParcelRec] = []
    parcel_id = 1
    for y in range(3):
        for x in range(3):
            parcels.append(
                ParcelRec(
                    parcel_id=parcel_id,
                    block_id=parcel_id,
                    geom=Polygon(
                        [
                            (x * 10, y * 10),
                            ((x + 1) * 10, y * 10),
                            ((x + 1) * 10, (y + 1) * 10),
                            (x * 10, (y + 1) * 10),
                        ]
                    ),
                )
            )
            parcel_id += 1
    boundary = Polygon([(0, 0), (30, 0), (30, 30), (0, 30)])

    streets, metrics = _street_graph_from_parcels(parcels, boundary, target_area=100.0)

    assert streets
    assert metrics["unreachable_parcel_count"] == 0
    assert metrics["unreachable_group_count"] >= 1
    assert metrics["unreachable_group_max_size"] >= 1
    assert metrics["street_graph_component_count"] == 1
    assert metrics["parcel_access_ratio_below_tau_count"] == 0
    assert metrics["access_non_progress_count"] == 0
    assert (
        metrics["access_i_shape_count"]
        + metrics["access_l_shape_count"]
        + metrics["access_greedy_fallback_count"]
        == metrics["street_access_path_count"]
    )


def test_chen_layout_hits_target_and_frontage() -> None:
    streets, parcels, guides, metrics = generate_chen_layout(
        width=220.0,
        height=150.0,
        parcel_count=96,
        seed=7,
        flow_strength=0.04,
    )
    assert streets
    assert guides
    assert len(parcels) == 96
    assert metrics["hard_metrics_pass"]
    assert metrics["frontage_rate"] == 1.0
    assert metrics["parcel_overlap_count"] == 0


def test_chen_proto_writes_artifacts(tmp_path: Path) -> None:
    out_dir = tmp_path / "chen"
    metrics = build_chen_proto(
        argparse.Namespace(
            out_dir=out_dir,
            width=180.0,
            height=120.0,
            parcel_count=64,
            seed=11,
            flow_strength=0.035,
            boundary="rectangle",
        )
    )
    assert metrics["hard_metrics_pass"]
    assert (out_dir / "proto_manifest.json").exists()
    assert (out_dir / "layout_metrics.json").exists()
    assert (out_dir / "boundary.geojson").exists()
    assert (out_dir / "roads.geojson").exists()
    assert (out_dir / "mesh_guides.geojson").exists()
    assert (out_dir / "parcels.geojson").exists()
    assert (out_dir / "faces.geojson").exists()
    assert (out_dir / "chen_layout.png").read_bytes().startswith(b"\x89PNG")
    assert (out_dir / "chen_layout.svg").read_text().startswith("<svg")


def test_chen_proto_irregular_boundary_stress(tmp_path: Path) -> None:
    out_dir = tmp_path / "chen_island"
    metrics = build_chen_proto(
        argparse.Namespace(
            out_dir=out_dir,
            width=180.0,
            height=120.0,
            parcel_count=80,
            seed=19,
            flow_strength=0.04,
            boundary="island",
        )
    )
    assert metrics["boundary"] == "island"
    assert metrics["parcel_count"] >= 56
    assert metrics["parcel_overlap_count"] == 0
    assert metrics["mesh_guide_count"] > 0
    assert (out_dir / "boundary.geojson").exists()
    assert (out_dir / "mesh_guides.geojson").exists()


def test_chen_triangle_uses_crossfield_streamline_splits() -> None:
    streets, parcels, guides, metrics = generate_chen_layout(
        width=220.0,
        height=150.0,
        parcel_count=90,
        seed=23,
        flow_strength=0.04,
        boundary="triangle",
    )
    assert len(parcels) == 90
    assert metrics["hard_metrics_pass"]
    assert metrics["boundary"] == "triangle"
    assert metrics["split_curve_count"] == 89
    assert metrics["coverage_rate"] > 0.99
    assert "near_parallel_split_count" in metrics
    assert "short_edge_count_before" in metrics
    assert "short_edge_count_after" in metrics
    assert "geometry_optimization_applied" in metrics
    assert "geometry_opt_width_p01_before" in metrics
    assert "geometry_opt_parcel_wrinkles_before" in metrics
    assert "geometry_opt_parcel_wrinkles_after" in metrics
    if metrics["geometry_optimization_applied"]:
        assert (
            metrics["geometry_opt_parcel_wrinkles_after"]
            <= metrics["geometry_opt_parcel_wrinkles_before"] * 1.05
        )
    assert "planar_face_count" in metrics
    assert "planar_nonmanifold_edge_count" in metrics
    assert metrics["split_weight_regularity"] > metrics["split_weight_access"]
    assert metrics["unreachable_parcel_count"] == 0
    assert metrics["street_access_path_count"] > 0
    assert "street_graph_component_count" in metrics
    assert "street_short_chain_count" in metrics
    assert "street_streamline_chain_count" in metrics
    assert "street_fragmented_chain_ratio" in metrics
    assert "street_long_chain_count" in metrics
    assert metrics["street_graph_component_count"] == 1
    assert metrics["street_streamline_chain_count"] > 0
    assert metrics["parcel_access_ratio_below_tau_count"] == 0
    assert metrics["access_non_progress_count"] == 0
    assert (
        metrics["seed_street_used_edge_count"]
        < metrics["seed_street_candidate_edge_count"]
    )
    assert metrics["seed_boundary_hug_reject_count"] > 0
    assert metrics["street_graph_dead_end_count"] <= 24
    assert metrics["boundary_parallel_street_ratio"] < 0.22
    assert "boundary_hugging_street_ratio" in metrics
    assert "street_wrinkle_turn_count" in metrics
    assert "seed_boundary_hug_reject_count" in metrics
    assert metrics["perimeter_street_count"] == 1
    assert metrics["perimeter_street_is_ring"]
    assert math.isclose(metrics["perimeter_street_length_ratio"], 1.0, rel_tol=1e-9)
    assert metrics["perimeter_street_hausdorff"] < 1e-7
    assert (
        metrics["global_street_cleanup_wrinkles_after"]
        <= metrics["global_street_cleanup_wrinkles_before"]
    )
    assert any(s.kind == "perimeter" and s.geom.is_ring for s in streets)
    assert any(s.kind == "street_access" for s in streets)
    assert any(g.kind == "cross_field" for g in guides)
    assert any(g.kind == "streamline_split" for g in guides)
