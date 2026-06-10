from __future__ import annotations

import importlib.util
import json
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from shapely import LineString, Polygon

from mapgen.chen_artifacts import ARTIFACT_FILES, write_strict_chen_artifacts
from mapgen.chen_core import (
    build_chen_layout,
    normalized_edge,
    parcel_mesh_from_polygons,
)
from mapgen.chen_generate import (
    STREAMLINE_MODE_YANG_B_FIELD,
    STREAMLINE_MODE_YANG_D_FIELD,
    BoundarySpec,
    generate_layout_for_boundary,
    generate_named_layout,
)

OPTIMIZATION_REGULARITY_KEYS = (
    "optimization_regularity_projection_kind",
    "optimization_regularity_projected_parcel_count_before",
    "optimization_regularity_projected_parcel_count_after",
    "optimization_regularity_skipped_parcel_count_before",
    "optimization_regularity_skipped_parcel_count_after",
    "optimization_regularity_skipped_by_reason_before",
    "optimization_regularity_skipped_by_reason_after",
    "optimization_regularity_projection_equation_count",
    "optimization_regularity_projection_residual_before",
    "optimization_regularity_projection_residual_after",
    "optimization_regularity_projection_target_displacement_rms_before",
    "optimization_regularity_projection_target_displacement_rms_after",
    "optimization_regularity_projection_target_displacement_max_before",
    "optimization_regularity_projection_target_displacement_max_after",
)


@dataclass(frozen=True)
class GeneratedStub:
    name: str
    boundary: Polygon
    layout: Any
    split_lines: list[LineString]
    metrics: dict[str, Any]


def _generated_stub(name: str = "square") -> GeneratedStub:
    boundary = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
    left = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    right = Polygon([(10, 0), (20, 0), (20, 10), (10, 10)])
    mesh = parcel_mesh_from_polygons([(1, left), (2, right)], boundary=boundary)
    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in mesh.vertices.values()
    }
    street_edges = {
        normalized_edge(point_to_node[(0.0, 0.0)], point_to_node[(10.0, 0.0)]),
        normalized_edge(point_to_node[(10.0, 0.0)], point_to_node[(20.0, 0.0)]),
        normalized_edge(point_to_node[(20.0, 0.0)], point_to_node[(20.0, 10.0)]),
        normalized_edge(point_to_node[(20.0, 10.0)], point_to_node[(10.0, 10.0)]),
        normalized_edge(point_to_node[(10.0, 10.0)], point_to_node[(0.0, 10.0)]),
        normalized_edge(point_to_node[(0.0, 10.0)], point_to_node[(0.0, 0.0)]),
    }
    return GeneratedStub(
        name=name,
        boundary=boundary,
        layout=build_chen_layout(mesh, street_edges),
        split_lines=[LineString([(10, 0), (10, 10)])],
        metrics={"source": "test"},
    )


def test_write_strict_chen_artifacts_writes_visual_geojson_and_manifest(
    tmp_path: Path,
) -> None:
    manifest = write_strict_chen_artifacts(_generated_stub(), tmp_path)

    for filename in ARTIFACT_FILES.values():
        assert (tmp_path / filename).exists()
    assert (tmp_path / "chen_strict_layout.png").read_bytes().startswith(b"\x89PNG")
    assert (tmp_path / "chen_strict_layout.svg").read_text().startswith("<svg")
    assert manifest["layout"] == "chen-strict"
    assert manifest["summary"]["parcel_count"] == 2
    assert manifest["summary"]["street_edge_count"] == 6
    assert manifest["summary"]["corner_graph_edge_count"] == 7
    assert manifest["summary"]["street_edge_density"] == 6 / 7
    assert manifest["summary"]["street_topology_reachability_pass"] is False
    assert manifest["summary"]["deprecated_chen_street_generation_pass_alias"] is False
    assert "chen_street_generation_pass" not in manifest["summary"]
    assert manifest["summary"]["street_selection_mode"] == "unknown"
    assert manifest["summary"]["chen_fig7_short_edge_cleanup_stage"] == "unknown"
    assert manifest["summary"]["chen_fig7_short_edge_cleanup_applied"] is False
    assert manifest["summary"]["chen_fig7_short_edge_cleanup_midpoint_merge_count"] == 0
    assert (
        manifest["summary"][
            "chen_fig7_short_edge_cleanup_failed_fig7_motif_ineligible_"
            "non_candidate_parcel_ring_after_merge_count"
        ]
        == 0
    )
    assert (
        manifest["summary"][
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_stage"
        ]
        == "unknown"
    )
    assert (
        manifest["summary"][
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_count"
        ]
        == 0
    )
    assert (
        manifest["summary"][
            "chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count"
        ]
        == 0
    )
    assert (
        manifest["summary"][
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail"
        ]
        == {}
    )
    assert (
        manifest["summary"][
            "chen_fig7_short_edge_cleanup_boundary_projected_merge_count"
        ]
        == 0
    )
    assert manifest["summary"]["chen_fig7_short_edge_cleanup_merge_point_modes"] == []
    assert manifest["summary"]["chen_fig7_short_shared_edge_candidate_count"] == 0
    assert manifest["summary"]["chen_fig7_unexplained_interior_t_junction_count"] == 0
    assert (
        manifest["summary"][
            "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count"
        ]
        == 0
    )
    assert (
        manifest["summary"][
            "chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count"
        ]
        == 0
    )
    assert (
        manifest["summary"]["chen_fig7_unexplained_t_junction_split_endpoint_count"]
        is None
    )
    assert (
        manifest["summary"]["chen_fig7_unexplained_t_junction_lies_on_split_line_count"]
        is None
    )
    assert (
        manifest["summary"]["chen_fig7_unexplained_t_junction_split_unknown_count"]
        is None
    )
    assert manifest["summary"]["optimization_stage"] == "unknown"
    assert manifest["summary"]["optimization_regularity_projection_kind"] == "disabled"
    assert (
        manifest["summary"]["optimization_regularity_projected_parcel_count_before"]
        == 0
    )
    assert (
        manifest["summary"]["optimization_regularity_projected_parcel_count_after"] == 0
    )
    assert (
        manifest["summary"]["optimization_regularity_skipped_parcel_count_before"] == 0
    )
    assert (
        manifest["summary"]["optimization_regularity_skipped_parcel_count_after"] == 0
    )
    assert manifest["summary"]["optimization_regularity_skipped_by_reason_before"] == {}
    assert manifest["summary"]["optimization_regularity_skipped_by_reason_after"] == {}
    assert manifest["summary"]["optimization_regularity_projection_equation_count"] == 0
    assert (
        manifest["summary"]["optimization_regularity_projection_residual_before"] == 0.0
    )
    assert (
        manifest["summary"]["optimization_regularity_projection_residual_after"] == 0.0
    )
    assert (
        manifest["summary"][
            "optimization_regularity_projection_target_displacement_rms_before"
        ]
        == 0.0
    )
    assert (
        manifest["summary"][
            "optimization_regularity_projection_target_displacement_rms_after"
        ]
        == 0.0
    )
    assert (
        manifest["summary"][
            "optimization_regularity_projection_target_displacement_max_before"
        ]
        == 0.0
    )
    assert (
        manifest["summary"][
            "optimization_regularity_projection_target_displacement_max_after"
        ]
        == 0.0
    )
    assert manifest["files"]["svg"] == "chen_strict_layout.svg"

    metrics = json.loads((tmp_path / "layout_metrics.json").read_text())
    assert metrics["source"] == "test"
    assert metrics["strict_invariants"]["paper_invariant_pass"]

    boundary = json.loads((tmp_path / "boundary.geojson").read_text())
    parcels = json.loads((tmp_path / "parcels.geojson").read_text())
    streets = json.loads((tmp_path / "streets.geojson").read_text())
    split_lines = json.loads((tmp_path / "split_lines.geojson").read_text())
    assert boundary["type"] == "FeatureCollection"
    assert len(boundary["features"]) == 1
    assert len(parcels["features"]) == 2
    assert len(streets["features"]) == 4
    assert len(split_lines["features"]) == 1


@pytest.mark.slow
def test_oval_artifacts_preserve_curved_split_and_street_polylines(
    tmp_path: Path,
) -> None:
    generated = generate_named_layout("oval", parcel_count=12, seed=0)

    manifest = write_strict_chen_artifacts(generated, tmp_path)

    # Section 4 driver stage + boundary contour fidelity for the oval preset.
    assert manifest["summary"]["generation_stage"] == (
        "chen_section4_hierarchical_co_generation_v2"
    )
    assert (
        manifest["summary"]["boundary_contour_fidelity_stage"]
        == "preset_ellipse_radial_error_v0"
    )
    assert manifest["summary"]["oval_boundary_vertex_count"] == 32
    assert manifest["summary"]["oval_boundary_normalized_radial_error_max"] <= 1e-12
    assert manifest["summary"]["oval_boundary_radial_error_max"] <= 1e-10
    assert manifest["summary"]["street_topology_reachability_pass"]
    assert manifest["summary"]["corner_graph_edge_count"] > 0
    assert manifest["summary"]["street_edge_density"] > 0.0
    assert manifest["summary"]["optimization_applied"]
    assert manifest["summary"]["optimization_geometry_changed"]
    assert manifest["summary"]["optimization_accepted_iteration_count"] > 0
    assert (
        manifest["summary"]["optimization_energy_after"]
        <= manifest["summary"]["optimization_energy_before"]
    )

    metrics = json.loads((tmp_path / "layout_metrics.json").read_text())
    assert metrics["generation_stage"] == (
        "chen_section4_hierarchical_co_generation_v2"
    )
    assert metrics["street_topology_reachability_pass"]
    assert metrics["geometry_valid_pass"]
    assert metrics["paper_invariant_pass"]
    assert metrics["optimization_stage"] == "chen_section_5_shapeop_like_projection_v1"
    assert metrics["optimization_applied"]
    assert metrics["optimization_layout_used"]
    assert metrics["optimization_diagnostics"]["accepted_iteration_count"] > 0
    assert (
        manifest["summary"]["optimization_regularity_projection_kind"]
        == "regular_polygon_similarity_transform_v0"
    )
    assert manifest["summary"]["optimization_regularity_projection_equation_count"] > 0
    assert (
        manifest["summary"]["optimization_regularity_projected_parcel_count_before"] > 0
    )
    assert (
        manifest["summary"]["optimization_regularity_projection_residual_before"] >= 0
    )
    assert manifest["summary"]["optimization_regularity_projection_residual_after"] >= 0
    assert (
        manifest["summary"][
            "optimization_regularity_projection_target_displacement_rms_before"
        ]
        >= 0.0
    )
    assert (
        manifest["summary"][
            "optimization_regularity_projection_target_displacement_rms_after"
        ]
        >= 0.0
    )
    assert (
        manifest["summary"][
            "optimization_regularity_projection_target_displacement_max_before"
        ]
        > 0.0
    )
    assert (
        manifest["summary"][
            "optimization_regularity_projection_target_displacement_max_after"
        ]
        > 0.0
    )
    for key in OPTIMIZATION_REGULARITY_KEYS:
        assert manifest["summary"][key] == generated.metrics[key]
        assert metrics[key] == generated.metrics[key]
    assert (
        manifest["summary"]["optimization_regularity_projection_kind"]
        == metrics["optimization_diagnostics"]["regularity_projection_kind"]
    )
    assert (
        manifest["summary"]["optimization_regularity_projection_equation_count"]
        == metrics["optimization_diagnostics"]["regularity_projection_equation_count"]
    )
    assert (
        manifest["summary"]["optimization_regularity_skipped_by_reason_before"]
        == metrics["optimization_diagnostics"]["regularity_skipped_by_reason_before"]
    )

    split_lines = json.loads((tmp_path / "split_lines.geojson").read_text())
    assert any(
        feature["properties"]["is_curved"] and feature["properties"]["point_count"] > 2
        for feature in split_lines["features"]
    )

    streets = json.loads((tmp_path / "streets.geojson").read_text())
    assert any(
        feature["geometry"]["type"] == "LineString"
        and len(feature["geometry"]["coordinates"]) > 2
        for feature in streets["features"]
    )


@pytest.mark.slow
def test_rectangle_streets_are_axis_aligned(
    tmp_path: Path,
) -> None:
    """An axis-aligned rectangle must produce axis-aligned streets.

    The Section 4 driver scores Q_acce against the real street network and welds
    short edges locally, so the rectangle no longer needs the old cleanup-debt /
    rectangular axis-guard diagnostics (those were removed in slice D). The
    correct expectation is simply that every street segment is axis-aligned.
    """
    generated = generate_layout_for_boundary(
        BoundarySpec(
            "rectangle",
            Polygon([(0, 0), (200, 0), (200, 100), (0, 100)]),
        ),
        parcel_count=20,
        seed=5,
    )

    write_strict_chen_artifacts(generated, tmp_path)

    streets = json.loads((tmp_path / "streets.geojson").read_text())
    tol = 1e-6
    for feature in streets["features"]:
        coords = feature["geometry"]["coordinates"]
        for (ax, ay), (bx, by) in zip(coords, coords[1:], strict=False):
            assert abs(ax - bx) <= tol or abs(ay - by) <= tol, (
                f"non-axis-aligned street segment {(ax, ay)}->{(bx, by)}"
            )

    assert generated.metrics["geometry_valid_pass"]
    assert generated.metrics["paper_invariant_pass"]
    assert generated.metrics["street_topology_reachability_pass"]


def test_yang_streamline_artifact_summary_includes_score_diagnostics(
    tmp_path: Path,
) -> None:
    generated = generate_named_layout(
        "oval",
        parcel_count=8,
        seed=0,
        apply_optimization=False,
        streamline_mode=STREAMLINE_MODE_YANG_D_FIELD,
    )

    manifest = write_strict_chen_artifacts(generated, tmp_path)

    # The Section 4 driver records the resolved streamline config used per
    # parcel. The legacy accepted-streamline score diagnostic sprawl was removed
    # with the geometry-only scorer rewrite (slice D).
    summary = manifest["summary"]
    assert summary["streamline_config_mode"] == STREAMLINE_MODE_YANG_D_FIELD
    assert summary["streamline_config_field_mode"] == "yang_d_field"
    assert summary["streamline_config_candidate_seed_mode"] == "yang_mesh_vertices"
    assert summary["streamline_config_score_mode"] == "yang_div_db_ds_ct"

    metrics = json.loads((tmp_path / "layout_metrics.json").read_text())
    assert metrics["streamline_config_mode"] == STREAMLINE_MODE_YANG_D_FIELD
    assert metrics["geometry_valid_pass"]
    assert metrics["paper_invariant_pass"]


def test_artifacts_preserve_fig7_unique_failure_diagnostics(
    tmp_path: Path,
) -> None:
    base = _generated_stub()
    failed_sample = {
        "edge": (1, 2),
        "parcels": (1, 2),
        "failure_reason": "failed_invalid_polygon",
        "failure_detail": "failed_non_simple_ring_after_merge",
        "failure_candidate_signature": {
            "parcel_pair": (1, 2),
            "midpoint": (4.0, 5.0),
            "path_points": ((4.0, 4.0), (4.0, 5.0), (4.0, 6.0)),
        },
        "failure_parcel_id": 2,
        "failure_validity_reason": "Self-intersection[4 5]",
    }
    generated = GeneratedStub(
        name=base.name,
        boundary=base.boundary,
        layout=base.layout,
        split_lines=base.split_lines,
        metrics={
            **base.metrics,
            "chen_fig7_short_edge_cleanup_failed_count": 3,
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_stage": (
                "groups_failed_attempts_by_parcel_pair_midpoint_and_path_geometry_v0"
            ),
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_count": 1,
            "chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count": 2,
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail": {
                "failed_non_simple_ring_after_merge": 1
            },
            (
                "chen_fig7_short_edge_cleanup_failed_fig7_motif_ineligible_"
                "non_candidate_parcel_ring_after_merge_count"
            ): 1,
            "chen_fig7_short_edge_cleanup_failed_samples": [failed_sample],
            "chen_fig7_short_edge_cleanup_failed_samples_by_detail": {
                "failed_non_simple_ring_after_merge": [failed_sample]
            },
        },
    )

    manifest = write_strict_chen_artifacts(generated, tmp_path)

    summary = manifest["summary"]
    assert summary["chen_fig7_short_edge_cleanup_failed_unique_candidate_stage"] == (
        "groups_failed_attempts_by_parcel_pair_midpoint_and_path_geometry_v0"
    )
    assert summary["chen_fig7_short_edge_cleanup_failed_unique_candidate_count"] == 1
    assert summary["chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count"] == 2
    assert summary[
        "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail"
    ] == {"failed_non_simple_ring_after_merge": 1}
    assert (
        summary[
            "chen_fig7_short_edge_cleanup_failed_fig7_motif_ineligible_"
            "non_candidate_parcel_ring_after_merge_count"
        ]
        == 1
    )
    assert summary["chen_fig7_short_edge_cleanup_failed_samples"] == _json_round_trip(
        [failed_sample]
    )
    assert summary["chen_fig7_short_edge_cleanup_failed_samples"][0][
        "failure_candidate_signature"
    ] == {
        "parcel_pair": [1, 2],
        "midpoint": [4.0, 5.0],
        "path_points": [[4.0, 4.0], [4.0, 5.0], [4.0, 6.0]],
    }
    metrics = json.loads((tmp_path / "layout_metrics.json").read_text())
    assert metrics[
        "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail"
    ] == {"failed_non_simple_ring_after_merge": 1}
    assert (
        metrics["chen_fig7_short_edge_cleanup_failed_samples"][0][
            "failure_candidate_signature"
        ]
        == summary["chen_fig7_short_edge_cleanup_failed_samples"][0][
            "failure_candidate_signature"
        ]
    )


@pytest.mark.slow
def test_yang_b_field_artifact_summary_includes_omega_and_mesh_diagnostics(
    tmp_path: Path,
) -> None:
    generated = generate_named_layout(
        "oval",
        parcel_count=8,
        seed=0,
        apply_optimization=False,
        streamline_mode=STREAMLINE_MODE_YANG_B_FIELD,
    )

    manifest = write_strict_chen_artifacts(generated, tmp_path)

    # The Section 4 driver records the resolved streamline config; the legacy
    # accepted-streamline B-field score sprawl and Fig.7 cleanup-debt metrics
    # were removed with the geometry-only scorer / local-weld rewrite (slice D).
    summary = manifest["summary"]
    assert summary["streamline_config_mode"] == STREAMLINE_MODE_YANG_B_FIELD
    assert summary["streamline_config_field_mode"] == "yang_b_field"
    assert summary["streamline_config_candidate_seed_mode"] == "yang_mesh_vertices"
    assert summary["streamline_config_score_mode"] == "yang_div_db_ds_ct"

    metrics = json.loads((tmp_path / "layout_metrics.json").read_text())
    assert metrics["streamline_config_mode"] == STREAMLINE_MODE_YANG_B_FIELD
    assert metrics["geometry_valid_pass"]
    assert metrics["paper_invariant_pass"]
    assert metrics["unreachable_parcel_count"] == 0


def _json_round_trip(value: Any) -> Any:
    return json.loads(json.dumps(value))


def test_strict_shape_runner_uses_expected_generation_interface(
    tmp_path: Path, monkeypatch: Any
) -> None:
    calls: list[tuple[str, int, int, float, float, str]] = []

    def generate_named_layout(
        name: str,
        parcel_count: int = 48,
        seed: int = 0,
        width: float = 180.0,
        height: float = 140.0,
        streamline_mode: str = "baseline",
    ) -> GeneratedStub:
        calls.append((name, parcel_count, seed, width, height, streamline_mode))
        return _generated_stub(name)

    fake_module = types.ModuleType("mapgen.chen_generate")
    fake_module.generate_named_layout = generate_named_layout
    monkeypatch.setitem(sys.modules, "mapgen.chen_generate", fake_module)

    runner = _load_runner_module()
    manifest = runner.run_shapes(
        out_dir=tmp_path,
        parcel_count=3,
        seed=11,
        width=40.0,
        height=30.0,
    )

    assert [call[0] for call in calls] == ["square", "oval", "triangle"]
    assert [call[2] for call in calls] == [11, 12, 13]
    assert all(call[1] == 3 for call in calls)
    assert all(call[3:] == (40.0, 30.0, "baseline") for call in calls)
    assert [shape["name"] for shape in manifest["shapes"]] == [
        "square",
        "oval",
        "triangle",
    ]
    assert manifest["parameters"]["streamline_mode"] == "baseline"
    assert manifest["limitations"] == [
        "grid_smooth_4rosy_laplace_v1_not_full_yang_global_solver",
        "bounded_junction_street_selection_v0_not_full_section_4_2_solver",
        "shapeop_like_projection_v1_not_exact_shapeop_solver",
    ]
    assert (tmp_path / "manifest.json").exists()
    for shape in ("square", "oval", "triangle"):
        assert (tmp_path / shape / "chen_strict_layout.svg").exists()
        assert (tmp_path / shape / "chen_strict_layout.png").exists()


def test_strict_shape_runner_can_request_yang_streamline_mode(
    tmp_path: Path, monkeypatch: Any
) -> None:
    calls: list[tuple[str, str]] = []

    def generate_named_layout(
        name: str,
        parcel_count: int = 48,
        seed: int = 0,
        width: float = 180.0,
        height: float = 140.0,
        streamline_mode: str = "baseline",
    ) -> GeneratedStub:
        calls.append((name, streamline_mode))
        return _generated_stub(name)

    fake_module = types.ModuleType("mapgen.chen_generate")
    fake_module.generate_named_layout = generate_named_layout
    monkeypatch.setitem(sys.modules, "mapgen.chen_generate", fake_module)

    runner = _load_runner_module()
    manifest = runner.run_shapes(
        out_dir=tmp_path,
        parcel_count=3,
        seed=11,
        width=40.0,
        height=30.0,
        streamline_mode=STREAMLINE_MODE_YANG_D_FIELD,
    )

    assert calls == [
        ("square", STREAMLINE_MODE_YANG_D_FIELD),
        ("oval", STREAMLINE_MODE_YANG_D_FIELD),
        ("triangle", STREAMLINE_MODE_YANG_D_FIELD),
    ]
    assert manifest["parameters"]["streamline_mode"] == STREAMLINE_MODE_YANG_D_FIELD
    assert (
        "yang_d_field_candidates_mode_is_opt_in_and_still_approximate"
        in manifest["limitations"]
    )


def test_strict_shape_runner_can_request_yang_b_field_streamline_mode(
    tmp_path: Path, monkeypatch: Any
) -> None:
    calls: list[tuple[str, str]] = []

    def generate_named_layout(
        name: str,
        parcel_count: int = 48,
        seed: int = 0,
        width: float = 180.0,
        height: float = 140.0,
        streamline_mode: str = "baseline",
    ) -> GeneratedStub:
        calls.append((name, streamline_mode))
        return _generated_stub(name)

    fake_module = types.ModuleType("mapgen.chen_generate")
    fake_module.generate_named_layout = generate_named_layout
    monkeypatch.setitem(sys.modules, "mapgen.chen_generate", fake_module)

    runner = _load_runner_module()
    manifest = runner.run_shapes(
        out_dir=tmp_path,
        parcel_count=3,
        seed=11,
        width=40.0,
        height=30.0,
        streamline_mode=STREAMLINE_MODE_YANG_B_FIELD,
    )

    assert calls == [
        ("square", STREAMLINE_MODE_YANG_B_FIELD),
        ("oval", STREAMLINE_MODE_YANG_B_FIELD),
        ("triangle", STREAMLINE_MODE_YANG_B_FIELD),
    ]
    assert manifest["parameters"]["streamline_mode"] == STREAMLINE_MODE_YANG_B_FIELD
    assert (
        "yang_b_field_candidates_mode_is_opt_in_uniform_clipped_mesh_laplacian_"
        "omega_boundary_alignment_and_still_approximate"
    ) in manifest["limitations"]


def _load_runner_module() -> Any:
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_chen_strict_shapes.py"
    spec = importlib.util.spec_from_file_location("run_chen_strict_shapes_test", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module
