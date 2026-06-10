from __future__ import annotations

import importlib.util
import json
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from mapgen.chen_generate import (
    STREAMLINE_MODE_YANG_B_FIELD,
    STREAMLINE_MODE_YANG_D_FIELD,
)

OPTIMIZATION_REGULARITY_REVIEW_KEYS = (
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
class GeneratedSuiteStub:
    name: str
    parcel_count: int
    seed: int
    streamline_mode: str = "baseline"


def test_scale_suite_writes_grouped_manifest_with_review_metrics(
    tmp_path: Path, monkeypatch: Any
) -> None:
    calls: list[tuple[str, int, int, float, float, str]] = []

    def generate_named_layout(
        name: str,
        parcel_count: int,
        seed: int,
        width: float,
        height: float,
        streamline_mode: str = "baseline",
    ) -> GeneratedSuiteStub:
        calls.append((name, parcel_count, seed, width, height, streamline_mode))
        return GeneratedSuiteStub(
            name=name,
            parcel_count=parcel_count,
            seed=seed,
            streamline_mode=streamline_mode,
        )

    _install_fake_mapgen_modules(
        monkeypatch,
        generate_named_layout=generate_named_layout,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )

    runner = _load_scale_runner_module()

    manifest = runner.run_suite(
        out_dir=tmp_path,
        parcel_counts=(12, 24),
        seed=5,
        width=40.0,
        height=30.0,
    )

    assert calls == [
        ("square", 12, 17, 40.0, 30.0, "baseline"),
        ("square", 24, 29, 40.0, 30.0, "baseline"),
        ("oval", 12, 100_017, 40.0, 30.0, "baseline"),
        ("oval", 24, 100_029, 40.0, 30.0, "baseline"),
        ("triangle", 12, 200_017, 40.0, 30.0, "baseline"),
        ("triangle", 24, 200_029, 40.0, 30.0, "baseline"),
    ]
    assert json.loads((tmp_path / "manifest.json").read_text()) == manifest

    square_12 = manifest["shapes"]["square"]["12"]
    assert square_12["dir"] == "square/parcels_0012"
    assert square_12["manifest"] == "square/parcels_0012/manifest.json"
    assert square_12["summary"]["parcel_count"] == 12
    # Slimmed review metrics — only keys in REVIEW_METRIC_KEYS are checked.
    assert square_12["review_metrics"]["streamline_config_mode"] == "baseline"
    assert square_12["review_metrics"]["corner_graph_edge_count"] == 24
    assert square_12["review_metrics"]["street_edge_density"] == pytest.approx(11 / 24)
    assert (
        square_12["review_metrics"]["optimization_regularity_projection_kind"]
        == "regular_polygon_similarity_transform_v0"
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_projected_parcel_count_before"
        ]
        == 12
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_projected_parcel_count_after"
        ]
        == 12
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_skipped_parcel_count_before"
        ]
        == 0
    )
    assert (
        square_12["review_metrics"]["optimization_regularity_skipped_by_reason_before"]
        == {}
    )
    assert (
        square_12["review_metrics"]["optimization_regularity_projection_equation_count"]
        == 48
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_projection_residual_before"
        ]
        == 12.0
    )
    assert (
        square_12["review_metrics"]["optimization_regularity_projection_residual_after"]
        == 6.0
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_projection_target_displacement_rms_before"
        ]
        == 1.2
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_projection_target_displacement_max_after"
        ]
        == 1.2
    )
    assert square_12["review_metrics"]["runtime_seconds"] >= 0.0
    assert (tmp_path / "square" / "parcels_0012" / "manifest.json").exists()

    # Legacy chen_fig7 and accepted_streamline score keys must NOT be in review.
    review = square_12["review_metrics"]
    for key in review:
        assert not key.startswith("chen_fig7_"), (
            f"legacy chen_fig7_ key in review_metrics: {key}"
        )
        assert not key.startswith("accepted_streamline_score_"), (
            f"legacy accepted_streamline_score_ key in review_metrics: {key}"
        )

    overall = manifest["aggregate_metrics"]["overall"]
    assert overall["run_count"] == 6
    assert overall["parcel_counts"] == [12, 24]
    assert overall["reachability_pass_count"] == 6
    assert overall["optimization_applied_count"] == 6
    assert overall["optimization_changed_count"] == 4
    assert overall["street_edge_count"] == 102
    assert overall["corner_graph_edge_count"] == 216
    assert overall["street_edge_density"] == pytest.approx(102 / 216)
    assert overall["optimization_regularity_projected_parcel_count_before"] == 108
    assert overall["optimization_regularity_projected_parcel_count_after"] == 108
    assert overall["optimization_regularity_skipped_parcel_count_before"] == 4
    assert overall["optimization_regularity_skipped_parcel_count_after"] == 4
    assert overall["optimization_regularity_skipped_by_reason_before"] == {
        "flipped_orientation": 4
    }
    assert overall["optimization_regularity_skipped_by_reason_after"] == {
        "flipped_orientation": 4
    }
    assert overall["optimization_regularity_projection_equation_count"] == 432
    assert overall["optimization_regularity_projection_residual_before"] == 24.0
    assert overall["optimization_regularity_projection_residual_after"] == 12.0
    assert (
        overall["optimization_regularity_projection_target_displacement_rms_before"]
        == 2.4
    )
    assert (
        overall["optimization_regularity_projection_target_displacement_rms_after"]
        == 1.2
    )
    assert (
        overall["optimization_regularity_projection_target_displacement_max_before"]
        == 4.8
    )
    assert (
        overall["optimization_regularity_projection_target_displacement_max_after"]
        == 2.4
    )


def test_scale_suite_subset_uses_canonical_shape_seed(
    tmp_path: Path, monkeypatch: Any
) -> None:
    calls: list[tuple[str, int, int, str]] = []

    def generate_named_layout(
        name: str,
        parcel_count: int,
        seed: int,
        width: float,
        height: float,
        streamline_mode: str = "baseline",
    ) -> GeneratedSuiteStub:
        calls.append((name, parcel_count, seed, streamline_mode))
        return GeneratedSuiteStub(
            name=name,
            parcel_count=parcel_count,
            seed=seed,
            streamline_mode=streamline_mode,
        )

    _install_fake_mapgen_modules(
        monkeypatch,
        generate_named_layout=generate_named_layout,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )

    runner = _load_scale_runner_module()

    manifest = runner.run_suite(
        out_dir=tmp_path,
        parcel_counts=(12,),
        seed=5,
        shapes=("oval",),
    )

    assert calls == [("oval", 12, 100_017, "baseline")]
    assert manifest["runs"][0]["seed"] == 100_017
    assert (
        manifest["parameters"]["seed_policy"]
        == "base_seed + canonical_shape_index * 100000 + parcel_count"
    )
    assert manifest["parameters"]["output_dir"] == str(tmp_path)
    assert manifest["parameters"]["streamline_mode"] == "baseline"
    assert manifest["limitations"] == [
        "grid_smooth_4rosy_laplace_v1_not_full_yang_global_solver",
        "bounded_junction_street_selection_v0_not_full_section_4_2_solver",
        "shapeop_like_projection_v1_not_exact_shapeop_solver",
    ]


@pytest.mark.parametrize(
    ("run_kwargs", "message"),
    [
        (
            {"parcel_counts": (12, 12), "shapes": ("square",)},
            "duplicate parcel count",
        ),
        (
            {"parcel_counts": (12,), "shapes": ("oval", "oval")},
            "duplicate Chen strict shape",
        ),
    ],
)
def test_scale_suite_rejects_duplicate_output_keys(
    tmp_path: Path, monkeypatch: Any, run_kwargs: dict[str, Any], message: str
) -> None:
    _install_fake_mapgen_modules(
        monkeypatch,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )
    runner = _load_scale_runner_module()
    out_dir = tmp_path / "suite"

    with pytest.raises(ValueError, match=message):
        runner.run_suite(out_dir=out_dir, **run_kwargs)

    assert not out_dir.exists()


def test_scale_suite_cli_defaults_are_stable(monkeypatch: Any) -> None:
    _install_fake_mapgen_modules(
        monkeypatch,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )
    runner = _load_scale_runner_module()

    args = runner.parse_args([])

    assert args.out_dir == runner.DEFAULT_OUT_DIR
    assert args.parcel_counts == [12, 24, 48, 96]
    assert args.shapes == ["square", "oval", "triangle"]
    assert args.seed == 0
    assert args.width == 180.0
    assert args.height == 140.0
    assert args.streamline_mode == "baseline"


def test_scale_suite_can_request_yang_streamline_mode(
    tmp_path: Path, monkeypatch: Any
) -> None:
    calls: list[tuple[str, int, str]] = []

    def generate_named_layout(
        name: str,
        parcel_count: int,
        seed: int,
        width: float,
        height: float,
        streamline_mode: str = "baseline",
    ) -> GeneratedSuiteStub:
        calls.append((name, parcel_count, streamline_mode))
        return GeneratedSuiteStub(
            name=name,
            parcel_count=parcel_count,
            seed=seed,
            streamline_mode=streamline_mode,
        )

    _install_fake_mapgen_modules(
        monkeypatch,
        generate_named_layout=generate_named_layout,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )

    runner = _load_scale_runner_module()

    manifest = runner.run_suite(
        out_dir=tmp_path,
        parcel_counts=(12,),
        seed=5,
        shapes=("oval",),
        streamline_mode=STREAMLINE_MODE_YANG_D_FIELD,
    )

    assert calls == [("oval", 12, STREAMLINE_MODE_YANG_D_FIELD)]
    assert manifest["parameters"]["streamline_mode"] == STREAMLINE_MODE_YANG_D_FIELD
    assert (
        "yang_d_field_candidates_mode_is_opt_in_and_still_approximate"
        in manifest["limitations"]
    )
    assert manifest["runs"][0]["review_metrics"]["streamline_config_mode"] == (
        STREAMLINE_MODE_YANG_D_FIELD
    )


def test_scale_suite_can_request_yang_b_field_streamline_mode(
    tmp_path: Path, monkeypatch: Any
) -> None:
    calls: list[tuple[str, int, str]] = []

    def generate_named_layout(
        name: str,
        parcel_count: int,
        seed: int,
        width: float,
        height: float,
        streamline_mode: str = "baseline",
    ) -> GeneratedSuiteStub:
        calls.append((name, parcel_count, streamline_mode))
        return GeneratedSuiteStub(
            name=name,
            parcel_count=parcel_count,
            seed=seed,
            streamline_mode=streamline_mode,
        )

    _install_fake_mapgen_modules(
        monkeypatch,
        generate_named_layout=generate_named_layout,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )

    runner = _load_scale_runner_module()

    manifest = runner.run_suite(
        out_dir=tmp_path,
        parcel_counts=(12,),
        seed=5,
        shapes=("oval",),
        streamline_mode=STREAMLINE_MODE_YANG_B_FIELD,
    )

    assert calls == [("oval", 12, STREAMLINE_MODE_YANG_B_FIELD)]
    assert manifest["parameters"]["streamline_mode"] == STREAMLINE_MODE_YANG_B_FIELD
    assert (
        "yang_b_field_candidates_mode_is_opt_in_uniform_clipped_mesh_laplacian_"
        "omega_boundary_alignment_and_still_approximate"
    ) in manifest["limitations"]
    review = manifest["runs"][0]["review_metrics"]
    assert review["streamline_config_mode"] == STREAMLINE_MODE_YANG_B_FIELD
    assert review["streamline_config_field_mode"] == "yang_b_field"
    for key in OPTIMIZATION_REGULARITY_REVIEW_KEYS:
        assert key in review
    assert (
        manifest["aggregate_metrics"]["overall"][
            "optimization_regularity_projected_parcel_count_before"
        ]
        == 12
    )
    assert (
        manifest["aggregate_metrics"]["overall"][
            "optimization_regularity_skipped_parcel_count_before"
        ]
        == 1
    )
    assert manifest["aggregate_metrics"]["overall"][
        "optimization_regularity_skipped_by_reason_before"
    ] == {"flipped_orientation": 1}
    assert (
        manifest["aggregate_metrics"]["overall"][
            "optimization_regularity_projection_equation_count"
        ]
        == 48
    )
    assert (
        manifest["aggregate_metrics"]["overall"][
            "optimization_regularity_projection_target_displacement_max_before"
        ]
        == 2.4
    )


def _write_fake_artifacts(
    generated: GeneratedSuiteStub, out_dir: Path
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "manifest.json").write_text("{}\n")
    is_square = generated.name == "square"
    is_oval = generated.name == "oval"
    is_d_field = generated.streamline_mode == STREAMLINE_MODE_YANG_D_FIELD
    is_b_field = generated.streamline_mode == STREAMLINE_MODE_YANG_B_FIELD
    regularity_skipped_count = 0 if is_square else 1
    regularity_skipped_reasons = (
        {} if is_square else {"flipped_orientation": regularity_skipped_count}
    )
    regularity_residual_before = float(generated.parcel_count)
    regularity_rms_before = float(generated.parcel_count) / 10.0
    regularity_max_before = float(generated.parcel_count) / 5.0
    return {
        "files": {
            "manifest": "manifest.json",
            "metrics": "layout_metrics.json",
            "svg": "chen_strict_layout.svg",
            "png": "chen_strict_layout.png",
            "boundary": "boundary.geojson",
            "parcels": "parcels.geojson",
            "streets": "streets.geojson",
            "split_lines": "split_lines.geojson",
            "partition_lines": "partition_lines.geojson",
        },
        "summary": {
            "parcel_count": generated.parcel_count,
            "street_edge_count": generated.parcel_count - 1,
            "corner_graph_edge_count": generated.parcel_count * 2,
            "street_edge_density": (generated.parcel_count - 1)
            / (generated.parcel_count * 2),
            "split_line_count": generated.parcel_count - 1,
            "interpolation_edge_count": 0,
            "generation_stage": "chen_section4_hierarchical_co_generation_v2",
            "boundary": generated.name,
            "boundary_name": generated.name,
            "boundary_area": 12000.0,
            "boundary_perimeter_world": 440.0,
            "requested_parcel_count": generated.parcel_count,
            "min_parcel_area": 100.0,
            "seed": generated.seed,
            "max_hierarchical_level": 5,
            "level_count": 6,
            "accepted_split_count": generated.parcel_count - 1,
            "weld_applied_count": 0,
            "weld_rejected_count": 0,
            "streamline_config_mode": generated.streamline_mode,
            "streamline_config_field_mode": "yang_b_field"
            if is_b_field
            else "yang_d_field"
            if is_d_field
            else "default",
            "streamline_config_candidate_seed_mode": "yang_mesh_vertices"
            if is_b_field or is_d_field
            else "default",
            "streamline_config_score_mode": "yang_div_db_ds_ct"
            if is_b_field or is_d_field
            else "default",
            "street_selection_mode": ("chen_section_4_2_per_level_extension_v2"),
            # Table-1 metrics (simplified)
            "parcel_tri_count": 0,
            "parcel_quad_count": generated.parcel_count - 1,
            "parcel_pent_count": 0 if is_square else 1,
            "parcel_hex_count": 0,
            "parcel_total_count": generated.parcel_count,
            "parcel_quad_fraction": (generated.parcel_count - 1)
            / generated.parcel_count,
            "irregularity_min": 0.0,
            "irregularity_max": 0.05 if is_square else 0.15,
            "irregularity_avg": 0.01 if is_square else 0.05,
            "street_count": 5,
            "street_junction_count": 8,
            "street_end_count": 0,
            "street_length_total": 500.0,
            "street_length_avg": 100.0,
            "junction_angle_dev_from_90_avg": 0.5 if is_square else 5.0,
            # Invariants
            "paper_invariant_pass": True,
            "geometry_valid_pass": True,
            "street_topology_reachability_pass": True,
            # Oval contour
            "boundary_contour_fidelity_stage": "preset_ellipse_radial_error_v0"
            if is_oval
            else "not_applicable",
            "oval_boundary_normalized_radial_error_max": 0.0,
            "oval_boundary_normalized_radial_error_mean": 0.0,
            # Optimization
            "optimization_stage": "chen_section_5_shapeop_like_projection_v1",
            "optimization_applied": True,
            "optimization_layout_used": True,
            "optimization_geometry_changed": not is_square,
            "optimization_accepted_iteration_count": 4,
            "optimization_energy_before": 10.0,
            "optimization_energy_after": 8.0,
            "optimization_regularity_projection_kind": (
                "regular_polygon_similarity_transform_v0"
            ),
            "optimization_regularity_projected_parcel_count_before": (
                generated.parcel_count
            ),
            "optimization_regularity_projected_parcel_count_after": (
                generated.parcel_count
            ),
            "optimization_regularity_skipped_parcel_count_before": (
                regularity_skipped_count
            ),
            "optimization_regularity_skipped_parcel_count_after": (
                regularity_skipped_count
            ),
            "optimization_regularity_skipped_by_reason_before": (
                regularity_skipped_reasons
            ),
            "optimization_regularity_skipped_by_reason_after": (
                regularity_skipped_reasons
            ),
            "optimization_regularity_projection_equation_count": (
                generated.parcel_count * 4
            ),
            "optimization_regularity_projection_residual_before": (
                regularity_residual_before
            ),
            "optimization_regularity_projection_residual_after": (
                regularity_residual_before / 2.0
            ),
            "optimization_regularity_projection_target_displacement_rms_before": (
                regularity_rms_before
            ),
            "optimization_regularity_projection_target_displacement_rms_after": (
                regularity_rms_before / 2.0
            ),
            "optimization_regularity_projection_target_displacement_max_before": (
                regularity_max_before
            ),
            "optimization_regularity_projection_target_displacement_max_after": (
                regularity_max_before / 2.0
            ),
            "generation_seconds": 5.0,
            "optimization_seconds": 1.0,
        },
    }


def _install_fake_mapgen_modules(
    monkeypatch: Any,
    *,
    write_strict_chen_artifacts: Any,
    generate_named_layout: Any | None = None,
) -> None:
    fake_package = types.ModuleType("mapgen")
    fake_package.__path__ = []
    fake_artifacts = types.ModuleType("mapgen.chen_artifacts")
    setattr(fake_artifacts, "write_strict_chen_artifacts", write_strict_chen_artifacts)
    monkeypatch.setitem(sys.modules, "mapgen", fake_package)
    monkeypatch.setitem(sys.modules, "mapgen.chen_artifacts", fake_artifacts)

    if generate_named_layout is not None:
        fake_generate = types.ModuleType("mapgen.chen_generate")
        setattr(fake_generate, "generate_named_layout", generate_named_layout)
        monkeypatch.setitem(sys.modules, "mapgen.chen_generate", fake_generate)


def _load_scale_runner_module() -> Any:
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_chen_strict_scale_suite.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_chen_strict_scale_suite_test", path
    )
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module
