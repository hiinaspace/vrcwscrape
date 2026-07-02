#!/usr/bin/env python
"""R1 Arm A — coarse recursive Chen on a real island (R1 probe spec item 3).

Drives ``generate_layout_for_boundary`` over the island-frame boundary polygon
with a terrain-derived ``RasterGuidanceField`` and regional Eq. 2 lambda
weights, then writes the Chen-native artifacts plus the cross-arm interface
files (``districts.geojson`` / ``arterials.geojson``) for the R1 comparison.

Does not modify any ``chen_*`` source. See ``docs/regional-2_5d-research.md``
(R1 probe spec, Arm A) and ``mapgen/src/mapgen/r1_arm_a.py``.

Deviation from the R1 spec: the spec's "loop street pattern" knob does not exist
in the Chen reimplementation. The closest lever is ``StreetConfig.
avoid_cul_de_sacs`` (street extension repairs dead-ends), used for the
``regional*`` configs. This is recorded in the manifest and report.
"""

from __future__ import annotations

import argparse
import json
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import shapely
from shapely import LineString, Polygon

from mapgen.chen_artifacts import _street_lines, write_strict_chen_artifacts
from mapgen.chen_core import DEFAULT_SPLIT_WEIGHTS, ChenSplitWeights, chen_irregularity
from mapgen.chen_generate import (
    BoundarySpec,
    GeneratedChenLayout,
    generate_layout_for_boundary,
)
from mapgen.chen_streets import StreetConfig
from mapgen.r1_arm_a import (
    REGIONAL_SPLIT_WEIGHTS,
    IslandFields,
    arterials_feature_collection,
    assign_worlds_to_parcels,
    build_density_field,
    build_terrain_guidance,
    default_max_parcel_mass,
    districts_feature_collection,
)

DEFAULT_INPUTS = Path("artifacts/r1/inputs")
DEFAULT_OUT = Path("artifacts/r1/arm_a")


@dataclass(frozen=True)
class RunConfig:
    name: str
    parcel_count: int
    use_guidance: bool
    guidance_strength: float
    density_ridge_boost: float
    split_weights: ChenSplitWeights
    avoid_cul_de_sacs: bool
    note: str
    # R2 density-mass split/termination. When ``use_density_mass`` is set, the
    # parcel "size" measure switches from geometric area to integrated density
    # mass, and termination targets ``total_mass / max_mass_districts`` worlds
    # per district. ``parcel_count`` then only sets the streamline scale +
    # geometric floor (kept fine so the mass gate governs).
    use_density_mass: bool = False
    max_mass_districts: int = 0


def build_run_configs() -> list[RunConfig]:
    return [
        RunConfig(
            name="baseline",
            parcel_count=24,
            use_guidance=False,
            guidance_strength=0.0,
            density_ridge_boost=0.0,
            split_weights=DEFAULT_SPLIT_WEIGHTS,
            avoid_cul_de_sacs=False,
            note="control: no guidance, paper-default Eq. 2 weights",
        ),
        RunConfig(
            name="regional",
            parcel_count=24,
            use_guidance=True,
            guidance_strength=3.0,
            density_ridge_boost=0.0,
            split_weights=REGIONAL_SPLIT_WEIGHTS,
            avoid_cul_de_sacs=True,
            note="guidance ON, regional weights (size+access over regularity)",
        ),
        RunConfig(
            name="regional_fine",
            parcel_count=48,
            use_guidance=True,
            guidance_strength=3.0,
            density_ridge_boost=0.0,
            split_weights=REGIONAL_SPLIT_WEIGHTS,
            avoid_cul_de_sacs=True,
            note="same as regional at finer parcel_count=48",
        ),
        RunConfig(
            # Extra knob: stronger guidance + density-ridge emphasis to test
            # whether terrain alignment reads through more strongly.
            name="regional_strong",
            parcel_count=24,
            use_guidance=True,
            guidance_strength=6.0,
            density_ridge_boost=2.0,
            split_weights=REGIONAL_SPLIT_WEIGHTS,
            avoid_cul_de_sacs=True,
            note="stronger guidance (strength 6 + density-ridge boost)",
        ),
        RunConfig(
            # R2: density-mass split. Geometric floor kept fine (parcel_count=64)
            # so the mass gate governs; mass target ~24 districts' worth of
            # worlds. Expectation: dense core subdivides finer, sparse fringe
            # terminates as a few large districts -> negative density-area rho.
            name="regional_density",
            parcel_count=64,
            use_guidance=True,
            guidance_strength=6.0,
            density_ridge_boost=2.0,
            split_weights=REGIONAL_SPLIT_WEIGHTS,
            avoid_cul_de_sacs=True,
            note="R2 density-mass termination (mass target ~24 districts)",
            use_density_mass=True,
            max_mass_districts=24,
        ),
    ]


def load_boundary(inputs_dir: Path) -> BoundarySpec:
    raw = json.loads((inputs_dir / "island_boundary.geojson").read_text())
    if raw.get("type") == "FeatureCollection":
        geometry = raw["features"][0]["geometry"]
    elif raw.get("type") == "Feature":
        geometry = raw["geometry"]
    else:
        geometry = raw
    geom = shapely.geometry.shape(geometry)
    if not isinstance(geom, Polygon):
        raise ValueError(f"island boundary must be a Polygon, got {geom.geom_type}")
    return BoundarySpec(name="island", geom=geom)


def load_points(inputs_dir: Path) -> list[tuple[float, float]]:
    df = pd.read_parquet(inputs_dir / "island_points.parquet")
    return list(zip(df["x"].astype(float), df["y"].astype(float), strict=True))


def _parcel_polys(generated: GeneratedChenLayout) -> list[tuple[int, Polygon]]:
    return [
        (int(pid), parcel.geom)
        for pid, parcel in sorted(generated.layout.mesh.parcels.items())
    ]


def _arterial_lines(generated: GeneratedChenLayout) -> list[tuple[int, LineString]]:
    lines: list[tuple[int, LineString]] = []
    for sid, (line, _props) in enumerate(_street_lines(generated.layout), start=1):
        lines.append((sid, line))
    return lines


def _emergent_metrics(
    generated: GeneratedChenLayout, world_counts: dict[int, int]
) -> dict[str, Any]:
    metrics = generated.metrics
    parcel_polys = _parcel_polys(generated)
    irregularities = [chen_irregularity(poly) for _pid, poly in parcel_polys]
    areas = [float(poly.area) for _pid, poly in parcel_polys]
    counts = [world_counts.get(pid, 0) for pid, _poly in parcel_polys]

    def _cv(values: list[float]) -> float:
        if not values:
            return 0.0
        mean = sum(values) / len(values)
        if mean <= 0.0:
            return 0.0
        var = sum((v - mean) ** 2 for v in values) / len(values)
        return (var**0.5) / mean

    return {
        "irregularity_avg": (
            sum(irregularities) / len(irregularities) if irregularities else 0.0
        ),
        "irregularity_max": max(irregularities) if irregularities else 0.0,
        "junction_angle_dev_from_90_avg": float(
            metrics.get("junction_angle_dev_from_90_avg", 0.0)
        ),
        "parcel_area_cv": _cv(areas),
        "world_count_cv": _cv([float(c) for c in counts]),
        "world_count_total_assigned": int(sum(counts)),
        "parcel_quad_fraction": float(metrics.get("parcel_quad_fraction", 0.0)),
        "max_hierarchical_level": int(metrics.get("max_hierarchical_level", 0)),
        "guidance_field_applied": bool(metrics.get("guidance_field_applied", False)),
        "density_mass_mode": bool(metrics.get("density_mass_mode", False)),
        "max_parcel_mass": metrics.get("max_parcel_mass"),
        "split_weight_size": float(metrics.get("split_weight_size", 0.0)),
        "split_weight_regularity": float(metrics.get("split_weight_regularity", 0.0)),
        "split_weight_access": float(metrics.get("split_weight_access", 0.0)),
        "paper_invariant_pass": bool(metrics.get("paper_invariant_pass", False)),
        "geometry_valid_pass": bool(metrics.get("geometry_valid_pass", False)),
        "street_topology_reachability_pass": bool(
            metrics.get("street_topology_reachability_pass", False)
        ),
    }


def run_config(
    cfg: RunConfig,
    *,
    boundary: BoundarySpec,
    fields: IslandFields,
    points: list[tuple[float, float]],
    out_dir: Path,
    seed: int,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "name": cfg.name,
        "note": cfg.note,
        "parcel_count_requested": cfg.parcel_count,
        "use_guidance": cfg.use_guidance,
        "guidance_strength": cfg.guidance_strength,
        "density_ridge_boost": cfg.density_ridge_boost,
        "split_weights": {
            "size": cfg.split_weights.size,
            "regularity": cfg.split_weights.regularity,
            "access": cfg.split_weights.access,
        },
        "avoid_cul_de_sacs": cfg.avoid_cul_de_sacs,
        "use_density_mass": cfg.use_density_mass,
        "seed": seed,
    }

    guidance = None
    if cfg.use_guidance:
        guidance = build_terrain_guidance(
            fields,
            strength=cfg.guidance_strength,
            density_ridge_boost=cfg.density_ridge_boost,
        )

    density_field = None
    max_parcel_mass = None
    if cfg.use_density_mass:
        density_field = build_density_field(fields)
        max_parcel_mass = default_max_parcel_mass(density_field, cfg.max_mass_districts)
        entry["max_mass_districts"] = cfg.max_mass_districts
        entry["max_parcel_mass"] = round(float(max_parcel_mass), 6)

    street_config = StreetConfig(avoid_cul_de_sacs=cfg.avoid_cul_de_sacs)

    config_dir = out_dir / cfg.name
    start = time.perf_counter()
    try:
        generated = generate_layout_for_boundary(
            boundary,
            parcel_count=cfg.parcel_count,
            seed=seed,
            split_weights=cfg.split_weights,
            guidance=guidance,
            density_field=density_field,
            max_parcel_mass=max_parcel_mass,
            street_config=street_config,
        )
    except Exception as exc:  # noqa: BLE001
        elapsed = time.perf_counter() - start
        entry["status"] = "error"
        entry["error"] = f"{type(exc).__name__}: {exc}"
        entry["traceback"] = traceback.format_exc()
        entry["generation_seconds"] = round(elapsed, 4)
        return entry
    elapsed = time.perf_counter() - start

    # Chen-native artifacts.
    config_dir.mkdir(parents=True, exist_ok=True)
    write_strict_chen_artifacts(generated, config_dir)

    # Cross-arm interface files.
    parcel_polys = _parcel_polys(generated)
    world_counts = assign_worlds_to_parcels(parcel_polys, points)
    (config_dir / "districts.geojson").write_text(
        json.dumps(districts_feature_collection(parcel_polys, world_counts), indent=2)
        + "\n"
    )
    (config_dir / "arterials.geojson").write_text(
        json.dumps(arterials_feature_collection(_arterial_lines(generated)), indent=2)
        + "\n"
    )

    entry["status"] = "ok"
    entry["generation_seconds"] = round(float(elapsed), 4)
    entry["generated_parcel_count"] = len(parcel_polys)
    entry["street_edge_count"] = int(generated.metrics.get("street_edge_count", 0))
    entry["metrics"] = _emergent_metrics(generated, world_counts)
    entry["outputs"] = [
        "chen_strict_layout.png",
        "chen_strict_layout.svg",
        "districts.geojson",
        "arterials.geojson",
        "manifest.json",
    ]
    return entry


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs-dir", type=Path, default=DEFAULT_INPUTS)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    boundary = load_boundary(args.inputs_dir)
    fields = IslandFields.from_npz(str(args.inputs_dir / "fields.npz"))
    points = load_points(args.inputs_dir)

    args.out_dir.mkdir(parents=True, exist_ok=True)

    configs = build_run_configs()
    results: list[dict[str, Any]] = []
    for cfg in configs:
        print(f"[arm-a] running {cfg.name} ...", flush=True)
        entry = run_config(
            cfg,
            boundary=boundary,
            fields=fields,
            points=points,
            out_dir=args.out_dir,
            seed=args.seed,
        )
        status = entry.get("status")
        print(f"[arm-a]   {cfg.name}: {status}", flush=True)
        results.append(entry)

    manifest = {
        "arm": "A",
        "description": "coarse recursive Chen on a real island (R1 probe item 3)",
        "seed": args.seed,
        "boundary": {
            "name": boundary.name,
            "vertex_count": len(boundary.geom.exterior.coords) - 1,
            "area": round(float(boundary.geom.area), 3),
        },
        "guidance_construction": (
            "angle = atan2(d/dy height_carved, d/dx height_carved) (4-RoSy "
            "contour-following); weight = normalize(slope) clipped [0,1] * "
            "strength; optional density-ridge boost where density high and its "
            "gradient low"
        ),
        "deviations": [
            "R1 spec 'loop street pattern' knob does not exist in the Chen "
            "reimplementation; StreetConfig.avoid_cul_de_sacs is the closest "
            "lever and is used for the regional* configs.",
            "Guidance is internally capped at effective weight 4.0 and faded "
            "near the boundary so boundary alignment still dominates; "
            "strength>4 mostly saturates.",
        ],
        "configs": results,
    }
    (args.out_dir / "arm_a_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=False) + "\n"
    )
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
