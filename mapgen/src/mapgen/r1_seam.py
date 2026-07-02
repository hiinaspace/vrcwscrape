"""R1 stage 2 — seam-spike pure helpers (macro+micro hybrid).

The "seam spike" probes the literature-flagged RISK in
``docs/large-scale-growth-research.md``: stitching Chen/R2's *local* street
fabric to the *macro* arterials that bound a single macro-block. This module
holds the small, deterministic, testable pieces used by
``scripts/run_r1_seam_spike.py``:

1. :func:`select_block_by_rank` — pick one mid-complexity macro-block (drop
   slivers, then take a block at a fractional area rank).
2. :func:`seam_gap` — quantify how cleanly the union of Chen districts fills the
   chosen block: (a) the max boundary-to-fabric distance (are bounding arterials
   left bare?) and (b) the uncovered-area fraction.

Boundary/mask IO lives in ``mapgen.r1_macro`` (``load_boundary`` /
``boundary_mask``); import it from there.

Coordinate convention: island frame (``x = x0 + (col + 0.5) * cell``), same as
``r1_macro`` / ``r1_arm_b``.
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import shapely.geometry as sg
from shapely.ops import unary_union

from mapgen.chen_artifacts import _street_lines
from mapgen.chen_generate import (
    BoundarySpec,
    GeneratedChenLayout,
    generate_layout_for_boundary,
)
from mapgen.chen_streets import StreetConfig
from mapgen.r1_arm_a import (
    REGIONAL_SPLIT_WEIGHTS,
    IslandFields,
    build_density_field,
    build_terrain_guidance,
)

# Seeds tried in order by :func:`chen_in_block`. A concave block breaking Chen on
# seed 7 but working on a later seed IS a finding, surfaced in the returned info.
DEFAULT_RETRY_SEEDS: tuple[int, ...] = (7, 1, 2, 13)


# ---------------------------------------------------------------------------
# Block selection
# ---------------------------------------------------------------------------


def select_block_by_rank(
    blocks: list[sg.Polygon],
    frac: float = 0.5,
    *,
    sliver_floor: float | None = None,
    min_block_area: float | None = None,
    sliver_factor: float = 1.5,
) -> tuple[int, sg.Polygon]:
    """Select one mid-complexity macro-block by fractional area rank.

    Sort blocks by area ascending, drop slivers (area below ``sliver_floor``),
    then return the block at fractional rank ``frac`` among the survivors
    (``frac=0.5`` -> median area, ``frac=0.0`` -> smallest survivor,
    ``frac=1.0`` -> largest).

    The sliver floor is ``sliver_floor`` when given, else
    ``sliver_factor * min_block_area`` (the macro layer's sliver-merge floor;
    e.g. ``1.5x``), else 0 (keep all). The returned index is the position in the
    *original* ``blocks`` list (so the caller can cross-reference geojson ids).

    Raises ``ValueError`` if ``blocks`` is empty or every block is a sliver.
    """
    if not blocks:
        raise ValueError("no macro-blocks to select from")
    if not 0.0 <= frac <= 1.0:
        raise ValueError(f"frac must be in [0, 1], got {frac}")

    if sliver_floor is None:
        if min_block_area is not None:
            sliver_floor = sliver_factor * min_block_area
        else:
            sliver_floor = 0.0

    # (original_index, area) for non-sliver blocks, sorted by area ascending.
    survivors = [
        (i, float(b.area))
        for i, b in enumerate(blocks)
        if float(b.area) >= sliver_floor
    ]
    if not survivors:
        raise ValueError(
            f"all {len(blocks)} blocks below sliver floor {sliver_floor:.3f}"
        )
    survivors.sort(key=lambda t: (t[1], t[0]))

    # Fractional rank -> integer index into the survivor list.
    rank = int(round(frac * (len(survivors) - 1)))
    rank = max(0, min(rank, len(survivors) - 1))
    chosen_idx = survivors[rank][0]
    return chosen_idx, blocks[chosen_idx]


# ---------------------------------------------------------------------------
# Seam metric
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SeamGap:
    """How cleanly a Chen district fabric fills a macro-block.

    Attributes
    ----------
    max_boundary_gap:
        Directed-Hausdorff-style max distance from any point sampled along the
        *block boundary* to the nearest Chen-district edge. Large => stretches
        of the bounding arterial are left bare (no local street meets them).
    mean_boundary_gap:
        Mean of the same per-sample boundary-to-fabric distances (a softer read
        than the max, which a single concave notch can dominate).
    uncovered_area:
        ``block.area - union(districts).area`` clipped at 0 (covered area can
        slightly exceed via Chen overshoot; see ``overshoot_area``).
    uncovered_frac:
        ``uncovered_area / block.area``. ~0 => fabric tiles the block;
        large => districts cover only part of it.
    overshoot_area:
        Area of the Chen union that falls *outside* the block (fabric spilling
        over the bounding arterials). Usually small; non-trivial values flag a
        boundary mismatch.
    n_boundary_samples:
        Number of boundary sample points used for the gap distances.
    """

    max_boundary_gap: float
    mean_boundary_gap: float
    uncovered_area: float
    uncovered_frac: float
    overshoot_area: float
    n_boundary_samples: int

    def to_dict(self) -> dict[str, float | int]:
        return {
            "max_boundary_gap": round(self.max_boundary_gap, 4),
            "mean_boundary_gap": round(self.mean_boundary_gap, 4),
            "uncovered_area": round(self.uncovered_area, 4),
            "uncovered_frac": round(self.uncovered_frac, 6),
            "overshoot_area": round(self.overshoot_area, 4),
            "n_boundary_samples": self.n_boundary_samples,
        }


def _sample_ring(ring: sg.LineString, spacing: float) -> list[sg.Point]:
    """Sample points along a ring at ~``spacing`` intervals (endpoints incl.)."""
    length = float(ring.length)
    if length <= 0:
        return []
    n = max(2, int(np.ceil(length / max(spacing, 1e-9))) + 1)
    return [ring.interpolate(float(d)) for d in np.linspace(0.0, length, n)]


def seam_gap(
    districts: list[sg.Polygon],
    block: sg.Polygon,
    *,
    sample_spacing: float = 1.0,
) -> SeamGap:
    """Quantify how well a Chen district fabric fills ``block``.

    Two complementary measures (kept deliberately simple):

    (a) **Boundary gap.** Sample the block boundary at ``sample_spacing``
        intervals and, for each sample, measure the distance to the nearest
        Chen *district edge* (the union's boundary). The max is a
        directed-Hausdorff-style read of the worst bare stretch of bounding
        arterial; the mean is a softer summary. If there are no districts, both
        gaps are reported as the longest block dimension (a definite "bare").

    (b) **Coverage.** ``uncovered_area = block.area - area(union ∩ block)`` and
        its fraction; plus ``overshoot_area`` for union spilling outside.

    All distances/areas are in island units (units²). Pure and deterministic.
    """
    block_area = float(block.area)
    minx, miny, maxx, maxy = block.bounds
    diag = float(np.hypot(maxx - minx, maxy - miny))

    # Boundary samples: exterior + any holes.
    rings: list[sg.LineString] = [block.exterior, *list(block.interiors)]
    samples: list[sg.Point] = []
    for ring in rings:
        samples.extend(_sample_ring(sg.LineString(ring), sample_spacing))
    n_samples = len(samples)

    if not districts:
        # No fabric: every boundary point is maximally bare; nothing covered.
        return SeamGap(
            max_boundary_gap=diag,
            mean_boundary_gap=diag,
            uncovered_area=max(block_area, 0.0),
            uncovered_frac=1.0 if block_area > 0 else 0.0,
            overshoot_area=0.0,
            n_boundary_samples=n_samples,
        )

    union = unary_union(districts)
    fabric_edge = union.boundary

    if n_samples > 0 and not fabric_edge.is_empty:
        dists = [float(p.distance(fabric_edge)) for p in samples]
        max_gap = max(dists)
        mean_gap = float(np.mean(dists))
    else:
        max_gap = diag
        mean_gap = diag

    inside = union.intersection(block)
    covered = float(inside.area)
    uncovered = max(block_area - covered, 0.0)
    uncovered_frac = (uncovered / block_area) if block_area > 0 else 0.0
    overshoot = max(float(union.area) - covered, 0.0)

    return SeamGap(
        max_boundary_gap=max_gap,
        mean_boundary_gap=mean_gap,
        uncovered_area=uncovered,
        uncovered_frac=uncovered_frac,
        overshoot_area=overshoot,
        n_boundary_samples=n_samples,
    )


# ---------------------------------------------------------------------------
# Per-block Chen/R2 (the reusable hybrid micro-engine)
# ---------------------------------------------------------------------------


@dataclass
class ChenInBlockResult:
    """Outcome of running Chen/R2 inside one macro-block.

    Attributes
    ----------
    generated:
        The :class:`GeneratedChenLayout` on success, else ``None`` (every seed
        raised). On failure the caller typically falls back to treating the block
        polygon itself as a single district.
    districts:
        Chen district polygons (empty on failure).
    streets:
        Chen street ``LineString`` s (empty on failure).
    info:
        Per-run record: the calibration inputs (``max_parcel_mass``,
        ``min_parcel_area``), the seed that worked (or ``"all_failed"``),
        per-seed attempts/errors, the emergent ``district_count``, and the
        cheap invariant flags (``geometry_valid_pass`` / ``paper_invariant_pass``).
    """

    generated: GeneratedChenLayout | None
    districts: list[sg.Polygon] = field(default_factory=list)
    streets: list[sg.LineString] = field(default_factory=list)
    info: dict[str, Any] = field(default_factory=dict)


def chen_in_block(
    block: sg.Polygon,
    fields: IslandFields,
    *,
    max_parcel_mass: float,
    min_parcel_area: float,
    seeds: tuple[int, ...] = DEFAULT_RETRY_SEEDS,
    guidance_strength: float = 6.0,
    density_ridge_boost: float = 2.0,
) -> ChenInBlockResult:
    """Run Chen/R2 (density-mass mode) inside ``block``, retrying across seeds.

    This is the per-block micro-engine of the macro+micro hybrid
    (``docs/large-scale-growth-research.md``, stages 2-3). The terrain guidance
    and density field are the island-wide R2 fields; only the boundary is the
    block. The two calibration knobs are passed in explicitly so the caller owns
    the policy:

    - ``max_parcel_mass`` — the density-mass cap. The seam spike (stage 2) passes
      a *block-local* cap (``density_field.mass(block) / target``); the full
      hybrid (stage 3) passes a *single global* district mass so district size
      stays consistent island-wide and dense blocks split into more districts.
    - ``min_parcel_area`` — a geometric robustness floor + streamline spacing.
      Keep it small so the mass gate (not the floor) governs termination.

    Everything else matches the validated R2 call: ``REGIONAL_SPLIT_WEIGHTS``,
    ``StreetConfig(avoid_cul_de_sacs=True)``, and the standard guidance/density
    fields. Returns a :class:`ChenInBlockResult`; never raises for a per-block
    Chen failure (the seed errors are captured in ``info`` instead), so a caller
    assembling many blocks can fall back per block without aborting the run.
    """
    if max_parcel_mass <= 0.0:
        raise ValueError("max_parcel_mass must be positive")
    if min_parcel_area <= 0.0:
        raise ValueError("min_parcel_area must be positive")

    spec = BoundarySpec(name="hybrid_block", geom=block)
    guidance = build_terrain_guidance(
        fields, strength=guidance_strength, density_ridge_boost=density_ridge_boost
    )
    density_field = build_density_field(fields)

    info: dict[str, Any] = {
        "max_parcel_mass": round(float(max_parcel_mass), 6),
        "min_parcel_area": round(float(min_parcel_area), 6),
        "block_mass": round(float(density_field.mass(block)), 4),
        "seeds_tried": [],
        "seed_used": "all_failed",
    }

    for seed in seeds:
        attempt: dict[str, Any] = {"seed": seed}
        start = time.perf_counter()
        try:
            generated = generate_layout_for_boundary(
                spec,
                min_parcel_area=min_parcel_area,
                seed=seed,
                split_weights=REGIONAL_SPLIT_WEIGHTS,
                guidance=guidance,
                density_field=density_field,
                max_parcel_mass=max_parcel_mass,
                street_config=StreetConfig(avoid_cul_de_sacs=True),
            )
        except Exception as exc:  # noqa: BLE001
            attempt["status"] = "error"
            attempt["error"] = f"{type(exc).__name__}: {exc}"
            attempt["traceback"] = traceback.format_exc()
            attempt["seconds"] = round(time.perf_counter() - start, 2)
            info["seeds_tried"].append(attempt)
            continue
        attempt["status"] = "ok"
        attempt["seconds"] = round(time.perf_counter() - start, 2)
        info["seeds_tried"].append(attempt)
        info["seed_used"] = seed
        districts = [p.geom for p in generated.layout.mesh.parcels.values()]
        streets = [line for line, _props in _street_lines(generated.layout)]
        info["district_count"] = len(districts)
        info["geometry_valid_pass"] = bool(generated.metrics.get("geometry_valid_pass"))
        info["paper_invariant_pass"] = bool(
            generated.metrics.get("paper_invariant_pass")
        )
        return ChenInBlockResult(
            generated=generated, districts=districts, streets=streets, info=info
        )

    return ChenInBlockResult(generated=None, info=info)
