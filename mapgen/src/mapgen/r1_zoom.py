"""Helpers for the Chen/R2 zoom-in visual validation harness.

Pure, testable pieces used by ``scripts/run_r1_zoom_review.py``: loading island
world points with their l0 cluster labels, computing the dominant cluster per
district (for annotation), and deriving a square zoom window around a density
peak. Rendering itself reuses the matplotlib primitives in ``r1_compare``.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

import numpy as np
import polars as pl
from shapely import Point, Polygon
from shapely.strtree import STRtree


@dataclass(frozen=True)
class LabeledPoints:
    """Island-frame world points with l0 cluster labels."""

    xs: np.ndarray  # (N,) float
    ys: np.ndarray  # (N,) float
    l0_ids: np.ndarray  # (N,) int
    l0_names: list[str]  # length N

    def __len__(self) -> int:
        return int(self.xs.shape[0])


def load_points_with_labels(path: str) -> LabeledPoints:
    """Load ``island_points.parquet`` columns x/y/l0_id/l0_name."""
    df = pl.read_parquet(path)
    return LabeledPoints(
        xs=df["x"].to_numpy().astype(float),
        ys=df["y"].to_numpy().astype(float),
        l0_ids=df["l0_id"].to_numpy().astype(int),
        l0_names=[str(v) for v in df["l0_name"].to_list()],
    )


def dominant_cluster_per_district(
    polys: list[Polygon],
    xs: np.ndarray,
    ys: np.ndarray,
    names: list[str],
) -> dict[int, tuple[str, int, int]]:
    """Modal l0 cluster name per district by point-in-polygon (STRtree indexed).

    Returns ``{district_index: (dominant_name, dominant_count, total_count)}`` for
    districts that contain at least one point. Mirrors the counting pattern in
    ``r1_arm_a.assign_worlds_to_parcels`` but tracks the per-district label
    histogram so the harness can annotate "this district is mostly <cluster>".
    """
    result: dict[int, tuple[str, int, int]] = {}
    if not polys or xs.size == 0:
        return result
    tree = STRtree(polys)
    histos: dict[int, Counter[str]] = {}
    for px, py, name in zip(xs, ys, names, strict=True):
        pt = Point(float(px), float(py))
        for idx in tree.query(pt):
            i = int(idx)
            if polys[i].contains(pt):
                histos.setdefault(i, Counter())[name] += 1
                break
    for i, counter in histos.items():
        dom_name, dom_count = counter.most_common(1)[0]
        result[i] = (dom_name, int(dom_count), int(sum(counter.values())))
    return result


def zoom_window(
    cx: float, cy: float, half_span: float
) -> tuple[float, float, float, float]:
    """Square window ``(minx, miny, maxx, maxy)`` centered at ``(cx, cy)``."""
    if half_span <= 0.0:
        raise ValueError("half_span must be positive")
    return (cx - half_span, cy - half_span, cx + half_span, cy + half_span)
