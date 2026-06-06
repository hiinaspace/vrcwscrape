"""Isolated Chen-style parcel/street co-generation prototype.

This module is intentionally artifact-first: given one rectangular district and
a target parcel count, it produces static geometry and an inspection sheet.
"""
# ruff: noqa: E501

from __future__ import annotations

import argparse
import heapq
import json
import math
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import shapely
import shapely.affinity
from scipy.spatial import Delaunay
from shapely import LineString, MultiPolygon, Polygon
from shapely.ops import polygonize, split, unary_union


@dataclass(frozen=True)
class SplitBlock:
    block_id: int
    u0: float
    u1: float
    v0: float
    v1: float
    target: int
    level: int


@dataclass(frozen=True)
class StreetRec:
    street_id: int
    kind: str
    geom: LineString


@dataclass(frozen=True)
class ParcelRec:
    parcel_id: int
    block_id: int
    geom: Polygon


@dataclass(frozen=True)
class FaceRec:
    face_id: int
    parcel_id: int
    geom: Polygon


@dataclass(frozen=True)
class MeshGuideRec:
    guide_id: int
    block_id: int
    kind: str
    geom: LineString


@dataclass(frozen=True)
class BoundarySpec:
    name: str
    geom: Polygon


@dataclass(frozen=True)
class SplitQualityWeights:
    size: float = 0.30
    regularity: float = 0.50
    access: float = 0.20


CHEN_IRREGULARITY_GAMMA_ANGLE = 0.75
CHEN_IRREGULARITY_GAMMA_LENGTH = 0.25
CHEN_ACCESS_RATIO_THRESHOLD = 0.5
COLLINEAR_ANGLE_THRESHOLD = math.radians(135)


def _write_png(rgb: np.ndarray, path: Path) -> None:
    h, w, _ = rgb.shape

    def chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + tag
            + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    raw = b"".join(b"\x00" + rgb[y].tobytes() for y in range(h))
    path.write_bytes(
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw, 9))
        + chunk(b"IEND", b"")
    )


def _feature_collection(features: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": features}


def _geom_feature(geom, props: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "properties": props,
        "geometry": json.loads(shapely.to_geojson(geom)),
    }


def _boundary_preset(name: str, width: float, height: float, seed: int) -> BoundarySpec:
    if name == "rectangle":
        geom = Polygon([(0, 0), (width, 0), (width, height), (0, height)])
    elif name == "triangle":
        geom = Polygon(
            [
                (width * 0.06, height * 0.08),
                (width * 0.94, height * 0.14),
                (width * (0.48 + 0.06 * math.sin(seed)), height * 0.94),
            ]
        )
    elif name == "oval":
        geom = shapely.affinity.scale(
            shapely.Point(width * 0.5, height * 0.5).buffer(0.5, quad_segs=24),
            xfact=width * 0.94,
            yfact=height * 0.86,
            origin=(width * 0.5, height * 0.5),
        )
    elif name == "island":
        pts = []
        for i in range(28):
            theta = math.tau * i / 28
            wave = 1.0 + 0.13 * math.sin(theta * 3.0 + seed * 0.11) + 0.07 * math.cos(theta * 5.0 - seed * 0.17)
            pts.append(
                (
                    width * (0.5 + 0.46 * wave * math.cos(theta)),
                    height * (0.5 + 0.42 * wave * math.sin(theta)),
                )
            )
        geom = Polygon(pts).buffer(0)
    else:
        raise SystemExit(f"unknown boundary preset: {name}")
    return BoundarySpec(name=name, geom=geom)


def _largest_polygon(geom) -> Polygon | None:
    if isinstance(geom, Polygon):
        return geom if geom.area > 1e-9 else None
    if isinstance(geom, MultiPolygon):
        polys = [p for p in geom.geoms if p.area > 1e-9]
        return max(polys, key=lambda p: p.area) if polys else None
    return None


def _line_parts(geom) -> list[LineString]:
    if isinstance(geom, LineString):
        return [geom] if geom.length > 1e-9 else []
    if hasattr(geom, "geoms"):
        parts: list[LineString] = []
        for part in geom.geoms:
            parts.extend(_line_parts(part))
        return parts
    return []


def _sample_polygon_points(poly: Polygon, target_cells: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    minx, miny, maxx, maxy = poly.bounds
    spacing = math.sqrt(poly.area / max(target_cells, 1)) * 0.95
    pts: list[tuple[float, float]] = []
    coords = [(float(x), float(y)) for x, y in poly.exterior.coords[:-1]]
    for a, b in zip(coords, coords[1:] + coords[:1], strict=True):
        length = math.hypot(b[0] - a[0], b[1] - a[1])
        count = max(2, int(math.ceil(length / max(spacing, 1e-9))))
        for i in range(count):
            t = i / count
            pts.append((a[0] * (1.0 - t) + b[0] * t, a[1] * (1.0 - t) + b[1] * t))

    nx = max(3, int(math.ceil((maxx - minx) / max(spacing, 1e-9))))
    ny = max(3, int(math.ceil((maxy - miny) / max(spacing, 1e-9))))
    for iy in range(ny + 1):
        for ix in range(nx + 1):
            x = minx + (ix + 0.5) * (maxx - minx) / (nx + 1)
            y = miny + (iy + 0.5) * (maxy - miny) / (ny + 1)
            jitter = spacing * 0.18
            x += float(rng.uniform(-jitter, jitter))
            y += float(rng.uniform(-jitter, jitter))
            if poly.contains(shapely.Point(x, y)):
                pts.append((x, y))
    return np.asarray(pts, dtype=np.float64)


def _polygon_edges(poly: Polygon) -> list[tuple[tuple[float, float], tuple[float, float]]]:
    coords = [(float(x), float(y)) for x, y in poly.exterior.coords[:-1]]
    return list(zip(coords, coords[1:] + coords[:1], strict=True))


def _principal_angle(poly: Polygon, seed: int) -> float:
    rect = poly.minimum_rotated_rectangle
    coords = [(float(x), float(y)) for x, y in rect.exterior.coords[:-1]]
    if len(coords) < 2:
        return seed * 0.137
    edges = [
        (math.hypot(b[0] - a[0], b[1] - a[1]), math.atan2(b[1] - a[1], b[0] - a[0]))
        for a, b in zip(coords, coords[1:] + coords[:1], strict=True)
    ]
    angle = max(edges, key=lambda item: item[0])[1]
    boundary_edges = _polygon_edges(poly)
    if boundary_edges:
        longest = max(boundary_edges, key=lambda edge: math.hypot(edge[1][0] - edge[0][0], edge[1][1] - edge[0][1]))
        boundary_angle = math.atan2(longest[1][1] - longest[0][1], longest[1][0] - longest[0][0])
        angle = _blend_axis_angles(angle, boundary_angle, 0.35)
    return angle


def _blend_axis_angles(a: float, b: float, t: float) -> float:
    # Cross fields are pi-periodic: blend doubled angles and halve the result.
    x = math.cos(2.0 * a) * (1.0 - t) + math.cos(2.0 * b) * t
    y = math.sin(2.0 * a) * (1.0 - t) + math.sin(2.0 * b) * t
    return 0.5 * math.atan2(y, x)


def _nearest_boundary_axis(poly: Polygon, x: float, y: float) -> tuple[float, float]:
    best_dist = math.inf
    best_angle = 0.0
    for a, b in _polygon_edges(poly):
        ax, ay = a
        bx, by = b
        vx = bx - ax
        vy = by - ay
        denom = vx * vx + vy * vy
        t = 0.0 if denom <= 1e-12 else max(0.0, min(1.0, ((x - ax) * vx + (y - ay) * vy) / denom))
        px = ax + vx * t
        py = ay + vy * t
        dist = math.hypot(x - px, y - py)
        if dist < best_dist:
            best_dist = dist
            best_angle = math.atan2(vy, vx)
    return best_angle, best_dist


def _crossfield_angle(poly: Polygon, x: float, y: float, base_angle: float) -> float:
    boundary_angle, dist = _nearest_boundary_axis(poly, x, y)
    scale = math.sqrt(max(poly.area, 1e-9))
    boundary_weight = float(np.clip(1.0 - dist / max(scale * 0.38, 1e-9), 0.0, 0.65))
    return _blend_axis_angles(base_angle, boundary_angle, boundary_weight)


def _split_curve(
    poly: Polygon,
    angle: float,
    offset: float,
    seed: int,
    strength: float,
    *,
    curved: bool,
) -> LineString:
    minx, miny, maxx, maxy = poly.bounds
    cx, cy = float(poly.centroid.x), float(poly.centroid.y)
    span = math.hypot(maxx - minx, maxy - miny) * 1.8
    ux, uy = math.cos(angle), math.sin(angle)
    nx, ny = -uy, ux
    phase = seed * 0.113 + poly.area * 0.0003
    pts: list[tuple[float, float]] = []
    for raw_t in np.linspace(-1.0, 1.0, 25):
        t = float(raw_t)
        bend = (
            strength
            * span
            * 0.030
            * math.sin((t * 0.82 + phase) * math.tau)
            * (1.0 - abs(t) ** 1.7)
            if curved
            else 0.0
        )
        x = cx + ux * span * t + nx * (offset + bend)
        y = cy + uy * span * t + ny * (offset + bend)
        pts.append((x, y))
    return LineString(pts)


def _trace_field_curve(
    poly: Polygon,
    domain: Polygon,
    base_angle: float,
    start: tuple[float, float],
    preferred_angle: float,
    seed: int,
    step: float,
    max_steps: int,
) -> LineString:
    def march(sign: float) -> list[tuple[float, float]]:
        pts: list[tuple[float, float]] = []
        x, y = start
        angle = preferred_angle
        phase = seed * 0.097
        for i in range(max_steps):
            field = _crossfield_angle(domain, x, y, base_angle)
            candidates = (field, field + math.pi * 0.5, field + math.pi, field - math.pi * 0.5)
            target = min(candidates, key=lambda a: abs(math.atan2(math.sin(a - angle), math.cos(a - angle))))
            angle = _blend_axis_angles(angle, target, 0.38)
            curve_bias = 0.10 * math.sin((i / max(max_steps, 1) + phase) * math.tau)
            dx = math.cos(angle + curve_bias) * step * sign
            dy = math.sin(angle + curve_bias) * step * sign
            x += dx
            y += dy
            pts.append((x, y))
            if not poly.buffer(step * 0.75).contains(shapely.Point(x, y)):
                break
        return pts

    backward = march(-1.0)
    forward = march(1.0)
    pts = list(reversed(backward)) + [start] + forward
    return LineString(pts)


def _split_polygons(poly: Polygon, curve: LineString) -> list[Polygon]:
    try:
        parts = split(poly, curve)
    except (ValueError, shapely.GEOSException):
        return []
    polys = [_largest_polygon(part) for part in parts.geoms]
    return [p for p in polys if p is not None and p.area > poly.area * 0.01]


def _split_orthogonality_penalty(parts: list[Polygon], split_line: LineString) -> float:
    coords = list(split_line.coords)
    if len(coords) < 2:
        return 1.0
    mid_a = coords[max(0, len(coords) // 2 - 1)]
    mid_b = coords[min(len(coords) - 1, len(coords) // 2 + 1)]
    split_angle = math.atan2(mid_b[1] - mid_a[1], mid_b[0] - mid_a[0])
    penalties: list[float] = []
    for poly in parts:
        for a, b in _polygon_edges(poly):
            edge = LineString([a, b])
            if edge.distance(split_line) > max(split_line.length * 0.015, 1e-6):
                continue
            edge_angle = math.atan2(b[1] - a[1], b[0] - a[0])
            delta = abs(math.atan2(math.sin(edge_angle - split_angle), math.cos(edge_angle - split_angle)))
            penalties.append(min(abs(delta - math.pi * 0.5), abs(delta)))
    return float(np.mean(penalties)) if penalties else 0.0


def _access_penalty(parts: list[Polygon], domain: Polygon, existing_split_lines: list[LineString], target_area: float) -> float:
    access_lines = [domain.boundary, *existing_split_lines]
    threshold = math.sqrt(target_area) * 1.6
    penalties: list[float] = []
    for part in parts:
        dist = min(float(part.boundary.distance(line)) for line in access_lines)
        penalties.append(min(1.0, dist / max(threshold, 1e-9)))
    return float(np.mean(penalties)) if penalties else 1.0


def _approx_side_lengths(poly: Polygon) -> list[float]:
    coords = _approx_polygon_coords(poly)
    if len(coords) < 2:
        return []
    return [
        math.hypot(b[0] - a[0], b[1] - a[1])
        for a, b in zip(coords, coords[1:] + coords[:1], strict=True)
    ]


def _line_overlap_length(line: LineString, access_lines: list[LineString], tolerance: float) -> float:
    if line.length <= 1e-9:
        return 0.0
    for access_line in access_lines:
        if line.distance(access_line) <= tolerance:
            return float(line.length)
    return 0.0


def _street_access_ratio(poly: Polygon, access_lines: list[LineString], tolerance: float) -> float:
    side_lengths = _approx_side_lengths(poly)
    avg_side = float(np.mean(side_lengths)) if side_lengths else 0.0
    if avg_side <= 1e-9:
        return 0.0
    access_len = sum(
        _line_overlap_length(LineString([a, b]), access_lines, tolerance)
        for a, b in _polygon_edges(poly)
    )
    return access_len / avg_side


def _chen_split_quality(
    parts: list[Polygon],
    domain: Polygon,
    existing_split_lines: list[LineString],
    weights: SplitQualityWeights,
    target_area: float,
) -> tuple[float, dict[str, float]]:
    if len(parts) != 2:
        return -math.inf, {}
    areas = sorted([part.area for part in parts])
    q_size = areas[0] / max(areas[1], 1e-9)
    q_regu = 1.0 / (1.0 + sum(_chen_irregularity(part) for part in parts))
    access_lines = [domain.boundary, *existing_split_lines]
    access_tol = math.sqrt(target_area) * 0.08
    access_ratios = [_street_access_ratio(part, access_lines, access_tol) for part in parts]
    q_acce = 2.0 if min(access_ratios) >= CHEN_ACCESS_RATIO_THRESHOLD else 1.0
    quality = weights.size * q_size + weights.regularity * q_regu + weights.access * q_acce
    return quality, {
        "q_size": q_size,
        "q_regu": q_regu,
        "q_acce": q_acce,
        "access_ratio_min": min(access_ratios),
    }


def _regularity_penalty(parts: list[Polygon], target_area: float) -> float:
    target_width = math.sqrt(target_area) * 0.62
    penalties: list[float] = []
    for part in parts:
        aspect = _aspect(part)
        width = _min_rect_width(part)
        aspect_penalty = max(0.0, aspect - 2.8) / 3.0
        width_penalty = max(0.0, target_width - width) / max(target_width, 1e-9)
        edge_penalty = min(1.0, _short_edge_count(part, math.sqrt(target_area) * 0.10) / 6.0)
        penalties.append(min(2.0, aspect_penalty + width_penalty * 1.35 + edge_penalty * 0.35))
    return float(np.mean(penalties)) if penalties else 1.0


def _mean_line_curvature(lines: list[LineString]) -> float:
    vals: list[float] = []
    for line in lines:
        coords = list(line.coords)
        if len(coords) < 3:
            continue
        turns = 0.0
        for a, b, c in zip(coords[:-2], coords[1:-1], coords[2:], strict=False):
            a0 = math.atan2(b[1] - a[1], b[0] - a[0])
            a1 = math.atan2(c[1] - b[1], c[0] - b[0])
            turns += abs(math.atan2(math.sin(a1 - a0), math.cos(a1 - a0)))
        vals.append(turns / max(line.length, 1e-9))
    return float(np.mean(vals)) if vals else 0.0


def _line_mid_angle(line: LineString) -> float:
    coords = list(line.coords)
    if len(coords) < 2:
        return 0.0
    i = max(0, len(coords) // 2 - 1)
    a = coords[i]
    b = coords[min(len(coords) - 1, i + 1)]
    return math.atan2(b[1] - a[1], b[0] - a[0])


def _parallel_split_penalty(candidate: LineString, existing: list[LineString], spacing: float) -> float:
    if not existing:
        return 0.0
    angle = _line_mid_angle(candidate)
    penalty = 0.0
    for line in existing:
        dist = candidate.distance(line)
        if dist > spacing:
            continue
        other = _line_mid_angle(line)
        parallel = _axis_delta(angle, other)
        if parallel < math.radians(14):
            penalty += (1.0 - dist / max(spacing, 1e-9)) * (1.0 - parallel / math.radians(14))
    return penalty


def _axis_delta(a: float, b: float) -> float:
    delta = abs(math.atan2(math.sin(a - b), math.cos(a - b)))
    return min(delta, abs(math.pi - delta))


def _near_parallel_split_count(lines: list[LineString], distance_threshold: float, angle_threshold: float) -> int:
    count = 0
    angles = [_line_mid_angle(line) for line in lines]
    for i, line_a in enumerate(lines):
        for j, line_b in enumerate(lines[i + 1 :], start=i + 1):
            if line_a.distance(line_b) < distance_threshold and _axis_delta(angles[i], angles[j]) < angle_threshold:
                count += 1
    return count


def _short_edge_count(poly: Polygon, threshold: float) -> int:
    return sum(1 for a, b in _polygon_edges(poly) if math.hypot(b[0] - a[0], b[1] - a[1]) < threshold)


def _approx_polygon_coords(poly: Polygon) -> list[tuple[float, float]]:
    coords = [(float(x), float(y)) for x, y in poly.exterior.coords[:-1]]
    if len(coords) <= 3:
        return coords
    approx: list[tuple[float, float]] = []
    for i, point in enumerate(coords):
        prev = coords[i - 1]
        nxt = coords[(i + 1) % len(coords)]
        a0 = math.atan2(prev[1] - point[1], prev[0] - point[0])
        a1 = math.atan2(nxt[1] - point[1], nxt[0] - point[0])
        interior = abs(math.atan2(math.sin(a1 - a0), math.cos(a1 - a0)))
        if interior > COLLINEAR_ANGLE_THRESHOLD:
            continue
        approx.append(point)
    return approx if len(approx) >= 3 else coords


def _chen_irregularity(poly: Polygon) -> float:
    """Chen 2024 Eq. 1 on the parcel's approximate polygon."""
    coords = _approx_polygon_coords(poly)
    n = len(coords)
    if n < 3:
        return 0.0
    lengths: list[float] = []
    angles: list[float] = []
    for i, point in enumerate(coords):
        prev = coords[i - 1]
        nxt = coords[(i + 1) % n]
        lengths.append(math.hypot(nxt[0] - point[0], nxt[1] - point[1]))
        a0 = math.atan2(prev[1] - point[1], prev[0] - point[0])
        a1 = math.atan2(nxt[1] - point[1], nxt[0] - point[0])
        angles.append(abs(math.atan2(math.sin(a1 - a0), math.cos(a1 - a0))))
    mean_angle = (n - 2) * math.pi / n
    mean_length = float(np.mean(lengths)) if lengths else 0.0
    if mean_length <= 1e-12:
        return 0.0
    angle_term = float(np.mean([(angle - mean_angle) ** 2 for angle in angles]))
    length_term = float(np.mean([(length - mean_length) ** 2 for length in lengths]) / (mean_length * mean_length))
    return CHEN_IRREGULARITY_GAMMA_ANGLE * angle_term + CHEN_IRREGULARITY_GAMMA_LENGTH * length_term


def _chen_irregularity_metrics(parcels: list[ParcelRec]) -> dict[str, Any]:
    vals = np.asarray([_chen_irregularity(parcel.geom) for parcel in parcels], dtype=np.float64)
    if not len(vals):
        return {
            "parcel_irregularity_avg": 0.0,
            "parcel_irregularity_p50": 0.0,
            "parcel_irregularity_p95": 0.0,
        }
    return {
        "parcel_irregularity_avg": float(np.mean(vals)),
        "parcel_irregularity_p50": float(np.quantile(vals, 0.50)),
        "parcel_irregularity_p95": float(np.quantile(vals, 0.95)),
    }


def _clean_short_edges(poly: Polygon, threshold: float) -> Polygon:
    coords = [(float(x), float(y)) for x, y in poly.exterior.coords[:-1]]
    if len(coords) <= 4:
        return poly
    changed = True
    while changed and len(coords) > 4:
        changed = False
        best_i = -1
        best_len = math.inf
        for i, p in enumerate(coords):
            prev = coords[i - 1]
            nxt = coords[(i + 1) % len(coords)]
            short_len = min(math.hypot(p[0] - prev[0], p[1] - prev[1]), math.hypot(nxt[0] - p[0], nxt[1] - p[1]))
            turn = abs(
                math.atan2(
                    math.sin(math.atan2(nxt[1] - p[1], nxt[0] - p[0]) - math.atan2(p[1] - prev[1], p[0] - prev[0])),
                    math.cos(math.atan2(nxt[1] - p[1], nxt[0] - p[0]) - math.atan2(p[1] - prev[1], p[0] - prev[0])),
                )
            )
            nearly_collinear = min(turn, abs(math.pi - turn)) < math.radians(6)
            if (short_len < threshold or nearly_collinear) and short_len < best_len:
                best_i = i
                best_len = short_len
        if best_i < 0:
            break
        trial_coords = coords[:best_i] + coords[best_i + 1 :]
        trial = Polygon(trial_coords).buffer(0)
        largest = _largest_polygon(trial)
        if largest is not None and largest.is_valid and abs(largest.area - poly.area) / max(poly.area, 1e-9) < 0.025:
            coords = [(float(x), float(y)) for x, y in largest.exterior.coords[:-1]]
            changed = True
        else:
            break
    return Polygon(coords).buffer(0)


def _overlap_count(polys: list[Polygon]) -> int:
    overlaps = 0
    for i, poly in enumerate(polys):
        for other in polys[i + 1 :]:
            if poly.intersection(other).area > 1e-7:
                overlaps += 1
    return overlaps


def _noded_faces_for_parcels(parcels: list[ParcelRec], boundary: Polygon) -> tuple[list[FaceRec], dict[str, Any]]:
    linework = unary_union([parcel.geom.boundary for parcel in parcels])
    raw_faces = [face for face in polygonize(linework) if face.area > boundary.area * 1e-9 and face.representative_point().within(boundary.buffer(1e-7))]
    faces: list[FaceRec] = []
    split_parcels: set[int] = set()
    parcel_face_counts = {parcel.parcel_id: 0 for parcel in parcels}
    for raw_face in raw_faces:
        point = raw_face.representative_point()
        best: tuple[float, int] = (0.0, -1)
        for parcel in parcels:
            if not parcel.geom.buffer(1e-7).contains(point):
                continue
            area = raw_face.intersection(parcel.geom).area
            if area > best[0]:
                best = (area, parcel.parcel_id)
        if best[1] < 0:
            continue
        parcel_face_counts[best[1]] += 1
        if parcel_face_counts[best[1]] > 1:
            split_parcels.add(best[1])
        faces.append(FaceRec(len(faces) + 1, best[1], raw_face))

    face_area = sum(face.geom.area for face in faces)
    parcel_area = sum(parcel.geom.area for parcel in parcels)
    edge_use: dict[tuple[tuple[int, int], tuple[int, int]], int] = {}
    for face in faces:
        for a, b in _polygon_edges(face.geom):
            key = (_node_key(a), _node_key(b))
            if key[1] < key[0]:
                key = (key[1], key[0])
            edge_use[key] = edge_use.get(key, 0) + 1
    return faces, {
        "planar_face_count": int(len(faces)),
        "planar_split_parcel_count": int(len(split_parcels)),
        "planar_face_area_delta": float(abs(face_area - parcel_area)),
        "planar_edge_count": int(len(edge_use)),
        "planar_nonmanifold_edge_count": int(sum(1 for count in edge_use.values() if count > 2)),
    }


def _parcel_width_quantiles(parcels: list[ParcelRec]) -> tuple[float, float]:
    widths = np.asarray([_min_rect_width(parcel.geom) for parcel in parcels], dtype=np.float64)
    if not len(widths):
        return 0.0, 0.0
    return float(np.quantile(widths, 0.01)), float(np.quantile(widths, 0.05))


def _node_key(pt: tuple[float, float]) -> tuple[int, int]:
    return int(round(pt[0] * 1_000_000)), int(round(pt[1] * 1_000_000))


def _parcel_node_graph(
    parcels: list[ParcelRec],
    boundary: Polygon,
    tol: float,
) -> tuple[list[tuple[float, float]], list[tuple[int, list[int]]], set[tuple[int, int]], set[int]]:
    node_ids: dict[tuple[int, int], int] = {}
    nodes: list[tuple[float, float]] = []
    rings: list[tuple[int, list[int]]] = []
    boundary_nodes: set[int] = set()

    def node_id(pt: tuple[float, float]) -> int:
        key = _node_key(pt)
        found = node_ids.get(key)
        if found is not None:
            return found
        node_ids[key] = len(nodes)
        nodes.append(pt)
        if shapely.Point(pt).distance(boundary.boundary) <= tol:
            boundary_nodes.add(len(nodes) - 1)
        return len(nodes) - 1

    for parcel in parcels:
        ids = [node_id((float(x), float(y))) for x, y in parcel.geom.exterior.coords[:-1]]
        rings.append((parcel.parcel_id, ids))

    edges: set[tuple[int, int]] = set()
    for _parcel_id, ring in rings:
        for a, b in zip(ring, ring[1:] + ring[:1], strict=True):
            if a != b:
                edges.add((a, b) if a < b else (b, a))
    return nodes, rings, edges, boundary_nodes


def _rebuild_parcels_from_nodes(
    nodes: np.ndarray,
    rings: list[tuple[int, list[int]]],
) -> list[ParcelRec]:
    parcels: list[ParcelRec] = []
    for parcel_id, ring in rings:
        poly = Polygon([(float(nodes[i, 0]), float(nodes[i, 1])) for i in ring]).buffer(0)
        largest = _largest_polygon(poly)
        if largest is None:
            continue
        parcels.append(ParcelRec(parcel_id, parcel_id, largest))
    parcels.sort(key=lambda p: p.parcel_id)
    return parcels


def _rebuild_parcels_from_moved_linework(
    nodes: np.ndarray,
    edges: set[tuple[int, int]],
    original_parcels: list[ParcelRec],
    boundary: Polygon,
) -> list[ParcelRec]:
    lines = [LineString([(float(nodes[a, 0]), float(nodes[a, 1])), (float(nodes[b, 0]), float(nodes[b, 1]))]) for a, b in edges]
    faces = [face for face in polygonize(unary_union(lines)) if face.area > boundary.area * 1e-9 and face.representative_point().within(boundary.buffer(1e-6))]
    grouped: dict[int, list[Polygon]] = {parcel.parcel_id: [] for parcel in original_parcels}
    for face in faces:
        point = face.representative_point()
        best: tuple[float, int] = (0.0, -1)
        for parcel in original_parcels:
            if not parcel.geom.buffer(math.sqrt(max(parcel.geom.area, 1e-9)) * 0.06).contains(point):
                continue
            area = face.intersection(parcel.geom).area
            if area > best[0]:
                best = (area, parcel.parcel_id)
        if best[1] >= 0:
            grouped[best[1]].append(face)

    rebuilt: list[ParcelRec] = []
    for parcel in original_parcels:
        parts = grouped.get(parcel.parcel_id, [])
        if not parts:
            continue
        merged = _largest_polygon(unary_union(parts))
        if merged is None:
            continue
        rebuilt.append(ParcelRec(parcel.parcel_id, parcel.block_id, merged))
    rebuilt.sort(key=lambda p: p.parcel_id)
    return rebuilt


def _optimize_shared_vertices(
    parcels: list[ParcelRec],
    boundary: Polygon,
    target_area: float,
) -> tuple[list[ParcelRec], dict[str, Any]]:
    tol = math.sqrt(target_area) * 0.025
    nodes_list, rings, edges, boundary_nodes = _parcel_node_graph(parcels, boundary, tol)
    if not nodes_list:
        return parcels, {"geometry_optimization_applied": False}
    nodes = np.asarray(nodes_list, dtype=np.float64)
    original = nodes.copy()
    adjacency: dict[int, set[int]] = {i: set() for i in range(len(nodes))}
    for a, b in edges:
        adjacency[a].add(b)
        adjacency[b].add(a)

    min_len = math.sqrt(target_area) * 0.24
    max_move = math.sqrt(target_area) * 0.40
    for _iter in range(36):
        disp = np.zeros_like(nodes)
        weight = np.zeros(len(nodes), dtype=np.float64)
        for a, b in edges:
            pa = nodes[a]
            pb = nodes[b]
            vec = pb - pa
            length = float(np.linalg.norm(vec))
            if length <= 1e-9:
                continue
            if length < min_len:
                force = (min_len - length) * vec / length * 0.18
                if a not in boundary_nodes:
                    disp[a] -= force
                    weight[a] += 1.0
                if b not in boundary_nodes:
                    disp[b] += force
                    weight[b] += 1.0

        for i, nbrs in adjacency.items():
            if i in boundary_nodes or len(nbrs) < 2:
                continue
            avg = np.mean(nodes[list(nbrs)], axis=0)
            disp[i] += (avg - nodes[i]) * 0.018
            weight[i] += 1.0

        for i in range(len(nodes)):
            if i in boundary_nodes or weight[i] <= 0:
                continue
            step = disp[i] / weight[i]
            norm = float(np.linalg.norm(step))
            cap = min_len * 0.10
            if norm > cap:
                step *= cap / norm
            moved = nodes[i] + step
            total_move = moved - original[i]
            total_norm = float(np.linalg.norm(total_move))
            if total_norm > max_move:
                moved = original[i] + total_move * (max_move / total_norm)
            if boundary.buffer(tol * 2.0).contains(shapely.Point(float(moved[0]), float(moved[1]))):
                nodes[i] = moved

    before_p01, before_p05 = _parcel_width_quantiles(parcels)
    short_threshold = math.sqrt(target_area) * 0.12
    before_short = sum(_short_edge_count(parcel.geom, short_threshold) for parcel in parcels)
    best_candidate = parcels
    best_metrics: dict[str, Any] | None = None
    applied = False
    for scale in (1.0, 0.65, 0.42, 0.25, 0.14, 0.08, 0.04, 0.02, 0.01, 0.005, 0.002, 0.001, 0.0005):
        trial_nodes = original + (nodes - original) * scale
        candidate = _rebuild_parcels_from_moved_linework(trial_nodes, edges, parcels, boundary)
        if len(candidate) != len(parcels):
            continue
        candidate_polys = [parcel.geom for parcel in candidate]
        coverage = sum(poly.area for poly in candidate_polys) / max(boundary.area, 1e-9)
        valid = all(poly.is_valid and poly.area > 1e-9 for poly in candidate_polys)
        overlaps = _overlap_count(candidate_polys)
        after_p01, after_p05 = _parcel_width_quantiles(candidate)
        after_short = sum(_short_edge_count(parcel.geom, short_threshold) for parcel in candidate)
        improved = after_p05 >= before_p05 * 0.98 and (after_p01 > before_p01 or after_short < before_short)
        metrics = {
            "geometry_opt_step_scale": float(scale),
            "geometry_opt_width_p01_after": after_p01,
            "geometry_opt_width_p05_after": after_p05,
            "geometry_opt_short_edges_after": int(after_short),
            "geometry_opt_candidate_overlap_count": int(overlaps),
            "geometry_opt_candidate_coverage": float(coverage),
        }
        if valid and overlaps == 0 and coverage > 0.995 and improved:
            best_candidate = candidate
            best_metrics = metrics
            applied = True
            break
        if best_metrics is None or (overlaps < int(best_metrics["geometry_opt_candidate_overlap_count"])):
            best_metrics = metrics
    return (
        best_candidate,
        {
            "geometry_optimization_applied": applied,
            "geometry_opt_node_count": int(len(nodes)),
            "geometry_opt_boundary_node_count": int(len(boundary_nodes)),
            "geometry_opt_min_edge_target": float(min_len),
            "geometry_opt_width_p01_before": before_p01,
            "geometry_opt_width_p05_before": before_p05,
            "geometry_opt_short_edges_before": int(before_short),
            **(best_metrics or {}),
        },
    )


def _build_corner_graph(
    parcels: list[ParcelRec],
    boundary: Polygon,
    tol: float,
    seed_street_lines: list[LineString] | None = None,
) -> tuple[list[tuple[float, float]], dict[tuple[int, int], dict[str, Any]], dict[int, list[tuple[int, int, float]]]]:
    node_ids: dict[tuple[int, int], int] = {}
    nodes: list[tuple[float, float]] = []
    edges: dict[tuple[int, int], dict[str, Any]] = {}

    def node_id(pt: tuple[float, float]) -> int:
        key = _node_key(pt)
        found = node_ids.get(key)
        if found is not None:
            return found
        node_ids[key] = len(nodes)
        nodes.append(pt)
        return len(nodes) - 1

    for parcel in parcels:
        for a, b in _polygon_edges(parcel.geom):
            ia = node_id(a)
            ib = node_id(b)
            if ia == ib:
                continue
            key = (ia, ib) if ia < ib else (ib, ia)
            line = LineString([nodes[ia], nodes[ib]])
            rec = edges.setdefault(
                key,
                {
                    "nodes": key,
                    "geom": line,
                    "length": float(line.length),
                    "parcels": set(),
                    "boundary": False,
                    "seed_street": False,
                },
            )
            rec["parcels"].add(parcel.parcel_id)
            endpoint_boundary_tol = max(tol * 4.0, 1e-7)
            if (
                shapely.Point(nodes[ia]).distance(boundary.boundary) <= endpoint_boundary_tol
                and shapely.Point(nodes[ib]).distance(boundary.boundary) <= endpoint_boundary_tol
            ):
                rec["boundary"] = True

    boundary_node_ids = [
        i
        for i, pt in enumerate(nodes)
        if shapely.Point(pt).distance(boundary.boundary) <= max(tol * 4.0, 1e-7)
    ]
    if len(boundary_node_ids) >= 3:
        boundary_node_ids.sort(key=lambda i: float(boundary.boundary.project(shapely.Point(nodes[i]))))
        for ia, ib in zip(boundary_node_ids, boundary_node_ids[1:] + boundary_node_ids[:1], strict=True):
            if ia == ib:
                continue
            key = (ia, ib) if ia < ib else (ib, ia)
            line = LineString([nodes[ia], nodes[ib]])
            rec = edges.setdefault(
                key,
                {
                    "nodes": key,
                    "geom": line,
                    "length": float(line.length),
                    "parcels": set(),
                    "boundary": True,
                },
            )
            rec["boundary"] = True

    if seed_street_lines:
        seed_tol = max(tol * 8.0, 1e-7)
        for rec in edges.values():
            line = rec["geom"]
            line_angle = _line_mid_angle(line)
            for seed_line in seed_street_lines:
                if line.distance(seed_line) > seed_tol:
                    continue
                if _axis_delta(line_angle, _line_mid_angle(seed_line)) > math.radians(30):
                    continue
                rec["seed_street"] = True
                break

    adjacency: dict[int, list[tuple[int, int, float]]] = {i: [] for i in range(len(nodes))}
    for edge_idx, (key, rec) in enumerate(edges.items()):
        a, b = key
        weight = float(rec["length"])
        adjacency[a].append((b, edge_idx, weight))
        adjacency[b].append((a, edge_idx, weight))
        rec["edge_idx"] = edge_idx
    return nodes, edges, adjacency


def _shortest_path_to_street(
    start: int,
    street_nodes: set[int],
    adjacency: dict[int, list[tuple[int, int, float]]],
    edge_by_idx: dict[int, tuple[int, int]],
    street_edges: set[tuple[int, int]],
) -> tuple[float, list[tuple[int, int]]]:
    if start in street_nodes:
        return 0.0, []
    heap: list[tuple[float, int]] = [(0.0, start)]
    dist = {start: 0.0}
    prev: dict[int, tuple[int, tuple[int, int]]] = {}
    while heap:
        cost, node = heapq.heappop(heap)
        if cost > dist.get(node, math.inf):
            continue
        if node in street_nodes:
            path: list[tuple[int, int]] = []
            cur = node
            while cur != start:
                old, edge_key = prev[cur]
                path.append(edge_key)
                cur = old
            path.reverse()
            return cost, path
        for nxt, edge_idx, length in adjacency.get(node, []):
            edge_key = edge_by_idx[edge_idx]
            reuse_bonus = 0.30 if edge_key in street_edges else 1.0
            new_cost = cost + length * reuse_bonus
            if new_cost < dist.get(nxt, math.inf):
                dist[nxt] = new_cost
                prev[nxt] = (node, edge_key)
                heapq.heappush(heap, (new_cost, nxt))
    return math.inf, []


def _shortest_path_to_targets(
    start: int,
    target_nodes: set[int],
    adjacency: dict[int, list[tuple[int, int, float]]],
    edge_by_idx: dict[int, tuple[int, int]],
) -> tuple[float, list[tuple[int, int]]]:
    if start in target_nodes:
        return 0.0, []
    heap: list[tuple[float, int]] = [(0.0, start)]
    dist = {start: 0.0}
    prev: dict[int, tuple[int, tuple[int, int]]] = {}
    while heap:
        cost, node = heapq.heappop(heap)
        if cost > dist.get(node, math.inf):
            continue
        if node in target_nodes:
            path: list[tuple[int, int]] = []
            cur = node
            while cur != start:
                old, edge_key = prev[cur]
                path.append(edge_key)
                cur = old
            path.reverse()
            return cost, path
        for nxt, edge_idx, length in adjacency.get(node, []):
            new_cost = cost + length
            if new_cost < dist.get(nxt, math.inf):
                dist[nxt] = new_cost
                prev[nxt] = (node, edge_by_idx[edge_idx])
                heapq.heappush(heap, (new_cost, nxt))
    return math.inf, []


def _shortest_path_between_nodes(
    start: int,
    goal: int,
    adjacency: dict[int, list[tuple[int, int, float]]],
    edge_by_idx: dict[int, tuple[int, int]],
    edge_data: dict[tuple[int, int], dict[str, Any]],
) -> tuple[float, list[tuple[int, int]]]:
    heap: list[tuple[float, int]] = [(0.0, start)]
    dist = {start: 0.0}
    prev: dict[int, tuple[int, tuple[int, int]]] = {}
    while heap:
        cost, node = heapq.heappop(heap)
        if cost > dist.get(node, math.inf):
            continue
        if node == goal:
            path: list[tuple[int, int]] = []
            cur = node
            while cur != start:
                old, edge_key = prev[cur]
                path.append(edge_key)
                cur = old
            path.reverse()
            return cost, path
        for nxt, edge_idx, length in adjacency.get(node, []):
            edge_key = edge_by_idx[edge_idx]
            rec = edge_data[edge_key]
            multiplier = 0.34 if rec.get("seed_street") else 1.0
            if rec.get("boundary"):
                multiplier = 4.0
            new_cost = cost + length * multiplier
            if new_cost < dist.get(nxt, math.inf):
                dist[nxt] = new_cost
                prev[nxt] = (node, edge_key)
                heapq.heappush(heap, (new_cost, nxt))
    return math.inf, []


def _street_edge_adjacency(street_edges: set[tuple[int, int]]) -> dict[int, set[int]]:
    selected_adj: dict[int, set[int]] = {}
    for a, b in street_edges:
        selected_adj.setdefault(a, set()).add(b)
        selected_adj.setdefault(b, set()).add(a)
    return selected_adj


def _street_component_sizes(selected_adj: dict[int, set[int]]) -> list[int]:
    seen: set[int] = set()
    component_sizes: list[int] = []
    for node in selected_adj:
        if node in seen:
            continue
        stack = [node]
        seen.add(node)
        size = 0
        while stack:
            cur = stack.pop()
            size += 1
            for nxt in selected_adj.get(cur, set()):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        component_sizes.append(size)
    return component_sizes


def _street_components(selected_adj: dict[int, set[int]]) -> list[set[int]]:
    seen: set[int] = set()
    components: list[set[int]] = []
    for node in selected_adj:
        if node in seen:
            continue
        stack = [node]
        seen.add(node)
        component: set[int] = set()
        while stack:
            cur = stack.pop()
            component.add(cur)
            for nxt in selected_adj.get(cur, set()):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        components.append(component)
    return components


def _connect_street_components(
    street_edges: set[tuple[int, int]],
    adjacency: dict[int, list[tuple[int, int, float]]],
    edge_by_idx: dict[int, tuple[int, int]],
) -> int:
    added_paths = 0
    while True:
        components = _street_components(_street_edge_adjacency(street_edges))
        if len(components) <= 1:
            return added_paths
        components.sort(key=len, reverse=True)
        target_nodes = set(components[0])
        best: tuple[float, list[tuple[int, int]]] = (math.inf, [])
        for component in components[1:]:
            for node in component:
                cost, path = _shortest_path_to_targets(node, target_nodes, adjacency, edge_by_idx)
                if path and cost < best[0]:
                    best = (cost, path)
        if not best[1]:
            return added_paths
        for edge in best[1]:
            street_edges.add(edge)
        added_paths += 1


def _select_seed_through_streets(
    nodes: list[tuple[float, float]],
    edge_data: dict[tuple[int, int], dict[str, Any]],
    adjacency: dict[int, list[tuple[int, int, float]]],
    edge_by_idx: dict[int, tuple[int, int]],
    seed_street_lines: list[LineString] | None,
    target_area: float,
) -> tuple[set[tuple[int, int]], int]:
    if not seed_street_lines:
        return set(), 0
    boundary_nodes = {
        node
        for edge, rec in edge_data.items()
        if rec.get("boundary")
        for node in edge
    }
    if len(boundary_nodes) < 2:
        return set(), 0
    selected: set[tuple[int, int]] = set()
    selected_paths = 0
    min_path_len = math.sqrt(target_area) * 3.2
    for seed_line in seed_street_lines[:8]:
        coords = list(seed_line.coords)
        if len(coords) < 2:
            continue
        endpoints = (shapely.Point(coords[0]), shapely.Point(coords[-1]))
        endpoint_nodes: list[int] = []
        for pt in endpoints:
            node = min(boundary_nodes, key=lambda candidate: shapely.Point(nodes[candidate]).distance(pt))
            endpoint_nodes.append(node)
        start, goal = endpoint_nodes
        if start == goal:
            continue
        _cost, path = _shortest_path_between_nodes(start, goal, adjacency, edge_by_idx, edge_data)
        if not path:
            continue
        path_len = sum(float(edge_data[edge]["length"]) for edge in path)
        seed_len = sum(float(edge_data[edge]["length"]) for edge in path if edge_data[edge].get("seed_street"))
        if path_len < min_path_len or seed_len / max(path_len, 1e-9) < 0.30:
            continue
        selected.update(path)
        selected_paths += 1
    return selected, selected_paths


def _node_pair_angle(nodes: list[tuple[float, float]], center: int, other: int) -> float:
    p = nodes[center]
    q = nodes[other]
    return math.atan2(q[1] - p[1], q[0] - p[0])


def _street_junction_angle_deviations(nodes: list[tuple[float, float]], selected_adj: dict[int, set[int]]) -> list[float]:
    deviations: list[float] = []
    for node, nbrs in selected_adj.items():
        if len(nbrs) < 2:
            continue
        angles = [_node_pair_angle(nodes, node, nbr) for nbr in nbrs]
        pair_devs: list[float] = []
        for i, angle in enumerate(angles):
            others = angles[:i] + angles[i + 1 :]
            if not others:
                continue
            pair_devs.append(min(abs(_axis_delta(angle, other) - math.pi * 0.5) for other in others))
        if len(nbrs) == 2 and pair_devs and min(pair_devs) > math.radians(45):
            continue
        deviations.extend(pair_devs)
    return deviations


def _street_smoothness_energy(streets: list[StreetRec]) -> float:
    total = 0.0
    length = 0.0
    for street in streets:
        coords = np.asarray(street.geom.coords, dtype=np.float64)
        if len(coords) < 3:
            continue
        second = coords[:-2] - 2.0 * coords[1:-1] + coords[2:]
        total += float(np.sum(second * second))
        length += float(street.geom.length)
    return total / max(length, 1e-9)


def _access_ratio_metrics(
    parcels: list[ParcelRec],
    parcel_edges: dict[int, list[tuple[int, int]]],
    edge_data: dict[tuple[int, int], dict[str, Any]],
    street_edges: set[tuple[int, int]],
) -> dict[str, Any]:
    ratios: list[float] = []
    for parcel in parcels:
        ratios.append(_parcel_access_ratio_value(parcel.parcel_id, parcel_edges, edge_data, street_edges))
    vals = np.asarray(ratios, dtype=np.float64)
    if not len(vals):
        return {
            "parcel_access_ratio_min": 0.0,
            "parcel_access_ratio_p05": 0.0,
            "parcel_access_ratio_p50": 0.0,
            "parcel_access_ratio_below_tau_count": 0,
        }
    return {
        "parcel_access_ratio_min": float(np.min(vals)),
        "parcel_access_ratio_p05": float(np.quantile(vals, 0.05)),
        "parcel_access_ratio_p50": float(np.quantile(vals, 0.50)),
        "parcel_access_ratio_below_tau_count": int(sum(1 for value in vals if value < CHEN_ACCESS_RATIO_THRESHOLD)),
    }


def _parcel_access_ratio_value(
    parcel_id: int,
    parcel_edges: dict[int, list[tuple[int, int]]],
    edge_data: dict[tuple[int, int], dict[str, Any]],
    street_edges: set[tuple[int, int]],
) -> float:
    edges = parcel_edges.get(parcel_id, [])
    lengths = [float(edge_data[edge]["length"]) for edge in edges]
    avg_side = float(np.mean(lengths)) if lengths else 0.0
    street_len = sum(float(edge_data[edge]["length"]) for edge in edges if edge in street_edges)
    return street_len / max(avg_side, 1e-9)


def _street_topology_metrics(
    nodes: list[tuple[float, float]],
    street_edges: set[tuple[int, int]],
    streets: list[StreetRec],
    target_area: float,
) -> dict[str, Any]:
    selected_adj = _street_edge_adjacency(street_edges)
    component_sizes = _street_component_sizes(selected_adj)
    chain_lengths = np.asarray([street.geom.length for street in streets], dtype=np.float64)
    angle_devs = np.asarray(_street_junction_angle_deviations(nodes, selected_adj), dtype=np.float64)
    total_chain_length = float(chain_lengths.sum()) if len(chain_lengths) else 0.0
    return {
        "street_graph_edge_count": len(street_edges),
        "street_graph_chain_count": len(streets),
        "street_graph_component_count": len(component_sizes),
        "street_graph_largest_component_share": float(max(component_sizes) / max(sum(component_sizes), 1)) if component_sizes else 0.0,
        "street_isolated_node_count": int(sum(1 for nbrs in selected_adj.values() if not nbrs)),
        "street_isolated_edge_count": int(sum(1 for a, b in street_edges if len(selected_adj.get(a, set())) <= 1 and len(selected_adj.get(b, set())) <= 1)),
        "street_graph_dead_end_count": int(sum(1 for nbrs in selected_adj.values() if len(nbrs) == 1)),
        "street_graph_branch_node_count": int(sum(1 for nbrs in selected_adj.values() if len(nbrs) >= 3)),
        "street_chain_length_total": total_chain_length,
        "street_chain_length_p50": float(np.quantile(chain_lengths, 0.50)) if len(chain_lengths) else 0.0,
        "street_chain_length_p90": float(np.quantile(chain_lengths, 0.90)) if len(chain_lengths) else 0.0,
        "street_short_chain_count": int(sum(1 for length in chain_lengths if length < math.sqrt(target_area) * 0.75)),
        "street_junction_angle_deviation_p50_deg": float(math.degrees(np.quantile(angle_devs, 0.50))) if len(angle_devs) else 0.0,
        "street_junction_angle_deviation_p95_deg": float(math.degrees(np.quantile(angle_devs, 0.95))) if len(angle_devs) else 0.0,
        "street_smoothness_energy": float(_street_smoothness_energy(streets)),
    }


def _edge_angle_from_node(nodes: list[tuple[float, float]], edge: tuple[int, int], node: int) -> float:
    a, b = edge
    other = b if node == a else a
    return _node_pair_angle(nodes, node, other)


def _extend_collinear_chain(
    nodes: list[tuple[float, float]],
    group_adj: dict[int, list[tuple[int, int]]],
    start_edge: tuple[int, int],
) -> set[tuple[int, int]]:
    chain = {start_edge}

    def extend(prev_node: int, node: int) -> None:
        while True:
            current_angle = _node_pair_angle(nodes, node, prev_node)
            candidates: list[tuple[float, int, tuple[int, int]]] = []
            for edge in group_adj.get(node, []):
                if edge in chain:
                    continue
                a, b = edge
                nxt = b if node == a else a
                delta = _axis_delta(current_angle, _node_pair_angle(nodes, node, nxt))
                if delta < math.radians(32):
                    candidates.append((delta, nxt, edge))
            if not candidates:
                break
            _delta, nxt, edge = min(candidates, key=lambda item: item[0])
            chain.add(edge)
            prev_node, node = node, nxt

    a, b = start_edge
    extend(a, b)
    extend(b, a)
    return chain


def _extend_collinear_ray(
    nodes: list[tuple[float, float]],
    group_adj: dict[int, list[tuple[int, int]]],
    start_node: int,
    start_edge: tuple[int, int],
) -> set[tuple[int, int]]:
    ray = {start_edge}
    a, b = start_edge
    prev_node = start_node
    node = b if start_node == a else a
    while True:
        current_angle = _node_pair_angle(nodes, node, prev_node)
        candidates: list[tuple[float, int, tuple[int, int]]] = []
        for edge in group_adj.get(node, []):
            if edge in ray:
                continue
            ea, eb = edge
            nxt = eb if node == ea else ea
            delta = _axis_delta(current_angle, _node_pair_angle(nodes, node, nxt))
            if delta < math.radians(32):
                candidates.append((delta, nxt, edge))
        if not candidates:
            break
        _delta, nxt, edge = min(candidates, key=lambda item: item[0])
        ray.add(edge)
        prev_node, node = node, nxt
    return ray


def _access_candidate_reaches(
    candidate: set[tuple[int, int]],
    group: set[int],
    parcel_edges: dict[int, list[tuple[int, int]]],
) -> set[int]:
    return {
        parcel_id
        for parcel_id in group
        if any(edge in candidate for edge in parcel_edges.get(parcel_id, []))
    }


def _access_candidate_length(candidate: set[tuple[int, int]], edge_data: dict[tuple[int, int], dict[str, Any]]) -> float:
    return sum(float(edge_data[edge]["length"]) for edge in candidate)


def _access_candidate_cost(candidate: set[tuple[int, int]], edge_data: dict[tuple[int, int], dict[str, Any]]) -> float:
    cost = 0.0
    for edge in candidate:
        rec = edge_data[edge]
        multiplier = 0.52 if rec.get("seed_street") else 1.0
        cost += float(rec["length"]) * multiplier
    return cost


def _group_access_candidates(
    nodes: list[tuple[float, float]],
    edge_data: dict[tuple[int, int], dict[str, Any]],
    parcel_edges: dict[int, list[tuple[int, int]]],
    group: set[int],
) -> list[tuple[str, set[tuple[int, int]]]]:
    group_edges = {
        edge
        for parcel_id in group
        for edge in parcel_edges.get(parcel_id, [])
        if edge in edge_data
    }
    group_adj: dict[int, list[tuple[int, int]]] = {}
    for edge in group_edges:
        a, b = edge
        group_adj.setdefault(a, []).append(edge)
        group_adj.setdefault(b, []).append(edge)

    seen: set[tuple[str, tuple[tuple[int, int], ...]]] = set()
    candidates: list[tuple[str, set[tuple[int, int]]]] = []

    def add(kind: str, edges: set[tuple[int, int]]) -> None:
        if not edges:
            return
        key = (kind, tuple(sorted(edges)))
        if key in seen:
            return
        seen.add(key)
        candidates.append((kind, edges))

    for edge in group_edges:
        add("I", _extend_collinear_chain(nodes, group_adj, edge))

    for node, incident in group_adj.items():
        for i, edge_a in enumerate(incident):
            for edge_b in incident[i + 1 :]:
                delta = _axis_delta(
                    _edge_angle_from_node(nodes, edge_a, node),
                    _edge_angle_from_node(nodes, edge_b, node),
                )
                if delta < math.radians(35) or abs(delta - math.pi * 0.5) > math.radians(35):
                    continue
                add(
                    "L",
                    _extend_collinear_ray(nodes, group_adj, node, edge_a)
                    | _extend_collinear_ray(nodes, group_adj, node, edge_b),
                )
    return candidates


def _choose_group_access_edges(
    nodes: list[tuple[float, float]],
    edge_data: dict[tuple[int, int], dict[str, Any]],
    parcel_edges: dict[int, list[tuple[int, int]]],
    group: set[int],
    street_edges: set[tuple[int, int]],
) -> tuple[set[tuple[int, int]], dict[str, int]]:
    remaining = set(group)
    chosen: set[tuple[int, int]] = set()
    counts = {"I": 0, "L": 0, "fallback": 0}
    candidates = _group_access_candidates(nodes, edge_data, parcel_edges, group)
    while remaining:
        scored: list[tuple[int, float, float, str, set[tuple[int, int]], set[int]]] = []
        for kind, edges in candidates:
            if not (edges - street_edges):
                continue
            reached = _access_candidate_reaches(edges, remaining, parcel_edges)
            if not reached:
                continue
            scored.append(
                (
                    0 if reached == remaining else 1,
                    _access_candidate_cost(edges, edge_data),
                    _access_candidate_length(edges, edge_data),
                    kind,
                    edges,
                    reached,
                )
            )
        if scored:
            _complete, _cost, _length, kind, edges, reached = min(
                scored,
                key=lambda item: (item[0], -len(item[5]), item[1], item[2], 0 if item[3] == "I" else 1),
            )
            chosen.update(edges)
            remaining -= reached
            counts[kind] += 1
            continue

        parcel_id = min(remaining)
        edges = [edge for edge in parcel_edges.get(parcel_id, []) if edge not in street_edges]
        if not edges:
            remaining.remove(parcel_id)
            continue
        edge = min(edges, key=lambda candidate: _access_candidate_cost({candidate}, edge_data))
        chosen.add(edge)
        remaining -= _access_candidate_reaches({edge}, remaining, parcel_edges)
        counts["fallback"] += 1
    return chosen, counts


def _street_graph_from_parcels(
    parcels: list[ParcelRec],
    boundary: Polygon,
    target_area: float,
    seed_street_lines: list[LineString] | None = None,
) -> tuple[list[StreetRec], dict[str, Any]]:
    tol = math.sqrt(target_area) * 0.025
    nodes, edge_data, adjacency = _build_corner_graph(parcels, boundary, tol, seed_street_lines)
    edge_by_idx = {int(rec["edge_idx"]): key for key, rec in edge_data.items()}
    seed_street_edges = {key for key, rec in edge_data.items() if rec.get("seed_street")}
    street_edges: set[tuple[int, int]] = {key for key, rec in edge_data.items() if rec["boundary"]}
    seed_through_edges, seed_through_path_count = _select_seed_through_streets(
        nodes,
        edge_data,
        adjacency,
        edge_by_idx,
        seed_street_lines,
        target_area,
    )
    street_edges.update(seed_through_edges)
    street_nodes: set[int] = set()
    for a, b in street_edges:
        street_nodes.add(a)
        street_nodes.add(b)
    component_connection_paths = _connect_street_components(street_edges, adjacency, edge_by_idx)
    street_nodes = {node for edge in street_edges for node in edge}

    parcel_edges: dict[int, list[tuple[int, int]]] = {parcel.parcel_id: [] for parcel in parcels}
    for key, rec in edge_data.items():
        for parcel_id in rec["parcels"]:
            parcel_edges[int(parcel_id)].append(key)

    def has_frontage(parcel_id: int) -> bool:
        return _parcel_access_ratio_value(parcel_id, parcel_edges, edge_data, street_edges) >= CHEN_ACCESS_RATIO_THRESHOLD

    parcel_neighbors: dict[int, set[int]] = {parcel.parcel_id: set() for parcel in parcels}
    for rec in edge_data.values():
        ids = [int(parcel_id) for parcel_id in rec["parcels"]]
        if len(ids) == 2:
            parcel_neighbors[ids[0]].add(ids[1])
            parcel_neighbors[ids[1]].add(ids[0])

    added_access_paths = 0
    unreachable_group_count = 0
    unreachable_group_max_size = 0
    access_i_shape_count = 0
    access_l_shape_count = 0
    access_greedy_fallback_count = 0
    access_non_progress_count = 0
    repair_iterations = 0
    while True:
        unreachable = {parcel.parcel_id for parcel in parcels if not has_frontage(parcel.parcel_id)}
        if not unreachable:
            break
        repair_iterations += 1
        if repair_iterations > len(parcels) * 4:
            access_non_progress_count += len(unreachable)
            break
        before_unreachable_count = len(unreachable)
        before_edge_count = len(street_edges)
        stack = [next(iter(unreachable))]
        group: set[int] = set()
        while stack:
            parcel_id = stack.pop()
            if parcel_id not in unreachable or parcel_id in group:
                continue
            group.add(parcel_id)
            stack.extend(parcel_neighbors.get(parcel_id, set()) & unreachable)
        unreachable_group_count += 1
        unreachable_group_max_size = max(unreachable_group_max_size, len(group))

        group_street_edges, access_counts = _choose_group_access_edges(nodes, edge_data, parcel_edges, group, street_edges)
        if not group_street_edges:
            break
        access_i_shape_count += access_counts["I"]
        access_l_shape_count += access_counts["L"]
        access_greedy_fallback_count += access_counts["fallback"]

        access_nodes = {node for edge in group_street_edges for node in edge}
        if not access_nodes & street_nodes:
            best_path: tuple[float, list[tuple[int, int]]] = (math.inf, [])
            for start in access_nodes:
                path_cost, path = _shortest_path_to_street(start, street_nodes, adjacency, edge_by_idx, street_edges)
                if path and path_cost < best_path[0]:
                    best_path = (path_cost, path)
            group_street_edges.update(best_path[1])

        for edge in group_street_edges:
            street_edges.add(edge)
            street_nodes.update(edge)
        component_connection_paths += _connect_street_components(street_edges, adjacency, edge_by_idx)
        street_nodes = {node for selected_edge in street_edges for node in selected_edge}
        added_access_paths += 1
        after_unreachable_count = sum(1 for parcel in parcels if not has_frontage(parcel.parcel_id))
        if after_unreachable_count >= before_unreachable_count and len(street_edges) == before_edge_count:
            access_non_progress_count += 1
            break

    streets = _merge_street_edges(nodes, edge_data, street_edges, boundary)

    metrics = {
        "corner_node_count": len(nodes),
        "corner_edge_count": len(edge_data),
        "seed_street_candidate_edge_count": int(len(seed_street_edges)),
        "seed_street_used_edge_count": int(sum(1 for edge in street_edges if edge in seed_street_edges)),
        "seed_through_street_path_count": int(seed_through_path_count),
        "seed_through_street_edge_count": int(len(seed_through_edges)),
        "street_access_path_count": added_access_paths,
        "street_component_connection_path_count": int(component_connection_paths),
        "unreachable_group_count": int(unreachable_group_count),
        "unreachable_group_max_size": int(unreachable_group_max_size),
        "access_i_shape_count": int(access_i_shape_count),
        "access_l_shape_count": int(access_l_shape_count),
        "access_greedy_fallback_count": int(access_greedy_fallback_count),
        "access_non_progress_count": int(access_non_progress_count),
        "unreachable_parcel_count": sum(1 for parcel in parcels if not has_frontage(parcel.parcel_id)),
    }
    metrics.update(_street_topology_metrics(nodes, street_edges, streets, target_area))
    metrics.update(_access_ratio_metrics(parcels, parcel_edges, edge_data, street_edges))
    return streets, metrics


def _merge_street_edges(
    nodes: list[tuple[float, float]],
    edge_data: dict[tuple[int, int], dict[str, Any]],
    street_edges: set[tuple[int, int]],
    boundary: Polygon,
) -> list[StreetRec]:
    selected_adj: dict[int, list[tuple[int, tuple[int, int]]]] = {}
    for edge in street_edges:
        if edge_data[edge]["boundary"]:
            continue
        a, b = edge
        selected_adj.setdefault(a, []).append((b, edge))
        selected_adj.setdefault(b, []).append((a, edge))

    def edge_angle(edge: tuple[int, int], from_node: int) -> float:
        a, b = edge
        other = b if from_node == a else a
        p = nodes[from_node]
        q = nodes[other]
        return math.atan2(q[1] - p[1], q[0] - p[0])

    def next_edge(prev_node: int, node: int, used: set[tuple[int, int]]) -> tuple[int, tuple[int, int]] | None:
        candidates = [(nxt, edge) for nxt, edge in selected_adj.get(node, []) if edge not in used and nxt != prev_node]
        if len(candidates) != 1:
            return None
        current_angle = edge_angle((min(prev_node, node), max(prev_node, node)), node)
        nxt, edge = candidates[0]
        delta = _axis_delta(current_angle, edge_angle(edge, node))
        if delta > math.radians(38):
            return None
        return nxt, edge

    used: set[tuple[int, int]] = set()
    chains: list[tuple[str, list[int]]] = []
    start_edges = sorted(
        street_edges,
        key=lambda edge: (
            len(selected_adj.get(edge[0], [])) == 2 and len(selected_adj.get(edge[1], [])) == 2,
            edge,
        ),
    )
    for edge in start_edges:
        if edge in used:
            continue
        if edge_data[edge]["boundary"]:
            continue
        kind = "street_access"
        a, b = edge
        used.add(edge)
        chain = [a, b]
        while True:
            found = next_edge(chain[-2], chain[-1], used)
            if found is None:
                break
            nxt, nxt_edge = found
            used.add(nxt_edge)
            chain.append(nxt)
        while True:
            found = next_edge(chain[1], chain[0], used)
            if found is None:
                break
            nxt, nxt_edge = found
            used.add(nxt_edge)
            chain.insert(0, nxt)
        chains.append((kind, chain))

    perimeter = StreetRec(1, "perimeter", LineString([(float(x), float(y)) for x, y in boundary.exterior.coords]))
    internal = [
        StreetRec(i + 2, kind, _smooth_line(LineString([nodes[node] for node in chain])))
        for i, (kind, chain) in enumerate(chains)
        if len(chain) >= 2
    ]
    return [perimeter, *internal]


def _smooth_line(line: LineString) -> LineString:
    coords = np.asarray(line.coords, dtype=np.float64)
    if len(coords) < 5 or line.length <= 1e-9:
        return line
    out = coords.copy()
    for _ in range(2):
        new = out.copy()
        for i in range(1, len(out) - 1):
            new[i] = out[i] * 0.50 + (out[i - 1] + out[i + 1]) * 0.25
        out = new
    return LineString([(float(x), float(y)) for x, y in out])


def _display_line(line: LineString) -> LineString:
    if line.length <= 1e-9:
        return line
    simplified = line.simplify(max(0.90, line.length * 0.012), preserve_topology=False)
    coords = np.asarray(simplified.coords, dtype=np.float64)
    if len(coords) < 3:
        return simplified
    out = coords.copy()
    for _ in range(4):
        new = out.copy()
        for i in range(1, len(out) - 1):
            new[i] = out[i] * 0.50 + (out[i - 1] + out[i + 1]) * 0.25
        out = new
    return LineString([(float(x), float(y)) for x, y in out])


def _display_polygon(poly: Polygon, tolerance: float) -> Polygon:
    simplified = poly.simplify(tolerance, preserve_topology=True)
    largest = _largest_polygon(simplified)
    return largest if largest is not None and largest.is_valid else poly


def _find_streamline_split(
    poly: Polygon,
    *,
    domain: Polygon,
    base_angle: float,
    level: int,
    seed: int,
    target_area: float,
    flow_strength: float,
    existing_split_lines: list[LineString],
    weights: SplitQualityWeights,
) -> tuple[str, LineString, list[Polygon]] | None:
    cx, cy = float(poly.centroid.x), float(poly.centroid.y)
    local_angle = _crossfield_angle(domain, cx, cy, base_angle)
    axis = local_angle + (math.pi * 0.5 if level % 2 else 0.0)
    span = math.sqrt(poly.area)
    attempts: list[tuple[float, float]] = []
    template_mode = poly.area < target_area * 7.5 and _aspect(poly) < 4.2
    angle_deltas = (0.0, math.radians(5), -math.radians(5), math.radians(11), -math.radians(11), math.pi * 0.5)
    offset_scales = (0.0, 0.10, -0.10, 0.22, -0.22, 0.34, -0.34)
    for angle_delta in angle_deltas:
        for offset_scale in offset_scales:
            attempts.append((axis + angle_delta, offset_scale * span))

    best: tuple[float, str, LineString, list[Polygon], dict[str, float]] | None = None
    for angle, offset in attempts:
        curved = not template_mode and level <= 5
        if curved:
            pcx, pcy = float(poly.centroid.x), float(poly.centroid.y)
            nx, ny = -math.sin(angle), math.cos(angle)
            start = (pcx + nx * offset, pcy + ny * offset)
            if not poly.contains(shapely.Point(*start)):
                continue
            curve = _trace_field_curve(
                poly,
                domain,
                base_angle,
                start,
                angle,
                seed + level * 17,
                step=max(math.sqrt(poly.area) * 0.045, 1.0),
                max_steps=44,
            )
        else:
            curve = _split_curve(poly, angle, offset, seed + level * 17, flow_strength, curved=False)
        parts = _split_polygons(poly, curve)
        if len(parts) != 2:
            continue
        small = min(p.area for p in parts)
        large = max(p.area for p in parts)
        if small < target_area * 0.45 or small / max(large, 1e-9) < 0.22:
            continue
        direction_penalty = abs(math.atan2(math.sin(angle - axis), math.cos(angle - axis))) * 0.04
        clipped = curve.intersection(poly)
        line_parts = _line_parts(clipped)
        if not line_parts:
            continue
        split_line = max(line_parts, key=lambda line: line.length)
        orthogonality = _split_orthogonality_penalty(parts, split_line)
        parallel = _parallel_split_penalty(split_line, existing_split_lines, math.sqrt(target_area) * 2.2)
        close_parallel = _parallel_split_penalty(split_line, existing_split_lines, math.sqrt(target_area) * 0.90)
        if close_parallel > 0.85:
            continue
        quality, quality_parts = _chen_split_quality(parts, domain, existing_split_lines, weights, target_area)
        score = quality - direction_penalty - orthogonality * 0.10 - parallel * 0.70
        if best is None or score > best[0]:
            split_kind = "template_split" if template_mode else "streamline_split"
            best = (score, split_kind, split_line, parts, quality_parts)
    if best is None:
        return None
    return best[1], best[2], best[3]


def _crossfield_guides(poly: Polygon, parcel_count: int, seed: int, base_angle: float) -> list[MeshGuideRec]:
    pts = _sample_polygon_points(poly, max(12, min(parcel_count, 90)), seed)
    if len(pts) < 3:
        return []
    tri = Delaunay(pts)
    guides: list[MeshGuideRec] = []
    stride = max(1, len(tri.simplices) // 60)
    scale = math.sqrt(poly.area / max(parcel_count, 1)) * 0.34
    for idx, simplex in enumerate(tri.simplices[::stride]):
        tri_pts = pts[simplex]
        cx = float(np.mean(tri_pts[:, 0]))
        cy = float(np.mean(tri_pts[:, 1]))
        if not poly.contains(shapely.Point(cx, cy)):
            continue
        angle = _crossfield_angle(poly, cx, cy, base_angle)
        for k, axis in enumerate((angle, angle + math.pi * 0.5)):
            ux, uy = math.cos(axis), math.sin(axis)
            guides.append(
                MeshGuideRec(
                    len(guides) + 1,
                    idx + 1,
                    "cross_field",
                    LineString([(cx - ux * scale, cy - uy * scale), (cx + ux * scale, cy + uy * scale)]),
                )
            )
            if k == 1 and len(guides) >= 120:
                return guides
    return guides


def _generate_crossfield_split_layout(
    *,
    boundary_spec: BoundarySpec,
    parcel_count: int,
    seed: int,
    flow_strength: float,
) -> tuple[list[StreetRec], list[ParcelRec], list[MeshGuideRec], dict[str, Any]]:
    target_area = boundary_spec.geom.area / max(parcel_count, 1)
    base_angle = _principal_angle(boundary_spec.geom, seed)
    split_weights = SplitQualityWeights()
    regions: list[tuple[Polygon, int]] = [(boundary_spec.geom, 0)]
    split_lines: list[tuple[str, LineString]] = []
    stalled = 0
    while len(regions) < parcel_count and stalled < parcel_count * 3:
        candidates = [(i, poly, level) for i, (poly, level) in enumerate(regions) if poly.area > target_area * 1.35]
        if not candidates:
            break
        i, poly, level = max(candidates, key=lambda item: item[1].area)
        result = _find_streamline_split(
            poly,
            domain=boundary_spec.geom,
            base_angle=base_angle,
            level=level,
            seed=seed + len(split_lines) * 31,
            target_area=target_area,
            flow_strength=flow_strength,
            existing_split_lines=[line for _kind, line in split_lines],
            weights=split_weights,
        )
        if result is None:
            stalled += 1
            regions[i] = (poly, level + 1)
            continue
        kind, line, parts = result
        regions.pop(i)
        regions.extend((part, level + 1) for part in sorted(parts, key=lambda p: p.area, reverse=True))
        split_lines.append((kind, line))

    sorted_regions = sorted(regions, key=lambda item: (item[0].centroid.y, item[0].centroid.x))
    short_edge_threshold = math.sqrt(target_area) * 0.12
    short_edges_before = sum(_short_edge_count(poly, short_edge_threshold) for poly, _level in sorted_regions)
    attempted_regions = [(_clean_short_edges(poly, short_edge_threshold), level) for poly, level in sorted_regions]
    attempted_polys = [poly for poly, _level in attempted_regions]
    attempted_coverage = sum(poly.area for poly in attempted_polys) / max(boundary_spec.geom.area, 1e-9)
    cleanup_applied = _overlap_count(attempted_polys) == 0 and attempted_coverage > 0.995
    cleaned_regions = attempted_regions if cleanup_applied else sorted_regions
    short_edges_after = sum(_short_edge_count(poly, short_edge_threshold) for poly, _level in cleaned_regions)
    parcels = [
        ParcelRec(parcel_id=i + 1, block_id=i + 1, geom=poly)
        for i, (poly, _level) in enumerate(cleaned_regions)
    ]
    _faces, face_metrics = _noded_faces_for_parcels(parcels, boundary_spec.geom)
    parcels, geometry_metrics = _optimize_shared_vertices(parcels, boundary_spec.geom, target_area)

    coarse_street_seed_count = max(3, min(14, int(math.sqrt(max(parcel_count, 1)) * 1.5)))
    coarse_street_lines = [line for _kind, line in split_lines[:coarse_street_seed_count]]
    streets, graph_metrics = _street_graph_from_parcels(
        parcels,
        boundary_spec.geom,
        target_area,
        seed_street_lines=coarse_street_lines,
    )
    guides = _crossfield_guides(boundary_spec.geom, parcel_count, seed, base_angle)
    guide_id = len(guides) + 1
    for kind, line in split_lines:
        guides.append(MeshGuideRec(guide_id, 0, kind, line))
        guide_id += 1
    metrics = _layout_metrics(streets, parcels, parcel_count, boundary_spec)
    metrics["boundary"] = boundary_spec.name
    metrics["mesh_guide_count"] = len(guides)
    metrics["split_curve_count"] = len(split_lines)
    metrics["split_weight_size"] = split_weights.size
    metrics["split_weight_regularity"] = split_weights.regularity
    metrics["split_weight_access"] = split_weights.access
    metrics["streamline_split_count"] = sum(1 for kind, _line in split_lines if kind == "streamline_split")
    metrics["template_split_count"] = sum(1 for kind, _line in split_lines if kind == "template_split")
    metrics["coarse_street_seed_count"] = int(len(coarse_street_lines))
    metrics["streamline_curvature_mean"] = _mean_line_curvature([line for kind, line in split_lines if kind == "streamline_split"])
    metrics["short_edge_threshold"] = float(short_edge_threshold)
    metrics["short_edge_count_before"] = int(short_edges_before)
    metrics["short_edge_count_after"] = int(short_edges_after)
    metrics["short_edge_cleanup_applied"] = bool(cleanup_applied)
    metrics.update(face_metrics)
    metrics.update(geometry_metrics)
    metrics["near_parallel_split_count"] = _near_parallel_split_count(
        [line for _kind, line in split_lines],
        math.sqrt(target_area) * 1.2,
        math.radians(12),
    )
    metrics.update(graph_metrics)
    metrics["crossfield_base_angle"] = float(base_angle)
    metrics["coverage_rate"] = float(sum(p.geom.area for p in parcels) / max(boundary_spec.geom.area, 1e-9))
    return streets, parcels, guides, metrics


def _split_fraction(seed: int, level: int, index: int) -> float:
    value = math.sin((seed + 1) * 12.9898 + (level + 3) * 78.233 + (index + 5) * 37.719)
    return float(np.clip(0.50 + 0.11 * value, 0.38, 0.62))


def _flow_warp(x: float, y: float, width: float, height: float, seed: int, strength: float) -> tuple[float, float]:
    u = x / max(width, 1e-9)
    v = y / max(height, 1e-9)
    phase = seed * 0.131
    v_fade = math.sin(math.pi * max(0.0, min(1.0, v))) ** 2
    u_fade = math.sin(math.pi * max(0.0, min(1.0, u))) ** 2
    dx = width * strength * v_fade * (
        0.72 * math.sin((v * 1.10 + phase) * math.tau)
        + 0.28 * math.sin((v * 2.35 - phase * 0.7) * math.tau)
    )
    dy = height * strength * 0.32 * u_fade * math.sin((u * 1.45 - phase * 0.5) * math.tau)
    return x + dx, y + dy


def _map_point(u: float, v: float, width: float, height: float, seed: int, strength: float) -> tuple[float, float]:
    return _flow_warp(u * width, v * height, width, height, seed, strength)


def _uv_line(
    samples: list[tuple[float, float]],
    width: float,
    height: float,
    seed: int,
    strength: float,
) -> LineString:
    return LineString([_map_point(u, v, width, height, seed, strength) for u, v in samples])


def _uv_poly(
    u0: float,
    u1: float,
    v0: float,
    v1: float,
    width: float,
    height: float,
    seed: int,
    strength: float,
) -> Polygon:
    samples: list[tuple[float, float]] = []
    for u in np.linspace(u0, u1, 7):
        samples.append((float(u), v0))
    for v in np.linspace(v0, v1, 7)[1:]:
        samples.append((u1, float(v)))
    for u in np.linspace(u1, u0, 7)[1:]:
        samples.append((float(u), v1))
    for v in np.linspace(v1, v0, 7)[1:-1]:
        samples.append((u0, float(v)))
    return Polygon([_map_point(u, v, width, height, seed, strength) for u, v in samples])


def _graph_split_samples(
    block: SplitBlock,
    *,
    split_u: bool,
    base: float,
    seed: int,
    width: float,
    height: float,
) -> list[tuple[float, float]]:
    """Find a low-cost layered path near a desired split line.

    This is deliberately small, but it mirrors the paper's useful separation:
    topology first, then a graph search through a richer local mesh to find a
    street-shaped path that follows the district flow without losing endpoints.
    """
    layers = 17
    offsets = np.linspace(-1.0, 1.0, 9)
    span = (block.u1 - block.u0) if split_u else (block.v1 - block.v0)
    wiggle = span * 0.11
    phase = seed * 0.173 + block.block_id * 0.619
    coords: list[list[tuple[float, float]]] = []
    for t in np.linspace(0.0, 1.0, layers):
        if split_u:
            v = block.v0 + (block.v1 - block.v0) * float(t)
            preferred = base + wiggle * math.sin((float(t) * 1.15 + phase) * math.tau)
            lo = block.u0 + span * 0.10
            hi = block.u1 - span * 0.10
            coords.append([(float(np.clip(preferred + off * span * 0.035, lo, hi)), v) for off in offsets])
        else:
            u = block.u0 + (block.u1 - block.u0) * float(t)
            preferred = base + wiggle * math.sin((float(t) * 1.15 + phase) * math.tau)
            lo = block.v0 + span * 0.10
            hi = block.v1 - span * 0.10
            coords.append([(u, float(np.clip(preferred + off * span * 0.035, lo, hi))) for off in offsets])

    costs = np.full((layers, len(offsets)), np.inf, dtype=np.float64)
    prev = np.full((layers, len(offsets)), -1, dtype=np.int32)
    costs[0, :] = 0.0
    for i in range(1, layers):
        for j, uv in enumerate(coords[i]):
            best_cost = math.inf
            best_k = -1
            for k, old_uv in enumerate(coords[i - 1]):
                dx = (uv[0] - old_uv[0]) * width
                dy = (uv[1] - old_uv[1]) * height
                step = math.hypot(dx, dy)
                drift = abs((uv[0] if split_u else uv[1]) - base) * max(width, height)
                bend = 0.0
                if i > 1 and prev[i - 1, k] >= 0:
                    older_uv = coords[i - 2][int(prev[i - 1, k])]
                    a0 = math.atan2((old_uv[1] - older_uv[1]) * height, (old_uv[0] - older_uv[0]) * width)
                    a1 = math.atan2((uv[1] - old_uv[1]) * height, (uv[0] - old_uv[0]) * width)
                    bend = abs(math.atan2(math.sin(a1 - a0), math.cos(a1 - a0))) * 18.0
                cost = costs[i - 1, k] + step + drift * 0.28 + bend
                if cost < best_cost:
                    best_cost = cost
                    best_k = k
            costs[i, j] = best_cost
            prev[i, j] = best_k

    j = int(np.argmin(costs[-1]))
    path: list[tuple[float, float]] = []
    for i in range(layers - 1, -1, -1):
        path.append(coords[i][j])
        j = int(prev[i, j]) if i > 0 else j
    path.reverse()
    if split_u:
        path[0] = (base, block.v0)
        path[-1] = (base, block.v1)
    else:
        path[0] = (block.u0, base)
        path[-1] = (block.u1, base)
    return path


def _make_street_blocks(
    *,
    parcel_count: int,
    width: float,
    height: float,
    seed: int,
    flow_strength: float,
) -> tuple[list[SplitBlock], list[StreetRec], list[MeshGuideRec]]:
    street_strength = 0.0
    guide_strength = flow_strength
    streets = [
        StreetRec(1, "perimeter", _uv_line([(0, 0), (1, 0)], width, height, seed, street_strength)),
        StreetRec(2, "perimeter", _uv_line([(1, 0), (1, 1)], width, height, seed, street_strength)),
        StreetRec(3, "perimeter", _uv_line([(1, 1), (0, 1)], width, height, seed, street_strength)),
        StreetRec(4, "perimeter", _uv_line([(0, 1), (0, 0)], width, height, seed, street_strength)),
    ]
    guides: list[MeshGuideRec] = []
    blocks = [SplitBlock(1, 0.0, 1.0, 0.0, 1.0, parcel_count, 0)]
    next_block_id = 2
    next_street_id = 5
    max_block_parcels = 8
    while True:
        candidates = [
            (i, b)
            for i, b in enumerate(blocks)
            if b.target > max_block_parcels and min(b.u1 - b.u0, b.v1 - b.v0) > 0.16
        ]
        if not candidates:
            break
        i, block = max(candidates, key=lambda item: item[1].target)
        blocks.pop(i)
        du = block.u1 - block.u0
        dv = block.v1 - block.v0
        split_u = du * width >= dv * height
        frac = _split_fraction(seed, block.level, block.block_id)
        target_a = max(1, int(round(block.target * frac)))
        target_b = max(1, block.target - target_a)
        if split_u:
            um = block.u0 + du * frac
            line = _uv_line(
                [(um, float(v)) for v in np.linspace(block.v0, block.v1, 9)],
                width,
                height,
                seed,
                street_strength,
            )
            guide_line = _uv_line(
                _graph_split_samples(block, split_u=True, base=um, seed=seed, width=width, height=height),
                width,
                height,
                seed,
                guide_strength,
            )
            blocks.append(SplitBlock(next_block_id, block.u0, um, block.v0, block.v1, target_a, block.level + 1))
            next_block_id += 1
            blocks.append(SplitBlock(next_block_id, um, block.u1, block.v0, block.v1, target_b, block.level + 1))
            next_block_id += 1
        else:
            vm = block.v0 + dv * frac
            line = _uv_line(
                [(float(u), vm) for u in np.linspace(block.u0, block.u1, 9)],
                width,
                height,
                seed,
                street_strength,
            )
            guide_line = _uv_line(
                _graph_split_samples(block, split_u=False, base=vm, seed=seed, width=width, height=height),
                width,
                height,
                seed,
                guide_strength,
            )
            blocks.append(SplitBlock(next_block_id, block.u0, block.u1, block.v0, vm, target_a, block.level + 1))
            next_block_id += 1
            blocks.append(SplitBlock(next_block_id, block.u0, block.u1, vm, block.v1, target_b, block.level + 1))
            next_block_id += 1
        streets.append(StreetRec(next_street_id, "street", line))
        guides.append(MeshGuideRec(next_street_id, block.block_id, "graph_split", guide_line))
        next_street_id += 1
    return blocks, streets, guides


def _parcel_grid(block: SplitBlock) -> tuple[int, int]:
    aspect = (block.u1 - block.u0) / max(block.v1 - block.v0, 1e-9)
    if aspect >= 1.0:
        rows = 2 if block.target > 1 else 1
        cols = max(1, int(math.ceil(block.target / rows)))
    else:
        cols = 2 if block.target > 1 else 1
        rows = max(1, int(math.ceil(block.target / cols)))
    return cols, rows


def generate_chen_layout(
    *,
    width: float,
    height: float,
    parcel_count: int,
    seed: int,
    flow_strength: float,
    boundary: str = "rectangle",
) -> tuple[list[StreetRec], list[ParcelRec], list[MeshGuideRec], dict[str, Any]]:
    boundary_spec = _boundary_preset(boundary, width, height, seed)
    if boundary in {"triangle", "oval", "island"}:
        return _generate_crossfield_split_layout(
            boundary_spec=boundary_spec,
            parcel_count=parcel_count,
            seed=seed,
            flow_strength=flow_strength,
        )
    blocks, streets, guides = _make_street_blocks(
        parcel_count=parcel_count,
        width=width,
        height=height,
        seed=seed,
        flow_strength=flow_strength,
    )
    parcels: list[ParcelRec] = []
    next_parcel_id = 1
    for block in blocks:
        cols, rows = _parcel_grid(block)
        made = 0
        for r in range(rows):
            for c in range(cols):
                if made >= block.target:
                    continue
                u0 = block.u0 + (block.u1 - block.u0) * c / cols
                u1 = block.u0 + (block.u1 - block.u0) * (c + 1) / cols
                v0 = block.v0 + (block.v1 - block.v0) * r / rows
                v1 = block.v0 + (block.v1 - block.v0) * (r + 1) / rows
                geom = _uv_poly(u0, u1, v0, v1, width, height, seed, 0.0)
                geom = _largest_polygon(geom.intersection(boundary_spec.geom))
                if geom is not None and geom.is_valid and geom.area > 1e-9:
                    parcels.append(ParcelRec(next_parcel_id, block.block_id, geom))
                    next_parcel_id += 1
                    made += 1
    clipped_streets: list[StreetRec] = []
    for street in streets:
        geom = street.geom.intersection(boundary_spec.geom)
        parts = _line_parts(geom)
        for part in parts:
            clipped_streets.append(StreetRec(street.street_id, street.kind, part))
    clipped_guides: list[MeshGuideRec] = []
    for guide in guides:
        for part in _line_parts(guide.geom.intersection(boundary_spec.geom)):
            clipped_guides.append(MeshGuideRec(guide.guide_id, guide.block_id, guide.kind, part))
    metrics = _layout_metrics(clipped_streets, parcels, parcel_count, boundary_spec)
    metrics["boundary"] = boundary
    metrics["mesh_guide_count"] = len(clipped_guides)
    return clipped_streets, parcels, clipped_guides, metrics


def _aspect(poly: Polygon) -> float:
    rect = poly.minimum_rotated_rectangle
    coords = np.asarray(rect.exterior.coords[:-1], dtype=np.float64)
    edges = np.linalg.norm(np.roll(coords, -1, axis=0) - coords, axis=1)
    vals = edges[edges > 1e-9]
    return float(vals.max() / max(vals.min(), 1e-9)) if len(vals) else 0.0


def _min_rect_width(poly: Polygon) -> float:
    rect = poly.minimum_rotated_rectangle
    coords = np.asarray(rect.exterior.coords[:-1], dtype=np.float64)
    edges = np.linalg.norm(np.roll(coords, -1, axis=0) - coords, axis=1)
    vals = edges[edges > 1e-9]
    return float(vals.min()) if len(vals) else 0.0


def _layout_metrics(
    streets: list[StreetRec],
    parcels: list[ParcelRec],
    target: int,
    boundary: BoundarySpec,
) -> dict[str, Any]:
    street_geom = unary_union([s.geom for s in streets])
    overlaps = 0
    for i, parcel in enumerate(parcels):
        for other in parcels[i + 1 :]:
            if parcel.geom.intersection(other.geom).area > 1e-7:
                overlaps += 1
    frontage_tolerance = math.sqrt(max(sum(p.geom.area for p in parcels), 1e-9)) * 0.006
    frontage = sum(
        1 for parcel in parcels if parcel.geom.distance(street_geom) <= frontage_tolerance
    )
    aspects = np.asarray([_aspect(parcel.geom) for parcel in parcels], dtype=np.float64)
    widths = np.asarray([_min_rect_width(parcel.geom) for parcel in parcels], dtype=np.float64)
    target_area = boundary.geom.area / max(target, 1)
    street_lengths = np.asarray([street.geom.length for street in streets], dtype=np.float64)
    metrics = {
        "target_parcels": int(target),
        "parcel_count": int(len(parcels)),
        "street_count": int(len(streets)),
        "street_length_total": float(np.sum(street_lengths)) if len(street_lengths) else 0.0,
        "street_length_p50": float(np.quantile(street_lengths, 0.50)) if len(street_lengths) else 0.0,
        "street_short_line_count": int(sum(1 for length in street_lengths if length < math.sqrt(target_area) * 0.75)),
        "street_smoothness_energy": float(_street_smoothness_energy(streets)),
        "parcel_overlap_count": int(overlaps),
        "frontage_count": int(frontage),
        "frontage_rate": float(frontage / max(len(parcels), 1)),
        "parcel_aspect_p95": float(np.quantile(aspects, 0.95)) if len(aspects) else 0.0,
        "parcel_aspect_p99": float(np.quantile(aspects, 0.99)) if len(aspects) else 0.0,
        "parcel_min_width_p01": float(np.quantile(widths, 0.01)) if len(widths) else 0.0,
        "parcel_min_width_p05": float(np.quantile(widths, 0.05)) if len(widths) else 0.0,
        "boundary_area": float(boundary.geom.area),
        "covered_area": float(sum(p.geom.area for p in parcels)),
        "hard_metrics_pass": bool(
            len(parcels) >= int(target * 0.70)
            and overlaps == 0
            and frontage == len(parcels)
        ),
    }
    metrics.update(_chen_irregularity_metrics(parcels))
    return metrics


def _render_svg(
    path: Path,
    streets: list[StreetRec],
    parcels: list[ParcelRec],
    guides: list[MeshGuideRec],
    boundary: BoundarySpec,
    width: float,
    height: float,
) -> None:
    pad = max(width, height) * 0.06
    minx, miny = -pad, -pad
    maxx, maxy = width + pad, height + pad
    sx = 900 / max(maxx - minx, 1e-9)
    sy = 900 / max(maxy - miny, 1e-9)
    scale = min(sx, sy)

    def pxy(x: float, y: float) -> tuple[float, float]:
        return (x - minx) * scale, 900 - (y - miny) * scale

    parts = ['<svg xmlns="http://www.w3.org/2000/svg" width="900" height="900" viewBox="0 0 900 900">']
    parts.append('<rect width="900" height="900" fill="#f4f0e6"/>')
    bpts = " ".join(f"{x:.2f},{y:.2f}" for x, y in [pxy(float(a), float(b)) for a, b in boundary.geom.exterior.coords])
    parts.append(f'<polygon points="{bpts}" fill="#efe7d3" stroke="#5d6f73" stroke-width="1.8"/>')
    display_tolerance = max(width, height) * 0.0120
    for parcel in parcels:
        geom = _display_polygon(parcel.geom, display_tolerance)
        pts = " ".join(f"{x:.2f},{y:.2f}" for x, y in [pxy(float(a), float(b)) for a, b in geom.exterior.coords])
        parts.append(f'<polygon points="{pts}" fill="#fbf7ec" stroke="#9d907f" stroke-width="0.8"/>')
    for guide in guides:
        coords = [pxy(float(x), float(y)) for x, y in guide.geom.coords]
        d = " ".join(f"{x:.2f},{y:.2f}" for x, y in coords)
        color = "#d7b88f" if guide.kind == "cross_field" else "#b8c4c1"
        opacity = "0.28" if guide.kind == "cross_field" else "0.22"
        width_px = "0.8" if guide.kind == "cross_field" else "0.8"
        parts.append(f'<polyline points="{d}" fill="none" stroke="{color}" stroke-width="{width_px}" stroke-linecap="round" opacity="{opacity}"/>')
    for street in streets:
        display_geom = _display_line(street.geom)
        coords = [pxy(float(x), float(y)) for x, y in display_geom.coords]
        d = " ".join(f"{x:.2f},{y:.2f}" for x, y in coords)
        width_px = 5 if street.kind == "perimeter" else 4 if street.kind == "streamline_split" else 2
        casing = "#7b969b" if street.kind == "streamline_split" else "#8fa2a5"
        parts.append(f'<polyline points="{d}" fill="none" stroke="#8fa2a5" stroke-width="{width_px + 2}" stroke-linecap="round"/>')
        parts[-1] = f'<polyline points="{d}" fill="none" stroke="{casing}" stroke-width="{width_px + 2}" stroke-linecap="round"/>'
        parts.append(f'<polyline points="{d}" fill="none" stroke="#fdfbf4" stroke-width="{width_px}" stroke-linecap="round"/>')
    parts.append("</svg>")
    path.write_text("\n".join(parts))


def _render_png(
    path: Path,
    streets: list[StreetRec],
    parcels: list[ParcelRec],
    guides: list[MeshGuideRec],
    boundary: BoundarySpec,
    width: float,
    height: float,
) -> None:
    size = 900
    img = np.full((size, size, 3), (244, 240, 230), dtype=np.uint8)
    pad = max(width, height) * 0.06
    minx, miny = -pad, -pad
    maxx, maxy = width + pad, height + pad
    scale = min(size / max(maxx - minx, 1e-9), size / max(maxy - miny, 1e-9))

    def pix(pt: tuple[float, float]) -> tuple[int, int]:
        return int((pt[0] - minx) * scale), int(size - 1 - (pt[1] - miny) * scale)

    def draw_line(coords: list[tuple[float, float]], color: tuple[int, int, int], width_px: int) -> None:
        for a, b in zip(coords[:-1], coords[1:], strict=False):
            x0, y0 = pix(a)
            x1, y1 = pix(b)
            steps = max(abs(x1 - x0), abs(y1 - y0), 1)
            for i in range(steps + 1):
                t = i / steps
                x = int(round(x0 * (1 - t) + x1 * t))
                y = int(round(y0 * (1 - t) + y1 * t))
                img[max(0, y - width_px) : min(size, y + width_px + 1), max(0, x - width_px) : min(size, x + width_px + 1)] = color

    draw_line([(float(x), float(y)) for x, y in boundary.geom.exterior.coords], (93, 111, 115), 2)
    display_tolerance = max(width, height) * 0.0120
    for parcel in parcels:
        geom = _display_polygon(parcel.geom, display_tolerance)
        draw_line([(float(x), float(y)) for x, y in geom.exterior.coords], (155, 143, 126), 0)
    for guide in guides:
        if guide.kind == "cross_field":
            draw_line([(float(x), float(y)) for x, y in guide.geom.coords], (218, 193, 159), 0)
        else:
            draw_line([(float(x), float(y)) for x, y in guide.geom.coords], (198, 207, 203), 0)
    for street in streets:
        display_geom = _display_line(street.geom)
        coords = [(float(x), float(y)) for x, y in display_geom.coords]
        outer = (123, 150, 155) if street.kind == "streamline_split" else (143, 162, 165)
        draw_line(coords, outer, 3 if street.kind == "perimeter" else 2)
        draw_line(coords, (255, 253, 246), 1)
    _write_png(img, path)


def build_chen_proto(args: argparse.Namespace) -> dict[str, Any]:
    args.out_dir.mkdir(parents=True, exist_ok=True)
    boundary_name = getattr(args, "boundary", "rectangle")
    boundary = _boundary_preset(boundary_name, args.width, args.height, args.seed)
    streets, parcels, guides, metrics = generate_chen_layout(
        width=args.width,
        height=args.height,
        parcel_count=args.parcel_count,
        seed=args.seed,
        flow_strength=args.flow_strength,
        boundary=boundary_name,
    )
    (args.out_dir / "layout_metrics.json").write_text(json.dumps(metrics, indent=2))
    (args.out_dir / "boundary.geojson").write_text(
        json.dumps(_feature_collection([_geom_feature(boundary.geom, {"boundary": boundary.name})]))
    )
    (args.out_dir / "roads.geojson").write_text(
        json.dumps(_feature_collection([_geom_feature(s.geom, {"street_id": s.street_id, "kind": s.kind}) for s in streets]))
    )
    (args.out_dir / "mesh_guides.geojson").write_text(
        json.dumps(_feature_collection([_geom_feature(g.geom, {"guide_id": g.guide_id, "block_id": g.block_id, "kind": g.kind}) for g in guides]))
    )
    (args.out_dir / "parcels.geojson").write_text(
        json.dumps(_feature_collection([_geom_feature(p.geom, {"parcel_id": p.parcel_id, "block_id": p.block_id}) for p in parcels]))
    )
    faces, _face_metrics = _noded_faces_for_parcels(parcels, boundary.geom)
    (args.out_dir / "faces.geojson").write_text(
        json.dumps(_feature_collection([_geom_feature(f.geom, {"face_id": f.face_id, "parcel_id": f.parcel_id}) for f in faces]))
    )
    _render_svg(args.out_dir / "chen_layout.svg", streets, parcels, guides, boundary, args.width, args.height)
    _render_png(args.out_dir / "chen_layout.png", streets, parcels, guides, boundary, args.width, args.height)
    (args.out_dir / "proto_manifest.json").write_text(
        json.dumps(
            {
                "layout": "chen-2024-isolated-proto",
                "boundary": boundary_name,
                "width": args.width,
                "height": args.height,
                "parcel_count": args.parcel_count,
                "outputs": {
                    "boundary": "boundary.geojson",
                    "roads": "roads.geojson",
                    "mesh_guides": "mesh_guides.geojson",
                    "parcels": "parcels.geojson",
                    "faces": "faces.geojson",
                    "metrics": "layout_metrics.json",
                    "png": "chen_layout.png",
                    "svg": "chen_layout.svg",
                },
            },
            indent=2,
        )
    )
    print(f"wrote {args.out_dir}")
    return metrics


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--width", type=float, default=220.0)
    ap.add_argument("--height", type=float, default=150.0)
    ap.add_argument("--parcel-count", type=int, default=180)
    ap.add_argument("--flow-strength", type=float, default=0.045)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--boundary", choices=["rectangle", "triangle", "oval", "island"], default="rectangle")
    args = ap.parse_args()
    metrics = build_chen_proto(args)
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
