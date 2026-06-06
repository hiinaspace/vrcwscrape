"""Run parameterized LocalMAP dimensional-reduction sweeps.

This command is intentionally artifact-first: every variant writes coordinates and
a shared metrics file that can be inspected before any app export is produced.
"""
# ruff: noqa: E501

from __future__ import annotations

import argparse
import inspect
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from scipy.spatial import cKDTree

from mapgen.common import l2_normalize


@dataclass(frozen=True)
class SweepPreset:
    name: str
    n_neighbors: int | None
    mn_ratio: float
    fp_ratio: float
    low_dist_thres: float
    num_iters: int | None = None
    distance: str = "euclidean"
    init: str = "pca"


PRESETS: dict[str, SweepPreset] = {
    "baseline": SweepPreset("baseline", 15, 0.5, 2.0, 10.0),
    "auto_global": SweepPreset("auto_global", 30, 0.5, 2.0, 10.0),
    "more_islands": SweepPreset("more_islands", 10, 0.25, 2.0, 8.0),
    "open_local": SweepPreset("open_local", 15, 0.5, 4.0, 8.0),
    "local_declump": SweepPreset("local_declump", 15, 0.5, 4.0, 4.0, 650),
    "smooth_global": SweepPreset("smooth_global", 30, 1.0, 2.0, 10.0),
}


def _parse_optional_int(value: str) -> int | None:
    if value.lower() in {"none", "auto", "null"}:
        return None
    return int(value)


def _resolve_presets(names: str) -> list[SweepPreset]:
    out = []
    for name in (n.strip() for n in names.split(",") if n.strip()):
        if name not in PRESETS:
            raise SystemExit(f"unknown LocalMAP preset {name!r}")
        out.append(PRESETS[name])
    if not out:
        raise SystemExit("at least one preset is required")
    return out


def _constructor_kwargs(cls: type, preset: SweepPreset, seed: int) -> dict[str, Any]:
    requested = {
        "n_components": 2,
        "n_neighbors": preset.n_neighbors,
        "MN_ratio": preset.mn_ratio,
        "FP_ratio": preset.fp_ratio,
        "low_dist_thres": preset.low_dist_thres,
        "num_iters": preset.num_iters,
        "distance": preset.distance,
        "metric": preset.distance,
        "random_state": seed,
    }
    sig = inspect.signature(cls)
    accepts_kwargs = any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    )
    if accepts_kwargs:
        return {k: v for k, v in requested.items() if v is not None}
    return {
        k: v
        for k, v in requested.items()
        if v is not None and k in sig.parameters
    }


def run_localmap_variant(
    emb: np.ndarray,
    preset: SweepPreset,
    *,
    seed: int,
) -> np.ndarray:
    import pacmap

    if not hasattr(pacmap, "LocalMAP"):
        raise RuntimeError("installed pacmap has no LocalMAP; upgrade pacmap")
    kwargs = _constructor_kwargs(pacmap.LocalMAP, preset, seed)
    reducer = pacmap.LocalMAP(**kwargs)
    return reducer.fit_transform(emb, init=preset.init)


def _knn_indices(x: np.ndarray, k: int) -> np.ndarray:
    k = max(1, min(k, len(x) - 1))
    _dist, idx = cKDTree(x).query(x, k=k + 1, workers=-1)
    return idx[:, 1:]


def _semantic_recall(high_knn: np.ndarray, low_xy: np.ndarray) -> float:
    if len(low_xy) < 2:
        return 1.0
    low_knn = _knn_indices(low_xy, high_knn.shape[1])
    recalls = [
        len(set(a.tolist()).intersection(b.tolist())) / high_knn.shape[1]
        for a, b in zip(high_knn, low_knn, strict=True)
    ]
    return float(np.mean(recalls))


def _density_metrics(xy: np.ndarray) -> dict[str, float]:
    if len(xy) < 2:
        return {"density_q50": 0.0, "density_q99": 0.0, "density_pressure": 0.0}
    dist, _idx = cKDTree(xy).query(xy, k=2, workers=-1)
    nn = np.maximum(dist[:, 1], 1e-9)
    density = 1.0 / (nn * nn)
    q50 = float(np.quantile(density, 0.50))
    q99 = float(np.quantile(density, 0.99))
    return {
        "density_q50": q50,
        "density_q99": q99,
        "density_pressure": float(q99 / max(q50, 1e-12)),
    }


def variant_metrics(
    emb: np.ndarray,
    xy: np.ndarray,
    *,
    high_knn: np.ndarray,
    metric_idx: np.ndarray,
    baseline_pressure: float | None = None,
    baseline_recall: float | None = None,
) -> dict[str, float | bool]:
    sample_xy = xy[metric_idx]
    out = _density_metrics(sample_xy)
    recall = _semantic_recall(high_knn, sample_xy)
    out["semantic_knn_recall"] = recall
    if baseline_pressure is not None and baseline_recall is not None:
        pressure_gain = (baseline_pressure - out["density_pressure"]) / max(
            baseline_pressure, 1e-12
        )
        recall_drop = baseline_recall - recall
        out["density_pressure_improvement"] = float(pressure_gain)
        out["semantic_recall_drop"] = float(recall_drop)
        out["passes_selection_rule"] = bool(pressure_gain >= 0.15 and recall_drop <= 0.05)
    return out


def run_sweep(args: argparse.Namespace) -> dict[str, Any]:
    args.out_dir.mkdir(parents=True, exist_ok=True)
    emb = l2_normalize(np.load(args.embeddings))
    if args.limit and args.limit < len(emb):
        emb = emb[: args.limit]
    ids = pl.read_parquet(args.meta).select("world_id").head(len(emb))
    rng = np.random.default_rng(args.seed)
    if args.metric_sample_size and args.metric_sample_size < len(emb):
        metric_idx = np.sort(
            rng.choice(len(emb), size=args.metric_sample_size, replace=False)
        )
    else:
        metric_idx = np.arange(len(emb), dtype=np.int64)
    high_knn = _knn_indices(emb[metric_idx], args.knn_metric_k)

    metrics: dict[str, Any] = {
        "point_count": int(len(emb)),
        "embedding_dimensions": int(emb.shape[1]),
        "metric_sample_size": int(len(metric_idx)),
        "knn_metric_k": int(min(args.knn_metric_k, max(len(emb) - 1, 1))),
        "variants": {},
    }
    baseline_pressure = None
    baseline_recall = None
    for preset in _resolve_presets(args.presets):
        t0 = time.time()
        coords = run_localmap_variant(emb, preset, seed=args.seed)
        coords = np.asarray(coords, dtype=np.float32)
        out = ids.with_columns(
            x=pl.Series(coords[:, 0]),
            y=pl.Series(coords[:, 1]),
        )
        coord_name = f"coords_localmap_{preset.name}.parquet"
        out.write_parquet(args.out_dir / coord_name)
        vm = variant_metrics(
            emb,
            coords.astype(np.float64),
            high_knn=high_knn,
            metric_idx=metric_idx,
            baseline_pressure=baseline_pressure,
            baseline_recall=baseline_recall,
        )
        vm.update(
            {
                "coords": coord_name,
                "seconds": float(time.time() - t0),
                "n_neighbors": preset.n_neighbors,
                "MN_ratio": preset.mn_ratio,
                "FP_ratio": preset.fp_ratio,
                "low_dist_thres": preset.low_dist_thres,
                "num_iters": preset.num_iters,
                "distance": preset.distance,
                "init": preset.init,
            }
        )
        if baseline_pressure is None:
            baseline_pressure = float(vm["density_pressure"])
            baseline_recall = float(vm["semantic_knn_recall"])
        metrics["variants"][preset.name] = vm
        print(f"  {preset.name}: wrote {coord_name} in {vm['seconds']:.1f}s")

    selectable = [
        (name, data)
        for name, data in metrics["variants"].items()
        if data.get("passes_selection_rule")
    ]
    if selectable:
        metrics["recommended_variant"] = min(
            selectable, key=lambda item: item[1]["density_pressure"]
        )[0]
    else:
        metrics["recommended_variant"] = "baseline"
        metrics["selection_note"] = (
            "No variant improved density pressure by at least 15% without "
            "dropping semantic kNN recall by more than 5%; keep current LocalMAP."
        )
    (args.out_dir / "dr_metrics.json").write_text(json.dumps(metrics, indent=2))
    return metrics


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--embeddings", type=Path, required=True, help="embeddings.npy")
    ap.add_argument("--meta", type=Path, required=True, help="embed_meta.parquet")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument(
        "--presets",
        default="baseline,auto_global,more_islands,open_local,local_declump,smooth_global",
    )
    ap.add_argument("--n-neighbors", type=_parse_optional_int)
    ap.add_argument("--MN-ratio", type=float)
    ap.add_argument("--FP-ratio", type=float)
    ap.add_argument("--low-dist-thres", type=float)
    ap.add_argument("--num-iters", type=int)
    ap.add_argument("--distance", default="euclidean")
    ap.add_argument("--init", default="pca")
    ap.add_argument("--name", default="custom")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--knn-metric-k", type=int, default=15)
    ap.add_argument("--metric-sample-size", type=int, default=20000)
    args = ap.parse_args()

    if args.n_neighbors is not None or args.MN_ratio is not None:
        PRESETS[args.name] = SweepPreset(
            args.name,
            args.n_neighbors,
            0.5 if args.MN_ratio is None else args.MN_ratio,
            2.0 if args.FP_ratio is None else args.FP_ratio,
            10.0 if args.low_dist_thres is None else args.low_dist_thres,
            args.num_iters,
            args.distance,
            args.init,
        )
        args.presets = args.name
    run_sweep(args)


if __name__ == "__main__":
    main()
