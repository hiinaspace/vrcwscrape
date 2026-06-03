"""Physically nested hierarchy labels for the static VRChat world map.

Toponymy is good at topic naming, but its cluster layers are not guaranteed to be
a strict tree in the final 2D map. This pass builds a map-first hierarchy:

* top layer: connected land/island components from the final 2D coordinates;
* middle layer: spatially constrained neighborhoods inside each island;
* fine layer: spatially constrained districts inside each neighborhood.

The output matches ``mapgen-app-export``'s expected ``layer_*.parquet`` files.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import httpx
import numpy as np
import polars as pl
from scipy import ndimage

from mapgen.common import DEFAULT_LLM_MODEL, l2_normalize, ollama_generate
from mapgen.raster_poly import _disk, _points_to_mask, estimate_cell_size
from mapgen.regions import _median_nn

LABEL_SCHEMA = {
    "type": "object",
    "properties": {
        "topic_name": {"type": "string"},
        "category": {"type": "string"},
    },
    "required": ["topic_name", "category"],
}

FORBIDDEN_LABEL_WORDS = {"and", "for", "with"}


@dataclass(frozen=True)
class LayerSpec:
    index: int
    name: str
    cluster_id: np.ndarray
    parent_id: np.ndarray | None = None


FALLBACK_LABEL_RE = re.compile(r"^(?:Island|Neighborhood|District) \d+$")
TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9'_-]{2,}|[\u3040-\u30ff\u3400-\u9fff]{2,}")
STOPWORDS = {
    "and",
    "are",
    "for",
    "from",
    "has",
    "have",
    "https",
    "into",
    "just",
    "like",
    "new",
    "not",
    "official",
    "the",
    "this",
    "that",
    "with",
    "world",
    "worlds",
    "vrchat",
    "www",
}


def _point_cells(
    xy: np.ndarray, *, x0: float, y0: float, cell_size: float, shape: tuple[int, int]
) -> tuple[np.ndarray, np.ndarray]:
    rows = np.floor((xy[:, 1] - y0) / cell_size).astype(np.int64)
    cols = np.floor((xy[:, 0] - x0) / cell_size).astype(np.int64)
    rows = np.clip(rows, 0, shape[0] - 1)
    cols = np.clip(cols, 0, shape[1] - 1)
    return rows, cols


def _compact_ids(labels: np.ndarray) -> np.ndarray:
    """Relabel positive ids to 0..N-1 by descending cluster size."""
    labels = labels.astype(np.int64, copy=True)
    ids, counts = np.unique(labels, return_counts=True)
    order = [int(i) for i in ids[np.argsort(-counts)] if i >= 0]
    mapping = {old: new for new, old in enumerate(order)}
    return np.array([mapping.get(int(v), -1) for v in labels], dtype=np.int32)


def _nearest_large_component(
    xy: np.ndarray, labels: np.ndarray, small_ids: set[int], large_ids: list[int]
) -> np.ndarray:
    if not small_ids or not large_ids:
        return labels
    centroids = np.stack([xy[labels == cid].mean(axis=0) for cid in large_ids])
    out = labels.copy()
    for cid in sorted(small_ids):
        idx = np.where(labels == cid)[0]
        if not len(idx):
            continue
        d2 = ((xy[idx, None, :] - centroids[None, :, :]) ** 2).sum(axis=2)
        out[idx] = np.array(large_ids, dtype=np.int64)[d2.argmin(axis=1)]
    return out


def _component_labels(
    xy: np.ndarray,
    *,
    raster_max_dim: int,
    raster_nn_cells: float,
    dilate_cells: int,
    close_cells: int,
    min_component_size: int,
) -> tuple[np.ndarray, dict[str, float | int]]:
    nn = _median_nn(xy)
    cell_size = estimate_cell_size(
        xy, max_dim=raster_max_dim, median_nn=nn, nn_cells=raster_nn_cells
    )
    pad_cells = max(2, int(dilate_cells + close_cells + 4))
    seed_mask, x0, y0 = _points_to_mask(xy, cell_size=cell_size, pad_cells=pad_cells)
    rows, cols = _point_cells(
        xy, x0=x0, y0=y0, cell_size=cell_size, shape=seed_mask.shape
    )

    mask = seed_mask.copy()
    if dilate_cells > 0:
        mask = ndimage.binary_dilation(mask, structure=_disk(dilate_cells))
    if close_cells > 0:
        mask = ndimage.binary_closing(mask, structure=_disk(close_cells))
    mask |= seed_mask
    mask = ndimage.binary_fill_holes(mask)

    labeled_mask, n_components = ndimage.label(
        mask, structure=np.ones((3, 3), dtype=bool)
    )
    labels = labeled_mask[rows, cols].astype(np.int64) - 1

    # Very small point islands produce unreadable polygons and unstable labels.
    # Attach singletons/pairs to the nearest real component, but keep all
    # components above the caller's threshold as independent map islands.
    ids, counts = np.unique(labels, return_counts=True)
    size_by_id = {int(i): int(c) for i, c in zip(ids, counts, strict=True) if i >= 0}
    if size_by_id and min_component_size > 1:
        large_ids = [
            cid for cid, count in size_by_id.items() if count >= min_component_size
        ]
        if not large_ids:
            large_ids = [max(size_by_id, key=size_by_id.get)]
        small_ids = {cid for cid in size_by_id if cid not in set(large_ids)}
        labels = _nearest_large_component(xy, labels, small_ids, large_ids)

    labels = _compact_ids(labels)
    info: dict[str, float | int] = {
        "median_nn": float(nn),
        "cell_size": float(cell_size),
        "grid_width": int(mask.shape[1]),
        "grid_height": int(mask.shape[0]),
        "occupied_cells": int(seed_mask.sum()),
        "filled_cells": int(mask.sum()),
        "raw_components": int(n_components),
        "components": int(len(set(labels.tolist()))),
    }
    return labels, info


def _standardize(x: np.ndarray) -> np.ndarray:
    x = x.astype(np.float32, copy=False)
    mu = x.mean(axis=0, keepdims=True)
    sigma = x.std(axis=0, keepdims=True)
    sigma[sigma < 1e-6] = 1.0
    return (x - mu) / sigma


def _feature_matrix(
    xy: np.ndarray,
    emb: np.ndarray,
    *,
    latent_dim: int,
    spatial_weight: float,
    latent_weight: float,
    seed: int,
) -> np.ndarray:
    parts = [_standardize(xy) * float(spatial_weight)]
    n_components = min(int(latent_dim), emb.shape[1] - 1, emb.shape[0] - 1)
    if n_components > 0 and latent_weight > 0:
        from sklearn.decomposition import TruncatedSVD

        print(f"latent projection: TruncatedSVD({n_components})")
        svd = TruncatedSVD(n_components=n_components, random_state=seed)
        latent = svd.fit_transform(emb)
        parts.append(_standardize(latent) * float(latent_weight))
    return np.hstack(parts).astype(np.float32)


def _child_count(
    n: int, *, target_size: int, min_child_size: int, max_children: int
) -> int:
    if n < max(min_child_size * 2, target_size):
        return 1
    k = max(1, int(round(n / max(target_size, 1))))
    k = min(k, max_children, max(1, n // max(min_child_size, 1)))
    return max(1, k)


def _subdivide(
    parent: np.ndarray,
    features: np.ndarray,
    *,
    target_size: int,
    min_child_size: int,
    max_children: int,
    seed: int,
    label: str,
) -> np.ndarray:
    from sklearn.cluster import MiniBatchKMeans

    out = np.full(parent.shape, -1, dtype=np.int32)
    next_id = 0
    parent_ids, counts = np.unique(parent, return_counts=True)
    ordered_parents = parent_ids[np.argsort(-counts)]
    for parent_id in ordered_parents:
        idx = np.where(parent == parent_id)[0]
        k = _child_count(
            len(idx),
            target_size=target_size,
            min_child_size=min_child_size,
            max_children=max_children,
        )
        if k <= 1:
            out[idx] = next_id
            next_id += 1
            continue
        km = MiniBatchKMeans(
            n_clusters=k,
            random_state=seed + int(parent_id) * 131,
            batch_size=min(8192, max(1024, len(idx))),
            n_init="auto",
            reassignment_ratio=0.01,
        )
        local = km.fit_predict(features[idx])
        local_ids, local_counts = np.unique(local, return_counts=True)
        for local_id in local_ids[np.argsort(-local_counts)]:
            out[idx[local == local_id]] = next_id
            next_id += 1
    print(f"{label}: {next_id:,} clusters")
    return out


def _representative_idx(emb: np.ndarray, members: np.ndarray, k: int) -> list[int]:
    centroid = emb[members].mean(axis=0)
    norm = np.linalg.norm(centroid)
    if norm > 0:
        centroid = centroid / norm
    sims = emb[members] @ centroid
    order = np.argsort(-sims)[:k]
    return members[order].tolist()


def _tokens(text: str) -> list[str]:
    return [
        tok
        for tok in TOKEN_RE.findall((text or "").lower())
        if tok not in STOPWORDS and not tok.isdigit()
    ]


def _cluster_keyphrases(
    texts: list[str],
    members: np.ndarray,
    *,
    max_terms: int,
    max_docs: int = 700,
) -> list[str]:
    if len(members) > max_docs:
        take = np.linspace(0, len(members) - 1, max_docs).astype(np.int64)
        docs = [texts[int(members[i])] for i in take]
    else:
        docs = [texts[int(i)] for i in members]
    unigram: Counter[str] = Counter()
    bigram: Counter[str] = Counter()
    for doc in docs:
        toks = _tokens(doc)[:80]
        unigram.update(toks)
        bigram.update(
            f"{a} {b}" for a, b in zip(toks, toks[1:], strict=False) if a != b
        )

    scored: list[tuple[str, float]] = []
    scored.extend((term, count * 1.6) for term, count in bigram.items() if count >= 2)
    scored.extend((term, float(count)) for term, count in unigram.items() if count >= 2)
    scored.sort(key=lambda kv: (-kv[1], kv[0]))
    out: list[str] = []
    seen_words: set[str] = set()
    for term, _ in scored:
        key = term.lower()
        if key in seen_words:
            continue
        out.append(term)
        seen_words.add(key)
        if len(out) >= max_terms:
            break
    return out


def _load_hint_layers(
    hint_topo_dir: Path | None, world_ids: list[str]
) -> list[list[str]]:
    if hint_topo_dir is None:
        return []
    layer_paths = sorted(
        hint_topo_dir.glob("layer_*.parquet"),
        key=lambda p: int(p.stem.split("_")[1]),
    )
    if not layer_paths:
        print(f"  note: no hint layer_*.parquet in {hint_topo_dir}")
        return []
    base = pl.DataFrame({"world_id": world_ids})
    layers: list[list[str]] = []
    for lp in layer_paths:
        layer = pl.read_parquet(lp)
        if "topic_name" not in layer.columns:
            continue
        aligned = base.join(
            layer.select("world_id", "topic_name"), on="world_id", how="left"
        )
        layers.append([str(v or "") for v in aligned["topic_name"].to_list()])
    print(f"loaded {len(layers)} Toponymy hint layers from {hint_topo_dir}")
    return layers


def _top_hint_labels(
    hint_layers: list[list[str]],
    members: np.ndarray,
    *,
    max_hints: int,
) -> list[tuple[str, int]]:
    counts: Counter[str] = Counter()
    for layer in hint_layers:
        for i in members:
            label = layer[int(i)].strip()
            if label and not FALLBACK_LABEL_RE.match(label):
                counts[label] += 1
    return counts.most_common(max_hints)


def _label_key(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", label.lower()).strip()


def _hint_candidate(
    hints: list[tuple[str, int]],
    fallback: str,
    seen_keys: set[str],
    *,
    max_words: int,
    max_chars: int,
) -> str | None:
    for hint, _ in hints:
        candidate = _clean_label(
            hint, fallback, max_words=max_words, max_chars=max_chars
        )
        key = _label_key(candidate)
        if candidate != fallback and key and key not in seen_keys:
            return candidate
    return None


def _parse_label(raw: str) -> tuple[str, str]:
    text = raw.strip()
    text = re.sub(r"^```(?:json)?", "", text).strip()
    text = re.sub(r"```$", "", text).strip()
    obj: object | None = None
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{[^{}]*\}", text, re.DOTALL)
        if m:
            try:
                obj = json.loads(m.group(0))
            except json.JSONDecodeError:
                obj = None
    if isinstance(obj, list) and obj:
        obj = obj[0]
    if isinstance(obj, dict):
        return str(obj.get("topic_name", "")).strip(), str(
            obj.get("category", "")
        ).strip()
    return text, "misc"


def _fallback_label(level_name: str, cid: int) -> str:
    base = {
        "island": "Island",
        "neighborhood": "Neighborhood",
        "district": "District",
    }.get(level_name, "Region")
    return f"{base} {cid}"


def _clean_label(raw: str, fallback: str, max_words: int, max_chars: int) -> str:
    label = (raw or "").strip().strip("\"'`")
    label = re.sub(r"[\r\n\t]+", " ", label)
    label = re.sub(r"[,;:]+", " ", label)
    words = [w for w in label.split() if w.lower() not in FORBIDDEN_LABEL_WORDS]
    if not words:
        return fallback
    words = words[:max_words]
    label = " ".join(words)
    if len(label) > max_chars:
        label = label[:max_chars].rstrip()
    return label or fallback


def _label_prompt(
    *,
    level_name: str,
    samples: str,
    parent_hint: str,
    keyphrases: list[str],
    topo_hints: list[tuple[str, int]],
    sibling_labels: list[str],
    name_instructions: str,
) -> str:
    parent_line = f"\nParent region: {parent_hint}" if parent_hint else ""
    keyword_line = ""
    if keyphrases:
        keyword_line = "\nKeywords for this group: " + ", ".join(keyphrases)
    hint_line = ""
    if topo_hints:
        hints = ", ".join(f"{name} ({count})" for name, count in topo_hints)
        hint_line = "\nExisting topic labels inside this area: " + hints
    avoid_line = ""
    if sibling_labels:
        avoid_line = "\nAlready-used sibling map labels to avoid: " + ", ".join(
            sibling_labels
        )
    return f"""You are an expert at classifying VRChat worlds into map regions.
You are naming one physically local {level_name} in a Google-Maps-like latent map.
Name the shared theme of these worlds, not the algorithm or the map shape.
{parent_line}
{keyword_line}
{hint_line}
{avoid_line}

Representative worlds:
{samples}

Use existing topic labels and keywords as stronger evidence than generic titles
like "world", "my world", or "test".
If this is a broad island, choose a broad place-like label. If this is inside a
parent region, choose a more specific label that distinguishes it from siblings.
Return one JSON object with keys "topic_name" and "category".
{name_instructions}"""


def _cluster_labels(
    layer: LayerSpec,
    *,
    emb: np.ndarray,
    names: list[str],
    texts: list[str],
    parent_names: list[str] | None,
    hint_layers: list[list[str]],
    args: argparse.Namespace,
) -> tuple[dict[int, str], dict[int, dict[str, object]], dict[int, list[str]]]:
    labels: dict[int, str] = {}
    entries: dict[int, dict[str, object]] = {}
    exemplars: dict[int, list[str]] = {}
    ids, counts = np.unique(layer.cluster_id, return_counts=True)
    ordered = [
        (int(cid), int(count))
        for cid, count in zip(
            ids[np.argsort(-counts)], counts[np.argsort(-counts)], strict=True
        )
        if int(cid) >= 0
    ]
    labeled = 0
    seen_by_parent: dict[int, list[str]] = defaultdict(list)
    client = None if args.no_label else httpx.Client(timeout=args.llm_timeout)
    try:
        for cid, size in ordered:
            members = np.where(layer.cluster_id == cid)[0]
            rep = _representative_idx(emb, members, args.samples_per_cluster)
            exemplars[cid] = [names[i] for i in rep]
            fallback = _fallback_label(layer.name, cid)
            topic_name = fallback
            category = "misc"
            parent_key = (
                int(layer.parent_id[members[0]])
                if layer.parent_id is not None and len(members)
                else -1
            )
            sibling_labels = seen_by_parent[parent_key][-12:]
            sibling_keys = {_label_key(label) for label in seen_by_parent[parent_key]}
            keyphrases = _cluster_keyphrases(
                texts, members, max_terms=args.max_keyphrases
            )
            topo_hints = _top_hint_labels(
                hint_layers, members, max_hints=args.max_hint_labels
            )
            should_label = (
                not args.no_label
                and size >= args.min_label_size
                and labeled < args.max_label_clusters
            )
            if should_label:
                samples = "\n".join(
                    f"- {names[i]}: {texts[i].replace(chr(10), ' ')[:180]}" for i in rep
                )
                parent_hint = ""
                if parent_names:
                    parent_hint = parent_names[members[0]]
                try:
                    raw = ollama_generate(
                        _label_prompt(
                            level_name=layer.name,
                            samples=samples,
                            parent_hint=parent_hint,
                            keyphrases=keyphrases,
                            topo_hints=topo_hints,
                            sibling_labels=sibling_labels,
                            name_instructions=args.name_instructions,
                        ),
                        model=args.llm_model,
                        base_url=args.ollama_url,
                        fmt=LABEL_SCHEMA,
                        options={"temperature": 0.15, "num_predict": 512},
                        client=client,
                        timeout=args.llm_timeout,
                    )
                    raw_name, category = _parse_label(raw)
                    topic_name = _clean_label(
                        raw_name,
                        fallback,
                        max_words=args.max_label_words,
                        max_chars=args.max_label_chars,
                    )
                    if _label_key(topic_name) in sibling_keys:
                        hint_candidate = _hint_candidate(
                            topo_hints,
                            fallback,
                            sibling_keys,
                            max_words=args.max_label_words,
                            max_chars=args.max_label_chars,
                        )
                        if hint_candidate:
                            topic_name = hint_candidate
                    labeled += 1
                except Exception as err:  # noqa: BLE001
                    print(f"  !! label failed for {layer.name} {cid}: {err}")
                    topic_name = fallback
            elif topo_hints:
                hint_candidate = _hint_candidate(
                    topo_hints,
                    fallback,
                    sibling_keys,
                    max_words=args.max_label_words,
                    max_chars=args.max_label_chars,
                )
                if hint_candidate:
                    topic_name = hint_candidate
            labels[cid] = topic_name
            seen_by_parent[parent_key].append(topic_name)
            entries[cid] = {
                "label": topic_name,
                "category": category or "misc",
                "size": size,
                "labeled": should_label and topic_name != fallback,
            }
            print(f"  {layer.name} {cid:>4} (n={size:>6}): {topic_name}")
    finally:
        if client is not None:
            client.close()
    return labels, entries, exemplars


def _write_layer(
    out_dir: Path,
    layer: LayerSpec,
    world_ids: list[str],
    labels: dict[int, str],
) -> None:
    topic_names = [
        labels.get(int(cid), _fallback_label(layer.name, int(cid)))
        for cid in layer.cluster_id
    ]
    pl.DataFrame(
        {
            "world_id": world_ids,
            "cluster_id": pl.Series(layer.cluster_id.astype(np.int32)),
            "topic_name": topic_names,
        }
    ).write_parquet(out_dir / f"layer_{layer.index}.parquet")


def _write_exemplars(
    path: Path,
    layers: list[LayerSpec],
    summaries: dict[int, dict[int, dict[str, object]]],
    exemplars: dict[int, dict[int, list[str]]],
    *,
    max_clusters_per_layer: int,
    max_samples: int,
) -> None:
    lines = ["# Physical hierarchy exemplars", ""]
    for layer in layers:
        rows = sorted(
            summaries[layer.index].items(),
            key=lambda kv: int(kv[1].get("size", 0)),
            reverse=True,
        )[:max_clusters_per_layer]
        lines.append(f"## Layer {layer.index}: {layer.name}")
        for cid, entry in rows:
            lines.append(
                f"### {entry.get('label')} "
                f"(id={cid}, n={entry.get('size')}, {entry.get('category')})"
            )
            lines.extend(
                f"- {s}" for s in exemplars[layer.index].get(cid, [])[:max_samples]
            )
            lines.append("")
    path.write_text("\n".join(lines))


def _read_inputs(
    *,
    coords_path: Path,
    embeddings_path: Path,
    meta_path: Path,
    limit: int | None,
) -> tuple[list[str], list[str], list[str], np.ndarray, np.ndarray]:
    emb_all = l2_normalize(np.load(embeddings_path).astype(np.float32))
    meta = pl.read_parquet(meta_path)
    cols = ["world_id", "name"]
    if "text" in meta.columns:
        cols.append("text")
    meta = meta.select(cols).with_row_index("_emb_idx")
    coords = pl.read_parquet(coords_path).select("world_id", "x", "y")
    joined = meta.join(coords, on="world_id", how="inner")
    if joined.height != meta.height:
        print(f"  note: {meta.height - joined.height:,} rows missing coords")
    if limit is not None:
        joined = joined.head(limit)
    idx = joined["_emb_idx"].to_numpy()
    world_ids = joined["world_id"].to_list()
    names = [str(v or "") for v in joined["name"].to_list()]
    if "text" in joined.columns:
        texts = [str(v or "") for v in joined["text"].to_list()]
    else:
        texts = names
    xy = joined.select("x", "y").to_numpy().astype(np.float64)
    emb = emb_all[idx]
    return world_ids, names, texts, xy, emb


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--coords", type=Path, required=True)
    ap.add_argument("--embeddings", type=Path, required=True)
    ap.add_argument("--meta", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument(
        "--hint-topo-dir",
        type=Path,
        default=None,
        help=(
            "optional existing Toponymy layer directory; its labels are used as "
            "semantic hints when naming physical clusters"
        ),
    )
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--seed", type=int, default=42)

    ap.add_argument("--component-raster-max-dim", type=int, default=2048)
    ap.add_argument("--component-raster-nn-cells", type=float, default=2.0)
    ap.add_argument("--component-dilate-cells", type=int, default=5)
    ap.add_argument("--component-close-cells", type=int, default=2)
    ap.add_argument(
        "--min-component-size",
        type=int,
        default=3,
        help=(
            "point components smaller than this are attached to the nearest "
            "larger component"
        ),
    )

    ap.add_argument("--mid-target-size", type=int, default=6000)
    ap.add_argument("--fine-target-size", type=int, default=1200)
    ap.add_argument("--min-child-size", type=int, default=120)
    ap.add_argument("--max-children", type=int, default=24)
    ap.add_argument("--latent-dim", type=int, default=16)
    ap.add_argument("--spatial-weight", type=float, default=2.5)
    ap.add_argument("--latent-weight", type=float, default=2.0)

    ap.add_argument("--samples-per-cluster", type=int, default=24)
    ap.add_argument("--max-keyphrases", type=int, default=18)
    ap.add_argument("--max-hint-labels", type=int, default=12)
    ap.add_argument("--max-label-clusters", type=int, default=500)
    ap.add_argument("--min-label-size", type=int, default=3)
    ap.add_argument("--max-label-words", type=int, default=3)
    ap.add_argument("--max-label-chars", type=int, default=28)
    ap.add_argument("--llm-model", default=DEFAULT_LLM_MODEL)
    ap.add_argument("--ollama-url", default=None)
    ap.add_argument("--llm-timeout", type=float, default=300.0)
    ap.add_argument("--no-label", action="store_true")
    ap.add_argument(
        "--name-instructions",
        default=(
            "Make topic_name a terse map label: 2-3 words, Title Case, at most "
            "28 characters. Output only the short label as the topic_name value. "
            "Never a sentence. Do not use and, for, with, commas, or generic "
            "words like Worlds unless no better label exists."
        ),
    )
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    world_ids, names, texts, xy, emb = _read_inputs(
        coords_path=args.coords,
        embeddings_path=args.embeddings,
        meta_path=args.meta,
        limit=args.limit,
    )
    n = len(world_ids)
    if n == 0:
        raise SystemExit("no points after aligning coords and metadata")
    print(f"physical hierarchy on {n:,} worlds (coords={args.coords.name})")
    hint_layers = _load_hint_layers(args.hint_topo_dir, world_ids)

    island_id, component_info = _component_labels(
        xy,
        raster_max_dim=args.component_raster_max_dim,
        raster_nn_cells=args.component_raster_nn_cells,
        dilate_cells=args.component_dilate_cells,
        close_cells=args.component_close_cells,
        min_component_size=args.min_component_size,
    )
    print(
        "islands: "
        f"{component_info['components']:,} components "
        f"(raw={component_info['raw_components']:,}, "
        f"grid={component_info['grid_width']}x{component_info['grid_height']}, "
        f"cell={component_info['cell_size']:.5g})"
    )

    features = _feature_matrix(
        xy,
        emb,
        latent_dim=args.latent_dim,
        spatial_weight=args.spatial_weight,
        latent_weight=args.latent_weight,
        seed=args.seed,
    )
    neighborhood_id = _subdivide(
        island_id,
        features,
        target_size=args.mid_target_size,
        min_child_size=args.min_child_size,
        max_children=args.max_children,
        seed=args.seed,
        label="neighborhoods",
    )
    district_id = _subdivide(
        neighborhood_id,
        features,
        target_size=args.fine_target_size,
        min_child_size=args.min_child_size,
        max_children=args.max_children,
        seed=args.seed + 101,
        label="districts",
    )

    # layer_0 is deepest/fine, last layer is coarsest. This matches
    # mapgen-app-export's default of using the two coarsest layers for polygons.
    layers = [
        LayerSpec(0, "district", district_id, parent_id=neighborhood_id),
        LayerSpec(1, "neighborhood", neighborhood_id, parent_id=island_id),
        LayerSpec(2, "island", island_id),
    ]

    layer_labels: dict[int, dict[int, str]] = {}
    summaries: dict[int, dict[int, dict[str, object]]] = {}
    exemplars: dict[int, dict[int, list[str]]] = {}
    parent_names: list[str] | None = None
    for layer in reversed(layers):
        print(f"labeling layer {layer.index} ({layer.name})")
        labels, entries, ex = _cluster_labels(
            layer,
            emb=emb,
            names=names,
            texts=texts,
            parent_names=parent_names,
            hint_layers=hint_layers,
            args=args,
        )
        layer_labels[layer.index] = labels
        summaries[layer.index] = entries
        exemplars[layer.index] = ex
        parent_names = [labels.get(int(cid), "") for cid in layer.cluster_id]

    for layer in layers:
        _write_layer(args.out_dir, layer, world_ids, layer_labels[layer.index])
        print(f"  wrote {args.out_dir / f'layer_{layer.index}.parquet'}")

    summary = {
        "coords": str(args.coords),
        "point_count": n,
        "component_info": component_info,
        "layers": [
            {
                "layer": layer.index,
                "name": layer.name,
                "n_topics": int(len(summaries[layer.index])),
                "topics": [
                    entry["label"]
                    for _, entry in sorted(
                        summaries[layer.index].items(),
                        key=lambda kv: int(kv[1].get("size", 0)),
                        reverse=True,
                    )
                ],
            }
            for layer in layers
        ],
        "params": {
            "mid_target_size": args.mid_target_size,
            "fine_target_size": args.fine_target_size,
            "min_child_size": args.min_child_size,
            "max_children": args.max_children,
            "latent_dim": args.latent_dim,
            "spatial_weight": args.spatial_weight,
            "latent_weight": args.latent_weight,
            "hint_topo_dir": str(args.hint_topo_dir) if args.hint_topo_dir else None,
        },
    }
    (args.out_dir / "hierarchy_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2)
    )
    _write_exemplars(
        args.out_dir / "physical_hierarchy_exemplars.md",
        layers,
        summaries,
        exemplars,
        max_clusters_per_layer=80,
        max_samples=12,
    )
    print(f"  wrote {args.out_dir / 'hierarchy_summary.json'}")
    print(f"  wrote {args.out_dir / 'physical_hierarchy_exemplars.md'}")


if __name__ == "__main__":
    main()
