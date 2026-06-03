"""Hybrid hierarchy: physical islands with per-island Toponymy interiors.

The physical hierarchy gives useful land/island components, but its internal
spatial+latent k-means labels are weaker than Toponymy's topic labels. This pass
keeps the physical island layer as the coarsest map geography, then reruns
Toponymy independently inside large islands so mid/fine regions cannot span
unrelated islands.

Output matches ``mapgen-app-export``'s expected ``layer_*.parquet`` files:

* layer_2: physical island ids + physical island labels.
* layer_1: per-large-island Toponymy coarse topics; small islands stay singleton.
* layer_0: per-large-island Toponymy finer topics; small islands stay singleton.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from mapgen.common import DEFAULT_LLM_MODEL

LABEL_SUFFIXES = {
    "world",
    "worlds",
    "hub",
    "hubs",
    "hangout",
    "hangouts",
    "space",
    "spaces",
    "zone",
    "zones",
    "place",
    "places",
    "area",
    "areas",
    "map",
    "maps",
}


@dataclass
class LocalLayer:
    cluster_id: np.ndarray
    label_by_cluster: dict[int, str]


def _discover_layers(topo_dir: Path) -> list[Path]:
    layers = sorted(
        topo_dir.glob("layer_*.parquet"),
        key=lambda p: int(p.stem.split("_")[1]),
    )
    if not layers:
        raise SystemExit(f"no layer_*.parquet in {topo_dir}")
    return layers


def _mode_label(values: list[str], fallback: str) -> str:
    cleaned = [_short_label(v) for v in values if _short_label(v)]
    if not cleaned:
        return fallback
    return Counter(cleaned).most_common(1)[0][0]


def _short_label(s: str | None, max_words: int = 4, max_chars: int = 32) -> str:
    words = (s or "").strip().strip("\"'`").split()
    while len(words) > 1 and words[-1].strip(".,").lower() in LABEL_SUFFIXES:
        words.pop()
    if len(words) > max_words:
        words = words[:max_words]
    out = " ".join(words)
    if len(out) > max_chars:
        out = out[:max_chars].rstrip()
    return out


def _standardize(x: np.ndarray) -> np.ndarray:
    x = x.astype(np.float32, copy=False)
    mu = x.mean(axis=0, keepdims=True)
    sigma = x.std(axis=0, keepdims=True)
    sigma[sigma < 1e-6] = 1.0
    return (x - mu) / sigma


def _assign_noise_by_map(
    cluster_id: np.ndarray, document_map: np.ndarray
) -> np.ndarray:
    """Assign Toponymy noise points to the nearest local 2D cluster centroid."""
    out = cluster_id.astype(np.int64, copy=True)
    cids = sorted(int(c) for c in np.unique(out).tolist() if int(c) >= 0)
    if not cids:
        out[:] = 0
        return out
    noise = np.where(out < 0)[0]
    if not len(noise):
        return out
    features = _standardize(document_map)
    centroids = np.stack([features[out == cid].mean(axis=0) for cid in cids])
    d2 = ((features[noise, None, :] - centroids[None, :, :]) ** 2).sum(axis=2)
    out[noise] = np.array(cids, dtype=np.int64)[d2.argmin(axis=1)]
    return out


def _layer_from_toponymy(
    layer: Any, document_map: np.ndarray, fallback: str
) -> LocalLayer:
    raw = getattr(layer, "cluster_labels", None)
    if raw is None:
        raise ValueError("Toponymy layer has no cluster_labels")
    raw_id = np.asarray(raw, dtype=np.int64)
    assigned = _assign_noise_by_map(raw_id, document_map)
    names = list(getattr(layer, "topic_name_vector", []))
    if len(names) != len(raw_id):
        names = [""] * len(raw_id)

    label_by_cluster: dict[int, str] = {}
    for cid in sorted(int(c) for c in np.unique(assigned).tolist() if int(c) >= 0):
        # Prefer names from core members, then fall back to all assigned members.
        core = [names[i] for i in np.where(raw_id == cid)[0]]
        all_members = [names[i] for i in np.where(assigned == cid)[0]]
        label_by_cluster[cid] = _mode_label(core, "") or _mode_label(
            all_members, fallback
        )
    return LocalLayer(cluster_id=assigned, label_by_cluster=label_by_cluster)


def _singleton_layer(n: int, label: str) -> LocalLayer:
    return LocalLayer(
        cluster_id=np.zeros(n, dtype=np.int64), label_by_cluster={0: label}
    )


def _globalize(
    *,
    local: LocalLayer,
    global_indices: np.ndarray,
    out_id: np.ndarray,
    out_name: list[str],
    next_id: int,
    fallback: str,
) -> tuple[int, list[str]]:
    local_ids, counts = np.unique(local.cluster_id, return_counts=True)
    order = [
        int(cid)
        for cid in local_ids[np.argsort(-counts)].tolist()
        if int(cid) >= 0
    ]
    labels: list[str] = []
    for cid in order:
        gid = next_id
        next_id += 1
        label = _short_label(local.label_by_cluster.get(cid), fallback) or fallback
        mask = local.cluster_id == cid
        target = global_indices[mask]
        out_id[target] = gid
        for i in target.tolist():
            out_name[i] = label
        labels.append(label)
    return next_id, labels


def _write_layer(
    out_dir: Path,
    layer_index: int,
    world_ids: list[str],
    cluster_id: np.ndarray,
    topic_name: list[str],
) -> None:
    pl.DataFrame(
        {
            "world_id": world_ids,
            "cluster_id": pl.Series(cluster_id.astype(np.int32)),
            "topic_name": topic_name,
        }
    ).write_parquet(out_dir / f"layer_{layer_index}.parquet")


def _run_toponymy(
    *,
    emb: np.ndarray,
    document_map: np.ndarray,
    texts: list[str],
    namer: Any,
    text_embedding_model: Any,
    min_clusters: int,
) -> list[Any]:
    from toponymy import Toponymy, ToponymyClusterer

    clusterer = ToponymyClusterer(min_clusters=min_clusters, verbose=True)
    clusterer.fit(document_map, emb)
    topic_model = Toponymy(
        llm_wrapper=namer,
        text_embedding_model=text_embedding_model,
        clusterer=clusterer,
        object_description="VRChat world",
        corpus_description="collection of social VR worlds in VRChat",
        verbose=True,
    )
    topic_model.fit(texts, emb, document_map)
    return list(topic_model.cluster_layers_)


def _build_namer(args: argparse.Namespace) -> Any:
    from toponymy.llm_wrappers import OllamaNamer

    class JsonOllamaNamer(OllamaNamer):
        def _gen(self, prompt: str, temperature: float, max_tokens: int) -> str:
            r = self.client.generate(
                model=self.model,
                prompt=prompt + self.extra_prompting,
                format="json",
                options={
                    "temperature": min(temperature, 0.3),
                    "num_predict": max(max_tokens, 2048),
                },
            )
            return r["response"]

        def _call_llm(self, prompt: str, temperature: float, max_tokens: int) -> str:
            return self._gen(prompt, temperature, max_tokens)

        def _call_llm_with_system_prompt(
            self,
            system_prompt: str,
            user_prompt: str,
            temperature: float,
            max_tokens: int,
        ) -> str:
            return self._gen(
                f"{system_prompt}\n\n{user_prompt}", temperature, max_tokens
            )

    return JsonOllamaNamer(
        model=args.llm_model,
        host=args.ollama_host,
        llm_specific_instructions=args.name_instructions,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--embeddings", type=Path, required=True, help="embeddings.npy")
    ap.add_argument("--meta", type=Path, required=True, help="embed_meta.parquet")
    ap.add_argument(
        "--coords",
        type=Path,
        required=True,
        help="coords_<method>.parquet aligned by world_id",
    )
    ap.add_argument(
        "--physical-topo-dir",
        type=Path,
        required=True,
        help="physical hierarchy dir containing layer_*.parquet",
    )
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument(
        "--island-level",
        type=int,
        default=None,
        help="physical hierarchy level to use as islands (default: coarsest layer)",
    )
    ap.add_argument(
        "--min-toponymy-size",
        type=int,
        default=3000,
        help="rerun Toponymy inside islands with at least this many worlds",
    )
    ap.add_argument(
        "--max-toponymy-islands",
        type=int,
        default=None,
        help="optional cap on large islands processed, in descending size order",
    )
    ap.add_argument("--min-clusters", type=int, default=6)
    ap.add_argument("--llm-model", default=DEFAULT_LLM_MODEL)
    ap.add_argument("--ollama-host", default="http://localhost:11434")
    ap.add_argument(
        "--st-model",
        default="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        help="sentence-transformers model Toponymy uses for keyphrase embedding",
    )
    ap.add_argument(
        "--name-instructions",
        default=(
            "Make topic_name a terse map label: 2-3 words, Title Case, at most "
            "24 characters, like 'Cozy Sleep Rooms', 'Horror Escape', "
            "'Avatar Hubs', 'Tropical Beaches'. Output ONLY the short label as "
            "the value. Never a sentence or description. Do NOT use 'and', "
            "'for', 'with', or commas."
        ),
        help="appended to every Toponymy naming prompt",
    )
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    emb_all = np.load(args.embeddings).astype("float32")
    meta = pl.read_parquet(args.meta)
    coords = pl.read_parquet(args.coords).select("world_id", "x", "y")

    physical_layers = _discover_layers(args.physical_topo_dir)
    island_level = (
        args.island_level
        if args.island_level is not None
        else int(physical_layers[-1].stem.split("_")[1])
    )
    physical = pl.read_parquet(args.physical_topo_dir / f"layer_{island_level}.parquet")
    physical = physical.select(
        "world_id",
        pl.col("cluster_id").alias("island_id"),
        pl.col("topic_name").alias("island_name"),
    )

    base = meta.select("world_id", "name", "text").join(
        coords, on="world_id", how="left"
    ).join(physical, on="world_id", how="left")
    valid = (
        base["x"].is_not_null()
        & base["y"].is_not_null()
        & base["island_id"].is_not_null()
    )
    if not valid.all():
        dropped = int((~valid).sum())
        print(f"dropping {dropped:,} rows missing coords or physical island")
    keep = valid.to_numpy()
    base = base.filter(valid)
    emb = emb_all[keep]
    document_map = base.select("x", "y").to_numpy().astype("float32")
    world_ids = base["world_id"].to_list()
    texts = base["text"].to_list()

    island_id = base["island_id"].cast(pl.Int64).to_numpy()
    island_names = [str(v or "") for v in base["island_name"].to_list()]
    n = len(world_ids)
    print(
        f"hybrid island Toponymy on {n:,} worlds; "
        f"physical island level={island_level}; "
        f"min_toponymy_size={args.min_toponymy_size}"
    )

    # layer_2 is the physical island layer, preserving original island ids.
    l2_id = island_id.astype(np.int32)
    label_by_island: dict[int, str] = {}
    for iid in sorted(int(v) for v in np.unique(island_id).tolist() if int(v) >= 0):
        idx = np.where(island_id == iid)[0]
        label_by_island[iid] = _mode_label(
            [island_names[i] for i in idx], f"Island {iid}"
        )
    l2_name = [label_by_island.get(int(i), f"Island {int(i)}") for i in island_id]

    l1_id = np.full(n, -1, dtype=np.int32)
    l0_id = np.full(n, -1, dtype=np.int32)
    l1_name = [""] * n
    l0_name = [""] * n
    next_l1 = 0
    next_l0 = 0

    from sentence_transformers import SentenceTransformer

    namer = _build_namer(args)
    st = SentenceTransformer(args.st_model)

    island_counts = Counter(int(i) for i in island_id.tolist())
    ordered_islands = [iid for iid, _ in island_counts.most_common()]
    large_islands = [
        iid for iid in ordered_islands if island_counts[iid] >= args.min_toponymy_size
    ]
    if args.max_toponymy_islands is not None:
        large_islands = large_islands[: args.max_toponymy_islands]
    large_set = set(large_islands)
    print(
        "large islands: "
        + ", ".join(
            f"{iid} ({island_counts[iid]:,}, {label_by_island[iid]})"
            for iid in large_islands
        )
    )

    island_summary: list[dict[str, object]] = []
    for iid in ordered_islands:
        idx = np.where(island_id == iid)[0]
        label = label_by_island[iid]
        if iid in large_set:
            print(f"=== island {iid} ({len(idx):,}): {label} ===")
            layers = _run_toponymy(
                emb=emb[idx],
                document_map=document_map[idx],
                texts=[texts[i] for i in idx],
                namer=namer,
                text_embedding_model=st,
                min_clusters=args.min_clusters,
            )
            print(f"  built {len(layers)} local layers")
            if len(layers) >= 2:
                fine_layer = _layer_from_toponymy(layers[-2], document_map[idx], label)
                sub_layer = _layer_from_toponymy(layers[-1], document_map[idx], label)
            elif len(layers) == 1:
                fine_layer = _layer_from_toponymy(layers[0], document_map[idx], label)
                sub_layer = fine_layer
            else:
                fine_layer = _singleton_layer(len(idx), label)
                sub_layer = fine_layer
        else:
            fine_layer = _singleton_layer(len(idx), label)
            sub_layer = fine_layer

        next_l1, sub_labels = _globalize(
            local=sub_layer,
            global_indices=idx,
            out_id=l1_id,
            out_name=l1_name,
            next_id=next_l1,
            fallback=label,
        )
        next_l0, fine_labels = _globalize(
            local=fine_layer,
            global_indices=idx,
            out_id=l0_id,
            out_name=l0_name,
            next_id=next_l0,
            fallback=label,
        )
        island_summary.append(
            {
                "island_id": int(iid),
                "label": label,
                "size": int(len(idx)),
                "mode": "toponymy" if iid in large_set else "physical-singleton",
                "subregion_count": len(sub_labels),
                "district_count": len(fine_labels),
                "subregion_labels": sub_labels,
                "district_labels": fine_labels[:40],
            }
        )
        print(
            f"  -> l1 {len(sub_labels):,} subregions; "
            f"l0 {len(fine_labels):,} districts"
        )

    if int((l1_id < 0).sum()) or int((l0_id < 0).sum()):
        raise RuntimeError("internal error: some worlds were not assigned hybrid ids")

    _write_layer(args.out_dir, 0, world_ids, l0_id, l0_name)
    _write_layer(args.out_dir, 1, world_ids, l1_id, l1_name)
    _write_layer(args.out_dir, 2, world_ids, l2_id, l2_name)

    hierarchy_summary = []
    for layer_index, names in [(0, l0_name), (1, l1_name), (2, l2_name)]:
        topics = sorted(set(n for n in names if n))
        hierarchy_summary.append(
            {"layer": layer_index, "n_topics": len(topics), "topics": topics}
        )
    (args.out_dir / "hierarchy_summary.json").write_text(
        json.dumps(hierarchy_summary, ensure_ascii=False, indent=2)
    )
    (args.out_dir / "island_toponymy_summary.json").write_text(
        json.dumps(island_summary, ensure_ascii=False, indent=2)
    )
    print(
        f"wrote hybrid hierarchy to {args.out_dir}: "
        f"l0={next_l0:,}, l1={next_l1:,}, l2={len(label_by_island):,}"
    )


if __name__ == "__main__":
    main()
