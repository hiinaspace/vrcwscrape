"""Stage D2: hierarchical topic map with toponymy + a local LLM (gemma4/ollama).

toponymy builds a balanced *multiresolution* cluster hierarchy over the text
embeddings (broad themes -> fine neighborhoods), names every node at every level
with an LLM, and feeds datamapplot an interactive map with zoom-dependent layered
labels -- the "google-maps feel": zoom out for continents, zoom in for streets.
The interactive HTML doubles as a lightweight inspection tool (hover = world name).

We keep our all-ollama setup by implementing toponymy's tiny LLMWrapper interface
on top of our existing ollama_generate (gemma4). Needs the `topo` extra:
    uv sync --extra topo
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import polars as pl

from mapgen.common import DEFAULT_LLM_MODEL


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--embeddings", type=Path, required=True, help="embeddings.npy")
    ap.add_argument("--meta", type=Path, required=True, help="embed_meta.parquet")
    ap.add_argument(
        "--coords",
        type=Path,
        required=True,
        help="coords_<method>.parquet (the 2D map toponymy labels)",
    )
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--llm-model", default=DEFAULT_LLM_MODEL)
    ap.add_argument("--ollama-host", default="http://localhost:11434")
    ap.add_argument(
        "--st-model",
        default="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        help="sentence-transformers model toponymy uses for keyphrase embedding "
        "(multilingual: our worlds are EN/JP)",
    )
    ap.add_argument("--min-clusters", type=int, default=6)
    ap.add_argument(
        "--name-instructions",
        default=(
            "Make the topic_name a SHORT map label: 2-4 words, Title Case, like "
            "'Cozy Sleep Rooms' or 'Horror Escape Games'. Never a full sentence."
        ),
        help="appended to every naming prompt to control label style",
    )
    ap.add_argument("--limit", type=int, default=None, help="subset for a quick test")
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    emb = np.load(args.embeddings).astype("float32")
    meta = pl.read_parquet(args.meta)
    coords = pl.read_parquet(args.coords)
    # align coords to meta row order
    coords = meta.select("world_id").join(coords, on="world_id", how="left")
    document_map = np.c_[
        coords["x"].to_numpy(), coords["y"].to_numpy()
    ].astype("float32")
    names = meta["name"].to_list()
    texts = meta["text"].to_list()

    if args.limit:
        emb = emb[: args.limit]
        document_map = document_map[: args.limit]
        names = names[: args.limit]
        texts = texts[: args.limit]
    n = emb.shape[0]
    print(f"toponymy on {n:,} worlds (coords={args.coords.name})")

    from sentence_transformers import SentenceTransformer
    from toponymy import Toponymy, ToponymyClusterer
    from toponymy.llm_wrappers import OllamaNamer

    class JsonOllamaNamer(OllamaNamer):
        """Force ollama format=json + a stable sampler. Plain gemma degenerates
        into runs of whitespace at default temperature (burning num_predict ->
        empty response) and wraps output in ```json fences; format=json fixes
        both so toponymy's JSON parser succeeds. System+user are folded into one
        prompt (gemma's chat template is finicky about the system role)."""

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
            return self._gen(f"{system_prompt}\n\n{user_prompt}", temperature, max_tokens)

    namer = JsonOllamaNamer(
        model=args.llm_model,
        host=args.ollama_host,
        llm_specific_instructions=args.name_instructions,
    )
    st = SentenceTransformer(args.st_model)

    clusterer = ToponymyClusterer(min_clusters=args.min_clusters, verbose=True)
    clusterer.fit(document_map, emb)  # cluster on the 2D map so regions match it

    topic_model = Toponymy(
        llm_wrapper=namer,
        text_embedding_model=st,
        clusterer=clusterer,
        object_description="VRChat world",
        corpus_description="collection of social VR worlds in VRChat",
        verbose=True,
    )
    topic_model.fit(texts, emb, document_map)

    layers = topic_model.cluster_layers_
    print(f"  built {len(layers)} hierarchy layers")

    # dump per-layer assignments + names for the app, and a summary
    wid = meta["world_id"].to_list()[:n]
    summary: list[dict] = []
    for li, layer in enumerate(layers):
        name_vec = list(getattr(layer, "topic_name_vector", []))
        cluster_labels = getattr(layer, "cluster_labels", None)
        cols = {"world_id": wid}
        if name_vec:
            cols["topic_name"] = name_vec
        if cluster_labels is not None:
            cols["cluster_id"] = list(np.asarray(cluster_labels).tolist())
        try:
            pl.DataFrame(cols).write_parquet(args.out_dir / f"layer_{li}.parquet")
        except Exception as err:  # noqa: BLE001
            print(f"  !! layer {li} dump failed: {err}")
        distinct = sorted(set(name_vec))
        summary.append({"layer": li, "n_topics": len(distinct), "topics": distinct})
        print(f"  layer {li}: {len(distinct)} topics")

    (args.out_dir / "hierarchy_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2)
    )

    import datamapplot

    label_layers = [list(layer.topic_name_vector) for layer in layers]
    plot = datamapplot.create_interactive_plot(
        document_map,
        *label_layers,
        hover_text=names,
        title="VRChat Worlds — latent map",
    )
    out_html = args.out_dir / "hierarchy_map.html"
    plot.save(str(out_html))
    print(f"  wrote {out_html}")


if __name__ == "__main__":
    main()
