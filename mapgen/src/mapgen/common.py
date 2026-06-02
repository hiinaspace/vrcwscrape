"""Shared helpers: world-text construction, tag cleaning, and the ollama client.

The whole pipeline talks to a single ollama instance (default: the local one on
the GPU box `<gpu-host>`) for both embeddings (bge-m3) and LLM cluster labels (gemma4).
Keeping everything on ollama means the venv is pure-CPU Python with no torch/CUDA
to fight with on NixOS.
"""

from __future__ import annotations

import os
import time
import unicodedata
from collections.abc import Sequence

import httpx
import numpy as np
from tqdm import tqdm

DEFAULT_OLLAMA_URL = "http://127.0.0.1:11434"
DEFAULT_EMBED_MODEL = "bge-m3"
DEFAULT_LLM_MODEL = "gemma4:e4b"

# Tag prefixes carrying real semantic signal. `author_tag_` are user-supplied
# theme tags (chill, cozy, japan, horror...); `content_` are content warnings
# (also thematically useful). Everything else (system_/admin_/debug_/feature_)
# is operational noise we drop before embedding.
KEEP_TAG_PREFIXES = ("author_tag_", "content_")


def clean_tags(tags: Sequence[str] | None) -> list[str]:
    """Keep only semantic tags, strip their prefix, dedupe (case-insensitive)."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in tags or []:
        if not raw:
            continue
        for prefix in KEEP_TAG_PREFIXES:
            if raw.startswith(prefix):
                val = raw[len(prefix) :].replace("_", " ").strip()
                if val and val.lower() not in seen:
                    seen.add(val.lower())
                    out.append(val)
                break
    return out


def build_world_text(
    name: str | None,
    description: str | None,
    tags: Sequence[str] | None,
) -> str:
    """Compose the per-world text fed to the embedder.

    Order: title, then cleaned tags, then description. bge-m3 handles the mixed
    EN/JP content natively, so we keep the original-language text verbatim.
    """
    parts: list[str] = []
    name = (name or "").strip()
    if name:
        parts.append(name)
    tags_clean = clean_tags(tags)
    if tags_clean:
        parts.append("Tags: " + ", ".join(tags_clean))
    desc = (description or "").strip()
    if desc:
        parts.append(desc)
    return "\n".join(parts) if parts else " "


def _strip_general_punct(text: str) -> str:
    """Replace Unicode General Punctuation (U+2000–206F: smart quotes, dashes,
    leaders) with spaces, keeping ASCII and CJK intact."""
    return "".join(" " if 0x2000 <= ord(c) <= 0x206F else c for c in text)


def _ascii_cjk_only(text: str) -> str:
    """Keep only ASCII and CJK/kana/fullwidth ranges; drop the rest."""
    out = [
        c
        for c in text
        if ord(c) < 128 or 0x3000 <= ord(c) <= 0x9FFF or 0xFF00 <= ord(c) <= 0xFFEF
    ]
    return "".join(out) or " "


class OllamaEmbedder:
    """Batched embeddings via ollama's /api/embed endpoint.

    ollama's bge-m3 occasionally 500s on specific (deterministic) token
    sequences — not single bad chars, but certain combinations. We stay robust:
    NFKC-normalize up front, and when a batch is rejected, bisect to isolate the
    offending text, then try sanitizing transforms; if nothing works, emit a
    zero vector (recorded in `failed_indices`) so output stays row-aligned.
    """

    def __init__(
        self,
        model: str = DEFAULT_EMBED_MODEL,
        base_url: str | None = None,
        batch_size: int = 128,
        timeout: float = 600.0,
    ) -> None:
        self.model = model
        self.base_url = (
            base_url or os.environ.get("OLLAMA_URL") or DEFAULT_OLLAMA_URL
        ).rstrip("/")
        self.batch_size = batch_size
        self.client = httpx.Client(timeout=timeout)
        self.failed_indices: list[int] = []

    def embed(self, texts: Sequence[str], show_progress: bool = True) -> np.ndarray:
        norm = [unicodedata.normalize("NFKC", t) if t else " " for t in texts]
        results: list[list[float] | None] = [None] * len(norm)
        self.failed_indices = []
        pbar = tqdm(
            total=len(norm), disable=not show_progress, desc=f"embed[{self.model}]"
        )
        for start in range(0, len(norm), self.batch_size):
            self._resolve(
                norm, start, min(start + self.batch_size, len(norm)), results, pbar
            )
        pbar.close()

        dim = next((len(v) for v in results if v is not None), 0)
        if not dim:
            raise RuntimeError("ollama /api/embed rejected every batch")
        arr = np.zeros((len(norm), dim), dtype=np.float32)
        for i, v in enumerate(results):
            if v is not None:
                arr[i] = v
        if self.failed_indices:
            print(
                f"  WARNING: {len(self.failed_indices)} texts unembeddable "
                f"-> zero vectors (idx e.g. {self.failed_indices[:5]})"
            )
        return arr

    def embed_one(self, text: str) -> np.ndarray:
        return self.embed([text], show_progress=False)[0]

    def _resolve(
        self,
        texts: list[str],
        s: int,
        e: int,
        results: list[list[float] | None],
        pbar: tqdm,
    ) -> None:
        vecs = self._post(texts[s:e])
        if vecs is not None:
            for k, i in enumerate(range(s, e)):
                results[i] = vecs[k]
            pbar.update(e - s)
            return
        if e - s == 1:
            self._resolve_single(texts[s], s, results, pbar)
            return
        mid = (s + e) // 2
        self._resolve(texts, s, mid, results, pbar)
        self._resolve(texts, mid, e, results, pbar)

    def _resolve_single(
        self, text: str, i: int, results: list[list[float] | None], pbar: tqdm
    ) -> None:
        for variant in (_strip_general_punct(text), _ascii_cjk_only(text)):
            vecs = self._post([variant])
            if vecs is not None:
                results[i] = vecs[0]
                pbar.update(1)
                return
        self.failed_indices.append(i)  # leave results[i] = None -> zero vector
        pbar.update(1)

    def _post(self, batch: list[str], retries: int = 3) -> list[list[float]] | None:
        """Return embeddings, or None if the server rejects this batch content.

        Retries only transport errors; a non-200 means the content is the
        problem, so we return None and let the caller bisect/sanitize.
        """
        for attempt in range(retries):
            try:
                resp = self.client.post(
                    f"{self.base_url}/api/embed",
                    json={"model": self.model, "input": batch},
                )
            except Exception:  # noqa: BLE001 - retry transport errors
                time.sleep(1.0 * (attempt + 1))
                continue
            if resp.status_code == 200:
                return resp.json()["embeddings"]
            return None
        return None


def l2_normalize(x: np.ndarray) -> np.ndarray:
    """Row-wise L2 normalize so euclidean distance approximates cosine."""
    norms = np.linalg.norm(x, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return (x / norms).astype(np.float32)


def ollama_generate(
    prompt: str,
    model: str = DEFAULT_LLM_MODEL,
    base_url: str | None = None,
    timeout: float = 300.0,
    options: dict | None = None,
    fmt: dict | str | None = None,
    images: list[str] | None = None,
    client: httpx.Client | None = None,
) -> str:
    """Single-shot text generation via ollama /api/generate (non-streaming).

    `fmt` maps to ollama's structured-output `format` field: pass "json" or a
    JSON schema to constrain the output (so labels always parse). `images` is a
    list of base64-encoded images for vision models (e.g. gemma4 captioning).
    Pass a `client` to reuse a connection across many calls.
    """
    base = (base_url or os.environ.get("OLLAMA_URL") or DEFAULT_OLLAMA_URL).rstrip("/")
    payload: dict = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": options or {"temperature": 0.2},
    }
    if fmt is not None:
        payload["format"] = fmt
    if images:
        payload["images"] = images
    own = client is None
    client = client or httpx.Client(timeout=timeout)
    try:
        resp = client.post(f"{base}/api/generate", json=payload)
        resp.raise_for_status()
        return resp.json().get("response", "")
    finally:
        if own:
            client.close()
