"""Shared text-building helpers for the embedding + eval scripts."""

import re
import unicodedata

import pandas as pd

MODELS = {
    # key: (hf id, kwargs, doc_prompt_name, query_prompt_name, dtype)
    "qwen3": ("Qwen/Qwen3-Embedding-0.6B", {}, None, "query", "float16"),
    "gemma2": ("google/embeddinggemma-2", {}, "Document", "SearchQuery", "bfloat16"),
    "bgem3": ("BAAI/bge-m3", {}, None, None, "float16"),
    "static": ("sentence-transformers/static-similarity-mrl-multilingual-v1", {}, None, None, "float32"),
}

_CJK = re.compile(r"[぀-ヿ㐀-䶿一-鿿가-힯ｦ-ﾟ]")
_KANA = re.compile(r"[぀-ヿｦ-ﾟ]")
_HANGUL = re.compile(r"[가-힯]")


def clean_tags(tags) -> list[str]:
    """Drop system_/admin_ plumbing tags, strip author_tag_/content_ prefixes."""
    out = []
    for t in tags if tags is not None else []:
        t = str(t)
        if t.startswith(("system_", "admin_")):
            continue
        for pre in ("author_tag_", "author_tag", "content_"):
            if t.startswith(pre):
                t = t[len(pre):]
                break
        t = t.replace("_", " ").strip()
        if t:
            out.append(t)
    return out


def short_text(name, tags) -> str:
    """Title + cleaned tags only (the browser-side / eval index text)."""
    tg = ", ".join(clean_tags(tags))
    return f"{(name or '').strip()}\n{tg}".strip()


def full_text(name, tags, author, desc, max_chars=1500) -> str:
    """title + tags + author + description (description truncated; ~512 tokens cap is
    also enforced by the model's max_seq_length)."""
    d = re.sub(r"\s+", " ", desc or "").strip()[:max_chars]
    parts = [(name or "").strip()]
    tg = ", ".join(clean_tags(tags))
    if tg:
        parts.append(f"tags: {tg}")
    if author:
        parts.append(f"by {author}")
    if d:
        parts.append(d)
    return "\n".join(parts)


def script_of(s: str) -> str:
    """Coarse script class of a title: kana (JP) | hangul | han | latin | other."""
    s = s or ""
    if _KANA.search(s):
        return "kana"
    if _HANGUL.search(s):
        return "hangul"
    if _CJK.search(s):
        return "han"
    letters = [c for c in s if unicodedata.category(c).startswith("L")]
    if letters and sum(c.isascii() for c in letters) / len(letters) > 0.8:
        return "latin"
    return "other"


def has_cjk(s: str) -> bool:
    return bool(_CJK.search(s or ""))


def ascii_ratio(s: str) -> float:
    letters = [c for c in (s or "") if unicodedata.category(c).startswith("L")]
    return sum(c.isascii() for c in letters) / len(letters) if letters else 0.0


def load_worlds(path: str) -> pd.DataFrame:
    return pd.read_parquet(path, columns=["world_id", "name", "description", "author_name", "author_id", "tags"])
