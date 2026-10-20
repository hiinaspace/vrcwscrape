"""Tokenizer shared (by contract) with web/src/tok.ts. Keep the two in sync.

Terms: latin/digit words (NFKC, lowercased) and CJK bigrams. CJK unigrams are
additionally emitted for title/author/tag fields so one-character queries work.
Title-phrase terms ("\\u0001" + normalized title) support exact / prefix title boosts.
"""

from __future__ import annotations

import unicodedata

import regex

CJK = "々-〇぀-ヿㇰ-ㇿ㐀-䶿一-鿿豈-﫿가-힯\U00020000-\U0002fa1f"
WORD = r"[\p{L}\p{N}\p{M}]"
_TOK = regex.compile(rf"([{CJK}]+)|((?:(?![{CJK}]){WORD})+)")
_NONWORD = regex.compile(rf"[^\p{{L}}\p{{N}}\p{{M}}]+")

TITLE_MARK = "\u0001"
TAG_MARK = "\u0002"
PAIR_MARK = "\u0003"
MARKS = (TITLE_MARK, TAG_MARK, PAIR_MARK)
MAX_WORD = 32
MAX_TITLE = 48

F_TITLE, F_AUTHOR, F_TAG, F_DESC = 1, 2, 4, 8


def norm(s: str) -> str:
    return unicodedata.normalize("NFKC", s).lower()


def title_key(s: str) -> str:
    """normalized phrase used for exact/prefix title matching"""
    return _NONWORD.sub(" ", norm(s)).strip()[:MAX_TITLE].strip()


def pieces(text: str, maxn: int = 10) -> list[str]:
    """whitespace pieces of the normalized (punctuation kept) title, for adjacent-pair terms"""
    return [w[:24] for w in norm(text).split()[:maxn]]


def tokens(text: str, unigrams: bool) -> list[str]:
    out: list[str] = []
    for m in _TOK.finditer(norm(text)):
        cjk, word = m.group(1), m.group(2)
        if cjk is not None:
            if len(cjk) == 1:
                out.append(cjk)
            else:
                out.extend(cjk[i : i + 2] for i in range(len(cjk) - 1))
                if unigrams:
                    out.extend(cjk)
        else:
            out.append(word[:MAX_WORD])
    return out


def shard_key(term: str) -> str:
    if term[:1] in MARKS:
        term = term[1:]
    return "".join(list(term)[:2])


def fnv1a16(s: str) -> int:
    """FNV-1a 32 over UTF-16 code units (matches JS charCodeAt loop)."""
    h = 0x811C9DC5
    b = s.encode("utf-16-le")
    for i in range(0, len(b), 2):
        h ^= b[i] | (b[i + 1] << 8)
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def shard_of(term: str, nshards: int) -> int:
    return fnv1a16(shard_key(term)) % nshards


def u16_key(s: str) -> bytes:
    return s.encode("utf-16-be", "surrogatepass")
