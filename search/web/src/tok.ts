// Mirrors search/build/tok.py -- keep in sync.
const CJK = "\\u3005-\\u3007\\u3040-\\u30ff\\u31f0-\\u31ff\\u3400-\\u4dbf\\u4e00-\\u9fff\\uf900-\\ufaff\\uac00-\\ud7af\\u{20000}-\\u{2fa1f}";
const WORD = "[\\p{L}\\p{N}\\p{M}]";
const TOK = new RegExp(`([${CJK}]+)|((?:(?![${CJK}])${WORD})+)`, "gu");
const NONWORD = /[^\p{L}\p{N}\p{M}]+/gu;

export const TITLE_MARK = "\u0001";
export const TAG_MARK = "\u0002";
export const PAIR_MARK = "\u0003";
export const AUTHOR_MARK = "\u0004";
const MAX_WORD = 32;
const MAX_TITLE = 48;

export const norm = (s: string) => s.normalize("NFKC").toLowerCase();

export function titleKey(s: string): string {
  return Array.from(norm(s).replace(NONWORD, " ").trim()).slice(0, MAX_TITLE).join("").trim();
}

export interface Tok {
  term: string;
  cjk: boolean;
  /** true for a latin word: may be prefix-expanded when it is the last token */
  word: boolean;
}

export function tokens(text: string): Tok[] {
  const out: Tok[] = [];
  TOK.lastIndex = 0;
  for (const m of norm(text).matchAll(TOK)) {
    if (m[1] !== undefined) {
      const cs = Array.from(m[1]);
      if (cs.length === 1) out.push({ term: cs[0], cjk: true, word: false });
      else for (let i = 0; i + 1 < cs.length; i++) out.push({ term: cs[i] + cs[i + 1], cjk: true, word: false });
    } else {
      out.push({ term: Array.from(m[2]).slice(0, MAX_WORD).join(""), cjk: false, word: true });
    }
  }
  return out;
}

export function shardKey(term: string): string {
  if (term[0] === TITLE_MARK || term[0] === TAG_MARK || term[0] === PAIR_MARK || term[0] === AUTHOR_MARK) term = term.slice(1);
  return Array.from(term).slice(0, 2).join("");
}

export function fnv1a(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

export const shardOf = (term: string, n: number) => fnv1a(shardKey(term)) % n;

/** whitespace pieces of the normalized text, punctuation kept (mirrors tok.pieces) */
export const pieces = (text: string, maxn = 10): string[] => norm(text).split(/\s+/).filter(Boolean).slice(0, maxn).map((w) => Array.from(w).slice(0, 24).join(""));
