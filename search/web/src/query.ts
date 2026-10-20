// Query language: parser + pill helpers. Pure (no DOM, no fetch); never throws.
//
//   expr   := item*                          juxtaposition (or explicit AND) = AND, the LOWEST precedence
//   item   := unary ( (OR | '|') unary )*    OR binds tighter than AND (Google/Gmail style): a b | c  ==  a (b | c)
//   unary  := ('-' | NOT) unary | '(' expr ')' | "phrase" | field:value | word
//
// Lowercase and/or/not are plain words. `-` only negates at the start of a word (half-life is one word).
// Unknown `foo:bar` is a plain word. A known field with a bad value reports an error and falls back to plain words.

export type TextField = "tag" | "title" | "author" | "cw" | "status" | "platform";
export type NumField = "cap" | "size" | "visits" | "favorites";
export type DateField = "updated" | "published";
export type FieldName = TextField | NumField | DateField;

export interface Span { start: number; end: number }

export type Node =
  | { t: "and"; c: Node[]; start: number; end: number }
  | { t: "or"; c: Node[]; start: number; end: number }
  | { t: "not"; c: Node; start: number; end: number }
  /** plain term or "phrase"; `text` is the unquoted source text */
  | { t: "term"; text: string; phrase: boolean; start: number; end: number }
  /** tag:x title:x author:x cw:x status:x platform:x ; value unquoted */
  | { t: "field"; field: TextField; value: string; start: number; end: number }
  /** inclusive-by-default numeric range; null = open. size is in MB (fractions allowed) */
  | { t: "num"; field: NumField; lo: number | null; hi: number | null; loEx: boolean; hiEx: boolean; start: number; end: number }
  /** inclusive ISO date range (UTC days), null = open */
  | { t: "date"; field: DateField; lo: string | null; hi: string | null; start: number; end: number };

export type PillKind = "term" | "phrase" | "field" | "num" | "date" | "group";
export interface Pill {
  /** stable within one parse (the span start) */
  id: string;
  kind: PillKind;
  /** set for kind field/num/date */
  field?: FieldName;
  /** short display label, e.g. `lake`, `"escape room"`, `tag: japan`, `cap ≥ 8`, `published 2024-06`, `(horror | scary)` */
  label: string;
  /** value part only (e.g. `japan`, `>=8`) */
  value: string;
  negated: boolean;
  /** source span [start,end) in the exact text passed to parseQuery, including a leading `-` / `NOT ` */
  start: number;
  end: number;
}
export interface ParseResult { ast: Node | null; pills: Pill[]; errors: string[] }

// ---------------------------------------------------------------- lexer
type TokK = "LP" | "RP" | "OR" | "AND" | "NOT" | "WORD" | "PHRASE";
interface Tok { k: TokK; s: number; e: number; text: string }

/** `-name-`: a leading dash on a word that also ends in a dash is part of the word, not a negation */
function decorated(src: string, i: number): boolean {
  let j = i;
  while (j < src.length && !/[\s()|"]/.test(src[j])) j++;
  return j - i >= 3 && src[j - 1] === "-";
}

function lex(src: string, errors: string[]): Tok[] {
  const out: Tok[] = [];
  const n = src.length;
  let i = 0;
  const isWs = (c: string) => /\s/.test(c);
  while (i < n) {
    const c = src[i];
    if (isWs(c)) { i++; continue; }
    if (c === "(") { out.push({ k: "LP", s: i, e: i + 1, text: c }); i++; continue; }
    if (c === ")") { out.push({ k: "RP", s: i, e: i + 1, text: c }); i++; continue; }
    if (c === "|") { out.push({ k: "OR", s: i, e: i + 1 + (src[i + 1] === "|" ? 1 : 0), text: "|" }); i += src[i + 1] === "|" ? 2 : 1; continue; }
    if (c === "-" && i + 1 < n && !isWs(src[i + 1]) && !decorated(src, i)) { out.push({ k: "NOT", s: i, e: i + 1, text: "-" }); i++; continue; }
    if (c === "-" && (i + 1 >= n || isWs(src[i + 1]))) { i++; continue; } // lone dash
    if (c === '"') {
      let j = i + 1;
      while (j < n && src[j] !== '"') j++;
      if (j >= n) errors.push("unterminated quote");
      out.push({ k: "PHRASE", s: i, e: Math.min(n, j + 1), text: src.slice(i + 1, j) });
      i = Math.min(n, j + 1);
      continue;
    }
    // word: up to whitespace ( ) | ; a quote right after ':' or inside keeps going until its closing quote
    let j = i;
    while (j < n && !isWs(src[j]) && src[j] !== "(" && src[j] !== ")" && src[j] !== "|") {
      if (src[j] === '"') {
        if (j > i && src[j - 1] === ":") { // field:"quoted value"
          j++;
          while (j < n && src[j] !== '"') j++;
          if (j >= n) errors.push("unterminated quote");
          j = Math.min(n, j + 1);
          continue;
        }
        break; // a quote mid-word starts a new phrase token
      }
      j++;
    }
    if (j === i) { i++; continue; }
    const text = src.slice(i, j);
    const k: TokK = text === "OR" ? "OR" : text === "AND" ? "AND" : text === "NOT" ? "NOT" : "WORD";
    out.push({ k, s: i, e: j, text });
    i = j;
  }
  return out;
}

// ---------------------------------------------------------------- field values
const FIELD_ALIAS: Record<string, FieldName> = {
  tag: "tag", tags: "tag", title: "title", name: "title", author: "author", by: "author", cw: "cw", status: "status",
  platform: "platform", plat: "platform", cap: "cap", capacity: "cap", players: "cap", size: "size", mb: "size",
  visits: "visits", visit: "visits", favorites: "favorites", favourites: "favorites", favs: "favorites", fav: "favorites",
  updated: "updated", update: "updated", published: "published", pub: "published", created: "published",
};
const NUM_FIELDS = new Set<string>(["cap", "size", "visits", "favorites"]);
const DATE_FIELDS = new Set<string>(["updated", "published"]);

function parseNum(s: string, field: NumField): number | null {
  const m = /^(\d+(?:\.\d+)?)([kKmM]?)$/.exec(s);
  if (!m) return null;
  let v = parseFloat(m[1]);
  const suf = m[2].toLowerCase();
  if (suf === "k") v *= 1e3; else if (suf === "m") v *= 1e6;
  if (field !== "size" && !Number.isInteger(v)) return null;
  return v;
}

/** [lo,hi] ISO days covered by 2025 / 2025-06 / 2025-06-03; null if invalid */
function datePeriod(s: string): [string, string] | null {
  const m = /^(\d{4})(?:-(\d{1,2})(?:-(\d{1,2}))?)?$/.exec(s);
  if (!m) return null;
  const y = +m[1], mo = m[2] ? +m[2] : 0, d = m[3] ? +m[3] : 0;
  const iso = (yy: number, mm: number, dd: number) => new Date(Date.UTC(yy, mm - 1, dd)).toISOString().slice(0, 10);
  if (y < 2000 || y > 2100) return null;
  if (!mo) return [iso(y, 1, 1), iso(y, 12, 31)];
  if (mo < 1 || mo > 12) return null;
  if (!d) return [iso(y, mo, 1), iso(y, mo + 1, 0)];
  const t = new Date(Date.UTC(y, mo - 1, d));
  if (t.getUTCMonth() !== mo - 1 || d < 1) return null;
  return [iso(y, mo, d), iso(y, mo, d)];
}

type Cmp = { op: ">" | ">=" | "<" | "<=" | "=" | ".."; a: string; b: string };
function splitCmp(v: string): Cmp {
  let m = /^(>=|<=|>|<|=)(.*)$/.exec(v);
  if (m) return { op: m[1] as Cmp["op"], a: m[2], b: "" };
  m = /^(.*?)\.\.(.*)$/.exec(v);
  if (m) return { op: "..", a: m[1], b: m[2] };
  return { op: "=", a: v, b: "" };
}

function numNode(field: NumField, v: string, start: number, end: number): Node | string {
  const c = splitCmp(v);
  const base = { t: "num" as const, field, start, end, loEx: false, hiEx: false };
  if (c.op === "..") {
    const lo = c.a ? parseNum(c.a, field) : null, hi = c.b ? parseNum(c.b, field) : null;
    if ((c.a && lo === null) || (c.b && hi === null) || (!c.a && !c.b)) return `bad ${field} range "${v}"`;
    return { ...base, lo, hi };
  }
  const x = parseNum(c.a, field);
  if (x === null) return `bad ${field} value "${v}"`;
  if (c.op === ">") return { ...base, lo: x, hi: null, loEx: true };
  if (c.op === ">=") return { ...base, lo: x, hi: null };
  if (c.op === "<") return { ...base, lo: null, hi: x, hiEx: true };
  if (c.op === "<=") return { ...base, lo: null, hi: x };
  return { ...base, lo: x, hi: x };
}

function dateNode(field: DateField, v: string, start: number, end: number): Node | string {
  const c = splitCmp(v);
  const base = { t: "date" as const, field, start, end };
  const p = (s: string) => datePeriod(s);
  const bad = `bad ${field} date "${v}"`;
  if (c.op === "..") {
    const a = c.a ? p(c.a) : null, b = c.b ? p(c.b) : null;
    if ((c.a && !a) || (c.b && !b) || (!c.a && !c.b)) return bad;
    return { ...base, lo: a ? a[0] : null, hi: b ? b[1] : null };
  }
  const x = p(c.a);
  if (!x) return bad;
  const dayAfter = (iso: string) => new Date(Date.parse(iso + "T00:00:00Z") + 86400000).toISOString().slice(0, 10);
  const dayBefore = (iso: string) => new Date(Date.parse(iso + "T00:00:00Z") - 86400000).toISOString().slice(0, 10);
  if (c.op === ">") return { ...base, lo: dayAfter(x[1]), hi: null };
  if (c.op === ">=") return { ...base, lo: x[0], hi: null };
  if (c.op === "<") return { ...base, lo: null, hi: dayBefore(x[0]) };
  if (c.op === "<=") return { ...base, lo: null, hi: x[1] };
  return { ...base, lo: x[0], hi: x[1] };
}

const unq = (s: string) => (s.length >= 2 && s[0] === '"' && s[s.length - 1] === '"' ? s.slice(1, -1) : s.startsWith('"') ? s.slice(1) : s);

/** one WORD token -> node, or null (incomplete like `tag:`), recording errors */
function atom(t: Tok, errors: string[]): Node | null {
  const word = t.text;
  const m = /^([A-Za-z]+):(.*)$/.exec(word);
  const plain = (): Node => ({ t: "term", text: word, phrase: false, start: t.s, end: t.e });
  if (!m) return plain();
  const field = FIELD_ALIAS[m[1].toLowerCase()];
  if (!field) return plain(); // Re:Zero, 12:30, urls ...
  const raw = m[2];
  if (raw === "" || raw === '"' || raw === '""') return null; // being typed
  const val = unq(raw).trim();
  if (val === "") return null;
  if (NUM_FIELDS.has(field)) {
    const r = numNode(field as NumField, val, t.s, t.e);
    if (typeof r === "string") { errors.push(r); return plain(); }
    return r;
  }
  if (DATE_FIELDS.has(field)) {
    const r = dateNode(field as DateField, val, t.s, t.e);
    if (typeof r === "string") { errors.push(r); return plain(); }
    return r;
  }
  if (field === "platform") {
    const v = val.toLowerCase();
    const norm = v === "pc" || v === "windows" || v === "desktop" ? "pc" : v === "quest" || v === "android" ? "quest" : v === "both" ? "both" : "";
    if (!norm) { errors.push(`unknown platform "${val}" (pc, quest, both)`); return plain(); }
    return { t: "field", field, value: norm, start: t.s, end: t.e };
  }
  if (field === "status") {
    const v = val.toLowerCase().replace(/^system_/, "");
    if (v !== "approved" && v !== "labs") { errors.push(`unknown status "${val}" (approved, labs)`); return plain(); }
    return { t: "field", field, value: v, start: t.s, end: t.e };
  }
  return { t: "field", field: field as TextField, value: val, start: t.s, end: t.e };
}

// ---------------------------------------------------------------- parser
class Parser {
  i = 0;
  constructor(public toks: Tok[], public errors: string[], public src: string) {}
  peek() { return this.toks[this.i]; }

  /** items joined by juxtaposition / AND until end or unmatched RP */
  parseAnd(depth: number): Node[] {
    const items: Node[] = [];
    for (let guard = 0; this.i < this.toks.length && guard < 10000; guard++) {
      const t = this.peek();
      if (t.k === "RP") {
        if (depth > 0) break;
        this.errors.push("unmatched )");
        this.i++;
        continue;
      }
      if (t.k === "AND") { this.i++; continue; }
      const before = this.i;
      const item = this.parseOr(depth);
      if (item) items.push(item);
      if (this.i === before) this.i++; // safety
    }
    return items;
  }

  parseOr(depth: number): Node | null {
    const first = this.peek();
    if (first && first.k === "OR") { this.errors.push("OR needs a term on the left"); this.i++; return this.parseOr(depth); }
    let left = this.parseUnary(depth);
    const alts: Node[] = left ? [left] : [];
    while (this.peek() && this.peek().k === "OR") {
      this.i++;
      const nx = this.peek();
      if (!nx || nx.k === "RP" || nx.k === "OR") { if (!nx || nx.k === "RP") this.errors.push("OR needs a term on the right"); continue; }
      const r = this.parseUnary(depth);
      if (r) alts.push(r);
    }
    if (alts.length === 0) return null;
    if (alts.length === 1) return alts[0];
    return { t: "or", c: alts, start: alts[0].start, end: alts[alts.length - 1].end };
  }

  parseUnary(depth: number): Node | null {
    const t = this.peek();
    if (!t) return null;
    if (t.k === "NOT") {
      this.i++;
      const nx = this.peek();
      if (!nx || nx.k === "RP" || nx.k === "OR" || nx.k === "AND") { if (t.text !== "-") this.errors.push("NOT needs a term"); return null; }
      const c = this.parseUnary(depth);
      return c ? { t: "not", c, start: t.s, end: c.end } : null;
    }
    if (t.k === "LP") {
      this.i++;
      const items = this.parseAnd(depth + 1);
      let end = t.e;
      const nx = this.peek();
      if (nx && nx.k === "RP") { end = nx.e; this.i++; }
      else { this.errors.push("missing )"); end = items.length ? items[items.length - 1].end : t.e; }
      if (!items.length) return null;
      const c: Node = items.length === 1 ? items[0] : { t: "and", c: items, start: items[0].start, end };
      // keep the parentheses in the span so pills cover them
      return { ...c, start: t.s, end } as Node;
    }
    if (t.k === "PHRASE") {
      this.i++;
      if (!t.text.trim()) return null;
      return { t: "term", text: t.text, phrase: true, start: t.s, end: t.e };
    }
    if (t.k === "WORD") { this.i++; return atom(t, this.errors); }
    // stray AND/OR/RP handled by callers
    this.i++;
    return null;
  }
}

function describe(n: Node, src: string): { kind: PillKind; field?: FieldName; label: string; value: string } {
  const num = (x: number, f: NumField) => (f === "size" ? `${x}` : x >= 1000 && x % 1000 === 0 ? `${x / 1000}k` : `${x}`);
  switch (n.t) {
    case "term":
      return n.phrase ? { kind: "phrase", label: `"${n.text}"`, value: n.text } : { kind: "term", label: n.text, value: n.text };
    case "field":
      return { kind: "field", field: n.field, label: `${n.field}: ${n.value}`, value: n.value };
    case "num": {
      const u = n.field === "size" ? " MB" : "";
      let v: string, l: string;
      if (n.lo !== null && n.hi !== null && n.lo === n.hi) { v = `${n.lo}`; l = `${n.field} ${num(n.lo, n.field)}${u}`; }
      else if (n.lo !== null && n.hi !== null) { v = `${n.lo}..${n.hi}`; l = `${n.field} ${num(n.lo, n.field)}–${num(n.hi, n.field)}${u}`; }
      else if (n.lo !== null) { v = `${n.loEx ? ">" : ">="}${n.lo}`; l = `${n.field} ${n.loEx ? ">" : "≥"} ${num(n.lo, n.field)}${u}`; }
      else { v = `${n.hiEx ? "<" : "<="}${n.hi}`; l = `${n.field} ${n.hiEx ? "<" : "≤"} ${num(n.hi!, n.field)}${u}`; }
      return { kind: "num", field: n.field, label: l, value: v };
    }
    case "date": {
      const lo = n.lo, hi = n.hi;
      const v = lo && hi ? (lo === hi ? lo : `${lo}..${hi}`) : lo ? `>=${lo}` : `<=${hi}`;
      const l = lo && hi ? (lo === hi ? `${n.field} ${lo}` : `${n.field} ${lo} – ${hi}`) : lo ? `${n.field} ≥ ${lo}` : `${n.field} ≤ ${hi}`;
      return { kind: "date", field: n.field, label: l, value: v };
    }
    case "not":
      return describe(n.c, src);
    default: {
      const s = src.slice(n.start, n.end).trim();
      return { kind: "group", label: s, value: s };
    }
  }
}

export function parseQuery(text: string): ParseResult {
  const errors: string[] = [];
  try {
    const toks = lex(text, errors);
    const p = new Parser(toks, errors, text);
    const items = p.parseAnd(0);
    if (!items.length) return { ast: null, pills: [], errors };
    const ast: Node = items.length === 1 ? items[0] : { t: "and", c: items, start: items[0].start, end: items[items.length - 1].end };
    const pills: Pill[] = items.map((it) => {
      const neg = it.t === "not";
      const d = describe(neg ? (it as Extract<Node, { t: "not" }>).c : it, text);
      return { id: String(it.start), negated: neg, start: it.start, end: it.end, ...d };
    });
    return { ast, pills, errors };
  } catch (e) { // defensive: the parser must never break typing
    const words = text.split(/[\s()|"]+/).filter(Boolean);
    const ast: Node | null = words.length ? { t: "and", c: words.map((w, i) => ({ t: "term" as const, text: w, phrase: false, start: i, end: i })), start: 0, end: text.length } : null;
    return { ast, pills: [], errors: [...errors, "query parse failed: " + String(e)] };
  }
}

// ---------------------------------------------------------------- pill helpers (return new query TEXT)
function tidy(s: string): string {
  s = s.replace(/\s+/g, " ").trim();
  for (let k = 0; k < 4; k++) s = s.replace(/^(AND|OR|\|)\s+/, "").replace(/\s+(AND|OR|\|)$/, "").replace(/\bAND\s+AND\b/g, "AND");
  return s;
}

/** text with this pill's condition removed. `pill` must come from parseQuery(text) of the same text. */
export function removePill(text: string, pill: Pill): string {
  return tidy(text.slice(0, pill.start) + " " + text.slice(pill.end));
}

/** text with this pill's sign flipped (include <-> exclude). */
export function flipPill(text: string, pill: Pill): string {
  const body = text.slice(pill.start, pill.end);
  let nb: string;
  if (pill.negated) nb = body.startsWith("-") ? body.slice(1) : body.replace(/^NOT\s+/, "");
  else {
    const wrapped = body.startsWith("(") && matchingClose(body, 0) === body.length - 1;
    nb = pill.kind === "group" && !wrapped ? `-(${body})` : "-" + body;
  }
  return tidy(text.slice(0, pill.start) + nb + text.slice(pill.end));
}

function matchingClose(s: string, open: number): number {
  let d = 0, q = false;
  for (let i = open; i < s.length; i++) {
    const c = s[i];
    if (c === '"') q = !q;
    else if (!q && c === "(") d++;
    else if (!q && c === ")" && --d === 0) return i;
  }
  return -1;
}

/** source text for one condition: condition("tag","escape room",true) -> `-tag:"escape room"`; field "" = plain term */
export function condition(field: string, value: string, negated = false): string {
  const v = value.replace(/"/g, "");
  const needsQuote = /[\s()|]/.test(v) && !/^(>=|<=|>|<|=)/.test(v);
  const body = field ? `${field}:${needsQuote ? `"${v}"` : v}` : needsQuote ? `"${v}"` : v;
  return (negated ? "-" : "") + body;
}

/** append a condition to the query text (skips it when an identical condition string is already present) */
export function addCondition(text: string, cond: string): string {
  const t = text.trim();
  if (!t) return cond;
  const re = new RegExp("(^|[\\s(|])" + cond.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "(?=$|[\\s)|])");
  if (re.test(t)) return t;
  return t + " " + cond;
}
