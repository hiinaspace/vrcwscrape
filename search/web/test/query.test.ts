import test from "node:test";
import assert from "node:assert/strict";
import { parseQuery, removePill, flipPill, condition, addCondition, Node } from "../src/query";

const show = (n: Node | null): string => {
  if (!n) return "∅";
  switch (n.t) {
    case "and": return "(" + n.c.map(show).join(" & ") + ")";
    case "or": return "(" + n.c.map(show).join(" | ") + ")";
    case "not": return "!" + show(n.c);
    case "term": return n.phrase ? `"${n.text}"` : n.text;
    case "field": return `${n.field}=${n.value}`;
    case "num": return `${n.field}[${n.lo === null ? "" : (n.loEx ? "(" : "") + n.lo}..${n.hi === null ? "" : n.hi + (n.hiEx ? ")" : "")}]`;
    case "date": return `${n.field}[${n.lo ?? ""}..${n.hi ?? ""}]`;
  }
};
const P = (s: string) => show(parseQuery(s).ast);

test("precedence: OR binds tighter than implicit AND", () => {
  assert.equal(P("lake -hangout"), "(lake & !hangout)");
  assert.equal(P("lake AND NOT hangout"), "(lake & !hangout)");
  assert.equal(P("a b | c"), "(a & (b | c))");
  assert.equal(P("a OR b"), "(a | b)");
  assert.equal(P("(horror | scary) -tag:game cap:>=8"), "((horror | scary) & !tag=game & cap[8..])");
  assert.equal(P("神社 OR shrine"), "(神社 | shrine)");
  assert.equal(P("-(a b)"), "!(a & b)");
  assert.equal(P("NOT NOT a"), "!!a");
});

test("lowercase and/or/not are words; dash only negates at word start", () => {
  assert.equal(P("lake and hangout"), "(lake & and & hangout)");
  assert.equal(P("not this or that"), "(not & this & or & that)");
  assert.equal(P("half-life"), "half-life");
  assert.equal(P("a - b"), "(a & b)");
  assert.equal(P("-"), "∅");
  assert.equal(P("-CelestialRose-"), "-CelestialRose-");
  assert.equal(P("Nest -night-"), "(Nest & -night-)");
  assert.equal(P("a -b-c"), "(a & !b-c)");
});

test("phrases and quoted field values", () => {
  assert.equal(P('"escape room" tag:japan'), '("escape room" & tag=japan)');
  assert.equal(P('tag:"escape room"'), "tag=escape room");
  assert.equal(P('-"a b"'), '!"a b"');
  assert.deepEqual(parseQuery('"abc').errors, ["unterminated quote"]);
  assert.equal(P('"abc'), '"abc"');
});

test("numeric and date predicates", () => {
  assert.equal(P("cap:>=16"), "cap[16..]");
  assert.equal(P("cap:>16"), "cap[(16..]");
  assert.equal(P("cap:8..32"), "cap[8..32]");
  assert.equal(P("cap:..8"), "cap[..8]");
  assert.equal(P("cap:<8"), "cap[..8)]");
  assert.equal(P("size:<50"), "size[..50)]");
  assert.equal(P("size:<=0.5"), "size[..0.5]");
  assert.equal(P("visits:>10k"), "visits[(10000..]");
  assert.equal(P("favs:>=50"), "favorites[50..]");
  assert.equal(P("updated:>=2025"), "updated[2025-01-01..]");
  assert.equal(P("updated:>2025"), "updated[2026-01-01..]");
  assert.equal(P("updated:<2025"), "updated[..2024-12-31]");
  assert.equal(P("published:2024-06"), "published[2024-06-01..2024-06-30]");
  assert.equal(P("published:2024-02-29"), "published[2024-02-29..2024-02-29]");
  assert.equal(P("published:2023..2024-06"), "published[2023-01-01..2024-06-30]");
});

test("bad field values report an error and fall back to a plain word; unknown fields are words", () => {
  for (const [q, word] of [["cap:abc", "cap:abc"], ["published:2024-13", "published:2024-13"], ["platform:switch", "platform:switch"], ["status:x", "status:x"], ["updated:2023-02-31", "updated:2023-02-31"]] as const) {
    const r = parseQuery(q);
    assert.equal(r.errors.length, 1, q);
    assert.equal(show(r.ast), word, q);
  }
  assert.deepEqual(parseQuery("Re:Zero").errors, []);
  assert.equal(P("Re:Zero"), "Re:Zero");
  assert.equal(P("tag:"), "∅"); // being typed
  assert.equal(P("lake -tag:"), "lake");
});

test("malformed input never throws and reports errors", () => {
  for (const q of ["(", ")", "((a)", "a))", "a |", "| a", "OR", "a OR OR b", "NOT", "-(", "\"", "()", "(a b", "a (b", "- - -", "|||", "AND AND", "a AND", "((((((((a"]) {
    const r = parseQuery(q);
    assert.ok(Array.isArray(r.errors) && Array.isArray(r.pills), q);
  }
  assert.ok(parseQuery("(a b").errors.includes("missing )"));
  assert.equal(P("(a b"), "(a & b)");
  assert.ok(parseQuery("a))").errors.includes("unmatched )"));
  assert.equal(P("a))"), "a");
  assert.ok(parseQuery("a |").errors.length > 0);
  assert.equal(P("a |"), "a");
  assert.equal(P("()"), "∅");
});

test("fuzz: random strings never throw", () => {
  const al = ['a', 'b', '神', '社', ' ', '-', '(', ')', '|', '"', ':', '>', '=', '.', 'O', 'R', 'N', 'T', 'cap', 'tag', '1', '2024', ' OR ', ' AND ', ' NOT '];
  let seed = 12345;
  const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
  for (let i = 0; i < 20000; i++) {
    let s = "";
    for (let k = 0, n = 1 + Math.floor(rnd() * 14); k < n; k++) s += al[Math.floor(rnd() * al.length)];
    const r = parseQuery(s);
    for (const p of r.pills) { assert.ok(p.start >= 0 && p.end <= s.length && p.start < p.end, s); removePill(s, p); flipPill(s, p); }
  }
});

test("pills: one per top-level condition, with labels and spans", () => {
  const q = "(horror | scary) -tag:game cap:>=8 \"escape room\" lake";
  const r = parseQuery(q);
  assert.deepEqual(r.pills.map((p) => [p.kind, p.negated, p.label]), [
    ["group", false, "(horror | scary)"], ["field", true, "tag: game"], ["num", false, "cap ≥ 8"], ["phrase", false, '"escape room"'], ["term", false, "lake"],
  ]);
  assert.equal(q.slice(r.pills[1].start, r.pills[1].end), "-tag:game");
  assert.equal(r.pills[2].field, "cap");
  assert.equal(r.pills[2].value, ">=8");
  const r2 = parseQuery("a OR b c");
  assert.deepEqual(r2.pills.map((p) => p.label), ["a OR b", "c"]);
  assert.equal(parseQuery("published:2024-06").pills[0].label, "published 2024-06-01 – 2024-06-30");
});

test("removePill / flipPill return new query text", () => {
  const q = "(horror | scary) -tag:game cap:>=8 lake";
  const r = parseQuery(q);
  assert.equal(removePill(q, r.pills[1]), "(horror | scary) cap:>=8 lake");
  assert.equal(removePill(q, r.pills[0]), "-tag:game cap:>=8 lake");
  assert.equal(removePill(q, r.pills[3]), "(horror | scary) -tag:game cap:>=8");
  assert.equal(flipPill(q, r.pills[1]), "(horror | scary) tag:game cap:>=8 lake");
  assert.equal(flipPill(q, r.pills[3]), "(horror | scary) -tag:game cap:>=8 -lake");
  assert.equal(flipPill(q, r.pills[0]), "-(horror | scary) -tag:game cap:>=8 lake");
  const q2 = "a OR b c";
  assert.equal(flipPill(q2, parseQuery(q2).pills[0]), "-(a OR b) c");
  const q3 = "NOT lake x";
  assert.equal(flipPill(q3, parseQuery(q3).pills[0]), "lake x");
  const q4 = "a AND b";
  assert.equal(removePill(q4, parseQuery(q4).pills[1]), "a");
  // round trip: flipping twice restores a normalised query
  for (const x of ["lake -hangout", "\"a b\" -c", "-(a | b) c", "tag:x -cw:horror"]) {
    const ps = parseQuery(x).pills;
    for (const p of ps) assert.equal(parseQuery(flipPill(flipPill(x, p), parseQuery(flipPill(x, p)).pills.find((q) => q.start === p.start)!)).pills.length, ps.length);
  }
});

test("condition / addCondition", () => {
  assert.equal(condition("tag", "escape room", true), '-tag:"escape room"');
  assert.equal(condition("cap", ">=8"), "cap:>=8");
  assert.equal(condition("", "two words"), '"two words"');
  assert.equal(addCondition("", "tag:x"), "tag:x");
  assert.equal(addCondition("lake", "-tag:x"), "lake -tag:x");
  assert.equal(addCondition("lake -tag:x", "-tag:x"), "lake -tag:x");
  assert.equal(addCondition("lake -tag:xy", "-tag:x"), "lake -tag:xy -tag:x");
});
