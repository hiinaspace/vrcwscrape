# Static world search: design and measurements

Data: June parquet, 218,097 worlds (all SUCCESS), measured on cc-0 (12 cores) and headless Chromium 1248 against
`vite preview` on localhost (no network latency; transfer sizes are real gzip bytes).

## Pipeline
`search/build.sh <parquet> [embeddings.npy ids.parquet]` -> `search/dist/` (vite app + `data/`). Build time (June data,
incl. similar): **32 s wall**, ~55 s CPU. Worlds are sorted by visits desc, so a world's index *is* its popularity rank.
All payloads are gzip, named `*.bin`, sniffed by magic bytes client-side (`DecompressionStream`), so any static host
works whether or not it adds `Content-Encoding`.

## Full-text design: measured

### (a) SQLite FTS5 over HTTP Range (not shipped)
Built with Python sqlite3 3.46, page_size 4096, `optimize` + `vacuum`; page reads recorded with an apsw VFS
(`build/eval_fts5.py`; lower bound for an sql.js-httpvfs / wa-sqlite range VFS, no read-ahead, cache off).

| index | file size |
|---|---|
| trigram on title+author+tags | 60.7 MB |
| trigram on title+author+tags+description(1000 chars) | 155.7 MB |
| unicode61 + prefix 2,3 on all four fields | 99.5 MB |

Per query (50 results, ordered by `rowid` = popularity, i.e. no bm25):

| query | 4K pages | contiguous runs (~sequential requests) | bytes |
|---|---|---|---|
| trigram `chill` | 17 | 14 | 68 KiB |
| trigram `ミラー` | 9 | 6 | 36 KiB |
| trigram `night club` | 33 | 21 | 132 KiB |
| trigram `睡眠` (2 chars) | no results: trigram needs >= 3 chars | | |
| unicode61 `chill*` | 71 | 6 | 284 KiB |
| unicode61 `cozy hangout*` | 31 | 11 | 124 KiB |

With `ORDER BY rank` (bm25) FTS5 must read whole doclists: `chill` trigram = 9,111 pages / **36 MB**, `ミラー` 6 MB,
unicode61 `chill*` 3 MB. Local latency 6-330 ms before any network.
Verdict: 1.5-6x the total byte budget (>= 255 MB for both tokenizers with description, vs ~200 MB cap), needs a custom range
VFS (wasm blob + chunked-file mapping since each file <= 25 MiB), 6-21 *dependent* round trips per query, cannot do
2-char CJK queries without a second index, and popularity-blended relevance means scanning doclists. Not built in-browser;
the above is a trace-based lower bound, **not a browser measurement**.

### (b) Custom sharded inverted index (shipped)
Terms: NFKC-lowercased latin/digit words + CJK bigrams (CJK unigrams for title/author/tag fields so 1-char queries work),
`build/tok.py` == `web/src/tok.ts`. Plus one `\u0001<normalized title>` term per world for exact/prefix-title boosts.
Fields: title, author, tag words, description (first 1000 chars, lower weight; 23 description-only stopwords dropped).
Dictionary+postings are sharded into 192 files by FNV-1a of the first two code points of the term (so prefix expansion of the
last word stays inside one shard). Postings = varint(delta-doc << 4 | field mask).

* Index: 192 shards, 14.2 MB gz total, largest 234 KB. 5.4 M postings, 565 k terms.
* Query = one shard fetch per distinct token (1 round trip, parallel), AND across tokens, last latin word is prefix-expanded.
  Ranking: sum of field weights (title 10, author 6, tag 6, desc 1; prefix-only 0.6x) + log(visits, favorites) prior;
  exact title +1000, title-prefix +300.
* Cold page load with `?q=` (core 2.3 MB + shard + first-24 metadata chunks), localhost:
  `chill` 2.72 MB / 31 requests / 292 ms to first rendered result; `ミラー` 2.71 MB / 173 ms; `cozy hangout` 2.91 MB / 176 ms;
  empty query 2.35 MB / 7 requests / 185 ms. (First-view budget 3 MB: thin margin.)
* Warm in-page query (`vrcwSearch.query`, 200 gold queries repeated): p50 2.2 ms, p95 4.3 ms, max 5.8 ms (shards cached).
  A *new* query needing a shard fetch plus 50 id-metadata chunks: 46-110 ms on localhost (13-54 small requests).
* Recall, own gold set sampled from the data (visits > 50), requiring the *sampled world itself* in the top 10 (stricter than the
  oracle's hit-any): exact title 198/206, 2-word partial title 65/83, CJK substring 14/17, author name 77/94.
  Misses are mostly duplicated titles / prolific authors (another equally valid world outranks the target).
* Known gaps: no sub-word latin substring ("hill" in "chill"), no typo tolerance; description only indexes the first 1000 chars;
  CJK matching is bigram-AND (can false-positive on non-adjacent bigrams, e.g. 1685 hits for `ミラー` vs 1678 true substrings).

## Facets / filters
`core.bin` (gz 2.36 MB) holds typed columns for every world (byte-plane shuffled for gzip): capacity u8, PC/Quest size u32
KB (ceil), created/updated u16 days, visits (delta of sorted order), favorites u32, platform bits, 8 top-tag bit flags,
CSR tag ids for tags with count >= max(20, n/5000) (rest in lazily loaded `tags_rare.bin`, needed only to filter on a
rare tag or when a filtered set's 20th tag count is below the cutoff and an exact top-20 is requested).
Filter + facet counting is a linear scan over 218 k worlds: 8-12 ms (cold JIT) per `query()`, filters verified exact
against DuckDB on four combos (41,056 / 3,411 / 10,579 match). Size filter units are KB-ceil, exact whenever `size_max_mb*1024`
is an integer. Semantics follow `search/oracle/SEMANTICS.md`.

## Similar worlds
`build/similar.py`: blocked numpy matmul (512 x 16384), cosine, k=20, neighbours stored as world indices, 4096-index shards
`data/sim/<n>.bin`. 20k June embeddings (row order = `mapgen/artifacts/embed_meta.parquet`): 6.5 s, 0.75 MB. Worlds without
an embedding show "no neighbor data". Full 235k at k=20 would be ~14 MB (budget ok), ~5 min compute.

## Sizes (June build)
dist: 60 MB, 2,913 files, largest file `core.bin` 2.36 MB (limit 25 MiB). By dir: fts 14.2 MB, meta 15 MB (128-world chunks),
card 12.7 MB (512-world chunks), ids 5.4 MB, authors 2.6 MB, sim 0.75 MB, core 2.4 MB, tag lists 0.55 MB.

## Round 2 changes (2026-10-20)
* **Ranking**: tag words are a weighted field (9 vs title 10); an exact tag-phrase term (`\u0002<tag with spaces>`) adds +70;
  title exact +1000, title prefix +60; adjacent-word title pairs (`\u0003w1 w2`, punctuation kept, so "Lounge -" and
  "(Fixed for" work) add +30, or +60 and no exact/prefix title bonus when the query contains punctuation.
  Measured on the fresh build, 80 random user-tag words: any tag carrier in top 10 = 92.5%, mean fraction of top 10 carrying the tag = 74.5% (was 78% / 33% in the harness run).
  Punctuation-only tokens were already dropped by the tokenizer; the misses were ranking, now fixed (checked by hand on the four harness examples).
* **Tag classes** (`tagClass` in engine.ts == `tag_class` in build_index.py): hidden = `admin_ adm_ feature_ debug_ system_*` except
  status; status = `system_approved`, `system_labs`; cw = `content_*`; user = everything else (author tags, stripped). Hidden tags are not
  FTS-indexed and never shown; they remain filterable through `tags=`.
* **New URL / filter params** (additive): `status=approved,labs` (require, AND), `cw=horror,gore` (require content_*),
  `cw_exclude=adult,sex` (exclude). Same keys in `vrcwSearch.query({filters:{status:[],cw:[],cw_exclude:[]}})`.
  `facetCounts.tags` is unchanged (all tags, oracle contract); extras `facetCounts.status`, `.cw`, `.userTags` drive the sidebar.
* Mobile (<=760px): filters live in a full-screen drawer behind a "Filters (n)" button; results are above the fold.
* Fresh data: built against `worlds-2026-10-20.parquet` (197,991 worlds; `platforms` list used, thumbnails from `thumbnail_url`
  hotlinked, naturalWidth 256 confirmed in headless Chromium on cc-0). Build 37 s, dist 65 MB, core.bin 2.14 MB.

## Round 3 (2026-10-20): public data, gemma2 neighbours, opt-in semantic search
* Site data: `worlds-2026-10-20-public.parquet`, 197,992 worlds. Build ~29 s for the index; `dist` 104 MB, 2,767 files, largest file 6.4 MB.
* Neighbours: embeddinggemma-2 fp16 768-d vectors, rows mapped to the public ids (all 197,992 have one), exact cosine top-20 by blocked
  matmul over RAM-resident normalised fp16 (460 s on cc-0, 8 threads, ~1 GB RSS). `data/sim/`: 49 shards, 9.3 MB, largest 193 KB.
* **Semantic search** (`web/src/sem.ts`, lazy chunk of 1.6 KB gz, assets in `data/sem/`, ~33 MB, only fetched after the user ticks
  "semantic search (downloads ~33 MB)" and confirms): static-similarity-mrl-multilingual-v1 truncated to 256-d. JS reimplements
  BertNormalizer (clean, CJK spacing, accent strip, lowercase) + BertPreTokenizer + WordPiece, wraps with [CLS]/[SEP], and mean-pools the
  int8 table rows x per-row f16 scale (token table 4 x 6.4 MB gz, scale 173 KB, tokenizer.json 0.8 MB gz, world sign vectors
  `vecs.bin` 6.3 MB = 197,992 x 32 bytes in index order, public only). Query vector -> sign bits -> Hamming scan of all worlds
  (honouring the active filters) -> best 500 rescored with the float query against the sign vectors -> top-k. No int8 world vectors
  (50 MB extra); the eval notes put binary at ~3 points of recall@10 below int8, so this is a deliberate size trade.
  Blending: semantic hits are shown as a separate "Related by meaning" section (12 hits, skipping worlds already in the FTS list)
  below the FTS results, never mixed into them, so FTS ranking and the oracle metrics are untouched. Hook:
  `window.vrcwSearch.semantic(q, k) -> Promise<string[]>` (unfiltered; loads the assets on first call without a prompt).
* Verified: JS query vectors vs a numpy reference (HF `tokenizers` + dequantised table) on 20 queries (en/ja/ko/zh/mixed/accents/
  fullwidth/punctuation): min cosine 1.00000. Semantic query latency in headless Chromium on cc-0 (n=60): p50 5.3 ms, p95 9.8 ms.
  Loading all assets: 35 MB transferred, 455 ms on localhost.
* First-view transfer with the toggle off (same-origin bytes): 2.15 MB (empty query), 2.65-2.88 MB with a text query. Thumbnails
  (~75 KB PNG each, hotlinked from the VRChat CDN, third-party) load immediately for cards in view (IntersectionObserver, 100 px margin);
  ~0.6 MB extra on a 1440x900 viewport, 24 eager thumbs were 1.78 MB. No delay is applied.

## Round 4 (2026-10-20): ranking, UI polish
* **Relevance = text score x popularity prior.** text = field weights (title 10, tag 9, author 6, description 0.4) + bonuses
  (exact title +150, title prefix +35, exact tag phrase +45, exact author name +80 via `\u0004` terms, adjacent title pair +20/+40).
  prior = exp(0.3 * (log10(1+visits) + 0.5*log10(1+favorites))) (1.0 .. ~25). Ties -> lower index = more visits. New sort `text`
  ("best text match") = text score only. Semantic hits: cosine(sign vectors, float query) + 0.015 * log-popularity over the Hamming top-1500.
  Top-5 before/after for horror/club/avatar/chill/spookyghostboo is in the round-4 report; tag-word precision@10 (own check) 0.76, any-hit 0.94.
* **UI-only defaults** (the hook/contract is untouched): adult content warnings (`content_adult`, `content_sex`) are hidden unless "Show adult worlds"
  is ticked (persistent in localStorage, URL `adult=1` overrides). Platform button counts are what-if counts (`facetCounts.platformIf`, only when
  `query({whatIf:true})`). Tag facet and tag search merge case/plural variants into groups (`tags=~chill` = any of chill|Chill; manifest `tagGroups`);
  plain `tags=chill` keeps exact raw semantics and `facetCounts.tags` stays raw for the oracle. Merged counts are sums (a world with two variants counts twice).
* Result rows show PC / Quest badges from `platforms` (size only when known); date shown matches the sort; semantic hits are a strip above the FTS list.

## Engine API v2 (round 5; owner: ENGINE builder; UI codes against this)
Source of truth for types: `web/src/engine.ts` (Engine, Filters, QueryIn/Out, FacetCounts) and `web/src/query.ts` (parser, Pill, helpers).
Step-1 commit has stubs (new facets empty, `thumbUrl` null, syntax parsed but not yet evaluated); signatures are final. Status of each item is
tracked in the Round 5 section below.

```ts
// ---- query.ts (pure, no fetch/DOM, never throws)
parseQuery(text: string): { ast: Node | null; pills: Pill[]; errors: string[] }
interface Pill {
  id: string; kind: "term"|"phrase"|"field"|"num"|"date"|"group"; field?: FieldName;   // field: tag|title|author|cw|status|platform|cap|size|visits|favorites|updated|published
  label: string;            // display text: lake | "escape room" | tag: japan | cap ≥ 8 | size < 50 MB | published 2024-06 | (horror | scary)
  value: string;            // value part only: japan | >=8 | 2024-06
  negated: boolean;         // pill came from -x / NOT x
  start: number; end: number; // [start,end) span in the exact text that was parsed, including the leading '-'/'NOT '
}
removePill(text: string, pill: Pill): string        // new query TEXT without this condition
flipPill(text: string, pill: Pill): string          // new query TEXT with include<->exclude flipped (group pills get -( ... ))
condition(field: string, value: string, negated?: boolean): string  // -> `-tag:"escape room"`, `cap:>=8`, field "" = plain term
addCondition(text: string, cond: string): string    // append (idempotent for an identical condition string)
// top-level pills = the top-level AND items of the query. `(horror | scary) -tag:game cap:>=8` -> 3 pills (group, negated field, num).

// ---- engine.ts
class Engine {
  init(): Promise<Engine>
  query(q: QueryIn): Promise<QueryOut>          // run() is an alias; main.ts's window.vrcwSearch.query must forward q verbatim (syntax allowed)
  thumbUrl(i: number): Promise<string | null>   // blob/object URL of the AVIF thumb for world ordinal i, or null (no thumb / not built). Never a remote URL.
  hasThumbs: boolean                            // manifest says thumbs exist (decide placeholder vs img)
  trendOf(i: number): number                    // recent visits/day estimate (dequantised); NaN when only the popularity fallback exists
  trend: { basis: "recent_visits_per_day" | "popularity_fallback"; windowDays: number | null }
  months: { min: number; max: number }          // global month-index domain (months since 2010-01) of published/updated, for stable histogram axes
  // unchanged: c (columns), m (manifest), info(idx[]), similar(idx,k), idToIdx(id), allowMask(filters), lp, groupOf, tagsAll()
}
monthToDate(m: number): string   // "YYYY-MM-01" first day of month index m
dateToMonth(iso: string): number

interface QueryIn {
  q?: string;                       // FULL syntax (see "Query language" below); '' = no text condition
  filters?: Filters;                // ANDed with q
  sort?: "trending"|"relevance"|"text"|"visits"|"favorites"|"updated"|"published"|"capacity";   // default "relevance"
  asc?: boolean;                    // ascending (visits/favorites/updated/published/capacity); ignored for trending/relevance/text
  limit?: number;                   // default 50
  offset?: number;                  // default 0 (paging; sort is deterministic, ties -> lower ordinal = more visits)
  facets?: boolean;                 // default true; false skips facet counting (total still exact) -- use for "load more"
  tagLimit?: number;                // entries in facetCounts.userTagList, default 40
  whatIf?: boolean;                 // adds facetCounts.platformIf
  exactTail?: boolean;              // legacy
}
interface Filters {                 // all optional; dates are inclusive calendar days "YYYY-MM-DD" (UI quantises to year/month itself)
  cap_min?, cap_max?: number;       // inclusive
  size_max_mb?: number;
  platform?: "pc"|"quest"|"both"|"";
  platform_ex?: "pc"|"quest"|"";    // NEW: exclude worlds that have this platform
  after?, before?: string;          // UPDATED date range (oracle contract: after/before = updated_at)
  pub_after?, pub_before?: string;  // NEW: PUBLISHED (created) date range. The uploads-over-time brush writes these.
  tags?: string[]; tags_ex?: string[];   // NEW tags_ex. "~group" = any case/plural variant (works in both)
  status?: string[]; status_ex?: string[];   // NEW status_ex ("approved"|"labs")
  cw?: string[]; cw_exclude?: string[]; author?: string;
}
interface QueryOut { idx: number[]; total: number; facetCounts: FacetCounts; ms: number; parse: ParseResult; textRanked: boolean }
interface FacetCounts {
  // unchanged: platform{pc,quest,both}, tags (RAW, oracle contract, top 60), capacity (5 buckets), status, cw, platformIf (whatIf only)
  userTagList: [key: string, count: number][];   // NEW ordered top-tagLimit user tags, case/plural merged ("~group" keys), count desc.
                                                  // Each world counts ONCE per merged group (round-4 sum double count fixed). userTags = same as a Record.
  months: { m0: number; counts: number[] };          // NEW published (created) histogram; counts[k] = month m0+k; ignores pub_after/pub_before only
  updatedMonths: { m0: number; counts: number[] };   // NEW updated histogram; ignores after/before only
  capacityHist: number[];                            // NEW worlds per capacity value, index 0..256; ignores cap_min/cap_max only
}
```
Histogram axes are the same for every query (m0 = `eng.months.min`, length covers `eng.months.max`), so the UI's x-axis never jumps. All other
facets stay conjunctive (counted over the final result set). `ms` includes parse + facets + top-k.

### Query language (q)
`lake -hangout` · `lake AND NOT hangout` · `神社 OR shrine` · `(horror | scary) -tag:game cap:>=8` · `"escape room" tag:japan -cw:horror`
* Terms AND by default (space or explicit `AND`). `-x` or `NOT x` excludes. `OR` (uppercase) or `|` = union. Parentheses group. Lowercase and/or/not are words.
* **OR binds tighter than AND-by-space** (Google/Gmail style): `a b | c` = `a AND (b OR c)`; so `horror | scary -tag:game` needs no parentheses.
* `"quoted phrase"`: title-adjacent words (title pair index), or the exact tag / author name, or all words in the description. Single quoted word = exact word, no prefix expansion.
* A plain term matches any indexed field (title, author, tags, description) with the site tokenizer (CJK bigrams). The last plain term of the query is prefix-expanded
  while the text has no trailing space (as-you-type), positive or negative.
* Fields: `tag:x` (exact tag, case/plural-merged; `tag:"escape room"`), `title:x`, `author:x` (name words, or `usr_...` id), `cw:x` (content_x), `status:approved|labs`,
  `platform:pc|quest|both`. Numeric: `cap:>=16`, `cap:8..32`, `size:<50` (MB, any platform), `visits:>1000` (k/m suffix ok), `favorites:>=50` (`favs`).
  Dates: `updated:>=2025`, `published:2024-06`, `published:2023..2024-06` (YYYY, YYYY-MM, YYYY-MM-DD; periods expand to their first/last day).
* Pure negative queries (`-hangout`) = all worlds minus the matches. Empty/incomplete conditions (`tag:`) are ignored while typing.
* Malformed input never throws: errors are listed in `parse.errors`; unbalanced parentheses auto-close, a stray `)`/`OR` is dropped, a known field with a bad value
  (`cap:abc`) falls back to a plain word and reports an error. Unknown `foo:bar` is a plain word.
* Ranking: sum of field-weighted scores of the positive leaves (+ the round-4 bonuses computed from the top-level plain terms) x popularity prior. With no positive text
  condition, "relevance" = popularity order.

## Round 5 UI (owner: UI builder; files `web/index.html`, `web/src/main.ts`, `style.css`, `sem.ts`, `web/src/ui/**`)
Layout: header (search box + `?` syntax popover + semantic switch), pill row, toolbar (summary + sort chips), results, optional right detail panel;
left sidebar order = uploads-over-time histogram, tag bar chart, platform, capacity, download size, content warnings, status, author.
Desktop >= 1024 px: detail panel is a sticky third column; 761-1023 px: right drawer; <= 760 px: full-screen sheet with a back button, filters in a drawer.
* **Semantic toggle bug (root cause, reproduced in Playwright Chromium 156 and Firefox 157 with real clicks)**: the old checkbox called native `confirm()`. (1) When the dialog
  is dismissed/suppressed (Playwright's default, Chrome after "prevent more dialogs", embedded webviews) `confirm()` returns false and the handler silently un-ticked the box: click
  = nothing. (2) When it is accepted, the 33 MB loaded in 0.3-0.4 s but the only visible results were a 12 px grey "ready" note, because the "Related by meaning" strip is hidden
  unless the query has >= 2 characters (empty/1-char: `semantic hidden = true`). The control was not hidden on mobile. Fix: a switch (`sem-toggle`) opens an inline confirm
  (`sem-enable` "Download 33 MB and enable"), a visible byte-based progress bar, and a visible "on" state; with no/short query the strip shows "Semantic search is on. Type a
  search ... " instead of disappearing. Consent is remembered (`localStorage vrcw-sem=1`) and re-enables from the HTTP cache on the next visit (no 33 MB download without prior consent).
  The semantic query text is the positive plain terms of the box (not `-x`, `tag:x`, `cap:>=8`).
* **State / URL** (additive to the oracle contract; `adult=` is ignored, nothing is hidden by default): `sort=` only when the user chose one (absent = automatic: `trending`
  without a positive text condition, `relevance` with one; an explicit choice sticks), `asc=1`, `platform_ex=pc|quest`, `tags_ex=`, `status_ex=`, `pub_after=`, `pub_before=`,
  `datef=updated|published` (which range the histogram edits). Existing: `q cap_min cap_max size_max_mb platform after before tags author status cw cw_exclude world`.
  Clicking the active sort chip flips ascending/descending (not for trending/relevance).
* **Controls**: sort = chips; capacity = double-ended slider with stops 1,2,4,8,16,24,32,40,50,64,80+ (lowest/highest stop = open end) over a mini histogram (`capacityHist`);
  download size = single-ended slider (5..1000 MB, any) + exact number input; uploads histogram = month bars from `facetCounts.months|updatedMonths`, fixed axis (global min..max
  of created/updated), drag = brush, drag handles = resize, drag body = move, click = one month / clear; year-quantised double slider below it; presets this week / this month /
  this year (rolling 7/30/365 days ending at the newest date in the data) / any. Updated/Published switch picks `after,before` vs `pub_after,pub_before` (both can be active).
  Tags, content warnings, status, platform are rows `[+][-] name bar count` with explicit include / exclude buttons (no click cycling); tag search rows use the same buttons.
* **Pills**: `parseQuery(q).pills` (sign flip = `flipPill`, x = `removePill`, both rewrite the box text) plus facet conditions (tags, warnings, status, platform with sign; capacity,
  size, dates, author without). Parse errors show next to the pills. Syntax popover examples fill the box.
* **Result row**: thumbnail (`eng.thumbUrl(i)`, gradient placeholder when null, never a CDN URL), title, author, cw/status badges, ALL user tags (click = include), first 160 chars of the
  description, log-scale bars for capacity, PC size, Quest size, visits (2nd..99.5th dataset percentile of the sampled column, numeric label inline, exact value in `title`),
  favorites, updated date. Rows render before the card chunks arrive; tags + description are patched in afterwards (`ui/rows.ts fillCards`).
* **Detail panel**: `[data-testid=detail-panel]` wraps `[data-testid=world-card]`; hero image links to `https://vrchat.com/home/world/<id>` (new tab). Esc / x / browser back close it; `world=` deep links open it.
* **Test ids added**: `sem-toggle` (button, `aria-checked`), `sem-enable`, `semantic-item`, `pill` (+ `data-src=query|facet`, `data-kind`, `data-neg`), `pill-sign`, `pill-remove`, `sort-<name>`
  (trending relevance visits favorites updated published capacity; container keeps `facet-sort`), `tag-include-<tag>` / `tag-exclude-<tag>` (tag without the `~` group prefix),
  `cw-include-<name>` / `cw-exclude-<name>`, `status-include-<name>` / `status-exclude-<name>`, `platform-include-pc|quest` / `platform-exclude-pc|quest`, `date-hist`, `date-years`,
  `datef-updated|published`, `date-preset-week|month|year|any`, `cap-range`, `size-range`, `syntax-help`, `detail-panel`, `no-results`. Kept: `results result-item search-input
  facet-<name> world-card similar-list semantic-results`. Removed: `adult-toggle`, `semantic-toggle` (checkbox). `window.vrcwSearch` is unchanged (query forwards `q` verbatim).
* **Checks**: `web/test/ui5/{launch,interact,semtest,shots,bytes}.mjs` (Playwright; Firefox 157 is installed under `/work/.tools/pw-browsers` because `~` is full, its GTK libs under
  `/work/.tools/ffdeps/root`). `interact.mjs` = 36 real-click/drag assertions, green in Chromium and Firefox; `semtest.mjs` = toggle flow incl. dialogs recorded (none are opened).
* **First-view bytes** (headless Chromium, localhost, with row cards): empty 2.37 MB, `?q=chill` 2.52 MB, `?q=lake -hangout` 2.90 MB (46 requests: scattered 512-world card chunks, see ROUND5-REQUESTS.md).

## Round 5 (engine): implementation notes and measurements
* **API additions after the v2 stub** (all in `web/src/engine.ts`): `eng.allowMask(filters, q?)` (typed conditions included; fresh array), `eng.rowCards(idx[]) -> {desc, tags}[]`
  (meta chunks now carry the 160-char description + visible tags, so `eng.info(idx)` returns `desc`/`tags` with no extra request), `eng.maxDay`
  (newest `updated` day), `QueryOut.literal`. `run()`/`allowMask()` are serialised (shared scratch buffers). `WorldInfo.thumb` is always "".
* **Evaluation**: parse -> dense per-leaf membership masks (postings decoded from the existing shards; all shards for a query are fetched in one parallel wave),
  AND/OR/NOT = byte-mask ops, NOT is a complement then ANDed with the filter mask. Scores = field-weighted sum of positive leaves; the round-4 bonuses use the raw top-level
  plain terms. Queries with no operators go through the unchanged round-4 path (319 gold/extra queries: identical top-10 and totals to the round-4 engine).
  A query whose raw adjacent word pairs all occur in one world's title (`Nest -night-`, `(Fixed for`, gold titles with ` -`) is read literally (`QueryOut.literal`);
  `-name-` (leading and trailing dash) is a plain word. Phrases: title-adjacent pair index, exact tag/author, or all words in the description (no positions in the index).
  `size:` = the smallest available package; `tag:x` merges case/plural variants of core tags (rare tags: exact lowercase match).
* **Filters/sorts/facets** as in "Engine API v2". One pass computes total, tag/group counts (groups counted once per world via a precomputed list of multi-variant worlds),
  month/capacity what-if histograms (per-world failure bits per dimension) and platform what-if counts (option bits, no extra scans). The last 4 (query, filters) results are cached
  (mask + scores), so sort changes and "load more" (`facets:false, offset`) cost ~1-2 ms. `exactTail` (rare-tag exact top-20) now defaults to on only for an empty `q`.
* **Trending** = column `trend` in core.bin (u16, 1024*log10(1+recent visits/day), +180 KB gz). Source `recent_visits_per_day` from `worlds-2026-10-20-public-v2.parquet`
  (window ~30 d; 181,506 worlds measured). The 16,486 worlds with `recent_basis = popularity_fallback` get the median recent rate of measured worlds with the same VRChat
  `popularity` value. Without the v2 columns the key degrades to `popularity*5000 + heat*500` (manifest `trend.basis = "popularity_fallback"`). Ties -> more visits first.
* **Chunk sizes**: meta 32 worlds (now with desc[0:160] + tags), card 64 worlds (full description 700 chars, for the detail panel). A scattered result page costs one ~3-5 KB meta
  request per distinct 32-world chunk instead of 128-world meta + 512-world card chunks (was ~1 MB, now ~0.15 MB). 10,116 files, largest 6.4 MB, core.bin 2.32 MB.
* **Thumbnails** (`build/pack_thumbs.py`, `web/src/thumbs.ts`): unique AVIFs (dedupe on sha256) stored back to back in `thumbs/p-NNN.bin` (<= 24 MiB, popularity order of first use, so the
  popular worlds share the first packs). Lazy index: `data/thumbs_index.bin` (header) + `data/th/w<k>.bin` (4096 worlds each: blob id codes) + `data/th/b<k>.bin` (2048 blobs each: u32 lengths;
  offsets are prefix sums per pack). A result page loads a few ~2-4 KB index shards. `thumbUrl(i)` coalesces requests made within 3 ms into one Range request per pack (gap <= 8 KB),
  returns a cached object URL per blob; HTTP 200 to a Range request switches to whole-pack download + slicing. `manifest.thumbs.base` (default `../thumbs/`) may be an absolute URL when the
  packs live on B2 (needs CORS: allow `Range` request header). Expected full scale: ~190k blobs x 3.7 KB = ~700 MB (does not fit Cloudflare Pages -> B2); index ~350 KB total, ~40 KB per page.
  `vite preview` (sirv) honours Range (206 verified); headless Chromium decodes the AVIFs (naturalWidth 256). Fixture measurement (5,455 worlds, real pilot AVIFs, 24-result pages):
  20 images = 74 KB in 10 requests (trending), 38 KB in 1 request (relevance, adjacent ordinals); waste from gap merging 3-9%.
* **Build**: `build.sh` packs thumbs when `THUMBS` (default `/work/projects/vrcwscrape-data/thumbs/thumbs.parquet`) exists, `REUSE_DATA=<old dist/data>` reuses sim/ + sem/ for an unchanged
  world order (full build 50-80 s). Tests: `cd web && node test/run.mjs` (bundles `test/*.test.ts` with esbuild, runs node:test against `work/r5/data`; `VRCW_DATA`, `VRCW_EXPECT` for thumbs).
  Latency (headless Chromium on a load-17 cc-0, uncached, 24 results + facets + what-if): p50 9-42 ms, p95 40-127 ms; node: cold-cache `q=""` facets 22 ms.
