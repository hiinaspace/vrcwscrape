# Filter / facet / search semantics the oracle assumes

The oracle (`common.py`, DuckDB) is the reference. The site must match these exactly
for `window.vrcwSearch.query(...)` (facet exact-match is graded against them).
Write to the oracle author if a choice is unworkable; do not silently diverge.

## `query({q, filters, sort, limit})`
`filters` keys mirror the URL params: `cap_min`, `cap_max` (numbers), `size_max_mb` (number),
`platform` (`"pc"|"quest"|"both"`), `after`, `before` (`"YYYY-MM-DD"`), `tags` (array of strings),
`author` (author_id). Absent/empty key = no constraint. `limit` caps `ids` only; `total` and
`facetCounts` always cover the whole filtered result set. Harness facet checks use `q: ""`.

## Filters (all ANDed)
- **capacity**: `cap_min <= capacity <= cap_max`, inclusive. A world with NULL capacity fails if either bound is set.
- **platform**: a world "has PC" iff `platforms` contains `pc` ("has Quest" iff it contains `quest`); if the parquet has no `platforms` column, fall back to a non-NULL `pc_size_*` / `quest_size_*`.
  `pc` = has PC; `quest` = has Quest; `both` = has both. (The fresh export's platform list, if it
  disagrees, wins; then the oracle changes, tell the orchestrator.)
- **size_max_mb**: size in MB = bytes / 1048576 (the parquet's `pc_size_mb`, `quest_size_mb`). Which package:
  - `platform=pc` -> `pc_size_mb <= X`; `platform=quest` -> `quest_size_mb <= X`;
  - `platform=both` -> both sizes `<= X`;
  - **no platform filter** -> `min(pc, quest over the non-NULL ones) <= X` ("downloadable under X on some platform").
  A missing size never satisfies a size filter.
- **after / before**: applies to `updated_at` (the "updated" date), UTC. `after` = `updated_at >= after 00:00:00`;
  `before` = `updated_at < before 00:00:00 + 1 day` (both bounds inclusive by calendar day).
  Published date (`created_at`) is only a sort key (`sort=published`), not a filter.
- **tags**: AND of all listed tags. Tag key = raw tag with the `author_tag_` prefix stripped (`author_tag_chill` -> `chill`);
  other prefixes (`system_approved`, `admin_...`) are kept verbatim. A world's tag set is deduplicated after stripping.
- **status** (`status=approved,labs`, array of `approved`/`labs`): AND; requires tags `system_approved` / `system_labs`.
- **cw** (`cw=horror,gore`): AND; requires `content_<name>` tags. **cw_exclude** (`cw_exclude=adult,sex`): world must carry none of them.
- Tag classes (DESIGN.md): hidden = `admin_ adm_ feature_ debug_ system_*` except status; status = `system_approved|system_labs`;
  cw = `content_*`; user = the rest. Hidden tags stay filterable via `tags=` and (unchanged contract) count in `facetCounts.tags`.
  Gold tag-word queries and similar-worlds Jaccard only use user-class tags.
- **author**: exact `author_id` match.

## `total` and `facetCounts` (conjunctive: counted over the filtered set, no "exclude own dimension" rule)
- `total`: number of worlds matching `q` and all filters.
- `facetCounts.platform = {pc, quest, both}`: number of matching worlds with PC / with Quest / with both.
- `facetCounts.tags = {tagKey: count}`: per-tag count within the matching set, tag keys as above. Must contain
  **at least the top 20 tags** (count desc, tag asc on ties); more is fine. Harness checks the truth's top-20 entries.
- `facetCounts.capacity` (optional, reported but not graded): buckets `"1-8"`, `"9-16"`, `"17-32"`, `"33-64"`, `"65+"`
  over non-NULL capacity; empty buckets omitted.

## Search quality scoring (gold set)
Gold query types: exact title, 2-3 word title substring, 2-4 char CJK substring (ja/ko/zh), tag word
(tag key with `_` -> space), author name. Each has an `expected` id set (all worlds matching the substring/name/tag,
capped at 200). Tag-word queries (>= 4 chars, stopwords dropped) are instead graded by **precision@10 = share of the top-10 that carry the tag (target >= 0.7)**. **recall@10 = fraction of queries whose top-10 `ids` contain at least one expected id**
(hit rate). We also report target-id@10 (the specific sampled world is in the top 10) and p@10. Gold substrings are
chosen so the title-match set has <= 20-30 worlds (not trivially common). The site's `ids` order is its
`sort=relevance` ranking. Tag words and author names must therefore be searchable via `q`.

## Similar worlds
`similar(worldId, k)` returns up to k world ids (excluding the world itself); empty array if no neighbor data.
Graded on up to 200 popular worlds that return a non-empty list (coverage reported): neighbors vs 10 random worlds on
tag Jaccard (system_ tags excluded), same author_id rate, same-script rate (name contains any kana/han/hangul = "cjk").
PASS = Jaccard > 1.5x random, same-author >= random, same-script > random.

## Semantic search (optional, `window.vrcwSearch.semantic`)
Assumed shape (DESIGN.md does not define it yet; harness adapts to either): an object with `load()` (triggers the model download)
and `query(q, k) -> Promise<string[] | {ids}>`, or a plain function `(q, k)` whose first call loads. If absent, the check is skipped.
Set: the 400 `cjk2en` rows of `/work/projects/vrcwscrape-data/emb/queries.parquet` (set, world_id, query); a hit is the source world_id in
the top 10 (offline static-model number: 20.2% recall@10, MRR 12.7). Reported: download bytes (CDP delta around `load()`), load time,
per-query latency p50/p95, recall@10 vs 20.2%. Recall is report-only: the queries derive from the indexed docs' own description text, so in-page recall is contaminated; the leak-free offline estimate is ~0.20. Graded: download <= 40 MiB, per-query p95 <= 100 ms, no errors.

## Popularity, author and content-default oracles
- Popularity: ~30 generic queries (`POP_QUERIES` in run.py); median log10(visits+1) of the site's top 10 (sort=relevance) must be >= the p75
  of all worlds whose title or tag ILIKE-matches the query (needs >= 20 matches); PASS if >= 80% of queries do.
- Author: 30 seeded authors with 3-150 worlds; query = exact `author_name`; share of top 10 by that author name, capped at min(10, their worlds); PASS if mean >= 0.7.
- Content default (UI-only; hook untouched): load `?sort=published` in a fresh context and read the rendered `result-item` world ids
  (a `wrld_...` id found anywhere in the item's HTML); none of the first <= 50 may carry content_sex / content_adult; then `&adult=1` must show >= 1 such world.
  Skip with `--content-default off`.

# Round 5 additions (2026-10-20)

## New filter keys (`query({filters})`, same names as URL params; truth in `common.where_sql`)
- `tags_ex` (array): world must carry **none** of the tags (tag key rules as `tags`).
- `status_ex` (`approved|labs`): world must lack `system_<x>`. `cw_exclude` (as before): lacks `content_<x>`.
- `platform_ex` (`pc|quest`): world must **not** have that platform (`platforms` list, same has-PC / has-Quest rule as `platform`).
- `pub_after` / `pub_before` (`YYYY-MM-DD`, inclusive calendar days, UTC): range on **published** (`created_at`). `after`/`before` stay `updated_at`.
- Facet counts stay conjunctive over the final set (exact `total`, `platform`, top-20 raw `tags`). The histogram facets are the exception:
  `facetCounts.months` = worlds per **published** month (`{m0, counts[]}`, month index = (year-2010)*12 + month-1, so `counts[k]` is month `m0+k`),
  computed with every filter **except** `pub_after`/`pub_before`; `updatedMonths` likewise for `updated_at` ignoring `after`/`before`;
  `capacityHist[c]` = worlds with capacity `c` (index 0..256) ignoring `cap_min`/`cap_max`. The harness compares non-zero months/capacities exactly
  on ~40 include/exclude combos. `userTagList` (merged groups) is reported by the site but not graded (grouping rules are the site's).

## Boolean query truth (`bool_truth.py`)
Queries are generated as an AST that renders to the site's syntax *and* to a DuckDB expression over table `docs`
(one row per public world; `blob` = ` title ## author ## tag words ## description[:1000] `, each part NFKC-lowercased, split into
`[\p{L}\p{N}\p{M}]+` latin words and separate CJK runs, joined by single spaces; tags joined by ` # `). Hidden/status tags (`admin_ adm_ feature_ debug_ system_*`)
are not text; `content_*` tags are (as the words `content horror`). A latin term matches as a whole word (`' term '` inside the padded blob), a CJK term as a substring.
Forms generated (about 48 queries on the real data): `a -b`, `a OR b` / `a | b`, `(a | b) -c`, `a b | c` (= a AND (b OR c): **OR binds tighter than the space-AND**),
`tag:x -tag:y`, `-cw:c tag:x`, `cap:>=16 a`, `updated:>=2025 a`, `"two words"`, plus `cap:8..32`, `visits:>1000`, `platform:quest`, `title:a -b`, `size:<50` (not gated: min-over-platforms), CJK bigram `-b`.
Terms are ASCII words of 4-10 letters whose document frequency is mid-range and whose description share is < 2% (so description-only stopword removal cannot differ).

Truth modes (each query is counted under all of them in one scan):

| mode | latin term rule | tag:x | phrase |
|---|---|---|---|
| `S` | whole word | exact raw tag | adjacent words in one field |
| `L` | whole word, **except the last plain term of the query, which prefix-matches (positive or negated)** | case-insensitive exact | adjacent anywhere OR all words in the description |
| `LG` | as `L` | case/plural-merged (`x`, `xs`, `x` without `s`) | as `L` |
| `P` | **every positive** term prefix-matches, negated terms whole word | as `LG` | as `L` |
| `N` / `PP` | negated only / all terms prefix-match | `N`: as `L`; `PP`: as `LG` | as `S` / as `L` |

`L`/`LG` are the rules written in DESIGN.md "Query language"; `P` is a deliberately loose superset used for the precision gate.

Grades (per generated query; `gate: false` rows are reported, not gated):
- **Precision (gate, must be 1.000):** every one of the first 50 ids from `query({q, filters:{}, limit:50})` satisfies `P`
  (positives present, possibly as a prefix; negated terms absent as whole words). Reported next to it: precision under the documented rule `LG`, and under the strict whole-word rule `S`.
  An empty result for a query whose truth has >= 15 hits (>= 80 on the real data) fails.
- **Total (gate):** `total` lies inside `[min over modes, max over modes]` widened by 3 % (plus 2 absolute), i.e. between the strictest and loosest reading.
- **Tight (graded >= 90 %):** `total` within +-3 % of the `L` or `LG` count: the site implements the documented rule.
- **Typed UI agreement:** 6 of the queries are typed into `search-input`; the first 10 rendered `result-item` ids must overlap the hook's top 10 in >= 8 places.

Documented tokenizer differences the grade allows for (and nothing else): (1) as-you-type prefix expansion of the last plain term (above);
(2) `tag:x` grouping of case / trailing-`s` variants; (3) quoted phrases are looser on the site (title pair OR exact tag/author OR all words in the description);
(4) CJK matching is bigram-AND on the site vs substring in truth (identical for 2-character terms, which is what is generated);
(5) description is indexed only to 1000 chars with its own stopwords dropped (avoided by term choice); (6) site NFKC folding vs the harness's `unicodedata` NFKC are the same data, accents are *not* stripped on either side.

## Default sort, thumbnails, nothing hidden (UI contract the harness reads)
- **Trending:** `query({q:"", sort:"trending", limit:20})` and the rendered default view (no params) must both have a median `recent_visits_per_day` >= the p95 (non-null values) of all worlds in the v2 parquet. Skipped when the parquet lacks the column.
- **Thumbnails:** no request to a `*.vrchat.cloud`, `*.vrchat.com` or `*.vrchat.net` host on first view (with scrolling) or after opening a result's detail panel; the detail panel should link the official `vrchat.com/home/world/<id>` page (reported). Up to 100 `<img>` inside `result-item`s (across six result pages) must have `naturalWidth > 0` once their src is a `blob:`/packed url; skipped if every image is a placeholder. Thumbnail bytes = `r.sizes()` of requests whose URL contains `thumb`, reported per page (report only).
- **Nothing hidden by default:** `?sort=published` with no other params renders, in the DOM, the same worlds as the engine's top-N published list (>= 98 % overlap, N up to 200 by scrolling / `load-more`), including every adult-tagged (`content_sex|content_adult`) world in that range; the engine's top 200 must contain at least one. This replaces the round-4 "adult hidden by default" check.
- **Real-UI semantic toggle (Chromium and Firefox):** load the site, (optionally type a query), click `sem-toggle`, click `sem-enable` if it appears (native `confirm()` dialogs are accepted and listed as a warning). With a query: >= 1 `semantic-item` must render within 180 s and the page body must have changed after the toggle click. Without a query: a visible enabled state (toggle `checked`/`aria-checked`/`data-state`, or text in `sem-state`/`sem-note` matching on|enabled|ready|active) and a changed DOM. Bytes = sum of `request.sizes()` after the click.
