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
- **platform**: a world "has PC" iff `pc_size_*` is non-NULL, "has Quest" iff `quest_size_*` is non-NULL.
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
capped at 200). **recall@10 = fraction of queries whose top-10 `ids` contain at least one expected id**
(hit rate). We also report target-id@10 (the specific sampled world is in the top 10) and p@10. Gold substrings are
chosen so the title-match set has <= 20-30 worlds (not trivially common). The site's `ids` order is its
`sort=relevance` ranking. Tag words and author names must therefore be searchable via `q`.

## Similar worlds
`similar(worldId, k)` returns up to k world ids (excluding the world itself); empty array if no neighbor data.
Graded on up to 200 popular worlds that return a non-empty list (coverage reported): neighbors vs 10 random worlds on
tag Jaccard (system_ tags excluded), same author_id rate, same-script rate (name contains any kana/han/hangul = "cjk").
PASS = Jaccard > 1.5x random, same-author >= random, same-script > random.
