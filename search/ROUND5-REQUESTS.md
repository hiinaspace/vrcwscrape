# Round 5 requests from the UI builder to the ENGINE builder

1. **Row-sized card data (bytes).** The result rows now show all user tags + the first 160 chars of the description, read from the 512-world `card/*.bin` chunks (~25 KB gz each).
   Popularity-ordered pages touch few chunks (empty query: +~30 KB) but relevance pages scatter: `?q=lake -hangout` = 46 requests / 2.9 MB first view (budget 3 MB; it was 2.65 before this round).
   Ask: a small per-row file, e.g. `rowcard/<n>.bin` with 64-world chunks holding only `{user tags, desc[0:160]}`, plus an `eng.rowCards(idx[])` that the UI can call instead of
   `cardChunk()` (the detail panel keeps the full chunk). Until then the UI patches tags/description into rows after first paint, so time-to-first-row is unaffected.
2. **Semantic strip should honour typed conditions.** `eng.allowMask(filters)` only covers `Filters`; `cap:>=16`, `-tag:game`, `updated:>=2025`, `platform:quest` typed in the box are
   ignored by the "Related by meaning" strip. Ask: `eng.allowMask(filters, q)` (or `QueryOut.mask`) that evaluates the non-ranking conditions of `q`.
3. **Optional: newest-date anchor.** The "this week/month/year" presets are relative to the newest `updated` day found by scanning `core` once in the UI; an `eng.maxDay`
   (days since 2010-01-01) would remove that scan.
4. **Info only, no action:** with a query that matches nothing the histogram and tag bars are empty (conjunctive facets); this is correct but may look odd next to the "No worlds match" state.

## ENGINE replies (2026-10-20)
1. Done differently, cheaper: meta chunks (32 worlds) now include desc[0:160] + visible tags, so eng.info(idx) / eng.rowCards(idx) need no card chunk at all
   (one ~4 KB request per distinct 32-world chunk). Card chunks (64 worlds, full 700-char description) are for the detail panel only. NOTE: the dist built at 16:10 still carries the UI bundle
   compiled from the old main.ts; rebuild web after switching rows to info.desc/info.tags.
2. Done: eng.allowMask(filters, q) (q conditions included, fresh array).
3. Done: eng.maxDay.
