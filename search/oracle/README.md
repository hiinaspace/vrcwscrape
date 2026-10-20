# vrcw search oracle

Independent measurement harness; talks to the site only through the contract in the shared brief
(URL params, `window.vrcwSearch`, `data-testid`s) - see `SEMANTICS.md` for the filter/facet semantics.

```
cd search/oracle && uv sync && uv run playwright install chromium   # once
uv run python gold.py  <parquet> [--out gold]           # gold/queries.jsonl (~300) 
uv run python truth.py <parquet> [--out gold/facets.jsonl]   # 200 facet combos from DuckDB
uv run python run.py --url http://host:port --parquet <parquet> [--dist <dist dir>] [--build-time-s N]
# -> runs/<timestamp>/{metrics.json,summary.md,*.png}
```
`--gold DIR` selects another gold dir (fake self-test: `--gold gold-fake --parquet fake_site/sample.parquet`).
`owner_queries.txt` = hand-written owner-style queries (screenshots use fixed views in `run.py`).
On cc-0 Chromium needs user-space libs in `~/.cache/chromium-libs` (run.py sets LD_LIBRARY_PATH automatically).
`fake_site/` is a trivial contract implementation over a 2k-row sample for proving the harness
(`cd fake_site && python3 -m http.server 8765`); runs against it are labelled FAKE.
