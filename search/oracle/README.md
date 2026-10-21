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

## Round 5 (2026-10-20)
```
uv run python run.py --url http://host:port --parquet <v2 parquet> --r5-only [--no-shots] [--no-semantic-ui]   # only the round-5 checks (~1.5 min)
uv run python run.py --url ... --parquet <v2 parquet> --dist <dist>          # everything (v2 parquet is a superset of the public one)
uv run python selftest.py [--only NAME ...]                                   # mutation self-test against broken copies of the fake site
```
* `r5.py`: real-UI semantic toggle (Chromium and Firefox), boolean query truth (`bool_truth.py`), include/exclude + histogram facet exactness,
  trending default, no-VRChat-host / thumbnail decode, nothing-hidden-by-default, owner screenshots (`r5_*.png`: zero state, `lake -hangout` pills,
  detail panel, brushed date histogram; desktop 1440x900 and mobile 390x844). Test ids are resolved from candidate lists in `r5.SEL`
  (first entry = DESIGN.md); screenshots whose element is missing are saved as `FAILED_*.png` and listed.
* Firefox on cc-0: `~/.cache` is full (10 GB volume at 100 %), so Playwright's Firefox lives in `/work/projects/vrcwscrape-pw-browsers`
  (`PLAYWRIGHT_BROWSERS_PATH=/work/projects/vrcwscrape-pw-browsers uv run playwright install firefox`; its GTK3 libs were extracted from Debian
  `.deb`s into `fflibs/` there without root). `common.launch_firefox` finds it (override with `VRCW_PW_BROWSERS`); without it the Firefox rows are skipped.
* Fake site (`fake_site/`): 6,000-row sample of the v2 public parquet (`make_data.py <v2 parquet>` regenerates `sample.parquet`, `data.json`, `sem.bin`;
  the last two are git-ignored). It implements the v2 contract (boolean syntax, `*_ex` filters, histogram facets, trending, `sem-toggle`/`sem-enable`,
  `date-hist` brush, detail panel, blob thumbnails). `selftest.py` mutates it (ignored negation, wrong OR precedence, dead semantic toggle, hotlinked thumbnails,
  hidden adult worlds, ...) and expects the matching rows to FAIL while the baseline passes.
* Old checks still run in full mode. Removed: "adult default hidden" (replaced by "nothing hidden by default").

## Round 6 (2026-10-21)
`r6.py` (also runs inside `run.py`, or alone with `--r6-only`; `--no-r6` skips): aligned percentile bars (`bar-<name>` tracks, first 15 rows, 1440 and 390 px),
bar fill vs true percentile rank (50 worlds, +-1 point, tie-aware), detail-panel history vs `history/uploads.parquet` (30 worlds incl. the 3 heaviest; summary total and
last-upload exact; `upload-tick` count graded only up to `VRCW_TICK_MAX`=60 events, heavier worlds must have `upload-month` counts (`data-count`) summing to the true total;
sparkline points in (0, metrics rows]; no `histor*` request before a panel opens; history bytes reported), recent filter 7/30/90 d and "most updated" top 20 >= p99.
Truth: `$VRCW_HISTORY_DIR` (default `/work/projects/vrcwscrape-data/history`; missing files -> history/recent rows skipped), as-of from `VRCW_ASOF` or an `asof*` file there.
Events collapse uploads chained within 10 minutes. Test ids / hook keys are candidate lists at the top of `r6.py` (`SEL`, `SORT_CANDIDATES`, `FILTER_KEY_CANDIDATES`; override with
`VRCW_RECENT_KEY`, `VRCW_SORT_MOST`) - sync them with DESIGN.md round 6. Fake site: `cd fake_site && uv run python make_history.py sample.parquet` then `VRCW_HISTORY_DIR=$PWD/fake_site/history`.
The dist total-size row is report-only now; file count < 19,000 and per-file <= 25 MiB stay graded.
