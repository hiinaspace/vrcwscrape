#!/usr/bin/env python3
"""Deploy baseline: which build is live, and which public ids it had. Stdlib only (used by refresh.py before any venv exists, and by gate.py).

STATE/deployed-ids.parquet + STATE/deployed.json {version, commit, commitTime, deployedAt, run} describe the live site; the gate compares every
candidate with them. They must never describe a different build than the live one, so a deploy goes through a durable intent record:

  1. write_pending(): STATE/pending-deploy/{ids.parquet, meta.json {version, commit, commitTime, run}} (tmp dir, fsync, rename)
  2. wrangler pages deploy ...                               <- a kill from here on leaves the intent record behind
  3. live manifest shows the version -> promote(): deployed-ids.parquet, then deployed.json (each tmp + fsync + replace), then the accepted
     drift baseline STATE/extract/accepted (a genstate generation, advanced ONLY here: never by a dry run, a failed or a killed run), then
     pending is retired with ONE rename to pending-deploy.retired-<run> and deleted file by file (any leftover is garbage-collected at the next start)

reconcile() runs at the START of every run, before the window: it fetches the live manifest version and
  * == pending.version   the previous run deployed and died: promote pending (idempotent);
  * == deployed.version  the deploy never happened: drop pending;
  * pending incomplete   (cannot happen after a completed write_pending; defensive) and live == deployed.version: discard it;
  * anything else        FAIL LOUDLY ("live site is at an unknown version"): somebody deployed outside the job, or the live site is broken.
A pending record is only ever promoted after the live site showed its version, so deployed-ids never gets ahead of what is live. The one-time
migration for the hand-built launch build applies only when deployed.json is absent and the live version is exactly LAUNCH_VERSION.

  baseline.py adopt --state DIR --ids PUBLIC.parquet --live-url URL     attended: declare the live site's current manifest as the baseline
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "build"))
import genstate  # noqa: E402

LAUNCH_VERSION = 1792630179  # the hand-built 2026-10-20 snapshot deployed on 10-23
LAUNCH_DEPLOYED_AT = "2026-10-23T00:00:00Z"
STALE_DAYS = 9
DRIFT_FILE = "public-v2.parquet"  # the one extract file gate.py compares week over week


class UnknownLive(RuntimeError):
    pass


class DataStale(RuntimeError):
    pass


class NoNewData(Exception):
    """Not an error: the pinned Dolt commit is the one already live."""


def parse_t(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_json(p: Path) -> dict | None:
    try:
        return json.loads(p.read_text())
    except FileNotFoundError:
        return None


def write_json(p: Path, d: dict) -> None:
    tmp = p.with_name(p.name + ".tmp")
    genstate.write_durable(tmp, json.dumps(d).encode())
    os.replace(tmp, p)
    genstate.fsync_dir(p.parent)


def deployed(state: Path) -> dict | None:
    return read_json(Path(state) / "deployed.json")


def write_pending(state: Path, ids_src: Path, meta: dict, drift_src: Path | None = None) -> None:
    """Complete record first (ids, the extract file the drift gate reads, meta last), then one rename: a pending-deploy dir is either absent or whole."""
    state = Path(state)
    pend, tmp = state / "pending-deploy", state / "pending-deploy.tmp"
    if pend.exists():
        raise RuntimeError(f"{pend} exists: the previous run's deploy was not reconciled")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    genstate.hook("pending-ids")
    shutil.copyfile(ids_src, tmp / "ids.parquet")
    if drift_src:
        genstate.hook("pending-drift")
        shutil.copyfile(drift_src, tmp / DRIFT_FILE)
    genstate.hook("pending-meta")
    genstate.write_durable(tmp / "meta.json", json.dumps(meta).encode())
    genstate.fsync_tree(tmp)
    genstate.hook("pending-rename")
    os.rename(tmp, pend)
    genstate.fsync_dir(state)


def pending_complete(state: Path) -> bool:
    p = Path(state) / "pending-deploy"
    return (p / "meta.json").is_file() and (p / "ids.parquet").is_file()


def gc_retired(state: Path) -> None:
    """Delete retired/half-written pending dirs, file by file (resumable: a kill leaves a smaller garbage dir that is found again)."""
    state = Path(state)
    for d in [*state.glob("pending-deploy.retired-*"), *state.glob("pending-deploy.tmp")]:
        for f in sorted(x for x in d.rglob("*") if x.is_file()):
            genstate.hook("cleanup-unlink")
            f.unlink()
        genstate.hook("cleanup-rmdir")
        shutil.rmtree(d, ignore_errors=True)
    genstate.fsync_dir(state)


def retire_pending(state: Path, run: str = "x") -> None:
    """The ONLY way pending-deploy goes away: one atomic rename (the record is then 'not pending'), fsync the parent, then delete."""
    state = Path(state)
    pend = state / "pending-deploy"
    if pend.exists():
        genstate.hook("retire-rename")
        os.rename(pend, state / f"pending-deploy.retired-{run}-{int(time.time())}")
        genstate.fsync_dir(state)
    gc_retired(state)


def promote_accepted(state: Path, src: Path) -> None:
    """Advance the accepted extract baseline (the drift gate's 'previous') to the extract of the build that is now live."""
    def populate(tmp: Path) -> None:
        genstate.hook("accepted-copy")
        shutil.copyfile(src, tmp / DRIFT_FILE)

    genstate.publish(Path(state) / "extract" / "accepted", populate)


def accepted_file(state: Path) -> Path | None:
    g = genstate.resolve(Path(state) / "extract" / "accepted")
    return g / DRIFT_FILE if g and (g / DRIFT_FILE).is_file() else None


def promote(state: Path) -> dict:
    """pending-deploy -> deployed-ids.parquet + deployed.json + accepted extract. Idempotent: pending is retired last, so a kill anywhere re-runs from the top."""
    state = Path(state)
    pend = state / "pending-deploy"
    meta = json.loads((pend / "meta.json").read_text())
    tmp = state / "deployed-ids.parquet.tmp"
    shutil.copyfile(pend / "ids.parquet", tmp)
    genstate.fsync_file(tmp)
    genstate.hook("promote-ids")
    os.replace(tmp, state / "deployed-ids.parquet")
    genstate.fsync_dir(state)
    genstate.hook("promote-json")
    d = {**meta, "deployedAt": meta.get("deployedAt") or now_iso()}
    write_json(state / "deployed.json", d)
    if (pend / DRIFT_FILE).is_file():
        genstate.hook("promote-accepted")
        promote_accepted(state, pend / DRIFT_FILE)
    genstate.hook("promote-drop")
    retire_pending(state, str(meta.get("run", "x")))
    return d


def drop_pending(state: Path) -> None:
    retire_pending(Path(state), "dropped")


def reconcile(state: Path, live_version: int, live: dict | None = None, say=print) -> dict:
    """Make deployed.json describe the live site, or raise UnknownLive. Returns deployed.json."""
    state = Path(state)
    gc_retired(state)  # leftovers of a retire/cleanup that was killed
    dj = deployed(state)
    incomplete = (state / "pending-deploy").exists() and not pending_complete(state)
    pend = read_json(state / "pending-deploy" / "meta.json") if not incomplete else None
    if dj is None and not pend:
        if live_version == LAUNCH_VERSION and (state / "deployed-ids.parquet").is_file():
            snap = ((live or {}).get("snapshot") or "")[:10]
            dj = {"version": LAUNCH_VERSION, "commit": None, "commitTime": f"{snap}T00:00:00Z" if snap else None, "deployedAt": LAUNCH_DEPLOYED_AT,
                  "run": "launch-migration"}
            write_json(state / "deployed.json", dj)
            say(f"baseline: seeded deployed.json for the launch build {LAUNCH_VERSION}")
        else:
            raise UnknownLive(f"live site is at an unknown version {live_version}: no deployed.json (the launch build is {LAUNCH_VERSION})")
    if incomplete:
        if dj and live_version == dj["version"]:
            say("baseline: incomplete pending-deploy (it was written completely before wrangler, so no deploy happened); discarding")
            retire_pending(state, "incomplete")
            return dj
        raise UnknownLive(f"{state / 'pending-deploy'} is incomplete and live {live_version} is not deployed.json's {dj and dj['version']}")
    if pend and live_version == pend["version"]:
        say(f"baseline: live is at pending version {live_version}: the previous run deployed but did not finish; promoting its ids")
        return promote(state)
    if pend and dj and live_version == dj["version"]:
        say(f"baseline: pending deploy {pend['version']} never reached the live site (live {live_version}); dropping it")
        drop_pending(state)
        return dj
    if pend:
        raise UnknownLive(f"live site is at an unknown version {live_version} (deployed.json {dj and dj['version']}, pending {pend['version']})")
    if live_version != dj["version"]:
        raise UnknownLive(f"live site is at an unknown version {live_version} (deployed.json says {dj['version']}); deployed outside the job? "
                          "re-baseline attended: baseline.py adopt")
    return dj


def preflight(dj: dict | None, head: dict, now: datetime | None = None, say=print) -> None:
    """Before the window. Raises NoNewData (exit 0) when the pinned commit is the live one, DataStale when that has been the case too long."""
    if not dj or not dj.get("commit") or dj["commit"] != head["hash"]:
        return
    now = now or datetime.now(timezone.utc)
    ct = dj.get("commitTime")
    age = (now - parse_t(ct)).total_seconds() / 86400 if ct else None
    if age is not None and age > STALE_DAYS:
        raise DataStale(f"data stale: no new Dolt commit for {age:.0f} days (live commit {dj['commit'][:10]} from {ct})")
    raise NoNewData(f"no new data; live data age {'?' if age is None else f'{age:.1f}'} days (Dolt main {head['hash'][:10]} is the live commit)")


def weeks_since(dj: dict | None, now: datetime | None = None) -> int:
    """Whole weeks (rounded up, 1..4) since the live build was deployed."""
    if not dj or not dj.get("deployedAt"):
        return 1
    days = ((now or datetime.now(timezone.utc)) - parse_t(dj["deployedAt"])).total_seconds() / 86400
    return min(4, max(1, -(-int(days * 1000) // 7000)))  # ceil(days / 7) without float fuzz


def fetch_live(url: str, tries: int = 3) -> dict:
    last = None
    for k in range(tries):
        try:
            req = urllib.request.Request(f"{url.rstrip('/')}/data/manifest.json?cb={int(time.time())}", headers={"User-Agent": "vrcw-search-refresh/1"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(5 * (k + 1))
    raise UnknownLive(f"live manifest unreadable: {last}")


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["adopt"])
    ap.add_argument("--state", type=Path, required=True)
    ap.add_argument("--ids", type=Path, required=True, help="public-ids parquet of the build that is live")
    ap.add_argument("--live-url", required=True)
    ap.add_argument("--commit", default=None)
    a = ap.parse_args()
    lm = fetch_live(a.live_url)
    shutil.copyfile(a.ids, a.state / "deployed-ids.parquet.tmp")
    os.replace(a.state / "deployed-ids.parquet.tmp", a.state / "deployed-ids.parquet")
    d = {"version": lm["version"], "commit": a.commit or (lm.get("refresh") or {}).get("commit"), "commitTime": (lm.get("refresh") or {}).get("commitTime") or f"{lm.get('snapshot')}T00:00:00Z",
         "deployedAt": now_iso(), "run": "adopt"}
    write_json(a.state / "deployed.json", d)
    shutil.rmtree(a.state / "pending-deploy", ignore_errors=True)
    print("adopted", d)
    return 0


if __name__ == "__main__":
    sys.exit(main())
