#!/usr/bin/env python3
"""Weekly worldsearch refresh (k8s CronJob `search-refresh`, namespace vrcw). Stdlib only: heavy steps run through `uv run`.

Phases: window (stop the scraper, pin the Dolt read to a commit, extract) -> build (incremental embeddings + thumbnails, index) ->
gate -> deploy (or DRY_RUN). See deploy/README.md for the runbook. Every phase logs its wall time; a summary goes to stdout and
$STATE/runs/<run-id>.{log,json}.

Crash safety (a kill at any step must leave next week's run correct or loudly failing): the maintenance flag is opened first thing and held
until after the post-extract dolt restart; the thumbnail, embedding and history-base states are generation dirs whose only commit point is
one atomic CURRENT pointer replace (build/genstate.py); a deploy is bracketed by STATE/pending-deploy and reconciled against the live
manifest at the start of the next run (baseline.py). A failed Job therefore leaves the live site untouched ONLY until `wrangler pages deploy`
has run; after that "failed" can mean "deployed but not verified" (the message says so) and the next run reconciles it.

env: SEARCH_COMMIT (informational; the code is already checked out), STATE=/state, WORK=/work, IMAGES=/images,
     DATABASE_URL (read-only `search` user), CLOUDFLARE_API_TOKEN/CLOUDFLARE_ACCOUNT_ID (deploy only),
     DRY_RUN=1 (everything except the deploy; writes the checked dist to state), REUSE_EXTRACT=1 (skip the DB window, reuse $STATE/extract/current),
     NS=vrcw, PROJECT=worldsearch, LIVE_URL, THREADS, FLAG_TTL_MIN=20 (rolling), HISTORY_FULL=1 (rescan every upload row, ~25 min).
"""
from __future__ import annotations

import json
import os
import re
import shutil
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

STATE = Path(os.environ.get("STATE", "/state"))
WORK = Path(os.environ.get("WORK", "/work"))
IMAGES = Path(os.environ.get("IMAGES", "/images"))
FLAG = Path(os.environ.get("FLAG", str(IMAGES / ".vrcw-maintenance")))
NS = os.environ.get("NS", "vrcw")
PROJECT = os.environ.get("PROJECT", "worldsearch")
LIVE_URL = os.environ.get("LIVE_URL", "https://worldsearch.hiina.space").rstrip("/")
DRY = os.environ.get("DRY_RUN", "") not in ("", "0")
REUSE = os.environ.get("REUSE_EXTRACT", "") not in ("", "0")
THREADS = os.environ.get("THREADS", "10")
WRANGLER = "wrangler@4.151.3"
SEARCH = Path(__file__).resolve().parents[1]  # <checkout>/search
sys.path.insert(0, str(Path(__file__).resolve().parent))
import baseline  # noqa: E402  (stdlib only)
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
SECRETS = [v for k in ("DATABASE_URL", "CLOUDFLARE_API_TOKEN") if (v := os.environ.get(k))]
if os.environ.get("DATABASE_URL"):
    pw = urlparse(os.environ["DATABASE_URL"]).password
    if pw:
        SECRETS.append(pw)

(STATE / "runs").mkdir(parents=True, exist_ok=True)
LOG = open(STATE / "runs" / f"{RUN_ID}.log", "a", buffering=1)
T_START = time.time()
TIMINGS: list[tuple[str, float]] = []
_lock = threading.Lock()


def redact(s: str) -> str:
    for v in SECRETS:
        s = s.replace(v, "***")
    return s


def say(msg: str) -> None:
    line = redact(f"[{time.time() - T_START:7.1f}s] {msg}")
    with _lock:
        print(line, flush=True)
        LOG.write(line + "\n")


class step:
    def __init__(self, name: str):
        self.name = name

    def __enter__(self):
        self.t = time.time()
        say(f"== {self.name}")

    def __exit__(self, et, ev, tb):
        dt = time.time() - self.t
        TIMINGS.append((self.name, dt))
        say(f"== {self.name}: {'FAILED ' if et else ''}{dt:.0f}s")


def sh(cmd: list[str], env: dict | None = None, cwd: Path | None = None, check: bool = True, tag: str = "") -> int:
    e = dict(os.environ, **(env or {}))
    say(f"$ {' '.join(c if not c.startswith('mysql://') else 'mysql://***' for c in cmd)[:300]}")
    p = subprocess.Popen(cmd, env=e, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace")
    for line in p.stdout:  # type: ignore[union-attr]
        say(f"{tag}{line.rstrip()}")
    rc = p.wait()
    if check and rc:
        raise RuntimeError(f"command failed rc={rc}: {cmd[0]} {cmd[1] if len(cmd) > 1 else ''}")
    return rc


# ---- Python env helpers
UVENV = {
    "UV_CACHE_DIR": str(WORK / "cache/uv"),
    "UV_PYTHON_DOWNLOADS": "never",
    "UV_PYTHON": "python3.12",
    "HOME": str(STATE / "home"),
    "HF_HOME": str(WORK / "cache/hf"),  # local copy of $STATE/cache/hf: loading 2 GB of weights straight from NFS took ~5 min
    "npm_config_cache": str(WORK / "cache/npm"),
    "OMP_NUM_THREADS": THREADS,
    "PYTHONDONTWRITEBYTECODE": "1",
    "PYTHONUNBUFFERED": "1",
}
for k, v in UVENV.items():
    os.environ.setdefault(k, v)
(STATE / "home").mkdir(parents=True, exist_ok=True)
# venvs live in the emptyDir (NFS small-file writes are very slow); the uv wheel cache and the npm cache stay on the state volume and are
# only written when a lockfile changes. Copy (not hardlink) from the cache into the emptyDir.
os.environ.setdefault("UV_LINK_MODE", "copy")
VENV_REFRESH = WORK / "venvs/refresh"
VENV_BUILD = WORK / "venvs/build"


def py(args: list[str], venv: Path = VENV_REFRESH, project: str = "refresh", env: dict | None = None, **kw) -> int:
    e = {"UV_PROJECT_ENVIRONMENT": str(venv), **(env or {})}
    return sh(["uv", "run", "--frozen", "--project", str(SEARCH / project), "python", *args], env=e, **kw)


# ---- Kubernetes API (in-cluster, stdlib)
SA = Path("/var/run/secrets/kubernetes.io/serviceaccount")


def kube(method: str, path: str):
    host, port = os.environ["KUBERNETES_SERVICE_HOST"], os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    ctx = ssl.create_default_context(cafile=str(SA / "ca.crt"))
    req = urllib.request.Request(f"https://{host}:{port}{path}", method=method, headers={"Authorization": f"Bearer {(SA / 'token').read_text().strip()}"})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def pods(selector: str | None = None) -> list[dict]:
    q = f"?labelSelector={selector}" if selector else ""
    return (kube("GET", f"/api/v1/namespaces/{NS}/pods{q}") or {}).get("items", [])


def parse_t(s: str) -> float:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()


def pod_ready(p: dict | None) -> bool:
    return bool(p) and any(c["type"] == "Ready" and c["status"] == "True" for c in p.get("status", {}).get("conditions", []))


# ---- maintenance flag (read by the scraper entrypoint; unix time in the future = skip runs)
def flag_set(until: float) -> None:
    with open(FLAG, "w") as f:  # in place: the flag is a single-file bind mount, no rename/delete possible
        f.write(f"{int(until)}\n")


FLAG_TTL = int(os.environ.get("FLAG_TTL_MIN", "20")) * 60
_keeper_stop = threading.Event()
_flag_held = False


def flag_open() -> None:
    """Hold the flag for the whole window: a keeper thread pushes the expiry to now + FLAG_TTL every 60 s until flag_clear(). If the pod dies,
    the flag lapses by itself within FLAG_TTL, so a crash costs at most one more skipped scrape."""
    global _flag_held
    _keeper_stop.clear()
    flag_set(time.time() + FLAG_TTL)
    _flag_held = True
    say(f"maintenance flag held (rolling {FLAG_TTL // 60} min expiry)")

    def keep():
        while not _keeper_stop.wait(60):
            try:
                flag_set(time.time() + FLAG_TTL)
            except OSError as e:
                say(f"WARNING: flag refresh failed: {e}")

    threading.Thread(target=keep, daemon=True).start()


def flag_clear() -> None:
    global _flag_held
    if not _flag_held:
        return
    _flag_held = False
    _keeper_stop.set()
    try:
        flag_set(0)
        say("maintenance flag cleared")
    except OSError as e:
        say(f"WARNING: could not clear maintenance flag: {e}")


def restart_dolt(reason: str) -> None:
    old = kube("GET", f"/api/v1/namespaces/{NS}/pods/dolt-0")
    say(f"deleting dolt-0 ({reason})")
    kube("DELETE", f"/api/v1/namespaces/{NS}/pods/dolt-0")
    deadline = time.time() + 600
    while time.time() < deadline:
        time.sleep(3)
        p = kube("GET", f"/api/v1/namespaces/{NS}/pods/dolt-0")
        if p and (not old or p["metadata"]["uid"] != old["metadata"]["uid"]) and pod_ready(p):
            say("dolt-0 Ready")
            time.sleep(3)
            return
    raise RuntimeError("dolt-0 not Ready within 10 min after delete")


# ---- dependency caches: NFS handles big sequential files well (~95 MB/s) but small files at ~8/s, so the uv wheel cache and the npm cache
# live in the emptyDir and are persisted on the state volume as ONE tarball per lockfile hash. Cold: download from PyPI/npm, then tar once.
import hashlib  # noqa: E402

CACHES: dict[str, tuple[str, Path, bool]] = {}  # name -> (key, dir, restored)


def _key(*parts: bytes) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p)
    return h.hexdigest()[:12]


def restore_cache(name: str, key: str, d: Path) -> None:
    tar = STATE / f"cache/{name}-{key}.tar"
    d.mkdir(parents=True, exist_ok=True)
    ok = tar.is_file()
    if ok:
        sh(["tar", "-xf", str(tar), "-C", str(d.parent)], tag=f"[cache {name}] ")
    CACHES[name] = (key, d, ok)
    say(f"cache {name} {key}: {'restored from state' if ok else 'cold (will be downloaded and saved)'}")


def save_cache(name: str) -> None:
    key, d, restored = CACHES[name]
    if restored or not d.is_dir():
        return
    tmp = STATE / f"cache/{name}-{key}.tar.part"
    sh(["tar", "-cf", str(tmp), "-C", str(d.parent), d.name], tag=f"[cache {name}] ")
    tmp.rename(STATE / f"cache/{name}-{key}.tar")
    for old in (STATE / "cache").glob(f"{name}-*.tar"):
        if old.name != f"{name}-{key}.tar":
            old.unlink()
    CACHES[name] = (key, d, True)
    say(f"cache {name} saved ({(STATE / f'cache/{name}-{key}.tar').stat().st_size / 1e6:.0f} MB)")


def restore_hf() -> None:
    src, dst = STATE / "cache/hf", WORK / "cache/hf"
    dst.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        with step("hf cache: copy model weights from state to local disk"):
            sh(["cp", "-a", f"{src}/.", str(dst)], tag="[hf] ")


def save_hf() -> None:
    """models downloaded by this run (cold state) go back to the state volume; existing files are not touched"""
    src, dst = WORK / "cache/hf", STATE / "cache/hf"
    dst.mkdir(parents=True, exist_ok=True)
    sh(["cp", "-an", f"{src}/.", str(dst)], check=False, tag="[hf] ")


# ---- phases
def prep_deps() -> None:
    with step("deps: caches + uv sync (refresh, build) + npm ci"):
        restore_cache("uv", _key((SEARCH / "refresh/uv.lock").read_bytes(), (SEARCH / "build/uv.lock").read_bytes()), WORK / "cache/uv")
        restore_cache("npm", _key((SEARCH / "web/package-lock.json").read_bytes(), WRANGLER.encode()), WORK / "cache/npm")
        for proj, venv in (("refresh", VENV_REFRESH), ("build", VENV_BUILD)):
            sh(["uv", "sync", "--frozen", "--project", str(SEARCH / proj)], env={"UV_PROJECT_ENVIRONMENT": str(venv)}, tag=f"[sync {proj}] ")
        sh(["npm", "ci", "--no-audit", "--no-fund", "--prefer-offline"], cwd=SEARCH / "web", tag="[npm] ")
        save_cache("uv")


def pin_head() -> dict:
    r = subprocess.run(["uv", "run", "--frozen", "--project", str(SEARCH / "refresh"), "python", str(SEARCH / "refresh/dbhead.py")],
                       env={**os.environ, "UV_PROJECT_ENVIRONMENT": str(VENV_REFRESH)}, capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError("dbhead failed: " + redact(r.stderr[-500:]))
    return json.loads(r.stdout.strip().splitlines()[-1])


def extract_window(ex: Path) -> dict:
    """The maintenance flag is already held (main). Wait for the scrape, restart dolt, read a pinned commit, extract. Returns {hash, time}.
    Raises baseline.NoNewData (nothing was extracted) when the pinned commit is the one already live."""
    new = STATE / "extract/new"
    shutil.rmtree(new, ignore_errors=True)
    new.mkdir(parents=True)
    bg = None
    early: list = []  # NoNewData / DataStale found by the cheap head check while the scrape is still running
    try:
        # install deps while we wait for the running scrape to end (cluster-side idle wait is the long pole), then look at Dolt's head:
        # a head that is already live ends the run before any restart or ETL
        err: list = []

        def bgdeps():
            try:
                prep_deps()
            except Exception as e:  # noqa: BLE001
                err.append(e)
                return
            try:
                h = pin_head()
                say(f"early check: dolt main = {h['hash']} committed {h['time']}")
                baseline.preflight(baseline.deployed(STATE), h, say=say)
            except (baseline.NoNewData, baseline.DataStale) as e:
                early.append(e)
            except Exception as e:  # noqa: BLE001
                say(f"WARNING: early head check failed: {e} (the pin after the restart decides)")

        bg = threading.Thread(target=bgdeps)
        bg.start()
        with step("window: wait for scrape pods to finish"):
            deadline = time.time() + 30 * 60  # a wave-week scrape runs 20-25 min and may have started before the flag was opened
            while not early:
                busy = [p["metadata"]["name"] for p in pods("app=vrcw-scrape") if p["status"].get("phase") in ("Running", "Pending")]
                if not busy:
                    break
                if time.time() > deadline:
                    raise RuntimeError(f"scrape still running after 30 min: {busy}")
                say(f"scrape running: {busy}")
                time.sleep(15)
        bg.join()  # deps + the cheap head check come first: an unchanged head must not cost a dolt restart
        if err:
            raise err[0]
        if early:
            raise early[0]
        with step("window: fresh dolt-0"):
            d = kube("GET", f"/api/v1/namespaces/{NS}/pods/dolt-0")
            done = [parse_t(c["state"]["terminated"]["finishedAt"]) for p in pods("app=vrcw-scrape") for c in p["status"].get("containerStatuses", []) if c.get("state", {}).get("terminated")]
            started = parse_t(d["status"]["startTime"]) if d and d["status"].get("startTime") else 0
            if d and pod_ready(d) and done and started > max(done):
                say("dolt-0 started after the last scrape ended: heap is fresh, no restart needed")
            else:
                restart_dolt("fresh heap for the extract")
        with step("window: pin commit"):
            head = pin_head()
            say(f"dolt main = {head['hash']} committed {head['time']}")
            baseline.preflight(baseline.deployed(STATE), head, say=say)
        u = urlparse(os.environ["DATABASE_URL"])
        url = urlunparse(u._replace(path="/vrcwscrape/" + head["hash"]))
        D = SEARCH / "data"
        with step("extract: ETL (full + public parquet)"):
            py([str(D / "etl_search.py"), "--database-url", url, "--output", str(new / "worlds-full.parquet")], tag="[etl] ")
        with step("extract: recent popularity"):
            py([str(D / "recent_popularity.py"), "--database-url", url, "--input", str(new / "worlds-full-public.parquet"),
                "--output", str(new / "public-v2.parquet")], tag="[recent] ")
        with step("extract: upload/metrics history"):
            py([str(D / "history_extract.py"), "--database-url", url, "--base", str(STATE / "history-base"), str(new / "public-v2.parquet"), str(new / "history")], tag="[hist] ")
        (new / "extract.json").write_text(json.dumps({**head, "extractedAt": datetime.now(timezone.utc).isoformat(timespec="seconds")}))
        return head
    finally:
        if bg and bg.is_alive():
            bg.join()
        try:
            if early:
                shutil.rmtree(new, ignore_errors=True)  # nothing was read from Dolt: no heap to free
            else:
                with step("window: restart dolt-0 (free the extract heap)"):
                    restart_dolt("release memory after the extract")
        except Exception as e:  # noqa: BLE001
            say(f"WARNING: post-window dolt restart failed: {e} (dolt-restart cron will do it)")
        flag_clear()  # only now: a scraper admitted earlier could have its database deleted by the restart


def live_manifest() -> dict | None:
    try:
        req = urllib.request.Request(f"{LIVE_URL}/data/manifest.json?cb={int(time.time())}", headers={"User-Agent": "vrcw-search-refresh/1"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    except Exception as e:  # noqa: BLE001
        say(f"WARNING: live manifest unreadable: {e}")
        return None


def main() -> int:
    say(f"run {RUN_ID} DRY_RUN={int(DRY)} REUSE_EXTRACT={int(REUSE)} code={os.environ.get('SEARCH_COMMIT', '?')[:10]} host={os.uname().nodename}")
    if not REUSE:
        flag_open()  # before anything slow: the 03:30 scrape must see it even if setup stalls (cleared in extract_window, and in finally)
    ok = False
    noop = False
    try:
        for d in ("cache/hf", "extract", "dists", "emb", "thumbs"):
            (STATE / d).mkdir(parents=True, exist_ok=True)
        ex = STATE / "extract/current"
        hf_thread = threading.Thread(target=restore_hf, daemon=True)  # a no-op run must not wait for the 2 GB copy at exit
        hf_thread.start()
        with step("baseline: reconcile with the live site"):
            lm = baseline.fetch_live(LIVE_URL)
            dj = baseline.reconcile(STATE, lm["version"], lm, say)
            say(f"live {lm['version']} = deployed.json (commit {str(dj.get('commit'))[:10]}, deployed {dj.get('deployedAt')})")
        if REUSE:
            prep_deps()
            head = json.loads((ex / "extract.json").read_text())
        else:
            head = extract_window(ex)
            if ex.exists():
                shutil.rmtree(STATE / "extract/previous", ignore_errors=True)
                ex.rename(STATE / "extract/previous")
            (STATE / "extract/new").rename(ex)
        say(f"data: dolt commit {head['hash']} at {head['time']}")
        build = WORK / "build"
        shutil.rmtree(build, ignore_errors=True)
        build.mkdir(parents=True)
        pub = ex / "public-v2.parquet"
        full = ex / "worlds-full.parquet"
        excl = SEARCH / "deploy/excluded_ids.txt"

        with step("filter: removal list"):
            filtered = build / "filtered.public.parquet"
            r = subprocess.run(["uv", "run", "--frozen", "--project", str(SEARCH / "build"), "python", str(SEARCH / "deploy/filter_parquet.py"), str(pub), str(excl), str(filtered)],
                               env={**os.environ, "UV_PROJECT_ENVIRONMENT": str(VENV_BUILD)}, capture_output=True, text=True)
            if r.returncode:
                raise RuntimeError("filter_parquet failed: " + r.stderr[-400:])
            removed = int(r.stdout.strip().splitlines()[-1])
            bpq = filtered if removed else pub
            say(f"removal list drops {removed} worlds")

        hf_thread.join()
        with step("embeddings: delta (CPU fp32) + static bits"):
            py([str(SEARCH / "refresh/embed_delta.py"), "--parquet", str(bpq), "--state", str(STATE / "emb"), "--out", str(build / "emb")], tag="[emb] ")
            save_hf()
        with step("thumbnails: encode new shas"):
            py([str(SEARCH / "refresh/thumbs_delta.py"), "--parquet", str(bpq), "--state", str(STATE / "thumbs"), "--images", str(IMAGES),
                "--out", str(build / "thumbs_new.parquet"), "--workers", THREADS], tag="[thumbs] ")
        with step("build: index + neighbours + semantic + packs + web"):
            dist = build / "dist"
            sh(["bash", str(SEARCH / "build.sh"), str(bpq), str(build / "emb/gemma2_full_f16.npy"), str(build / "emb/ids.parquet")],
               env={"DIST": str(dist), "WORK": str(build / "work"), "BUILD_VENV": str(VENV_BUILD), "THUMBS": "/nonexistent",
                    "THUMB_STATE": str(STATE / "thumbs"), "THUMB_NEW": str(build / "thumbs_new.parquet"),
                    "STATIC_DIR": str(build / "emb/static"), "PUBLISH_PQ": str(bpq), "HISTORY": str(ex / "history"),
                    "FINALIZE_ARGS": f"--asof {head['time']} --commit {head['hash']}"}, tag="[build] ")
        with step("gate"):
            rc = py([str(SEARCH / "refresh/gate.py"), "--dist", str(dist), "--public", str(bpq), "--full", str(full), "--excluded", str(excl),
                     "--deployed-ids", str(STATE / "deployed-ids.parquet"), "--thumb-state", str(STATE / "thumbs"), "--live-url", LIVE_URL,
                     "--search", str(SEARCH), "--deployed-json", str(STATE / "deployed.json"), "--extract-dir", str(STATE / "extract"),
                     "--report", str(STATE / "runs" / f"{RUN_ID}.gate.json")],
                    venv=VENV_BUILD, project="build", check=False, tag="[gate] ")
            if rc:
                raise RuntimeError("GATE FAILED: no deploy")
        man = json.loads((dist / "data/manifest.json").read_text())
        ddir = STATE / "dists" / f"{RUN_ID}.tar"
        with step("save checked dist to state (one tarball: NFS is slow for small files)"):
            (STATE / "dists").mkdir(exist_ok=True)
            sh(["tar", "-cf", str(ddir) + ".part", "-C", str(dist), "."])
            Path(str(ddir) + ".part").rename(ddir)
            for old in sorted(STATE.glob("dists/*.tar"))[:-2]:
                old.unlink()
            say(f"kept dists: {sorted(p.name for p in (STATE / 'dists').glob('*.tar'))}")
        if DRY:
            with step("deploy: skipped (DRY_RUN); wrangler smoke"):
                sh(["npx", "--yes", WRANGLER, "--version"], check=False, tag="[wrangler] ")
                save_cache("npm")
            say(f"DRY_RUN done: checked dist tarball at {ddir} (manifest version {man['version']})")
        else:
            pending = {"version": man["version"], "commit": head["hash"], "commitTime": head["time"], "run": RUN_ID, "deployedAt": baseline.now_iso()}
            with step("deploy: record pending baseline (before wrangler)"):
                baseline.write_pending(STATE, bpq, pending)  # public ids + meta, fsynced; reconciled against the live manifest at the next start
            with step("deploy: wrangler pages deploy"):
                sh(["npx", "--yes", WRANGLER, "pages", "deploy", str(dist), "--project-name", PROJECT, "--branch", "main",
                    "--commit-dirty=true", "--commit-message", f"refresh dolt {head['hash'][:10]}"], tag="[wrangler] ")
                try:
                    save_cache("npm")
                except Exception as e:  # noqa: BLE001
                    say(f"WARNING: npm cache save failed after the deploy: {e}")
            with step("deploy: verify live manifest"):
                for _ in range(60):
                    lm = live_manifest()
                    if lm and lm.get("version") == man["version"]:
                        break
                    time.sleep(10)
                else:
                    raise RuntimeError(f"DEPLOYED BUT NOT VERIFIED: wrangler succeeded but the live manifest is not at {man['version']} after 10 min; "
                                       "pending-deploy is kept and the next run reconciles it against the live site")
                baseline.promote(STATE)
                say(f"baseline promoted: deployed.json = version {man['version']}, commit {head['hash'][:10]}")
        ok = True
        return 0
    except baseline.NoNewData as e:
        say(str(e))
        ok = noop = True
        return 0
    except Exception as e:  # noqa: BLE001
        say(f"FAILED: {e}")
        return 1
    finally:
        flag_clear()  # no-op after extract_window; covers a failure before it
        total = time.time() - T_START
        say("---- timings ----")
        for n, s in TIMINGS:
            say(f"{s:8.0f}s  {n}")
        say(f"{total:8.0f}s  TOTAL ({'ok' if ok else 'FAILED'})")
        (STATE / "runs" / f"{RUN_ID}.json").write_text(json.dumps({"ok": ok, "dry": DRY, "noop": noop, "total": total, "steps": TIMINGS}))
        shutil.rmtree(WORK / "build", ignore_errors=True) if ok else None


if __name__ == "__main__":
    sys.exit(main())
