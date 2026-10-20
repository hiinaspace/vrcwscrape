"""Mutation self-test: prove the round-5 checks fail on deliberately broken copies of the fake site.

Usage: uv run python selftest.py [--parquet fake_site/sample.parquet]
Each mutation patches a copy of fake_site/index.html, serves it on a free port (own subprocess,
terminated by PID), runs `run.py --r5-only --no-shots`, and expects the named rows to FAIL
(the unmutated copy must pass them). Prints a table; exit 1 if any expectation is violated.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).parent
FAKE = HERE / "fake_site"

# name -> (list of (old, new) replacements in index.html, substrings of row names that must FAIL)
MUTATIONS: dict[str, tuple[list[tuple[str, str]], list[str]]] = {
    "baseline": ([], []),
    "negation_ignored": (
        [('case "not": return !ev(n.c, w, ctx);', 'case "not": return true;')],
        ["boolean: top-50 precision"],
    ),
    "or_as_and": (
        [
            (
                'case "or": return n.c.some(c => ev(c, w, ctx));',
                'case "or": return n.c.every(c => ev(c, w, ctx));',
            )
        ],
        ["boolean: result total"],
    ),
    "left_assoc_or": (  # OR binds looser than AND (wrong precedence per DESIGN)
        [('c.push(alts.length > 1 ? { t: "or", c: alts } : u);', "c.push(u);")],
        ["boolean"],
    ),
    "tags_ex_ignored": (
        [("for (const t of f.tags_ex || []) if (w.tagset.includes(t)) return false;", "")],
        ["include/exclude facet counts"],
    ),
    "hist_wrong": (
        [
            (
                "cs[(+d.slice(0, 4) - 2010) * 12 + +d.slice(5, 7) - 1]++",
                "cs[(+d.slice(0, 4) - 2010) * 12 + +d.slice(5, 7)]++",
            )
        ],
        ["histogram facets"],
    ),
    "semantic_toggle_dead": (  # round-4 bug: click does nothing
        [('document.getElementById("sempop").hidden = false;', "")],
        ["semantic UI"],
    ),
    "semantic_no_items": (
        [
            (
                "if (semOn && s.q) for (const w of semHits(s.q))",
                "if (false) for (const w of semHits(s.q))",
            )
        ],
        ["query -> semantic-item"],
    ),
    "vrchat_hotlink": (
        [
            (
                'd.querySelector("img").src = url;',
                'd.querySelector("img").src = "https://files.vrchat.cloud/thumbs/" + w.id + ".png";',
            )
        ],
        ["no VRChat-host requests"],
    ),
    "broken_thumbs": (
        [('d.querySelector("img").src = url;', 'd.querySelector("img").src = url + "x";')],
        ["sampled thumbnails decode"],
    ),
    "adult_hidden": (
        [
            (
                "let hits = W.filter(w => matchF(w, filters)",
                "let hits = W.filter(w => !w.tagset.includes('content_adult') && !w.tagset.includes('content_sex') && matchF(w, filters)",
            )
        ],
        ["nothing hidden by default"],
    ),
    "trending_ignored": (
        [
            (
                'sort = sort || (q.trim() ? "relevance" : "trending");',
                'sort = sort === "trending" ? "published" : (sort || "relevance");',
            )
        ],
        ["trending default"],
    ),
}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run_one(name: str, patches, parquet: str, runs_dir: Path) -> list[dict]:
    tmp = Path(
        tempfile.mkdtemp(
            prefix="vrcw-selftest-",
            dir=HERE.parent.parent / "work" if (HERE.parent.parent / "work").exists() else None,
        )
    )
    try:
        html = (FAKE / "index.html").read_text()
        for old, new in patches:
            if old not in html:
                raise SystemExit(f"mutation {name}: pattern not found: {old[:60]}")
            html = html.replace(old, new)
        (tmp / "index.html").write_text(html)
        for f in ("data.json", "sem.bin"):
            (tmp / f).symlink_to((FAKE / f).resolve())
        port = free_port()
        srv = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "http.server",
                str(port),
                "--bind",
                "127.0.0.1",
                "--directory",
                str(tmp),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            time.sleep(1.0)
            r = subprocess.run(
                [
                    sys.executable,
                    str(HERE / "run.py"),
                    "--url",
                    f"http://127.0.0.1:{port}",
                    "--parquet",
                    parquet,
                    "--r5-only",
                    "--no-shots",
                    "--label",
                    f"FAKE mutation {name}",
                    "--runs-dir",
                    str(runs_dir),
                ],
                capture_output=True,
                env={**os.environ, "VRCW_SEM_TIMEOUT_S": "10"},
                text=True,
                cwd=HERE,
            )
            run_dir = [l for l in r.stdout.splitlines() if l.startswith("run dir:")][-1].split(
                ": ", 1
            )[1]
            return json.loads((Path(run_dir) / "metrics.json").read_text())["verdicts"]
        finally:
            srv.terminate()
            srv.wait(10)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default=str(FAKE / "sample.parquet"))
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--runs-dir", default=str(HERE / "runs" / "selftest"))
    a = ap.parse_args()
    bad = 0
    for name, (patches, must_fail) in MUTATIONS.items():
        if a.only and name not in a.only:
            continue
        rows = run_one(name, patches, a.parquet, Path(a.runs_dir))
        failed = [r["metric"] for r in rows if r["pass"] is False]
        if name == "baseline":
            ok = not failed
        else:
            ok = all(any(m in f for f in failed) for m in must_fail)
        bad += not ok
        print(
            f"{'OK ' if ok else 'BAD'} {name}: failing rows = {[f[:60] for f in failed][:5]}",
            flush=True,
        )
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
