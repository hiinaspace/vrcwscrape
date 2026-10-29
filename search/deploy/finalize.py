#!/usr/bin/env python3
"""Publish-prep step on a built dist: stamp about.html with snapshot date and world count, install _headers.

usage: uv run --project search/build python search/deploy/finalize.py DIST PUBLIC_PARQUET [--asof ISO_TIME --commit HASH]
With --asof/--commit (the weekly refresh): the "data as of" date is the Dolt commit time, and the commit hash goes into about.html and
manifest.refresh. Without them: snapshot = newest last_scrape_time, as before.
Idempotent on a fresh build (vite copies web/public/about.html with the @@placeholders@@ each time). No network.
"""

import json
import shutil
import sys
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent


def main() -> int:
    args = sys.argv[1:]
    opts = {}
    for k in ("--asof", "--commit"):
        if k in args:
            i = args.index(k)
            opts[k] = args[i + 1]
            del args[i : i + 2]
    dist, parquet = Path(args[0]), Path(args[1])
    t = pq.read_table(parquet, columns=["last_scrape_time"])
    newest = pc.max(t["last_scrape_time"]).as_py()
    n = json.loads((dist / "data/manifest.json").read_text())["n"]
    about = dist / "about.html"
    man = json.loads((dist / "data/manifest.json").read_text())
    s = about.read_text(encoding="utf-8")
    # The thumbnail bullet is only true when the packs are in this build (manifest.hasThumb).
    a, b = s.find("<!--THUMBS-->"), s.find("<!--/THUMBS-->")
    if a >= 0 and b > a:
        inner = s[a + len("<!--THUMBS-->") : b]
        if man.get("hasThumb"):
            repl = inner
        else:
            repl = ("<li>This build has no thumbnails; the detail panel links to each world's image and official page on "
                    "vrchat.com.</li>")
        s = s[:a] + repl + s[b + len("<!--/THUMBS-->") :]
        about.write_text(s, encoding="utf-8")
        print("about.html: thumbnail bullet", "kept" if man.get("hasThumb") else "replaced (hasThumb=false)")
    # manifest.snapshot: the world-metadata snapshot date, shown in the top-bar byline ("Data as of ...")
    snap = opts["--asof"][:10] if "--asof" in opts else newest.strftime("%Y-%m-%d")
    if "--commit" in opts:
        man.setdefault("refresh", {}).update({"commit": opts["--commit"], "commitTime": opts.get("--asof")})
    # Weekly refresh: world data and upload history both come from the one pinned commit (history_extract anchors its asof to the commit
    # time), so the byline (history.asof) and About (snapshot) must show the same day. Disagreement = a bug upstream: fail the build.
    if "--asof" in opts and man.get("history") and (man["history"].get("asof") or "")[:10] != snap:
        sys.exit(f"finalize: manifest.history.asof {man['history'].get('asof')!r} != commit date {snap!r}; refusing to stamp inconsistent dates")
    if man.get("snapshot") != snap:
        man["snapshot"] = snap
        print(f"manifest.snapshot = {snap}")
    (dist / "data/manifest.json").write_text(json.dumps(man, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    hist_asof = ((man.get("history") or {}).get("asof") or snap)[:10]
    s = s.replace("@@HISTORY@@", hist_asof)
    commit_txt = f" (Dolt commit <code>{opts['--commit'][:10]}</code>)" if "--commit" in opts else ""
    s = s.replace("@@COMMIT@@", commit_txt)
    about.write_text(s, encoding="utf-8")
    if "@@SNAPSHOT@@" not in s and "@@COUNT@@" not in s:
        print("about.html already stamped")
    else:
        s = s.replace("@@SNAPSHOT@@", snap).replace("@@COUNT@@", f"{n:,}")
        about.write_text(s, encoding="utf-8")
        print(f"about.html: snapshot {snap}, {n:,} worlds")
    if (dist / "data/sem").is_dir():  # Apache-2.0 section 4: licence + notice travel with the redistributed model assets
        head = (HERE / "license_head.txt").read_text(encoding="utf-8")
        (dist / "data/sem/LICENSE.txt").write_text(head + (HERE / "LICENSE-Apache-2.0.txt").read_text(encoding="utf-8"), encoding="utf-8")
        print("data/sem/LICENSE.txt written")
    shutil.copyfile(HERE / "_headers", dist / "_headers")
    print("_headers installed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
