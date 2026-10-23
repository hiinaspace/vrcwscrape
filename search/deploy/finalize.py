#!/usr/bin/env python3
"""Publish-prep step on a built dist: stamp about.html with snapshot date and world count, install _headers.

usage: uv run --project search/build python search/deploy/finalize.py DIST PUBLIC_PARQUET
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
    dist, parquet = Path(sys.argv[1]), Path(sys.argv[2])
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
    snap = newest.strftime("%Y-%m-%d")
    if man.get("snapshot") != snap:
        man["snapshot"] = snap
        (dist / "data/manifest.json").write_text(json.dumps(man, separators=(",", ":")), encoding="utf-8")
        print(f"manifest.snapshot = {snap}")
    hist_asof = ((man.get("history") or {}).get("asof") or snap)[:10]
    s = s.replace("@@HISTORY@@", hist_asof)
    about.write_text(s, encoding="utf-8")
    if "@@SNAPSHOT@@" not in s and "@@COUNT@@" not in s:
        print("about.html already stamped")
    else:
        s = s.replace("@@SNAPSHOT@@", newest.strftime("%Y-%m-%d")).replace("@@COUNT@@", f"{n:,}")
        about.write_text(s, encoding="utf-8")
        print(f"about.html: snapshot {newest:%Y-%m-%d}, {n:,} worlds")
    if (dist / "data/sem").is_dir():  # Apache-2.0 section 4: licence + notice travel with the redistributed model assets
        head = (HERE / "license_head.txt").read_text(encoding="utf-8")
        (dist / "data/sem/LICENSE.txt").write_text(head + (HERE / "LICENSE-Apache-2.0.txt").read_text(encoding="utf-8"), encoding="utf-8")
        print("data/sem/LICENSE.txt written")
    shutil.copyfile(HERE / "_headers", dist / "_headers")
    print("_headers installed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
