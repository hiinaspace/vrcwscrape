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
    s = about.read_text(encoding="utf-8")
    if "@@SNAPSHOT@@" not in s and "@@COUNT@@" not in s:
        print("about.html already stamped")
    else:
        s = s.replace("@@SNAPSHOT@@", newest.strftime("%Y-%m-%d")).replace("@@COUNT@@", f"{n:,}")
        about.write_text(s, encoding="utf-8")
        print(f"about.html: snapshot {newest:%Y-%m-%d}, {n:,} worlds")
    shutil.copyfile(HERE / "_headers", dist / "_headers")
    print("_headers installed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
