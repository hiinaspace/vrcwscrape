#!/usr/bin/env python3
"""Read a tar stream of full-size PNGs (<aa>/<bb>/<sha256>.png) on stdin, write a parquet part of AVIF thumbnails.

Run on cc-0 (never on the live scraper host). The reader throttles the stream to --rate MB/s average, which
back-pressures the upstream `tar` over ssh. Output: <out> (sha256, avif, width, height), written to .part then
renamed; <out>.missing.txt lists expected shas that never arrived (no file in store); <out>.errors.txt decode failures.

  tar -cf - -T list | ssh cc-0 'encode_stream.py --out parts/part-0000.parquet --expect lists/batch-0000.txt'
"""
import argparse
import io
import sys
import tarfile
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

WIDTH = 256
QUALITY = 50
SPEED = 4


def encode(png: bytes) -> tuple[bytes, int, int]:
    im = Image.open(io.BytesIO(png))
    im.draft("RGB", (WIDTH * 2, WIDTH * 2))  # no-op for PNG, harmless
    im = im.convert("RGB")
    h = max(1, round(WIDTH * im.height / im.width))
    im = im.resize((WIDTH, h), Image.LANCZOS)
    out = io.BytesIO()
    im.save(out, "AVIF", quality=QUALITY, speed=SPEED)
    return out.getvalue(), WIDTH, h


def encode_named(args):
    sha, png = args
    try:
        return sha, encode(png), None
    except Exception as e:  # noqa: BLE001
        return sha, None, repr(e)


class Throttled:
    """File-like wrapper limiting average read rate."""

    def __init__(self, f, mb_per_s: float):
        self.f, self.rate, self.n, self.t0 = f, mb_per_s * 1e6, 0, time.monotonic()

    def read(self, size=-1):
        b = self.f.read(min(size, 1 << 16) if size and size > 0 else 1 << 16)
        self.n += len(b)
        ahead = self.n / self.rate - (time.monotonic() - self.t0)
        if ahead > 0:
            time.sleep(ahead)
        return b


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--expect", type=Path, help="file of expected sha256 (one per line) to compute missing")
    ap.add_argument("--rate", type=float, default=20.0, help="MB/s average read cap")
    ap.add_argument("--workers", type=int, default=5)
    a = ap.parse_args()
    t0 = time.monotonic()
    src = Throttled(sys.stdin.buffer, a.rate)
    rows: dict[str, tuple[bytes, int, int]] = {}
    errors: list[str] = []
    nbytes = 0
    pending: deque = deque()

    def drain(limit: int) -> None:
        while len(pending) > limit:
            sha, res, err = pending.popleft().result()
            if err:
                errors.append(f"{sha}\t{err}")
            else:
                rows[sha] = res

    with ProcessPoolExecutor(a.workers) as pool, tarfile.open(fileobj=src, mode="r|") as tf:
        for m in tf:
            if not m.isfile():
                continue
            data = tf.extractfile(m).read()  # type: ignore[union-attr]
            nbytes += len(data)
            sha = Path(m.name).stem
            pending.append(pool.submit(encode_named, (sha, data)))
            drain(a.workers * 3)
        drain(0)
    shas = sorted(rows)
    tbl = pa.table(
        {
            "sha256": shas,
            "avif": [rows[s][0] for s in shas],
            "width": pa.array([rows[s][1] for s in shas], pa.int16()),
            "height": pa.array([rows[s][2] for s in shas], pa.int16()),
        }
    )
    a.out.parent.mkdir(parents=True, exist_ok=True)
    part = a.out.with_name(a.out.name + ".part")
    pq.write_table(tbl, part, compression="none")  # avif is already compressed
    if a.expect:
        want = {x.strip() for x in a.expect.read_text().splitlines() if x.strip()}
        a.out.with_name(a.out.name + ".missing.txt").write_text("".join(f"{s}\n" for s in sorted(want - set(rows) - {e.split(chr(9))[0] for e in errors})))
    if errors:
        a.out.with_name(a.out.name + ".errors.txt").write_text("\n".join(errors) + "\n")
    part.rename(a.out)
    dt = time.monotonic() - t0
    tot = sum(len(v[0]) for v in rows.values())
    print(f"{a.out.name}: {len(rows)} encoded, {len(errors)} errors, in {nbytes/1e6:.0f} MB, out {tot/1e3:.0f} KB, "
          f"{dt:.0f}s, {nbytes/1e6/dt:.1f} MB/s", flush=True)


if __name__ == "__main__":
    main()
