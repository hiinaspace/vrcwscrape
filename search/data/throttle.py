#!/usr/bin/env python3
"""stdin -> stdout copy at <= N MB/s average (back-pressures the upstream tar over ssh)."""
import sys, time

rate = float(sys.argv[1]) * 1e6
t0 = time.monotonic()
n = 0
inp, out = sys.stdin.buffer, sys.stdout.buffer
while True:
    b = inp.read1(1 << 16)
    if not b:
        break
    out.write(b)
    n += len(b)
    ahead = n / rate - (time.monotonic() - t0)
    if ahead > 0:
        time.sleep(ahead)
out.flush()
