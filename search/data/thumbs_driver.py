#!/usr/bin/env python3
"""Run on natto (needs ssh agent forwarding to chirashi, which can reach claude@cc.hl). Stdlib only.

For each resumable batch of unique thumb shas: chirashi `tar` (nice/ionice idle, read-only) -> ssh -> cc-0
encode_stream.py (throttled, encodes AVIF). Between batches it samples chirashi health (load, swap, IO pressure,
scraper unit state) into THUMBS_DIR/run/health.log and stops if the scraper unit is in an unexpected state.
Batch lists live on cc-0 (THUMBS_DIR/lists); nothing is written on chirashi.
"""
import subprocess
import sys
import time

CC = "cc"
D = "/work/projects/vrcwscrape-data/thumbs"
SRC = "/work/projects/vrcwscrape-search/search/data"
IMAGES = "/mnt/nvme/vrcw-scrape/images"
BATCH = 10_000
RATE = 20


def sh(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, text=True, capture_output=True, **kw)


def health(tag: str) -> str:
    r = sh(["ssh", "chirashi",
            "echo load $(cut -d' ' -f1-3 /proc/loadavg) $(grep -E 'SwapFree|MemAvailable' /proc/meminfo | tr -s ' ' | tr '\\n' ' ');"
            " grep -E '^(some|full)' /proc/pressure/io | tr '\\n' ' ';"
            " systemctl --user show vrcwscrape.service -p ActiveState -p Result | tr '\\n' ' '"], timeout=60)
    line = f"{time.strftime('%H:%M:%S')} {tag} {r.stdout.strip()} {r.stderr.strip()[:100]}"
    sh(["ssh", CC, f"mkdir -p {D}/run && echo '{line}' >> {D}/run/health.log"])
    print(line, flush=True)
    return r.stdout


def wait_inactive() -> None:
    """Block until the scraper is idle (inactive or failed; i.e. not active/activating) before starting a batch."""
    while True:
        r = sh(["ssh", "chirashi", "systemctl --user is-active vrcwscrape.service"], timeout=60)
        if r.stdout.strip() not in ("active", "activating"):
            return
        time.sleep(20)


def scraper_runs_ok() -> bool:
    """Stop rule: any recent run failing with a reason other than the known 'timeout' shutdown hang, or >=5 of the
    last 6 runs failing (baseline ~40%), means we stop and report."""
    r = sh(["ssh", "chirashi", "journalctl --user -u vrcwscrape.service --since -4h --no-pager -o cat | "
            "grep -E \"^(Finished vrcwscrape|vrcwscrape.service: Failed with result)\" | tail -6"], timeout=120)
    lines = r.stdout.strip().splitlines()
    bad = [x for x in lines if "Failed with result" in x]
    odd = [x for x in bad if "'timeout'" not in x]
    print(f"scraper last {len(lines)} runs: {len(bad)} failed, {len(odd)} non-timeout", flush=True)
    return not odd and not (len(lines) >= 6 and len(bad) >= 5)


def main() -> None:
    shas = sh(["ssh", CC, f"cat {D}/all_shas.txt"]).stdout.split()
    nb = (len(shas) + BATCH - 1) // BATCH
    only = {int(x) for x in sys.argv[1:]}  # optional explicit batch numbers
    for b in range(nb):
        if only and b not in only:
            continue
        part = f"{D}/parts/part-{b:04d}.parquet"
        if sh(["ssh", CC, f"test -f {part}"]).returncode == 0:
            continue
        chunk = shas[b * BATCH:(b + 1) * BATCH]
        wait_inactive()
        h = health(f"before batch {b}/{nb}")
        if not h or not scraper_runs_ok():
            print("scraper failure pattern changed, stopping", flush=True)
            sys.exit(3)
        lst = f"{D}/lists/batch-{b:04d}.txt"
        sh(["ssh", CC, f"mkdir -p {D}/lists {D}/parts && cat > {lst}"], input="".join(f"{s}\n" for s in chunk))
        t0 = time.time()
        paths = "".join(f"{s[:2]}/{s[2:4]}/{s}.png\n" for s in chunk)
        enc = (f"ssh claude@cc.hl 'cd {SRC} && nice -n10 ~/.local/bin/uv run python encode_stream.py --out {part} "
               f"--expect {lst} --rate {RATE}'")
        # tar runs in the background so its PID is recorded; the watchdog SIGSTOPs exactly that PID whenever the
        # scraper unit is not 'inactive' and SIGCONTs it afterwards. Pause/resume events go to stderr (-> health.log).
        remote = (
            f"{{ nice -n19 ionice -c3 tar -cf - -C {IMAGES} -T - <&0 2>/dev/null & TP=$!; "
            "( st=run; while kill -0 $TP 2>/dev/null; do "
            "a=$(systemctl --user is-active vrcwscrape.service); "
            "if [ \"$a\" = active ] || [ \"$a\" = activating ]; then if [ $st = run ]; then kill -STOP $TP; st=stop; echo \"PAUSE $(date +%T) unit=$a\" >&2; fi; "
            "else if [ $st = stop ]; then kill -CONT $TP; st=run; echo \"RESUME $(date +%T)\" >&2; fi; fi; sleep 2; done ) & WD=$!; "
            f"wait $TP; kill $WD 2>/dev/null; }} | {enc}"
        )
        r = sh(["ssh", "-A", "chirashi", remote], input=paths)
        ev = [l for l in r.stderr.splitlines() if l.startswith(("PAUSE", "RESUME"))]
        if ev:
            sh(["ssh", CC, f"echo 'batch {b}: {len(ev)//2} pauses: {' ; '.join(ev)}' >> {D}/run/health.log"])
        print(f"batch {b}: {time.time()-t0:.0f}s rc={r.returncode} {r.stdout.strip()} {r.stderr.strip()[-200:]}", flush=True)
        if r.returncode not in (0, 2):
            print("unexpected failure, stopping", flush=True)
            sys.exit(4)
    health("done")


if __name__ == "__main__":
    main()
