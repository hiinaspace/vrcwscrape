"""Container entrypoint: Healthchecks start/exit pings around `main --mode oneshot`.

Mirrors the chirashi systemd drop-in (ExecStartPre ping /start, ExecStopPost
ping /${EXIT_STATUS}). Pings are best effort and never change the exit code.
Extra argv is passed to `main` (default: --mode oneshot).
"""

import os
import signal
import subprocess
import sys
import urllib.request


def ping(base: str | None, suffix: str) -> None:
    if not base:
        return
    url = f"{base.rstrip('/')}/{suffix}"
    for _ in range(3):
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                resp.read()
            return
        except Exception as exc:  # noqa: BLE001 - best effort
            print(f"healthchecks ping failed ({suffix}): {type(exc).__name__}", file=sys.stderr)


def main() -> int:
    base = os.environ.get("HEALTHCHECKS_PING_URL") or None
    args = sys.argv[1:] or ["--mode", "oneshot"]
    # Logfire aborts at import time when it has neither a write token nor `logfire auth`
    # credentials. Chirashi uses a local credentials file; in a container use
    # LOGFIRE_TOKEN, and fall back to local-only logging when there is none.
    if not os.environ.get("LOGFIRE_TOKEN"):
        os.environ.pop("LOGFIRE_TOKEN", None)
        os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
    ping(base, "start")
    child = subprocess.Popen(["main", *args])

    def forward(signum, _frame):
        child.send_signal(signum)

    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)
    code = child.wait()
    if code < 0:  # killed by signal -> shell-style status
        code = 128 - code
    ping(base, str(code))
    return code


if __name__ == "__main__":
    sys.exit(main())
