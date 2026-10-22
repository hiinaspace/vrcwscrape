#!/usr/bin/env python3
"""Local stand-in for Cloudflare Pages: serves DIST with its _headers applied (cumulative rules, `! Header` detach),
HTTP Range (206) and no compression. For verifying the CSP / cache headers in headless Chromium before a deploy.

usage: python3 search/deploy/serve_headers.py DIST [PORT=5180]
Not a faithful Pages emulator (no clean-URL redirects, no _redirects); splat `*` matches any characters including '/'.
"""

import mimetypes
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

mimetypes.add_type("application/javascript", ".js")


def parse_headers(text: str):
    rules, cur = [], None
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line[0].isspace():
            if line.startswith("http"):  # host-qualified rule: not applicable on localhost
                cur = None
                continue
            pat = re.compile("^" + ".*".join(re.escape(x) for x in line.strip().split("*")) + "$")
            cur = (pat, [])
            rules.append(cur)
        elif cur is not None:
            s = line.strip()
            if s.startswith("! "):
                cur[1].append(("detach", s[2:].strip(), None))
            else:
                k, v = s.split(":", 1)
                cur[1].append(("set", k.strip(), v.strip()))
    return rules


class H(BaseHTTPRequestHandler):
    root: Path
    rules: list

    def log_message(self, *a):  # quiet
        pass

    def do_HEAD(self):
        self.do_GET(head=True)

    def do_GET(self, head=False):
        path = unquote(urlsplit(self.path).path)
        rel = path.lstrip("/") or "index.html"
        f = (self.root / rel).resolve()
        if f.is_dir():
            f = f / "index.html"
        if not str(f).startswith(str(self.root)) or not f.is_file():
            self.send_response(404)
            self.end_headers()
            return
        hdrs: dict[str, str] = {}
        for pat, items in self.rules:
            if pat.match(path):
                for kind, k, v in items:
                    kl = k.lower()
                    if kind == "detach":
                        hdrs.pop(kl, None)
                    else:
                        hdrs[kl] = f"{hdrs[kl]}, {v}" if kl in hdrs else v
        data = f.read_bytes()
        ctype = hdrs.pop("content-type", None) or mimetypes.guess_type(f.name)[0] or "application/octet-stream"
        status, lo, hi = 200, 0, len(data) - 1
        m = re.match(r"bytes=(\d*)-(\d*)$", self.headers.get("Range", ""))
        if m and (m.group(1) or m.group(2)):
            if m.group(1):
                lo = int(m.group(1))
                hi = int(m.group(2)) if m.group(2) else len(data) - 1
            else:
                lo = max(0, len(data) - int(m.group(2)))
            hi = min(hi, len(data) - 1)
            status = 206
        body = data[lo : hi + 1]
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        if status == 206:
            self.send_header("Content-Range", f"bytes {lo}-{hi}/{len(data)}")
        for k, v in hdrs.items():
            self.send_header(k, v)
        self.end_headers()
        if not head:
            self.wfile.write(body)


def main() -> None:
    root = Path(sys.argv[1]).resolve()
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5180
    H.root = root
    H.rules = parse_headers((root / "_headers").read_text()) if (root / "_headers").is_file() else []
    host = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"
    print(f"serving {root} with {len(H.rules)} _headers rules on http://{host}:{port}", flush=True)
    ThreadingHTTPServer((host, port), H).serve_forever()


if __name__ == "__main__":
    main()
