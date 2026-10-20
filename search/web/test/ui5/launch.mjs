// Launch helpers for UI round-5 checks. Chromium libs are user-space copies (no root on cc-0, see oracle/run.py);
// Firefox lives under /work/.tools (home is full) and needs the GTK3 closure extracted at /work/.tools/ffdeps/root.
import { chromium, firefox } from "playwright";
import os from "os";
const CH = os.homedir() + "/.cache/chromium-libs/usr/lib/x86_64-linux-gnu";
const FFDIR = "/work/.tools/pw-browsers/firefox-1555/firefox";
const FFLIB = FFDIR + ":/work/.tools/ffdeps/root/usr/lib/x86_64-linux-gnu";
export async function launch(kind = "chromium") {
  if (kind === "firefox")
    return firefox.launch({ executablePath: FFDIR + "/firefox", env: { ...process.env, LD_LIBRARY_PATH: FFLIB } });
  return chromium.launch({ env: { ...process.env, LD_LIBRARY_PATH: CH + ":" + (process.env.LD_LIBRARY_PATH ?? "") } });
}
