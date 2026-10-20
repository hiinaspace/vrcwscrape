// Opt-in semantic search control: switch + inline confirm (no native confirm()) + visible progress + visible enabled state.
//   off --click--> confirm --"Download 33 MB and enable"--> loading --> on --click--> off
// The consent is remembered (localStorage) so the next visit re-enables from the HTTP cache without asking again.
import { bytesFetched } from "../data";
import { lsGet, lsSet } from "./util";

export const SEM_MB = 33;
export type SemState = "off" | "confirm" | "loading" | "on" | "error";

export interface SemUi {
  state(): SemState;
  /** enable without asking (used when consent was remembered, and by tests) */
  enable(): Promise<void>;
  remembered(): boolean;
}

export function semUi(host: HTMLElement, panel: HTMLElement, opts: {
  load: (progress: (done: number, total: number) => void) => Promise<void>;
  unload: () => void;
  onState: (s: SemState) => void;
}): SemUi {
  let st: SemState = "off";
  host.innerHTML = `<button type="button" class="sw" data-testid="sem-toggle" role="switch" aria-checked="false" aria-controls="sem-panel" title="Find worlds by meaning (in-browser model, ~${SEM_MB} MB download)">
      <span class="knob"></span><span class="lbl">Semantic search</span></button><span class="sem-note" aria-live="polite"></span>`;
  panel.innerHTML = `<div class="sem-confirm" data-state="confirm" hidden>
      <p><b>Semantic search</b> finds worlds by meaning rather than by words (try <i>cozy night cabin</i>, or a Japanese phrase) and shows them as
      “Related by meaning” under the normal results. It runs entirely in your browser and needs a one-time download of about <b>${SEM_MB} MB</b>
      (cached afterwards). Nothing you type is sent anywhere.</p>
      <div class="sem-actions"><button type="button" class="primary" data-testid="sem-enable">Download ${SEM_MB} MB and enable</button><button type="button" class="sem-cancel">Not now</button></div></div>
    <div class="sem-loading" data-state="loading" hidden role="status"><div class="pbar"><i></i></div><span class="pnote">Downloading…</span></div>`;
  const sw = host.querySelector(".sw") as HTMLButtonElement;
  const note = host.querySelector(".sem-note") as HTMLElement;
  const conf = panel.querySelector(".sem-confirm") as HTMLElement;
  const load = panel.querySelector(".sem-loading") as HTMLElement;
  const fill = load.querySelector("i") as HTMLElement;
  const pnote = load.querySelector(".pnote") as HTMLElement;

  const set = (s: SemState, msg = "") => {
    st = s;
    host.dataset.state = s;
    sw.setAttribute("aria-checked", String(s === "on"));
    conf.hidden = s !== "confirm";
    load.hidden = s !== "loading";
    panel.hidden = s !== "confirm" && s !== "loading" && s !== "error";
    note.textContent = s === "on" ? "on" : msg;
    note.classList.toggle("err", s === "error");
    if (s === "error") { panel.hidden = false; }
    opts.onState(s);
  };

  async function enable() {
    if (st === "loading" || st === "on") return;
    const b0 = bytesFetched.total, t0 = performance.now();
    set("loading");
    fill.style.width = "2%";
    pnote.textContent = `Downloading… 0 / ${SEM_MB} MB`;
    const tick = setInterval(() => {
      const mb = (bytesFetched.total - b0) / 1048576;
      fill.style.width = Math.min(98, Math.max(2, (mb / SEM_MB) * 100)) + "%";
      pnote.textContent = `Downloading… ${mb.toFixed(1)} / ${SEM_MB} MB`;
    }, 100);
    try {
      await opts.load(() => {});
      clearInterval(tick);
      fill.style.width = "100%";
      lsSet("vrcw-sem", "1");
      set("on", `ready (${((performance.now() - t0) / 1000).toFixed(1)} s)`);
      setTimeout(() => { if (st === "on") note.textContent = "on"; }, 3000);
    } catch (err) {
      clearInterval(tick);
      set("error", "download failed: " + (err as Error).message);
      pnote.textContent = "";
    }
  }

  sw.addEventListener("click", () => {
    if (st === "on") { opts.unload(); lsSet("vrcw-sem", "0"); set("off"); }
    else if (st === "off" || st === "error") set("confirm");
    else if (st === "confirm") set("off");
  });
  panel.querySelector("[data-testid=sem-enable]")!.addEventListener("click", () => void enable());
  panel.querySelector(".sem-cancel")!.addEventListener("click", () => set("off"));
  set("off");
  return { state: () => st, enable, remembered: () => lsGet("vrcw-sem") === "1" };
}
