// Thumbnails: the engine serves self-hosted AVIF as object URLs (eng.thumbUrl). Never a VRChat CDN URL; placeholder when null.
import type { Engine } from "../engine";
import { hueOf } from "./util";

export const placeholder = (id: string) => `<div class="ph" style="--h:${hueOf(id)}"></div>`;

let obs: IntersectionObserver | null = null;
let engine: Engine;

export function initThumbs(eng: Engine) { engine = eng; }

async function fill(el: HTMLElement) {
  const i = +(el.dataset.i ?? "-1");
  if (i < 0) return;
  try {
    const url = await engine.thumbUrl(i);
    if (url && el.isConnected) {
      const img = new Image();
      img.alt = "";
      img.decoding = "async";
      img.onload = () => { if (el.isConnected) el.replaceChildren(img); };
      img.src = url;
    }
  } catch { /* placeholder stays */ }
}

/** lazily resolve `.tb[data-i]` thumbnail slots (rows also carry data-i) under root when they come into view */
export function lazyThumbs(root: ParentNode) {
  obs ??= new IntersectionObserver((es) => {
    for (const e of es) if (e.isIntersecting) { obs!.unobserve(e.target); void fill(e.target as HTMLElement); }
  }, { rootMargin: "200px" });
  root.querySelectorAll<HTMLElement>(".tb[data-i]").forEach((el) => obs!.observe(el));
}

export const thumbBox = (i: number, id: string) => `<span class="tb" data-i="${i}">${placeholder(id)}</span>`;
