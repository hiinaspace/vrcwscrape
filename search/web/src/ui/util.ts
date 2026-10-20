// Small DOM/format helpers shared by the round-5 UI modules.
export const $ = <T extends HTMLElement>(s: string, root: ParentNode = document) => root.querySelector(s) as T;
export const esc = (s: string) => s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]!));
export const fmt = (n: number) => (n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : n >= 1e4 ? Math.round(n / 1e3) + "k" : n >= 1e3 ? (n / 1e3).toFixed(1) + "k" : String(n));
export const NOSIZE = 0xffffffff;
export const kb = (v: number) => (v === NOSIZE ? "–" : v >= 1024 ? (v / 1024).toFixed(v >= 10240 ? 0 : 1) + " MB" : v + " KB");
export const label = (k: string) => (k.startsWith("~") ? k.slice(1) : k);
export const lsGet = (k: string) => { try { return localStorage.getItem(k); } catch { return null; } };
export const lsSet = (k: string, v: string) => { try { localStorage.setItem(k, v); } catch { /* private mode / blocked */ } };
export const hueOf = (id: string) => (parseInt(id.slice(5, 9), 16) % 360) || 0;
export const csv = (v: string | null) => (v ?? "").split(",").map((s) => s.trim()).filter(Boolean);
export const num = (v: string | null) => (v === null || v === "" || isNaN(+v) ? undefined : +v);
