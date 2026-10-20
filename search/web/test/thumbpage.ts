import { Engine } from "../src/engine";
(async () => {
  const e = await new Engine().init();
  (window as any).eng = e;
  (window as any).ready = true;
})();
