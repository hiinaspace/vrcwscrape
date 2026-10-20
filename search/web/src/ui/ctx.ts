import type { Engine } from "../engine";
import type { Meters } from "./meters";
import type { State } from "./state";

/** shared mutable context: modules read ctx.state at call time (it is replaced on popstate) */
export interface Ctx {
  eng: Engine;
  state: State;
  meters: Meters;
  /** state changed: update URL and re-run the query after `debounceMs` */
  /** mode "push" (default) = new history entry; "type" = continuous input (typing, slider drag): one entry per burst, then replaceState */
  change(debounceMs?: number, mode?: "push" | "type"): void;
}
