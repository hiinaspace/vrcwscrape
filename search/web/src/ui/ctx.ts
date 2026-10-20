import type { Engine } from "../engine";
import type { Meters } from "./meters";
import type { State } from "./state";

/** shared mutable context: modules read ctx.state at call time (it is replaced on popstate) */
export interface Ctx {
  eng: Engine;
  state: State;
  meters: Meters;
  /** state changed: update URL and re-run the query after `debounceMs` */
  change(debounceMs?: number): void;
}
