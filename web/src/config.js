// ============================================================================
// Map tunables — tweak these and the dev server hot-reloads. Safe to iterate on.
// ============================================================================

// Zoom tier thresholds, as offsets from the initial fit-to-data zoom.
//   far  : z < midOffset            -> continent fills + labels + capitols
//   mid  : midOffset..nearOffset    -> sub-region fills + labels + capitols
//   near : z >= nearOffset          -> individual world cells, every visible one labeled
export const ZOOM = {
  midOffset: 2.5,
  nearOffset: 5,
};

// Per-world Voronoi cells.
export const CELLS = {
  // Max cell radius = radiusK * typical world spacing. SMALLER = tighter little
  // plots with more gaps (regions show through as background); larger = solid fill.
  radiusK: 0.3,
  shrink: 0.9, // 0–1, inset toward cell centroid (gap/margin between plots)
  fillAlpha: 90, // 0–255
  strokeAlpha: 150, // cell border alpha (only drawn when zoomed past mid)
  strokeMinZoomTier: "mid", // "far" | "mid" | "near": draw borders from this tier in
  // Only render per-world cells from this tier inward (region fills carry the wider
  // views). Keeps huge datasets fast — at "far"/"mid" tens of thousands of cells
  // would draw each frame. "near" = cells appear only once zoomed to individual
  // plots (gmaps-style). Set "mid" for the small 20k set if you want plots sooner.
  renderFromTier: "near", // "far" | "mid" | "near"
};

// "Ocean" (the viewport backdrop) + "land" (where worlds actually sit). The land is
// a precomputed alpha-shape polygon over the world coordinates, so it stays solid at
// any zoom without drawing every world as an overlapping scatter point.
export const OCEAN = "#28323f"; // viewport background (any CSS color)
export const LAND = {
  color: [226, 229, 231], // neutral light-grey landmass
};

// Region "background" fills (continent l3 + sub-region l2), drawn under the cells
// to give the space an underlying color field even where cells leave gaps.
export const REGION_BG = {
  l3Alpha: 60,
  l2Alpha: 50,
  outlineAlpha: 150,
  outlineWidth: 1.5,
};

// Muted, map-like palette (gmaps / OSM / Apple Maps feel). Regions are colored by
// index into this list, so reordering/recoloring here restyles the whole map.
export const REGION_PALETTE = [
  "#b8cda7", // sage green
  "#e7d8a8", // sand
  "#a9c6da", // sky blue
  "#cdbdda", // lilac
  "#e3bd9a", // clay
  "#a9d0c6", // teal
  "#e6bfc6", // rose
  "#d2d3a0", // olive
  "#c3c8de", // periwinkle
  "#ddd0a6", // wheat
  "#bcd6c9", // mint
  "#dcc3b0", // taupe
];

// Labels.
export const LABELS = {
  sizeL3: 12,
  sizeL2: 12,
  sizeCapitol: 10,
  sizeWorld: 10,
  color: [25, 25, 25],
  outlineWidth: 1, // deck SDF outline is thin; the background box does the contrast
  outlineColor: [255, 255, 255, 230],
  background: true, // white rounded rectangle behind label text for contrast
  backgroundColor: [255, 255, 255, 125],
  backgroundPadding: [4, 1, 4, 1], // [left, top, right, bottom] px
  // Extra px of clearance enforced around each label by the greedy declutter — higher
  // = more breathing room between labels (fewer shown), lower = tighter packing.
  labelGap: 3,
  // Debounce (ms) before recomputing the viewport-culled cell/label sets — a zoom or
  // pan gesture won't rebuild thousands of objects mid-gesture, only once it settles.
  settleMs: 130,
  // SDF font-atlas resolution (a CEILING — auto-reduced for datasets with many
  // unique glyphs so the atlas never overflows the GPU texture; see atlasFor in
  // Map.jsx). At 64 deck.gl captures tall-letter descenders ("J", j/g/y/p) that get
  // clipped at <=48; big CJK-heavy sets (218k) fall back to a smaller size to fit.
  // sdfBuffer is the atlas padding ceiling — kept small: a large buffer both wastes
  // atlas room (forcing a smaller fontSize) and triggers deck.gl's buffer-driven
  // glyph clipping (visgl/deck.gl#7211). radius is the SDF spread (>= outlineWidth).
  sdfFontSize: 64,
  sdfBuffer: 4,
  sdfRadius: 8,
};

export const CAPITOLS_PER_REGION = 2;

// "capitol" anchor points shown at far/mid zoom
export const CAPITOL_DOT = {
  radius: 4,
  minPixels: 3,
  maxPixels: 6,
};

export const SEARCH_PIN = {
  radius: 7,
  minPixels: 6,
  maxPixels: 11,
  fill: [222, 54, 54, 245],
  outline: [255, 255, 255, 245],
  outlineWidth: 2,
};

// Per-kind label color / offset / wrapping. (Sizes, bg box, collision in LABELS.)
export const LABEL_STYLE = {
  worldColor: [30, 30, 30],
  regionColor: [45, 145, 45],
  capitolColor: [40, 65, 120], // distinct hue so "major cities" read differently
  capitolPixelOffset: [0, -7], // lift the capitol label off its pin
  wordBreak: "break-word", // wrap long names at word boundaries…
  maxWidth: 9, // …at this width (multiples of font size)
  worldMaxChars: 42, // hard-truncate very long names (with …) before wrapping
};

// Smooth world-label LOD. The number of label-eligible worlds (top-N by visits)
// grows with zoom, and the collision filter culls overlaps within that set. This
// gives many gmaps-like density steps instead of 3 hard tiers:
//   budget = base * growth^floor((zoom - fitZoom) / zoomStep), clamped to `max`.
// So far zoom shows ~`base` globally-major worlds; each `zoomStep` deeper multiplies
// the candidate count by `growth`, up to `max` (where every world can label).
export const WORLD_LABELS = {
  base: 5, // worlds labeled at the initial fit-to-data zoom
  growth: 1.4, // candidate-count multiplier per LOD step
  zoomStep: 0.5, // zoom units per LOD step (smaller = more, finer steps)
  max: 300000, // cap — high enough that every world is eligible at deep zoom
  // Hard ceiling on labels actually fed to the TextLayers per frame (deck has no
  // spatial culling, so this bounds GPU/collision cost in dense viewports). Labels
  // are already viewport-culled; this caps the densest near-zoom views by visits.
  maxOnscreen: 2500,
};

// Hierarchy browsing (sidebar drill-down + clickable regions). The toponymy layers
// are a soft DAG, not a strict tree, so a sub-cluster's worlds can span several
// parents. For navigation we (a) attach each sub-cluster to its DOMINANT parent
// (where most of its worlds live) so it reads as a real spatial sub-area, and
// (b) hide tiny clusters that are really just tags, not regions.
export const BROWSE = {
  minClusterSize: 8, // hide sub-areas/regions with fewer worlds than this
};

// Selected world plot styling (instead of flat black).
export const SELECTED = {
  fillAlpha: 255, // fully opaque
  valueMul: 0.8, // multiply base RGB: <1 darker (pops on the light map), >1 brighter
  outline: [20, 20, 20, 120],
  outlineWidth: 3,
};

// Hover tooltip: ms the cursor must dwell before it appears (fewer flickers).
export const HOVER = { dwellMs: 350 };
