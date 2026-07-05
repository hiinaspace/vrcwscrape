import { useEffect, useMemo, useRef, useState } from "react";
import DeckGL from "@deck.gl/react";
import { OrbitView, OrthographicView } from "@deck.gl/core";
import { GeoJsonLayer, PolygonLayer, ScatterplotLayer, TextLayer } from "@deck.gl/layers";
import { Delaunay } from "d3-delaunay";
import { DATA_DIR, EXTRUDE_MODE, getLevels, getManifest, loadPoints } from "./duckdb.js";
import { hexToRgb, labelPoint } from "./util.js";
import {
  BLOCKS,
  BUILDINGS,
  CAPITOL_DOT,
  CAPITOLS_PER_REGION,
  CELLS,
  EXTRUDE,
  LABEL_STYLE,
  LABELS,
  LAND,
  LANDUSE,
  OCEAN,
  PARCELS,
  REGION_BG,
  REGION_PALETTE,
  ROADS,
  SEARCH_PIN,
  SELECTED,
  WORLD_LABELS,
  ZOOM,
} from "./config.js";

// CollisionFilterExtension priorities must live in ~[-1000,1000]; carve brackets so
// level dominates: continents 900+ > capitols 700 > sub-regions 500+ > worlds 0-400.
const priL3 = (size) => 900 + Math.min(99, Math.round(size / 70));
const priCapitol = (visits) => 700 + Math.min(99, Math.round(Math.log10(visits + 1) * 16));
const priL2 = (size) => 500 + Math.min(99, Math.round(size / 4));
const priWorld = (visits) => Math.min(400, Math.round(Math.log10(visits + 1) * 60));

const TIER_RANK = { far: 0, mid: 1, near: 2 };
const FOCUS_ANIMATION_MS = 650;
const easeOutCubic = (t) => 1 - (1 - t) ** 3;

// A label is "wide" if it contains any non-Latin code unit (CJK/kana/hangul/symbols,
// or a surrogate half from an emoji). Wide labels use the big atlas; the rest use a
// tiny Latin-only atlas that can stay at full fontSize without overflowing.
// The char class below is the Latin range U+0020..U+04FF (ASCII, Latin-ext, Greek,
// Cyrillic); anything outside it is treated as wide.
const NON_LATIN = /[^\u0000-\u04ff]/;

// deck.gl packs glyphs into a fixed 1024px-wide SDF atlas that grows in height; too
// many unique glyphs (CJK-heavy 218k names ~6k glyphs) at a large fontSize overflow
// the GPU max texture, the atlas silently fails, and NO labels render. Shrink the
// atlas fontSize as the unique-glyph count grows so it stays well under ~16k px tall.
// Small sets (e.g. the 20k, ~2.2k glyphs) keep the crisp configured size.
function atlasFor(nChars, maxFs, maxBuf, maxRadius) {
  // A small buffer keeps atlas cells compact (so a larger fontSize fits the same
  // texture budget) and avoids deck.gl's buffer-driven vertical glyph clipping
  // (visgl/deck.gl#7211). Larger fontSize = less SDF erosion of thin strokes /
  // descenders, which is the remaining "J"/j/g/y/p softness on big CJK sets.
  let fontSize = maxFs;
  if (nChars > 5000) fontSize = 40;
  else if (nChars > 3500) fontSize = 48;
  else if (nChars > 2200) fontSize = 56;
  fontSize = Math.min(maxFs, fontSize);
  const buffer = Math.min(maxBuf, Math.max(2, Math.round(fontSize / 12)));
  const radius = Math.min(maxRadius, Math.max(3, Math.round(fontSize / 8)));
  return { fontSize, buffer, radius };
}

function fitBounds([minX, minY, maxX, maxY], size, pad = 0.85) {
  const dx = Math.max(maxX - minX, 1e-6);
  const dy = Math.max(maxY - minY, 1e-6);
  return {
    target: [(minX + maxX) / 2, (minY + maxY) / 2, 0],
    zoom: Math.log2(Math.min((size.width * pad) / dx, (size.height * pad) / dy)),
  };
}

function clampZoom(vs, zoom) {
  return Math.max(vs?.minZoom ?? -Infinity, Math.min(vs?.maxZoom ?? Infinity, zoom));
}

function zoomValue(vs) {
  if (Array.isArray(vs?.zoom)) return Math.min(vs.zoom[0], vs.zoom[1]);
  return vs?.zoom ?? vs?.zoomX ?? vs?.zoomY ?? 0;
}

function withZoom(vs, zoom) {
  return { ...vs, zoom, zoomX: zoom, zoomY: zoom };
}

function focusView(vs, next) {
  if (!vs) return vs;
  const zoom = next.zoom == null ? null : clampZoom(vs, next.zoom);
  const out = { ...vs, ...next };
  return zoom == null ? out : withZoom(out, zoom);
}

function lerp(a, b, t) {
  return a + (b - a) * t;
}

function lerpTarget(a, b, t) {
  return [
    lerp(a?.[0] ?? 0, b?.[0] ?? 0, t),
    lerp(a?.[1] ?? 0, b?.[1] ?? 0, t),
    lerp(a?.[2] ?? 0, b?.[2] ?? 0, t),
  ];
}

function interpolateView(start, end, t) {
  const zoom = lerp(zoomValue(start), zoomValue(end), t);
  return withZoom(
    {
      ...end,
      target: lerpTarget(start.target, end.target, t),
    },
    zoom,
  );
}

function withViewConstraints(prev, next) {
  const out = { ...next };
  const minZoom = next.minZoom ?? prev?.minZoom;
  const maxZoom = next.maxZoom ?? prev?.maxZoom;
  if (minZoom != null) {
    out.minZoom = minZoom;
    out.minZoomX = next.minZoomX ?? prev?.minZoomX ?? minZoom;
    out.minZoomY = next.minZoomY ?? prev?.minZoomY ?? minZoom;
  }
  if (maxZoom != null) {
    out.maxZoom = maxZoom;
    out.maxZoomX = next.maxZoomX ?? prev?.maxZoomX ?? maxZoom;
    out.maxZoomY = next.maxZoomY ?? prev?.maxZoomY ?? maxZoom;
  }
  return out;
}

function dataBounds(points) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const p of points) {
    const [x, y] = p.position;
    if (x < minX) minX = x;
    if (y < minY) minY = y;
    if (x > maxX) maxX = x;
    if (y > maxY) maxY = y;
  }
  return [minX, minY, maxX, maxY];
}

// Soft-assigned cluster members include noise reassigned by embedding-nearest, which
// scatters them across the 2D plane; framing their raw extent jumps far off the
// visible mass. Trim to a central quantile so framing tracks the dense core.
function robustBounds(points, q = 0.06) {
  if (points.length < 24) return dataBounds(points);
  const xs = points.map((p) => p.position[0]).sort((a, b) => a - b);
  const ys = points.map((p) => p.position[1]).sort((a, b) => a - b);
  const lo = Math.floor(points.length * q);
  const hi = Math.ceil(points.length * (1 - q)) - 1;
  return [xs[lo], ys[lo], xs[hi], ys[hi]];
}

// Extent of a GeoJSON (Multi)Polygon feature — used to frame the exact region
// outline that gets highlighted (core polygon), so framing matches the highlight.
function featureBounds(feature) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const add = (ring) => {
    for (const [x, y] of ring) {
      if (x < minX) minX = x;
      if (y < minY) minY = y;
      if (x > maxX) maxX = x;
      if (y > maxY) maxY = y;
    }
  };
  const g = feature.geometry;
  if (g.type === "Polygon") g.coordinates.forEach(add);
  else if (g.type === "MultiPolygon") g.coordinates.forEach((poly) => poly.forEach(add));
  return [minX, minY, maxX, maxY];
}

function geojsonBounds(gj) {
  if (!gj) return null;
  if (gj.type === "Feature") return featureBounds(gj);
  if (gj.type !== "FeatureCollection") return null;
  let out = [Infinity, Infinity, -Infinity, -Infinity];
  for (const f of gj.features ?? []) {
    const b = featureBounds(f);
    out = [
      Math.min(out[0], b[0]),
      Math.min(out[1], b[1]),
      Math.max(out[2], b[2]),
      Math.max(out[3], b[3]),
    ];
  }
  return Number.isFinite(out[0]) ? out : null;
}

// --- convex-polygon clipping to an axis-aligned rect (Sutherland-Hodgman) ---
const interpX = (a, b, x) => [x, a[1] + ((x - a[0]) / (b[0] - a[0])) * (b[1] - a[1])];
const interpY = (a, b, y) => [a[0] + ((y - a[1]) / (b[1] - a[1])) * (b[0] - a[0]), y];
function clipEdge(pts, inside, intersect) {
  const res = [];
  const n = pts.length;
  for (let i = 0; i < n; i++) {
    const cur = pts[i];
    const prev = pts[(i + n - 1) % n];
    const ci = inside(cur);
    const pi = inside(prev);
    if (ci) {
      if (!pi) res.push(intersect(prev, cur));
      res.push(cur);
    } else if (pi) {
      res.push(intersect(prev, cur));
    }
  }
  return res;
}
function clipRect(poly, x0, y0, x1, y1) {
  let p = clipEdge(poly, (c) => c[0] >= x0, (a, b) => interpX(a, b, x0));
  if (p.length) p = clipEdge(p, (c) => c[0] <= x1, (a, b) => interpX(a, b, x1));
  if (p.length) p = clipEdge(p, (c) => c[1] >= y0, (a, b) => interpY(a, b, y0));
  if (p.length) p = clipEdge(p, (c) => c[1] <= y1, (a, b) => interpY(a, b, y1));
  return p.length >= 3 ? p : null;
}
function shrink(ring, f) {
  let cx = 0, cy = 0;
  for (const [x, y] of ring) {
    cx += x;
    cy += y;
  }
  cx /= ring.length;
  cy /= ring.length;
  return ring.map(([x, y]) => [cx + (x - cx) * f, cy + (y - cy) * f]);
}

function parcelPolygon(point) {
  const [x, y] = point.position;
  const width = Math.max(point.parcelWidth ?? point.parcelSize ?? 0, 1e-9);
  const depth = Math.max(point.parcelDepth ?? point.parcelSize ?? 0, 1e-9);
  const a = point.parcelAngle ?? 0;
  const skew = point.parcelSkew ?? 0;
  const ca = Math.cos(a);
  const sa = Math.sin(a);
  const local = [
    [-width / 2 - skew * depth / 2, -depth / 2],
    [width / 2 - skew * depth / 2, -depth / 2],
    [width / 2 + skew * depth / 2, depth / 2],
    [-width / 2 + skew * depth / 2, depth / 2],
  ];
  return local.map(([lx, ly]) => [x + lx * ca - ly * sa, y + lx * sa + ly * ca]);
}

function buildingPolygon(point) {
  // The building rect is centered at its OWN footprint center (building_cx/cy,
  // exported separately from the lot anchor) when the dataset provides it -- a
  // clipped footprint or a Voronoi-fallback district's footprint can have its
  // true center offset from point.position (the lot anchor / map-click/label
  // position), and centering there instead would draw the building off its own
  // footprint. Falls back to point.position for datasets without it.
  const [px, py] = point.position;
  const x = point.buildingCx ?? px;
  const y = point.buildingCy ?? py;
  const width = Math.max(point.buildingWidth ?? 0, 1e-9);
  const depth = Math.max(point.buildingDepth ?? 0, 1e-9);
  const a = point.buildingAngle ?? 0;
  const ca = Math.cos(a);
  const sa = Math.sin(a);
  const local = [
    [-width / 2, -depth / 2],
    [width / 2, -depth / 2],
    [width / 2, depth / 2],
    [-width / 2, depth / 2],
  ];
  return local.map(([lx, ly]) => [x + lx * ca - ly * sa, y + lx * sa + ly * ca]);
}

export default function WorldMap({
  onSelect,
  onPickRegion,
  onRegions,
  selected,
  focus,
  highlight,
  searchResults = [],
}) {
  const [points, setPoints] = useState([]);
  // "top" = coarsest hierarchy level (continents), "sub" = next (sub-regions).
  // Their numeric level varies by dataset (20k: 3/2; 218k: 5/4), discovered at load.
  const [lvl, setLvl] = useState(null); // { top, sub, levels }
  // 2.5D extrude elevationScale = EXTRUDE.exaggeration / meters_per_app_unit, derived
  // from the loaded manifest.json; EXTRUDE.elevationScale is the fallback for a
  // dataset whose manifest has no meters_per_app_unit (see config.js EXTRUDE comment).
  const [elevationScale, setElevationScale] = useState(EXTRUDE.elevationScale);
  const [land, setLand] = useState(null);
  const [regionsTop, setRegionsTop] = useState(null);
  const [regionsSub, setRegionsSub] = useState(null);
  const [roads, setRoads] = useState(null);
  const [roadsMid, setRoadsMid] = useState(null);
  const [roadsNear, setRoadsNear] = useState(null);
  const [parcels, setParcels] = useState(null);
  const [landuse, setLanduse] = useState(null);
  const [blocks, setBlocks] = useState(null);
  const [viewState, setViewState] = useState(null);
  const [size, setSize] = useState({ width: 1, height: 1 });
  // Hold label rendering until Inter is loaded, so deck builds the SDF atlas from the
  // self-hosted font (its glyph cache is keyed by settings, not rebuilt on font load).
  const [fontReady, setFontReady] = useState(false);
  const baseZoom = useRef(0);
  const wrapRef = useRef(null);
  const focusAnimation = useRef(null);
  const parcelsRequested = useRef(false);
  const blocksRequested = useRef(false);

  const cancelFocusAnimation = () => {
    if (focusAnimation.current != null) cancelAnimationFrame(focusAnimation.current);
    focusAnimation.current = null;
  };

  const animateFocus = (next) => {
    setViewState((vs) => {
      if (!vs) return vs;
      cancelFocusAnimation();
      const start = { ...vs };
      const end = focusView(vs, next);
      const t0 = performance.now();
      const step = (now) => {
        const k = Math.min(1, (now - t0) / FOCUS_ANIMATION_MS);
        const frame = interpolateView(start, end, easeOutCubic(k));
        setViewState((cur) => withViewConstraints(cur, frame));
        if (k < 1) {
          focusAnimation.current = requestAnimationFrame(step);
        } else {
          focusAnimation.current = null;
        }
      };
      focusAnimation.current = requestAnimationFrame(step);
      return start;
    });
  };

  useEffect(() => () => cancelFocusAnimation(), []);

  useEffect(() => {
    const base = import.meta.env.BASE_URL + DATA_DIR;
    getLevels()
      .then((info) => {
        setLvl(info);
        fetch(base + "land.geojson").then((r) => r.json()).then(setLand);
        fetch(base + `regions_l${info.top}.geojson`).then((r) => r.json()).then(setRegionsTop);
        fetch(base + `regions_l${info.sub}.geojson`).then((r) => r.json()).then(setRegionsSub);
        fetch(base + "roads.geojson")
          .then((r) => (r.ok ? r.json() : null))
          .then(setRoads, () => setRoads(null));
        fetch(base + "roads_mid.geojson")
          .then((r) => (r.ok ? r.json() : null))
          .then(setRoadsMid, () => setRoadsMid(null));
        fetch(base + "roads_near.geojson")
          .then((r) => (r.ok ? r.json() : null))
          .then(setRoadsNear, () => setRoadsNear(null));
        // landuse.geojson is small (park/developed patches) — fetch eagerly like
        // roads; datasets without it (non-city) just resolve null, no console spam.
        fetch(base + "landuse.geojson")
          .then((r) => (r.ok ? r.json() : null))
          .then(setLanduse, () => setLanduse(null));
        getManifest().then((manifest) => {
          const mpau = manifest?.meters_per_app_unit;
          setElevationScale(
            mpau ? EXTRUDE.exaggeration / mpau : EXTRUDE.elevationScale,
          );
        });
        return loadPoints();
      })
      .then((d) => {
        setPoints(d);
        if (import.meta.env.DEV) window.__sampleWorldId = d[(d.length / 3) | 0]?.world_id;
      });
  }, []);

  // Report which cluster-ids actually have a region polygon, per level — the sidebar
  // uses this so only clusters with a visible bounded region are navigable.
  useEffect(() => {
    if (!regionsSub || !regionsTop || !lvl || !onRegions) return;
    const ids = (gj) => new Set(gj.features.map((f) => f.properties.cluster_id));
    onRegions({ [lvl.sub]: ids(regionsSub), [lvl.top]: ids(regionsTop) });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [regionsSub, regionsTop, lvl]);

  useEffect(() => {
    let live = true;
    const done = () => live && setFontReady(true);
    const fonts = document.fonts;
    if (fonts?.load) {
      Promise.all([fonts.load('700 16px Inter'), fonts.load('400 16px Inter')]).then(done, done);
    } else {
      done();
    }
    // safety net in case the fonts API never settles
    const t = setTimeout(done, 3000);
    return () => {
      live = false;
      clearTimeout(t);
    };
  }, []);

  useEffect(() => {
    if (!wrapRef.current) return;
    const ro = new ResizeObserver(([e]) =>
      setSize({ width: e.contentRect.width, height: e.contentRect.height }),
    );
    ro.observe(wrapRef.current);
    return () => ro.disconnect();
  }, []);

  // Hoisted above the viewState-init effect below (which needs it to gate the
  // extrude-only rotationX/rotationOrbit fields) -- city datasets provide
  // explicit building footprints, older road/legacy datasets don't.
  const isCity = useMemo(
    () => points.some((p) => p.buildingWidth != null && p.buildingDepth != null),
    [points],
  );

  useEffect(() => {
    if (viewState || size.width < 2) return;
    const bounds = geojsonBounds(land) ?? (points.length ? dataBounds(points) : null);
    if (!bounds) return;
    const v = fitBounds(bounds, size);
    baseZoom.current = v.zoom;
    setViewState({
      ...v,
      minZoom: v.zoom - 2,
      maxZoom: v.zoom + 13,
      minZoomX: v.zoom - 2,
      minZoomY: v.zoom - 2,
      maxZoomX: v.zoom + 13,
      maxZoomY: v.zoom + 13,
      // Extra fields consumed only by OrbitView (extrude mode's pitched view); the
      // default OrthographicView ignores them. Gated on isCity too -- extrude mode
      // only actually applies to city (building) datasets, same as extrudeMode below.
      ...(EXTRUDE_MODE && isCity
        ? { rotationX: EXTRUDE.rotationX, rotationOrbit: EXTRUDE.rotationOrbit }
        : {}),
    });
  }, [land, points, viewState, size, isCity]);

  useEffect(() => {
    if (import.meta.env.DEV) {
      window.__vs = viewState;
      window.__baseZoom = baseZoom.current;
    }
  }, [viewState]);

  // region id -> palette color (regions colored by index, see config REGION_PALETTE)
  const regionRGB = useMemo(() => {
    const regionIds = points.length
      ? points.map((p) => p.region)
      : [regionsTop, regionsSub].flatMap((gj) =>
          (gj?.features ?? []).map((f) => f.properties.region),
        );
    const ids = [...new Set(regionIds)].sort((a, b) => a - b);
    const idx = new Map(ids.map((id, i) => [id, i]));
    return (rid) =>
      hexToRgb(REGION_PALETTE[(idx.get(rid) ?? 0) % REGION_PALETTE.length]);
  }, [points, regionsTop, regionsSub]);

  // The map body. City datasets provide explicit building footprints; older road
  // datasets provide parcel squares; legacy datasets fall back to bounded Voronoi.
  const cells = useMemo(() => {
    if (!points.length) return [];
    if (isCity) return [];
    if (points.some((p) => p.parcelSize != null)) {
      return points.map((p) => ({ polygon: parcelPolygon(p), point: p }));
    }
    const b = dataBounds(points);
    const pad = (b[2] - b[0]) * 0.05;
    const delaunay = Delaunay.from(points, (p) => p.position[0], (p) => p.position[1]);
    const vor = delaunay.voronoi([b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad]);
    const spacing = Math.sqrt(((b[2] - b[0]) * (b[3] - b[1])) / points.length);
    const r = spacing * CELLS.radiusK;
    const out = [];
    for (let i = 0; i < points.length; i++) {
      const cell = vor.cellPolygon(i);
      if (!cell) continue;
      const [px, py] = points[i].position;
      const clipped = clipRect(cell.slice(0, -1), px - r, py - r, px + r, py + r);
      if (clipped) out.push({ polygon: shrink(clipped, CELLS.shrink), point: points[i] });
    }
    return out;
  }, [points, isCity]);

  // Two fixed global character sets (stable, so no per-pan atlas rebuilds): Latin-only
  // labels' glyphs vs. everything else (CJK/kana/emoji + any Latin co-occurring in
  // those strings). The Latin set is tiny so its atlas stays at full fontSize.
  const { latinChars, wideChars } = useMemo(() => {
    const latin = new Set(" …");
    const wide = new Set(" …");
    const add = (text) => {
      const set = NON_LATIN.test(text) ? wide : latin;
      for (const ch of text) set.add(ch);
    };
    for (const p of points) if (p.name) add(p.name);
    for (const gj of [regionsTop, regionsSub])
      for (const f of gj?.features ?? []) add(f.properties.label);
    return { latinChars: Array.from(latin), wideChars: Array.from(wide) };
  }, [points, regionsTop, regionsSub]);

  // atlas size adapts to each set's glyph count so neither overflows the GPU texture
  const atlasLatin = useMemo(
    () => atlasFor(latinChars.length, LABELS.sdfFontSize, LABELS.sdfBuffer, LABELS.sdfRadius),
    [latinChars],
  );
  const atlasWide = useMemo(
    () => atlasFor(wideChars.length, LABELS.sdfFontSize, LABELS.sdfBuffer, LABELS.sdfRadius),
    [wideChars],
  );

  const capitols = useMemo(() => {
    const byRegion = new Map();
    for (const p of points) {
      const a = byRegion.get(p.region);
      if (a) a.push(p);
      else byRegion.set(p.region, [p]);
    }
    const out = [];
    for (const arr of byRegion.values()) {
      arr.sort((a, b) => b.visits - a.visits);
      for (const p of arr.slice(0, CAPITOLS_PER_REGION)) out.push(p);
    }
    return out;
  }, [points]);

  const zoom = viewState ? viewState.zoom : 0;
  const tier =
    zoom >= baseZoom.current + ZOOM.nearOffset
      ? "near"
      : zoom >= baseZoom.current + ZOOM.midOffset
        ? "mid"
        : "far";

  // Only render cells from CELLS.renderFromTier inward, and only those near the
  // viewport — at scale (218k worlds) drawing every cell each frame is the killer.
  // Recompute the visible set only when the view moves ~a viewport or zoom changes a
  // step (a generous margin covers movement between recomputes), not every frame.
  const renderCells =
    TIER_RANK[tier] >=
    TIER_RANK[isCity ? BUILDINGS.renderFromTier : CELLS.renderFromTier];
  const z2 = 2 ** zoom;
  const hw = size.width / 2 / z2;
  const hh = size.height / 2 / z2;
  const gx = viewState ? Math.floor(viewState.target[0] / Math.max(hw, 1e-6)) : 0;
  const gy = viewState ? Math.floor(viewState.target[1] / Math.max(hh, 1e-6)) : 0;
  const qz = Math.round(zoom * 2);
  // Debounce the heavy culled-set recompute until the view settles, so a zoom/pan
  // gesture doesn't rebuild thousands of cell/label objects on every quantized step.
  // `settle` is the committed (gx,gy,qz); the memos key off it and read the (now
  // stationary) viewState when it changes.
  const [settle, setSettle] = useState({ gx: 0, gy: 0, qz: 0 });
  useEffect(() => {
    const t = setTimeout(() => setSettle({ gx, gy, qz }), LABELS.settleMs);
    return () => clearTimeout(t);
  }, [gx, gy, qz]);

  const visibleCells = useMemo(() => {
    if (!renderCells || !viewState) return [];
    const source = isCity ? points : cells;
    if (!source.length) return [];
    const m = 1.6; // render a margin beyond the viewport so panning stays covered
    const [cx, cy] = viewState.target;
    const mhw = hw * m;
    const mhh = hh * m;
    const visible = source.filter((item) => {
      const point = isCity ? item : item.point;
      const [x, y] = point.position;
      return x >= cx - mhw && x <= cx + mhw && y >= cy - mhh && y <= cy + mhh;
    });
    if (!isCity) return visible;
    return visible.map((p) => ({ polygon: buildingPolygon(p), point: p }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [renderCells, isCity, points, cells, settle]);

  // focus: smoothly frame a world, result set, or cluster extent.
  useEffect(() => {
    if (!focus || size.width < 2) return;
    if (focus.results?.length) {
      if (focus.results.length === 1) {
        const p = focus.results[0];
        animateFocus({
          target: [p.position[0], p.position[1], 0],
          zoom: baseZoom.current + ZOOM.nearOffset + 1,
        });
      } else {
        animateFocus(fitBounds(dataBounds(focus.results), size, 0.6));
      }
      return;
    }
    if (!points.length) return;
    if (focus.world_id) {
      const p = points.find((q) => q.world_id === focus.world_id);
      if (p)
        animateFocus({
          target: [p.position[0], p.position[1], 0],
          zoom: baseZoom.current + ZOOM.nearOffset + 1,
        });
      return;
    }
    if (focus.level != null && focus.sid != null) {
      // Prefer the core region polygon's extent (matches the highlight outline);
      // fall back to a robust extent of the soft members for finer levels w/o polys.
      const gj =
        focus.level === lvl?.top ? regionsTop : focus.level === lvl?.sub ? regionsSub : null;
      const f = gj?.features?.find((ft) => ft.properties.cluster_id === focus.sid);
      if (f) {
        animateFocus(fitBounds(featureBounds(f), size, 0.6));
      } else {
        const members = points.filter((p) => p.sid[focus.level] === focus.sid);
        if (members.length) animateFocus(fitBounds(robustBounds(members), size, 0.6));
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [focus]);

  const topLabels = useMemo(
    () =>
      (regionsTop?.features ?? []).map((f) => ({
        position: labelPoint(f),
        text: f.properties.label,
        size: LABELS.sizeL3,
        priority: priL3(f.properties.size || 0),
        kind: "region",
        level: lvl?.top,
        sid: f.properties.cluster_id,
        point: { name: f.properties.label, region_name: "" },
      })),
    [regionsTop, lvl],
  );
  const subLabels = useMemo(
    () =>
      (regionsSub?.features ?? []).map((f) => ({
        position: labelPoint(f),
        text: f.properties.label,
        size: LABELS.sizeL2,
        priority: priL2(f.properties.size || 0),
        kind: "region",
        level: lvl?.sub,
        sid: f.properties.cluster_id,
        point: { name: f.properties.label, region_name: "" },
      })),
    [regionsSub, lvl],
  );

  // Explicit label LOD by zoom tier (the collision filter still culls overlaps):
  //   far  -> region (continents) + capitol anchors only
  //   mid  -> region (l3+l2) + capitols + "major" worlds (top-N globally by visits)
  //   near -> every world eligible for a label (up to WORLD_LABELS.max)
  // "Capitols" (top-per-region) get a distinct color + a pin offset.
  const truncName = (s) =>
    s.length > LABEL_STYLE.worldMaxChars
      ? s.slice(0, LABEL_STYLE.worldMaxChars).trimEnd() + "…"
      : s;
  const labelOfWorld = (p, cap) => ({
    position: p.position,
    world_id: p.world_id,
    text: truncName(p.name),
    size: cap ? LABELS.sizeCapitol : LABELS.sizeWorld,
    priority: cap ? priCapitol(p.visits) : priWorld(p.visits),
    kind: cap ? "capitol" : "world",
    point: p,
  });

  const sortedWorlds = useMemo(
    () => [...points].filter((p) => p.name).sort((a, b) => b.visits - a.visits),
    [points],
  );
  const capitolIdSet = useMemo(() => new Set(capitols.map((p) => p.world_id)), [capitols]);

  // World-label candidates: only worlds near the viewport (deck has no spatial
  // culling — it processes every instance each frame, so feeding it 218k labels at
  // near zoom is the bog). sortedWorlds is visit-ordered, so scanning + early-stop at
  // the budget yields the top-by-visits worlds in view. The LOD budget grows with
  // zoom: far (whole set in view) => tiny budget => a few major worlds; near (small
  // viewport) => essentially every visible cell, capped by maxOnscreen. Recomputes
  // only when the view settles (keyed off `settle`).
  const worldLabelData = useMemo(() => {
    if (!viewState) return [];
    const lod = Math.max(0, Math.floor((viewState.zoom - baseZoom.current) / WORLD_LABELS.zoomStep));
    const budget = Math.min(
      WORLD_LABELS.max,
      Math.round(WORLD_LABELS.base * WORLD_LABELS.growth ** lod),
    );
    const cap = Math.min(budget, WORLD_LABELS.maxOnscreen);
    const [cx, cy] = viewState.target;
    const m = 1.25;
    const mhw = hw * m;
    const mhh = hh * m;
    const out = [];
    for (const p of sortedWorlds) {
      const [x, y] = p.position;
      if (x < cx - mhw || x > cx + mhw || y < cy - mhh || y > cy + mhh) continue;
      out.push(labelOfWorld(p, capitolIdSet.has(p.world_id)));
      if (out.length >= cap) break;
    }
    return out;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sortedWorlds, settle, capitolIdSet]);

  // Greedy screen-space label declutter (cartographic-style), instead of deck's
  // GPU CollisionFilterExtension: project each candidate to pixels, measure its real
  // box with the loaded font, then place highest-priority first (regions outrank
  // worlds) and skip any that overlap an already-placed label. This guarantees
  // non-overlap, lets region names always win, and naturally drops sprawling long
  // labels (they collide with more). Recomputes only when the view settles.
  const measureCtx = useMemo(() => document.createElement("canvas").getContext("2d"), []);
  const { latinLabels, wideLabels } = useMemo(() => {
    if (!viewState || !fontReady) return { latinLabels: [], wideLabels: [] };
    const scale = 2 ** viewState.zoom;
    const [cx, cy] = viewState.target;
    const region = tier === "far" ? topLabels : topLabels.concat(subLabels);
    const cands = region.concat(worldLabelData).sort((a, b) => b.priority - a.priority);
    const maxW = LABEL_STYLE.maxWidth;
    const gap = LABELS.labelGap;
    const placed = [];
    const latin = [];
    const wide = [];
    for (const d of cands) {
      const off = d.kind === "capitol" ? LABEL_STYLE.capitolPixelOffset : [0, 0];
      const px = size.width / 2 + (d.position[0] - cx) * scale + off[0];
      const py = size.height / 2 - (d.position[1] - cy) * scale + off[1];
      if (px < -120 || px > size.width + 120 || py < -80 || py > size.height + 80) continue;
      measureCtx.font = `700 ${d.size}px Inter, sans-serif`;
      const w = measureCtx.measureText(d.text).width;
      const lineW = maxW * d.size; // TextLayer wraps at maxWidth (multiples of size)
      const lines = w > lineW ? Math.ceil(w / lineW) : 1;
      const bw = Math.min(w, lineW) / 2 + gap;
      const bh = (lines * d.size * 1.2) / 2 + gap;
      const x0 = px - bw, x1 = px + bw, y0 = py - bh, y1 = py + bh;
      let ok = true;
      for (const r of placed) {
        if (x0 < r.x1 && x1 > r.x0 && y0 < r.y1 && y1 > r.y0) {
          ok = false;
          break;
        }
      }
      if (!ok) continue;
      placed.push({ x0, y0, x1, y1 });
      (NON_LATIN.test(d.text) ? wide : latin).push(d);
    }
    return { latinLabels: latin, wideLabels: wide };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [settle, fontReady, tier, topLabels, subLabels, worldLabelData, size]);

  const highlightFeature = useMemo(() => {
    if (!highlight) return null;
    const gj = highlight.level === lvl?.top ? regionsTop : regionsSub;
    const f = (gj?.features ?? []).find((ft) => ft.properties.cluster_id === highlight.sid);
    return f ? { type: "FeatureCollection", features: [f] } : null;
  }, [highlight, regionsTop, regionsSub, lvl]);

  const roadFeatures = useMemo(() => {
    const source =
      tier === "near" && roadsNear ? roadsNear : tier === "mid" && roadsMid ? roadsMid : roads;
    const features = source?.features ?? [];
    if (tier === "far") return features.filter((f) => f.properties.kind === "arterial");
    if (tier === "mid") {
      return features.filter(
        (f) => f.properties.kind !== "minor" && f.properties.kind !== "service",
      );
    }
    return features;
  }, [roads, roadsMid, roadsNear, tier]);

  useEffect(() => {
    if (!isCity || parcels || parcelsRequested.current) return;
    if (TIER_RANK[tier] < TIER_RANK[PARCELS.visibleFromTier]) return;
    parcelsRequested.current = true;
    const base = import.meta.env.BASE_URL + DATA_DIR;
    fetch(base + "parcels.geojson")
      .then((r) => (r.ok ? r.json() : null))
      .then(setParcels, () => setParcels(null));
  }, [isCity, parcels, tier]);

  // blocks.geojson can be large (thousands of features covering the whole city), so
  // it's lazily fetched once the zoom tier reaches BLOCKS.visibleFromTier — same
  // lazy/gate convention as parcels above. Datasets without the file resolve null.
  useEffect(() => {
    if (!isCity || blocks || blocksRequested.current) return;
    if (TIER_RANK[tier] < TIER_RANK[BLOCKS.visibleFromTier]) return;
    blocksRequested.current = true;
    const base = import.meta.env.BASE_URL + DATA_DIR;
    fetch(base + "blocks.geojson")
      .then((r) => (r.ok ? r.json() : null))
      .then(setBlocks, () => setBlocks(null));
  }, [isCity, blocks, tier]);

  if (!viewState) {
    return (
      <div ref={wrapRef} className="map-wrap" style={{ background: OCEAN }}>
        <div className="map-loading">Loading map…</div>
      </div>
    );
  }

  const cellsStroked =
    TIER_RANK[tier] >=
    TIER_RANK[isCity ? BUILDINGS.strokeMinZoomTier : CELLS.strokeMinZoomTier];
  const roadsVisible = roads && TIER_RANK[tier] >= TIER_RANK[ROADS.visibleFromTier];
  const parcelsVisible =
    isCity && parcels && TIER_RANK[tier] >= TIER_RANK[PARCELS.visibleFromTier];
  const landuseVisible =
    isCity && landuse && TIER_RANK[tier] >= TIER_RANK[LANDUSE.visibleFromTier];
  const blocksVisible =
    isCity && blocks && TIER_RANK[tier] >= TIER_RANK[BLOCKS.visibleFromTier];
  const landuseFillColor = (f) =>
    f.properties.kind === "park" ? LANDUSE.parkColor : LANDUSE.developedColor;
  const extrudeMode = EXTRUDE_MODE && isCity;
  const roadWidth = (f) =>
    f.properties.kind === "arterial"
      ? ROADS.arterialWidth
      : f.properties.kind === "minor" || f.properties.kind === "service"
        ? (ROADS.minorWidth ?? ROADS.localWidth * 0.55)
        : f.properties.kind === "collector"
          ? (ROADS.collectorWidth ?? Math.max(ROADS.localWidth * 1.45, 2.05))
          : ROADS.localWidth;
  const roadColor = (f) =>
    f.properties.kind === "arterial"
      ? ROADS.arterialColor
      : f.properties.kind === "minor" || f.properties.kind === "service"
        ? (ROADS.minorColor ?? ROADS.localColor)
        : f.properties.kind === "collector"
          ? (ROADS.collectorColor ?? ROADS.localColor)
          : ROADS.localColor;
  const roadCasingExtra = (f) =>
    f.properties.kind === "minor" || f.properties.kind === "service"
      ? (ROADS.minorCasingExtraWidth ?? 0.25)
      : ROADS.casingExtraWidth;

  // one TextLayer per atlas (Latin / wide). Data is pre-decluttered in JS, so no
  // collision extension is needed here (deck just renders the chosen labels).
  const textLayer = (id, data, chars, at) =>
    new TextLayer({
      id,
      data,
      getPosition: (d) => d.position,
      getText: (d) => d.text,
      getSize: (d) => d.size,
      sizeUnits: "pixels",
      characterSet: chars,
      getColor: (d) =>
        d.kind === "capitol"
          ? LABEL_STYLE.capitolColor
          : d.kind === "region"
            ? LABEL_STYLE.regionColor
            : LABEL_STYLE.worldColor,
      getPixelOffset: (d) => (d.kind === "capitol" ? LABEL_STYLE.capitolPixelOffset : [0, 0]),
      wordBreak: LABEL_STYLE.wordBreak,
      maxWidth: LABEL_STYLE.maxWidth,
      fontFamily:
        "'Inter', -apple-system, 'Segoe UI', 'Hiragino Sans', 'Noto Sans JP', sans-serif",
      fontWeight: 700,
      fontSettings: { sdf: true, fontSize: at.fontSize, buffer: at.buffer, radius: at.radius },
      outlineWidth: LABELS.outlineWidth,
      outlineColor: LABELS.outlineColor,
      background: LABELS.background,
      getBackgroundColor: LABELS.backgroundColor,
      backgroundPadding: LABELS.backgroundPadding,
      pickable: true,
      // clicking a region label frames+browses that region (gmaps "click a city");
      // clicking a world/capitol label selects that world.
      onClick: (info) => {
        const d = info.object;
        if (!d) return;
        if (d.kind === "region") onPickRegion?.(d.level, d.sid, d.text);
        else if (d.world_id) onSelect(d.world_id);
      },
    });

  const layers = [
    // "land": a precomputed rasterized/dilated polygon over all world coordinates. This
    // keeps the occupied area solid at close zoom without drawing every world as a
    // large overlapping scatter point during pan/zoom.
    new GeoJsonLayer({
      id: "land",
      data: land,
      filled: true,
      stroked: false,
      getFillColor: LAND.color,
      pickable: false,
    }),
    // continent background color field
    !isCity &&
      new GeoJsonLayer({
        id: "bg-top",
        data: regionsTop,
        filled: true,
        stroked: false,
        getFillColor: (f) => [...regionRGB(f.properties.region), REGION_BG.l3Alpha],
        pickable: false,
      }),
    // sub-region tint (adds a second density layer when zoomed in a bit)
    !isCity &&
      new GeoJsonLayer({
        id: "bg-sub",
        data: regionsSub,
        visible: tier !== "far",
        filled: true,
        stroked: false,
        getFillColor: (f) => [...regionRGB(f.properties.region), REGION_BG.l2Alpha],
        pickable: false,
      }),
    // landuse (park/developed patches) — a filled backdrop under blocks/buildings.
    landuseVisible &&
      new GeoJsonLayer({
        id: "landuse",
        data: landuse,
        filled: true,
        stroked: false,
        getFillColor: landuseFillColor,
        pickable: false,
      }),
    // block outlines — the road-network cells that group lots, faint structure
    // beneath parcels/buildings.
    blocksVisible &&
      new GeoJsonLayer({
        id: "blocks",
        data: blocks,
        filled: BLOCKS.fillColor[3] > 0,
        stroked: true,
        getFillColor: BLOCKS.fillColor,
        getLineColor: BLOCKS.lineColor,
        lineWidthUnits: "pixels",
        getLineWidth: BLOCKS.lineWidth,
        pickable: false,
      }),
    new PolygonLayer({
      id: isCity ? "buildings" : "cells",
      data: visibleCells,
      getPolygon: (d) => d.polygon,
      getFillColor: (d) =>
        d.point.world_id === selected
          ? [...regionRGB(d.point.region).map((c) => Math.round(c * SELECTED.valueMul)), SELECTED.fillAlpha]
          : [
              ...regionRGB(d.point.region),
              isCity ? BUILDINGS.fillAlpha : CELLS.fillAlpha,
            ],
      getLineColor: (d) =>
        d.point.world_id === selected
          ? SELECTED.outline
          : [255, 255, 255, isCity ? BUILDINGS.strokeAlpha : CELLS.strokeAlpha],
      getLineWidth: (d) => (d.point.world_id === selected ? SELECTED.outlineWidth : 0.5),
      lineWidthUnits: "pixels",
      stroked: cellsStroked || selected != null,
      filled: true,
      pickable: true,
      onClick: (info) => info.object && onSelect(info.object.point.world_id),
      // 2.5D mode (?extrude=1, city datasets only): extrude each building footprint
      // by building_height, scaled down to the layout's normalized units (see the
      // EXTRUDE comment in config.js — height and footprint size live on very
      // different scales in this dataset; elevationScale here is derived per-dataset
      // from manifest.json's meters_per_app_unit, see the state hook above).
      extruded: extrudeMode,
      wireframe: EXTRUDE.wireframe,
      material: extrudeMode ? EXTRUDE.material : null,
      getElevation: extrudeMode
        ? (d) => (d.point.buildingHeight ?? 0) * elevationScale
        : 0,
      updateTriggers: {
        getFillColor: selected,
        getLineColor: selected,
        getLineWidth: selected,
      },
    }),
    parcelsVisible &&
      new GeoJsonLayer({
        id: "parcels",
        data: parcels,
        filled: false,
        stroked: true,
        getLineColor: PARCELS.lineColor,
        lineWidthUnits: "pixels",
        getLineWidth: PARCELS.lineWidth,
        pickable: false,
      }),
    roadsVisible &&
      new GeoJsonLayer({
        id: "roads-casing",
        data: { type: "FeatureCollection", features: roadFeatures },
        filled: false,
        stroked: true,
        getLineColor: ROADS.casingColor,
        lineWidthUnits: "pixels",
        getLineWidth: (f) => roadWidth(f) + roadCasingExtra(f),
        lineJointRounded: true,
        lineCapRounded: true,
        pickable: false,
      }),
    roadsVisible &&
      new GeoJsonLayer({
        id: "roads",
        data: { type: "FeatureCollection", features: roadFeatures },
        filled: false,
        stroked: true,
        getLineColor: roadColor,
        lineWidthUnits: "pixels",
        getLineWidth: roadWidth,
        lineJointRounded: true,
        lineCapRounded: true,
        pickable: false,
      }),
    // region outlines (continent when far, sub-region when mid)
    (tier === "far" ? regionsTop : tier === "mid" ? regionsSub : null) &&
      new GeoJsonLayer({
        id: "region-outlines",
        data: tier === "far" ? regionsTop : regionsSub,
        filled: false,
        stroked: true,
        getLineColor: (f) => [...regionRGB(f.properties.region), REGION_BG.outlineAlpha],
        lineWidthUnits: "pixels",
        getLineWidth: REGION_BG.outlineWidth,
        pickable: false,
      }),
    new ScatterplotLayer({
      id: "capitols",
      data: capitols,
      visible: tier !== "near",
      getPosition: (d) => d.position,
      getFillColor: (d) => (d.world_id === selected ? [255, 255, 255] : [30, 30, 30]),
      getRadius: CAPITOL_DOT.radius,
      radiusUnits: "pixels",
      radiusMinPixels: CAPITOL_DOT.minPixels,
      radiusMaxPixels: CAPITOL_DOT.maxPixels,
      stroked: true,
      lineWidthUnits: "pixels",
      getLineWidth: 1.5,
      getLineColor: [255, 255, 255, 230],
      pickable: true,
      onClick: (info) => info.object && onSelect(info.object.world_id),
      updateTriggers: { getFillColor: selected },
    }),
    new ScatterplotLayer({
      id: "search-results",
      data: searchResults,
      visible: searchResults.length > 0,
      getPosition: (d) => d.position,
      getFillColor: SEARCH_PIN.fill,
      getRadius: SEARCH_PIN.radius,
      radiusUnits: "pixels",
      radiusMinPixels: SEARCH_PIN.minPixels,
      radiusMaxPixels: SEARCH_PIN.maxPixels,
      stroked: true,
      lineWidthUnits: "pixels",
      getLineWidth: SEARCH_PIN.outlineWidth,
      getLineColor: SEARCH_PIN.outline,
      pickable: true,
      onClick: (info) => info.object && onSelect(info.object.world_id),
    }),
    highlightFeature &&
      new GeoJsonLayer({
        id: "highlight",
        data: highlightFeature,
        filled: false,
        stroked: true,
        getLineColor: [20, 20, 20, 235],
        lineWidthUnits: "pixels",
        getLineWidth: 3,
        pickable: false,
      }),
    ...(fontReady
      ? [
          textLayer("labels-latin", latinLabels, latinChars, atlasLatin),
          textLayer("labels-wide", wideLabels, wideChars, atlasWide),
        ]
      : []),
  ];

  return (
    <div ref={wrapRef} className="map-wrap" style={{ background: OCEAN }}>
      <DeckGL
        views={
          extrudeMode
            ? new OrbitView({ orthographic: true, controller: true })
            : new OrthographicView({ flipY: false })
        }
        viewState={viewState}
        controller={true}
        pickingRadius={6}
        layers={layers}
        onViewStateChange={({ viewState: vs, interactionState }) => {
          if (
            interactionState?.isDragging ||
            interactionState?.isPanning ||
            interactionState?.isZooming
          ) {
            cancelFocusAnimation();
          }
          setViewState((prev) => withViewConstraints(prev, vs));
        }}
        getCursor={({ isDragging, isHovering }) =>
          isDragging ? "grabbing" : isHovering ? "pointer" : "grab"
        }
      />
    </div>
  );
}
