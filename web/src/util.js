// Small shared helpers for the map.

const _hexCache = new Map();

/** "#rrggbb" -> [r, g, b] (0-255). Cached; falls back to mid-gray. */
export function hexToRgb(hex) {
  if (_hexCache.has(hex)) return _hexCache.get(hex);
  let rgb = [136, 136, 136];
  if (typeof hex === "string" && hex[0] === "#" && hex.length === 7) {
    rgb = [
      parseInt(hex.slice(1, 3), 16),
      parseInt(hex.slice(3, 5), 16),
      parseInt(hex.slice(5, 7), 16),
    ];
  }
  _hexCache.set(hex, rgb);
  return rgb;
}

/**
 * A point to place a region's label: the centroid of its largest island's
 * exterior ring. Averaging *all* vertices of a fragmented MultiPolygon can land
 * the label in the empty gap between islands, so we pick the biggest ring first
 * (same intent as the static map's representative_point on the largest island).
 */
export function labelPoint(feature) {
  const g = feature.geometry;
  let rings = [];
  if (g.type === "Polygon") rings = [g.coordinates[0]];
  else if (g.type === "MultiPolygon") rings = g.coordinates.map((p) => p[0]);
  if (rings.length === 0) return [0, 0];
  let best = rings[0];
  for (const r of rings) if (r.length > best.length) best = r;
  let sx = 0;
  let sy = 0;
  for (const [x, y] of best) {
    sx += x;
    sy += y;
  }
  return [sx / best.length, sy / best.length];
}
