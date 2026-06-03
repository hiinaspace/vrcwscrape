// DuckDB-WASM data access: load the two parquet assets and expose the queries the
// app needs. Salvaged connector/loadParquet pattern from web-prototype (which used
// Mosaic's wasmConnector under the hood); here we drive duckdb-wasm directly.

import * as duckdb from "@duckdb/duckdb-wasm";
import { BROWSE } from "./config.js";
import { hexToRgb } from "./util.js";

// Dataset switch: the full LocalMAP export is the default. `?data=20k` loads the
// smaller root export. `?layout=umap`, `?layout=pacmap`, and `?layout=localmap`
// select full-size comparison exports. No-labs exports are explicit query-param
// choices so the old full-data views remain available for A/B inspection.
// DATA_DIR is also used by Map.jsx for the geojson assets.
const _params = new URLSearchParams(location.search);
const _data = (_params.get("data") || "").toLowerCase();
const _layout = (_params.get("layout") || "").toLowerCase();

function datasetFromParams(data, layout) {
  if (data === "20k") return { key: "20k", dir: "", label: "20k UMAP" };
  if (
    data === "nolabs-umap" ||
    data === "full-nolabs" ||
    data === "full-nolabs-umap" ||
    (data === "nolabs" && layout === "umap")
  ) {
    return { key: "nolabs-umap", dir: "full-nolabs/", label: "No-Labs UMAP" };
  }
  if (
    data === "nolabs-pacmap" ||
    data === "full-nolabs-pacmap" ||
    (data === "nolabs" && layout === "pacmap")
  ) {
    return {
      key: "nolabs-pacmap",
      dir: "full-nolabs-pacmap/",
      label: "No-Labs PaCMAP",
    };
  }
  if (
    data === "nolabs-localmap" ||
    data === "full-nolabs-localmap" ||
    (data === "nolabs" && layout === "localmap")
  ) {
    return {
      key: "nolabs-localmap",
      dir: "full-nolabs-localmap/",
      label: "No-Labs LocalMAP",
    };
  }
  if (
    data === "nolabs-v2" ||
    data === "nolabs-physical-v2" ||
    data === "nolabs-localmap-physical-v2" ||
    data === "full-nolabs-localmap-physical-v2" ||
    layout === "localmap-physical-v2" ||
    (data === "nolabs" && layout === "physical-v2")
  ) {
    return {
      key: "nolabs-localmap-physical-v2",
      dir: "full-nolabs-localmap-physical-v2/",
      label: "No-Labs LocalMAP Physical v2",
    };
  }
  if (
    data === "nolabs" ||
    data === "nolabs-physical" ||
    data === "nolabs-localmap-physical" ||
    data === "full-nolabs-localmap-physical" ||
    layout === "localmap-physical" ||
    (data === "nolabs" && layout === "physical")
  ) {
    return {
      key: "nolabs-localmap-physical",
      dir: "full-nolabs-localmap-physical/",
      label: "No-Labs LocalMAP Physical",
    };
  }
  if (data === "umap" || data === "full" || data === "full-umap" || layout === "umap") {
    return { key: "full", dir: "full/", label: "Full UMAP" };
  }
  if (data === "pacmap" || data === "full-pacmap" || layout === "pacmap") {
    return { key: "pacmap", dir: "full-pacmap/", label: "Full PaCMAP" };
  }
  if (
    data === "localmap" ||
    data === "full-localmap" ||
    layout === "localmap"
  ) {
    return { key: "localmap", dir: "full-localmap/", label: "Full LocalMAP" };
  }
  return { key: "localmap", dir: "full-localmap/", label: "Full LocalMAP" };
}

export const DATASET = datasetFromParams(_data, _layout);
export const DATA_DIR = DATASET.dir;
const asset = (f) => new URL(import.meta.env.BASE_URL + DATA_DIR + f, location.href).href;

function searchTerms(query) {
  return query
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[%_]/g, " ")
    .split(/\s+/)
    .map((s) => s.trim())
    .filter((s) => s.length >= 2)
    .slice(0, 6);
}

let _manifestPromise;
async function getManifest() {
  if (_manifestPromise) return _manifestPromise;
  _manifestPromise = (async () => {
    try {
      const res = await fetch(asset("manifest.json"));
      if (!res.ok) return null;
      return await res.json();
    } catch {
      return null;
    }
  })();
  return _manifestPromise;
}

let _dbPromise;
async function getDB() {
  if (_dbPromise) return _dbPromise;
  _dbPromise = (async () => {
    const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
    // The jsDelivr worker is cross-origin; wrap it in a same-origin blob that
    // importScripts() it (the standard duckdb-wasm browser bootstrap).
    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], {
        type: "text/javascript",
      }),
    );
    const worker = new Worker(workerUrl);
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    return db;
  })();
  return _dbPromise;
}

let _connPromise;
async function getConn() {
  if (_connPromise) return _connPromise;
  _connPromise = (async () => {
    const db = await getDB();
    const conn = await db.connect();
    for (const f of ["app_points.parquet", "worlds_meta.parquet"]) {
      await db.registerFileURL(f, asset(f), duckdb.DuckDBDataProtocol.HTTP, false);
    }
    return conn;
  })();
  return _connPromise;
}

// The toponymy hierarchy depth varies by dataset (20k -> 4 layers l0..l3; 218k ->
// 6 layers l0..l5). Discover the soft-id levels present so the rest of the app can
// treat the coarsest as "continents" and the next as "sub-regions" generically.
let _levels;
export async function getLevels() {
  if (_levels) return _levels;
  const manifest = await getManifest();
  if (manifest?.levels?.length) {
    const levels = manifest.levels.map(Number).sort((a, b) => a - b);
    _levels = {
      levels,
      top: Number(manifest.top ?? levels[levels.length - 1]),
      sub: Number(manifest.sub ?? levels[levels.length - 2]),
    };
    return _levels;
  }
  const conn = await getConn();
  const res = await conn.query(
    `DESCRIBE SELECT * FROM read_parquet('app_points.parquet')`,
  );
  const levels = res
    .toArray()
    .map((r) => /^l(\d+)_sid$/.exec(r.column_name))
    .filter(Boolean)
    .map((m) => Number(m[1]))
    .sort((a, b) => a - b);
  _levels = { levels, top: levels[levels.length - 1], sub: levels[levels.length - 2] };
  return _levels;
}

/** All map points as plain objects ready for deck.gl. */
export async function loadPoints() {
  const conn = await getConn();
  const { levels } = await getLevels();
  const sidCols = levels.map((i) => `l${i}_sid`).join(", ");
  const res = await conn.query(`
    SELECT world_id, x, y, region, region_name, color, name, visits, ${sidCols}
    FROM read_parquet('app_points.parquet')
  `);
  return res.toArray().map((r) => {
    const sid = []; // indexed by level number, so p.sid[level] works for any depth
    for (const i of levels) sid[i] = r[`l${i}_sid`];
    return {
      world_id: r.world_id,
      position: [r.x, r.y],
      color: hexToRgb(r.color),
      name: r.name ?? "",
      visits: Number(r.visits ?? 0),
      region: r.region,
      region_name: r.region_name ?? "",
      sid,
    };
  });
}

/** Full sidebar metadata + cluster path for one world, or null. */
export async function queryWorld(worldId) {
  const conn = await getConn();
  const { levels } = await getLevels();
  const pCols = levels.map((i) => `p.l${i}_sid, p.l${i}_sname`).join(",\n           ");
  const stmt = await conn.prepare(`
    SELECT m.world_id, m.name, m.description, m.author_name, m.visits, m.favorites,
           strftime(m.created_at, '%Y-%m-%d') AS created,
           strftime(m.updated_at, '%Y-%m-%d') AS updated,
           m.pc_size_mb, m.quest_size_mb, m.scrape_status, m.tags,
           ${pCols}
    FROM read_parquet('worlds_meta.parquet') m
    LEFT JOIN read_parquet('app_points.parquet') p USING (world_id)
    WHERE m.world_id = ?
  `);
  const res = await stmt.query(worldId);
  await stmt.close();
  const rows = res.toArray();
  if (rows.length === 0) return null;
  const r = rows[0];
  // coarse -> fine cluster path, dropping empty/duplicate names
  const path = [];
  for (const lvl of [...levels].reverse()) {
    const name = r[`l${lvl}_sname`];
    const sid = r[`l${lvl}_sid`];
    if (name && sid != null && (!path.length || path[path.length - 1].name !== name)) {
      path.push({ level: lvl, sid: Number(sid), name });
    }
  }
  return {
    world_id: r.world_id,
    name: r.name ?? "(untitled)",
    description: r.description ?? "",
    author_name: r.author_name ?? "",
    visits: Number(r.visits ?? 0),
    favorites: Number(r.favorites ?? 0),
    created: r.created ?? "",
    updated: r.updated ?? "",
    pc_size_mb: r.pc_size_mb == null ? null : Number(r.pc_size_mb),
    quest_size_mb: r.quest_size_mb == null ? null : Number(r.quest_size_mb),
    scrape_status: r.scrape_status ?? "",
    tags: r.tags ? Array.from(r.tags, String) : [],
    path,
  };
}

/** Top-level clusters (continents): id, name, world count. For the zero-state. */
export async function queryTopClusters(level) {
  if (level == null) level = (await getLevels()).top;
  const conn = await getConn();
  const res = await conn.query(`
    SELECT l${level}_sid AS sid, any_value(l${level}_sname) AS name, count(*) AS n
    FROM read_parquet('app_points.parquet')
    WHERE l${level}_sname <> ''
    GROUP BY l${level}_sid
    HAVING count(*) >= ${BROWSE.minClusterSize}
    ORDER BY n DESC
  `);
  return res.toArray().map((r) => ({
    level,
    sid: Number(r.sid),
    name: r.name ?? "(area)",
    n: Number(r.n ?? 0),
  }));
}

/**
 * Child sub-clusters (one level finer) within a parent cluster, for drill-down.
 * The hierarchy is a soft DAG — a finer cluster's worlds can span several parents —
 * so we attach each child to its DOMINANT parent (the one holding most of its
 * worlds) and only list it under that parent. This makes the navigation a tree and
 * keeps a clicked sub-area spatially inside the region you drilled from. Tiny
 * clusters (more tag-like than region-like) are dropped via BROWSE.minClusterSize.
 */
export async function queryChildClusters(parentLevel, parentSid) {
  const child = parentLevel - 1;
  if (child < 0) return [];
  const conn = await getConn();
  const stmt = await conn.prepare(`
    WITH agg AS (
      SELECT l${child}_sid AS sid, l${parentLevel}_sid AS psid,
             any_value(l${child}_sname) AS name, count(*) AS n
      FROM read_parquet('app_points.parquet')
      WHERE l${child}_sname <> ''
      GROUP BY l${child}_sid, l${parentLevel}_sid
    ),
    tot AS (
      SELECT sid, any_value(name) AS name, sum(n) AS total, arg_max(psid, n) AS dom
      FROM agg GROUP BY sid
    )
    SELECT sid, name, total AS n FROM tot
    WHERE dom = ? AND total >= ${BROWSE.minClusterSize}
    ORDER BY total DESC
  `);
  const res = await stmt.query(parentSid);
  await stmt.close();
  return res.toArray().map((r) => ({
    level: child,
    sid: Number(r.sid),
    name: r.name ?? "(area)",
    n: Number(r.n ?? 0),
  }));
}

/** Top worlds (by visits) in a hierarchy cluster, for the sidebar's area list. */
export async function queryClusterWorlds(level, sid, limit = 12) {
  const conn = await getConn();
  const stmt = await conn.prepare(`
    SELECT world_id, name, visits
    FROM read_parquet('app_points.parquet')
    WHERE l${level}_sid = ?
    ORDER BY visits DESC
    LIMIT ${limit}
  `);
  const res = await stmt.query(sid);
  await stmt.close();
  return res.toArray().map((r) => ({
    world_id: r.world_id,
    name: r.name ?? "(untitled)",
    visits: Number(r.visits ?? 0),
  }));
}

/** Weighted metadata search with coordinates for map result pins. */
export async function searchWorlds(query, limit = 40) {
  const terms = searchTerms(query);
  if (!terms.length) return [];
  const conn = await getConn();
  const q = terms.join(" ");
  const needle = `%${q}%`;
  const where = terms.map(() => "haystack LIKE ?").join(" AND ");
  const safeLimit = Math.max(1, Math.min(100, Number(limit) || 40));
  const stmt = await conn.prepare(`
    WITH docs AS (
      SELECT
        m.world_id,
        m.name,
        m.author_name,
        m.visits,
        m.favorites,
        strftime(m.created_at, '%Y-%m-%d') AS created,
        strftime(m.updated_at, '%Y-%m-%d') AS updated,
        m.pc_size_mb,
        m.quest_size_mb,
        p.x,
        p.y,
        p.region_name,
        lower(coalesce(m.name, '')) AS lname,
        lower(coalesce(m.author_name, '')) AS lauthor,
        lower(concat_ws(
          ' ',
          coalesce(m.name, ''),
          coalesce(m.description, ''),
          coalesce(m.author_name, ''),
          coalesce(cast(m.tags AS varchar), '')
        )) AS haystack
      FROM read_parquet('worlds_meta.parquet') m
      JOIN read_parquet('app_points.parquet') p USING (world_id)
    )
    SELECT
      world_id, name, author_name, visits, favorites, created, updated,
      pc_size_mb, quest_size_mb, x, y, region_name,
      (
        CASE WHEN lname = ? THEN 120 ELSE 0 END +
        CASE WHEN lname LIKE ? THEN 80 ELSE 0 END +
        CASE WHEN lname LIKE ? THEN 45 ELSE 0 END +
        CASE WHEN lauthor LIKE ? THEN 18 ELSE 0 END +
        ln(cast(coalesce(visits, 0) AS double) + 1)
      ) AS score
    FROM docs
    WHERE ${where}
    ORDER BY score DESC, visits DESC NULLS LAST
    LIMIT ${safeLimit}
  `);
  const res = await stmt.query(q, `${q}%`, needle, needle, ...terms.map((t) => `%${t}%`));
  await stmt.close();
  return res.toArray().map((r, i) => ({
    rank: i + 1,
    world_id: r.world_id,
    name: r.name ?? "(untitled)",
    author_name: r.author_name ?? "",
    visits: Number(r.visits ?? 0),
    favorites: Number(r.favorites ?? 0),
    created: r.created ?? "",
    updated: r.updated ?? "",
    pc_size_mb: r.pc_size_mb == null ? null : Number(r.pc_size_mb),
    quest_size_mb: r.quest_size_mb == null ? null : Number(r.quest_size_mb),
    position: [Number(r.x), Number(r.y)],
    region_name: r.region_name ?? "",
    score: Number(r.score ?? 0),
  }));
}
