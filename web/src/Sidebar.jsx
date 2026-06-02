import { useEffect, useState } from "react";
import {
  queryChildClusters,
  queryClusterWorlds,
  queryTopClusters,
  queryWorld,
} from "./duckdb.js";

function vrchatUrl(id) {
  return `https://vrchat.com/home/world/${id}/info`;
}

export default function Sidebar({
  worldId,
  browseReq,
  regionSets,
  onClose,
  onFocusWorld,
  onFocusCluster,
  onBrowseCluster,
}) {
  // a cluster is navigable only if it has a region polygon on the map at that level
  const navigable = (level, sid) => !!regionSets?.[level]?.has(sid);
  const [world, setWorld] = useState(null);
  const [loading, setLoading] = useState(false);
  // world-detail "area" list (the world's sub-region)
  const [cluster, setCluster] = useState(null);
  const [clusterWorlds, setClusterWorlds] = useState([]);
  // browse mode (no world selected): a drill-down stack of clusters
  const [path, setPath] = useState([]); // [{level,sid,name}], last = current node
  const [children, setChildren] = useState([]); // sub-clusters of the current node
  const [browseWorlds, setBrowseWorlds] = useState([]); // top worlds of current node

  // ---------- world-detail loading ----------
  useEffect(() => {
    if (!worldId) {
      setWorld(null);
      return;
    }
    let live = true;
    setLoading(true);
    queryWorld(worldId).then((w) => {
      if (!live) return;
      setWorld(w);
      setLoading(false);
      setCluster(w?.path?.length ? w.path[Math.min(1, w.path.length - 1)] : null);
    });
    return () => {
      live = false;
    };
  }, [worldId]);

  useEffect(() => {
    if (!cluster) return setClusterWorlds([]);
    let live = true;
    queryClusterWorlds(cluster.level, cluster.sid).then((ws) => live && setClusterWorlds(ws));
    return () => {
      live = false;
    };
  }, [cluster]);

  // a map region-label click (or a world's breadcrumb) opens browse mode rooted at
  // that cluster. worldId is cleared by the parent, so this lands in the browse view.
  useEffect(() => {
    if (!browseReq) return;
    setPath([{ level: browseReq.level, sid: browseReq.sid, name: browseReq.name }]);
  }, [browseReq]);

  // ---------- browse mode (children + worlds of the current node) ----------
  useEffect(() => {
    if (worldId) return;
    let live = true;
    const node = path[path.length - 1];
    const childP = node ? queryChildClusters(node.level, node.sid) : queryTopClusters();
    // only offer drilling into clusters that have a visible region on the map
    childP.then((cs) => live && setChildren(cs.filter((c) => navigable(c.level, c.sid))));
    if (node) queryClusterWorlds(node.level, node.sid).then((ws) => live && setBrowseWorlds(ws));
    else setBrowseWorlds([]);
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [worldId, path, regionSets]);

  const drillTo = (c) => {
    setPath((p) => [...p, c]);
    onFocusCluster?.(c.level, c.sid);
  };
  const breadcrumbTo = (i) => {
    const c = path[i];
    setPath(path.slice(0, i + 1));
    if (c) onFocusCluster?.(c.level, c.sid);
  };
  // ============================ BROWSE (no world) ============================
  if (!worldId) {
    const node = path[path.length - 1];
    return (
      <aside className="sidebar sidebar-empty">
        <h1>VRChat Worlds</h1>
        {!node && (
          <p className="hint">
            A latent map of ~20k worlds. Drill into a region below, or click any world
            on the map. Zoom in until worlds become individual plots.
          </p>
        )}
        <nav className="breadcrumb">
          <button className="crumb" onClick={() => setPath([])}>
            All regions
          </button>
          {path.map((c, i) => (
            <span key={`${c.level}-${c.sid}`}>
              <span className="sep">›</span>
              <button
                className={"crumb" + (i === path.length - 1 ? " active" : "")}
                onClick={() => breadcrumbTo(i)}
              >
                {c.name}
              </button>
            </span>
          ))}
        </nav>

        {children.length > 0 && (
          <>
            <h3 className="area-title">{node ? "Sub-areas" : "Regions"}</h3>
            <ol className="area-list">
              {children.map((c) => (
                <li key={`${c.level}-${c.sid}`}>
                  <button className="area-world drill" onClick={() => drillTo(c)}>
                    <span className="area-name">{c.name}</span>
                    <span className="area-visits">{c.n.toLocaleString()} ›</span>
                  </button>
                </li>
              ))}
            </ol>
          </>
        )}

        {node && browseWorlds.length > 0 && (
          <div className="area">
            <h3 className="area-title">Top worlds here</h3>
            <WorldList worlds={browseWorlds} onPick={onFocusWorld} currentId={null} />
          </div>
        )}
      </aside>
    );
  }

  if (loading || !world) return <aside className="sidebar">Loading…</aside>;

  // ============================ WORLD DETAIL ============================
  return (
    <aside className="sidebar">
      <button className="close" onClick={onClose} title="Close">
        ×
      </button>
      <h2 className="title">{world.name}</h2>
      <div className="author">by {world.author_name || "unknown"}</div>
      <a className="vrc-link" href={vrchatUrl(world.world_id)} target="_blank" rel="noreferrer">
        Open on VRChat ↗
      </a>

      {world.path?.length > 0 && (
        <nav className="breadcrumb" aria-label="cluster path">
          {world.path.map((c, i) => (
            <span key={`${c.level}-${c.sid}`}>
              {i > 0 && <span className="sep">›</span>}
              {navigable(c.level, c.sid) ? (
                <button
                  className="crumb"
                  onClick={() => onBrowseCluster?.(c.level, c.sid, c.name)}
                  title="Go to this area"
                >
                  {c.name}
                </button>
              ) : (
                <span className="crumb-static">{c.name}</span>
              )}
            </span>
          ))}
        </nav>
      )}

      <div className="stats">
        <div>
          <span className="num">{world.visits.toLocaleString()}</span>
          <span className="lbl">visits</span>
        </div>
        <div>
          <span className="num">{world.favorites.toLocaleString()}</span>
          <span className="lbl">favorites</span>
        </div>
      </div>

      <div className="dates">
        <span>Published {world.created || "—"}</span>
        <span>Updated {world.updated || "—"}</span>
      </div>

      {world.tags.length > 0 && (
        <div className="tags">
          {world.tags.map((t) => (
            <span className="tag" key={t}>
              {t}
            </span>
          ))}
        </div>
      )}

      {world.description && <p className="desc">{world.description}</p>}

      <div className="sizes">
        {world.pc_size_mb != null && <span>PC {Math.round(world.pc_size_mb)} MB</span>}
        {world.quest_size_mb != null && <span>Quest {Math.round(world.quest_size_mb)} MB</span>}
      </div>

      {cluster && clusterWorlds.length > 0 && (
        <div className="area">
          <h3 className="area-title">Top worlds in {cluster.name}</h3>
          <WorldList worlds={clusterWorlds} onPick={onFocusWorld} currentId={worldId} />
        </div>
      )}
    </aside>
  );
}

function WorldList({ worlds, onPick, currentId }) {
  return (
    <ol className="area-list">
      {worlds.map((w) => (
        <li key={w.world_id}>
          <button
            className={"area-world" + (w.world_id === currentId ? " current" : "")}
            onClick={() => onPick?.(w.world_id)}
            title={w.name}
          >
            <span className="area-name">{w.name}</span>
            <span className="area-visits">{w.visits.toLocaleString()}</span>
          </button>
        </li>
      ))}
    </ol>
  );
}
