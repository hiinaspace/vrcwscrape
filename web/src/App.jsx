import { useCallback, useEffect, useState } from "react";
import WorldMap from "./Map.jsx";
import Sidebar from "./Sidebar.jsx";

export default function App() {
  const [selected, setSelected] = useState(null);
  const [searchResults, setSearchResults] = useState([]);
  // focus = fly the map to a world or cluster; highlight = outline a cluster.
  // browseReq = ask the sidebar to open browse mode at a given cluster.
  const [focus, setFocus] = useState(null);
  const [highlight, setHighlight] = useState(null);
  const [browseReq, setBrowseReq] = useState(null);
  // which cluster-ids have a region polygon (per level), reported by the map; the
  // sidebar uses it to only offer navigation into clusters with a visible region.
  const [regionSets, setRegionSets] = useState(null);

  const selectWorld = useCallback((world_id) => {
    setSelected(world_id);
  }, []);
  const focusWorld = useCallback((world_id) => {
    setSelected(world_id);
    setFocus({ world_id, nonce: Date.now() });
  }, []);

  // dev-only hook so the headless smoke test can drive selection (mjolnir's tap
  // recognizer ignores some synthetic clicks)
  useEffect(() => {
    if (import.meta.env.DEV) window.__select = focusWorld;
  }, [focusWorld]);
  const focusCluster = (level, sid) => {
    setHighlight({ level, sid });
    setFocus({ level, sid, nonce: Date.now() });
  };
  // Navigate to a region (from a map region-label click or a world's breadcrumb):
  // drop the selected world, frame+outline the region, open browse mode there.
  const browseCluster = (level, sid, name) => {
    setSelected(null);
    setHighlight({ level, sid });
    setFocus({ level, sid, nonce: Date.now() });
    setBrowseReq({ level, sid, name, nonce: Date.now() });
  };
  const updateSearchResults = useCallback((results) => {
    setSearchResults(results);
    if (results.length) {
      setFocus({ results: results.slice(0, 40), nonce: Date.now() });
    }
  }, []);

  return (
    <div className="app">
      <Sidebar
        worldId={selected}
        browseReq={browseReq}
        regionSets={regionSets}
        onClose={() => {
          setSelected(null);
          setHighlight(null);
        }}
        onFocusWorld={focusWorld}
        onFocusCluster={focusCluster}
        onBrowseCluster={browseCluster}
        onSearchResults={updateSearchResults}
      />
      <div className="map-pane">
        <WorldMap
          onSelect={selectWorld}
          onPickRegion={browseCluster}
          onRegions={setRegionSets}
          selected={selected}
          focus={focus}
          highlight={highlight}
          searchResults={searchResults}
        />
      </div>
    </div>
  );
}
