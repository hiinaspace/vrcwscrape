"""Chen 2024 Section 4.2 per-level street extension over the parcel corner graph.

This implements the paper's four-step street generation, run once per
hierarchical level with the street network that already exists at that level:

1. Identify and group unreachable parcels. A parcel is reachable when at least
   one of its corner-graph boundary edges is already in the street network.
   Unreachable parcels are grouped into connected components of the parcel
   adjacency graph (parcels sharing a corner-graph edge).
2. Generate a street access for each group. For each corner on the group
   boundary we enumerate I-shaped accesses (one collinear chain per incident
   edge of the corner); two consecutive edges are colinear when their included
   angle is larger than 135 degrees, otherwise the shared vertex is a junction.
   If an I-shaped access makes every group parcel reachable, the shortest such
   access is chosen. Otherwise L-shaped accesses (two collinear rays sharing a
   junction corner) are enumerated and the shortest complete one is chosen. If
   no single access reaches all parcels, the access reaching the most parcels is
   chosen and the algorithm recurses on the remaining parcels (paper Fig. 9
   case #5).
3. Connect the chosen access to the existing street network with a weighted
   shortest path (Dijkstra) in the parcel corner graph. Each edge weight is
   ``length_weight * edge_length + junction_weight * (1 if the included angle
   with the previous edge is smaller than 135 degrees else 0)``, with the
   junction component weighted higher.
4. Optionally avoid cul-de-sacs by connecting each street end back to the
   network with the same weighted shortest path.

The primary entry point is :func:`extend_street_network`. The legacy
:func:`generate_street_network` is a thin deprecated wrapper that seeds the
boundary ring and calls :func:`extend_street_network` once.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field
from typing import Any

from mapgen.chen_core import (
    CHEN_COLLINEAR_THRESHOLD_RAD,
    ChenLayout,
    EdgeKey,
    MeshVertex,
    ParcelCornerGraph,
    ParcelMesh,
    StreetNetworkGraph,
    connected_component_count,
    edge_path_length,
    normalized_edge,
    parcel_corner_graph,
    ring_edges,
)


@dataclass(frozen=True)
class StreetConfig:
    """Configuration for Chen Section 4.2 street extension.

    The Dijkstra connection weight is the paper's two-component form:
    ``length_weight * edge_length + junction_weight * junction_indicator`` where
    the junction indicator is 1 when the included angle with the previous edge is
    smaller than the collinear threshold (135 degrees). ``junction_weight`` is
    deliberately the dominant term, matching "we give the second component a
    larger weight" in the paper.
    """

    collinear_angle_threshold_rad: float = CHEN_COLLINEAR_THRESHOLD_RAD
    length_weight: float = 1.0
    junction_weight: float = 10.0
    avoid_cul_de_sacs: bool = False


# Backwards-compatible alias for the legacy configuration name. The legacy
# wrapper keeps these extra fields for callers that still construct the old
# config; only the fields above influence the paper algorithm.
@dataclass(frozen=True)
class StreetGenerationConfig:
    """Deprecated configuration retained for the legacy wrapper.

    Prefer :class:`StreetConfig`. Only ``collinear_angle_threshold_rad``,
    ``junction_penalty`` (mapped to ``junction_weight``), and
    ``avoid_cul_de_sacs`` are honoured; the remaining fields are inert and kept
    so old call sites keep importing.
    """

    selection_strategy: str = "section_4_2_connected_junctions"
    collinear_angle_threshold_rad: float = CHEN_COLLINEAR_THRESHOLD_RAD
    length_weight: float = 1.0
    junction_penalty: float = 10.0
    avoid_cul_de_sacs: bool = False

    def to_street_config(self) -> StreetConfig:
        return StreetConfig(
            collinear_angle_threshold_rad=self.collinear_angle_threshold_rad,
            length_weight=self.length_weight,
            junction_weight=self.junction_penalty,
            avoid_cul_de_sacs=self.avoid_cul_de_sacs,
        )


DEFAULT_STREET_CONFIG = StreetConfig()


@dataclass(frozen=True)
class StreetAccessCandidate:
    """A candidate I/L-shaped street access for a group of parcels."""

    kind: str
    edges: frozenset[EdgeKey]
    reached_parcels: frozenset[int]
    length: float
    connection_edges: tuple[EdgeKey, ...] = ()
    connection_cost: float = 0.0


@dataclass(frozen=True)
class StreetExtensionResult:
    """Result of one :func:`extend_street_network` call."""

    added_edges: set[EdgeKey]
    diagnostics: dict[str, Any]
    selected_access_candidates: tuple[StreetAccessCandidate, ...] = ()
    evaluated_candidates: tuple[StreetAccessCandidate, ...] = ()


@dataclass(frozen=True)
class _StreetContext:
    mesh: ParcelMesh | None
    vertices: dict[int, MeshVertex]
    corner_graph: ParcelCornerGraph
    parcel_corner_rings: dict[int, tuple[int, ...]]
    edge_parcels: dict[EdgeKey, frozenset[int]]
    parcel_neighbors: dict[int, set[int]]
    corner_adjacency: dict[int, set[int]]
    all_edges: frozenset[EdgeKey]


def _empty_diagnostics() -> dict[str, Any]:
    return {
        "group_count": 0,
        "i_chosen_count": 0,
        "l_chosen_count": 0,
        "recursed_count": 0,
        "dijkstra_connection_count": 0,
        "dijkstra_connection_failure_count": 0,
        "cul_de_sacs_repaired_count": 0,
    }


def extend_street_network(
    source: ChenLayout | ParcelMesh | ParcelCornerGraph,
    existing_street_edges: set[EdgeKey],
    config: StreetConfig = DEFAULT_STREET_CONFIG,
) -> StreetExtensionResult:
    """Extend ``existing_street_edges`` so every parcel becomes reachable.

    ``source`` provides the parcel corner graph (and mesh geometry, if a
    :class:`ChenLayout`/:class:`ParcelMesh` is given). ``existing_street_edges``
    is the street edge set at the current hierarchical level. The returned
    :class:`StreetExtensionResult` holds the edges that were added (not including
    the input edges) and small count diagnostics.

    Calling this again with the grown edge set is a no-op (no added edges) when
    all parcels are already reachable.
    """
    ctx = _street_context(source)
    street_edges = {normalized_edge(*edge) for edge in existing_street_edges}
    unknown = street_edges - ctx.all_edges
    if unknown:
        raise ValueError(
            f"existing street edges are not in the parcel corner graph: {unknown}"
        )

    initial_edges = frozenset(street_edges)
    diagnostics = _empty_diagnostics()
    selected: list[StreetAccessCandidate] = []
    evaluated: list[StreetAccessCandidate] = []

    # Iterate level-locally until no unreachable group remains. Each outer pass
    # handles one connected group; recursion inside a group (Fig. 9 case #5) is
    # handled by re-grouping the still-unreachable members on the next pass.
    safety_limit = max(len(ctx.parcel_corner_rings) * 4, 1)
    for _ in range(safety_limit):
        unreachable = _unreachable_parcels(ctx, street_edges)
        if not unreachable:
            break
        groups = _unreachable_groups(ctx, unreachable)
        if not groups:
            break
        group = groups[0]
        diagnostics["group_count"] += 1
        _extend_for_group(
            ctx, group, street_edges, config, diagnostics, selected, evaluated
        )

    added_edges = set(street_edges) - initial_edges

    if config.avoid_cul_de_sacs:
        repaired = _avoid_cul_de_sacs(ctx, street_edges, config)
        diagnostics["cul_de_sacs_repaired_count"] = repaired
        added_edges = set(street_edges) - initial_edges

    unreachable_after = _unreachable_parcels(ctx, street_edges)
    diagnostics.update(
        {
            "parcel_count": int(len(ctx.parcel_corner_rings)),
            "added_edge_count": int(len(added_edges)),
            "street_network_edge_count": int(len(street_edges)),
            "unreachable_parcel_count_after": int(len(unreachable_after)),
            "street_graph_component_count": int(
                connected_component_count(StreetNetworkGraph(street_edges))
            ),
            "street_network_subset_of_corner_graph": bool(
                street_edges <= set(ctx.all_edges)
            ),
        }
    )

    return StreetExtensionResult(
        added_edges=added_edges,
        diagnostics=diagnostics,
        selected_access_candidates=tuple(selected),
        evaluated_candidates=tuple(evaluated),
    )


def _extend_for_group(
    ctx: _StreetContext,
    group: frozenset[int],
    street_edges: set[EdgeKey],
    config: StreetConfig,
    diagnostics: dict[str, Any],
    selected: list[StreetAccessCandidate],
    evaluated: list[StreetAccessCandidate],
) -> None:
    """Choose and apply street access(es) that make ``group`` reachable.

    Implements the paper's recurse-on-remainder rule (Fig. 9 case #5) as an
    in-group loop: pick the shortest complete I- (then L-) access if it exists,
    otherwise the access reaching the most parcels and repeat on the remainder.
    """
    remaining = set(group)
    is_recursion = False
    safety_limit = max(len(group) * 2, 1)
    for _ in range(safety_limit):
        remaining = {
            parcel_id
            for parcel_id in remaining
            if not _parcel_reachable(ctx, parcel_id, street_edges)
        }
        if not remaining:
            break
        remaining_frozen = frozenset(remaining)
        candidates = _access_candidates(ctx, remaining_frozen, street_edges, config)
        evaluated.extend(candidates)
        if not candidates:
            diagnostics["dijkstra_connection_failure_count"] += 1
            break

        complete = [c for c in candidates if c.reached_parcels >= remaining_frozen]
        if complete:
            chosen = min(complete, key=lambda c: _candidate_key(c, remaining_frozen))
        else:
            chosen = min(candidates, key=lambda c: _candidate_key(c, remaining_frozen))

        if is_recursion:
            diagnostics["recursed_count"] += 1
        if chosen.kind == "I":
            diagnostics["i_chosen_count"] += 1
        else:
            diagnostics["l_chosen_count"] += 1

        connection_edges, connection_cost = _connect_access_to_street(
            ctx, set(chosen.edges), street_edges, config
        )
        access_nodes = _edge_nodes(chosen.edges)
        street_nodes = _edge_nodes(street_edges)
        if street_edges and not (access_nodes & street_nodes) and not connection_edges:
            diagnostics["dijkstra_connection_failure_count"] += 1
        elif connection_edges:
            diagnostics["dijkstra_connection_count"] += 1

        selected.append(
            StreetAccessCandidate(
                kind=chosen.kind,
                edges=chosen.edges,
                reached_parcels=chosen.reached_parcels,
                length=chosen.length,
                connection_edges=tuple(sorted(connection_edges)),
                connection_cost=connection_cost,
            )
        )
        before = len(street_edges)
        street_edges.update(chosen.edges)
        street_edges.update(connection_edges)
        if len(street_edges) == before:
            # No progress is possible; bail to avoid an infinite loop.
            break
        is_recursion = True


def _street_context(
    source: ChenLayout | ParcelMesh | ParcelCornerGraph,
) -> _StreetContext:
    if isinstance(source, ChenLayout):
        mesh: ParcelMesh | None = source.mesh
        corner = source.corner_graph
    elif isinstance(source, ParcelMesh):
        mesh = source
        corner = parcel_corner_graph(source)
    elif isinstance(source, ParcelCornerGraph):
        mesh = None
        corner = source
    else:
        raise TypeError(
            "extend_street_network expects a ChenLayout, ParcelMesh, "
            "or ParcelCornerGraph"
        )

    edge_parcels = _edge_parcel_index(corner)
    parcel_neighbors: dict[int, set[int]] = {
        parcel_id: set() for parcel_id in corner.parcel_corner_rings
    }
    for parcel_ids in edge_parcels.values():
        if len(parcel_ids) != 2:
            continue
        a, b = sorted(parcel_ids)
        parcel_neighbors[a].add(b)
        parcel_neighbors[b].add(a)

    return _StreetContext(
        mesh=mesh,
        vertices=corner.vertices if mesh is None else mesh.vertices,
        corner_graph=corner,
        parcel_corner_rings=corner.parcel_corner_rings,
        edge_parcels=edge_parcels,
        parcel_neighbors=parcel_neighbors,
        corner_adjacency=_edge_adjacency(corner.edges),
        all_edges=frozenset(normalized_edge(*edge) for edge in corner.edges),
    )


def _edge_parcel_index(corner: ParcelCornerGraph) -> dict[EdgeKey, frozenset[int]]:
    by_edge: dict[EdgeKey, set[int]] = {
        normalized_edge(*edge): set() for edge in corner.edges
    }
    for parcel_id, ring in corner.parcel_corner_rings.items():
        for edge in ring_edges(ring):
            normalized = normalized_edge(*edge)
            if normalized in by_edge:
                by_edge[normalized].add(parcel_id)
    return {edge: frozenset(parcel_ids) for edge, parcel_ids in by_edge.items()}


def _edge_adjacency(edges: set[EdgeKey] | frozenset[EdgeKey]) -> dict[int, set[int]]:
    adjacency: dict[int, set[int]] = {}
    for a, b in edges:
        adjacency.setdefault(a, set()).add(b)
        adjacency.setdefault(b, set()).add(a)
    return adjacency


def boundary_ring_seed_edges(
    source: ChenLayout | ParcelMesh | ParcelCornerGraph,
) -> set[EdgeKey]:
    """Boundary ring edges used to seed a level-0 street network.

    These are corner-graph edges incident to only one parcel (the perimeter of
    the parcel mesh). Chen seeds level 0 with the boundary ring; the legacy
    wrapper uses this so the single-shot pipeline keeps a connected seed.
    """
    ctx = _street_context(source)
    return {
        edge for edge, parcel_ids in ctx.edge_parcels.items() if len(parcel_ids) == 1
    }


def _parcel_reachable(
    ctx: _StreetContext, parcel_id: int, street_edges: set[EdgeKey]
) -> bool:
    return any(
        edge in street_edges for edge in ring_edges(ctx.parcel_corner_rings[parcel_id])
    )


def _unreachable_parcels(
    ctx: _StreetContext, street_edges: set[EdgeKey]
) -> frozenset[int]:
    return frozenset(
        parcel_id
        for parcel_id in ctx.parcel_corner_rings
        if not _parcel_reachable(ctx, parcel_id, street_edges)
    )


def _unreachable_groups(
    ctx: _StreetContext, unreachable: frozenset[int]
) -> list[frozenset[int]]:
    groups: list[frozenset[int]] = []
    seen: set[int] = set()
    for start in sorted(unreachable):
        if start in seen:
            continue
        stack = [start]
        group: set[int] = set()
        seen.add(start)
        while stack:
            parcel_id = stack.pop()
            group.add(parcel_id)
            for nxt in sorted(ctx.parcel_neighbors.get(parcel_id, set()) & unreachable):
                if nxt in seen:
                    continue
                seen.add(nxt)
                stack.append(nxt)
        groups.append(frozenset(group))
    return groups


def _access_candidates(
    ctx: _StreetContext,
    group: frozenset[int],
    street_edges: set[EdgeKey],
    config: StreetConfig,
) -> tuple[StreetAccessCandidate, ...]:
    """Enumerate I-shaped then L-shaped accesses for ``group``.

    I-shaped accesses are collinear chains seeded from each group-boundary edge
    (paper: for each corner, an I-shaped access per incident edge). L-shaped
    accesses are pairs of collinear rays that meet at a junction corner (included
    angle <= 135 degrees) on the group boundary.
    """
    group_edges = _group_edges(ctx, group) - street_edges
    group_adjacency = _edge_adjacency(group_edges)
    seen: set[tuple[str, tuple[EdgeKey, ...]]] = set()
    candidates: list[StreetAccessCandidate] = []

    def add(kind: str, edges: set[EdgeKey]) -> None:
        edges = edges & group_edges
        if not edges:
            return
        key = (kind, tuple(sorted(edges)))
        if key in seen:
            return
        reached = _candidate_reaches(ctx, edges, group)
        if not reached:
            return
        seen.add(key)
        candidates.append(
            StreetAccessCandidate(
                kind=kind,
                edges=frozenset(edges),
                reached_parcels=frozenset(reached),
                length=_edge_set_length(ctx, edges),
            )
        )

    for edge in sorted(group_edges):
        add("I", _extend_collinear_chain(ctx, group_adjacency, edge, config))

    threshold = config.collinear_angle_threshold_rad
    for joint in sorted(group_adjacency):
        neighbors = sorted(group_adjacency[joint])
        for i, left in enumerate(neighbors):
            for right in neighbors[i + 1 :]:
                # A junction corner: the two edges are NOT colinear.
                if _included_angle(ctx, left, joint, right) > threshold:
                    continue
                left_ray = _extend_collinear_ray(
                    ctx, group_adjacency, joint, left, config
                )
                right_ray = _extend_collinear_ray(
                    ctx, group_adjacency, joint, right, config
                )
                add("L", left_ray | right_ray)

    return tuple(sorted(candidates, key=_candidate_stable_key))


def _group_edges(ctx: _StreetContext, parcel_ids: frozenset[int]) -> set[EdgeKey]:
    edges: set[EdgeKey] = set()
    for parcel_id in parcel_ids:
        edges.update(ring_edges(ctx.parcel_corner_rings[parcel_id]))
    return {edge for edge in edges if edge in ctx.all_edges}


def _candidate_reaches(
    ctx: _StreetContext, edges: set[EdgeKey], parcel_ids: frozenset[int]
) -> set[int]:
    return {
        parcel_id
        for parcel_id in parcel_ids
        if any(edge in edges for edge in ring_edges(ctx.parcel_corner_rings[parcel_id]))
    }


def _candidate_key(
    candidate: StreetAccessCandidate, group: frozenset[int]
) -> tuple[int, int, int, float, tuple[EdgeKey, ...]]:
    """Selection order: complete first, then I over L, then most parcels, then
    shortest, then deterministic edge order."""
    complete_rank = 0 if candidate.reached_parcels >= group else 1
    kind_rank = 0 if candidate.kind == "I" else 1
    return (
        complete_rank,
        kind_rank,
        -len(candidate.reached_parcels),
        candidate.length,
        tuple(sorted(candidate.edges)),
    )


def _candidate_stable_key(
    candidate: StreetAccessCandidate,
) -> tuple[str, tuple[EdgeKey, ...]]:
    return candidate.kind, tuple(sorted(candidate.edges))


def _extend_collinear_chain(
    ctx: _StreetContext,
    adjacency: dict[int, set[int]],
    edge: EdgeKey,
    config: StreetConfig,
) -> set[EdgeKey]:
    a, b = edge
    nodes = [a, b]
    used = {normalized_edge(a, b)}
    _extend_nodes(ctx, adjacency, nodes, used, forward=True, config=config)
    _extend_nodes(ctx, adjacency, nodes, used, forward=False, config=config)
    return {
        normalized_edge(start, end)
        for start, end in zip(nodes, nodes[1:], strict=False)
    }


def _extend_collinear_ray(
    ctx: _StreetContext,
    adjacency: dict[int, set[int]],
    joint: int,
    neighbor: int,
    config: StreetConfig,
) -> set[EdgeKey]:
    nodes = [joint, neighbor]
    used = {normalized_edge(joint, neighbor)}
    _extend_nodes(ctx, adjacency, nodes, used, forward=True, config=config)
    return {
        normalized_edge(start, end)
        for start, end in zip(nodes, nodes[1:], strict=False)
    }


def _extend_nodes(
    ctx: _StreetContext,
    adjacency: dict[int, set[int]],
    nodes: list[int],
    used: set[EdgeKey],
    *,
    forward: bool,
    config: StreetConfig,
) -> None:
    while True:
        prev = nodes[-2] if forward else nodes[1]
        node = nodes[-1] if forward else nodes[0]
        nxt = _next_collinear_node(ctx, adjacency, used, prev, node, config)
        if nxt is None:
            return
        used.add(normalized_edge(node, nxt))
        if forward:
            nodes.append(nxt)
        else:
            nodes.insert(0, nxt)


def _next_collinear_node(
    ctx: _StreetContext,
    adjacency: dict[int, set[int]],
    used: set[EdgeKey],
    prev: int,
    node: int,
    config: StreetConfig,
) -> int | None:
    threshold = config.collinear_angle_threshold_rad
    candidates = [
        nxt
        for nxt in adjacency.get(node, set())
        if nxt != prev
        and normalized_edge(node, nxt) not in used
        and _included_angle(ctx, prev, node, nxt) > threshold
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda nxt: (
            -_included_angle(ctx, prev, node, nxt),
            _edge_length(ctx, normalized_edge(node, nxt)),
            nxt,
        ),
    )


def _connect_access_to_street(
    ctx: _StreetContext,
    access_edges: set[EdgeKey],
    street_edges: set[EdgeKey],
    config: StreetConfig,
) -> tuple[set[EdgeKey], float]:
    """Connect the access subgraph to the existing street network (paper step 3).

    Returns the new edges of the weighted shortest path and its cost. If the
    access already touches the network, or there is no network yet, no
    connection is needed.
    """
    access_nodes = _edge_nodes(access_edges)
    street_nodes = _edge_nodes(street_edges)
    if not access_nodes or not street_nodes or access_nodes & street_nodes:
        return set(), 0.0

    path, cost = _shortest_path_between_node_sets(
        ctx, access_nodes, street_nodes, config
    )
    if not path:
        return set(), math.inf
    path_edges = {normalized_edge(a, b) for a, b in zip(path, path[1:], strict=False)}
    return path_edges - access_edges - street_edges, cost


def _edge_weight(
    ctx: _StreetContext,
    prev: int | None,
    node: int,
    nxt: int,
    config: StreetConfig,
) -> tuple[float, float]:
    """Paper Dijkstra edge weight (cost, geometric length).

    cost = length_weight * length + junction_weight * (1 if the included angle
    with the previous edge is smaller than 135 degrees else 0).
    """
    edge = normalized_edge(node, nxt)
    length = _edge_length(ctx, edge)
    cost = config.length_weight * length
    if (
        prev is not None
        and _included_angle(ctx, prev, node, nxt) < config.collinear_angle_threshold_rad
    ):
        cost += config.junction_weight
    return cost, length


def _shortest_path_between_node_sets(
    ctx: _StreetContext,
    start_nodes: set[int],
    target_nodes: set[int],
    config: StreetConfig,
) -> tuple[tuple[int, ...], float]:
    heap: list[tuple[float, float, tuple[int, ...], int | None, int]] = []
    best: dict[tuple[int | None, int], float] = {}
    for start in sorted(start_nodes):
        state = (None, start)
        best[state] = 0.0
        heapq.heappush(heap, (0.0, 0.0, (start,), None, start))

    while heap:
        cost, length, path, prev, node = heapq.heappop(heap)
        if cost > best.get((prev, node), math.inf) + 1e-12:
            continue
        if node in target_nodes and prev is not None:
            return path, cost
        for nxt in sorted(ctx.corner_adjacency.get(node, set())):
            if nxt == prev or nxt in path:
                continue
            step_cost, step_length = _edge_weight(ctx, prev, node, nxt, config)
            new_cost = cost + step_cost
            new_length = length + step_length
            state = (node, nxt)
            if new_cost >= best.get(state, math.inf) - 1e-12:
                continue
            best[state] = new_cost
            heapq.heappush(heap, (new_cost, new_length, (*path, nxt), node, nxt))
    return (), math.inf


def _avoid_cul_de_sacs(
    ctx: _StreetContext,
    street_edges: set[EdgeKey],
    config: StreetConfig,
) -> int:
    """Connect each street end back to the network (paper step 4, optional).

    A street end is a degree-1 node in the street graph. Each is connected to the
    rest of the network via the same weighted shortest path. Returns the number
    of cul-de-sacs successfully repaired.
    """
    repaired = 0
    safety_limit = max(len(ctx.corner_adjacency), 1)
    for _ in range(safety_limit):
        adjacency = _edge_adjacency(street_edges)
        leaves = sorted(
            node for node, neighbors in adjacency.items() if len(neighbors) == 1
        )
        if not leaves:
            break
        progressed = False
        for leaf in leaves:
            target_nodes = _edge_nodes(street_edges) - {leaf}
            if not target_nodes:
                continue
            path, _cost = _shortest_path_from_leaf(
                ctx, leaf, target_nodes, street_edges, config
            )
            if not path:
                continue
            new_edges = {
                normalized_edge(a, b) for a, b in zip(path, path[1:], strict=False)
            } - street_edges
            if not new_edges:
                continue
            street_edges.update(new_edges)
            repaired += 1
            progressed = True
        if not progressed:
            break
    return repaired


def _shortest_path_from_leaf(
    ctx: _StreetContext,
    leaf: int,
    target_nodes: set[int],
    street_edges: set[EdgeKey],
    config: StreetConfig,
) -> tuple[tuple[int, ...], float]:
    """Weighted shortest path from a cul-de-sac end back to the network.

    Uses the same paper weight as :func:`_shortest_path_between_node_sets` but
    only traverses corner edges not already in the street network (so the new
    connection genuinely closes the loop).
    """
    heap: list[tuple[float, float, tuple[int, ...], int | None, int]] = [
        (0.0, 0.0, (leaf,), None, leaf)
    ]
    best: dict[tuple[int | None, int], float] = {(None, leaf): 0.0}
    while heap:
        cost, length, path, prev, node = heapq.heappop(heap)
        if cost > best.get((prev, node), math.inf) + 1e-12:
            continue
        if node in target_nodes and len(path) > 1:
            return path, cost
        for nxt in sorted(ctx.corner_adjacency.get(node, set())):
            if nxt == prev or nxt in path:
                continue
            if normalized_edge(node, nxt) in street_edges:
                continue
            step_cost, step_length = _edge_weight(ctx, prev, node, nxt, config)
            new_cost = cost + step_cost
            state = (node, nxt)
            if new_cost >= best.get(state, math.inf) - 1e-12:
                continue
            best[state] = new_cost
            heapq.heappush(
                heap, (new_cost, length + step_length, (*path, nxt), node, nxt)
            )
    return (), math.inf


def _included_angle(ctx: _StreetContext, prev: int, node: int, nxt: int) -> float:
    p = ctx.vertices[node].point
    a = ctx.vertices[prev].point
    b = ctx.vertices[nxt].point
    a0 = math.atan2(a[1] - p[1], a[0] - p[0])
    a1 = math.atan2(b[1] - p[1], b[0] - p[0])
    return abs(math.atan2(math.sin(a1 - a0), math.cos(a1 - a0)))


def _edge_length(ctx: _StreetContext, edge: EdgeKey) -> float:
    normalized = normalized_edge(*edge)
    if ctx.mesh is not None and normalized in ctx.corner_graph.edge_paths:
        return edge_path_length(ctx.mesh, ctx.corner_graph, normalized)
    a, b = normalized
    pa = ctx.vertices[a].point
    pb = ctx.vertices[b].point
    return math.hypot(pb[0] - pa[0], pb[1] - pa[1])


def _edge_set_length(ctx: _StreetContext, edges: set[EdgeKey]) -> float:
    return sum(_edge_length(ctx, edge) for edge in edges)


def _edge_nodes(edges: set[EdgeKey] | frozenset[EdgeKey]) -> set[int]:
    return {node for edge in edges for node in edge}


# ---------------------------------------------------------------------------
# Deprecated legacy wrapper kept until the hierarchical driver (slice D) calls
# ``extend_street_network`` per level directly.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StreetGenerationResult:
    """Deprecated single-shot result. Prefer :class:`StreetExtensionResult`."""

    street_edges: frozenset[EdgeKey]
    seed_edges: frozenset[EdgeKey]
    selected_access_candidates: tuple[StreetAccessCandidate, ...]
    evaluated_candidates: tuple[StreetAccessCandidate, ...]
    diagnostics: dict[str, Any] = field(default_factory=dict)


def generate_street_network(
    layout_or_mesh: ChenLayout | ParcelMesh | ParcelCornerGraph,
    *,
    seed_edges: set[EdgeKey] | None = None,
    config: StreetGenerationConfig | StreetConfig | None = None,
) -> StreetGenerationResult:
    """Deprecated: seed the boundary ring and run one street extension.

    This preserves the pre-slice-D single-shot entry point that builds an entire
    level-0 street network from a finished mesh. Prefer
    :func:`extend_street_network`, called once per hierarchical level with the
    street edges that already exist at that level. This wrapper exists only so
    the legacy generation pipeline stays green until the driver is rewritten.
    """
    if config is None:
        street_config = DEFAULT_STREET_CONFIG
    elif isinstance(config, StreetGenerationConfig):
        street_config = config.to_street_config()
    else:
        street_config = config

    if seed_edges is None:
        seed = boundary_ring_seed_edges(layout_or_mesh)
    else:
        seed = {normalized_edge(*edge) for edge in seed_edges}

    result = extend_street_network(layout_or_mesh, set(seed), street_config)
    street_edges = set(seed) | result.added_edges

    return StreetGenerationResult(
        street_edges=frozenset(street_edges),
        seed_edges=frozenset(seed),
        selected_access_candidates=result.selected_access_candidates,
        evaluated_candidates=result.evaluated_candidates,
        diagnostics=result.diagnostics,
    )
