"""Chen-style street network selection over parcel corner graphs.

This keeps streets as a connected subgraph of the parcel corner graph, starts
from perimeter frontage, repairs unreachable parcel groups with I/L-shaped
access candidates, adds bounded connected paths that improve useful junction
structure, and can optionally remove or repair avoidable cul-de-sacs.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass
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
    street_adjacency,
)


@dataclass(frozen=True)
class StreetGenerationConfig:
    selection_strategy: str = "section_4_2_connected_junctions"
    collinear_angle_threshold_rad: float = CHEN_COLLINEAR_THRESHOLD_RAD
    l_turn_min_angle_rad: float = math.radians(45.0)
    junction_angle_threshold_rad: float = CHEN_COLLINEAR_THRESHOLD_RAD
    junction_penalty: float = 10.0
    avoid_cul_de_sacs: bool = False
    cul_de_sac_repair_max_path_edges: int = 12
    cul_de_sac_repair_max_added_edges: int | None = None
    cul_de_sac_repair_max_added_edge_ratio: float = 0.10
    complete_junctions: bool = True
    junction_completion_target_degree: int = 4
    junction_completion_min_corner_degree: int = 4
    junction_completion_min_street_degree: int = 2
    junction_completion_max_path_edges: int = 12
    junction_completion_max_added_edges: int | None = None
    junction_completion_max_added_edge_ratio: float = 0.20
    max_repair_iterations: int | None = None


@dataclass(frozen=True)
class StreetAccessCandidate:
    kind: str
    edges: frozenset[EdgeKey]
    reached_parcels: frozenset[int]
    length: float
    connection_edges: tuple[EdgeKey, ...] = ()
    connection_cost: float = 0.0


@dataclass(frozen=True)
class StreetGenerationResult:
    street_edges: frozenset[EdgeKey]
    seed_edges: frozenset[EdgeKey]
    selected_access_candidates: tuple[StreetAccessCandidate, ...]
    evaluated_candidates: tuple[StreetAccessCandidate, ...]
    diagnostics: dict[str, Any]


DEFAULT_STREET_GENERATION_CONFIG = StreetGenerationConfig()


@dataclass(frozen=True)
class _StreetContext:
    mesh: ParcelMesh | None
    vertices: dict[int, MeshVertex]
    corner_graph: ParcelCornerGraph
    parcel_corner_rings: dict[int, tuple[int, ...]]
    edge_parcels: dict[EdgeKey, frozenset[int]]
    parcel_neighbors: dict[int, set[int]]
    corner_adjacency: dict[int, set[int]]


@dataclass(frozen=True)
class _JunctionCompletionCandidate:
    edges: tuple[EdgeKey, ...]
    start: int
    end: int
    length: float
    parcel_touch_count: int
    four_way_gain: int
    progress_gain: int
    dead_end_delta: int


@dataclass(frozen=True)
class _CulDeSacRepairCandidate:
    edges: tuple[EdgeKey, ...]
    leaf: int
    end: int
    length: float
    parcel_touch_count: int
    dead_end_delta: int


def generate_street_network(
    layout_or_mesh: ChenLayout | ParcelMesh | ParcelCornerGraph,
    *,
    seed_edges: set[EdgeKey] | None = None,
    config: StreetGenerationConfig = DEFAULT_STREET_GENERATION_CONFIG,
) -> StreetGenerationResult:
    """Select a connected Chen street subgraph over the parcel corner graph."""
    if config.selection_strategy != "section_4_2_connected_junctions":
        raise ValueError(
            f"unsupported street selection strategy: {config.selection_strategy!r}"
        )

    ctx = _street_context(layout_or_mesh)
    all_edges = {normalized_edge(*edge) for edge in ctx.corner_graph.edges}
    if seed_edges is None:
        seed = _perimeter_seed_edges(ctx)
    else:
        seed = {normalized_edge(*edge) for edge in seed_edges}
        unknown = seed - all_edges
        if unknown:
            raise ValueError(
                f"seed edges are not in the parcel corner graph: {unknown}"
            )

    street_edges = set(seed)
    reachable_before = _reachable_parcels(ctx, street_edges)
    unreachable_before = frozenset(ctx.parcel_corner_rings) - reachable_before

    diagnostics: dict[str, Any] = {
        "street_selection_strategy": config.selection_strategy,
        "parcel_count": int(len(ctx.parcel_corner_rings)),
        "seed_edge_count": int(len(seed)),
        "reachable_parcel_count_before": int(len(reachable_before)),
        "unreachable_parcel_count_before": int(len(unreachable_before)),
        "unreachable_parcel_ids_before": tuple(sorted(unreachable_before)),
        "i_candidate_count": 0,
        "l_candidate_count": 0,
        "dijkstra_connection_count": 0,
        "dijkstra_connection_failure_count": 0,
        "selected_access_edge_count": 0,
        "selected_access_candidate_count": 0,
        "unreachable_group_count": 0,
        "unreachable_group_max_size": 0,
        "cul_de_sac_avoidance_enabled": bool(config.avoid_cul_de_sacs),
        "cul_de_sac_avoidance_applied": False,
        "cul_de_sac_avoidance_changed_network": False,
        "cul_de_sac_pruned_edge_count": 0,
        "cul_de_sac_prune_attempt_count": 0,
        "cul_de_sac_repair_attempt_count": 0,
        "cul_de_sac_repair_candidate_count": 0,
        "cul_de_sac_repair_success_count": 0,
        "cul_de_sac_repair_added_edge_count": 0,
        "cul_de_sac_repair_budget_edge_count": 0,
        "junction_completion_enabled": bool(config.complete_junctions),
        "junction_completion_applied": False,
        "junction_completion_candidate_count": 0,
        "junction_completion_selected_path_count": 0,
        "junction_completion_added_edge_count": 0,
        "junction_completion_parcel_touch_count": 0,
        "junction_completion_budget_edge_count": 0,
    }

    selected_candidates: list[StreetAccessCandidate] = []
    evaluated_candidates: list[StreetAccessCandidate] = []
    repair_limit = config.max_repair_iterations or max(
        len(ctx.parcel_corner_rings) * 4, 1
    )

    for _iteration in range(repair_limit):
        unreachable = frozenset(ctx.parcel_corner_rings) - _reachable_parcels(
            ctx, street_edges
        )
        if not unreachable:
            break

        groups = _unreachable_groups(ctx, unreachable)
        if not groups:
            break
        group = groups[0]
        diagnostics["unreachable_group_count"] += 1
        diagnostics["unreachable_group_max_size"] = max(
            diagnostics["unreachable_group_max_size"], len(group)
        )

        candidates = _access_candidates(ctx, group, street_edges, config)
        evaluated_candidates.extend(candidates)
        diagnostics["i_candidate_count"] += sum(1 for c in candidates if c.kind == "I")
        diagnostics["l_candidate_count"] += sum(1 for c in candidates if c.kind == "L")
        if not candidates:
            diagnostics["dijkstra_connection_failure_count"] += 1
            break

        before_unreachable_count = len(unreachable)
        before_edge_count = len(street_edges)
        chosen = min(candidates, key=lambda c: _candidate_selection_key(c, group))
        connection_edges, connection_cost = _connect_access_to_street(
            ctx, set(chosen.edges), street_edges, config
        )
        if (
            street_edges
            and not connection_edges
            and not (_edge_nodes(chosen.edges) & _edge_nodes(street_edges))
        ):
            diagnostics["dijkstra_connection_failure_count"] += 1
        elif connection_edges:
            diagnostics["dijkstra_connection_count"] += 1

        selected = StreetAccessCandidate(
            kind=chosen.kind,
            edges=chosen.edges,
            reached_parcels=chosen.reached_parcels,
            length=chosen.length,
            connection_edges=tuple(sorted(connection_edges)),
            connection_cost=connection_cost,
        )
        selected_candidates.append(selected)
        street_edges.update(chosen.edges)
        street_edges.update(connection_edges)
        diagnostics["selected_access_edge_count"] += len(chosen.edges)
        diagnostics["selected_access_candidate_count"] += 1

        after_unreachable_count = len(
            frozenset(ctx.parcel_corner_rings) - _reachable_parcels(ctx, street_edges)
        )
        if (
            after_unreachable_count >= before_unreachable_count
            and len(street_edges) == before_edge_count
        ):
            break

    reachability_edges = set(street_edges)
    reachability_degree_diagnostics = _street_degree_diagnostics(street_edges)
    diagnostics.update(
        {
            "reachability_repair_edge_count": int(len(reachability_edges) - len(seed)),
            "reachability_repair_dead_end_count": int(
                reachability_degree_diagnostics["dead_end_node_count"]
            ),
            "reachability_repair_junction_count": int(
                reachability_degree_diagnostics["junction_count"]
            ),
            "reachability_repair_four_way_intersection_count": int(
                reachability_degree_diagnostics["four_way_intersection_count"]
            ),
        }
    )

    completion_diagnostics = _complete_junctions(ctx, street_edges, config)
    diagnostics.update(completion_diagnostics)

    diagnostics["cul_de_sac_count_before_avoidance"] = int(
        _dead_end_count(street_edges)
    )
    if config.avoid_cul_de_sacs:
        avoidance_diagnostics = _avoid_cul_de_sacs_preserving_access_and_junctions(
            ctx, street_edges, config
        )
        diagnostics.update(avoidance_diagnostics)
    diagnostics["cul_de_sac_count_after_avoidance"] = int(_dead_end_count(street_edges))

    reachable_after = _reachable_parcels(ctx, street_edges)
    unreachable_after = frozenset(ctx.parcel_corner_rings) - reachable_after
    component_count = connected_component_count(StreetNetworkGraph(street_edges))
    final_degree_diagnostics = _street_degree_diagnostics(street_edges)
    diagnostics.update(
        {
            "reachable_parcel_count_after": int(len(reachable_after)),
            "unreachable_parcel_count_after": int(len(unreachable_after)),
            "unreachable_parcel_ids_after": tuple(sorted(unreachable_after)),
            "street_network_edge_count": int(len(street_edges)),
            "street_graph_component_count": int(component_count),
            "cul_de_sac_count": int(_dead_end_count(street_edges)),
            "street_network_subset_of_corner_graph": bool(street_edges <= all_edges),
            **{
                f"street_{key}": value
                for key, value in final_degree_diagnostics.items()
            },
        }
    )

    return StreetGenerationResult(
        street_edges=frozenset(street_edges),
        seed_edges=frozenset(seed),
        selected_access_candidates=tuple(selected_candidates),
        evaluated_candidates=tuple(evaluated_candidates),
        diagnostics=diagnostics,
    )


def _street_context(
    layout_or_mesh: ChenLayout | ParcelMesh | ParcelCornerGraph,
) -> _StreetContext:
    if isinstance(layout_or_mesh, ChenLayout):
        mesh = layout_or_mesh.mesh
        corner = layout_or_mesh.corner_graph
    elif isinstance(layout_or_mesh, ParcelMesh):
        mesh = layout_or_mesh
        corner = parcel_corner_graph(mesh)
    elif isinstance(layout_or_mesh, ParcelCornerGraph):
        mesh = None
        corner = layout_or_mesh
    else:
        raise TypeError(
            "generate_street_network expects a ChenLayout, ParcelMesh, "
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


def _perimeter_seed_edges(ctx: _StreetContext) -> set[EdgeKey]:
    seed = {
        edge for edge, parcel_ids in ctx.edge_parcels.items() if len(parcel_ids) == 1
    }
    seed.update(
        edge
        for edge in ctx.corner_graph.edges
        if ctx.vertices[edge[0]].on_boundary and ctx.vertices[edge[1]].on_boundary
    )
    return seed


def _reachable_parcels(
    ctx: _StreetContext, street_edges: set[EdgeKey]
) -> frozenset[int]:
    return frozenset(
        parcel_id
        for parcel_id, ring in ctx.parcel_corner_rings.items()
        if any(edge in street_edges for edge in ring_edges(ring))
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
    config: StreetGenerationConfig,
) -> tuple[StreetAccessCandidate, ...]:
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

    for joint in sorted(group_adjacency):
        neighbors = sorted(group_adjacency[joint])
        for i, left in enumerate(neighbors):
            for right in neighbors[i + 1 :]:
                angle = _included_angle(ctx, left, joint, right)
                if (
                    angle < config.l_turn_min_angle_rad
                    or angle > config.collinear_angle_threshold_rad
                ):
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
    return {edge for edge in edges if edge in ctx.corner_graph.edges}


def _candidate_reaches(
    ctx: _StreetContext, edges: set[EdgeKey], parcel_ids: frozenset[int]
) -> set[int]:
    return {
        parcel_id
        for parcel_id in parcel_ids
        if any(edge in edges for edge in ring_edges(ctx.parcel_corner_rings[parcel_id]))
    }


def _candidate_selection_key(
    candidate: StreetAccessCandidate, group: frozenset[int]
) -> tuple[int, int, float, int, tuple[EdgeKey, ...]]:
    complete_rank = 0 if candidate.reached_parcels >= group else 1
    kind_rank = 0 if candidate.kind == "I" else 1
    return (
        complete_rank,
        -len(candidate.reached_parcels),
        candidate.length,
        kind_rank,
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
    config: StreetGenerationConfig,
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
    config: StreetGenerationConfig,
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
    config: StreetGenerationConfig,
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
    config: StreetGenerationConfig,
) -> int | None:
    candidates = [
        nxt
        for nxt in adjacency.get(node, set())
        if nxt != prev
        and normalized_edge(node, nxt) not in used
        and _included_angle(ctx, prev, node, nxt) > config.collinear_angle_threshold_rad
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
    config: StreetGenerationConfig,
) -> tuple[set[EdgeKey], float]:
    access_nodes = _edge_nodes(access_edges)
    street_nodes = _edge_nodes(street_edges)
    if not access_nodes or not street_nodes or access_nodes & street_nodes:
        return set(), 0.0

    path, cost = _shortest_path_between_node_sets(
        ctx, access_nodes, street_nodes, street_edges, config
    )
    if not path:
        return set(), math.inf
    path_edges = {normalized_edge(a, b) for a, b in zip(path, path[1:], strict=False)}
    return path_edges - access_edges - street_edges, cost


def _shortest_path_between_node_sets(
    ctx: _StreetContext,
    start_nodes: set[int],
    target_nodes: set[int],
    street_edges: set[EdgeKey],
    config: StreetGenerationConfig,
) -> tuple[tuple[int, ...], float]:
    street_graph_adjacency = street_adjacency(StreetNetworkGraph(street_edges))
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
        if node in target_nodes:
            return path, cost
        for nxt in sorted(ctx.corner_adjacency.get(node, set())):
            if nxt == prev or nxt in path:
                continue
            edge = normalized_edge(node, nxt)
            step_length = _edge_length(ctx, edge)
            step_cost = step_length
            if (
                prev is not None
                and _included_angle(ctx, prev, node, nxt)
                <= config.junction_angle_threshold_rad
            ):
                step_cost += config.junction_penalty
            if nxt in target_nodes and _creates_penalized_junction(
                ctx, node, nxt, street_graph_adjacency, config
            ):
                step_cost += config.junction_penalty

            new_cost = cost + step_cost
            new_length = length + step_length
            state = (node, nxt)
            if new_cost >= best.get(state, math.inf) - 1e-12:
                continue
            best[state] = new_cost
            heapq.heappush(heap, (new_cost, new_length, (*path, nxt), node, nxt))
    return (), math.inf


def _creates_penalized_junction(
    ctx: _StreetContext,
    incoming: int,
    node: int,
    street_graph_adjacency: dict[int, set[int]],
    config: StreetGenerationConfig,
) -> bool:
    return any(
        _included_angle(ctx, incoming, node, street_neighbor)
        <= config.junction_angle_threshold_rad
        for street_neighbor in street_graph_adjacency.get(node, set())
        if street_neighbor != incoming
    )


def _complete_junctions(
    ctx: _StreetContext,
    street_edges: set[EdgeKey],
    config: StreetGenerationConfig,
) -> dict[str, Any]:
    """Add bounded cross-junction paths between already selected street nodes."""
    budget = _junction_completion_budget(ctx, config)
    before = _street_degree_diagnostics(street_edges)
    diagnostics: dict[str, Any] = {
        "junction_completion_budget_edge_count": int(budget),
        "junction_completion_four_way_before": int(
            before["four_way_intersection_count"]
        ),
        "junction_completion_dead_end_before": int(before["dead_end_node_count"]),
        "junction_completion_candidate_count": 0,
        "junction_completion_selected_path_count": 0,
        "junction_completion_added_edge_count": 0,
        "junction_completion_applied": False,
    }
    if not config.complete_junctions or budget <= 0 or not street_edges:
        after = _street_degree_diagnostics(street_edges)
        diagnostics.update(
            {
                "junction_completion_four_way_after": int(
                    after["four_way_intersection_count"]
                ),
                "junction_completion_dead_end_after": int(after["dead_end_node_count"]),
            }
        )
        return diagnostics

    added_edges: set[EdgeKey] = set()
    selected_path_count = 0
    parcel_touch_count = 0
    evaluated_candidate_count = 0
    while len(added_edges) < budget:
        remaining_budget = budget - len(added_edges)
        candidates = _junction_completion_candidates(
            ctx, street_edges, config, max_edge_count=remaining_budget
        )
        evaluated_candidate_count += len(candidates)
        if not candidates:
            break

        chosen = min(candidates, key=_junction_completion_selection_key)
        before_edges = set(street_edges)
        street_edges.update(chosen.edges)
        if not _street_edges_preserve_reachability(ctx, before_edges, street_edges):
            street_edges.clear()
            street_edges.update(before_edges)
            break

        added_edges.update(set(chosen.edges) - before_edges)
        parcel_touch_count += chosen.parcel_touch_count
        selected_path_count += 1

    after = _street_degree_diagnostics(street_edges)
    diagnostics.update(
        {
            "junction_completion_candidate_count": int(evaluated_candidate_count),
            "junction_completion_selected_path_count": int(selected_path_count),
            "junction_completion_added_edge_count": int(len(added_edges)),
            "junction_completion_parcel_touch_count": int(parcel_touch_count),
            "junction_completion_applied": bool(added_edges),
            "junction_completion_four_way_after": int(
                after["four_way_intersection_count"]
            ),
            "junction_completion_dead_end_after": int(after["dead_end_node_count"]),
        }
    )
    return diagnostics


def _junction_completion_budget(
    ctx: _StreetContext, config: StreetGenerationConfig
) -> int:
    if not config.complete_junctions:
        return 0
    ratio = max(config.junction_completion_max_added_edge_ratio, 0.0)
    ratio_budget = int(math.floor(len(ctx.corner_graph.edges) * ratio))
    if ratio > 0.0 and ratio_budget == 0:
        ratio_budget = 1
    if config.junction_completion_max_added_edges is not None:
        ratio_budget = min(
            ratio_budget, max(config.junction_completion_max_added_edges, 0)
        )
    return ratio_budget


def _junction_completion_candidates(
    ctx: _StreetContext,
    street_edges: set[EdgeKey],
    config: StreetGenerationConfig,
    *,
    max_edge_count: int,
) -> tuple[_JunctionCompletionCandidate, ...]:
    if max_edge_count <= 0:
        return ()

    street_adjacencies = _edge_adjacency(street_edges)
    street_nodes = set(street_adjacencies)
    before_four_way = _street_degree_diagnostics(street_edges)[
        "four_way_intersection_count"
    ]
    before_dead_ends = _street_degree_diagnostics(street_edges)["dead_end_node_count"]
    before_progress = _junction_completion_progress(
        ctx, street_edges, config, eligible_nodes=street_nodes
    )
    candidates: list[_JunctionCompletionCandidate] = []
    seen: set[tuple[EdgeKey, ...]] = set()

    for start in sorted(street_nodes):
        corner_degree = len(ctx.corner_adjacency.get(start, set()))
        street_degree = len(street_adjacencies.get(start, set()))
        if corner_degree < config.junction_completion_min_corner_degree:
            continue
        if street_degree < config.junction_completion_min_street_degree:
            continue
        if street_degree >= config.junction_completion_target_degree:
            continue

        missing_neighbors = ctx.corner_adjacency.get(start, set()) - (
            street_adjacencies.get(start, set())
        )
        for neighbor in sorted(missing_neighbors):
            path = _trace_completion_path(
                ctx, start, neighbor, street_nodes, street_edges, config
            )
            if path is None:
                continue
            edges = tuple(
                normalized_edge(a, b) for a, b in zip(path, path[1:], strict=False)
            )
            new_edges = tuple(edge for edge in edges if edge not in street_edges)
            if not new_edges or len(new_edges) > max_edge_count:
                continue
            key = tuple(sorted(new_edges))
            if key in seen:
                continue
            seen.add(key)

            trial_edges = set(street_edges)
            trial_edges.update(new_edges)
            after = _street_degree_diagnostics(trial_edges)
            progress_gain = (
                _junction_completion_progress(
                    ctx, trial_edges, config, eligible_nodes=street_nodes
                )
                - before_progress
            )
            four_way_gain = int(after["four_way_intersection_count"]) - before_four_way
            if four_way_gain <= 0 and progress_gain <= 0:
                continue
            if not _street_edges_preserve_reachability(ctx, street_edges, trial_edges):
                continue

            candidates.append(
                _JunctionCompletionCandidate(
                    edges=tuple(sorted(new_edges)),
                    start=start,
                    end=path[-1],
                    length=_edge_set_length(ctx, set(new_edges)),
                    parcel_touch_count=_edge_parcel_touch_count(ctx, new_edges),
                    four_way_gain=four_way_gain,
                    progress_gain=progress_gain,
                    dead_end_delta=int(after["dead_end_node_count"])
                    - int(before_dead_ends),
                )
            )

    return tuple(candidates)


def _trace_completion_path(
    ctx: _StreetContext,
    start: int,
    neighbor: int,
    street_nodes: set[int],
    street_edges: set[EdgeKey],
    config: StreetGenerationConfig,
) -> tuple[int, ...] | None:
    first_edge = normalized_edge(start, neighbor)
    if first_edge in street_edges:
        return None

    max_edges = max(config.junction_completion_max_path_edges, 1)
    nodes = [start, neighbor]
    used = {first_edge}
    while len(nodes) - 1 <= max_edges:
        node = nodes[-1]
        if node in street_nodes and node != start:
            return tuple(nodes)
        if len(nodes) - 1 >= max_edges:
            return None

        prev = nodes[-2]
        nxt = _next_collinear_node(ctx, ctx.corner_adjacency, used, prev, node, config)
        if nxt is None:
            return None
        edge = normalized_edge(node, nxt)
        if edge in street_edges and nxt not in street_nodes:
            return None
        used.add(edge)
        nodes.append(nxt)
    return None


def _junction_completion_progress(
    ctx: _StreetContext,
    street_edges: set[EdgeKey],
    config: StreetGenerationConfig,
    *,
    eligible_nodes: set[int],
) -> int:
    adjacency = _edge_adjacency(street_edges)
    score = 0
    for node in sorted(eligible_nodes):
        if (
            len(ctx.corner_adjacency.get(node, set()))
            < config.junction_completion_min_corner_degree
        ):
            continue
        degree = len(adjacency.get(node, set()))
        if degree < config.junction_completion_min_street_degree:
            continue
        score += min(degree, config.junction_completion_target_degree)
    return score


def _junction_completion_selection_key(
    candidate: _JunctionCompletionCandidate,
) -> tuple[int, int, int, int, int, float, int, int, tuple[EdgeKey, ...]]:
    return (
        -candidate.four_way_gain,
        -candidate.progress_gain,
        candidate.dead_end_delta,
        len(candidate.edges),
        -candidate.parcel_touch_count,
        candidate.length,
        candidate.start,
        candidate.end,
        candidate.edges,
    )


def _street_edges_preserve_reachability(
    ctx: _StreetContext,
    before_edges: set[EdgeKey],
    after_edges: set[EdgeKey],
) -> bool:
    before_reachable = _reachable_parcels(ctx, before_edges)
    after_reachable = _reachable_parcels(ctx, after_edges)
    if not before_reachable <= after_reachable:
        return False
    before_components = connected_component_count(StreetNetworkGraph(before_edges))
    after_components = connected_component_count(StreetNetworkGraph(after_edges))
    return after_components <= max(before_components, 1)


def _avoid_cul_de_sacs_preserving_access_and_junctions(
    ctx: _StreetContext,
    street_edges: set[EdgeKey],
    config: StreetGenerationConfig,
) -> dict[str, Any]:
    before_edges = set(street_edges)
    repair_budget = _cul_de_sac_repair_budget(ctx, config)

    pruned_before, prune_attempts_before = _prune_cul_de_sacs_preserving_access(
        ctx, street_edges, preserve_four_way=True
    )
    (
        repaired_edges,
        repair_attempt_count,
        repair_candidate_count,
        repair_success_count,
    ) = _repair_cul_de_sacs_preserving_access(
        ctx, street_edges, config, budget=repair_budget
    )
    pruned_after, prune_attempts_after = _prune_cul_de_sacs_preserving_access(
        ctx, street_edges, preserve_four_way=True
    )

    pruned_edges = set(pruned_before) | set(pruned_after)
    changed = street_edges != before_edges
    return {
        "cul_de_sac_avoidance_applied": bool(changed),
        "cul_de_sac_avoidance_changed_network": bool(changed),
        "cul_de_sac_pruned_edge_count": int(len(pruned_edges)),
        "cul_de_sac_prune_attempt_count": int(
            prune_attempts_before + prune_attempts_after
        ),
        "cul_de_sac_repair_attempt_count": int(repair_attempt_count),
        "cul_de_sac_repair_candidate_count": int(repair_candidate_count),
        "cul_de_sac_repair_success_count": int(repair_success_count),
        "cul_de_sac_repair_added_edge_count": int(len(repaired_edges)),
        "cul_de_sac_repair_budget_edge_count": int(repair_budget),
    }


def _cul_de_sac_repair_budget(
    ctx: _StreetContext, config: StreetGenerationConfig
) -> int:
    ratio = max(config.cul_de_sac_repair_max_added_edge_ratio, 0.0)
    ratio_budget = int(math.floor(len(ctx.corner_graph.edges) * ratio))
    if ratio > 0.0 and ratio_budget == 0:
        ratio_budget = 1
    if config.cul_de_sac_repair_max_added_edges is not None:
        ratio_budget = min(
            ratio_budget, max(config.cul_de_sac_repair_max_added_edges, 0)
        )
    return ratio_budget


def _repair_cul_de_sacs_preserving_access(
    ctx: _StreetContext,
    street_edges: set[EdgeKey],
    config: StreetGenerationConfig,
    *,
    budget: int,
) -> tuple[frozenset[EdgeKey], int, int, int]:
    added_edges: set[EdgeKey] = set()
    attempt_count = 0
    candidate_count = 0
    success_count = 0

    while len(added_edges) < budget:
        adjacency = _edge_adjacency(street_edges)
        dead_end_nodes = sorted(
            node for node, neighbors in adjacency.items() if len(neighbors) == 1
        )
        if not dead_end_nodes:
            break

        remaining_budget = budget - len(added_edges)
        attempt_count += len(dead_end_nodes)
        candidates = _cul_de_sac_repair_candidates(
            ctx,
            street_edges,
            config,
            dead_end_nodes=dead_end_nodes,
            max_edge_count=remaining_budget,
        )
        candidate_count += len(candidates)
        if not candidates:
            break

        chosen = min(candidates, key=_cul_de_sac_repair_selection_key)
        before_edges = set(street_edges)
        street_edges.update(chosen.edges)
        if not _street_edges_preserve_reachability(ctx, before_edges, street_edges):
            street_edges.clear()
            street_edges.update(before_edges)
            break

        added_edges.update(set(chosen.edges) - before_edges)
        success_count += 1

    return frozenset(added_edges), attempt_count, candidate_count, success_count


def _cul_de_sac_repair_candidates(
    ctx: _StreetContext,
    street_edges: set[EdgeKey],
    config: StreetGenerationConfig,
    *,
    dead_end_nodes: list[int],
    max_edge_count: int,
) -> tuple[_CulDeSacRepairCandidate, ...]:
    if max_edge_count <= 0:
        return ()

    before_dead_ends = _dead_end_count(street_edges)
    candidates: list[_CulDeSacRepairCandidate] = []
    seen: set[tuple[EdgeKey, ...]] = set()
    for leaf in dead_end_nodes:
        path, length = _shortest_new_path_from_leaf_to_street(
            ctx, leaf, street_edges, config
        )
        if not path:
            continue
        edges = tuple(
            normalized_edge(a, b) for a, b in zip(path, path[1:], strict=False)
        )
        new_edges = tuple(edge for edge in edges if edge not in street_edges)
        if not new_edges or len(new_edges) > max_edge_count:
            continue
        key = tuple(sorted(new_edges))
        if key in seen:
            continue
        seen.add(key)

        trial_edges = set(street_edges)
        trial_edges.update(new_edges)
        dead_end_delta = _dead_end_count(trial_edges) - before_dead_ends
        if dead_end_delta >= 0:
            continue
        if not _street_edges_preserve_reachability(ctx, street_edges, trial_edges):
            continue

        candidates.append(
            _CulDeSacRepairCandidate(
                edges=tuple(sorted(new_edges)),
                leaf=leaf,
                end=path[-1],
                length=length,
                parcel_touch_count=_edge_parcel_touch_count(ctx, new_edges),
                dead_end_delta=dead_end_delta,
            )
        )

    return tuple(candidates)


def _shortest_new_path_from_leaf_to_street(
    ctx: _StreetContext,
    leaf: int,
    street_edges: set[EdgeKey],
    config: StreetGenerationConfig,
) -> tuple[tuple[int, ...], float]:
    street_nodes = _edge_nodes(street_edges)
    target_nodes = street_nodes - {leaf}
    if not target_nodes:
        return (), math.inf

    max_edges = max(config.cul_de_sac_repair_max_path_edges, 1)
    heap: list[tuple[float, float, tuple[int, ...], int | None, int]] = []
    best: dict[tuple[int | None, int], float] = {(None, leaf): 0.0}
    heapq.heappush(heap, (0.0, 0.0, (leaf,), None, leaf))

    while heap:
        cost, length, path, prev, node = heapq.heappop(heap)
        if cost > best.get((prev, node), math.inf) + 1e-12:
            continue
        if len(path) > 1 and node in target_nodes:
            return path, length
        if len(path) - 1 >= max_edges:
            continue

        for nxt in sorted(ctx.corner_adjacency.get(node, set())):
            if nxt == prev or nxt in path:
                continue
            edge = normalized_edge(node, nxt)
            if edge in street_edges:
                continue

            step_length = _edge_length(ctx, edge)
            step_cost = step_length
            if (
                prev is not None
                and _included_angle(ctx, prev, node, nxt)
                <= config.junction_angle_threshold_rad
            ):
                step_cost += config.junction_penalty

            new_cost = cost + step_cost
            state = (node, nxt)
            if new_cost >= best.get(state, math.inf) - 1e-12:
                continue
            best[state] = new_cost
            heapq.heappush(
                heap,
                (
                    new_cost,
                    length + step_length,
                    (*path, nxt),
                    node,
                    nxt,
                ),
            )

    return (), math.inf


def _cul_de_sac_repair_selection_key(
    candidate: _CulDeSacRepairCandidate,
) -> tuple[int, int, int, float, int, int, tuple[EdgeKey, ...]]:
    return (
        candidate.dead_end_delta,
        len(candidate.edges),
        -candidate.parcel_touch_count,
        candidate.length,
        candidate.leaf,
        candidate.end,
        candidate.edges,
    )


def _prune_cul_de_sacs_preserving_access(
    ctx: _StreetContext,
    street_edges: set[EdgeKey],
    *,
    preserve_four_way: bool,
) -> tuple[frozenset[EdgeKey], int]:
    baseline_reachable = _reachable_parcels(ctx, street_edges)
    baseline_components = connected_component_count(StreetNetworkGraph(street_edges))
    removed: set[EdgeKey] = set()
    attempt_count = 0

    changed = True
    while changed:
        changed = False
        adjacency = _edge_adjacency(street_edges)
        dead_end_nodes = sorted(
            node for node, neighbors in adjacency.items() if len(neighbors) == 1
        )
        for node in dead_end_nodes:
            attempt_count += 1
            neighbors = adjacency.get(node, set())
            if len(neighbors) != 1:
                continue
            edge = normalized_edge(node, next(iter(neighbors)))
            if edge not in street_edges:
                continue
            trial_edges = set(street_edges)
            trial_edges.remove(edge)
            if not trial_edges:
                continue
            if _reachable_parcels(ctx, trial_edges) != baseline_reachable:
                continue
            if (
                connected_component_count(StreetNetworkGraph(trial_edges))
                > baseline_components
            ):
                continue
            if preserve_four_way and (
                _street_degree_diagnostics(trial_edges)["four_way_intersection_count"]
                < _street_degree_diagnostics(street_edges)[
                    "four_way_intersection_count"
                ]
            ):
                continue
            street_edges.remove(edge)
            removed.add(edge)
            changed = True

    return frozenset(removed), attempt_count


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


def _edge_parcel_touch_count(
    ctx: _StreetContext,
    edges: tuple[EdgeKey, ...] | set[EdgeKey] | frozenset[EdgeKey],
) -> int:
    return sum(len(ctx.edge_parcels.get(edge, ())) for edge in edges)


def _edge_nodes(edges: set[EdgeKey] | frozenset[EdgeKey]) -> set[int]:
    return {node for edge in edges for node in edge}


def _dead_end_count(edges: set[EdgeKey]) -> int:
    adjacency = _edge_adjacency(edges)
    return sum(1 for neighbors in adjacency.values() if len(neighbors) == 1)


def _street_degree_diagnostics(edges: set[EdgeKey]) -> dict[str, Any]:
    adjacency = _edge_adjacency(edges)
    degree_counts: dict[int, int] = {}
    for neighbors in adjacency.values():
        degree = len(neighbors)
        degree_counts[degree] = degree_counts.get(degree, 0) + 1
    junction_count = sum(
        count for degree, count in degree_counts.items() if degree >= 3
    )
    return {
        "node_count": int(len(adjacency)),
        "dead_end_node_count": int(degree_counts.get(1, 0)),
        "degree_2_node_count": int(degree_counts.get(2, 0)),
        "junction_count": int(junction_count),
        "t_junction_count": int(degree_counts.get(3, 0)),
        "four_way_intersection_count": int(degree_counts.get(4, 0)),
        "high_degree_intersection_count": int(
            sum(count for degree, count in degree_counts.items() if degree >= 5)
        ),
        "degree_counts": dict(sorted(degree_counts.items())),
    }
