from __future__ import annotations


def find_all_cycles_with_elements(has_cycles: set[str], edges: dict) -> list[list[str]]:
    """Given a set of connected nodes and a mapping between them (edges), find and return all cycles.

    Raises:
        KeyError: No loop found or edge does not exist.

    Args:
        has_cycles (set[str]): No description.
        edges (dict): No description.
    Returns:
        list[list[str]]: No description."""
    cycles = []
    skip: set[str] = set()
    while has_cycles:
        start = has_cycles.pop()
        visited, cycle = find_extended_cycle(start, edges, skip)
        if cycle:
            cycles.append(cycle)
        has_cycles -= visited
        skip |= visited
    return cycles


def find_extended_cycle(slow: str, edges: dict, skip: set[str]) -> tuple[set[str], list[str]]:
    """Uses Floyd's cycle-finding algo with two pointers "slow" and "fast" moving at different speeds.

    Raises:
        KeyError: No loop found or edge does not exist.

    Args:
        slow (str): No description.
        edges (dict): No description.
        skip (set[str]): No description.
    Returns:
        tuple[set[str], list[str]]: No description."""
    all_elements = {slow}
    fast = edges[slow]
    while slow != fast:
        if slow in skip:
            return all_elements, []

        all_elements.add(slow := edges[slow])  # type: ignore [arg-type, assignment]
        fast = edges[edges[fast]]

    loop_elements = [loop_start := slow]
    while (slow := edges[slow]) != loop_start:
        if slow in skip:
            return all_elements.union(loop_elements), []
        loop_elements.append(slow)

    return all_elements.union(loop_elements), loop_elements
