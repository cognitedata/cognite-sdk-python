from __future__ import annotations

from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import Node, NodeList
from cognite.client.data_classes.data_modeling.views import View


def resolve_source(
    source: View | ViewId | tuple[str, str, str],
    canonical_view_id: ViewId,
) -> tuple[list[ViewId], bool]:
    """Resolve a user-supplied source into a list of ViewIds to query, plus a strip flag.

    When the user passes a custom source, ``canonical_view_id`` is appended to guarantee
    only nodes of the expected type are returned. The strip flag tells the caller to remove
    ``canonical_view_id`` properties from results so nodes come back with a single source.
    """
    match source:
        case ViewId():
            source_as_id = source
        case View():
            source_as_id = source.as_id()
        case [str(), str(), str()]:
            source_as_id = ViewId(*source)
        case _:
            raise TypeError(f"Expected View, ViewId, or a (space, external_id, version) tuple, got {type(source)}")

    if source_as_id == canonical_view_id:
        return [source_as_id], False

    return [source_as_id, canonical_view_id], True


def strip_canonical_source(result: Node | NodeList[Node] | None, canonical_view_id: ViewId) -> None:
    if not result:
        return

    for node in [result] if isinstance(result, Node) else result:
        node.drop_source(canonical_view_id)
