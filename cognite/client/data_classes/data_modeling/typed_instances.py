from __future__ import annotations

from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelationReference,
)
from cognite.client.data_classes.data_modeling.ids import EdgeId, NodeId


class TypedNodeWrite:
    def __init__(
        self,
        space: str,
        external_id: str,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.existing_version = existing_version
        self.type = DirectRelationReference.load(type) if type else None

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)


class TypedEdgeWrite:
    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        existing_version: int | None = None,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.type = DirectRelationReference.load(type)
        self.start_node = DirectRelationReference.load(start_node)
        self.end_node = DirectRelationReference.load(end_node)
        self.existing_version = existing_version

    def as_id(self) -> EdgeId:
        return EdgeId(space=self.space, external_id=self.external_id)
