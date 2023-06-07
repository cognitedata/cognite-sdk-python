from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, List, Literal, Type, TypeVar, Union

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling._core import DataModelingResource
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelationReference,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId
from cognite.client.utils._text import convert_all_keys_to_camel_case_recursive, convert_all_keys_to_snake_case

PropertyValue = Union[str, int, float, bool, dict, List[str], List[int], List[float], List[bool], List[dict]]
Space = str
PropertyIdentifier = str


@dataclass
class NodeOrEdgeData:
    source: ContainerId | ViewId
    properties: dict[str, PropertyValue]

    @classmethod
    def load(cls, data: dict) -> NodeOrEdgeData:
        return cls(**convert_all_keys_to_snake_case(data))

    def dump(self, camel_case: bool = False) -> dict:
        output = asdict(self)
        if self.source:
            output["source"] = self.source.dump(camel_case)
        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output


class InstanceCore(DataModelingResource):
    """A node or edge
    Args:
        instance_type (Literal["node", "edge"]) The type of instance.
        space (str): The workspace for the instance.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
    """

    def __init__(self, space: str, external_id: str, instance_type: Literal["node", "edge"] = "node", **_: dict):
        validate_data_modeling_identifier(space, external_id)
        self.instance_type = instance_type
        self.space = space
        self.external_id = external_id


class InstanceApply(InstanceCore):
    """A node or edge. This is the write version of the instance.
    Args:
        instance_type (Literal["node", "edge"]) The type of instance.
        space (str): The workspace for the instance.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
        existing_version (int): Fail the ingestion request if the node's version is greater than or equal to this value.
                                If no existingVersion is specified, the ingestion will always overwrite any
                                existing data for the edge (for the specified container or instance). If existingVersion is
                                set to 0, the upsert will behave as an insert, so it will fail the bulk if the
                                item already exists. If skipOnVersionConflict is set on the ingestion request,
                                then the item will be skipped instead of failing the ingestion request.
        sources (list[NodeOrEdgeData]): List of source properties to write. The properties are from the instance and/or
                        container the container(s) making up this node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        instance_type: Literal["node", "edge"] = "node",
        existing_version: int = None,
        sources: list[NodeOrEdgeData] = None,
        **_: dict,
    ):
        super().__init__(space, external_id, instance_type)
        self.existing_version = existing_version
        self.sources = sources

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.sources:
            output["sources"] = [source.dump(camel_case) for source in self.sources]
        return output

    @classmethod
    def load(cls: Type[T_Instance_Apply], data: dict | str) -> T_Instance_Apply:
        data = data if isinstance(data, dict) else json.loads(data)
        instance = cls(**convert_all_keys_to_snake_case(data))
        if "source" in data:
            instance.sources = [NodeOrEdgeData.load(source) for source in data["source"]]
        return instance


T_Instance_Apply = TypeVar("T_Instance_Apply", bound=InstanceApply)


class Instance(InstanceCore):
    """A node or edge. This is the read version of the instance.
    Args:
        instance_type (Literal["node", "edge"]) The type of instance.
        space (str): The workspace for the instance.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
        version (str): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        deleted_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
                            Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        last_updated_time: int,
        created_time: int,
        instance_type: Literal["node", "edge"] = "node",
        deleted_time: int = None,
        properties: dict[Space, dict[PropertyIdentifier, dict[str, PropertyValue]]] = None,
        **_: dict,
    ):
        super().__init__(space, external_id, instance_type)
        self.version = version
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self.deleted_time = deleted_time
        self.properties = properties

    def as_apply(self, source: ViewId | ContainerId, existing_version: int) -> InstanceApply:
        return InstanceApply(
            space=self.space,
            external_id=self.external_id,
            instance_type=self.instance_type,
            existing_version=existing_version,
            sources=[
                NodeOrEdgeData(source=source, properties=space_properties)  # type: ignore[arg-type]
                for space_properties in self.properties.values()
            ]
            if self.properties
            else None,
        )


class InstanceUpdate(InstanceCore):
    """A node or edge. This represents the update on the instance.
    Args:
        instance_type (Literal["node", "edge"]) The type of instance.
        space (str): The workspace for the instance.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
        version (str): DMS version of the instance.
        was_modified (bool): Whether the instance was modified by the ingestion.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        instance_type: Literal["node", "edge"],
        space: str,
        external_id: str,
        version: str,
        was_modified: bool,
        last_updated_time: int = None,
        created_time: int = None,
        **_: dict,
    ):
        super().__init__(space, external_id, instance_type)
        self.version = version
        self.was_modified = was_modified
        self.last_updated_time = last_updated_time
        self.created_time = created_time


class NodeApply(InstanceApply):
    """A node. This is the write version of the node.
    Args:
        space (str): The workspace for the node.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the node.
        existing_version (int): Fail the ingestion request if the node's version is greater than or equal to this value.
                                If no existingVersion is specified, the ingestion will always overwrite any
                                existing data for the edge (for the specified container or node). If existingVersion is
                                set to 0, the upsert will behave as an insert, so it will fail the bulk if the
                                item already exists. If skipOnVersionConflict is set on the ingestion request,
                                then the item will be skipped instead of failing the ingestion request.
        sources (list[NodeOrEdgeData]): List of source properties to write. The properties are from the node and/or
                        container the container(s) making up this node.
    """

    ...


class Node(Instance):
    """A node. This is the read version of the node.
    Args:
        space (str): The workspace for the node.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the node.
        version (str): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        deleted_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
                            Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
        properties
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        last_updated_time: int,
        created_time: int,
        deleted_time: int = None,
        properties: dict[str, dict[str, dict[str, PropertyValue]]] = None,
        **_: dict,
    ):
        super().__init__(space, external_id, version, last_updated_time, created_time, "node", deleted_time, properties)

    def as_apply(self, source: ViewId | ContainerId, existing_version: int) -> NodeApply:
        return NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=existing_version,
            sources=[
                NodeOrEdgeData(source=source, properties=space_properties[source.identifier])  # type: ignore[arg-type]
                for space_properties in self.properties.values()
            ]
            if self.properties
            else None,
        )


class NodeUpdate(InstanceUpdate):
    """A node. This represents the update on the node.
    Args:
        space (str): The workspace for the node a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the node.
        version (str): DMS version of the node.
        was_modified (bool): Whether the node was modified by the ingestion.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        was_modified: bool,
        last_updated_time: int = None,
        created_time: int = None,
        **_: dict,
    ):
        super().__init__(
            instance_type="node",
            space=space,
            external_id=external_id,
            version=version,
            was_modified=was_modified,
            last_updated_time=last_updated_time,
            created_time=created_time,
        )


class EdgeApply(InstanceApply):
    """An Edge. This is the write version of the edge.
    Args:
        space (str): The workspace for the edge.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the edge.
        type (DirectRelationReference): The type of edge.
        existing_version (int): Fail the ingestion request if the node's version is greater than or equal to this value.
                                If no existingVersion is specified, the ingestion will always overwrite any
                                existing data for the edge (for the specified container or edge). If existingVersion is
                                set to 0, the upsert will behave as an insert, so it will fail the bulk if the
                                item already exists. If skipOnVersionConflict is set on the ingestion request,
                                then the item will be skipped instead of failing the ingestion request.
        sources (list[NodeOrEdgeData]): List of source properties to write. The properties are from the edge and/or
                        container the container(s) making up this node.
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        existing_version: int = None,
        sources: list[NodeOrEdgeData] = None,
        **_: dict,
    ):
        super().__init__(space, external_id, "edge", existing_version, sources)
        self.type = type
        self.start_node = start_node
        self.end_node = end_node


class Edge(Instance):
    """An Edge.  This is the read version of the edge.
    Args:
        space (str): The workspace for the edge an unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the edge.
        version (str): DMS version.
        type (DirectRelationReference): The type of edge.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        deleted_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
                            Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        type: DirectRelationReference,
        last_updated_time: int,
        created_time: int,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        deleted_time: int = None,
        properties: dict[str, dict] = None,
        **_: dict,
    ):
        super().__init__(space, external_id, version, last_updated_time, created_time, "edge", deleted_time, properties)
        self.type = type
        self.start_node = start_node
        self.end_node = end_node

    def as_apply(self, source: ViewId | ContainerId, existing_version: int = None) -> InstanceApply:
        return EdgeApply(
            space=self.space,
            external_id=self.external_id,
            type=self.type,
            start_node=self.start_node,
            end_node=self.end_node,
            existing_version=existing_version or None,
            sources=[
                NodeOrEdgeData(source=source, properties=space_properties[source.identifier])  # type: ignore[arg-type]
                for space_properties in self.properties.values()  # type: ignore[union-attr]
            ]
            or None,
        )


class EdgeUpdate(InstanceUpdate):
    """An Edge. This represents the update on the edge.
    Args:
        space (str): The workspace for the edge a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the edge.
        version (str): DMS version.
        was_modified (bool): Whether the edge was modified by the ingestion.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        was_modified: bool,
        last_updated_time: int = None,
        created_time: int = None,
        **_: dict,
    ):
        super().__init__(
            instance_type="edge",
            space=space,
            external_id=external_id,
            version=version,
            was_modified=was_modified,
            last_updated_time=last_updated_time,
            created_time=created_time,
        )


class InstanceList(CogniteResourceList):
    _RESOURCE = Instance


class NodeApplyList(CogniteResourceList):
    _RESOURCE = NodeApply


class NodeUpdateList(CogniteResourceList):
    _RESOURCE = NodeUpdate


class NodeList(CogniteResourceList):
    _RESOURCE = Node


class EdgeApplyList(CogniteResourceList):
    _RESOURCE = EdgeApply


class EdgeUpdateList(CogniteResourceList):
    _RESOURCE = EdgeUpdate


class EdgeList(CogniteResourceList):
    _RESOURCE = Edge


class InstanceFilter(CogniteFilter):
    """Represent the filer arguments for the list endpoint.
    Args:
        include_typing (bool): Whether to return property type information as part of the result.
        sources (list[ViewReference]): Views to retrieve properties from.
        instance_type(Literal["node", "edge"]): Whether to query for nodes or edges.
    """

    def __init__(
        self,
        include_typing: bool = False,
        sources: list[ViewId] = None,
        instance_type: Literal["node", "edge"] = "node",
    ):
        self.include_typing = include_typing
        self.sources = sources
        self.instance_type = instance_type

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        dumped = super().dump(camel_case)
        if "sources" in dumped:
            dumped["sources"] = [
                {"source": v if isinstance(v, dict) else v.dump(camel_case)} for v in dumped["sources"]
            ]
        return dumped


class InstanceSort(CogniteFilter):
    def __init__(
        self,
        property: list[str],
        direction: Literal["ascending", "descending"] = "ascending",
        nulls_first: bool = False,
    ):
        self.property = property
        self.direction = direction
        self.nulls_first = nulls_first
