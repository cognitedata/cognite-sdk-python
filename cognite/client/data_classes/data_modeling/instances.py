from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.shared import DirectRelationReference, ViewReference

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class InstanceCore(CogniteResource):
    """A node or edge
    Args:
        instance_type (Literal["node", "edge"]) The type of instance.
        space (str): The workspace for the instance.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
    """

    def __init__(
        self,
        instance_type: Literal["node", "edge"] = "node",
        space: str = None,
        external_id: str = None,
        cognite_client: CogniteClient = None,
    ):
        validate_data_modeling_identifier(space, external_id)
        self.instance_type = instance_type
        self.space = space
        self.external_id = external_id
        self._cognite_client = cast("CogniteClient", cognite_client)


class InstanceApply(InstanceCore):
    """A node or edge. This is the write version of the instance.
    Args:
        instance_type (Literal["node", "edge"]) The type of instance.
        space (str): The workspace for the instance.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
        existing_version (int): Fail the ingestion request if the node's version is greater than or equal to this value.
                                If no existingVersion is specified, the ingestion will always overwrite any
                                existing data for the edge (for the specified container or instance). If existingVersion is
                                sset to 0, the upsert will behave as an insert, so it will fail the bulk if the
                                item already exists. If skipOnVersionConflict is set on the ingestion request,
                                then the item will be skipped instead of failing the ingestion request.
        sources (list): List of source properties to write. The properties are from the instance and/or
                        container the container(s) making up this node.
    """

    def __init__(
        self,
        instance_type: Literal["node", "edge"] = "node",
        space: str = None,
        external_id: str = None,
        existing_version: int = None,
        sources: list = None,
        cognite_client: CogniteClient = None,
    ):
        super().__init__(instance_type, space, external_id, cognite_client)
        self.existing_version = existing_version
        self.sources = sources


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
        instance_type: Literal["node", "edge"] = "node",
        space: str = None,
        external_id: str = None,
        version: str = None,
        last_updated_time: int = None,
        created_time: int = None,
        deleted_time: int = None,
        properties: dict = None,
        cognite_client: CogniteClient = None,
    ):
        super().__init__(instance_type, space, external_id, cognite_client)
        self.version = version
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self.deleted_time = deleted_time
        self.properties = properties


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
        sources (list): List of source properties to write. The properties are from the node and/or
                        container the container(s) making up this node.
    """

    def __init__(
        self,
        space: str = None,
        external_id: str = None,
        existing_version: int = None,
        sources: list = None,
        cognite_client: CogniteClient = None,
    ):
        super().__init__(
            "node",
            space,
            external_id,
            existing_version,
            sources,
            cognite_client,
        )


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
        space: str = None,
        external_id: str = None,
        version: str = None,
        last_updated_time: int = None,
        created_time: int = None,
        deleted_time: int = None,
        properties: dict[str, dict] = None,
        cognite_client: CogniteClient = None,
    ):
        super().__init__(
            "node",
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            deleted_time,
            properties,
            cognite_client,
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
        sources (list): List of source properties to write. The properties are from the edge and/or
                        container the container(s) making up this node.
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
    """

    def __init__(
        self,
        space: str = None,
        external_id: str = None,
        type: DirectRelationReference = None,
        existing_version: int = None,
        sources: list = None,
        start_node: DirectRelationReference = None,
        end_node: DirectRelationReference = None,
        cognite_client: CogniteClient = None,
    ):
        super().__init__(
            "edge",
            space,
            external_id,
            existing_version,
            sources,
            cognite_client,
        )
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
        space: str = None,
        external_id: str = None,
        version: str = None,
        type: DirectRelationReference = None,
        last_updated_time: int = None,
        created_time: int = None,
        deleted_time: int = None,
        properties: dict[str, dict] = None,
        start_node: DirectRelationReference = None,
        end_node: DirectRelationReference = None,
        cognite_client: CogniteClient = None,
    ):
        super().__init__(
            "edge",
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            deleted_time,
            properties,
            cognite_client,
        )
        self.type = type
        self.start_node = start_node
        self.end_node = end_node


class InstanceList(CogniteResourceList):
    _RESOURCE = Instance


class NodeApplyList(CogniteResourceList):
    _RESOURCE = NodeApply


class NodeList(CogniteResourceList):
    _RESOURCE = Node


class EdgeApplyList(CogniteResourceList):
    _RESOURCE = EdgeApply


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
        sources: list[ViewReference] = None,
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
