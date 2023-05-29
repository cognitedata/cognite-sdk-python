from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling.shared import DirectRelationReference, ViewReference
from cognite.client.utils._validation import validate_data_modeling_identifier

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Instance(CogniteResource):

    """A node or edge

    Args:
        instance_type (Literal["node", "edge"]) The type of instance.
        space (str): The workspace for the view.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        version (str): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        deleted_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
                            Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        instance_type: Literal["node", "edge"],
        space: str = None,
        external_id: str = None,
        version: str = None,
        last_updated_time: int = None,
        created_time: int = None,
        deleted_time: int = None,
        properties: dict = None,
        cognite_client: CogniteClient = None,
    ):
        validate_data_modeling_identifier(space, external_id)
        self.instance_type = instance_type
        self.space = space
        self.external_id = external_id
        self.version = version
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self.deleted_time = deleted_time
        self.properties = properties
        self._cognite_client = cast("CogniteClient", cognite_client)


class Node(Instance):
    """A node

    Args:
        instance_type (Literal["node"]) Must be node.
        space (str): The workspace for the view.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        version (str): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        deleted_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
                            Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
        properties
    """

    def __init__(
        self,
        instance_type: Literal["node"] = "node",
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
            instance_type,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            deleted_time,
            properties,
            cognite_client,
        )


class Edge(Instance):
    """An Edge

    Args:
        instance_type (Literal["edge"]) Must be edge.
        space (str): The workspace for the view.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        version (str): DMS version.
        type ():
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        deleted_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
                            Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
        properties
    """

    def __init__(
        self,
        instance_type: Literal["edge"] = "edge",
        space: str = None,
        external_id: str = None,
        version: str = None,
        type: dict[str, DirectRelationReference] = None,
        last_updated_time: int = None,
        created_time: int = None,
        deleted_time: int = None,
        properties: dict[str, dict] = None,
        start_node: DirectRelationReference = None,
        end_node: DirectRelationReference = None,
        cognite_client: CogniteClient = None,
    ):
        super().__init__(
            instance_type,
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


class NodeList(CogniteResourceList):
    _RESOURCE = Node


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
        self.instance_type = (instance_type,)


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
