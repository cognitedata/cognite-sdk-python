from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling.shared import DirectRelationReference, ViewReference
from cognite.client.utils._text import to_snake_case
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

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient = None) -> Node | Edge:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        if resource["instanceType"] == "node":
            return Node._load(resource, cognite_client)
        elif resource["instanceType"] == "edge":
            return Edge._load(resource, cognite_client)
        else:
            raise ValueError(f"Unsupported resource type {resource['type']}")


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

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient = None) -> Node:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        node = cls()
        for name, value in resource.items():
            snake_name = to_snake_case(name)
            if hasattr(node, snake_name):
                setattr(node, snake_name, value)
        return node


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

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient = None) -> Edge:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        for field in ["type", "start_node", "end_node", "startNode", "endNode"]:
            if field in resource:
                resource[field] = DirectRelationReference.load(resource[field])
        edge = cls()
        for name, value in resource.items():
            snake_name = to_snake_case(name)
            if hasattr(edge, snake_name):
                setattr(edge, snake_name, value)
        return edge


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
