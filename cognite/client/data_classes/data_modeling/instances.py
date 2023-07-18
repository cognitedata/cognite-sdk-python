from __future__ import annotations

import json
from abc import abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Dict,
    ItemsView,
    Iterator,
    KeysView,
    List,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    ValuesView,
    cast,
    overload,
)

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling._core import DataModelingResource
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.aggregations import AggregatedNumberedValue
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelationReference,
)
from cognite.client.data_classes.data_modeling.ids import (
    ContainerId,
    ContainerIdentifier,
    EdgeId,
    NodeId,
    ViewId,
    ViewIdentifier,
)
from cognite.client.utils._text import convert_all_keys_to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient
PropertyValue = Union[str, int, float, bool, dict, List[str], List[int], List[float], List[bool], List[dict]]
Space = str
PropertyIdentifier = str


@dataclass
class NodeOrEdgeData:
    """This represents the data values of a node or edge.

    Args:
        source (ContainerId | ViewId): The container or view the node or edge property is in
        properties (Mapping[str, PropertyValue]): The properties of the node or edge.
    """

    source: ContainerId | ViewId
    properties: Mapping[str, PropertyValue]

    @classmethod
    def load(cls, data: dict) -> NodeOrEdgeData:
        return cls(**convert_all_keys_to_snake_case(data))

    def dump(self, camel_case: bool = False) -> dict:
        output: Dict[str, Any] = {"properties": dict(self.properties.items())}
        if self.source:
            if isinstance(self.source, (ContainerId, ViewId)):
                output["source"] = self.source.dump(camel_case)
            elif isinstance(self.source, dict):
                output["source"] = self.source
            else:
                raise TypeError(f"source must be ContainerId, ViewId or a dict, but was {type(self.source)}")
        return output


class InstanceCore(DataModelingResource):
    """A node or edge
    Args:
        instance_type (Literal["node", "edge"]) The type of instance.
        space (str): The workspace for the instance.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
    """

    def __init__(self, space: str, external_id: str, instance_type: Literal["node", "edge"] = "node"):
        self.instance_type = instance_type
        self.space = space
        self.external_id = external_id


class InstanceApply(InstanceCore):
    """A node or edge. This is the write version of the instance.

    Args:
        space (str): The workspace for the instance.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
        instance_type (Literal["node", "edge"]) The type of instance.
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
        existing_version: Optional[int] = None,
        sources: Optional[list[NodeOrEdgeData]] = None,
    ):
        validate_data_modeling_identifier(space, external_id)
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
        data = convert_all_keys_to_snake_case(data)
        if cls is not InstanceApply:
            # NodeApply and EdgeApply does not support instance type
            data.pop("instance_type", None)
        instance = cls(**convert_all_keys_to_snake_case(data))
        if "sources" in data:
            instance.sources = [NodeOrEdgeData.load(source) for source in data["sources"]]
        return instance


T_Instance_Apply = TypeVar("T_Instance_Apply", bound=InstanceApply)

_T = TypeVar("_T")


class Properties(MutableMapping[ViewIdentifier, MutableMapping[PropertyIdentifier, PropertyValue]]):
    def __init__(self, properties: MutableMapping[ViewId, MutableMapping[PropertyIdentifier, PropertyValue]]):
        self.data = properties

    @classmethod
    def load(
        cls, data: MutableMapping[Space, MutableMapping[str, MutableMapping[PropertyIdentifier, PropertyValue]]]
    ) -> Properties:
        props: MutableMapping[ViewId, MutableMapping[PropertyIdentifier, PropertyValue]] = {}
        for space, view_properties in data.items():
            for view_id_str, properties in view_properties.items():
                view_tuple = tuple(view_id_str.split("/", 1))
                if len(view_tuple) != 2:
                    raise ValueError("View id must be in the format <external_id>/<version>")
                view_id = ViewId.load(cast(Tuple[str, str, str], (space, *view_tuple)))
                props[view_id] = properties
        return Properties(props)

    def dump(self) -> dict[Space, dict[str, dict[PropertyIdentifier, PropertyValue]]]:
        props: dict[Space, dict[str, dict[PropertyIdentifier, PropertyValue]]] = defaultdict(dict)
        for view_id, properties in self.data.items():
            view_id_str = f"{view_id.external_id}/{view_id.version}"
            props[view_id.space][view_id_str] = cast(Dict[PropertyIdentifier, PropertyValue], properties)
        return props

    def items(self) -> ItemsView[ViewId, MutableMapping[PropertyIdentifier, PropertyValue]]:
        return self.data.items()

    def keys(self) -> KeysView[ViewId]:
        return self.data.keys()

    def values(self) -> ValuesView[MutableMapping[PropertyIdentifier, PropertyValue]]:
        return self.data.values()

    def __iter__(self) -> Iterator[ViewId]:
        yield from self.keys()

    def __getitem__(self, view: ViewIdentifier) -> MutableMapping[PropertyIdentifier, PropertyValue]:
        view_id = ViewId.load(view)
        return self.data.get(view_id, {})

    def __contains__(self, item: Any) -> bool:
        view_id = ViewId.load(item)
        return view_id in self.data

    @overload
    def get(self, view: ViewIdentifier) -> Optional[MutableMapping[PropertyIdentifier, PropertyValue]]:
        ...

    @overload
    def get(
        self, view: ViewIdentifier, default: MutableMapping[PropertyIdentifier, PropertyValue] | _T
    ) -> MutableMapping[PropertyIdentifier, PropertyValue] | _T:
        ...

    def get(
        self,
        view: ViewIdentifier,
        default: Optional[Optional[MutableMapping[PropertyIdentifier, PropertyValue]] | _T] = None,
    ) -> Optional[MutableMapping[PropertyIdentifier, PropertyValue]] | _T:
        view_id = ViewId.load(view)
        return self.data.get(view_id, default)

    def __len__(self) -> int:
        return len(self.data)

    def __delitem__(self, view: ViewIdentifier) -> None:
        view_id = ViewId.load(view)
        del self.data[view_id]

    def __setitem__(self, view: ViewIdentifier, properties: MutableMapping[PropertyIdentifier, PropertyValue]) -> None:
        view_id = ViewId.load(view)
        self.data[view_id] = properties


T_Instance = TypeVar("T_Instance", bound="Instance")


class Instance(InstanceCore):
    """A node or edge. This is the read version of the instance.

    Args:
        space (str): The workspace for the instance.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
        version (str): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        deleted_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
                            Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
        instance_type (Literal["node", "edge"]) The type of instance.
        properties (Properties): Properties of the instance.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        last_updated_time: int,
        created_time: int,
        instance_type: Literal["node", "edge"] = "node",
        deleted_time: Optional[int] = None,
        properties: Optional[Properties] = None,
        **_: dict,
    ):
        super().__init__(space, external_id, instance_type)
        self.version = version
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self.deleted_time = deleted_time
        self.properties: Properties = properties or Properties({})

    @classmethod
    def load(cls: Type[T_Instance], data: dict | str) -> T_Instance:
        data = json.loads(data) if isinstance(data, str) else data
        if "properties" in data:
            data["properties"] = Properties.load(data["properties"])
        res = super().load(data)
        return res

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        dumped = super().dump(camel_case)
        if "properties" in dumped:
            dumped["properties"] = self.properties.dump()
        return dumped

    @abstractmethod
    def as_apply(self, source: ViewIdentifier | ContainerIdentifier, existing_version: int) -> InstanceApply:
        """Convert the instance to an apply instance."""
        raise NotImplementedError()


class InstanceApplyResult(InstanceCore):
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
        last_updated_time: int,
        created_time: int,
        **_: dict,
    ):
        super().__init__(space, external_id, instance_type)
        self.version = version
        self.was_modified = was_modified
        self.last_updated_time = last_updated_time
        self.created_time = created_time


class InstanceAggregationResult(DataModelingResource):
    """A node or edge. This represents the update on the instance.

    Args:
        aggregates (list[AggregatedNumberedValue]) : List of aggregated values.
        group (dict[str, str | int | float | bool]) : The grouping used for the aggregation.
    """

    def __init__(self, aggregates: list[AggregatedNumberedValue], group: dict[str, str | int | float | bool]):
        self.aggregates = aggregates
        self.group = group

    @classmethod
    def load(cls, data: dict | str) -> InstanceAggregationResult:
        """
        Loads an instance from a json string or dictionary.

        Args:
            data (dict | str): The json string or dictionary.

        Returns:
            An instance.

        """
        data = json.loads(data) if isinstance(data, str) else data

        return cls(
            aggregates=[AggregatedNumberedValue.load(agg) for agg in data["aggregates"]],
            group=cast(Dict[str, Union[str, int, float, bool]], data.get("group")),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        """
        Dumps the aggregation results to a dictionary.

        Args:
            camel_case (bool): Whether to convert the keys to camel case.

        Returns:
            A dictionary with the instance results.

        """
        return {
            "aggregates": [agg.dump(camel_case) for agg in self.aggregates],
            "group": self.group,
        }


class InstanceAggregationResultList(CogniteResourceList[InstanceAggregationResult]):
    _RESOURCE = InstanceAggregationResult


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

    def __init__(
        self,
        space: str,
        external_id: str,
        existing_version: Optional[int] = None,
        sources: Optional[list[NodeOrEdgeData]] = None,
    ):
        super().__init__(space, external_id, "node", existing_version, sources)

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)


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
        properties (dict[Space, dict[PropertyIdentifier, dict[str, PropertyValue]]]): Properties of the node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        last_updated_time: int,
        created_time: int,
        deleted_time: Optional[int] = None,
        properties: Optional[Properties] = None,
        **_: dict,
    ):
        super().__init__(space, external_id, version, last_updated_time, created_time, "node", deleted_time, properties)

    def as_apply(self, source: ViewIdentifier | ContainerIdentifier, existing_version: int) -> NodeApply:
        """
        This is a convenience function for converting the read to a write node.

        It makes the simplifying assumption that all properties are from the same view. Note that this
        is not true in general.

        Args:
            source (ViewIdentifier | ContainerIdentifier): The view or container to with all the properties.
            existing_version (int): Fail the ingestion request if the node's version is greater than or equal to this value.
                                    If no existingVersion is specified, the ingestion will always overwrite any
                                    existing data for the edge (for the specified container or instance). If existingVersion is
                                    set to 0, the upsert will behave as an insert, so it will fail the bulk if the
                                    item already exists. If skipOnVersionConflict is set on the ingestion request,
                                    then the item will be skipped instead of failing the ingestion request.

        Returns:
            A write node, NodeApply

        """
        return NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=existing_version,
            sources=[
                NodeOrEdgeData(source=view_id, properties=properties) for view_id, properties in self.properties.items()
            ]
            if self.properties
            else None,
        )

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)


class NodeApplyResult(InstanceApplyResult):
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
        last_updated_time: int,
        created_time: int,
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

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)


class EdgeApply(InstanceApply):
    """An Edge. This is the write version of the edge.

    Args:
        space (str): The workspace for the edge, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the edge.
        type (DirectRelationReference): The type of edge.
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        existing_version (int): Fail the ingestion request if the node's version is greater than or equal to this value.
                                If no existingVersion is specified, the ingestion will always overwrite any
                                existing data for the edge (for the specified container or edge). If existingVersion is
                                set to 0, the upsert will behave as an insert, so it will fail the bulk if the
                                item already exists. If skipOnVersionConflict is set on the ingestion request,
                                then the item will be skipped instead of failing the ingestion request.
        sources (list[NodeOrEdgeData]): List of source properties to write. The properties are from the edge and/or
                        container the container(s) making up this node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        existing_version: Optional[int] = None,
        sources: Optional[list[NodeOrEdgeData]] = None,
    ):
        super().__init__(space, external_id, "edge", existing_version, sources)
        self.type = type if isinstance(type, DirectRelationReference) else DirectRelationReference.load(type)
        self.start_node = (
            start_node if isinstance(start_node, DirectRelationReference) else DirectRelationReference.load(start_node)
        )
        self.end_node = (
            end_node if isinstance(end_node, DirectRelationReference) else DirectRelationReference.load(end_node)
        )

    def as_id(self) -> EdgeId:
        return EdgeId(space=self.space, external_id=self.external_id)

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.type:
            output["type"] = self.type.dump(camel_case)
        if self.start_node:
            output["startNode" if camel_case else "start_node"] = self.start_node.dump(camel_case)
        if self.end_node:
            output["endNode" if camel_case else "end_node"] = self.end_node.dump(camel_case)
        return output

    @classmethod
    def load(cls, data: dict | str) -> EdgeApply:
        data = json.loads(data) if isinstance(data, str) else data
        instance = cast(EdgeApply, super().load(data))

        instance.type = DirectRelationReference.load(data["type"])
        instance.start_node = DirectRelationReference.load(data["startNode"])
        instance.end_node = DirectRelationReference.load(data["endNode"])
        return instance


class Edge(Instance):
    """An Edge.  This is the read version of the edge.

    Args:
        space (str): The workspace for the edge an unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the edge.
        version (str): DMS version.
        type (DirectRelationReference): The type of edge.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        deleted_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
                            Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        deleted_time: Optional[int] = None,
        properties: Optional[Properties] = None,
        **_: dict,
    ):
        super().__init__(space, external_id, version, last_updated_time, created_time, "edge", deleted_time, properties)
        self.type = type
        self.start_node = start_node
        self.end_node = end_node

    def as_apply(
        self, source: ViewIdentifier | ContainerIdentifier, existing_version: Optional[int] = None
    ) -> EdgeApply:
        """
        This is a convenience function for converting the read to a write edge.

        It makes the simplifying assumption that all properties are from the same view. Note that this
        is not true in general.

        Args:
            source (ViewIdentifier | ContainerIdentifier): The view or container to with all the properties.
            existing_version (int): Fail the ingestion request if the node's version is greater than or equal to this value.
                                    If no existingVersion is specified, the ingestion will always overwrite any
                                    existing data for the edge (for the specified container or instance). If existingVersion is
                                    set to 0, the upsert will behave as an insert, so it will fail the bulk if the
                                    item already exists. If skipOnVersionConflict is set on the ingestion request,
                                    then the item will be skipped instead of failing the ingestion request.

        Returns:
            A write edge, EdgeApply
        """
        return EdgeApply(
            space=self.space,
            external_id=self.external_id,
            type=self.type,
            start_node=self.start_node,
            end_node=self.end_node,
            existing_version=existing_version or None,
            sources=[
                NodeOrEdgeData(source=view_id, properties=properties) for view_id, properties in self.properties.items()
            ]
            or None,
        )

    def as_id(self) -> EdgeId:
        return EdgeId(space=self.space, external_id=self.external_id)

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.type:
            output["type"] = self.type.dump(camel_case)
        if self.start_node:
            output["startNode" if camel_case else "start_node"] = self.start_node.dump(camel_case)
        if self.end_node:
            output["endNode" if camel_case else "end_node"] = self.end_node.dump(camel_case)
        return output

    @classmethod
    def load(cls, data: dict | str) -> Edge:
        data = json.loads(data) if isinstance(data, str) else data
        instance = cast(Edge, super().load(data))

        instance.type = DirectRelationReference.load(data["type"])
        instance.start_node = DirectRelationReference.load(data["startNode"])
        instance.end_node = DirectRelationReference.load(data["endNode"])
        return instance


class EdgeApplyResult(InstanceApplyResult):
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
        last_updated_time: int,
        created_time: int,
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

    def as_id(self) -> EdgeId:
        return EdgeId(space=self.space, external_id=self.external_id)


class NodeApplyResultList(CogniteResourceList[NodeApplyResult]):
    _RESOURCE = NodeApplyResult

    def as_ids(self) -> list[NodeId]:
        return [result.as_id() for result in self]


class NodeList(CogniteResourceList[Node]):
    _RESOURCE = Node

    def as_ids(self) -> list[NodeId]:
        return [node.as_id() for node in self]


class NodeListWithCursor(NodeList):
    def __init__(self, resources: Collection[Any], cognite_client: Optional[CogniteClient] = None):
        super().__init__(resources, cognite_client)
        self.cursor: str | None = None


class EdgeApplyResultList(CogniteResourceList[EdgeApplyResult]):
    _RESOURCE = EdgeApplyResult

    def as_ids(self) -> list[EdgeId]:
        return [edge.as_id() for edge in self]


class EdgeList(CogniteResourceList[Edge]):
    _RESOURCE = Edge

    def as_ids(self) -> list[EdgeId]:
        return [edge.as_id() for edge in self]


class EdgeListWithCursor(EdgeList):
    def __init__(self, resources: Collection[Any], cognite_client: Optional[CogniteClient] = None):
        super().__init__(resources, cognite_client)
        self.cursor: str | None = None


class InstanceSort(CogniteFilter):
    def __init__(
        self,
        property: list[str] | tuple[str, ...],
        direction: Literal["ascending", "descending"] = "ascending",
        nulls_first: bool = False,
    ):
        self.property = property
        self.direction = direction
        self.nulls_first = nulls_first


@dataclass
class InstancesResult:
    """This represents the read result of an instance query

    Args:
        nodes (NodeList): A list of nodes.
        edges (EdgeList): A list of edges.

    """

    nodes: NodeList
    edges: EdgeList

    @classmethod
    def load(cls, data: str | dict) -> InstancesResult:
        raise NotImplementedError()


@dataclass
class InstancesApplyResult:
    """This represents the write result of an instance query

    Args:
        nodes (NodeApplyResultList): A list of nodes.
        edges (EdgeApplyResultList): A list of edges.

    """

    nodes: NodeApplyResultList
    edges: EdgeApplyResultList


@dataclass
class InstancesDeleteResult:
    """This represents the delete result of an instance query

    Args:
        nodes (list[NodeId]): A list of node ids.
        edges (list[EdgeId]): A list of edge ids.

    """

    nodes: list[NodeId]
    edges: list[EdgeId]
