from __future__ import annotations

import threading
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
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
    TypeVar,
    Union,
    ValuesView,
    cast,
    overload,
)

from typing_extensions import Self, TypeAlias

from cognite.client.data_classes._base import CogniteResourceList, T_CogniteResource
from cognite.client.data_classes.aggregations import AggregatedNumberedValue
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.core import (
    DataModelingInstancesList,
    DataModelingResource,
    DataModelingSort,
    WritableDataModelingResource,
)
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelationReference,
)
from cognite.client.data_classes.data_modeling.ids import (
    ContainerId,
    EdgeId,
    NodeId,
    ViewId,
    ViewIdentifier,
)
from cognite.client.utils._importing import local_import
from cognite.client.utils._text import convert_all_keys_to_snake_case

if TYPE_CHECKING:
    import pandas as pd

    from cognite.client import CogniteClient

PropertyValue: TypeAlias = Union[
    str,
    int,
    float,
    bool,
    dict,
    List[str],
    List[int],
    List[float],
    List[bool],
    List[dict],
    NodeId,
    DirectRelationReference,
]
Space: TypeAlias = str
PropertyIdentifier: TypeAlias = str


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
        try:
            source_type = data["source"]["type"]
        except KeyError as e:
            raise ValueError("source must be a dict with a type key") from e
        source: ContainerId | ViewId
        if source_type == "container":
            source = ContainerId.load(data["source"])
        elif source_type == "view":
            source = ViewId.load(data["source"])
        else:
            raise ValueError(f"source type must be container or view, but was {source_type}")
        return cls(
            source=source,
            properties=data["properties"],
        )

    def dump(self, camel_case: bool = True) -> dict:
        properties: dict[str, PropertyValue] = {}
        for key, value in self.properties.items():
            if isinstance(value, NodeId):
                # We don't want to dump the instance_type field when serializing NodeId in this context
                properties[key] = value.dump(camel_case, include_instance_type=False)
            elif isinstance(value, DirectRelationReference):
                properties[key] = value.dump(camel_case)
            else:
                properties[key] = value
        output: dict[str, Any] = {"properties": properties}
        if self.source:
            if isinstance(self.source, (ContainerId, ViewId)):
                output["source"] = self.source.dump(camel_case)
            elif isinstance(self.source, dict):
                output["source"] = self.source
            else:
                raise TypeError(f"source must be ContainerId, ViewId or a dict, but was {type(self.source)}")
        return output


class InstanceCore(DataModelingResource, ABC):
    """A node or edge
    Args:
        space (str): The workspace for the instance, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
        instance_type (Literal["node", "edge"]): No description.
    """

    def __init__(self, space: str, external_id: str, instance_type: Literal["node", "edge"]) -> None:
        super().__init__(space=space)
        self.instance_type = instance_type
        self.external_id = external_id


class WritableInstanceCore(WritableDataModelingResource[T_CogniteResource], ABC):
    def __init__(self, space: str, external_id: str, instance_type: Literal["node", "edge"]) -> None:
        super().__init__(space=space)
        self.instance_type = instance_type
        self.external_id = external_id


class InstanceApply(WritableInstanceCore[T_CogniteResource], ABC):
    """A node or edge. This is the write version of the instance.

    Args:
        space (str): The workspace for the instance, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
        instance_type (Literal["node", "edge"]): No description.
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        sources (list[NodeOrEdgeData] | None): List of source properties to write. The properties are from the instance and/or container the container(s) making up this node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        instance_type: Literal["node", "edge"] = "node",
        existing_version: int | None = None,
        sources: list[NodeOrEdgeData] | None = None,
    ) -> None:
        validate_data_modeling_identifier(space, external_id)
        super().__init__(space, external_id, instance_type)
        self.existing_version = existing_version
        self.sources = sources

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        resource = convert_all_keys_to_snake_case(resource)
        if resource["instance_type"] == "node":
            return cast(Self, NodeApply._load(resource))
        if resource["instance_type"] == "edge":
            return cast(Self, EdgeApply._load(resource))
        raise ValueError(f"instance_type must be node or edge, but was {resource['instance_type']}")


_T = TypeVar("_T")


class Properties(MutableMapping[ViewIdentifier, MutableMapping[PropertyIdentifier, PropertyValue]]):
    def __init__(self, properties: MutableMapping[ViewId, MutableMapping[PropertyIdentifier, PropertyValue]]) -> None:
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
                view_id = ViewId.load((space, *view_tuple))
                props[view_id] = properties
        return cls(props)

    def dump(self) -> dict[Space, dict[str, dict[PropertyIdentifier, PropertyValue]]]:
        props: dict[Space, dict[str, dict[PropertyIdentifier, PropertyValue]]] = defaultdict(dict)
        for view_id, properties in self.data.items():
            view_id_str = f"{view_id.external_id}/{view_id.version}"
            props[view_id.space][view_id_str] = cast(Dict[PropertyIdentifier, PropertyValue], properties)
        # Defaultdict is not yaml serializable
        return dict(props)

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
    def get(self, view: ViewIdentifier) -> MutableMapping[PropertyIdentifier, PropertyValue] | None:
        ...

    @overload
    def get(
        self, view: ViewIdentifier, default: MutableMapping[PropertyIdentifier, PropertyValue] | _T
    ) -> MutableMapping[PropertyIdentifier, PropertyValue] | _T:
        ...

    def get(
        self,
        view: ViewIdentifier,
        default: MutableMapping[PropertyIdentifier, PropertyValue] | None | _T | None = None,
    ) -> MutableMapping[PropertyIdentifier, PropertyValue] | None | _T:
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


class Instance(WritableInstanceCore[T_CogniteResource], ABC):
    """A node or edge. This is the read version of the instance.

    Args:
        space (str): The workspace for the instance, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        instance_type (Literal["node", "edge"]): The type of instance.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
        properties (Properties | None): Properties of the instance.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        instance_type: Literal["node", "edge"],
        deleted_time: int | None,
        properties: Properties | None,
    ) -> None:
        super().__init__(space, external_id, instance_type)
        self.version = version
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self.deleted_time = deleted_time
        self.properties: Properties = properties or Properties({})

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case)
        if "properties" in dumped:
            dumped["properties"] = self.properties.dump()
        return dumped

    def to_pandas(  # type: ignore [override]
        self,
        ignore: list[str] | None = None,
        camel_case: bool = False,
        convert_timestamps: bool = True,
        expand_properties: bool = False,
        remove_property_prefix: bool = True,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Convert the instance into a pandas DataFrame.

        Args:
            ignore (list[str] | None): List of row keys to skip when converting to a data frame. Is applied before expansions.
            camel_case (bool): Convert attribute names to camel case (e.g. `externalId` instead of `external_id`). Does not affect properties if expanded.
            convert_timestamps (bool): Convert known attributes storing CDF timestamps (milliseconds since epoch) to datetime. Does not affect properties.
            expand_properties (bool): Expand the properties into separate rows. Note: Will change default to True in the next major version.
            remove_property_prefix (bool): Remove view ID prefix from row names of expanded properties (in index). Requires data to be from a single view.
            **kwargs (Any): For backwards compatability.

        Returns:
            pd.DataFrame: The dataframe.
        """
        kwargs.pop("expand_metadata", None), kwargs.pop("metadata_prefix", None)
        if kwargs:
            raise TypeError(f"Unsupported keyword arguments: {kwargs}")
        if not expand_properties:
            warnings.warn(
                "Keyword argument 'expand_properties' will change default from False to True in the next major version.",
                DeprecationWarning,
            )
        df = super().to_pandas(
            expand_metadata=False, ignore=ignore, camel_case=camel_case, convert_timestamps=convert_timestamps
        )
        if not expand_properties or "properties" not in df.index:
            return df

        pd = local_import("pandas")
        col = df.squeeze()
        prop_df = pd.json_normalize(col.pop("properties"), max_level=2)
        if remove_property_prefix and not prop_df.empty:
            # We only do/allow this if we have a single source:
            view_id, *extra = self.properties.keys()
            if not extra:
                prop_df.columns = prop_df.columns.str.removeprefix("{}.{}/{}.".format(*view_id.as_tuple()))
            else:
                warnings.warn(
                    "Can't remove view ID prefix from expanded property rows as source was not unique",
                    RuntimeWarning,
                )
        return pd.concat((col, prop_df.T.squeeze())).to_frame(name="value")

    @abstractmethod
    def as_apply(self) -> InstanceApply:
        """Convert the instance to an apply instance."""
        raise NotImplementedError()


class InstanceApplyResult(InstanceCore, ABC):
    """A node or edge. This represents the update on the instance.

    Args:
        instance_type (Literal["node", "edge"]): The type of instance.
        space (str): The workspace for the instance, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
        version (int): DMS version of the instance.
        was_modified (bool): Whether the instance was modified by the ingestion.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        instance_type: Literal["node", "edge"],
        space: str,
        external_id: str,
        version: int,
        was_modified: bool,
        last_updated_time: int,
        created_time: int,
    ) -> None:
        super().__init__(space, external_id, instance_type)
        self.version = version
        self.was_modified = was_modified
        self.last_updated_time = last_updated_time
        self.created_time = created_time


class InstanceAggregationResult(DataModelingResource):
    """Represents instances aggregation results.

    Args:
        aggregates (list[AggregatedNumberedValue]): List of aggregated values.
        group (dict[str, str | int | float | bool]): The grouping used for the aggregation.
    """

    def __init__(self, aggregates: list[AggregatedNumberedValue], group: dict[str, str | int | float | bool]) -> None:
        self.aggregates = aggregates
        self.group = group

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        """
        Loads an instance from a json string or dictionary.

        Args:
            resource (dict): No description.
            cognite_client (CogniteClient | None): No description.

        Returns:
            Self: An instance.

        """
        return cls(
            aggregates=[AggregatedNumberedValue.load(agg) for agg in resource["aggregates"]],
            group=cast(Dict[str, Union[str, int, float, bool]], resource.get("group")),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """
        Dumps the aggregation results to a dictionary.

        Args:
            camel_case (bool): Whether to convert the keys to camel case.

        Returns:
            dict[str, Any]: A dictionary with the instance results.

        """
        return {
            "aggregates": [agg.dump(camel_case) for agg in self.aggregates],
            "group": self.group,
        }


class InstanceAggregationResultList(CogniteResourceList[InstanceAggregationResult]):
    _RESOURCE = InstanceAggregationResult


class NodeApply(InstanceApply["NodeApply"]):
    """A node. This is the write version of the node.

    Args:
        space (str): The workspace for the node, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the node.
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        sources (list[NodeOrEdgeData] | None): List of source properties to write. The properties are from the node and/or container the container(s) making up this node.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        existing_version: int | None = None,
        sources: list[NodeOrEdgeData] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, "node", existing_version, sources)
        if isinstance(type, tuple):
            self.type: DirectRelationReference | None = DirectRelationReference.load(type)
        else:
            self.type = type

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.sources:
            output["sources"] = [source.dump(camel_case) for source in self.sources]
        if self.type:
            output["type"] = self.type.dump(camel_case)
        return output

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> NodeApply:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            existing_version=resource.get("existingVersion"),
            sources=[NodeOrEdgeData.load(source) for source in resource.get("sources", [])] or None,
            type=DirectRelationReference.load(resource["type"]) if "type" in resource else None,
        )

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)

    def as_write(self) -> NodeApply:
        """Returns this NodeApply instance"""
        return self


class Node(Instance["NodeApply"]):
    """A node. This is the read version of the node.

    Args:
        space (str): The workspace for the node, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the node.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
        properties (Properties | None): Properties of the node.
        type (DirectRelationReference | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        deleted_time: int | None,
        properties: Properties | None,
        type: DirectRelationReference | None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, "node", deleted_time, properties)
        self.type = type

    def as_apply(self) -> NodeApply:
        """
        This is a convenience function for converting the read to a write node.

        It makes the simplifying assumption that all properties are from the same view. Note that this
        is not true in general.

        Returns:
            NodeApply: A write node, NodeApply

        """
        return NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.version,
            sources=[
                NodeOrEdgeData(source=view_id, properties=properties) for view_id, properties in self.properties.items()
            ]
            or None,
            type=self.type,
        )

    def as_write(self) -> NodeApply:
        return self.as_apply()

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.type:
            output["type"] = self.type.dump(camel_case)
        return output

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Node:
        return Node(
            space=resource["space"],
            external_id=resource["externalId"],
            version=resource["version"],
            last_updated_time=resource["lastUpdatedTime"],
            created_time=resource["createdTime"],
            deleted_time=resource.get("deletedTime"),
            properties=Properties.load(resource["properties"]) if "properties" in resource else None,
            type=DirectRelationReference.load(resource["type"]) if "type" in resource else None,
        )


class NodeApplyResult(InstanceApplyResult):
    """A node. This represents the update on the node.

    Args:
        space (str): The workspace for the node, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the node.
        version (int): DMS version of the node.
        was_modified (bool): Whether the node was modified by the ingestion.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self, space: str, external_id: str, version: int, was_modified: bool, last_updated_time: int, created_time: int
    ) -> None:
        super().__init__(
            instance_type="node",
            space=space,
            external_id=external_id,
            version=version,
            was_modified=was_modified,
            last_updated_time=last_updated_time,
            created_time=created_time,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=resource["version"],
            was_modified=resource["wasModified"],
            last_updated_time=resource["lastUpdatedTime"],
            created_time=resource["createdTime"],
        )

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)


class EdgeApply(InstanceApply["EdgeApply"]):
    """An Edge. This is the write version of the edge.

    Args:
        space (str): The workspace for the edge, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the edge.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        sources (list[NodeOrEdgeData] | None): List of source properties to write. The properties are from the edge and/or container the container(s) making up this node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        existing_version: int | None = None,
        sources: list[NodeOrEdgeData] | None = None,
    ) -> None:
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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.sources:
            output["sources"] = [source.dump(camel_case) for source in self.sources]
        if self.type:
            output["type"] = self.type.dump(camel_case)
        if self.start_node:
            output["startNode" if camel_case else "start_node"] = self.start_node.dump(camel_case)
        if self.end_node:
            output["endNode" if camel_case else "end_node"] = self.end_node.dump(camel_case)
        return output

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            existing_version=resource.get("existingVersion"),
            sources=[NodeOrEdgeData.load(source) for source in resource.get("sources", [])] or None,
            type=DirectRelationReference.load(resource["type"]),
            start_node=DirectRelationReference.load(resource["startNode"]),
            end_node=DirectRelationReference.load(resource["endNode"]),
        )

    def as_write(self) -> EdgeApply:
        """Returns this EdgeApply instance"""
        return self


class Edge(Instance[EdgeApply]):
    """An Edge. This is the read version of the edge.

    Args:
        space (str): The workspace for the edge, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the edge.
        version (int): DMS version.
        type (DirectRelationReference): The type of edge.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
        properties (Properties | None): No description.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        type: DirectRelationReference,
        last_updated_time: int,
        created_time: int,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        deleted_time: int | None,
        properties: Properties | None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, "edge", deleted_time, properties)
        self.type = type
        self.start_node = start_node
        self.end_node = end_node

    def as_apply(self) -> EdgeApply:
        """
        This is a convenience function for converting the read to a write edge.

        It makes the simplifying assumption that all properties are from the same view. Note that this
        is not true in general.

        Returns:
            EdgeApply: A write edge, EdgeApply
        """
        return EdgeApply(
            space=self.space,
            external_id=self.external_id,
            type=self.type,
            start_node=self.start_node,
            end_node=self.end_node,
            existing_version=self.version,
            sources=[
                NodeOrEdgeData(source=view_id, properties=properties) for view_id, properties in self.properties.items()
            ]
            or None,
        )

    def as_write(self) -> EdgeApply:
        return self.as_apply()

    def as_id(self) -> EdgeId:
        return EdgeId(space=self.space, external_id=self.external_id)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.type:
            output["type"] = self.type.dump(camel_case)
        if self.start_node:
            output["startNode" if camel_case else "start_node"] = self.start_node.dump(camel_case)
        if self.end_node:
            output["endNode" if camel_case else "end_node"] = self.end_node.dump(camel_case)
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            type=DirectRelationReference.load(resource["type"]),
            version=resource["version"],
            last_updated_time=resource["lastUpdatedTime"],
            created_time=resource["createdTime"],
            start_node=DirectRelationReference.load(resource["startNode"]),
            end_node=DirectRelationReference.load(resource["endNode"]),
            deleted_time=resource.get("deletedTime"),
            properties=Properties.load(resource["properties"]) if "properties" in resource else None,
        )


class EdgeApplyResult(InstanceApplyResult):
    """An Edge. This represents the update on the edge.

    Args:
        space (str): The workspace for the edge, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the edge.
        version (int): DMS version.
        was_modified (bool): Whether the edge was modified by the ingestion.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self, space: str, external_id: str, version: int, was_modified: bool, last_updated_time: int, created_time: int
    ) -> None:
        super().__init__(
            instance_type="edge",
            space=space,
            external_id=external_id,
            version=version,
            was_modified=was_modified,
            last_updated_time=last_updated_time,
            created_time=created_time,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=resource["version"],
            was_modified=resource["wasModified"],
            last_updated_time=resource["lastUpdatedTime"],
            created_time=resource["createdTime"],
        )

    def as_id(self) -> EdgeId:
        return EdgeId(space=self.space, external_id=self.external_id)


class NodeApplyResultList(CogniteResourceList[NodeApplyResult]):
    _RESOURCE = NodeApplyResult

    def as_ids(self) -> list[NodeId]:
        """
        Convert the list of nodes to a list of node ids.

        Returns:
            list[NodeId]: A list of node ids.
        """
        return [result.as_id() for result in self]


class NodeApplyList(CogniteResourceList[NodeApply]):
    _RESOURCE = NodeApply

    def as_ids(self) -> list[NodeId]:
        """
        Convert the list of nodes to a list of node ids.

        Returns:
            list[NodeId]: A list of node ids.
        """
        return [node.as_id() for node in self]


class NodeList(DataModelingInstancesList[NodeApply, Node]):
    _RESOURCE = Node

    def as_ids(self) -> list[NodeId]:
        """
        Convert the list of nodes to a list of node ids.

        Returns:
            list[NodeId]: A list of node ids.
        """
        return [node.as_id() for node in self]

    def as_write(self) -> NodeApplyList:
        """Returns this NodeList as a NodeApplyList"""
        return NodeApplyList([node.as_write() for node in self])


class NodeListWithCursor(NodeList):
    def __init__(
        self, resources: Collection[Any], cursor: str | None, cognite_client: CogniteClient | None = None
    ) -> None:
        super().__init__(resources, cognite_client)
        self.cursor = cursor


class EdgeApplyResultList(CogniteResourceList[EdgeApplyResult]):
    _RESOURCE = EdgeApplyResult

    def as_ids(self) -> list[EdgeId]:
        """
        Convert the list of edges to a list of edge ids.

        Returns:
            list[EdgeId]: A list of edge ids.
        """
        return [edge.as_id() for edge in self]


class EdgeApplyList(CogniteResourceList[EdgeApply]):
    _RESOURCE = EdgeApply

    def as_ids(self) -> list[EdgeId]:
        """
        Convert the list of edges to a list of edge ids.

        Returns:
            list[EdgeId]: A list of edge ids.
        """
        return [edge.as_id() for edge in self]


class EdgeList(DataModelingInstancesList[EdgeApply, Edge]):
    _RESOURCE = Edge

    def as_ids(self) -> list[EdgeId]:
        """
        Convert the list of edges to a list of edge ids.

        Returns:
            list[EdgeId]: A list of edge ids.
        """
        return [edge.as_id() for edge in self]

    def as_write(self) -> EdgeApplyList:
        """Returns this EdgeList as a EdgeApplyList"""
        return EdgeApplyList([edge.as_write() for edge in self], cognite_client=self._get_cognite_client())


class EdgeListWithCursor(EdgeList):
    def __init__(
        self, resources: Collection[Any], cursor: str | None, cognite_client: CogniteClient | None = None
    ) -> None:
        super().__init__(resources, cognite_client)
        self.cursor = cursor


# This is a utility class. It is not used by in the SDK codebase, but used in projects that use the SDK.
@dataclass
class InstancesApply:
    """
    This represents the write request of an instance query
    Args:
        nodes (NodeApplyList): A list of nodes.
        edges (EdgeApplyList): A list of edges.
    """

    nodes: NodeApplyList
    edges: EdgeApplyList


class InstanceSort(DataModelingSort):
    def __init__(
        self,
        property: list[str] | tuple[str, ...],
        direction: Literal["ascending", "descending"] = "ascending",
        nulls_first: bool = False,
    ) -> None:
        super().__init__(property, direction, nulls_first)


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


@dataclass
class SubscriptionContext:
    last_successful_sync: datetime | None = None
    last_successful_callback: datetime | None = None
    _canceled: bool = False
    _thread: threading.Thread | None = None

    def cancel(self) -> None:
        self._canceled = True

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
