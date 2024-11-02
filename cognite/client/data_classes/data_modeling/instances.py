from __future__ import annotations

import inspect
import threading
import warnings
from abc import ABC, abstractmethod
from collections import UserDict, defaultdict
from collections.abc import (
    Collection,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    MutableMapping,
    Sequence,
    ValuesView,
)
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    NoReturn,
    TypeAlias,
    TypeVar,
    cast,
    overload,
)

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
    T_CogniteResource,
    T_WriteClass,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.aggregations import AggregatedNumberedValue
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.core import (
    DataModelingResource,
    DataModelingSort,
    WritableDataModelingResource,
)
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelationReference,
    PropertyType,
    UnitReference,
    UnitSystemReference,
)
from cognite.client.data_classes.data_modeling.ids import (
    ContainerId,
    EdgeId,
    NodeId,
    ViewId,
    ViewIdentifier,
)
from cognite.client.utils._auxiliary import exactly_one_is_not_none, find_duplicates, flatten_dict
from cognite.client.utils._identifier import InstanceId
from cognite.client.utils._importing import local_import
from cognite.client.utils._text import convert_all_keys_to_snake_case, to_camel_case
from cognite.client.utils._time import convert_data_modelling_timestamp
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    import pandas as pd

    from cognite.client import CogniteClient

PropertyValue: TypeAlias = (
    str | int | float | bool | dict | list[str] | list[int] | list[float] | list[bool] | list[dict]
)
PropertyValueWrite: TypeAlias = (
    str
    | int
    | float
    | bool
    | dict
    | SequenceNotStr[str]
    | Sequence[int]
    | Sequence[float]
    | Sequence[bool]
    | Sequence[dict]
    | NodeId
    | DirectRelationReference
    | date
    | datetime
    | Sequence[NodeId | DirectRelationReference]
    | Sequence[date]
    | Sequence[datetime]
    | None
)

Space: TypeAlias = str
PropertyIdentifier: TypeAlias = str


@dataclass
class NodeOrEdgeData(CogniteObject):
    """This represents the data values of a node or edge.

    Args:
        source (ContainerId | ViewId): The container or view the node or edge property is in
        properties (Mapping[str, PropertyValue]): The properties of the node or edge.
    """

    source: ContainerId | ViewId
    properties: Mapping[str, PropertyValueWrite]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        try:
            source_type = resource["source"]["type"]
        except KeyError as e:
            raise ValueError("source must be a dict with a type key") from e
        source: ContainerId | ViewId
        if source_type == "container":
            source = ContainerId.load(resource["source"])
        elif source_type == "view":
            source = ViewId.load(resource["source"])
        else:
            raise ValueError(f"source type must be container or view, but was {source_type}")
        return cls(
            source=source,
            properties=resource["properties"],
        )

    def dump(self, camel_case: bool = True) -> dict:
        properties = _PropertyValueSerializer.serialize_values(self.properties, camel_case)
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
        instance_type (Literal['node', 'edge']): The type of instance.
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
        instance_type (Literal['node', 'edge']): The type of instance.
        existing_version (int | None): Fail the ingestion request if the instance's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the instance (for the specified container or instance). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the instance already exists. If skipOnVersionConflict is set on the ingestion request, then the instance will be skipped instead of failing the ingestion request.
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
        self.__sources = sources or []

    @property
    def sources(self) -> list[NodeOrEdgeData]:
        return self.__sources

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
            "instanceType" if camel_case else "instance_type": self.instance_type,
        }
        if self.existing_version is not None:
            output["existingVersion" if camel_case else "existing_version"] = self.existing_version
        if self.sources:
            output["sources"] = [source.dump(camel_case) for source in self.sources]
        return output

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
                    warnings.warn(
                        f"Unknown type of view id: {view_id_str}, expected format <external_id>/<version>. "
                        "Skipping...",
                        stacklevel=2,
                    )
                    continue
                view_id = ViewId.load((space, *view_tuple))
                props[view_id] = properties
        return cls(props)

    def dump(self) -> dict[Space, dict[str, dict[PropertyIdentifier, PropertyValue]]]:
        props: dict[Space, dict[str, dict[PropertyIdentifier, PropertyValue]]] = defaultdict(dict)
        for view_id, properties in self.data.items():
            view_id_str = f"{view_id.external_id}/{view_id.version}"
            props[view_id.space][view_id_str] = cast(dict[PropertyIdentifier, PropertyValue], properties)
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
    def get(self, view: ViewIdentifier) -> MutableMapping[PropertyIdentifier, PropertyValue] | None: ...

    @overload
    def get(
        self, view: ViewIdentifier, default: MutableMapping[PropertyIdentifier, PropertyValue] | _T
    ) -> MutableMapping[PropertyIdentifier, PropertyValue] | _T: ...

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

    def _repr_html_(self) -> str:
        pd = local_import("pandas")
        index_names = "space", "view", "version"
        if not self:
            df = pd.DataFrame(index=pd.MultiIndex(levels=([], [], []), codes=([], [], []), names=index_names))
        else:
            df = pd.DataFrame.from_dict(
                {view_id.as_tuple(): props for view_id, props in self.data.items()},
                orient="index",
            )
            df.index.names = index_names
        return df._repr_html_()


class Instance(WritableInstanceCore[T_CogniteResource], ABC):
    """A node or edge. This is the read version of the instance.

    Args:
        space (str): The workspace for the instance, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the instance.
        version (int): Current version of the instance.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        instance_type (Literal['node', 'edge']): The type of instance.
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
        self.__properties: Properties = properties or Properties({})

        if not isinstance(self, TypedInstance) and len(self.properties) == 1:
            (self.__prop_lookup,) = self.properties.values()
        else:
            # For speed, we want this to fail (to avoid LBYL pattern):
            self.__prop_lookup = None  # type: ignore [assignment]

    def __raise_if_non_singular_source(self, attr: str) -> NoReturn:
        if isinstance(self, TypedInstance):
            raise AttributeError(f"For typed instances, use direct attribute access: `instance.{attr}`")

        err_msg = "Quick property access is only possible on instances from a single source."
        if len(self.properties) > 1:
            err_msg += f" Hint: You may use `instance.properties[view_id][{attr!r}]`"
        raise RuntimeError(err_msg) from None

    @property
    def properties(self) -> Properties:
        return self.__properties

    @overload
    def get(self, attr: str) -> PropertyValue | None: ...

    @overload
    def get(self, attr: str, default: _T) -> PropertyValue | _T: ...

    def get(self, attr: str, default: Any = None) -> Any:
        try:
            return self.__prop_lookup.get(attr, default)
        except AttributeError:
            self.__raise_if_non_singular_source(attr)

    def __getitem__(self, attr: str) -> PropertyValue:
        try:
            return self.__prop_lookup[attr]
        except TypeError:
            self.__raise_if_non_singular_source(attr)

    def __setitem__(self, attr: str, value: PropertyValue) -> None:
        try:
            self.__prop_lookup[attr] = value
        except TypeError:
            self.__raise_if_non_singular_source(attr)

    def __delitem__(self, attr: str) -> None:
        try:
            del self.__prop_lookup[attr]
        except TypeError:
            self.__raise_if_non_singular_source(attr)

    def __contains__(self, attr: str) -> bool:
        try:
            return attr in self.__prop_lookup
        except TypeError:
            self.__raise_if_non_singular_source(attr)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
            "version": self.version,
            "lastUpdatedTime" if camel_case else "last_updated_time": self.last_updated_time,
            "createdTime" if camel_case else "created_time": self.created_time,
            "instanceType" if camel_case else "instance_type": self.instance_type,
            "properties": self.properties.dump(),
        }
        if self.deleted_time is not None:
            dumped["deletedTime" if camel_case else "deleted_time"] = self.deleted_time
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
            **kwargs (Any): For backwards compatibility.

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
            view_id, *extra = self.properties.keys()
            # We only do/allow this if we have a single source:
            if not extra:
                prop_df.columns = prop_df.columns.str.removeprefix("{}.{}/{}.".format(*view_id.as_tuple()))
                if isinstance(self, TypedInstance):
                    attr_name_mapping = self._get_descriptor_property_name_mapping()
                    prop_df = prop_df.rename(columns=attr_name_mapping)
            else:
                warnings.warn(
                    "Can't remove view ID prefix from expanded property rows as source was not unique",
                    RuntimeWarning,
                )
        return pd.concat((col, prop_df.T.squeeze(axis=1))).to_frame(name="value")

    @abstractmethod
    def as_apply(self) -> InstanceApply:
        """Convert the instance to an apply instance."""
        raise NotImplementedError


class InstanceApplyResult(InstanceCore, ABC):
    """A node or edge. This represents the update on the instance.

    Args:
        instance_type (Literal['node', 'edge']): The type of instance.
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
            group=cast(dict[str, str | int | float | bool], resource.get("group")),
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
        self.type = DirectRelationReference.load(type) if type else None

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> NodeApply:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            existing_version=resource.get("existingVersion"),
            sources=[NodeOrEdgeData.load(source) for source in resource.get("sources", [])] or None,
            type=DirectRelationReference.load(resource["type"]) if "type" in resource else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.type:
            output["type"] = self.type.dump(camel_case)
        return output

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)

    def as_write(self) -> Self:
        """Returns this NodeApply instance"""
        return self


class Node(Instance[NodeApply]):
    """A node. This is the read version of the node.

    Args:
        space (str): The workspace for the node, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the node.
        version (int): Current version of the node.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
        properties (Properties | None): Properties of the node.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
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
        type: DirectRelationReference | tuple[str, str] | None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, "node", deleted_time, properties)
        self.type = DirectRelationReference.load(type) if type else None

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
        version (int): Current version of the node.
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
        self.type = DirectRelationReference.load(type)
        self.start_node = DirectRelationReference.load(start_node)
        self.end_node = DirectRelationReference.load(end_node)

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
        version (int): Current version of the edge.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
        properties (Properties | None): No description.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        type: DirectRelationReference | tuple[str, str],
        last_updated_time: int,
        created_time: int,
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        deleted_time: int | None,
        properties: Properties | None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, "edge", deleted_time, properties)
        self.type = DirectRelationReference.load(type)
        self.start_node = DirectRelationReference.load(start_node)
        self.end_node = DirectRelationReference.load(end_node)

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
        version (int): Current version of the edge.
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


T_Instance = TypeVar("T_Instance", bound=Instance)


class DataModelingInstancesList(WriteableCogniteResourceList[T_WriteClass, T_Instance], ABC):
    def _build_id_mappings(self) -> None:
        self._instance_id_to_item = {(inst.space, inst.external_id): inst for inst in self.data}
        # We allow fast lookup via external ID when not ambiguous:
        self._ambiguous_xids = find_duplicates(inst.external_id for inst in self.data)
        self._ext_id_to_item = {inst.external_id: inst for inst in self.data}

    def get(  # type: ignore [override]
        self,
        instance_id: InstanceId | tuple[str, str] | None = None,
        external_id: str | None = None,
        *,
        id: InstanceId | tuple[str, str] | None = None,
    ) -> T_Instance | None:
        """Get an instance from this list by instance ID.

        Args:
            instance_id (InstanceId | tuple[str, str] | None): The instance ID to get. A tuple on the form (space, external_id) is also accepted.
            external_id (str | None): The external ID of the instance to return. Will raise ValueError when ambiguous (in presence of multiple spaces).
            id (InstanceId | tuple[str, str] | None): (DEPRECATED) Backwards-compatible alias for instance_id. Will be removed in the next major version.

        Returns:
            T_Instance | None: The requested instance if present, else None
        """
        if not exactly_one_is_not_none(instance_id, external_id, id):
            raise ValueError(
                "Pass exactly one of 'instance_id' or 'external_id' ('id' is a deprecated alias for 'instance_id'). "
                "Using an external ID requires all instances to be from the same space."
            )
        if id is not None:
            instance_id = id
            warnings.warn(
                "Calling .get using `id` is deprecated and will be removed in the next major version. "
                "Use 'instance_id' instead",
                UserWarning,
            )
        elif external_id is not None:
            if external_id in self._ambiguous_xids:
                raise ValueError(
                    f"{external_id=} is ambiguous (multiple spaces are present). Pass 'instance_id' instead."
                )
            return self._ext_id_to_item.get(external_id)

        if isinstance(instance_id, InstanceId):
            instance_id = instance_id.as_tuple()
        return self._instance_id_to_item.get(instance_id)  # type: ignore [arg-type]

    def extend(self, other: Iterable[Any]) -> None:
        other_res_list = type(self)(other)  # See if we can accept the types
        if self._instance_id_to_item.keys().isdisjoint(other_res_list._instance_id_to_item):
            self.data.extend(other_res_list.data)
            self._build_id_mappings()
        else:
            raise ValueError("Unable to extend as this would introduce duplicates")

    def to_pandas(  # type: ignore [override]
        self,
        camel_case: bool = False,
        convert_timestamps: bool = True,
        expand_properties: bool = False,
        remove_property_prefix: bool = True,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Convert the instance into a pandas DataFrame. Note that if the properties column is expanded and there are
        keys in the metadata that already exist in the DataFrame, then an error will be raised by pandas during joining.

        Args:
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`). Does not apply to properties.
            convert_timestamps (bool): Convert known columns storing CDF timestamps (milliseconds since epoch) to datetime. Does not affect properties.
            expand_properties (bool): Expand the properties into separate columns. Note: Will change default to True in the next major version.
            remove_property_prefix (bool): Remove view ID prefix from columns names of expanded properties. Requires data to be from a single view.
            **kwargs (Any): For backwards compatibility.

        Returns:
            pd.DataFrame: The Cognite resource as a dataframe.
        """
        kwargs.pop("expand_metadata", None), kwargs.pop("metadata_prefix", None)
        if kwargs:
            raise TypeError(f"Unsupported keyword arguments: {kwargs}")
        if not expand_properties:
            warnings.warn(
                "Keyword argument 'expand_properties' will change default from False to True in the next major version.",
                DeprecationWarning,
            )
        df = super().to_pandas(camel_case=camel_case, expand_metadata=False, convert_timestamps=convert_timestamps)
        if not expand_properties or "properties" not in df.columns:
            return df

        prop_df = local_import("pandas").json_normalize(df.pop("properties"), max_level=2)
        if remove_property_prefix and not prop_df.empty:
            typed_view_ids = {item.get_source() for item in self if isinstance(item, TypedInstance)}  # type: ignore [attr-defined]
            view_id, *extra = typed_view_ids | set(vid for item in self for vid in item.properties)
            # We only do/allow this if we have a single source:
            if not extra:
                prop_df.columns = prop_df.columns.str.removeprefix("{}.{}/{}.".format(*view_id.as_tuple()))
                if len(self) > 0 and isinstance(self[0], TypedInstance):
                    attr_name_mapping = self[0]._get_descriptor_property_name_mapping()
                    prop_df = prop_df.rename(columns=attr_name_mapping)
            else:
                warnings.warn(
                    "Can't remove view ID prefix from expanded property columns as multiple sources exist",
                    RuntimeWarning,
                )
        return df.join(prop_df)


T_Node = TypeVar("T_Node", bound=Node)


class NodeList(DataModelingInstancesList[NodeApply, T_Node]):
    _RESOURCE = Node  # type: ignore[assignment]

    def __init__(
        self,
        resources: Collection[Any],
        typing: TypeInformation | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(resources, cognite_client)
        self.typing = typing

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

    @classmethod
    def _load_raw_api_response(cls, responses: list[dict[str, Any]], cognite_client: CogniteClient) -> Self:
        typing = next((TypeInformation._load(resp["typing"]) for resp in responses if "typing" in resp), None)
        resources = [
            cls._RESOURCE._load(item, cognite_client=cognite_client)  # type: ignore[has-type]
            for response in responses
            for item in response.get("items", [])
        ]
        return cls(resources, typing, cognite_client=cognite_client)

    def dump_raw(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "items": self.dump(camel_case),
        }
        if self.typing:
            output["typing"] = self.typing.dump(camel_case)
        return output


class NodeListWithCursor(NodeList[T_Node]):
    def __init__(
        self,
        resources: Collection[Any],
        cursor: str | None,
        typing: TypeInformation | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(resources, typing, cognite_client)
        self.cursor = cursor

    def extend(self, other: NodeListWithCursor) -> None:  # type: ignore[override]
        if not isinstance(other, type(self)):
            raise TypeError("Unable to extend as the types do not match")
        other_res_list = type(self)(other, other.cursor)  # See if we can accept the types
        if self._instance_id_to_item.keys().isdisjoint(other_res_list._instance_id_to_item):
            self.data.extend(other.data)
            self._build_id_mappings()
            self.cursor = other.cursor
        else:
            raise ValueError("Unable to extend as this would introduce duplicates")


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


T_Edge = TypeVar("T_Edge", bound=Edge)


class EdgeList(DataModelingInstancesList[EdgeApply, T_Edge]):
    _RESOURCE = Edge  # type: ignore[assignment]

    def __init__(
        self,
        resources: Collection[Any],
        typing: TypeInformation | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(resources, cognite_client)
        self.typing = typing

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

    @classmethod
    def _load_raw_api_response(cls, responses: list[dict[str, Any]], cognite_client: CogniteClient) -> Self:
        typing = next((TypeInformation._load(resp["typing"]) for resp in responses if "typing" in resp), None)
        resources = [
            cls._RESOURCE._load(item, cognite_client=cognite_client)  # type: ignore[has-type]
            for response in responses
            for item in response.get("items", [])
        ]
        return cls(resources, typing, cognite_client=cognite_client)

    def dump_raw(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "items": self.dump(camel_case),
        }
        if self.typing:
            output["typing"] = self.typing.dump(camel_case)
        return output


class EdgeListWithCursor(EdgeList):
    def __init__(
        self,
        resources: Collection[Any],
        cursor: str | None,
        typing: TypeInformation | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(resources, typing, cognite_client)
        self.cursor = cursor

    def extend(self, other: EdgeListWithCursor) -> None:  # type: ignore[override]
        if not isinstance(other, type(self)):
            raise TypeError("Unable to extend as the types do not match")
        other_res_list = type(self)(other, other.cursor)  # See if we can accept the types
        if self._instance_id_to_item.keys().isdisjoint(other_res_list._instance_id_to_item):
            self.data.extend(other.data)
            self._build_id_mappings()
            self.cursor = other.cursor
        else:
            raise ValueError("Unable to extend as this would introduce duplicates")


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
        nulls_first: bool | None = None,
    ) -> None:
        super().__init__(property, direction, nulls_first)


@dataclass
class InstancesResult(Generic[T_Node, T_Edge]):
    """This represents the read result of an instance query

    Args:
        nodes (NodeList): A list of nodes.
        edges (EdgeList): A list of edges.

    """

    nodes: NodeList[T_Node]
    edges: EdgeList[T_Edge]


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


@dataclass
class TargetUnit(CogniteObject):
    property: str
    unit: UnitReference | UnitSystemReference

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"property": self.property, "unit": self.unit.dump(camel_case)}

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> TargetUnit:
        return cls(
            property=resource["property"],
            unit=UnitReference.load(resource["unit"])
            if "externalId" in resource["unit"]
            else UnitSystemReference.load(resource["unit"]),
        )


@dataclass
class TypePropertyDefinition(CogniteObject):
    type: PropertyType
    nullable: bool = True
    auto_increment: bool = False
    immutable: bool = False
    default_value: str | int | dict | None = None
    name: str | None = None
    description: str | None = None

    def dump(self, camel_case: bool = True, return_flat_dict: bool = False) -> dict[str, Any]:
        output: dict[str, Any] = {}
        if return_flat_dict:
            dumped = flatten_dict(self.type.dump(camel_case), ("type",), sep=".")
            output.update({key: value for key, value in dumped.items()})
        else:
            output["type"] = self.type.dump(camel_case)
        output.update(
            {
                "nullable": self.nullable,
                "autoIncrement": self.auto_increment,
                "defaultValue": self.default_value,
                "name": self.name,
                "description": self.description,
                "immutable": self.immutable,
            }
        )
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> TypePropertyDefinition:
        return cls(
            type=PropertyType.load(resource["type"]),
            nullable=resource.get("nullable"),  # type: ignore[arg-type]
            immutable=resource.get("immutable"),  # type: ignore[arg-type]
            auto_increment=resource.get("autoIncrement"),  # type: ignore[arg-type]
            default_value=resource.get("defaultValue"),
            name=resource.get("name"),
            description=resource.get("description"),
        )


class TypeInformation(UserDict, CogniteObject):
    def __init__(self, data: dict[str, dict[str, dict[str, TypePropertyDefinition]]] | None = None) -> None:
        super().__init__(data or {})

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for space_name, space_data in self.items():
            output.setdefault(space_name, {})
            for view_or_container_id, view_data in space_data.items():
                output[space_name].setdefault(view_or_container_id, {})
                for type_name, type_data in view_data.items():
                    output[space_name][view_or_container_id][type_name] = type_data.dump(camel_case)
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> TypeInformation:
        return cls(
            {
                space_name: {
                    view_or_container_id: {
                        type_name: TypePropertyDefinition.load(type_data)
                        for type_name, type_data in view_data.items()
                        if isinstance(type_data, dict)
                    }
                    for view_or_container_id, view_data in space_data.items()
                    if isinstance(view_data, dict)
                }
                for space_name, space_data in resource.items()
                if isinstance(space_data, dict)
            }
        )

    def to_pandas(self) -> pd.DataFrame:
        pd = local_import("pandas")

        index_names = "space_name", "view_or_container"
        if not self:
            df = pd.DataFrame(index=pd.MultiIndex(levels=([], []), codes=([], []), names=index_names))
        else:
            df = pd.DataFrame.from_dict(
                {
                    (space_name, view_or_container_id): {
                        "identifier": type_name,
                        **type_data.dump(camel_case=False, return_flat_dict=True),
                    }
                    for space_name, space_data in self.data.items()
                    for view_or_container_id, view_data in space_data.items()
                    for type_name, type_data in view_data.items()
                },
                orient="index",
            )
            df.index.names = index_names
        return df

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()

    @overload
    def __getitem__(self, item: str) -> dict[str, dict[str, TypePropertyDefinition]]: ...

    @overload
    def __getitem__(self, item: tuple[str, str]) -> dict[str, TypePropertyDefinition]: ...

    @overload
    def __getitem__(self, item: tuple[str, str, str]) -> TypePropertyDefinition: ...

    def __getitem__(
        self, item: str | tuple[str, str] | tuple[str, str, str]
    ) -> dict[str, dict[str, TypePropertyDefinition]] | dict[str, TypePropertyDefinition] | TypePropertyDefinition:
        if isinstance(item, str):
            return super().__getitem__(item)
        elif isinstance(item, tuple) and len(item) == 2:
            return super().__getitem__(item[0])[item[1]]
        elif isinstance(item, tuple) and len(item) == 3:
            return super().__getitem__(item[0])[item[1]][item[2]]
        else:
            raise ValueError(f"Invalid key: {item}")

    def __setitem__(self, key: str | tuple[str, str] | tuple[str, str, str], value: Any) -> None:
        if isinstance(key, str):
            super().__setitem__(key, value)
        elif isinstance(key, tuple) and len(key) == 2:
            super().__setitem__(key[0], {key[1]: value})
        elif isinstance(key, tuple) and len(key) == 3:
            super().__setitem__(key[0], {key[1]: {key[2]: value}})
        else:
            raise ValueError(f"Invalid key: {key}")

    def __delitem__(self, key: str | tuple[str, str] | tuple[str, str, str]) -> None:
        if isinstance(key, str):
            super().__delitem__(key)
        elif isinstance(key, tuple) and len(key) == 2:
            del self[key[0]][key[1]]
        elif isinstance(key, tuple) and len(key) == 3:
            del self[key[0]][key[1]][key[2]]
        else:
            raise ValueError(f"Invalid key: {key}")


class PropertyOptions:
    """This is a descriptor class for instance properties in a typed class.

    It is used when you have a property that has a different name in the Data Model
    compared to the name in the Python class.

    Args:
        identifier (str | None): The name of the property in the Data Model. Defaults to the name of the property in the Python class.
    """

    def __init__(self, identifier: str | None = None) -> None:
        self.name = cast(str, identifier)  # mypy help, set_name guarantees str

    def __set_name__(self, owner: type, attribute_name: str) -> None:
        self.name = self.name or attribute_name
        if self.name in _RESERVED_PROPERTY_NAMES:
            self.name = _RESERVED_PROPERTY_CONFLICT_PREFIX + self.name

    def __get__(self, instance: TypedInstance, owner: type) -> Any:
        try:
            return instance.__dict__[self.name]
        except KeyError:
            raise AttributeError(f"'{owner.__name__}' object has no attribute '{self.name}'")

    def __set__(self, instance: TypedInstance, value: Any) -> None:
        try:
            instance.__dict__[self.name] = value
        except KeyError:
            raise AttributeError(f"'{instance.__class__.__name__}' object has no attribute '{self.name}'")

    def __delete__(self, instance: TypedInstance) -> None:
        try:
            del instance.__dict__[self.name]
        except KeyError:
            raise AttributeError(f"'{instance.__class__.__name__}' object has no attribute '{self.name}'")

    @property
    def property_name(self) -> str:
        return self.resolve_property(self.name)

    @staticmethod
    def resolve_property(name: str) -> str:
        if name.startswith(_RESERVED_PROPERTY_CONFLICT_PREFIX):
            return name[len(_RESERVED_PROPERTY_CONFLICT_PREFIX) :]
        return name


class TypedInstance(ABC):
    @classmethod
    @abstractmethod
    def get_source(cls) -> ViewId:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def _base_properties() -> set[str]:
        raise NotImplementedError

    def _dump_properties(self) -> dict[str, Any]:
        props = {key: prop for key, prop in vars(self).items() if key not in _RESERVED_PROPERTY_NAMES}
        return _PropertyValueSerializer.serialize_values(props, camel_case=True)

    @classmethod
    @lru_cache(32)
    def _get_constructor_parameters(cls) -> dict[str, inspect.Parameter]:
        return {key: param for key, param in inspect.signature(cls.__init__).parameters.items() if key != "self"}

    @classmethod
    def _load_instance(cls, resource: dict[str, Any], properties: dict[str, Any]) -> Self:
        return cls(**cls._load_base_properties(resource), **cls._load_properties(properties))

    @classmethod
    def _load_base_properties(cls, resource: dict[str, Any]) -> dict[str, Any]:
        args: dict[str, Any] = {}
        signature_params = cls._get_constructor_parameters()
        for key in cls._base_properties():
            camel_key = to_camel_case(key)
            if key in signature_params:
                param = signature_params[key]
                default = param.default
                if camel_key in resource:
                    args[key] = cls._deserialize_values(resource[camel_key], param)
                elif default is inspect.Parameter.empty:
                    args[key] = None
                else:
                    args[key] = cls._deserialize_values(default or None, param)
        return args

    @classmethod
    def _load_properties(cls, resource: dict[str, Any]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        property_by_name = cls._get_properties_by_name()
        signature_params = cls._get_constructor_parameters()
        for name, parameter in signature_params.items():
            if name in cls._base_properties():
                continue
            if name in resource:
                output[name] = cls._deserialize_values(resource[name], parameter)
            elif name in property_by_name:
                prop = property_by_name[name].property_name
                if prop in resource:
                    output[name] = cls._deserialize_values(resource[prop], parameter)
        return output

    @classmethod
    @lru_cache(32)
    def _get_descriptor_property_name_mapping(cls) -> MappingProxyType[str, str]:
        return MappingProxyType(
            {
                v.property_name: k
                for base_class in cls.__mro__[:-1]
                for k, v in vars(base_class).items()
                if isinstance(v, PropertyOptions)
            }
        )

    @classmethod
    @lru_cache(32)
    def _get_user_defined_typed_instance_base_classes(cls) -> list[type]:
        # We want to exclude TypedInstance and its direct subclasses and all its superclasses.
        # So, only the base classes up to TypedNode, TypedNodeApply, etc...
        typed_instance_classes = {TypedInstance, *TypedInstance.__subclasses__(), *TypedInstance.mro()}
        return [base for base in cls.mro() if base not in typed_instance_classes]

    @classmethod
    def _get_properties_by_name(cls) -> dict[str, PropertyOptions]:
        property_by_name: dict[str, PropertyOptions] = {}
        for current_cls in cls._get_user_defined_typed_instance_base_classes():
            for name, value in vars(current_cls).items():
                if isinstance(value, PropertyOptions):
                    property_by_name[name] = value
        return property_by_name

    @classmethod
    def _deserialize_values(cls, value: Any, parameter: inspect.Parameter) -> Any:
        if isinstance(value, SequenceNotStr):
            # TODO, performance: Deserialize list of values should not inspect parameter/value repeatedly
            return [cls._deserialize_value(v, parameter) for v in value]
        else:
            return cls._deserialize_value(value, parameter)

    @staticmethod
    def _deserialize_value(value: Any, parameter: inspect.Parameter) -> Any:
        if parameter.annotation is inspect.Parameter.empty:
            return value
        annotation = str(parameter.annotation)
        if "datetime" in annotation and isinstance(value, str):
            return convert_data_modelling_timestamp(value)
        elif "date" in annotation and isinstance(value, str):
            return date.fromisoformat(value)
        elif isinstance(value, dict):
            if DirectRelationReference.__name__ in annotation:
                return DirectRelationReference.load(value)
            elif NodeId.__name__ in annotation:
                return NodeId.load(value)
            elif EdgeId.__name__ in annotation:
                return EdgeId.load(value)
        return value


class TypedNodeApply(NodeApply, TypedInstance):
    def __init__(
        self,
        space: str,
        external_id: str,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, existing_version, type=type)

    @staticmethod
    @lru_cache(1)
    def _base_properties() -> set[str]:
        return set(TypedNodeApply._get_constructor_parameters().keys())

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        sources = resource.pop("sources", [])
        properties = sources[0].get("properties", {}) if sources else {}
        return cls._load_instance(resource, properties)

    @property
    def sources(self) -> list[NodeOrEdgeData]:
        return [NodeOrEdgeData(source=self.get_source(), properties=self._dump_properties())]


class TypedEdgeApply(EdgeApply, TypedInstance):
    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        existing_version: int | None = None,
    ) -> None:
        super().__init__(space, external_id, type, start_node, end_node, existing_version)

    @staticmethod
    @lru_cache(1)
    def _base_properties() -> set[str]:
        return set(TypedEdgeApply._get_constructor_parameters().keys())

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        sources = resource.pop("sources", [])
        properties = sources[0].get("properties", {}) if sources else {}
        return cls._load_instance(resource, properties)

    @property
    def sources(self) -> list[NodeOrEdgeData]:
        return [NodeOrEdgeData(source=self.get_source(), properties=self._dump_properties())]


class TypedNode(Node, TypedInstance):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        deleted_time: int | None,
        type: DirectRelationReference | tuple[str, str] | None,
    ) -> None:
        super().__init__(
            space, external_id, version, last_updated_time, created_time, deleted_time, properties=None, type=type
        )

    @staticmethod
    @lru_cache(1)
    def _base_properties() -> set[str]:
        return set(TypedNode._get_constructor_parameters().keys())

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        all_properties = resource.pop("properties", {})
        source = cls.get_source()
        properties = all_properties.get(source.space, {}).get(source.as_source_identifier(), {})
        return cls._load_instance(resource, properties)

    @property
    def properties(self) -> Properties:
        return Properties({self.get_source(): self._dump_properties()})


class TypedEdge(Edge, TypedInstance):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        type: DirectRelationReference | tuple[str, str],
        last_updated_time: int,
        created_time: int,
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        deleted_time: int | None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            type,
            last_updated_time,
            created_time,
            start_node,
            end_node,
            deleted_time,
            properties=None,
        )

    @staticmethod
    @lru_cache(1)
    def _base_properties() -> set[str]:
        return set(TypedEdge._get_constructor_parameters().keys())

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        all_properties = resource.pop("properties", {})
        source = cls.get_source()
        properties = all_properties.get(source.space, {}).get(source.as_source_identifier(), {})
        return cls._load_instance(resource, properties)

    @property
    def properties(self) -> Properties:
        return Properties({self.get_source(): self._dump_properties()})


_RESERVED_PROPERTY_CONFLICT_PREFIX = "__________"
_RESERVED_PROPERTY_NAMES = (
    TypedNodeApply._base_properties()
    | TypedEdgeApply._base_properties()
    | TypedNode._base_properties()
    | TypedEdge._base_properties()
) | {"instance_type", "existing_version", "_InstanceApply__sources", "_Instance__properties", "_Instance__prop_lookup"}


class _PropertyValueSerializer:
    @classmethod
    def serialize_values(cls, props: Mapping[str, Any], camel_case: bool = True) -> dict[str, PropertyValue]:
        properties: dict[str, Any] = {}
        for key, value in props.items():
            key = PropertyOptions.resolve_property(key)
            if isinstance(value, SequenceNotStr):
                properties[key] = [cls._serialize_value(v, camel_case) for v in value]
            else:
                properties[key] = cls._serialize_value(value, camel_case)
        return properties

    @staticmethod
    def _serialize_value(value: Any, camel_case: bool) -> PropertyValue:
        if isinstance(value, NodeId):
            # We don't want to dump the instance_type field when serializing NodeId in this context
            return value.dump(camel_case, include_instance_type=False)
        elif isinstance(value, DirectRelationReference):
            return value.dump(camel_case)
        elif isinstance(value, datetime):
            return value.isoformat(timespec="milliseconds")
        elif isinstance(value, date):
            return value.isoformat()
        else:
            return value
