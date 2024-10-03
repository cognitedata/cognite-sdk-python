from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, TypeVar, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteObject,
    CogniteResourceList,
    UnknownCogniteObject,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.core import DataModelingSchemaResource
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelation,
    DirectRelationReference,
    PropertyType,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId, PropertyId, ViewId
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._text import convert_all_keys_to_camel_case_recursive, to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class ViewCore(DataModelingSchemaResource["ViewApply"], ABC):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        description: str | None,
        name: str | None,
        filter: Filter | None,
        implements: list[ViewId] | None,
    ) -> None:
        super().__init__(space=space, external_id=external_id, name=name, description=description)
        self.filter = filter
        self.implements: list[ViewId] = implements or []
        self.version = version

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)

        if self.implements:
            output["implements"] = [v.dump(camel_case) for v in self.implements]
        if self.filter:
            output["filter"] = self.filter.dump()

        return output

    def as_id(self) -> ViewId:
        return ViewId(
            space=self.space,
            external_id=self.external_id,
            version=self.version,
        )

    def as_property_ref(self, property: str) -> tuple[str, str, str]:
        return self.as_id().as_property_ref(property)


class ViewApply(ViewCore):
    """A group of properties. Write only version.

    Args:
        space (str): The workspace for the view, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        version (str): DMS version.
        description (str | None): Textual description of the view
        name (str | None): Human readable name for the view.
        filter (Filter | None): A filter Domain Specific Language (DSL) used to create advanced filter queries.
        implements (list[ViewId] | None): References to the views from where this view will inherit properties and edges.
        properties (dict[str, ViewPropertyApply] | None): No description.

    .. note::
        The order of elements (i.e., `ViewId`) in :code:`implements` matters, as it indicates priority on how to handle
        collisions of same properties from different views.
        See docs on
        `implemented property conflicts <https://docs.cognite.com/cdf/dm/dm_concepts/dm_containers_views_datamodels/#implemented-property-conflicts-and-precedence>`_
        for more details.

    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        description: str | None = None,
        name: str | None = None,
        filter: Filter | None = None,
        implements: list[ViewId] | None = None,
        properties: dict[str, ViewPropertyApply] | None = None,
    ) -> None:
        validate_data_modeling_identifier(space, external_id)
        super().__init__(space, external_id, version, description, name, filter, implements)
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        properties = (
            {k: ViewPropertyApply.load(v) for k, v in resource["properties"].items()}
            if "properties" in resource
            else None
        )
        implements = [ViewId.load(v) for v in resource["implements"]] if "implements" in resource else None
        filter = Filter.load(resource["filter"]) if "filter" in resource else None
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=resource["version"],
            description=resource.get("description"),
            name=resource.get("name"),
            filter=filter,
            implements=implements,
            properties=properties,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if "properties" in output:
            output["properties"] = {k: v.dump(camel_case) for k, v in output["properties"].items()}

        return output

    def as_write(self) -> ViewApply:
        """Returns this ViewApply instance."""
        return self

    def referenced_containers(self) -> set[ContainerId]:
        """Helper function to get the set of containers referenced by this view.

        Returns:
            set[ContainerId]: The set of containers referenced by this view.
        """
        referenced_containers = set()
        for prop in (self.properties or {}).values():
            if isinstance(prop, MappedPropertyApply):
                referenced_containers.add(prop.container)
        return referenced_containers


class View(ViewCore):
    """A group of properties. Read only version.

    Args:
        space (str): The workspace for the view, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        version (str): DMS version.
        properties (dict[str, ViewProperty]): View with included properties and expected edges, indexed by a unique space-local identifier.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description (str | None): Textual description of the view
        name (str | None): Human readable name for the view.
        filter (Filter | None): A filter Domain Specific Language (DSL) used to create advanced filter queries.
        implements (list[ViewId] | None): References to the views from where this view will inherit properties and edges.
        writable (bool): Whether the view supports write operations.
        used_for (Literal['node', 'edge', 'all']): Does this view apply to nodes, edges or both.
        is_global (bool): Whether this is a global view.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        properties: dict[str, ViewProperty],
        last_updated_time: int,
        created_time: int,
        description: str | None,
        name: str | None,
        filter: Filter | None,
        implements: list[ViewId] | None,
        writable: bool,
        used_for: Literal["node", "edge", "all"],
        is_global: bool,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            description,
            name,
            filter,
            implements,
        )
        self.writable = writable
        self.used_for = used_for
        self.is_global = is_global
        self.properties = properties
        self.last_updated_time = last_updated_time
        self.created_time = created_time

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> View:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=resource["version"],
            description=resource.get("description"),
            name=resource.get("name"),
            last_updated_time=resource["lastUpdatedTime"],
            created_time=resource["createdTime"],
            filter=Filter.load(resource["filter"]) if "filter" in resource else None,
            implements=[ViewId.load(v) for v in resource["implements"]] if "implements" in resource else None,
            writable=resource["writable"],
            used_for=resource["usedFor"],
            is_global=resource["isGlobal"],
            properties={k: ViewProperty.load(v) for k, v in resource.get("properties", {}).items()},
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if "properties" in output:
            output["properties"] = {k: v.dump(camel_case) for k, v in output["properties"].items()}

        return output

    def as_apply(self) -> ViewApply:
        """Convert to a view applies.

        Returns:
            ViewApply: The view apply.
        """
        properties: dict[str, ViewPropertyApply] | None = None
        if self.properties:
            for k, v in self.properties.items():
                if isinstance(
                    v,
                    (
                        MappedProperty,
                        SingleEdgeConnection,
                        MultiEdgeConnection,
                        SingleReverseDirectRelation,
                        MultiReverseDirectRelation,
                    ),
                ):
                    if properties is None:
                        properties = {}
                    properties[k] = v.as_apply()
                else:
                    raise NotImplementedError(f"Unsupported conversion to apply for property type {type(v)}")

        return ViewApply(
            space=self.space,
            external_id=self.external_id,
            version=self.version,
            description=self.description,
            name=self.name,
            filter=self.filter,
            implements=self.implements,
            properties=properties,
        )

    def as_write(self) -> ViewApply:
        return self.as_apply()

    def referenced_containers(self) -> set[ContainerId]:
        """Helper function to get the set of containers referenced by this view.

        Returns:
            set[ContainerId]: The set of containers referenced by this view.
        """
        referenced_containers = set()
        for prop in self.properties.values():
            if isinstance(prop, MappedProperty):
                referenced_containers.add(prop.container)
        return referenced_containers


class ViewApplyList(CogniteResourceList[ViewApply]):
    _RESOURCE = ViewApply

    def as_ids(self) -> list[ViewId]:
        """Returns the list of ViewIds

        Returns:
            list[ViewId]: The list of ViewIds
        """
        return [v.as_id() for v in self]

    def referenced_containers(self) -> set[ContainerId]:
        """Helper function to get the set of containers referenced by this view.

        Returns:
            set[ContainerId]: The set of containers referenced by this view.
        """
        referenced_containers = set()
        for view in self:
            referenced_containers.update(view.referenced_containers())
        return referenced_containers


class ViewList(WriteableCogniteResourceList[ViewApply, View]):
    _RESOURCE = View

    def as_apply(self) -> ViewApplyList:
        """Convert to a view an apply list.

        Returns:
            ViewApplyList: The view apply list.
        """
        return ViewApplyList(resources=[v.as_apply() for v in self])

    def as_ids(self) -> list[ViewId]:
        """Returns the list of ViewIds

        Returns:
            list[ViewId]: The list of ViewIds
        """
        return [v.as_id() for v in self]

    def as_write(self) -> ViewApplyList:
        return self.as_apply()

    def referenced_containers(self) -> set[ContainerId]:
        """Helper function to get the set of containers referenced by this view.

        Returns:
            set[ContainerId]: The set of containers referenced by this view.
        """
        referenced_containers = set()
        for view in self:
            referenced_containers.update(view.referenced_containers())
        return referenced_containers


class ViewFilter(CogniteFilter):
    """Represent the filer arguments for the list endpoint.

    Args:
        space (str | None): The space to query
        include_inherited_properties (bool): Whether to include properties inherited from views this view implements.
        all_versions (bool): Whether to return all versions. If false, only the newest version is returned, which is determined based on the 'createdTime' field.
        include_global (bool): Whether to include global views.
    """

    def __init__(
        self,
        space: str | None = None,
        include_inherited_properties: bool = True,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> None:
        self.space = space
        self.include_inherited_properties = include_inherited_properties
        self.all_versions = all_versions
        self.include_global = include_global


class ViewProperty(CogniteObject, ABC):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        if "connectionType" in resource:
            return cast(Self, ConnectionDefinition.load(resource))
        elif "direction" in resource:
            warnings.warn(
                "Connection Definition is missing field 'connectionType'. Loading default MultiEdgeConnection. "
                "This will be required in the next major version",
                DeprecationWarning,
            )
            return cast(Self, MultiEdgeConnection.load(resource))
        else:
            return cast(Self, MappedProperty.load(resource))

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        raise NotImplementedError


class ViewPropertyApply(CogniteObject, ABC):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        if "connectionType" in resource:
            return cast(Self, ConnectionDefinitionApply.load(resource))
        elif "direction" in resource:
            warnings.warn(
                "Connection Definition is missing field 'connectionType'. Loading default MultiEdgeConnection. "
                "This will be required in the next major version",
                DeprecationWarning,
            )
            return cast(Self, MultiEdgeConnectionApply.load(resource))
        else:
            return cast(Self, MappedPropertyApply.load(resource))

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        raise NotImplementedError


@dataclass
class MappedPropertyApply(ViewPropertyApply):
    container: ContainerId
    container_property_identifier: str
    name: str | None = None
    description: str | None = None
    source: ViewId | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            container=ContainerId.load(resource["container"]),
            container_property_identifier=resource["containerPropertyIdentifier"],
            name=resource.get("name"),
            description=resource.get("description"),
            source=ViewId.load(resource["source"]) if "source" in resource else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        key = "containerPropertyIdentifier" if camel_case else "container_property_identifier"
        output: dict[str, Any] = {
            "container": self.container.dump(camel_case, include_type=True),
            key: self.container_property_identifier,
        }
        if self.name is not None:
            output["name"] = self.name
        if self.description is not None:
            output["description"] = self.description
        if self.source is not None:
            output["source"] = self.source.dump(camel_case, include_type=True)

        return output


@dataclass
class MappedProperty(ViewProperty):
    container: ContainerId
    container_property_identifier: str
    type: PropertyType
    nullable: bool
    immutable: bool
    auto_increment: bool
    source: ViewId | None = None
    default_value: str | int | dict | None = None
    name: str | None = None
    description: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource["type"]
        source = type_.get("source", None) or resource.get("source")

        return cls(
            container=ContainerId.load(resource["container"]),
            container_property_identifier=resource["containerPropertyIdentifier"],
            type=PropertyType.load({k: v for k, v in type_.items() if k != "source"}),
            nullable=resource["nullable"],
            immutable=resource["immutable"],
            auto_increment=resource["autoIncrement"],
            source=ViewId.load(source) if source else None,
            default_value=resource.get("defaultValue"),
            name=resource.get("name"),
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = asdict(self)
        output["type"] = self.type.dump(camel_case)
        if self.source and isinstance(self.type, DirectRelation):
            output["type"]["source"] = output.pop("source", None)
        if camel_case:
            return convert_all_keys_to_camel_case_recursive(output)
        return output

    def as_apply(self) -> MappedPropertyApply:
        return MappedPropertyApply(
            container=self.container,
            container_property_identifier=self.container_property_identifier,
            name=self.name,
            description=self.description,
            source=self.source,
        )


@dataclass
class ConnectionDefinition(ViewProperty, ABC):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        if "connectionType" not in resource:
            raise ValueError(f"{cls.__name__} must have a connectionType")
        connection_type = to_snake_case(resource["connectionType"])

        if connection_type == "single_edge_connection":
            return cast(Self, SingleEdgeConnection.load(resource))
        if connection_type == "multi_edge_connection":
            return cast(Self, MultiEdgeConnection.load(resource))
        if connection_type == "single_reverse_direct_relation":
            return cast(Self, SingleReverseDirectRelation.load(resource))
        if connection_type == "multi_reverse_direct_relation":
            return cast(Self, MultiReverseDirectRelation.load(resource))

        return cast(Self, UnknownCogniteObject(resource))

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        raise NotImplementedError


@dataclass
class EdgeConnection(ConnectionDefinition, ABC):
    """Describes the edge(s) that are likely to exist to aid in discovery and documentation of the view.
    A listed edge is not required. i.e. It does not have to exist when included in this list.
    A connection has a max distance of one hop.

    Args:
        type (DirectRelationReference): Reference to the node pointed to by the direct relation. The reference
            consists of a space and an external-id.
        source (ViewId): The target node(s) of this connection can be read through the view specified in 'source'.
        name (str | None): Readable property name.
        description (str | None): Description of the content and suggested use for this property.
        edge_source (ViewId | None): The edge(s) of this connection can be read through the view specified in
            'edgeSource'.
        direction (Literal["outwards", "inwards"]): The direction of the edge. The outward direction is used to
            indicate that the edge points from the source to the target. The inward direction is used to indicate
            that the edge points from the target to the source.
    """

    type: DirectRelationReference
    source: ViewId
    name: str | None
    description: str | None
    edge_source: ViewId | None
    direction: Literal["outwards", "inwards"]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = cls(
            type=DirectRelationReference.load(resource["type"]),
            source=ViewId.load(resource["source"]),
            name=resource.get("name"),
            description=resource.get("description"),
            edge_source=(edge_source := resource.get("edgeSource")) and ViewId.load(edge_source),
            direction=resource["direction"],
        )

        return instance

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = asdict(self)
        if self.type:
            output["type"] = self.type.dump(camel_case)
        if self.source:
            output["source"] = self.source.dump(camel_case)
        if self.edge_source:
            output["edge_source"] = self.edge_source.dump(camel_case)

        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output


@dataclass
class SingleEdgeConnection(EdgeConnection):
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = super().dump(camel_case)
        if camel_case:
            output["connectionType"] = "single_edge_connection"
        else:
            output["connection_type"] = "single_edge_connection"

        return output

    def as_apply(self) -> SingleEdgeConnectionApply:
        return SingleEdgeConnectionApply(
            type=self.type,
            source=self.source,
            name=self.name,
            description=self.description,
            edge_source=self.edge_source,
            direction=self.direction,
        )


@dataclass
class MultiEdgeConnection(EdgeConnection):
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = super().dump(camel_case)
        if camel_case:
            output["connectionType"] = "multi_edge_connection"
        else:
            output["connection_type"] = "multi_edge_connection"

        return output

    def as_apply(self) -> MultiEdgeConnectionApply:
        return MultiEdgeConnectionApply(
            type=self.type,
            source=self.source,
            name=self.name,
            description=self.description,
            edge_source=self.edge_source,
            direction=self.direction,
        )


SingleHopConnectionDefinition: TypeAlias = MultiEdgeConnection


@dataclass
class ReverseDirectRelation(ConnectionDefinition, ABC):
    """Describes the direct relation(s) pointing to instances read through this view. This connection type is used to
    aid in discovery and documentation of the view

    It is called 'ReverseDirectRelationConnection' in the API spec.

    Args:
        source (ViewId): The node(s) containing the direct relation property can be read through
            the view specified in 'source'.
        through (PropertyId): The view or container of the node containing the direct relation property.
        name (str | None): Readable property name.
        description (str | None): Description of the content and suggested use for this property.

    """

    source: ViewId
    through: PropertyId
    name: str | None = None
    description: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            source=ViewId.load(resource["source"]),
            through=PropertyId.load(resource["through"]),
            name=resource.get("name"),
            description=resource.get("description"),
        )

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "source": self.source.dump(camel_case),
            "through": self.through.dump(camel_case),
            "name": self.name,
            "description": self.description,
        }


@dataclass
class SingleReverseDirectRelation(ReverseDirectRelation):
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = super().dump(camel_case)
        if camel_case:
            output["connectionType"] = "single_reverse_direct_relation"
        else:
            output["connection_type"] = "single_reverse_direct_relation"

        return output

    def as_apply(self) -> SingleReverseDirectRelationApply:
        return SingleReverseDirectRelationApply(
            source=self.source,
            through=self.through,
            name=self.name,
            description=self.description,
        )


@dataclass
class MultiReverseDirectRelation(ReverseDirectRelation):
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = super().dump(camel_case)
        if camel_case:
            output["connectionType"] = "multi_reverse_direct_relation"
        else:
            output["connection_type"] = "multi_reverse_direct_relation"

        return output

    def as_apply(self) -> MultiReverseDirectRelationApply:
        return MultiReverseDirectRelationApply(
            source=self.source,
            through=self.through,
            name=self.name,
            description=self.description,
        )


@dataclass
class ConnectionDefinitionApply(ViewPropertyApply, ABC):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        if "connectionType" not in resource:
            raise ValueError(f"{cls.__name__} must have a connectionType")
        connection_type = to_snake_case(resource["connectionType"])

        if connection_type == "single_edge_connection":
            return cast(Self, SingleEdgeConnectionApply.load(resource))
        if connection_type == "multi_edge_connection":
            return cast(Self, MultiEdgeConnectionApply.load(resource))
        if connection_type == "single_reverse_direct_relation":
            return cast(Self, SingleReverseDirectRelationApply.load(resource))
        if connection_type == "multi_reverse_direct_relation":
            return cast(Self, MultiReverseDirectRelationApply.load(resource))
        return cast(Self, UnknownCogniteObject(resource))

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        raise NotImplementedError


T_ConnectionDefinitionApply = TypeVar("T_ConnectionDefinitionApply", bound=ConnectionDefinitionApply)


@dataclass
class EdgeConnectionApply(ConnectionDefinitionApply, ABC):
    """Describes the edge(s) that are likely to exist to aid in discovery and documentation of the view.
    A listed edge is not required. i.e. It does not have to exist when included in this list.
    A connection has a max distance of one hop.

    It is called 'EdgeConnection' in the API spec.

    Args:
        type (DirectRelationReference): Reference to the node pointed to by the direct relation. The reference
            consists of a space and an external-id.
        source (ViewId): The target node(s) of this connection can be read through the view specified in 'source'.
        name (str | None): Readable property name.
        description (str | None): Description of the content and suggested use for this property.
        edge_source (ViewId | None): The edge(s) of this connection can be read through the view specified in
            'edgeSource'.
        direction (Literal["outwards", "inwards"]): The direction of the edge. The outward direction is used to
            indicate that the edge points from the source to the target. The inward direction is used to indicate
            that the edge points from the target to the source.
    """

    type: DirectRelationReference
    source: ViewId
    name: str | None = None
    description: str | None = None
    edge_source: ViewId | None = None
    direction: Literal["outwards", "inwards"] = "outwards"

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = cls(
            type=DirectRelationReference.load(resource["type"]),
            source=ViewId.load(resource["source"]),
            name=resource.get("name"),
            description=resource.get("description"),
            edge_source=(edge_source := resource.get("edgeSource")) and ViewId.load(edge_source),
        )
        if "direction" in resource:
            instance.direction = resource["direction"]
        return instance

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict:
        output: dict[str, Any] = {
            "type": self.type.dump(camel_case),
            "source": self.source.dump(camel_case, include_type=True),
            "direction": self.direction,
        }
        if self.name is not None:
            output["name"] = self.name
        if self.description is not None:
            output["description"] = self.description
        if self.edge_source is not None:
            output[("edgeSource" if camel_case else "edge_source")] = self.edge_source.dump(
                camel_case, include_type=True
            )

        return output


@dataclass
class SingleEdgeConnectionApply(EdgeConnectionApply):
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = super().dump(camel_case)
        if camel_case:
            output["connectionType"] = "single_edge_connection"
        else:
            output["connection_type"] = "single_edge_connection"

        return output


@dataclass
class MultiEdgeConnectionApply(EdgeConnectionApply):
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = super().dump(camel_case)
        if camel_case:
            output["connectionType"] = "multi_edge_connection"
        else:
            output["connection_type"] = "multi_edge_connection"

        return output


SingleHopConnectionDefinitionApply: TypeAlias = MultiEdgeConnectionApply


@dataclass
class ReverseDirectRelationApply(ConnectionDefinitionApply, ABC):
    """Describes the direct relation(s) pointing to instances read through this view. This connection type is used to
    aid in discovery and documentation of the view.

    It is called 'ReverseDirectRelationConnection' in the API spec.

    Args:
        source (ViewId): The node(s) containing the direct relation property can be read through
            the view specified in 'source'.
        through (PropertyId): The view or container of the node containing the direct relation property.
        name (str | None): Readable property name.
        description (str | None): Description of the content and suggested use for this property.

    """

    source: ViewId
    through: PropertyId
    name: str | None = None
    description: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = cls(
            source=ViewId.load(resource["source"]),
            through=PropertyId.load(resource["through"]),
        )
        if "name" in resource:
            instance.name = resource["name"]
        if "description" in resource:
            instance.description = resource["description"]

        return instance

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "source": self.source.dump(camel_case, include_type=True),
            "through": self.through.dump(camel_case),
        }
        if self.name is not None:
            output["name"] = self.name
        if self.description is not None:
            output["description"] = self.description

        return output


@dataclass
class SingleReverseDirectRelationApply(ReverseDirectRelationApply):
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = super().dump(camel_case)
        if camel_case:
            output["connectionType"] = "single_reverse_direct_relation"
        else:
            output["connection_type"] = "single_reverse_direct_relation"

        return output


@dataclass
class MultiReverseDirectRelationApply(ReverseDirectRelationApply):
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = super().dump(camel_case)
        if camel_case:
            output["connectionType"] = "multi_reverse_direct_relation"
        else:
            output["connection_type"] = "multi_reverse_direct_relation"

        return output
