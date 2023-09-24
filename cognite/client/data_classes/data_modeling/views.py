from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Literal, TypeVar, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling._core import DataModelingResource
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelation,
    DirectRelationReference,
    PropertyType,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._text import (
    convert_all_keys_to_camel_case_recursive,
    convert_all_keys_to_snake_case,
)


class ViewCore(DataModelingResource):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        description: str | None = None,
        name: str | None = None,
        filter: Filter | None = None,
        implements: list[ViewId] | None = None,
        **_: Any,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.description = description
        self.name = name
        self.filter = filter
        self.implements = implements
        self.version = version

    @classmethod
    def load(cls, resource: dict | str) -> ViewCore:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "implements" in data:
            data["implements"] = [ViewId.load(v) for v in data["implements"]] or None
        if "filter" in data:
            data["filter"] = Filter.load(data["filter"])

        return super().load(data)

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
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
        properties (dict[str, MappedPropertyApply | ConnectionDefinitionApply] | None): No description.
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
        properties: dict[str, MappedPropertyApply | ConnectionDefinitionApply] | None = None,
    ) -> None:
        validate_data_modeling_identifier(space, external_id)
        super().__init__(space, external_id, version, description, name, filter, implements)
        self.properties = properties

    @classmethod
    def load(cls, resource: dict | str) -> ViewApply:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "properties" in data and isinstance(data["properties"], dict):
            data["properties"] = {k: ViewPropertyApply.load(v) for k, v in data["properties"].items()} or None

        return cast(ViewApply, super().load(data))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        if "properties" in output:
            output["properties"] = {k: v.dump(camel_case) for k, v in output["properties"].items()}

        return output


class View(ViewCore):
    """A group of properties. Read only version.

    Args:
        space (str): The workspace for the view, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        version (str): DMS version.
        properties (dict[str, MappedProperty | ConnectionDefinition]): View with included properties and expected edges, indexed by a unique space-local identifier.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description (str | None): Textual description of the view
        name (str | None): Human readable name for the view.
        filter (Filter | None): A filter Domain Specific Language (DSL) used to create advanced filter queries.
        implements (list[ViewId] | None): References to the views from where this view will inherit properties and edges.
        writable (bool): Whether the view supports write operations.
        used_for (Literal["node", "edge", "all"]): Does this view apply to nodes, edges or both.
        is_global (bool): Whether this is a global container, i.e., one of the out-of-the-box models.
        **_ (Any): No description.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        properties: dict[str, MappedProperty | ConnectionDefinition],
        last_updated_time: int,
        created_time: int,
        description: str | None = None,
        name: str | None = None,
        filter: Filter | None = None,
        implements: list[ViewId] | None = None,
        writable: bool = False,
        used_for: Literal["node", "edge", "all"] = "node",
        is_global: bool = False,
        **_: Any,
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
    def load(cls, resource: dict | str) -> View:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "properties" in data and isinstance(data["properties"], dict):
            data["properties"] = {k: ViewProperty.load(v) for k, v in data["properties"].items()} or None

        return cast(View, super().load(data))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        if "properties" in output:
            output["properties"] = {k: v.dump(camel_case) for k, v in output["properties"].items()}

        return output

    def as_apply(self) -> ViewApply:
        """Convert to a view applies.

        Returns:
            ViewApply: The view apply.
        """
        properties: dict[str, MappedPropertyApply | ConnectionDefinitionApply] | None = None
        if self.properties:
            for k, v in self.properties.items():
                if isinstance(v, (MappedProperty, SingleHopConnectionDefinition)):
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


class ViewList(CogniteResourceList[View]):
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


class ViewApplyList(CogniteResourceList[ViewApply]):
    _RESOURCE = ViewApply

    def as_ids(self) -> list[ViewId]:
        """Returns the list of ViewIds

        Returns:
            list[ViewId]: The list of ViewIds
        """
        return [v.as_id() for v in self]


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


class ViewProperty(ABC):
    @classmethod
    def load(cls, data: dict[str, Any]) -> ViewProperty:
        if "direction" in data:
            return SingleHopConnectionDefinition.load(data)
        else:
            return MappedProperty.load(data)

    @abstractmethod
    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        raise NotImplementedError


class ViewPropertyApply(ABC):
    @classmethod
    def load(cls, data: dict[str, Any]) -> ViewPropertyApply:
        if "direction" in data:
            return SingleHopConnectionDefinitionApply.load(data)
        else:
            return MappedPropertyApply.load(data)

    @abstractmethod
    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        raise NotImplementedError


@dataclass
class MappedPropertyApply(ViewPropertyApply):
    container: ContainerId
    container_property_identifier: str
    name: str | None = None
    description: str | None = None
    source: ViewId | None = None

    @classmethod
    def load(cls, data: dict) -> MappedPropertyApply:
        output = cls(**convert_all_keys_to_snake_case(data))
        if isinstance(data.get("container"), dict):
            output.container = ContainerId.load(data["container"])
        if isinstance(data.get("source"), dict):
            output.source = ViewId.load(data["source"])
        return output

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: dict[str, Any] = {
            "container": self.container.dump(camel_case, include_type=True),
            (
                "containerPropertyIdentifier" if camel_case else "container_property_identifier"
            ): self.container_property_identifier,
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
    auto_increment: bool
    source: ViewId | None = None
    default_value: str | int | dict | None = None
    name: str | None = None
    description: str | None = None

    @classmethod
    def load(cls, data: dict[str, Any]) -> MappedProperty:
        output = cls(**convert_all_keys_to_snake_case(data))
        if isinstance(data.get("container"), dict):
            output.container = ContainerId.load(data["container"])
        if "type" in data:
            if data["type"].get("type") == "direct":
                type_data = data["type"]
                source = type_data.pop("source", None)
                output.type = DirectRelation.load(type_data)
                output.source = ViewId.load(source) if source else None
            else:
                output.type = PropertyType.load(data["type"])
        return output

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = asdict(self)
        output["type"] = self.type.dump(camel_case)
        if self.source:
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
        )


@dataclass
class ConnectionDefinition(ViewProperty):
    ...


@dataclass
class SingleHopConnectionDefinition(ConnectionDefinition):
    type: DirectRelationReference
    source: ViewId
    name: str | None = None
    description: str | None = None
    edge_source: ViewId | None = None
    direction: Literal["outwards", "inwards"] = "outwards"

    @classmethod
    def load(cls, data: dict[str, Any]) -> SingleHopConnectionDefinition:
        output = cls(**convert_all_keys_to_snake_case(data))
        if (type_ := data.get("type")) is not None:
            output.type = DirectRelationReference.load(type_)
        if (source := data.get("source")) is not None:
            output.source = ViewId.load(source)
        if (edge_source := data.get("edgeSource")) is not None:
            output.edge_source = ViewId.load(edge_source)
        return output

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = asdict(self)

        if self.type:
            output["type"] = self.type.dump(camel_case)

        if self.source:
            output["source"] = self.source.dump(camel_case)

        if self.edge_source:
            output["edge_source"] = self.edge_source.dump(camel_case)

        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output

    def as_apply(self) -> SingleHopConnectionDefinitionApply:
        return SingleHopConnectionDefinitionApply(
            type=self.type,
            source=self.source,
            name=self.name,
            description=self.description,
            edge_source=self.edge_source,
            direction=self.direction,
        )


@dataclass
class ConnectionDefinitionApply(ViewPropertyApply):
    ...


T_ConnectionDefinitionApply = TypeVar("T_ConnectionDefinitionApply", bound=ConnectionDefinitionApply)


@dataclass
class SingleHopConnectionDefinitionApply(ConnectionDefinitionApply):
    type: DirectRelationReference
    source: ViewId
    name: str | None = None
    description: str | None = None
    edge_source: ViewId | None = None
    direction: Literal["outwards", "inwards"] = "outwards"

    @classmethod
    def load(cls, data: dict) -> SingleHopConnectionDefinitionApply:
        output = cls(**convert_all_keys_to_snake_case(data))
        if (type_ := data.get("type")) is not None:
            output.type = DirectRelationReference.load(type_)
        if (source := data.get("source")) is not None:
            output.source = ViewId.load(source)
        if (edge_source := data.get("edgeSource")) is not None:
            output.edge_source = ViewId.load(edge_source)
        return output

    def dump(self, camel_case: bool = False) -> dict:
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
