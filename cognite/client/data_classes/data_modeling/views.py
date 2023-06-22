from __future__ import annotations

import json
from abc import ABC
from dataclasses import asdict, dataclass
from typing import Any, Dict, Literal, Optional, Union, cast

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
from cognite.client.data_classes.data_modeling.filters import Filter
from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId
from cognite.client.utils._auxiliary import rename_and_exclude_keys
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
        description: str = None,
        name: str = None,
        filter: Filter | None = None,
        implements: list[ViewId] = None,
        **_: dict,
    ):
        validate_data_modeling_identifier(space, external_id)
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

        return cast(ViewCore, super().load(data))

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
        description (str): Textual description of the view
        name (str): Human readable name for the view.
        filter (dict): A filter Domain Specific Language (DSL) used to create advanced filter queries.
        implements (list): References to the views from where this view will inherit properties and edges.
        version (str): DMS version.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        description: str = None,
        name: str = None,
        filter: Filter | None = None,
        implements: list[ViewId] = None,
        properties: dict[str, MappedApplyPropertyDefinition | ConnectionDefinition] = None,
    ):
        super().__init__(space, external_id, version, description, name, filter, implements)
        self.properties = properties

    @classmethod
    def load(cls, resource: dict | str) -> ViewApply:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "properties" in data and isinstance(data["properties"], dict):
            data["properties"] = {k: ViewPropertyDefinition.load(v) for k, v in data["properties"].items()} or None

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
        description (str): Textual description of the view
        name (str): Human readable name for the view.
        filter (dict): A filter Domain Specific Language (DSL) used to create advanced filter queries.
        implements (list): References to the views from where this view will inherit properties and edges.
        version (str): DMS version.
        writable (bool): Whether the view supports write operations.
        used_for (Literal["node", "edge", "all"]): Does this view apply to nodes, edges or both.
        is_global (bool): Whether this is a global container, i.e., one of the out-of-the-box models.
        properties (dict): View with included properties and expected edges, indexed by a unique space-local identifier.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        properties: dict[str, MappedPropertyDefinition | ConnectionDefinition],
        last_updated_time: int,
        created_time: int,
        description: str = None,
        name: str = None,
        filter: Filter | None = None,
        implements: list[ViewId] = None,
        writable: bool = False,
        used_for: Literal["node", "edge", "all"] = "node",
        is_global: bool = False,
        **_: dict,
    ):
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
            data["properties"] = {k: ViewPropertyDefinition.load(v) for k, v in data["properties"].items()} or None

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
        properties: Optional[Dict[str, Union[MappedApplyPropertyDefinition, ConnectionDefinition]]] = None
        if self.properties:
            properties = {
                k: (v.as_apply() if isinstance(v, MappedPropertyDefinition) else v) for k, v in self.properties.items()
            }

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

    def as_id(self) -> ViewId:
        """Convert to a view id.

        Returns:
            ViewId: The view id.
        """
        return ViewId(space=self.space, external_id=self.external_id, version=self.version)


class ViewList(CogniteResourceList[View]):
    _RESOURCE = View

    def to_view_apply(self) -> ViewApplyList:
        """Convert to a view an apply list.

        Returns:
            ViewApplyList: The view apply list.
        """
        return ViewApplyList(resources=[v.as_apply() for v in self.items])


class ViewApplyList(CogniteResourceList[ViewApply]):
    _RESOURCE = ViewApply


class ViewFilter(CogniteFilter):
    """Represent the filer arguments for the list endpoint.

    Args:
        space (str | None): The space to query
        include_inherited_properties (bool): Whether to include properties inherited from views this view implements.
        all_versions (bool): Whether to return all versions. If false, only the newest version is returned,
                             which is determined based on the 'createdTime' field.
        include_global (bool): Whether to include global views.
    """

    def __init__(
        self,
        space: str = None,
        include_inherited_properties: bool = True,
        all_versions: bool = False,
        include_global: bool = False,
    ):
        self.space = space
        self.include_inherited_properties = include_inherited_properties
        self.all_versions = all_versions
        self.include_global = include_global


@dataclass
class ViewDirectRelation(DirectRelation):
    source: Optional[ViewId] = None

    @classmethod
    def load(cls, data: dict) -> ViewDirectRelation:
        output = cls(**convert_all_keys_to_snake_case(rename_and_exclude_keys(data, exclude={"type"})))
        if isinstance(data.get("source"), dict):
            output.source = ViewId.load(data["source"])
        return output

    def dump(self, camel_case: bool = False) -> dict:
        output = super().dump(camel_case)

        if self.source:
            output["source"] = self.source.dump(camel_case)
        return output


@dataclass
class ViewPropertyDefinition(ABC):
    @classmethod
    def _load(cls, data: dict) -> ViewPropertyDefinition:
        return cls(**convert_all_keys_to_snake_case(data))

    @classmethod
    def load(cls, data: dict) -> MappedPropertyDefinition | ConnectionDefinition | MappedApplyPropertyDefinition:
        if "container" in data and "source" in data:
            return MappedApplyPropertyDefinition.load(data)
        elif "container" in data:
            return MappedPropertyDefinition.load(data)
        elif "type" in data:
            return SingleHopConnectionDefinition.load(data)

        raise ValueError(f"Unknown type of ViewPropertyDefinition: {data.get('type')}")

    def dump(self, camel_case: bool = False) -> dict:
        output = asdict(self)
        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output


@dataclass
class MappedCorePropertyDefinition(ViewPropertyDefinition):
    container: ContainerId
    container_property_identifier: str
    name: str | None = None
    description: str | None = None

    @classmethod
    def load(cls, data: dict) -> MappedCorePropertyDefinition:  # type: ignore[override]
        output = cast(MappedCorePropertyDefinition, super()._load(data))
        if isinstance(data.get("container"), dict):
            output.container = ContainerId.load(data["container"])
        return output

    def dump(self, camel_case: bool = False) -> dict:
        output = asdict(self)
        if self.container:
            output["container"] = self.container.dump(camel_case)

        if camel_case:
            output = convert_all_keys_to_camel_case_recursive(output)

        return output


@dataclass
class MappedApplyPropertyDefinition(MappedCorePropertyDefinition):
    source: ViewId | None = None

    @classmethod
    def load(cls, data: dict) -> MappedApplyPropertyDefinition:
        output = cast(MappedApplyPropertyDefinition, super().load(data))
        if isinstance(data.get("source"), dict):
            output.source = ViewId.load(data["source"])
        return output

    def dump(self, camel_case: bool = False) -> dict:
        output = super().dump(camel_case)
        if self.source:
            output["source"] = self.source.dump(camel_case)
        return output


@dataclass
class MappedPropertyDefinition(MappedCorePropertyDefinition):
    type: PropertyType | None = None
    nullable: bool = True
    auto_increment: bool = False
    default_value: str | int | dict | None = None

    @classmethod
    def load(cls, data: dict) -> MappedPropertyDefinition:
        output = cast(MappedPropertyDefinition, super().load(data))
        if "type" in data:
            if data["type"].get("type") == "direct":
                output.type = ViewDirectRelation.load(data["type"])
            else:
                output.type = PropertyType.load(data["type"])
        return output

    def dump(self, camel_case: bool = False) -> dict:
        output = super().dump(camel_case)
        if self.type:
            try:
                output["type"] = self.type.dump(camel_case)
            except AttributeError:
                raise
        return output

    def as_apply(self) -> MappedApplyPropertyDefinition:
        return MappedApplyPropertyDefinition(
            container=self.container,
            container_property_identifier=self.container_property_identifier,
            name=self.name,
            description=self.description,
        )


@dataclass
class ConnectionDefinition(ViewPropertyDefinition):
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
    def load(cls, data: dict) -> SingleHopConnectionDefinition:
        output = cast(SingleHopConnectionDefinition, super()._load(data))
        if "type" in data:
            output.type = DirectRelationReference.load(data["type"])
        if "source" in data:
            output.source = ViewId.load(data["source"])
        if "edgeSource" in data or "edge_source" in data:
            edge_source = data.get("edgeSource", data.get("edge_source"))
            output.edge_source = ViewId.load(edge_source) if edge_source else None
        return output

    def dump(self, camel_case: bool = False) -> dict:
        output = asdict(self)

        if self.type:
            output["type"] = self.type.dump(camel_case)

        if self.source:
            output["source"] = self.source.dump(camel_case)

        if self.edge_source:
            output["edgeSource" if camel_case else "edge_source"] = self.edge_source.dump(camel_case)

        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output
