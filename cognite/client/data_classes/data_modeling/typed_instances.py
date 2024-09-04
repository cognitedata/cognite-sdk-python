from __future__ import annotations

import inspect
from abc import ABC
from collections.abc import Iterable
from datetime import date
from typing import TYPE_CHECKING, Any, cast

from typing_extensions import Self

from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelationReference,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId, EdgeId, NodeId, ViewId
from cognite.client.data_classes.data_modeling.instances import (
    Edge,
    EdgeApply,
    Node,
    NodeApply,
    _serialize_property_value,
)
from cognite.client.utils._text import to_camel_case
from cognite.client.utils._time import convert_data_modelling_timestamp

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class PropertyOptions:
    """This is a descriptor class for instance properties in a typed class.

    It is used when you have a property that has a different name in the Data Model
    compared to the name in the Python class.

    Args:
        identifier (str | None): The name of the property in the Data Model. Defaults to the name of the property in the Python class.
    """

    def __init__(self, identifier: str | None = None) -> None:
        self.name = identifier

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = self.name or name
        if self.name in _RESERVED_PROPERTY_NAMES:
            self.name = f"__{self.name}"

    def __get__(self, instance: Any, owner: type) -> Any:
        try:
            return instance.__dict__[self.name]
        except KeyError:
            raise AttributeError(f"'{owner.__name__}' object has no attribute '{self.name}'")

    def __set__(self, instance: Any, value: Any) -> None:
        try:
            instance.__dict__[self.name] = value
        except KeyError:
            raise AttributeError(f"'{instance.__class__.__name__}' object has no attribute '{self.name}'")

    def __delete__(self, instance: Any) -> None:
        try:
            del instance.__dict__[self.name]
        except KeyError:
            raise AttributeError(f"'{instance.__class__.__name__}' object has no attribute '{self.name}'")


class TypedNodeApply(NodeApply, ABC):
    _instance_properties: frozenset[str] = frozenset(
        {"space", "external_id", "existing_version", "type", "instance_type", "sources"}
    )

    @classmethod
    def get_source(cls) -> ContainerId | ViewId:
        raise NotImplementedError

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if "sources" not in output:
            output["sources"] = []
        output["sources"].append(
            {
                "source": self.get_source().dump(camel_case),
                "properties": _dump_properties(self, camel_case, self._instance_properties),
            }
        )
        return output

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        sources = resource.pop("sources", [])
        properties = sources[0].get("properties", {}) if sources else {}
        return cast(Self, _load_instance(cls, resource, properties, cls._instance_properties))


class TypedEdgeApply(EdgeApply, ABC):
    _instance_properties = frozenset(
        {"space", "external_id", "existing_version", "type", "start_node", "end_node", "instance_type", "sources"}
    )

    @classmethod
    def get_source(cls) -> ContainerId | ViewId:
        raise NotImplementedError

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if "sources" not in output:
            output["sources"] = []
        output["sources"].append(
            {
                "source": self.get_source().dump(camel_case),
                "properties": _dump_properties(self, camel_case, self._instance_properties),
            }
        )
        return output

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        sources = resource.pop("sources", [])
        properties = sources[0].get("properties", {}) if sources else {}
        return cast(Self, _load_instance(cls, resource, properties, cls._instance_properties))


class TypedNode(Node, ABC):
    _instance_properties = frozenset(
        {
            "space",
            "external_id",
            "version",
            "last_updated_time",
            "created_time",
            "deleted_time",
            "type",
            "instance_type",
            "properties",
        }
    )

    @classmethod
    def get_source(cls) -> ViewId:
        raise NotImplementedError

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if "properties" not in output:
            output["properties"] = {}
        source = self.get_source()
        output["properties"] = {
            source.space: {
                source.as_source_identifier(): _dump_properties(self, camel_case, self._instance_properties),
            }
        }
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        all_properties = resource.pop("properties", {})
        source = cls.get_source()
        properties = all_properties.get(source.space, {}).get(source.as_source_identifier(), {})
        return cast(Self, _load_instance(cls, resource, properties, cls._instance_properties))


class TypedEdge(Edge, ABC):
    _instance_properties = frozenset(
        {
            "space",
            "external_id",
            "version",
            "last_updated_time",
            "created_time",
            "deleted_time",
            "type",
            "start_node",
            "end_node",
            "instance_type",
            "properties",
        }
    )

    @classmethod
    def get_source(cls) -> ViewId:
        raise NotImplementedError

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if "properties" not in output:
            output["properties"] = {}
        source = self.get_source()
        output["properties"] = {
            source.space: {
                source.as_source_identifier(): _dump_properties(self, camel_case, self._instance_properties),
            }
        }
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        all_properties = resource.pop("properties", {})
        source = cls.get_source()
        properties = all_properties.get(source.space, {}).get(source.as_source_identifier(), {})
        return cast(Self, _load_instance(cls, resource, properties, cls._instance_properties))


def _load_instance(
    cls: type, resource: dict[str, Any], properties: dict[str, Any], instance_properties: frozenset[str]
) -> dict[str, Any]:
    args: dict[str, Any] = {}
    resource.pop("instanceType", None)
    signature = inspect.signature(cls.__init__)  # type: ignore[misc]
    args.update(_load_properties(cls, properties, instance_properties, signature))
    args.update(_load_fixed_attributes(resource, instance_properties, signature))
    return cls(**args)


def _load_fixed_attributes(
    resource: dict[str, Any], instance_properties: frozenset[str], signature: inspect.Signature
) -> dict[str, Any]:
    args: dict[str, Any] = {}
    for key in instance_properties:
        camel_key = to_camel_case(key)
        if camel_key in resource:
            if key in signature.parameters and key in signature.parameters:
                args[key] = _deserialize_value(resource[camel_key], signature.parameters[key])
        elif key in signature.parameters:
            if signature.parameters[key].default is inspect.Parameter.empty:
                args[key] = None
            else:
                args[key] = _deserialize_value(signature.parameters[key].default or None, signature.parameters[key])
    return args


def _load_properties(
    cls: type, resource: dict[str, Any], instance_properties: frozenset[str], signature: inspect.Signature
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    property_by_name = _get_properties_by_name(cls)

    for name, parameter in signature.parameters.items():
        if name in instance_properties:
            continue
        if name in resource:
            output[name] = _deserialize_values(resource[name], parameter)
        elif name in property_by_name:
            property_name = cast(str, property_by_name[name].name)
            if property_name.startswith("__"):
                property_name = property_name[2:]
            if property_name in resource:
                output[name] = _deserialize_values(resource[property_name], parameter)

    return output


def _get_properties_by_name(cls: type) -> dict[str, PropertyOptions]:
    to_search = [cls]
    property_by_name: dict[str, PropertyOptions] = {}
    while to_search:
        current_cls = to_search.pop()
        for name, value in current_cls.__dict__.items():
            if isinstance(value, PropertyOptions):
                property_by_name[name] = value
        to_search.extend(
            [
                b
                for b in current_cls.__bases__
                if {b not in {object, TypedNode, TypedEdge, Edge, Node, TypedNodeApply, TypedEdgeApply}}
            ]
        )
    return property_by_name


_RESERVED_PROPERTY_NAMES = (
    TypedNodeApply._instance_properties
    | TypedEdgeApply._instance_properties
    | TypedNode._instance_properties
    | TypedEdge._instance_properties
)


def _dump_properties(obj: object, camel_case: bool, instance_properties: frozenset[str]) -> dict[str, Any]:
    properties: dict[str, str | int | float | bool | dict | list] = {}
    properties_by_name = _get_properties_by_name(type(obj))
    for key, value in vars(obj).items():
        if key in instance_properties or value is None:
            continue
        if key.startswith("__"):
            key = key[2:]

        if key in properties_by_name:
            key = cast(str, properties_by_name[key].name)

        if isinstance(value, Iterable) and not isinstance(value, (str, dict)):
            properties[key] = [_serialize_property_value(v, camel_case) for v in value]
        else:
            properties[key] = _serialize_property_value(value, camel_case)
    return properties


def _deserialize_values(value: Any, parameter: inspect.Parameter) -> Any:
    if isinstance(value, list):
        return [_deserialize_value(v, parameter) for v in value]
    else:
        return _deserialize_value(value, parameter)


def _deserialize_value(value: Any, parameter: inspect.Parameter) -> Any:
    if parameter.annotation is inspect.Parameter.empty:
        return value
    annotation = str(parameter.annotation)
    if "datetime" in annotation and isinstance(value, str):
        return convert_data_modelling_timestamp(value)
    elif "date" in annotation and isinstance(value, str):
        return date.fromisoformat(value)
    elif DirectRelationReference.__name__ in annotation and isinstance(value, dict):
        return DirectRelationReference.load(value)
    elif NodeId.__name__ in annotation and isinstance(value, dict):
        return NodeId.load(value)
    elif EdgeId.__name__ in annotation and isinstance(value, dict):
        return EdgeId.load(value)

    return value
