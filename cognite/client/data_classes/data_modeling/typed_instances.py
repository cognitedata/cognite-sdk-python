from __future__ import annotations

import inspect
from abc import ABC
from collections.abc import Iterable
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, ClassVar

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, T_WriteClass, WriteableCogniteResource
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelationReference,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId, EdgeId, NodeId, ViewId
from cognite.client.data_classes.data_modeling.instances import _serialize_property_value
from cognite.client.utils._text import to_camel_case

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


class TypedInstanceWrite(CogniteResource, ABC):
    _instance_properties: frozenset[str]
    _instance_type: ClassVar[str]

    def __init__(self, space: str, external_id: str, existing_version: int | None = None) -> None:
        self.space = space
        self.external_id = external_id
        self.existing_version = existing_version

    @classmethod
    def get_source(cls) -> ContainerId | ViewId:
        raise NotImplementedError()

    def dump(self, camel_case: bool = True) -> dict[str, str | dict]:
        output = self._dump_instance(camel_case)
        properties = self._dump_properties(camel_case)
        if properties:
            output["sources"] = [
                {
                    "source": self.get_source().dump(camel_case),
                    "properties": properties,
                }
            ]
        return output

    def _dump_instance(self, camel_case: bool) -> dict[str, Any]:
        output: dict[str, Any] = {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
            "instanceType" if camel_case else "instance_type": self._instance_type,
        }
        if self.existing_version:
            output["existingVersion" if camel_case else "existing_version"] = self.existing_version
        return output

    def _dump_properties(self, camel_case: bool) -> dict[str, Any]:
        properties: dict[str, str | int | float | bool | dict | list] = {}
        for key, value in vars(self).items():
            if key in self._instance_properties or value is None:
                continue
            if isinstance(value, Iterable) and not isinstance(value, (str, dict)):
                properties[key] = [_serialize_property_value(v, camel_case) for v in value]
            else:
                properties[key] = _serialize_property_value(value, camel_case)
        return properties

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        args: dict[str, Any] = {}
        args.update(cls._load_properties(resource))
        args.update(cls._load_instance(resource))
        return cls(**args)

    @classmethod
    def _load_instance(cls, resource: dict[str, Any]) -> dict[str, Any]:
        args: dict[str, Any] = {}
        for key in cls._instance_properties:
            camel_key = to_camel_case(key)
            if camel_key in resource:
                args[key] = resource[camel_key]
        return args

    @classmethod
    def _load_properties(cls, resource: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError()


class TypedNodeWrite(TypedInstanceWrite, ABC):
    _instance_properties: frozenset[str] = frozenset({"space", "external_id", "existing_version", "type"})
    _instance_type = "node"

    def __init__(
        self,
        space: str,
        external_id: str,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, existing_version)
        self.type = DirectRelationReference.load(type) if type else None

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)

    def _dump_instance(self, camel_case: bool) -> dict[str, Any]:
        output = super()._dump_instance(camel_case)
        if self.type:
            output["type"] = self.type.dump(camel_case)
        return output


class TypedEdgeWrite(TypedInstanceWrite, ABC):
    _instance_properties = frozenset({"space", "external_id", "existing_version", "type", "start_node", "end_node"})
    _instance_type = "edge"

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        existing_version: int | None = None,
    ) -> None:
        super().__init__(space, external_id, existing_version)
        self.type = DirectRelationReference.load(type)
        self.start_node = DirectRelationReference.load(start_node)
        self.end_node = DirectRelationReference.load(end_node)

    def as_id(self) -> EdgeId:
        return EdgeId(space=self.space, external_id=self.external_id)

    def _dump_instance(self, camel_case: bool) -> dict[str, Any]:
        output = super()._dump_instance(camel_case)
        output["type"] = self.type.dump(camel_case)
        output["startNode" if camel_case else "start_node"] = self.start_node.dump(camel_case)
        output["endNode" if camel_case else "end_node"] = self.end_node.dump(camel_case)
        return output


class TypedInstance(WriteableCogniteResource[T_WriteClass], ABC):
    _instance_properties: frozenset[str]
    _instance_type: ClassVar[str]

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        deleted_time: int | None,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.version = version
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self.deleted_time = deleted_time

    @classmethod
    def get_source(cls) -> ViewId | ContainerId:
        raise NotImplementedError()

    def dump(self, camel_case: bool = True) -> dict[str, str | dict]:
        output = self._dump_instance(camel_case)
        properties = self._dump_properties(camel_case)
        if properties:
            source = self.get_source()
            output["properties"] = {
                source.space: {
                    source.as_source_identifier(): properties,
                }
            }
        return output

    def _dump_instance(self, camel_case: bool) -> dict[str, Any]:
        output: dict[str, Any] = {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
            "instanceType" if camel_case else "instance_type": self._instance_type,
            "version": self.version,
            "lastUpdatedTime" if camel_case else "last_updated_time": self.last_updated_time,
            "createdTime" if camel_case else "created_time": self.created_time,
        }
        if self.deleted_time:
            output["deletedTime" if camel_case else "deleted_time"] = self.deleted_time
        return output

    def _dump_properties(self, camel_case: bool) -> dict[str, Any]:
        properties: dict[str, str | int | float | bool | dict | list] = {}
        for key, value in vars(self).items():
            if key in self._instance_properties:
                continue
            if isinstance(value, Iterable) and not isinstance(value, (str, dict)):
                properties[key] = [_serialize_property_value(v, camel_case) for v in value]
            else:
                properties[key] = _serialize_property_value(value, camel_case)
        return properties

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        args: dict[str, Any] = {}
        all_properties = resource.get("properties", {})
        if all_properties:
            source = cls.get_source()
            properties = all_properties.get(source.space, {}).get(source.as_source_identifier(), {})
            args.update(cls._load_properties(properties))
        args.update(cls._load_instance(resource))
        return cls(**args)

    @classmethod
    def _load_instance(cls, resource: dict[str, Any]) -> dict[str, Any]:
        args: dict[str, Any] = {}
        for key in cls._instance_properties:
            camel_key = to_camel_case(key)
            if camel_key in resource:
                args[key] = resource[camel_key]
        return args

    @classmethod
    def _load_properties(cls, resource: dict[str, Any]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        signature = inspect.signature(cls.__init__)
        for name, parameter in signature.parameters.items():
            if name in cls._instance_properties:
                continue
            if name in resource:
                output[name] = _deserialize_values(resource[name], parameter)
            elif (
                name in cls.__dict__
                and isinstance(cls.__dict__[name], PropertyOptions)
                and cls.__dict__[name].name in resource
            ):
                output[name] = _deserialize_values(resource[cls.__dict__[name].name], parameter)
        return output


def _deserialize_values(value: Any, parameter: inspect.Parameter) -> Any:
    if isinstance(value, list):
        return [_deserialize_value(v, parameter) for v in value]
    else:
        return _deserialize_value(value, parameter)


def _deserialize_value(value: Any, parameter: inspect.Parameter) -> Any:
    if parameter.annotation is inspect.Parameter.empty:
        return value
    annotation = str(parameter.annotation)
    if "date" in annotation and isinstance(value, str):
        return date.fromisoformat(value)
    elif "datetime" in annotation and isinstance(value, str):
        return datetime.fromisoformat(value)
    elif DirectRelationReference.__name__ in annotation and isinstance(value, dict):
        return DirectRelationReference.load(value)
    elif NodeId.__name__ in annotation and isinstance(value, dict):
        return NodeId.load(value)
    elif EdgeId.__name__ in annotation and isinstance(value, dict):
        return EdgeId.load(value)

    return value


class TypedNode(TypedInstance[T_WriteClass], ABC):
    _instance_properties = frozenset(
        {"space", "external_id", "version", "last_updated_time", "created_time", "deleted_time", "type"}
    )
    _instance_type = "node"

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, deleted_time)
        self.type = DirectRelationReference.load(type) if type else None

    def _dump_instance(self, camel_case: bool) -> dict[str, Any]:
        output = super()._dump_instance(camel_case)
        if self.type:
            output["type"] = self.type.dump(camel_case)
        return output


class TypedEdge(TypedInstance[T_WriteClass], ABC):
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
        }
    )
    _instance_type = "edge"

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        version: int,
        last_updated_time: int,
        created_time: int,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, deleted_time)
        self.type = DirectRelationReference.load(type)
        self.start_node = DirectRelationReference.load(start_node)
        self.end_node = DirectRelationReference.load(end_node)

    def _dump_instance(self, camel_case: bool) -> dict[str, Any]:
        output = super()._dump_instance(camel_case)
        output["type"] = self.type.dump(camel_case)
        output["startNode" if camel_case else "start_node"] = self.start_node.dump(camel_case)
        output["endNode" if camel_case else "end_node"] = self.end_node.dump(camel_case)
        return output
