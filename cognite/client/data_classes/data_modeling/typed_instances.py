from __future__ import annotations

from abc import ABC
from collections.abc import Iterable
from datetime import date, datetime
from typing import Any, ClassVar

from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelationReference,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId, EdgeId, NodeId, ViewId
from cognite.client.data_classes.data_modeling.instances import PropertyValueWrite


class TypedInstanceWrite(CogniteObject, ABC):
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
            if key in self._instance_properties:
                continue
            if isinstance(value, Iterable) and not isinstance(value, (str, dict)):
                properties[key] = [self._serialize_value(v, camel_case) for v in value]
            else:
                properties[key] = self._serialize_value(value, camel_case)
        return properties

    @staticmethod
    def _serialize_value(value: PropertyValueWrite, camel_case: bool) -> str | int | float | bool | dict | list:
        if isinstance(value, (NodeId, EdgeId)):
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


class TypedNodeWrite(TypedInstanceWrite):
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


class TypedEdgeWrite(TypedInstanceWrite):
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
