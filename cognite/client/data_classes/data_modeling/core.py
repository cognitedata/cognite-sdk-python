from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    T_CogniteResource,
    WriteableCogniteResource,
    basic_instance_dump,
)
from cognite.client.utils import _json

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class DataModelingResource(CogniteResource, ABC):
    def __init__(self, space: str):
        self.space = space

    def __repr__(self) -> str:
        args = []
        if hasattr(self, "space"):
            space = self.space
            args.append(f"{space=}")
        if hasattr(self, "external_id"):
            external_id = self.external_id
            args.append(f"{external_id=}")
        if hasattr(self, "version"):
            version = self.version
            args.append(f"{version=}")

        return f"<{type(self).__qualname__}({', '.join(args)}) at {id(self):#x}>"


class WritableDataModelingResource(WriteableCogniteResource[T_CogniteResource], ABC):
    def __init__(self, space: str) -> None:
        self.space = space

    def __repr__(self) -> str:
        args = []
        if hasattr(self, "space"):
            space = self.space
            args.append(f"{space=}")
        if hasattr(self, "external_id"):
            external_id = self.external_id
            args.append(f"{external_id=}")
        if hasattr(self, "version"):
            version = self.version
            args.append(f"{version=}")

        return f"<{type(self).__qualname__}({', '.join(args)}) at {id(self):#x}>"


class DataModelingSchemaResource(WritableDataModelingResource[T_CogniteResource], ABC):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None,
        description: str | None,
    ) -> None:
        super().__init__(space=space)
        self.external_id = external_id
        self.name = name
        self.description = description


class DataModelingSort(CogniteObject):
    def __init__(
        self,
        property: str | list[str] | tuple[str, ...],
        direction: Literal["ascending", "descending"] = "ascending",
        nulls_first: bool | None = None,
    ) -> None:
        super().__init__()
        self.property = property
        self.direction = direction
        self.nulls_first = nulls_first

    def __eq__(self, other: Any) -> bool:
        return type(other) is type(self) and self.dump() == other.dump()

    def __str__(self) -> str:
        return _json.dumps(self, indent=4)

    def __repr__(self) -> str:
        return str(self)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        if not isinstance(resource, dict):
            raise TypeError(f"Resource must be mapping, not {type(resource)}")

        instance = cls(property=resource["property"])
        if "direction" in resource:
            instance.direction = resource["direction"]
        if "nullsFirst" in resource:
            instance.nulls_first = resource["nullsFirst"]

        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return basic_instance_dump(self, camel_case=camel_case)
