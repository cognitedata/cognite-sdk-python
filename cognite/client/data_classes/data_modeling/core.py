from __future__ import annotations

import json
from abc import ABC
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteObject, CogniteResource, basic_instance_dump
from cognite.client.utils._auxiliary import json_dump_default
from cognite.client.utils._text import convert_all_keys_to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class DataModelingResource(CogniteResource, ABC):
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

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(**convert_all_keys_to_snake_case(resource))


class DataModelingSort(CogniteObject):
    def __init__(
        self,
        property: str | list[str] | tuple[str, ...],
        direction: Literal["ascending", "descending"] = "ascending",
        nulls_first: bool = False,
    ) -> None:
        super().__init__()
        self.property = property
        self.direction = direction
        self.nulls_first = nulls_first

    def __eq__(self, other: Any) -> bool:
        return type(other) is type(self) and self.dump() == other.dump()

    def __str__(self) -> str:
        return json.dumps(self.dump(), default=json_dump_default, indent=4)

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
