from __future__ import annotations

import json
from typing import TYPE_CHECKING, Optional, Type, TypeVar

from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils._text import convert_all_keys_to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class DataModelingResource(CogniteResource):
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
    def load(cls: Type[T_DataModelingResource], data: dict | str) -> T_DataModelingResource:
        data = json.loads(data) if isinstance(data, str) else data
        return cls(**convert_all_keys_to_snake_case(data))

    @classmethod
    def _load(
        cls: Type[T_DataModelingResource], resource: dict | str, cognite_client: Optional[CogniteClient] = None
    ) -> T_DataModelingResource:
        return cls.load(resource)


T_DataModelingResource = TypeVar("T_DataModelingResource", bound=DataModelingResource)
