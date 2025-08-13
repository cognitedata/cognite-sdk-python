from __future__ import annotations

from collections import defaultdict
from collections.abc import ItemsView, Iterator, KeysView, MutableMapping, ValuesView
from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    cast,
    overload,
)

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteObject, CogniteResource, CogniteResourceList
from cognite.client.data_classes.data_modeling.ids import ContainerId, ContainerIdentifier
from cognite.client.data_classes.data_modeling.instances import (
    PropertyIdentifier,
    PropertyValue,
    SourceData,
)
from cognite.client.utils import datetime_to_ms
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass(frozen=True)
class RecordId:
    space: str
    external_id: str

    def dump(self, camel_case: bool = True) -> dict[str, str]:
        return {"space": self.space, "externalId" if camel_case else "external_id": self.external_id}

    @classmethod
    def load(cls, data: dict[str, str] | tuple[str, str] | Self) -> Self:
        if isinstance(data, cls):
            return data
        elif isinstance(data, tuple) and len(data) == 2:
            return cls(*data)
        elif isinstance(data, dict):
            if "externalId" in data:
                return cls(space=data["space"], external_id=data["externalId"])
            if "external_id" in data:
                return cls(space=data["space"], external_id=data["external_id"])
        raise KeyError(f"Cannot load {data} into {cls}, missing 'externalId' or 'external_id' key")

    def as_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(space={self.space!r}, external_id={self.external_id!r})"


@dataclass
class RecordIngest(CogniteObject):
    id: RecordId
    sources: list[SourceData]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "space": self.id.space,
            "externalId" if camel_case else "external_id": self.id.external_id,
            "sources": [s.dump(camel_case) for s in self.sources],
        }

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        id = RecordId(space=resource["space"], external_id=resource["externalId"])
        sources = [SourceData._load(source, cognite_client) for source in resource["sources"]]
        return cls(id=id, sources=sources)


_T = TypeVar("_T")


# TODO: Use the Properties class from cognite.client.data_classes.data_modeling.instances, and make that support
#   both container ids and view ids. We need that for when we support containers for instances anyway.
class Properties(MutableMapping[ContainerIdentifier, MutableMapping[PropertyIdentifier, PropertyValue]]):
    def __init__(
        self, properties: MutableMapping[ContainerId, MutableMapping[PropertyIdentifier, PropertyValue]]
    ) -> None:
        self.data = properties

    @classmethod
    def load(
        cls, data: MutableMapping[str, MutableMapping[str, MutableMapping[PropertyIdentifier, PropertyValue]]]
    ) -> Properties:
        props: MutableMapping[ContainerId, MutableMapping[PropertyIdentifier, PropertyValue]] = {}
        for space, container_properties in data.items():
            for container_id_str, properties in container_properties.items():
                container_id = ContainerId.load((space, container_id_str))
                props[container_id] = properties
        return cls(props)

    def dump(self) -> dict[str, dict[str, dict[PropertyIdentifier, PropertyValue]]]:
        props: dict[str, dict[str, dict[PropertyIdentifier, PropertyValue]]] = defaultdict(dict)
        for container_id, properties in self.data.items():
            extid = container_id.external_id
            props[container_id.space][extid] = cast(dict[PropertyIdentifier, PropertyValue], properties)
        # Defaultdict is not yaml serializable
        return dict(props)

    def items(self) -> ItemsView[ContainerId, MutableMapping[PropertyIdentifier, PropertyValue]]:
        return self.data.items()

    def keys(self) -> KeysView[ContainerId]:
        return self.data.keys()

    def values(self) -> ValuesView[MutableMapping[PropertyIdentifier, PropertyValue]]:
        return self.data.values()

    def __iter__(self) -> Iterator[ContainerId]:
        yield from self.keys()

    def __getitem__(self, view: ContainerIdentifier) -> MutableMapping[PropertyIdentifier, PropertyValue]:
        view_id = ContainerId.load(view)
        return self.data.get(view_id, {})

    def __contains__(self, item: Any) -> bool:
        view_id = ContainerId.load(item)
        return view_id in self.data

    @overload
    def get(self, source: ContainerIdentifier) -> MutableMapping[PropertyIdentifier, PropertyValue] | None: ...

    @overload
    def get(
        self, source: ContainerIdentifier, default: MutableMapping[PropertyIdentifier, PropertyValue] | _T
    ) -> MutableMapping[PropertyIdentifier, PropertyValue] | _T: ...

    def get(
        self,
        source: ContainerIdentifier,
        default: MutableMapping[PropertyIdentifier, PropertyValue] | None | _T | None = None,
    ) -> MutableMapping[PropertyIdentifier, PropertyValue] | None | _T:
        source_id = ContainerId.load(source)
        return self.data.get(source_id, default)

    def __len__(self) -> int:
        return len(self.data)

    def __delitem__(self, source: ContainerIdentifier) -> None:
        source_id = ContainerId.load(source)
        del self.data[source_id]

    def __setitem__(
        self, source: ContainerIdentifier, properties: MutableMapping[PropertyIdentifier, PropertyValue]
    ) -> None:
        source_id = ContainerId.load(source)
        self.data[source_id] = properties


@dataclass
class Record(CogniteResource):
    id: RecordId
    created_time: int
    last_updated_time: int
    properties: Properties

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {
            "space": self.id.space,
            "external_id": self.id.external_id,
            "created_time": self.created_time,
            "last_updated_time": self.last_updated_time,
            "properties": self.properties.dump(),
        }
        if camel_case:
            return convert_all_keys_to_camel_case(out)
        return out

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        id = RecordId.load(resource)
        properties = Properties.load(resource.get("properties", {}))
        return cls(
            id=id,
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            properties=properties,
        )


class RecordList(CogniteResourceList[Record]):
    _RESOURCE = Record


class RecordListWithCursor(RecordList):
    def __init__(self, resources: list[Record], cursor: str | None, has_next: bool | None = None) -> None:
        super().__init__(resources)
        self.cursor = cursor
        self.has_next = has_next


@dataclass
class LastUpdatedRange(CogniteObject):
    gt: int | datetime | None = None
    gte: int | datetime | None = None
    lt: int | datetime | None = None
    lte: int | datetime | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.gt is not None:
            out["gt"] = datetime_to_ms(self.gt) if isinstance(self.gt, datetime) else self.gt
        if self.gte is not None:
            out["gte"] = datetime_to_ms(self.gte) if isinstance(self.gte, datetime) else self.gte
        if self.lt is not None:
            out["lt"] = datetime_to_ms(self.lt) if isinstance(self.lt, datetime) else self.lt
        if self.lte is not None:
            out["lte"] = datetime_to_ms(self.lte) if isinstance(self.lte, datetime) else self.lte
        return out
