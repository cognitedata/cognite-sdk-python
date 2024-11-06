from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    PropertySpec,
    UnknownCogniteObject,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class CustomMapping(CogniteObject):
    expression: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(expression=resource["expression"])


@dataclass
class InputMapping(CogniteObject, ABC):
    _type: ClassVar[str]

    @classmethod
    @abstractmethod
    def _load_input(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type")
        job_cls = _INPUTMAPPING_CLASS_BY_TYPE.get(type_)
        if job_cls is None:
            return UnknownCogniteObject(resource)  # type: ignore[return-value]
        return cast(Self, job_cls._load_input(resource))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["type"] = self._type
        return output


@dataclass
class ProtoBufFile(CogniteObject):
    file_name: str
    content: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            file_name=resource["fileName"],
            content=resource["content"],
        )


@dataclass
class ProtoBufInput(InputMapping):
    _type = "protobuf"

    message_name: str
    files: list[ProtoBufFile]

    @classmethod
    def _load_input(cls, resource: dict[str, Any]) -> Self:
        return cls(
            message_name=resource["messageName"],
            files=[ProtoBufFile._load(file) for file in resource["files"]],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "type": self._type,
            "messageName" if camel_case else "message_name": self.message_name,
            "files": [file.dump(camel_case) for file in self.files],
        }


@dataclass
class CSVInput(InputMapping):
    _type = "csv"
    delimiter: str = ","
    custom_keys: list[str] | None = None

    @classmethod
    def _load_input(cls, resource: dict[str, Any]) -> Self:
        return cls(
            delimiter=resource.get("delimiter", ","),
            custom_keys=resource.get("customKeys"),
        )


@dataclass
class XMLInput(InputMapping):
    _type = "xml"

    @classmethod
    def _load_input(cls, resource: dict[str, Any]) -> Self:
        return cls()


@dataclass
class JSONInput(InputMapping):
    _type = "json"

    @classmethod
    def _load_input(cls, resource: dict[str, Any]) -> Self:
        return cls()


class _MappingCore(WriteableCogniteResource["MappingWrite"]):
    def __init__(self, external_id: str, mapping: CustomMapping, published: bool) -> None:
        self.external_id = external_id
        self.mapping = mapping
        self.published = published

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["mapping"] = self.mapping.dump(camel_case)
        return output


class MappingWrite(_MappingCore):
    """A mapping is a custom transformation, translating the source format to a format that can be ingested into CDF.
    Mappings are written in the Cognite transformation language.

    This is the write/request format of a mapping.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        mapping (CustomMapping): The custom mapping.
        published (bool): Whether this mapping is published and should be available to be used in jobs.
        input (InputMapping | Literal['csv', 'json', 'xml']): The input mapping. Defaults to 'json'
    """

    def __init__(
        self,
        external_id: str,
        mapping: CustomMapping,
        published: bool,
        input: InputMapping | Literal["csv", "json", "xml"] = "json",
    ) -> None:
        super().__init__(external_id=external_id, mapping=mapping, published=published)
        self.input: InputMapping
        if isinstance(input, str) and input in _INPUTMAPPING_CLASS_BY_TYPE:
            self.input = _INPUTMAPPING_CLASS_BY_TYPE[input]()
        elif isinstance(input, InputMapping):
            self.input = input
        else:
            raise TypeError(f"Invalid input type: {input}")

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            mapping=CustomMapping._load(resource["mapping"]),
            published=resource["published"],
            input=InputMapping._load(resource["input"]) if "input" in resource else JSONInput(),
        )

    def as_write(self) -> MappingWrite:
        return self

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if isinstance(self.input, InputMapping):
            output["input"] = self.input.dump(camel_case)
        return output


class Mapping(_MappingCore):
    """A mapping is a custom transformation, translating the source format to a format that can be ingested into CDF.
    Mappings are written in the Cognite transformation language.

    This is the read/response format of a mapping.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        mapping (CustomMapping): The custom mapping.
        published (bool): Whether this mapping is published and should be available to be used in jobs.
        input (InputMapping): The input mapping.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        external_id: str,
        mapping: CustomMapping,
        published: bool,
        input: InputMapping,
        created_time: int,
        last_updated_time: int,
    ) -> None:
        super().__init__(external_id=external_id, mapping=mapping, published=published)
        self.input = input
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            mapping=CustomMapping._load(resource["mapping"]),
            published=resource["published"],
            input=InputMapping._load(resource["input"]),
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
        )

    def as_write(self) -> MappingWrite:
        return MappingWrite(
            external_id=self.external_id, mapping=self.mapping, published=self.published, input=self.input
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["input"] = self.input.dump(camel_case)
        return output


class MappingWriteList(CogniteResourceList[MappingWrite], ExternalIDTransformerMixin):
    _RESOURCE = MappingWrite


class MappingList(WriteableCogniteResourceList[MappingWrite, Mapping], ExternalIDTransformerMixin):
    _RESOURCE = Mapping

    def as_write(self) -> MappingWriteList:
        return MappingWriteList([mapping.as_write() for mapping in self.data])


class MappingUpdate(CogniteUpdate):
    def __init__(self, external_id: str) -> None:
        super().__init__(external_id=external_id)

    class _MappingUpdate(CognitePrimitiveUpdate):
        def set(self, value: CustomMapping) -> MappingUpdate:
            return self._set(value.dump())

    class _InputMappingUpdate(CognitePrimitiveUpdate):
        def set(self, value: InputMapping | None) -> MappingUpdate:
            return self._set(value.dump() if value is not None else None)

    class _PublishedUpdate(CognitePrimitiveUpdate):
        def set(self, value: bool) -> MappingUpdate:
            return self._set(value)

    @property
    def mapping(self) -> MappingUpdate._MappingUpdate:
        return self._MappingUpdate(self, "mapping")

    @property
    def input(self) -> MappingUpdate._InputMappingUpdate:
        return self._InputMappingUpdate(self, "input")

    @property
    def published(self) -> MappingUpdate._PublishedUpdate:
        return self._PublishedUpdate(self, "published")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("mapping", is_nullable=False),
            PropertySpec("input", is_nullable=True),
            PropertySpec("published", is_nullable=False),
        ]


_INPUTMAPPING_CLASS_BY_TYPE: dict[str, type[InputMapping]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in InputMapping.__subclasses__()
}
