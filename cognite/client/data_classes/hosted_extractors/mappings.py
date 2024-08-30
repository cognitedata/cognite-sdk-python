from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
    ExternalIDTransformerMixin,
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
        input (InputMapping | None): The input mapping.
    """

    def __init__(
        self, external_id: str, mapping: CustomMapping, published: bool, input: InputMapping | None = None
    ) -> None:
        super().__init__(external_id=external_id, mapping=mapping, published=published)
        self.input = input

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            mapping=CustomMapping._load(resource["mapping"]),
            published=resource["published"],
            input=InputMapping._load(resource["input"]) if "input" in resource else None,
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
        created_time (int): No description.
        last_updated_time (int): No description.
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
