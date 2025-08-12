from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case_recursive

if TYPE_CHECKING:
    from cognite.client import CogniteClient

TemplateName = Literal[
    "ImmutableTestStream",
    "ImmutableDataStaging",
    "ImmutableNormalizedData",
    "ImmutableArchive",
    "MutableTestStream",
    "MutableLiveData",
]


@dataclass
class StreamTemplate(CogniteObject):
    """A template for a stream.

    Args:
        name (str): The name of the stream template.
    """

    name: TemplateName


@dataclass
class StreamSettings(CogniteObject):
    template: StreamTemplate


class StreamApply(WriteableCogniteResource):
    """A stream of records. This is the write version.

    Args:
        external_id (str): Textual description of the stream
        settings (StreamSettings): The settings for the stream, including the template.
    """

    def __init__(self, external_id: str, settings: StreamSettings) -> None:
        self.external_id = external_id
        self.settings = settings

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        settings = resource["settings"]
        template = settings["template"]
        return cls(
            external_id=resource["externalId"],
            settings=StreamSettings(
                template=StreamTemplate(
                    name=template["name"],
                ),
            ),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = {
            "external_id": self.external_id,
            "settings": {
                "template": {"name": self.settings.template.name},
            },
        }
        if camel_case:
            return convert_all_keys_to_camel_case_recursive(dumped)
        return dumped

    def as_write(self) -> StreamApply:
        """Returns this SpaceApply instance."""
        return self


class Stream(WriteableCogniteResource):
    """A stream of records. This is the read version."""

    def __init__(
        self,
        external_id: str,
        created_time: datetime,
        type: Literal["Immutable", "Mutable"],
        created_from_template: TemplateName,
    ) -> None:
        self.external_id = external_id
        self.created_time = created_time
        self.type = type
        self.created_from_template = created_from_template

    def as_apply(self) -> StreamApply:
        return StreamApply(
            external_id=self.external_id,
            settings=StreamSettings(template=StreamTemplate(name=self.created_from_template)),
        )

    def as_write(self) -> StreamApply:
        return self.as_apply()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            created_time=datetime.fromtimestamp(resource["createdTime"] / 1000.0),
            type=resource["type"],
            created_from_template=resource["createdFromTemplate"],
        )


class StreamApplyList(CogniteResourceList[StreamApply]):
    _RESOURCE = StreamApply

    def as_ids(self) -> list[str]:
        """
        Converts all the spaces to a space id list.

        Returns:
            list[str]: A list of space ids.
        """
        return [item.external_id for item in self]


class StreamList(WriteableCogniteResourceList[StreamApply, Stream]):
    """A list of Stream objects."""

    _RESOURCE = Stream

    def as_ids(self) -> list[str]:
        """
        Converts all the spaces to a space id list.

        Returns:
            list[str]: A list of space ids.
        """
        return [item.external_id for item in self]

    def as_apply(self) -> StreamApplyList:
        """
        Converts all the spaces to a space apply list.

        Returns:
            StreamApplyList: A list of space applies.
        """
        return StreamApplyList(
            resources=[item.as_apply() for item in self],
            cognite_client=self._get_cognite_client(),
        )

    def as_write(self) -> StreamApplyList:
        """
        Converts all the spaces to a space apply list.

        Returns:
            StreamApplyList: A list of space applies.
        """
        return self.as_apply()
