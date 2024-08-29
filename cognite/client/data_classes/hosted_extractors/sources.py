from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, ClassVar, Literal

from typing_extensions import TypeAlias

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    T_WriteClass,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    pass


SourceType: TypeAlias = Literal["mqtt5", "mqtt3", "eventhub"]


class SourceWrite(CogniteResource, ABC):
    """A hosted extractor source represents an external source system on the internet.
    The source resource in CDF contains all the information the extractor needs to
    connect to the external source system.

    This is the write/request format of the source resource.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    _type: ClassVar[str]

    def __init__(self, external_id: str) -> None:
        self.external_id = external_id


class Source(WriteableCogniteResource[T_WriteClass], ABC):
    """A hosted extractor source represents an external source system on the internet.
    The source resource in CDF contains all the information the extractor needs to
    connect to the external source system.

    This is the read/response format of the source resource.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    _type: ClassVar[str]

    def __init__(self, external_id: str) -> None:
        self.external_id = external_id


class EventHubSourceWrite(SourceWrite):
    """A hosted extractor source represents an external source system on the internet.
    The source resource in CDF contains all the information the extractor needs to
    connect to the external source system.

    This is the write/request format of the source resource.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        host (str): URL of the event hub consumer endpoint.
        event_hub_name (str): Name of the event hub
        key_name (str): The name of the Event Hub key to use.
        key_value (str): Value of the Event Hub key to use for authentication.
        consumer_group (str | None): The event hub consumer group to use. Microsoft recommends having a distinct consumer group for each application consuming data from event hub. If left out, this uses the default consumer group.
    """

    def __init__(
        self,
        external_id: str,
        host: str,
        event_hub_name: str,
        key_name: str,
        key_value: str,
        consumer_group: str | None = None,
    ) -> None:
        super().__init__(external_id)
        self.host = host
        self.event_hub_name = event_hub_name
        self.key_name = key_name
        self.key_value = key_value
        self.consumer_group = consumer_group

    def as_write(self) -> SourceWrite:
        return self


class EventHubSource(Source):
    """A hosted extractor source represents an external source system on the internet.
    The source resource in CDF contains all the information the extractor needs to
    connect to the external source system.

    This is the read/response format of the source resource.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        host (str): URL of the event hub consumer endpoint.
        event_hub_name (str): Name of the event hub
        key_name (str): The name of the Event Hub key to use.
        created_time (int): No description.
        last_updated_time (int): No description.
        consumer_group (str | None): The event hub consumer group to use. Microsoft recommends having a distinct consumer group for each application consuming data from event hub. If left out, this uses the default consumer group.
    """

    def __init__(
        self,
        external_id: str,
        host: str,
        event_hub_name: str,
        key_name: str,
        created_time: int,
        last_updated_time: int,
        consumer_group: str | None = None,
    ) -> None:
        super().__init__(external_id)
        self.host = host
        self.event_hub_name = event_hub_name
        self.key_name = key_name
        self.consumer_group = consumer_group
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    def as_write(self, key_value: str | None = None) -> EventHubSourceWrite:
        if key_value is None:
            raise ValueError("key_value must be provided")
        return EventHubSourceWrite(
            external_id=self.external_id,
            host=self.host,
            event_hub_name=self.event_hub_name,
            key_name=self.key_name,
            key_value=key_value,
            consumer_group=self.consumer_group,
        )


class SourceWriteList(CogniteResourceList[SourceWrite], ExternalIDTransformerMixin):
    _RESOURCE = SourceWrite


class SourceList(WriteableCogniteResourceList[SourceWrite, Source], ExternalIDTransformerMixin):
    _RESOURCE = Source

    def as_write(
        self,
    ) -> SourceWriteList:
        raise TypeError(f"{type(self).__name__} cannot be converted to write")
