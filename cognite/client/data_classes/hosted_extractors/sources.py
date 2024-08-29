from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import Self, TypeAlias

from cognite.client.data_classes._base import (
    CogniteResourceList,
    ExternalIDTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient


SourceType: TypeAlias = Literal["mqtt5", "mqtt3", "eventhub"]


class _SourceCore(WriteableCogniteResource["SourceWrite"], ABC):
    def __init__(
        self,
        type: SourceType,
        external_id: str,
        host: str,
        event_hub_name: str,
        key_name: str,
        consumer_group: str | None = None,
    ) -> None:
        self.type = type
        self.external_id = external_id
        self.host = host
        self.event_hub_name = event_hub_name
        self.key_name = key_name
        self.consumer_group = consumer_group


class SourceWrite(_SourceCore):
    """A hosted extractor source represents an external source system on the internet.
    The source resource in CDF contains all the information the extractor needs to
    connect to the external source system.

    This is the write/request format of the source resource.

    Args:
        type (SourceType): Source type.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        host (str): URL of the event hub consumer endpoint.
        event_hub_name (str): Name of the event hub
        key_name (str): The name of the Event Hub key to use.
        key_value (str): Value of the Event Hub key to use for authentication.
        consumer_group (str | None): The event hub consumer group to use. Microsoft recommends having a distinct consumer group for each application consuming data from event hub. If left out, this uses the default consumer group.
    """

    def __init__(
        self,
        type: SourceType,
        external_id: str,
        host: str,
        event_hub_name: str,
        key_name: str,
        key_value: str,
        consumer_group: str | None = None,
    ) -> None:
        super().__init__(type, external_id, host, event_hub_name, key_name, consumer_group)
        self.key_value = key_value

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> SourceWrite:
        return cls(
            type=resource["type"],
            external_id=resource["externalId"],
            host=resource["host"],
            event_hub_name=resource["eventHubName"],
            key_name=resource["keyName"],
            key_value=resource["keyValue"],
            consumer_group=resource.get("consumerGroup"),
        )

    def as_write(self) -> SourceWrite:
        return self


class Source(_SourceCore):
    """A hosted extractor source represents an external source system on the internet.
    The source resource in CDF contains all the information the extractor needs to
    connect to the external source system.

    This is the read/response format of the source resource.

    Args:
        type (SourceType): Source type.
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
        type: SourceType,
        external_id: str,
        host: str,
        event_hub_name: str,
        key_name: str,
        created_time: int,
        last_updated_time: int,
        consumer_group: str | None = None,
    ) -> None:
        super().__init__(type, external_id, host, event_hub_name, key_name, consumer_group)
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            type=resource["type"],
            external_id=resource["externalId"],
            host=resource["host"],
            event_hub_name=resource["eventHubName"],
            key_name=resource["keyName"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            consumer_group=resource.get("consumerGroup"),
        )

    def as_write(self, key_value: str | None = None) -> SourceWrite:
        if key_value is None:
            raise ValueError("key_value must be provided")
        return SourceWrite(
            type=self.type,
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

    def as_write(self, key_values: SequenceNotStr[str] | None = None) -> SourceWriteList:
        if key_values is None:
            raise ValueError("key_values must be provided")
        if len(self) != len(key_values):
            raise ValueError("key_values must be the same length as the sources")
        return SourceWriteList([source.as_write(key_value) for source, key_value in zip(self, key_values)])
