from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
    WriteableCogniteResource,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class StreamWriteSettings(CogniteObject):
    """Settings for creating a stream.

    Args:
        template_name (str): The name of the stream template. Templates are project-specific and should be retrieved from your CDF project configuration.
    """

    def __init__(self, template_name: str) -> None:
        self.template_name = template_name

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(template_name=resource["template"]["name"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"template": {"name": self.template_name}}


class StreamLimit(CogniteObject):
    """Limit settings for a stream resource.

    Args:
        provisioned (float): Amount of resource provisioned.
        consumed (float | None): Amount of resource consumed. Only returned when include_statistics=True.
    """

    def __init__(self, provisioned: float, consumed: float | None = None) -> None:
        self.provisioned = provisioned
        self.consumed = consumed

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(provisioned=resource["provisioned"], consumed=resource.get("consumed"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"provisioned": self.provisioned, "consumed": self.consumed}


class StreamLifecycleSettings(CogniteObject):
    """Lifecycle settings for a stream.

    These settings are populated from the stream creation template and cannot be changed.

    Args:
        retained_after_soft_delete (str): Time until soft-deleted stream is actually deleted, in ISO-8601 format.
        hot_phase_duration (str | None): Time for which records are kept in hot storage, in ISO-8601 format. Only for immutable streams.
        data_deleted_after (str | None): Time after which records are scheduled for deletion, in ISO-8601 format. Only for immutable streams.
    """

    def __init__(
        self,
        retained_after_soft_delete: str,
        hot_phase_duration: str | None = None,
        data_deleted_after: str | None = None,
    ) -> None:
        self.retained_after_soft_delete = retained_after_soft_delete
        self.hot_phase_duration = hot_phase_duration
        self.data_deleted_after = data_deleted_after

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            retained_after_soft_delete=resource["retainedAfterSoftDelete"],
            hot_phase_duration=resource.get("hotPhaseDuration"),
            data_deleted_after=resource.get("dataDeletedAfter"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "retainedAfterSoftDelete": self.retained_after_soft_delete,
            "hotPhaseDuration": self.hot_phase_duration,
            "dataDeletedAfter": self.data_deleted_after,
        }


class StreamLimitSettings(CogniteObject):
    """Limit settings for a stream.

    Args:
        max_records_total (StreamLimit): Maximum number of records that can be stored.
        max_giga_bytes_total (StreamLimit): Maximum amount of data in gigabytes.
        max_filtering_interval (str | None): Maximum time range for lastUpdatedTime filter, in ISO-8601 format. Only for immutable streams.
    """

    def __init__(
        self,
        max_records_total: StreamLimit,
        max_giga_bytes_total: StreamLimit,
        max_filtering_interval: str | None = None,
    ) -> None:
        self.max_records_total = max_records_total
        self.max_giga_bytes_total = max_giga_bytes_total
        self.max_filtering_interval = max_filtering_interval

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            max_records_total=StreamLimit._load(resource["maxRecordsTotal"]),
            max_giga_bytes_total=StreamLimit._load(resource["maxGigaBytesTotal"]),
            max_filtering_interval=resource.get("maxFilteringInterval"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "maxRecordsTotal": self.max_records_total.dump(camel_case),
            "maxGigaBytesTotal": self.max_giga_bytes_total.dump(camel_case),
            "maxFilteringInterval": self.max_filtering_interval,
        }


class StreamSettings(CogniteObject):
    """Response settings for a stream.

    These are read-only settings returned when retrieving a stream.

    Args:
        lifecycle (StreamLifecycleSettings): Lifecycle settings.
        limits (StreamLimitSettings): Limit settings.
    """

    def __init__(self, lifecycle: StreamLifecycleSettings, limits: StreamLimitSettings) -> None:
        self.lifecycle = lifecycle
        self.limits = limits

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            lifecycle=StreamLifecycleSettings._load(resource["lifecycle"]),
            limits=StreamLimitSettings._load(resource["limits"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "lifecycle": self.lifecycle.dump(camel_case),
            "limits": self.limits.dump(camel_case),
        }


class StreamWrite(WriteableCogniteResource):
    """A stream for data ingestion. This is the write version.

    Args:
        external_id (str): A unique identifier for the stream.
        settings (StreamWriteSettings): The settings for the stream, including the template.
    """

    def __init__(self, external_id: str, settings: StreamWriteSettings) -> None:
        self.external_id = external_id
        self.settings = settings

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            settings=StreamWriteSettings._load(resource["settings"], cognite_client),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "externalId": self.external_id,
            "settings": self.settings.dump(camel_case=camel_case),
        }

    def as_write(self) -> StreamWrite:
        """Returns this StreamWrite instance."""
        return self


class Stream(WriteableCogniteResource["StreamWrite"]):
    """A stream for data ingestion. Streams are immutable after creation and cannot be updated.

    Args:
        external_id (str): A unique identifier for the stream.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        type (Literal['Immutable', 'Mutable']): The type of the stream.
        created_from_template (str): The template the stream was created from.
        settings (StreamSettings): The stream settings including lifecycle and limits.
    """

    def __init__(
        self,
        external_id: str,
        created_time: int,
        type: Literal["Immutable", "Mutable"],
        created_from_template: str,
        settings: StreamSettings,
    ) -> None:
        self.external_id = external_id
        self.created_time = created_time
        self.type = type
        self.created_from_template = created_from_template
        self.settings = settings

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            type=resource["type"],
            created_from_template=resource["createdFromTemplate"],
            settings=StreamSettings._load(resource["settings"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "externalId": self.external_id,
            "createdTime": self.created_time,
            "type": self.type,
            "createdFromTemplate": self.created_from_template,
            "settings": self.settings.dump(camel_case),
        }

    def as_write(self) -> StreamWrite:
        """Returns a StreamWrite object with the same external_id and template.

        Note: Since streams are immutable and cannot be updated, this method creates
        a StreamWrite suitable for creating a NEW stream with the same template settings,
        not for updating the existing stream. To create a new stream, you must provide
        a different external_id.

        Returns:
            StreamWrite: A write object based on this stream's template.
        """
        return StreamWrite(
            external_id=self.external_id,
            settings=StreamWriteSettings(template_name=self.created_from_template),
        )


class StreamList(CogniteResourceList[Stream]):
    _RESOURCE = Stream

    def as_ids(self) -> list[str]:
        """
        Converts all the streams to a stream id list.

        Returns:
            list[str]: A list of stream ids.
        """
        return [item.external_id for item in self]
