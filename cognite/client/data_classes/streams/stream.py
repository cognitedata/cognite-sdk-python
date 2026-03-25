from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class StreamLimit(CogniteObject):
    """Numeric limit bucket for a stream (provisioned / optionally consumed)."""

    def __init__(self, provisioned: float, consumed: float | None = None) -> None:
        self.provisioned = provisioned
        self.consumed = consumed

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            provisioned=resource["provisioned"],
            consumed=resource.get("consumed"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {"provisioned": self.provisioned}
        if self.consumed is not None:
            out["consumed"] = self.consumed
        return convert_all_keys_to_camel_case(out) if camel_case else out


class StreamLifecycleSettings(CogniteObject):
    """Lifecycle metadata for a stream (human-readable)."""

    def __init__(
        self,
        retained_after_soft_delete: str,
        data_deleted_after: str | None = None,
    ) -> None:
        self.retained_after_soft_delete = retained_after_soft_delete
        self.data_deleted_after = data_deleted_after

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            retained_after_soft_delete=resource["retainedAfterSoftDelete"],
            data_deleted_after=resource.get("dataDeletedAfter"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {"retained_after_soft_delete": self.retained_after_soft_delete}
        if self.data_deleted_after is not None:
            out["data_deleted_after"] = self.data_deleted_after
        return convert_all_keys_to_camel_case(out) if camel_case else out


class StreamLimitSettings(CogniteObject):
    """Provisioned/consumed limits for a stream."""

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
            max_records_total=StreamLimit._load(resource["maxRecordsTotal"], cognite_client=cognite_client),
            max_giga_bytes_total=StreamLimit._load(resource["maxGigaBytesTotal"], cognite_client=cognite_client),
            max_filtering_interval=resource.get("maxFilteringInterval"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {
            "max_records_total": self.max_records_total.dump(camel_case=camel_case),
            "max_giga_bytes_total": self.max_giga_bytes_total.dump(camel_case=camel_case),
        }
        if self.max_filtering_interval is not None:
            out["max_filtering_interval"] = self.max_filtering_interval
        return convert_all_keys_to_camel_case(out) if camel_case else out


class StreamSettings(CogniteObject):
    """Read model for stream settings (lifecycle + limits)."""

    def __init__(self, lifecycle: StreamLifecycleSettings, limits: StreamLimitSettings) -> None:
        self.lifecycle = lifecycle
        self.limits = limits

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            lifecycle=StreamLifecycleSettings._load(resource["lifecycle"], cognite_client=cognite_client),
            limits=StreamLimitSettings._load(resource["limits"], cognite_client=cognite_client),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "lifecycle": self.lifecycle.dump(camel_case=camel_case),
            "limits": self.limits.dump(camel_case=camel_case),
        }


class Stream(CogniteResource):
    """A stream (ILA ``StreamResponseItem``)."""

    def __init__(
        self,
        external_id: str,
        created_time: int,
        created_from_template: str,
        type: str,
        settings: StreamSettings,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.external_id = external_id
        self.created_time = created_time
        self.created_from_template = created_from_template
        self.type = type
        self.settings = settings
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            created_from_template=resource["createdFromTemplate"],
            type=resource["type"],
            settings=StreamSettings._load(resource["settings"], cognite_client=cognite_client),
            cognite_client=cognite_client,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out = {
            "external_id": self.external_id,
            "created_time": self.created_time,
            "created_from_template": self.created_from_template,
            "type": self.type,
            "settings": self.settings.dump(camel_case=camel_case),
        }
        return convert_all_keys_to_camel_case(out) if camel_case else out


class StreamList(CogniteResourceList[Stream], ExternalIDTransformerMixin):
    """List of streams (``StreamResponse.items``)."""

    _RESOURCE = Stream


class StreamWrite(CogniteObject):
    """Request item for creating a stream (``StreamRequestItem``)."""

    def __init__(self, external_id: str, settings: dict[str, Any]) -> None:
        self.external_id = external_id
        self.settings = settings

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(external_id=resource["externalId"], settings=resource["settings"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out = {"external_id": self.external_id, "settings": self.settings}
        return convert_all_keys_to_camel_case(out) if camel_case else out


class StreamDeleteItem(CogniteObject):
    """Identifier for ``POST /streams/delete``."""

    def __init__(self, external_id: str) -> None:
        self.external_id = external_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(external_id=resource["externalId"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out = {"external_id": self.external_id}
        return convert_all_keys_to_camel_case(out) if camel_case else out
