from __future__ import annotations

from typing import Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    WriteableCogniteResource,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case


class StreamLimit(CogniteResource):
    """Numeric limit bucket for a stream (provisioned / optionally consumed)."""

    def __init__(self, provisioned: float, consumed: float | None = None) -> None:
        self.provisioned = provisioned
        self.consumed = consumed

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            provisioned=resource["provisioned"],
            consumed=resource.get("consumed"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {"provisioned": self.provisioned}
        if self.consumed is not None:
            out["consumed"] = self.consumed
        return convert_all_keys_to_camel_case(out) if camel_case else out


class StreamLifecycleSettings(CogniteResource):
    """Lifecycle metadata for a stream (human-readable)."""

    def __init__(
        self,
        retained_after_soft_delete: str,
        data_deleted_after: str | None = None,
    ) -> None:
        self.retained_after_soft_delete = retained_after_soft_delete
        self.data_deleted_after = data_deleted_after

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            retained_after_soft_delete=resource["retainedAfterSoftDelete"],
            data_deleted_after=resource.get("dataDeletedAfter"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {"retained_after_soft_delete": self.retained_after_soft_delete}
        if self.data_deleted_after is not None:
            out["data_deleted_after"] = self.data_deleted_after
        return convert_all_keys_to_camel_case(out) if camel_case else out


class StreamLimitSettings(CogniteResource):
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
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            max_records_total=StreamLimit._load(resource["maxRecordsTotal"]),
            max_giga_bytes_total=StreamLimit._load(resource["maxGigaBytesTotal"]),
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


class StreamSettings(CogniteResource):
    """Read model for stream settings (lifecycle + limits)."""

    def __init__(self, lifecycle: StreamLifecycleSettings, limits: StreamLimitSettings) -> None:
        self.lifecycle = lifecycle
        self.limits = limits

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            lifecycle=StreamLifecycleSettings._load(resource["lifecycle"]),
            limits=StreamLimitSettings._load(resource["limits"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "lifecycle": self.lifecycle.dump(camel_case=camel_case),
            "limits": self.limits.dump(camel_case=camel_case),
        }


class Stream(CogniteResource):
    """A stream (``StreamResponseItem``)."""

    def __init__(
        self,
        external_id: str,
        created_time: int,
        created_from_template: str,
        type: str,
        settings: StreamSettings,
    ) -> None:
        self.external_id = external_id
        self.created_time = created_time
        self.created_from_template = created_from_template
        self.type = type
        self.settings = settings

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            created_from_template=resource["createdFromTemplate"],
            type=resource["type"],
            settings=StreamSettings._load(resource["settings"]),
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


class StreamTemplate(CogniteResource):
    """Reference to an stream template (``StreamRequestItem.settings.template``)."""

    def __init__(self, name: str, version: str | None = None) -> None:
        self.name = name
        self.version = version

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(name=resource["name"], version=resource.get("version"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {"name": self.name}
        if self.version is not None:
            out["version"] = self.version
        return convert_all_keys_to_camel_case(out) if camel_case else out


class StreamTemplateWriteSettings(CogniteResource):
    """Write-side settings for creating a stream from a template (``{"template": {...}}``)."""

    def __init__(self, template: StreamTemplate) -> None:
        self.template = template

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(template=StreamTemplate._load(resource["template"]))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"template": self.template.dump(camel_case=camel_case)}


def _parse_stream_write_settings(raw: dict[str, Any]) -> StreamTemplateWriteSettings | dict[str, Any]:
    if set(raw.keys()) == {"template"} and isinstance(raw["template"], dict) and "name" in raw["template"]:
        return StreamTemplateWriteSettings._load(raw)
    return raw


class StreamWrite(WriteableCogniteResource["StreamWrite"]):
    """Request item for creating a stream (``StreamRequestItem``)."""

    def __init__(
        self,
        external_id: str,
        settings: StreamTemplateWriteSettings | dict[str, Any],
    ) -> None:
        self.external_id = external_id
        self.settings = settings

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            settings=_parse_stream_write_settings(resource["settings"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if isinstance(self.settings, CogniteResource):
            settings_dumped = self.settings.dump(camel_case=camel_case)
        else:
            settings_dumped = self.settings
        out = {"external_id": self.external_id, "settings": settings_dumped}
        return convert_all_keys_to_camel_case(out) if camel_case else out

    def as_write(self) -> Self:
        return self
