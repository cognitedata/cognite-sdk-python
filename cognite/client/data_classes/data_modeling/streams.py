from __future__ import annotations

from typing import Any, Literal

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


class StreamLifecycleSettings(CogniteResource):
    """Lifecycle metadata for a stream."""

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


class Stream(WriteableCogniteResource["StreamWrite"]):
    """A stream instance returned from the streams API.

    This is the read version of :class:`StreamWrite`.
    """

    def __init__(
        self,
        external_id: str,
        created_time: int,
        created_from_template: str,
        type: Literal["Immutable", "Mutable"],
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

    def as_write(self) -> StreamWrite:
        return StreamWrite(
            external_id=self.external_id,
            settings=StreamWriteSettings(template=StreamTemplate(name=self.created_from_template)),
        )


class StreamList(CogniteResourceList[Stream], ExternalIDTransformerMixin):
    _RESOURCE = Stream


class StreamTemplate(CogniteResource):
    """Reference to a stream template by name.

    Args:
        name (str): Name of the stream template to create the stream from.
    """

    def __init__(self, name: str) -> None:
        self.name = name

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(name=resource["name"])


class StreamWriteSettings(CogniteResource):
    """Write-side settings that specify which template to create the stream from.

    Args:
        template (StreamTemplate): The template to create the stream from.
    """

    def __init__(self, template: StreamTemplate) -> None:
        self.template = template

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(template=StreamTemplate._load(resource["template"]))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"template": self.template.dump(camel_case=camel_case)}


class StreamWrite(WriteableCogniteResource["StreamWrite"]):
    """Write representation of a stream, used when creating a new stream.

    This is the write version of :class:`Stream`.

    Args:
        external_id (str): External ID of the stream, must be unique within the project.
        settings (StreamWriteSettings): Settings specifying which template to create the stream from.
    """

    def __init__(
        self,
        external_id: str,
        settings: StreamWriteSettings,
    ) -> None:
        self.external_id = external_id
        self.settings = settings

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            settings=StreamWriteSettings._load(resource["settings"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out = {"external_id": self.external_id, "settings": self.settings.dump(camel_case=camel_case)}
        return convert_all_keys_to_camel_case(out) if camel_case else out

    def as_write(self) -> StreamWrite:
        return self


class StreamTemplateLimit(CogniteResource):
    """A single limit or lifecycle setting of a stream template.

    Args:
        type (str): The kind of setting, e.g. ``"fixed"``.
        name (str): Human readable description of the setting.
        value (int | float | str): Integer or float for numeric limits, an ISO-8601 duration string
            (e.g. ``"PT168H"``) for durations, or ``"infinity"``.
    """

    def __init__(self, type: str, name: str, value: int | float | str) -> None:
        self.type = type
        self.name = name
        self.value = value

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(type=resource["type"], name=resource["name"], value=resource["value"])


class StreamTemplateSettings(CogniteResource):
    """Limits and lifecycle settings of a stream template.

    Args:
        limits (dict[str, StreamTemplateLimit]): Limits keyed by identifier, e.g. ``"maxRecordsTotal"``.
        lifecycle (dict[str, StreamTemplateLimit]): Lifecycle settings keyed by identifier, e.g. ``"retainedAfterSoftDelete"``.
    """

    def __init__(
        self,
        limits: dict[str, StreamTemplateLimit],
        lifecycle: dict[str, StreamTemplateLimit],
    ) -> None:
        self.limits = limits
        self.lifecycle = lifecycle

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            limits={k: StreamTemplateLimit._load(v) for k, v in resource["limits"].items()},
            lifecycle={k: StreamTemplateLimit._load(v) for k, v in resource["lifecycle"].items()},
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        # The keys of the limits/lifecycle maps are identifiers, not attribute names, so they are kept as-is.
        return {
            "limits": {k: v.dump(camel_case=camel_case) for k, v in self.limits.items()},
            "lifecycle": {k: v.dump(camel_case=camel_case) for k, v in self.lifecycle.items()},
        }


class StreamTemplateInfo(CogniteResource):
    """A stream template available in the project, as returned by the list templates endpoint.

    Only templates the project may create streams from (at least one provisioned) are returned.

    Args:
        name (str): Name of the template, used when creating a stream (see :class:`StreamTemplate`).
        type (Literal['Immutable', 'Mutable']): The type of streams created from the template.
        max_streams (int): Number of streams that can be provisioned from this template.
        settings (StreamTemplateSettings): Limits and lifecycle settings of streams created from the template.
    """

    def __init__(
        self,
        name: str,
        type: Literal["Immutable", "Mutable"],
        max_streams: int,
        settings: StreamTemplateSettings,
    ) -> None:
        self.name = name
        self.type = type
        self.max_streams = max_streams
        self.settings = settings

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            name=resource["name"],
            type=resource["type"],
            max_streams=resource["maxStreams"]["provisioned"],
            settings=StreamTemplateSettings._load(resource["settings"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        max_streams_key = "maxStreams" if camel_case else "max_streams"
        return {
            "name": self.name,
            "type": self.type,
            max_streams_key: {"provisioned": self.max_streams},
            "settings": self.settings.dump(camel_case=camel_case),
        }


class StreamTemplateInfoList(CogniteResourceList[StreamTemplateInfo]):
    _RESOURCE = StreamTemplateInfo
