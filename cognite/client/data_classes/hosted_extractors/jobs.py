from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from typing_extensions import Self, TypeAlias

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    UnknownCogniteObject,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient

TargetStatus: TypeAlias = Literal["running", "paused"]
Status: TypeAlias = Literal[
    "startup_error",
    "paused",
    "shutting_down",
    "connected",
    "running",
    "waiting",
    "connection_error",
    "cdf_write_error",
    "transform_error",
]


@dataclass
class JobFormat(CogniteObject, ABC):
    _type: ClassVar[str]

    @classmethod
    @abstractmethod
    def _load_job(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type")
        job_cls = _JOBFORMAT_CLASS_BY_TYPE.get(type_)
        if job_cls is None:
            return UnknownCogniteObject(resource)  # type: ignore[return-value]
        return cast(Self, job_cls._load_job(resource))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["type"] = self._type
        return output


@dataclass
class Prefix(CogniteObject):
    from_topic: bool | None = None
    prefix: str | None = None


@dataclass
class ValueFormat(JobFormat):
    _type = "value"
    encoding: Literal["utf16", "utf16le"]
    compression: str = "gzip"
    prefix: Prefix | None = None

    @classmethod
    def _load_job(cls, resource: dict[str, Any]) -> ValueFormat:
        return cls(
            encoding=resource["encoding"],
            compression=resource["compression"],
            prefix=Prefix._load(resource["prefix"]) if "prefix" in resource else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.prefix:
            output["prefix"] = self.prefix.dump(camel_case)
        return output


@dataclass
class RockwellFormat(JobFormat):
    _type = "rockwell"
    encoding: Literal["utf16", "utf16le"]
    compression: str = "gzip"
    prefix: Prefix | None = None

    @classmethod
    def _load_job(cls, resource: dict[str, Any]) -> RockwellFormat:
        return cls(
            encoding=resource["encoding"],
            compression=resource["compression"],
            prefix=Prefix._load(resource["prefix"]) if "prefix" in resource else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.prefix:
            output["prefix"] = self.prefix.dump(camel_case)
        return output


@dataclass
class CustomFormat(JobFormat):
    _type = "custom"
    encoding: Literal["utf16", "utf16le"]
    mapping_id: str
    compression: str = "gzip"

    @classmethod
    def _load_job(cls, resource: dict[str, Any]) -> CustomFormat:
        return cls(
            encoding=resource["encoding"],
            compression=resource.get("compression", "gzip"),
            mapping_id=resource["mappingId"],
        )


@dataclass
class CogniteFormat(JobFormat):
    _type = "cognite"
    encoding: Literal["utf16", "utf16le"]
    compression: str = "gzip"
    prefix: Prefix | None = None

    @classmethod
    def _load_job(cls, resource: dict[str, Any]) -> CogniteFormat:
        return cls(
            encoding=resource["encoding"],
            compression=resource["compression"],
            prefix=Prefix._load(resource["prefix"]) if "prefix" in resource else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.prefix:
            output["prefix"] = self.prefix.dump(camel_case)
        return output


@dataclass
class JobConfig(CogniteObject, ABC): ...


@dataclass
class MQTTConfig(JobConfig):
    topic_filter: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(topic_filter=resource["topicFilter"])


@dataclass
class KafkaConfig(JobConfig):
    topic: str
    partitions: int = 1

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(topic=resource["topic"], partitions=resource.get("partitions", 1))


class _JobCore(WriteableCogniteResource["JobWrite"]):
    def __init__(
        self, external_id: str, destination_id: str, source_id: str, format: JobFormat, config: JobConfig
    ) -> None:
        self.external_id = external_id
        self.destination_id = destination_id
        self.source_id = source_id
        self.format = format
        self.config = config


class JobWrite(_JobCore):
    """A hosted extractor job represents the running extractor.

    Jobs produce logs and metrics that give the state of the job.

    This is the write/request format of the job.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        destination_id (str): ID of the destination this job should write to.
        source_id (str): ID of the source this job should read from.
        format: The format of the messages from the source. This is used to convert
            messages coming from the source system to a format that can be inserted into CDF.
        config: Configuration for the job. This is specific to the source system.

    """

    def as_write(self) -> JobWrite:
        return self


class Job(_JobCore):
    """A hosted extractor job represents the running extractor.

    Jobs produce logs and metrics that give the state of the job.

    This is the read/response format of the job.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        destination_id (str): ID of the destination this job should write to.
        source_id (str): ID of the source this job should read from.
        format (JobFormat): The format of the messages from the source. This is used to convert messages coming from the source system to a format that can be inserted into CDF.
        target_status (TargetStatus): The target status of a job. Set this to start or stop the job.
        status (Status): Status of this job.
        config (JobConfig): Configuration for the job. This is specific to the source system.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.

    """

    def __init__(
        self,
        external_id: str,
        destination_id: str,
        source_id: str,
        format: JobFormat,
        target_status: TargetStatus,
        status: Status,
        config: JobConfig,
        created_time: int,
        last_updated_time: int,
    ) -> None:
        super().__init__(external_id, destination_id, source_id, format, config)
        self.target_status = target_status
        self.status = status
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    def as_write(self) -> JobWrite:
        return JobWrite(
            external_id=self.external_id,
            destination_id=self.destination_id,
            source_id=self.source_id,
            format=self.format,
            config=self.config,
        )


class JobWriteList(CogniteResourceList[JobWrite], ExternalIDTransformerMixin):
    _RESOURCE = JobWrite


class JobList(WriteableCogniteResourceList[JobWrite, Job], ExternalIDTransformerMixin):
    _RESOURCE = Job

    def as_write(self) -> JobWriteList:
        return JobWriteList([job.as_write() for job in self.data])


_JOBFORMAT_CLASS_BY_TYPE: dict[str, type[JobFormat]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in JobFormat.__subclasses__()
    if hasattr(subclass, "_type")
}
