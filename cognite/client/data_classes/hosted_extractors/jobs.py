from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from typing_extensions import Self, TypeAlias

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


class JobUpdate(CogniteUpdate):
    def __init__(self, external_id: str) -> None:
        super().__init__(external_id=external_id)

    class _StringUpdate(CognitePrimitiveUpdate):
        def set(self, value: str) -> JobUpdate:
            return self._set(value)

    class _FormatUpdate(CognitePrimitiveUpdate):
        def set(self, value: JobFormat) -> JobUpdate:
            return self._set(value.dump(camel_case=True))

    class ConfigUpdate(CognitePrimitiveUpdate):
        def set(self, value: JobConfig) -> JobUpdate:
            return self._set(value.dump(camel_case=True))

    class _TargetStatusUpdate(CognitePrimitiveUpdate):
        def set(self, value: TargetStatus) -> JobUpdate:
            return self._set(value)

    @property
    def destination_id(self) -> JobUpdate._StringUpdate:
        return self._StringUpdate(self, "destinationId")

    @property
    def source_id(self) -> JobUpdate._StringUpdate:
        return self._StringUpdate(self, "sourceId")

    @property
    def format(self) -> JobUpdate._FormatUpdate:
        return self._FormatUpdate(self, "format")

    @property
    def target_status(self) -> JobUpdate._TargetStatusUpdate:
        return self._TargetStatusUpdate(self, "targetStatus")

    @property
    def config(self) -> JobUpdate.ConfigUpdate:
        return self.ConfigUpdate(self, "config")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("destination_id", is_nullable=False),
            PropertySpec("source_id", is_nullable=False),
            PropertySpec("format", is_nullable=False),
            PropertySpec("target_status", is_nullable=False),
            PropertySpec("config", is_nullable=False),
        ]


class JobLog(CogniteResource):
    """Logs for a hosted extractor job.

    Args:
        job_external_id (str): The external ID of the job.
        type (Literal["paused", "startup_error", "connection_error", "connected", "transform_error", "cdf_write_error", "ok"]): Type of log entry.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        message (str | None): Log message. Not all log entries have messages.

    Statuses

    * `paused`: indicates that the job has been stopped manually.
    * `startup_error`: indicates that the job failed to start at all and requires changes to configuration.
    * `connection_error`: indicates that the job lost connection to the source system.
    * `connected`: indicates that the job has connected to the source but did not yet receive any data,
    * `transform_error`: indicates that the job received data, but it failed to transform.
    * `cdf_write_error`: indicates that ingesting the data to CDF failed.
    * `ok`: means that data was successfully ingested to CDF.

    """

    def __init__(
        self,
        job_external_id: str,
        type: Literal[
            "paused", "startup_error", "connection_error", "connected", "transform_error", "cdf_write_error", "ok"
        ],
        created_time: int,
        message: str | None = None,
    ) -> None:
        self.job_external_id = job_external_id
        self.type = type
        self.created_time = created_time
        self.message = message

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> JobLog:
        return cls(
            job_external_id=resource["jobExternalId"],
            type=resource["type"],
            created_time=resource["createdTime"],
            message=resource.get("message"),
        )


class JobMetric(CogniteResource):
    """Metrics for a hosted extractor job.

    Args:
        job_external_id (str): External ID of the job this metrics batch belongs to.
        timestamp (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Metrics are from the UTC hour this timestamp is ingest. For example, if this timestamp is at 01:43:15, the metrics batch contains metrics from 01:00:00 to 01:43:15.
        source_messages (int): Number of messages received from the source system.
        cdf_input_values (int): Destination resources successfully transformed and passed to CDF.
        cdf_requests (int): Requests made to CDF containing data produced by this job.
        transform_failures (int): Source messages that failed to transform.
        cdf_write_failures (int): Times the destination received data from transformations, but failed to produce a valid request to CDF.
        cdf_skipped_values (int): Values the destination received from the source, then decided to skip due to data type mismatch, invalid content, or other.
        cdf_failed_values (int): Values the destination was unable to upload to CDF.
        cdf_uploaded_values (int): Values the destination successfully uploaded to CDF.

    """

    def __init__(
        self,
        job_external_id: str,
        timestamp: int,
        source_messages: int,
        cdf_input_values: int,
        cdf_requests: int,
        transform_failures: int,
        cdf_write_failures: int,
        cdf_skipped_values: int,
        cdf_failed_values: int,
        cdf_uploaded_values: int,
    ) -> None:
        self.job_external_id = job_external_id
        self.timestamp = timestamp
        self.source_messages = source_messages
        self.cdf_input_values = cdf_input_values
        self.cdf_requests = cdf_requests
        self.transform_failures = transform_failures
        self.cdf_write_failures = cdf_write_failures
        self.cdf_skipped_values = cdf_skipped_values
        self.cdf_failed_values = cdf_failed_values
        self.cdf_uploaded_values = cdf_uploaded_values

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            job_external_id=resource["jobExternalId"],
            timestamp=resource["timestamp"],
            source_messages=resource["sourceMessages"],
            cdf_input_values=resource["cdfInputValues"],
            cdf_requests=resource["cdfRequests"],
            transform_failures=resource["transformFailures"],
            cdf_write_failures=resource["cdfWriteFailures"],
            cdf_skipped_values=resource["cdfSkippedValues"],
            cdf_failed_values=resource["cdfFailedValues"],
            cdf_uploaded_values=resource["cdfUploadedValues"],
        )


class JobLogList(CogniteResourceList[JobLog]):
    _RESOURCE = JobLog


class JobMetricList(CogniteResourceList[JobMetric]):
    _RESOURCE = JobMetric


_JOBFORMAT_CLASS_BY_TYPE: dict[str, type[JobFormat]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in JobFormat.__subclasses__()
    if hasattr(subclass, "_type")
}
