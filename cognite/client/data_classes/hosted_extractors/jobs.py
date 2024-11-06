from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeAlias, cast

from typing_extensions import Self

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
JobStatus: TypeAlias = Literal[
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
class JobConfig(CogniteObject, ABC):
    @classmethod
    @abstractmethod
    def _load_config(cls, data: dict[str, Any]) -> Self:
        raise NotImplementedError()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        if "topicFilter" in resource:
            return cast(Self, MQTTConfig._load_config(resource))
        elif "topic" in resource:
            return cast(Self, KafkaConfig._load_config(resource))
        elif "interval" in resource and "path" in resource:
            return cast(Self, RestConfig._load_config(resource))
        else:
            return cast(Self, UnknownCogniteObject(resource))


@dataclass
class MQTTConfig(JobConfig):
    topic_filter: str

    @classmethod
    def _load_config(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(topic_filter=resource["topicFilter"])


@dataclass
class KafkaConfig(JobConfig):
    topic: str
    partitions: int = 1

    @classmethod
    def _load_config(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(topic=resource["topic"], partitions=resource.get("partitions", 1))


@dataclass
class IncrementalLoad(CogniteObject, ABC):
    _type: ClassVar[str]

    @classmethod
    @abstractmethod
    def _load_incremental_load(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type")
        incremental_load_cls = _INCREMENTALLOAD_CLASS_BY_TYPE.get(type_)
        if incremental_load_cls is None:
            return UnknownCogniteObject(resource)  # type: ignore[return-value]
        return cast(Self, incremental_load_cls._load_incremental_load(resource))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["type"] = self._type
        return output


@dataclass
class BodyLoad(IncrementalLoad):
    _type = "body"
    value: str

    @classmethod
    def _load_incremental_load(cls, resource: dict[str, Any]) -> Self:
        return cls(value=resource["value"])


@dataclass
class HeaderValueLoad(IncrementalLoad):
    _type = "headerValue"
    key: str
    value: str

    @classmethod
    def _load_incremental_load(cls, resource: dict[str, Any]) -> Self:
        return cls(key=resource["key"], value=resource["value"])


@dataclass
class QueryParamLoad(IncrementalLoad):
    _type = "queryParam"
    key: str
    value: str

    @classmethod
    def _load_incremental_load(cls, resource: dict[str, Any]) -> Self:
        return cls(key=resource["key"], value=resource["value"])


@dataclass
class NextUrlLoad(IncrementalLoad):
    _type = "nextUrl"
    value: str

    @classmethod
    def _load_incremental_load(cls, resource: dict[str, Any]) -> Self:
        return cls(value=resource["value"])


@dataclass
class RestConfig(JobConfig):
    interval: Literal["5m", "15m", "1h", "6h", "12h", "1d"]
    path: str
    method: Literal["get", "post"] = "get"
    body: str | None = None
    query: dict[str, str] | None = None
    headers: dict[str, str] | None = None
    incremental_load: IncrementalLoad | None = None
    pagination: IncrementalLoad | None = None

    def __post_init__(self) -> None:
        if isinstance(self.incremental_load, NextUrlLoad):
            raise ValueError("incremental_load cannot be of type NextUrlLoad")

    @classmethod
    def _load_config(cls, resource: dict[str, Any]) -> Self:
        return cls(
            interval=resource["interval"],
            path=resource["path"],
            method=resource.get("method", "get"),
            body=resource.get("body"),
            query=resource.get("query"),
            headers=resource.get("headers"),
            incremental_load=IncrementalLoad._load(resource["incrementalLoad"])
            if "incrementalLoad" in resource
            else None,
            pagination=IncrementalLoad._load(resource["pagination"]) if "pagination" in resource else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.incremental_load:
            output["incrementalLoad"] = self.incremental_load.dump(camel_case)
        if self.pagination:
            output["pagination"] = self.pagination.dump(camel_case)
        return output


class _JobCore(WriteableCogniteResource["JobWrite"]):
    def __init__(
        self, external_id: str, destination_id: str, source_id: str, format: JobFormat, config: JobConfig | None = None
    ) -> None:
        self.external_id = external_id
        self.destination_id = destination_id
        self.source_id = source_id
        self.format = format
        self.config = config

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["format"] = self.format.dump(camel_case)
        if self.config is not None:
            output["config"] = self.config.dump(camel_case)
        return output


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
        config: Configuration for the job. This must match the source. For example, if the source is MQTT,
            this must be an MQTT configuration.

    """

    def as_write(self) -> JobWrite:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> JobWrite:
        return cls(
            external_id=resource["externalId"],
            destination_id=resource["destinationId"],
            source_id=resource["sourceId"],
            format=JobFormat._load(resource["format"]),
            config=JobConfig._load(resource["config"]) if "config" in resource else None,
        )


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
        status (JobStatus): Status of this job.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        config (JobConfig | None): Configuration for the job. This is specific to the source system.

    """

    def __init__(
        self,
        external_id: str,
        destination_id: str,
        source_id: str,
        format: JobFormat,
        target_status: TargetStatus,
        status: JobStatus,
        created_time: int,
        last_updated_time: int,
        config: JobConfig | None = None,
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

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Job:
        return cls(
            external_id=resource["externalId"],
            destination_id=resource["destinationId"],
            source_id=resource["sourceId"],
            format=JobFormat._load(resource["format"]),
            target_status=resource["targetStatus"],
            status=resource["status"],
            config=JobConfig._load(resource["config"]) if "config" in resource else None,
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
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

    class _ConfigUpdate(CognitePrimitiveUpdate):
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
    def config(self) -> JobUpdate._ConfigUpdate:
        return self._ConfigUpdate(self, "config")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("destination_id", is_nullable=False),
            PropertySpec("source_id", is_nullable=False),
            PropertySpec("format", is_nullable=False),
            PropertySpec("target_status", is_nullable=False),
            PropertySpec("config", is_nullable=False),
        ]


class JobLogs(CogniteResource):
    """Logs for a hosted extractor job.

    Args:
        job_external_id (str): The external ID of the job.
        type (Literal['paused', 'startup_error', 'connection_error', 'connected', 'transform_error', 'cdf_write_error', 'ok']): Type of log entry.
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
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> JobLogs:
        return cls(
            job_external_id=resource["jobExternalId"],
            type=resource["type"],
            created_time=resource["createdTime"],
            message=resource.get("message"),
        )


class JobMetrics(CogniteResource):
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


class JobLogsList(CogniteResourceList[JobLogs]):
    _RESOURCE = JobLogs


class JobMetricsList(CogniteResourceList[JobMetrics]):
    _RESOURCE = JobMetrics


_JOBFORMAT_CLASS_BY_TYPE: dict[str, type[JobFormat]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in JobFormat.__subclasses__()
}

_INCREMENTALLOAD_CLASS_BY_TYPE: dict[str, type[IncrementalLoad]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in IncrementalLoad.__subclasses__()
}
