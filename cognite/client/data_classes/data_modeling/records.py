from __future__ import annotations

from typing import Any, Literal, TypeAlias

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.filters import Filter

# ─── Write helpers ─────────────────────────────────────────────────────────────


class RecordSourceReference(CogniteResource):
    """Container reference used as a source in a record write.

    Args:
        space (str): Space that contains the container.
        external_id (str): External ID of the container.
        type (str): Must be ``"container"`` (default).
    """

    def __init__(self, space: str, external_id: str, type: str = "container") -> None:
        self.space = space
        self.external_id = external_id
        self.type = type

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            type=resource.get("type", "container"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "type": self.type,
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
        }


class RecordSource(CogniteResource):
    """Container source with property values for a record write.

    Args:
        source (RecordSourceReference): Reference to the container.
        properties (dict[str, Any]): Map of ``{property_id: value}``.
    """

    def __init__(self, source: RecordSourceReference, properties: dict[str, Any]) -> None:
        self.source = source
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            source=RecordSourceReference._load(resource["source"]),
            properties=resource["properties"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "source": self.source.dump(camel_case=camel_case),
            "properties": self.properties,
        }


class RecordWrite(WriteableCogniteResource["RecordWrite"]):
    """Write representation of a record, used for ingest and upsert.

    This is the write version of :class:`Record`.

    Args:
        space (str): Space the record belongs to.
        external_id (str): External ID of the record (1–256 chars, no null bytes).
        sources (list[RecordSource]): Container property values to write (1–100 sources).

    Examples:

        Build a record write object:

            >>> from cognite.client.data_classes.data_modeling.records import (
            ...     RecordWrite,
            ...     RecordSource,
            ...     RecordSourceReference,
            ... )
            >>> rec = RecordWrite(
            ...     space="my-space",
            ...     external_id="rec-1",
            ...     sources=[
            ...         RecordSource(
            ...             source=RecordSourceReference(
            ...                 space="my-space", external_id="my-container"
            ...             ),
            ...             properties={"temperature": 22.5, "location": "north"},
            ...         )
            ...     ],
            ... )
    """

    def __init__(self, space: str, external_id: str, sources: list[RecordSource]) -> None:
        self.space = space
        self.external_id = external_id
        self.sources = sources

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            sources=[RecordSource._load(s) for s in resource.get("sources", [])],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
            "sources": [s.dump(camel_case=camel_case) for s in self.sources],
        }

    def as_write(self) -> RecordWrite:
        return self


class RecordWriteList(CogniteResourceList[RecordWrite]):
    """A list of :class:`RecordWrite` objects."""

    _RESOURCE = RecordWrite


# ─── Read DTOs ─────────────────────────────────────────────────────────────────


class Record(WriteableCogniteResource[RecordWrite]):
    """A record returned from the stream records API.

    This is the read version of :class:`RecordWrite`.

    Args:
        space (str): Space the record belongs to.
        external_id (str): External ID of the record.
        created_time (int): Creation time in milliseconds since epoch.
        last_updated_time (int): Last updated time in milliseconds since epoch.
        properties (dict[str, dict[str, dict[str, Any]]] | None): Property values keyed by
            ``{space: {container_external_id: {property_id: value}}}``.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        properties: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            properties=resource.get("properties"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        d: dict[str, Any] = {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
            "createdTime" if camel_case else "created_time": self.created_time,
            "lastUpdatedTime" if camel_case else "last_updated_time": self.last_updated_time,
        }
        if self.properties is not None:
            d["properties"] = self.properties
        return d

    def as_write(self) -> RecordWrite:
        """Convert to :class:`RecordWrite` by grouping read properties back into sources."""
        sources = [
            RecordSource(
                source=RecordSourceReference(space=space, external_id=container),
                properties=dict(props),
            )
            for space, containers in (self.properties or {}).items()
            for container, props in containers.items()
        ]
        return RecordWrite(space=self.space, external_id=self.external_id, sources=sources)


class RecordList(WriteableCogniteResourceList[RecordWrite, Record], ExternalIDTransformerMixin):
    """A list of :class:`Record` objects."""

    _RESOURCE = Record

    def as_write(self) -> RecordWriteList:
        return RecordWriteList([r.as_write() for r in self])


# ─── Sync DTOs ─────────────────────────────────────────────────────────────────


class SyncRecord(Record):
    """A record returned by the sync endpoint, annotated with a change status.

    For ``status="deleted"`` tombstones, :attr:`properties` is ``None``.

    Args:
        space (str): Space the record belongs to.
        external_id (str): External ID of the record.
        created_time (int): Creation time in milliseconds since epoch.
        last_updated_time (int): Last updated time in milliseconds since epoch.
        status (Literal["created", "updated", "deleted"]): Change status.
        properties (dict[str, dict[str, dict[str, Any]]] | None): Property values (absent for deleted tombstones).
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        status: Literal["created", "updated", "deleted"],
        properties: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> None:
        super().__init__(
            space=space,
            external_id=external_id,
            created_time=created_time,
            last_updated_time=last_updated_time,
            properties=properties,
        )
        self.status = status

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            status=resource["status"],
            properties=resource.get("properties"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        d = super().dump(camel_case=camel_case)
        d["status"] = self.status
        return d


class SyncRecordList(CogniteResourceList[SyncRecord]):
    """A list of :class:`SyncRecord` objects."""

    _RESOURCE = SyncRecord


# ─── Request helpers ───────────────────────────────────────────────────────────


class TimeRange(CogniteResource):
    """Inclusive/exclusive time range for ``last_updated_time`` filters.

    At least one lower bound (``gte`` or ``gt``) is **required** for immutable streams.

    Args:
        gte (str | int | None): Lower bound, inclusive (ISO-8601 string or ms since epoch).
        gt (str | int | None): Lower bound, exclusive.
        lte (str | int | None): Upper bound, inclusive.
        lt (str | int | None): Upper bound, exclusive.

    Examples:

        Create a time range for the last 24 hours:

            >>> import time
            >>> from cognite.client.data_classes.data_modeling.records import TimeRange
            >>> now_ms = int(time.time() * 1000)
            >>> tr = TimeRange(gte=now_ms - 86_400_000)
    """

    def __init__(
        self,
        gte: str | int | None = None,
        gt: str | int | None = None,
        lte: str | int | None = None,
        lt: str | int | None = None,
    ) -> None:
        self.gte = gte
        self.gt = gt
        self.lte = lte
        self.lt = lt

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            gte=resource.get("gte"),
            gt=resource.get("gt"),
            lte=resource.get("lte"),
            lt=resource.get("lt"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            k: v for k, v in {"gte": self.gte, "gt": self.gt, "lte": self.lte, "lt": self.lt}.items() if v is not None
        }


class RecordSourceSelector(CogniteResource):
    """Selects which container properties to include in filter and sync responses.

    Args:
        source (RecordSourceReference): The container to select properties from.
        properties (list[str]): Property IDs to return. Use ``["*"]`` to return all.
    """

    def __init__(self, source: RecordSourceReference, properties: list[str]) -> None:
        self.source = source
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            source=RecordSourceReference._load(resource["source"]),
            properties=resource["properties"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "source": self.source.dump(camel_case=camel_case),
            "properties": self.properties,
        }


class RecordSortSpec(CogniteResource):
    """Sort specification for record list requests.

    Args:
        property (list[str]): Property path. Use 3 segments for container properties
            (``[space, container_external_id, property_id]``), or a single segment for
            top-level fields (``["space"]``, ``["externalId"]``, etc.).
        direction (Literal["ascending", "descending"]): Sort direction (default ``"ascending"``).
    """

    def __init__(self, property: list[str], direction: Literal["ascending", "descending"] = "ascending") -> None:
        self.property = property
        self.direction = direction

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(property=resource["property"], direction=resource.get("direction", "ascending"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"property": self.property, "direction": self.direction}


class TargetUnitProperty(CogniteResource):
    """Per-property unit conversion specification for record queries.

    Args:
        property (str): Property ID to convert.
        external_id (str): External ID of the target unit.
    """

    def __init__(self, property: str, external_id: str) -> None:
        self.property = property
        self.external_id = external_id

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(property=resource["property"], external_id=resource["unit"]["externalId"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"property": self.property, "unit": {"externalId": self.external_id}}


class TargetUnits(CogniteResource):
    """Unit conversion specification for record queries.

    Provide either ``unit_system_name`` for a global conversion or ``properties``
    for per-property control — not both.

    Args:
        unit_system_name (str | None): Convert all convertible properties to a named unit system.
        properties (list[TargetUnitProperty] | None): Per-property unit conversion specifications.
    """

    def __init__(
        self,
        unit_system_name: str | None = None,
        properties: list[TargetUnitProperty] | None = None,
    ) -> None:
        self.unit_system_name = unit_system_name
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            unit_system_name=resource.get("unitSystemName"),
            properties=(
                [TargetUnitProperty._load(p) for p in resource["properties"]] if "properties" in resource else None
            ),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if self.unit_system_name is not None:
            return {"unitSystemName" if camel_case else "unit_system_name": self.unit_system_name}
        if self.properties is not None:
            return {"properties": [p.dump(camel_case=camel_case) for p in self.properties]}
        return {}


# ─── Aggregate metric types ────────────────────────────────────────────────────


class AvgAggregate(CogniteResource):
    """Average metric aggregate.

    Args:
        property (list[str]): 3-segment property path ``[space, container_external_id, property_id]``.
    """

    def __init__(self, property: list[str]) -> None:
        self.property = property

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(property=resource["avg"]["property"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"avg": {"property": self.property}}


class CountAggregate(CogniteResource):
    """Count of non-null values metric aggregate.

    Args:
        property (list[str]): 3-segment property path ``[space, container_external_id, property_id]``.
    """

    def __init__(self, property: list[str]) -> None:
        self.property = property

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(property=resource["count"]["property"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"count": {"property": self.property}}


class MinAggregate(CogniteResource):
    """Minimum value metric aggregate.

    Args:
        property (list[str]): 3-segment property path ``[space, container_external_id, property_id]``.
    """

    def __init__(self, property: list[str]) -> None:
        self.property = property

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(property=resource["min"]["property"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"min": {"property": self.property}}


class MaxAggregate(CogniteResource):
    """Maximum value metric aggregate.

    Args:
        property (list[str]): 3-segment property path ``[space, container_external_id, property_id]``.
    """

    def __init__(self, property: list[str]) -> None:
        self.property = property

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(property=resource["max"]["property"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"max": {"property": self.property}}


class SumAggregate(CogniteResource):
    """Sum metric aggregate.

    Args:
        property (list[str]): 3-segment property path ``[space, container_external_id, property_id]``.
    """

    def __init__(self, property: list[str]) -> None:
        self.property = property

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(property=resource["sum"]["property"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"sum": {"property": self.property}}


# ─── Aggregate bucket types ────────────────────────────────────────────────────


class NumberHistogramHardBounds(CogniteResource):
    """Hard bounds that limit the range of bucket keys in a histogram.

    Args:
        min (float | None): Minimum bucket key (inclusive).
        max (float | None): Maximum bucket key (inclusive).
    """

    def __init__(self, min: float | None = None, max: float | None = None) -> None:
        self.min = min
        self.max = max

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(min=resource.get("min"), max=resource.get("max"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {k: v for k, v in {"min": self.min, "max": self.max}.items() if v is not None}


class UniqueValuesAggregate(CogniteResource):
    """Bucket aggregate grouping records by unique property values.

    Args:
        property (list[str]): Property path (1–3 segments).
        aggregates (dict[str, AggregateSpec] | None): Optional nested aggregates computed per bucket.
    """

    def __init__(self, property: list[str], aggregates: dict[str, AggregateSpec] | None = None) -> None:
        self.property = property
        self.aggregates = aggregates

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        spec = resource["uniqueValues"]
        agg_data = spec.get("aggregates")
        return cls(
            property=spec["property"],
            aggregates={k: _load_aggregate_spec(v) for k, v in agg_data.items()} if agg_data else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        spec: dict[str, Any] = {"property": self.property}
        if self.aggregates is not None:
            spec["aggregates"] = {k: v.dump(camel_case=camel_case) for k, v in self.aggregates.items()}
        return {"uniqueValues": spec}


class TimeHistogramAggregate(CogniteResource):
    """Bucket aggregate grouping records into time-based intervals.

    Provide either ``calendar_interval`` or ``fixed_interval``, not both.

    Args:
        property (list[str]): 3-segment path to a timestamp-type property.
        calendar_interval (Literal["1s", "1m", "1h", "1d", "1w", "1M", "1q", "1y"] | None): Calendar-aligned bucket width.
        fixed_interval (str | None): Fixed bucket width in duration format (e.g. ``"12h"``).
        hard_bounds (NumberHistogramHardBounds | None): Limits the range of emitted bucket keys.
        aggregates (dict[str, AggregateSpec] | None): Optional nested aggregates computed per bucket.
    """

    def __init__(
        self,
        property: list[str],
        calendar_interval: Literal["1s", "1m", "1h", "1d", "1w", "1M", "1q", "1y"] | None = None,
        fixed_interval: str | None = None,
        hard_bounds: NumberHistogramHardBounds | None = None,
        aggregates: dict[str, AggregateSpec] | None = None,
    ) -> None:
        self.property = property
        self.calendar_interval = calendar_interval
        self.fixed_interval = fixed_interval
        self.hard_bounds = hard_bounds
        self.aggregates = aggregates

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        spec = resource["timeHistogram"]
        agg_data = spec.get("aggregates")
        hard_bounds_data = spec.get("hardBounds")
        return cls(
            property=spec["property"],
            calendar_interval=spec.get("calendarInterval"),
            fixed_interval=spec.get("fixedInterval"),
            hard_bounds=NumberHistogramHardBounds._load(hard_bounds_data) if hard_bounds_data else None,
            aggregates={k: _load_aggregate_spec(v) for k, v in agg_data.items()} if agg_data else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        spec: dict[str, Any] = {"property": self.property}
        if self.calendar_interval is not None:
            spec["calendarInterval" if camel_case else "calendar_interval"] = self.calendar_interval
        if self.fixed_interval is not None:
            spec["fixedInterval" if camel_case else "fixed_interval"] = self.fixed_interval
        if self.hard_bounds is not None:
            spec["hardBounds" if camel_case else "hard_bounds"] = self.hard_bounds.dump(camel_case=camel_case)
        if self.aggregates is not None:
            spec["aggregates"] = {k: v.dump(camel_case=camel_case) for k, v in self.aggregates.items()}
        return {"timeHistogram" if camel_case else "time_histogram": spec}


class NumberHistogramAggregate(CogniteResource):
    """Bucket aggregate grouping records into fixed-width numeric intervals.

    Args:
        property (list[str]): 3-segment path to a numeric property.
        interval (float): Width of each bucket.
        hard_bounds (NumberHistogramHardBounds | None): Limits the range of emitted bucket keys.
        aggregates (dict[str, AggregateSpec] | None): Optional nested aggregates computed per bucket.
    """

    def __init__(
        self,
        property: list[str],
        interval: float,
        hard_bounds: NumberHistogramHardBounds | None = None,
        aggregates: dict[str, AggregateSpec] | None = None,
    ) -> None:
        self.property = property
        self.interval = interval
        self.hard_bounds = hard_bounds
        self.aggregates = aggregates

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        spec = resource["numberHistogram"]
        agg_data = spec.get("aggregates")
        hard_bounds_data = spec.get("hardBounds")
        return cls(
            property=spec["property"],
            interval=spec["interval"],
            hard_bounds=NumberHistogramHardBounds._load(hard_bounds_data) if hard_bounds_data else None,
            aggregates={k: _load_aggregate_spec(v) for k, v in agg_data.items()} if agg_data else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        spec: dict[str, Any] = {"property": self.property, "interval": self.interval}
        if self.hard_bounds is not None:
            spec["hardBounds" if camel_case else "hard_bounds"] = self.hard_bounds.dump(camel_case=camel_case)
        if self.aggregates is not None:
            spec["aggregates"] = {k: v.dump(camel_case=camel_case) for k, v in self.aggregates.items()}
        return {"numberHistogram" if camel_case else "number_histogram": spec}


class FiltersAggregate(CogniteResource):
    """Bucket aggregate that creates one bucket per filter expression.

    Args:
        filters (list[Filter]): One bucket per filter (1–10 filters; max 30 filter
            buckets total across all :class:`FiltersAggregate` instances in a request).
        aggregates (dict[str, AggregateSpec] | None): Optional nested aggregates computed per bucket.
    """

    def __init__(self, filters: list[Filter], aggregates: dict[str, AggregateSpec] | None = None) -> None:
        self.filters = filters
        self.aggregates = aggregates

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        spec = resource["filters"]
        agg_data = spec.get("aggregates")
        return cls(
            filters=[Filter.load(f) for f in spec.get("filters", [])],
            aggregates={k: _load_aggregate_spec(v) for k, v in agg_data.items()} if agg_data else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        spec: dict[str, Any] = {"filters": [f.dump() for f in self.filters]}
        if self.aggregates is not None:
            spec["aggregates"] = {k: v.dump(camel_case=camel_case) for k, v in self.aggregates.items()}
        return {"filters": spec}


AggregateSpec: TypeAlias = (
    AvgAggregate
    | CountAggregate
    | MinAggregate
    | MaxAggregate
    | SumAggregate
    | UniqueValuesAggregate
    | TimeHistogramAggregate
    | NumberHistogramAggregate
    | FiltersAggregate
)
AggregateResult: TypeAlias = dict[str, Any]

_AGGREGATE_KEY_MAP: dict[str, type[AggregateSpec]] = {
    "avg": AvgAggregate,
    "count": CountAggregate,
    "min": MinAggregate,
    "max": MaxAggregate,
    "sum": SumAggregate,
    "uniqueValues": UniqueValuesAggregate,
    "timeHistogram": TimeHistogramAggregate,
    "numberHistogram": NumberHistogramAggregate,
    "filters": FiltersAggregate,
}


def _load_aggregate_spec(data: dict[str, Any]) -> AggregateSpec:
    key = next(iter(data))
    cls = _AGGREGATE_KEY_MAP.get(key)
    if cls is None:
        raise ValueError(f"Unknown aggregate type key: {key!r}")
    return cls._load(data)
