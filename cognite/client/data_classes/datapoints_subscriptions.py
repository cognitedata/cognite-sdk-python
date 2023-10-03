from __future__ import annotations

import json
from dataclasses import dataclass
from enum import auto
from typing import TYPE_CHECKING, Any

from cognite.client.data_classes import Datapoints, filters
from cognite.client.data_classes._base import (
    CogniteListUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    EnumProperty,
    PropertySpec,
    T_CogniteResource,
)
from cognite.client.data_classes.filters import Filter, _validate_filter
from cognite.client.utils._auxiliary import exactly_one_is_not_none
from cognite.client.utils._text import convert_all_keys_to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient

ExternalId = str

_DATAPOINT_SUBSCRIPTION_SUPPORTED_FILTERS: frozenset[type[Filter]] = frozenset(
    {
        filters.And,
        filters.Or,
        filters.Not,
        filters.In,
        filters.Equals,
        filters.Exists,
        filters.Range,
        filters.Prefix,
        filters.ContainsAny,
        filters.ContainsAll,
    }
)


class DatapointSubscriptionCore(CogniteResource):
    def __init__(
        self,
        external_id: ExternalId,
        partition_count: int,
        filter: Filter | None = None,
        name: str | None = None,
        description: str | None = None,
        **_: Any,
    ) -> None:
        self.external_id = external_id
        self.partition_count = partition_count
        self.filter = filter
        self.name = name
        self.description = description

    @classmethod
    def _load(
        cls: type[T_CogniteResource], resource: dict | str, cognite_client: CogniteClient | None = None
    ) -> T_CogniteResource:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        if "filter" in resource:
            resource["filter"] = Filter.load(resource["filter"])

        resource = convert_all_keys_to_snake_case(resource)
        return cls(**resource)

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        data = super().dump(camel_case)
        if "filter" in data:
            data["filter"] = data["filter"].dump()
        return data


class DatapointSubscription(DatapointSubscriptionCore):
    """A data point subscription is a way to listen to changes to time series data points, in ingestion order.
        This is the read version of a subscription, used when reading subscriptions from CDF.

    Args:
        external_id (ExternalId): Externally provided ID for the subscription. Must be unique.
        partition_count (int): The maximum effective parallelism of this subscription (the number of clients that can read from it concurrently) will be limited to this number, but a higher partition count will cause a higher time overhead.
        created_time (int): Time when the subscription was created in CDF in milliseconds since Jan 1, 1970.
        last_updated_time (int): Time when the subscription was last updated in CDF in milliseconds since Jan 1, 1970.
        time_series_count (int): The number of time series in the subscription.
        filter (Filter | None): If present, the subscription is defined by this filter.
        name (str | None): No description.
        description (str | None): A summary explanation for the subscription.
        **_ (Any): No description.
    """

    def __init__(
        self,
        external_id: ExternalId,
        partition_count: int,
        created_time: int,
        last_updated_time: int,
        time_series_count: int,
        filter: Filter | None = None,
        name: str | None = None,
        description: str | None = None,
        **_: Any,
    ) -> None:
        super().__init__(external_id, partition_count, filter, name, description)
        self.time_series_count = time_series_count
        self.created_time = created_time
        self.last_updated_time = last_updated_time


class DataPointSubscriptionCreate(DatapointSubscriptionCore):
    """A data point subscription is a way to listen to changes to time series data points, in ingestion order.
        This is the write version of a subscription, used to create new subscriptions.

    A subscription can either be defined directly by a list of time series ids or indirectly by a filter.

    Args:
        external_id (str): Externally provided ID for the subscription. Must be unique.
        partition_count (int): The maximum effective parallelism of this subscription (the number of clients that can read from it concurrently) will be limited to this number, but a higher partition count will cause a higher time overhead. The partition count must be between 1 and 100. CAVEAT: This cannot change after the subscription has been created.
        time_series_ids (list[ExternalId] | None): List of (external) ids of time series that this subscription will listen to. Not compatible with filter.
        filter (Filter | None): A filter DSL (Domain Specific Language) to define advanced filter queries. Not compatible with time_series_ids.
        name (str | None): No description.
        description (str | None): A summary explanation for the subscription.
    """

    def __init__(
        self,
        external_id: str,
        partition_count: int,
        time_series_ids: list[ExternalId] | None = None,
        filter: Filter | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        if not exactly_one_is_not_none(time_series_ids, filter):
            raise ValueError("Exactly one of time_series_ids and filter must be given")
        _validate_filter(filter, _DATAPOINT_SUBSCRIPTION_SUPPORTED_FILTERS, "DataPointSubscriptions")
        super().__init__(external_id, partition_count, filter, name, description)
        self.time_series_ids = time_series_ids


class DataPointSubscriptionUpdate(CogniteUpdate):
    """Changes applied to datapoint subscription

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    def __init__(self, external_id: str) -> None:
        super().__init__(external_id=external_id)

    class _PrimitiveDataPointSubscriptionUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> DataPointSubscriptionUpdate:
            return self._set(value)

    class _FilterDataPointSubscriptionUpdate(CognitePrimitiveUpdate):
        def set(self, value: Filter) -> DataPointSubscriptionUpdate:
            return self._set(value.dump())  # type: ignore[arg-type]

    class _ListDataPointSubscriptionUpdate(CogniteListUpdate):
        def set(self, value: list) -> DataPointSubscriptionUpdate:
            return self._set(value)

        def add(self, value: list) -> DataPointSubscriptionUpdate:
            return self._add(value)

        def remove(self, value: list) -> DataPointSubscriptionUpdate:
            return self._remove(value)

    @property
    def name(self) -> _PrimitiveDataPointSubscriptionUpdate:
        return DataPointSubscriptionUpdate._PrimitiveDataPointSubscriptionUpdate(self, "name")

    @property
    def time_series_ids(self) -> _ListDataPointSubscriptionUpdate:
        return DataPointSubscriptionUpdate._ListDataPointSubscriptionUpdate(self, "timeSeriesIds")

    @property
    def filter(self) -> _FilterDataPointSubscriptionUpdate:
        return DataPointSubscriptionUpdate._FilterDataPointSubscriptionUpdate(self, "filter")

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("name"),
            PropertySpec("time_series_ids", is_container=True),
            PropertySpec("filter", is_nullable=False),
        ]


class TimeSeriesID(CogniteResource):
    """
    A TimeSeries Identifier to uniquely identify a time series.

    Args:
        id (int): A server-generated ID for the object.
        external_id (ExternalId | None): The external ID provided by the client. Must be unique for the resource type.
    """

    def __init__(self, id: int, external_id: ExternalId | None = None) -> None:
        self.id = id
        self.external_id = external_id

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> TimeSeriesID:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(id=resource["id"], external_id=resource.get("externalId"))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        resource: dict[str, Any] = {"id": self.id}
        if self.external_id is not None:
            resource["externalId" if camel_case else "external_id"] = self.external_id
        return resource


@dataclass
class DataDeletion:
    inclusive_begin: int
    exclusive_end: int | None

    @classmethod
    def _load(cls, data: dict[str, Any]) -> DataDeletion:
        return cls(inclusive_begin=data["inclusiveBegin"], exclusive_end=data.get("exclusiveEnd"))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        resource: dict[str, Any] = {("inclusiveBegin" if camel_case else "inclusive_begin"): self.inclusive_begin}
        if self.exclusive_end is not None:
            resource["exclusiveEnd" if camel_case else "exclusive_end"] = self.exclusive_end
        return resource


@dataclass
class DatapointsUpdate:
    time_series: TimeSeriesID
    upserts: Datapoints
    deletes: list[DataDeletion]

    @classmethod
    def _load(cls, data: dict[str, Any]) -> DatapointsUpdate:
        datapoints: dict[str, Any] = {"upserts": Datapoints(), "deletes": []}
        if (values := data["upserts"]) and ("value" in values[0]):
            datapoints["upserts"] = Datapoints._load(
                {
                    "id": data["timeSeries"]["id"],
                    "externalId": data["timeSeries"].get("externalId"),
                    "isString": isinstance(values[0]["value"], str),
                    "datapoints": values,
                }
            )
        if values := data["deletes"]:
            datapoints["deletes"] = [DataDeletion._load(value) for value in values]
        return cls(
            time_series=TimeSeriesID._load(data["timeSeries"], None),
            **datapoints,
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        resource: dict[str, Any] = {("timeSeries" if camel_case else "time_series"): self.time_series.dump(camel_case)}
        if self.upserts is not None:
            resource["upserts"] = self.upserts.dump(camel_case)
        if self.deletes is not None:
            resource["deletes"] = [d.dump(camel_case) for d in self.deletes]
        return resource


@dataclass
class SubscriptionTimeSeriesUpdate:
    added: list[TimeSeriesID]
    removed: list[TimeSeriesID]

    @classmethod
    def _load(cls, data: dict[str, Any]) -> SubscriptionTimeSeriesUpdate:
        return cls(
            added=[TimeSeriesID._load(added) for added in data.get("added", [])],
            removed=[TimeSeriesID._load(added) for added in data.get("removed", [])],
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        resource: dict[str, Any] = {}
        resource["added"] = [id_.dump() for id_ in self.added]
        resource["removed"] = [id_.dump() for id_ in self.removed]
        return resource


@dataclass
class DatapointSubscriptionPartition:
    index: int
    cursor: str | None = None

    @classmethod
    def create(
        cls, data: tuple[int, str | None] | int | DatapointSubscriptionPartition
    ) -> DatapointSubscriptionPartition:
        if isinstance(data, DatapointSubscriptionPartition):
            return data
        if isinstance(data, tuple):
            return cls(*data)
        return cls(data)

    @classmethod
    def _load(cls, data: dict[str, Any]) -> DatapointSubscriptionPartition:
        return cls(index=data["index"], cursor=data.get("cursor") or data.get("nextCursor"))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: dict[str, Any] = {"index": self.index}
        if self.cursor is not None:
            output["cursor"] = self.cursor
        return output


@dataclass(frozen=True)
class DatapointSubscriptionBatch:
    updates: list[DatapointsUpdate]
    subscription_changes: SubscriptionTimeSeriesUpdate
    has_next: bool
    cursor: str


@dataclass(frozen=True)
class _DatapointSubscriptionBatchWithPartitions:
    """A batch of data from a subscription.

    Args:
        updates (list[DatapointsUpdate]): List of updates from the subscription, sorted by point in time they were applied to the time series. Every update contains a time series along with a set of changes to that time series.
        partitions (list[DatapointSubscriptionPartition]): Which partitions/cursors to use for the next request. Map from partition index to cursor.
        has_next (bool): Whether there is more data available at the time of the query. In rare cases, we may return true, even if there is no data available. If that is the case, just continue to query with the updated cursors, and it will eventually return false.
        subscription_changes (SubscriptionTimeSeriesUpdate): If present, this object represents changes to the subscription definition. The subscription will now start/stop listening to changes from the time series listed here.
    """

    updates: list[DatapointsUpdate]
    subscription_changes: SubscriptionTimeSeriesUpdate
    has_next: bool
    partitions: list[DatapointSubscriptionPartition]

    @classmethod
    def _load(cls, resource: dict | str) -> _DatapointSubscriptionBatchWithPartitions:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            updates=[DatapointsUpdate._load(u) for u in resource["updates"]],
            partitions=[DatapointSubscriptionPartition._load(p) for p in resource["partitions"]],
            has_next=resource["hasNext"],
            subscription_changes=SubscriptionTimeSeriesUpdate._load(resource.get("subscriptionChanges", [])),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        resource: dict[str, Any] = {
            "updates": [u.dump(camel_case) for u in self.updates],
            "partitions": [p.dump(camel_case) for p in self.partitions],
            ("hasNext" if camel_case else "has_next"): self.has_next,
        }
        if self.subscription_changes is not None:
            resource[
                ("subscriptionChanges" if camel_case else "subscription_changes")
            ] = self.subscription_changes.dump(camel_case)
        return resource


class DatapointSubscriptionList(CogniteResourceList[DatapointSubscription]):
    _RESOURCE = DatapointSubscription


def _metadata(key: str) -> list[str]:
    return ["metadata", key]


class DatapointSubscriptionFilterProperties(EnumProperty):
    description = auto()
    external_id = auto()
    metadata = _metadata
    name = auto()  # type: ignore [assignment]
    unit = auto()
    asset_id = auto()
    asset_root_id = auto()
    created_time = auto()
    data_set_id = auto()
    id = auto()
    last_updated_time = auto()
    is_step = auto()
    is_string = auto()
