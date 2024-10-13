from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from enum import auto
from typing import TYPE_CHECKING, Any, TypeAlias

from typing_extensions import Self

from cognite.client.data_classes import Datapoints
from cognite.client.data_classes._base import (
    CogniteListUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    EnumProperty,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.filters import _BASIC_FILTERS as _FILTERS_SUPPORTED
from cognite.client.data_classes.filters import Filter, _validate_filter
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import exactly_one_is_not_none

if TYPE_CHECKING:
    from cognite.client import CogniteClient

ExternalId: TypeAlias = str


class DatapointSubscriptionCore(WriteableCogniteResource["DataPointSubscriptionWrite"], ABC):
    def __init__(
        self,
        external_id: ExternalId,
        partition_count: int,
        filter: Filter | None,
        name: str | None,
        description: str | None,
        data_set_id: int | None,
    ) -> None:
        self.external_id = external_id
        self.partition_count = partition_count
        self.filter = filter
        self.name = name
        self.description = description
        self.data_set_id = data_set_id

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
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
        data_set_id (int | None): The id of the dataset this subscription belongs to.
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
        data_set_id: int | None = None,
    ) -> None:
        super().__init__(external_id, partition_count, filter, name, description, data_set_id)
        self.time_series_count = time_series_count
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            partition_count=resource["partitionCount"],
            filter=Filter.load(resource["filter"]) if "filter" in resource else None,
            name=resource.get("name"),
            description=resource.get("description"),
            data_set_id=resource.get("dataSetId"),
            time_series_count=resource["timeSeriesCount"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
        )

    def as_write(self) -> DataPointSubscriptionWrite:
        """Returns this DatapointSubscription as a DataPointSubscriptionWrite"""
        return DataPointSubscriptionWrite(
            external_id=self.external_id,
            partition_count=self.partition_count,
            filter=self.filter,
            name=self.name,
            description=self.description,
            data_set_id=self.data_set_id,
        )


class DataPointSubscriptionWrite(DatapointSubscriptionCore):
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
        data_set_id (int | None): The id of the dataset this subscription belongs to.
    """

    def __init__(
        self,
        external_id: str,
        partition_count: int,
        time_series_ids: list[ExternalId] | None = None,
        filter: Filter | None = None,
        name: str | None = None,
        description: str | None = None,
        data_set_id: int | None = None,
    ) -> None:
        if not exactly_one_is_not_none(time_series_ids, filter):
            raise ValueError("Exactly one of time_series_ids and filter must be given")
        _validate_filter(filter, _FILTERS_SUPPORTED, "DataPointSubscriptions")
        super().__init__(external_id, partition_count, filter, name, description, data_set_id)
        self.time_series_ids = time_series_ids

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        filter = Filter.load(resource["filter"]) if "filter" in resource else None
        return cls(
            external_id=resource["externalId"],
            partition_count=resource["partitionCount"],
            time_series_ids=resource.get("timeSeriesIds"),
            filter=filter,
            name=resource.get("name"),
            description=resource.get("description"),
            data_set_id=resource.get("dataSetId"),
        )

    def as_write(self) -> DataPointSubscriptionWrite:
        """Returns this DatapointSubscription instance"""
        return self


# TODO: Remove this in next major release
DataPointSubscriptionCreate = DataPointSubscriptionWrite


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
            return self._set(value.dump())

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
    def data_set_id(self) -> _PrimitiveDataPointSubscriptionUpdate:
        return DataPointSubscriptionUpdate._PrimitiveDataPointSubscriptionUpdate(self, "dataSetId")

    @property
    def time_series_ids(self) -> _ListDataPointSubscriptionUpdate:
        return DataPointSubscriptionUpdate._ListDataPointSubscriptionUpdate(self, "timeSeriesIds")

    @property
    def filter(self) -> _FilterDataPointSubscriptionUpdate:
        return DataPointSubscriptionUpdate._FilterDataPointSubscriptionUpdate(self, "filter")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name"),
            PropertySpec("time_series_ids", is_list=True),
            PropertySpec("filter", is_nullable=False),
            PropertySpec("data_set_id"),
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

    def __repr__(self) -> str:
        return f"TimeSeriesID(id={self.id}, external_id={self.external_id})"

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> TimeSeriesID:
        return cls(id=resource["id"], external_id=resource.get("externalId"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        resource: dict[str, Any] = {"id": self.id}
        if self.external_id is not None:
            resource["externalId" if camel_case else "external_id"] = self.external_id
        return resource


@dataclass
class DataDeletion:
    inclusive_begin: int
    exclusive_end: int | None

    @classmethod
    def load(cls, data: dict[str, Any]) -> DataDeletion:
        return cls(inclusive_begin=data["inclusiveBegin"], exclusive_end=data.get("exclusiveEnd"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
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
    def load(
        cls, data: dict[str, Any], include_status: bool = False, ignore_bad_datapoints: bool = True
    ) -> DatapointsUpdate:
        identifier = TimeSeriesID._load(data["timeSeries"])
        is_string = data["timeSeries"]["isString"]
        for dp in data["upserts"]:
            if include_status:
                dp.setdefault("status", {"code": 0, "symbol": "Good"})  # Not returned from API by default
            if not ignore_bad_datapoints:
                # Bad data can have value missing (we translate to None):
                dp.setdefault("value", None)
                if not is_string:
                    dp["value"] = _json.convert_to_float(dp["value"])
        return cls(
            time_series=identifier,
            upserts=Datapoints._load({**identifier.dump(), "isString": is_string, "datapoints": data["upserts"]}),
            deletes=[DataDeletion.load(value) for value in data["deletes"]],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
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
    def load(cls, data: dict[str, Any]) -> SubscriptionTimeSeriesUpdate:
        return cls(
            added=[TimeSeriesID._load(ts_id) for ts_id in data.get("added", [])],
            removed=[TimeSeriesID._load(ts_id) for ts_id in data.get("removed", [])],
        )

    def dump(self, camel_case: bool = True) -> dict[str, list[dict[str, Any]]]:
        return {
            "added": [ts_id.dump() for ts_id in self.added],
            "removed": [ts_id.dump() for ts_id in self.removed],
        }


@dataclass
class DatapointSubscriptionPartition:
    index: int
    cursor: str | None = None

    @classmethod
    def load(cls, data: dict[str, Any]) -> DatapointSubscriptionPartition:
        return cls(index=data["index"], cursor=data.get("nextCursor") or data.get("cursor"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
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
    def load(
        cls, resource: dict, include_status: bool = False, ignore_bad_datapoints: bool = True
    ) -> _DatapointSubscriptionBatchWithPartitions:
        return cls(
            updates=[
                DatapointsUpdate.load(upd, include_status=include_status, ignore_bad_datapoints=ignore_bad_datapoints)
                for upd in resource["updates"]
            ],
            partitions=[DatapointSubscriptionPartition.load(p) for p in resource["partitions"]],
            has_next=resource["hasNext"],
            subscription_changes=SubscriptionTimeSeriesUpdate.load(resource.get("subscriptionChanges", [])),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        resource: dict[str, Any] = {
            "updates": [u.dump(camel_case) for u in self.updates],
            "partitions": [p.dump(camel_case) for p in self.partitions],
            ("hasNext" if camel_case else "has_next"): self.has_next,
        }
        if self.subscription_changes is not None:
            resource[("subscriptionChanges" if camel_case else "subscription_changes")] = (
                self.subscription_changes.dump(camel_case)
            )
        return resource


class DatapointSubscriptionWriteList(CogniteResourceList[DataPointSubscriptionWrite], ExternalIDTransformerMixin):
    _RESOURCE = DataPointSubscriptionWrite


class DatapointSubscriptionList(
    WriteableCogniteResourceList[DataPointSubscriptionWrite, DatapointSubscription], ExternalIDTransformerMixin
):
    _RESOURCE = DatapointSubscription

    def as_write(self) -> DatapointSubscriptionWriteList:
        """Returns this DatapointSubscriptionList as a DatapointSubscriptionWriteList"""
        return DatapointSubscriptionWriteList(
            [x.as_write() for x in self.data], cognite_client=self._get_cognite_client()
        )


class TimeSeriesIDList(CogniteResourceList[TimeSeriesID], IdTransformerMixin):
    _RESOURCE = TimeSeriesID


class DatapointSubscriptionProperty(EnumProperty):
    description = auto()
    external_id = auto()
    name = auto()  # type: ignore [assignment]
    unit = auto()
    unit_external_id = auto()
    unit_quantity = auto()
    asset_id = auto()
    asset_root_id = auto()
    created_time = auto()
    data_set_id = auto()
    id = auto()
    last_updated_time = auto()
    is_step = auto()
    is_string = auto()

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return ["metadata", key]

    metadata = metadata_key  # TODO: Remove in major version 8


# TODO: Remove in major version 8:
DatapointSubscriptionFilterProperties = DatapointSubscriptionProperty
