from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscription,
    DatapointSubscriptionBatch,
    DatapointSubscriptionList,
    DatapointSubscriptionPartition,
    DataPointSubscriptionUpdate,
    DataPointSubscriptionWrite,
    TimeSeriesID,
    TimeSeriesIDList,
    _DatapointSubscriptionBatchWithPartitions,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class DatapointsSubscriptionAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/subscriptions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"
        self._warning = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="alpha", feature_name="DataPoint Subscriptions"
        )

    def create(self, subscription: DataPointSubscriptionWrite) -> DatapointSubscription:
        """`Create a subscription <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/postSubscriptions>`_

        Create a subscription that can be used to listen for changes in data points for a set of time series.

        Args:
            subscription (DataPointSubscriptionWrite): Subscription to create.

        Returns:
            DatapointSubscription: Created subscription

        Examples:

            Create a subscription with explicit time series IDs:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataPointSubscriptionWrite
                >>> c = CogniteClient()
                >>> sub = DataPointSubscriptionWrite("mySubscription", partition_count=1, time_series_ids=["myFistTimeSeries", "mySecondTimeSeries"], name="My subscription")
                >>> created = c.time_series.subscriptions.create(sub)

            Create a filter defined subscription for all numeric time series:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataPointSubscriptionWrite
                >>> from cognite.client.data_classes import filters as flt
                >>> from cognite.client.data_classes.datapoints_subscriptions import DatapointSubscriptionFilterProperties
                >>> c = CogniteClient()
                >>> prop = DatapointSubscriptionFilterProperties.is_string
                >>> numeric_timeseries = flt.Equals(prop, False)
                >>> sub = DataPointSubscriptionWrite(
                ...     "mySubscription",
                ...     partition_count=1,
                ...     filter=numeric_timeseries,
                ...     name="My subscription for Numeric time series")
                >>> created = c.time_series.subscriptions.create(sub)
        """
        self._warning.warn()

        return self._create_multiple(
            subscription,
            list_cls=DatapointSubscriptionList,
            resource_cls=DatapointSubscription,
            input_resource_cls=DataPointSubscriptionWrite,
        )

    def delete(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete subscription(s). This operation cannot be undone. <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/deleteSubscriptions>`_

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external IDs of subscriptions to delete.
            ignore_unknown_ids (bool): Whether to ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete a subscription by external ID:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.time_series.subscriptions.delete("my_subscription")
        """
        self._warning.warn()

        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
            wrap_ids=True,
        )

    def retrieve(self, external_id: str, ignore_unknown_ids: bool = False) -> DatapointSubscription | None:
        """`Retrieve one subscription by external ID. <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/getSubscriptionsByIds>`_

        Args:
            external_id (str): External ID of the subscription to retrieve.
            ignore_unknown_ids (bool): Whether to ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            DatapointSubscription | None: The requested subscription.

        Examples:

            Retrieve a subscription by external ID:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.subscriptions.retrieve("my_subscription")
        """
        self._warning.warn()

        result = self._retrieve_multiple(
            list_cls=DatapointSubscriptionList,
            resource_cls=DatapointSubscription,
            identifiers=IdentifierSequence.load(external_ids=[external_id]),
            ignore_unknown_ids=ignore_unknown_ids,
        )
        if result:
            return result[0]
        else:
            return None

    def list_member_time_series(self, external_id: str, limit: int | None = DEFAULT_LIMIT_READ) -> TimeSeriesIDList:
        """`List time series in a subscription <https://api-docs.cognite.com/20230101-beta/tag/Data-point-subscriptions/operation/listSubscriptionMembers>`_

        Retrieve a list of time series (IDs) that the subscription is currently retrieving updates from

        Args:
            external_id (str): External ID of the subscription to retrieve members of.
            limit (int | None): Maximum number of time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            TimeSeriesIDList: List of time series in the subscription.

        Examples:

            List time series in a subscription:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataPointSubscriptionUpdate
                >>> c = CogniteClient()
                >>> members = c.time_series.subscriptions.list_member_time_series("my_subscription")
                >>> timeseries_external_ids = members.as_external_ids()
        """
        self._warning.warn()

        return self._list(
            method="GET",
            limit=limit,
            list_cls=TimeSeriesIDList,
            resource_cls=TimeSeriesID,
            resource_path="/timeseries/subscriptions/members",
            other_params={"externalId": external_id},
        )

    def update(self, update: DataPointSubscriptionUpdate) -> DatapointSubscription:
        """`Update a subscriptions <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/updateSubscriptions>`_

        Update a subscription. Note that Fields that are not included in the request are not changed.
        Furthermore, the subscription partition cannot be changed.

        Args:
            update (DataPointSubscriptionUpdate): The subscription update.

        Returns:
            DatapointSubscription: Updated subscription.

        Examples:

            Change the name of a preexisting subscription:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataPointSubscriptionUpdate
                >>> c = CogniteClient()
                >>> update = DataPointSubscriptionUpdate("my_subscription").name.set("My New Name")
                >>> updated = c.time_series.subscriptions.update(update)


            Add a time series to a preexisting subscription:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataPointSubscriptionUpdate
                >>> c = CogniteClient()
                >>> update = DataPointSubscriptionUpdate("my_subscription").time_series_ids.add(["MyNewTimeSeriesExternalId"])
                >>> updated = c.time_series.subscriptions.update(update)
        """
        self._warning.warn()

        return self._update_multiple(
            items=update,
            list_cls=DatapointSubscriptionList,
            resource_cls=DatapointSubscription,
            update_cls=DataPointSubscriptionUpdate,
        )

    def iterate_data(
        self,
        external_id: str,
        start: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        partition: int = 0,
        poll_timeout: int = 5,
        cursor: str | None = None,
    ) -> Iterator[DatapointSubscriptionBatch]:
        """`Iterate over data from a given subscription. <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/listSubscriptionData>`_

        Data can be ingested datapoints and time ranges where data is deleted. This endpoint will also return changes to
        the subscription itself, that is, if time series are added or removed from the subscription.

        Warning:
            This endpoint will store updates from when the subscription was created, but updates
            older than 7 days may be discarded.

        Args:
            external_id (str): The external ID of the subscription.
            start (str | None): When to start the iteration. If set to None, the iteration will start from the beginning. The format is "N[timeunit]-ago", where timeunit is w,d,h,m (week, day, hour, minute). For example, "12h-ago" will start the iteration from 12 hours ago. You can also set it to "now" to jump straight to the end. Defaults to None.
            limit (int): Approximate number of results to return across all partitions.
            partition (int): The partition to iterate over. Defaults to 0.
            poll_timeout (int): How many seconds to wait for new data, until an empty response is sent. Defaults to 5.
            cursor (str | None): Optional cursor to start iterating from.

        Yields:
            DatapointSubscriptionBatch: Changes to the subscription and data in the subscribed time series.

        Examples:

            Iterate over changes to subscription timeseries since the beginning until there is no more data:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for batch in c.time_series.subscriptions.iterate_data("my_subscription"):
                ...     print(f"Added {len(batch.subscription_changes.added)} timeseries")
                ...     print(f"Removed {len(batch.subscription_changes.removed)} timeseries")
                ...     print(f"Changed timeseries data in {len(batch.updates)} updates")
                ...     if not batch.has_next:
                ...         break

            Iterate continuously over all changes to the subscription newer than 3 days:

                >>> import time
                >>> for batch in c.time_series.subscriptions.iterate_data("my_subscription", "3d-ago"):
                ...     print(f"Added {len(batch.subscription_changes.added)} timeseries")
                ...     print(f"Removed {len(batch.subscription_changes.removed)} timeseries")
                ...     print(f"Changed timeseries data in {len(batch.updates)} updates")
        """
        self._warning.warn()

        current_partitions = [DatapointSubscriptionPartition.create((partition, cursor))]
        while True:
            body = {
                "externalId": external_id,
                "partitions": [p.dump(camel_case=True) for p in current_partitions],
                "limit": limit,
                "pollTimeoutSeconds": poll_timeout,
            }
            if start is not None:
                body["initializeCursors"] = start
            start = None

            res = self._post(url_path=self._RESOURCE_PATH + "/data/list", json=body)
            batch = _DatapointSubscriptionBatchWithPartitions.load(res.json())

            cursor = batch.partitions[0].cursor
            assert cursor is not None

            yield DatapointSubscriptionBatch(batch.updates, batch.subscription_changes, batch.has_next, cursor)

            current_partitions = batch.partitions

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> DatapointSubscriptionList:
        """`List data point subscriptions <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/listSubscriptions>`_

        Args:
            limit (int | None): Maximum number of subscriptions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
        Returns:
            DatapointSubscriptionList: List of requested datapoint subscriptions

        Examples:

            List 5 subscriptions:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> subscriptions = c.time_series.subscriptions.list(limit=5)

        """
        self._warning.warn()

        return self._list(
            method="GET", limit=limit, list_cls=DatapointSubscriptionList, resource_cls=DatapointSubscription
        )
