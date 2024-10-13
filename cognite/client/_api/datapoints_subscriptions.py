from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Literal, cast, overload

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
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class DatapointsSubscriptionAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/subscriptions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._DELETE_LIMIT = 1

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[DatapointSubscription]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[DatapointSubscriptionList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[DatapointSubscription] | Iterator[DatapointSubscriptionList]:
        """Iterate over all datapoint subscriptions.

        Args:
            chunk_size (int | None): The number of datapoint subscriptions to fetch per request. Defaults to yielding one datapoint subscription at a time.
            limit (int | None): Maximum number of items to return. Defaults to return all datapoint subscriptions.

        Returns:
            Iterator[DatapointSubscription] | Iterator[DatapointSubscriptionList]: Yields datapoint subscriptions one by one if chunk is not specified, otherwise returns a list of datapoint subscriptions.
        """
        return self._list_generator(
            method="GET",
            limit=limit,
            chunk_size=chunk_size,
            list_cls=DatapointSubscriptionList,
            resource_cls=DatapointSubscription,
        )

    def __iter__(self) -> Iterator[DatapointSubscription]:
        """Iterate over all datapoint subscriptions."""
        return self()

    def create(self, subscription: DataPointSubscriptionWrite) -> DatapointSubscription:
        """`Create a subscription <https://api-docs.cognite.com/20230101/tag/Data-point-subscriptions/operation/postSubscriptions>`_

        Create a subscription that can be used to listen for changes in data points for a set of time series.

        Args:
            subscription (DataPointSubscriptionWrite): Subscription to create.

        Returns:
            DatapointSubscription: Created subscription

        Examples:

            Create a subscription with explicit time series IDs:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataPointSubscriptionWrite
                >>> client = CogniteClient()
                >>> sub = DataPointSubscriptionWrite(
                ...     external_id="my_subscription",
                ...     name="My subscription",
                ...     partition_count=1,
                ...     time_series_ids=["myFistTimeSeries", "mySecondTimeSeries"])
                >>> created = client.time_series.subscriptions.create(sub)

            Create a filter defined subscription for all numeric time series that are stepwise:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataPointSubscriptionWrite
                >>> from cognite.client.data_classes import filters as flt
                >>> from cognite.client.data_classes.datapoints_subscriptions import DatapointSubscriptionProperty
                >>> client = CogniteClient()
                >>> is_numeric_stepwise = flt.And(
                ...     flt.Equals(DatapointSubscriptionProperty.is_string, False),
                ...     flt.Equals(DatapointSubscriptionProperty.is_step, True))
                >>> sub = DataPointSubscriptionWrite(
                ...     external_id="my_subscription",
                ...     name="My subscription for numeric, stepwise time series",
                ...     partition_count=1,
                ...     filter=is_numeric_stepwise)
                >>> created = client.time_series.subscriptions.create(sub)
        """

        return self._create_multiple(
            subscription,
            list_cls=DatapointSubscriptionList,
            resource_cls=DatapointSubscription,
            input_resource_cls=DataPointSubscriptionWrite,
        )

    def delete(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete subscription(s). This operation cannot be undone. <https://api-docs.cognite.com/20230101/tag/Data-point-subscriptions/operation/deleteSubscriptions>`_

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external IDs of subscriptions to delete.
            ignore_unknown_ids (bool): Whether to ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete a subscription by external ID:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.time_series.subscriptions.delete("my_subscription")
        """

        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
            wrap_ids=True,
        )

    def retrieve(self, external_id: str) -> DatapointSubscription | None:
        """`Retrieve one subscription by external ID. <https://api-docs.cognite.com/20230101/tag/Data-point-subscriptions/operation/getSubscriptionsByIds>`_

        Args:
            external_id (str): External ID of the subscription to retrieve.

        Returns:
            DatapointSubscription | None: The requested subscription.

        Examples:

            Retrieve a subscription by external ID:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.subscriptions.retrieve("my_subscription")
        """

        result = self._retrieve_multiple(
            list_cls=DatapointSubscriptionList,
            resource_cls=DatapointSubscription,
            identifiers=IdentifierSequence.load(external_ids=[external_id]),
            ignore_unknown_ids=True,
        )
        if result:
            return result[0]
        else:
            return None

    def list_member_time_series(self, external_id: str, limit: int | None = DEFAULT_LIMIT_READ) -> TimeSeriesIDList:
        """`List time series in a subscription <https://api-docs.cognite.com/20230101/tag/Data-point-subscriptions/operation/listSubscriptionMembers>`_

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
                >>> client = CogniteClient()
                >>> members = client.time_series.subscriptions.list_member_time_series("my_subscription")
                >>> timeseries_external_ids = members.as_external_ids()
        """

        return self._list(
            method="GET",
            limit=limit,
            list_cls=TimeSeriesIDList,
            resource_cls=TimeSeriesID,
            resource_path="/timeseries/subscriptions/members",
            other_params={"externalId": external_id},
        )

    def update(
        self,
        update: DataPointSubscriptionUpdate | DataPointSubscriptionWrite,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> DatapointSubscription:
        """`Update a subscriptions <https://api-docs.cognite.com/20230101/tag/Data-point-subscriptions/operation/updateSubscriptions>`_

        Update a subscription. Note that Fields that are not included in the request are not changed.
        Furthermore, the subscription partition cannot be changed.

        Args:
            update (DataPointSubscriptionUpdate | DataPointSubscriptionWrite): The subscription update.
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (DataPointSubscriptionWrite). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing.

        Returns:
            DatapointSubscription: Updated subscription.

        Examples:

            Change the name of a preexisting subscription:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataPointSubscriptionUpdate
                >>> client = CogniteClient()
                >>> update = DataPointSubscriptionUpdate("my_subscription").name.set("My New Name")
                >>> updated = client.time_series.subscriptions.update(update)


            Add a time series to a preexisting subscription:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataPointSubscriptionUpdate
                >>> client = CogniteClient()
                >>> update = DataPointSubscriptionUpdate("my_subscription").time_series_ids.add(["MyNewTimeSeriesExternalId"])
                >>> updated = client.time_series.subscriptions.update(update)
        """

        return self._update_multiple(
            items=update,
            list_cls=DatapointSubscriptionList,
            resource_cls=DatapointSubscription,
            update_cls=DataPointSubscriptionUpdate,
            mode=mode,
        )

    def iterate_data(
        self,
        external_id: str,
        start: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        partition: int = 0,
        poll_timeout: int = 5,
        cursor: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> Iterator[DatapointSubscriptionBatch]:
        """`Iterate over data from a given subscription. <https://api-docs.cognite.com/20230101/tag/Data-point-subscriptions/operation/listSubscriptionData>`_

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
            include_status (bool): Also return the status code, an integer, for each datapoint in the response.
            ignore_bad_datapoints (bool): Do not return bad datapoints. Default: True.
            treat_uncertain_as_bad (bool): Treat datapoints with uncertain status codes as bad. If false, treat datapoints with uncertain status codes as good. Default: True.

        Yields:
            DatapointSubscriptionBatch: Changes to the subscription and data in the subscribed time series.

        Examples:

            Iterate over changes to subscription timeseries since the beginning until there is no more data:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for batch in client.time_series.subscriptions.iterate_data("my_subscription"):
                ...     # Changes to the subscription itself:
                ...     print(f"Added {len(batch.subscription_changes.added)} timeseries")
                ...     print(f"Removed {len(batch.subscription_changes.removed)} timeseries")
                ...     print(f"Changed timeseries data in {len(batch.updates)} updates")
                ...     # Changes to datapoints for time series in the subscription:
                ...     for update in batch.updates:
                ...         upserts.time_series  # The time series the update belongs to
                ...         upserts.upserts  # The upserted datapoints, if any
                ...         upserts.deletes  # Ranges of deleted periods, if any
                ...     if not batch.has_next:
                ...         break

            Iterate continuously over all changes to the subscription newer than 3 days:

                >>> for batch in client.time_series.subscriptions.iterate_data("my_subscription", "3d-ago"):
                ...     pass  # do something
        """
        current_partitions = [DatapointSubscriptionPartition(partition, cursor)]
        while True:
            body = {
                "externalId": external_id,
                "partitions": [p.dump(camel_case=True) for p in current_partitions],
                "limit": limit,
                "pollTimeoutSeconds": poll_timeout,
                "includeStatus": include_status,
                "ignoreBadDataPoints": ignore_bad_datapoints,
                "treatUncertainAsBad": treat_uncertain_as_bad,
            }
            if start is not None:
                body["initializeCursors"] = start
                start = None

            res = self._post(url_path=self._RESOURCE_PATH + "/data/list", json=body)
            batch = _DatapointSubscriptionBatchWithPartitions.load(
                res.json(), include_status=include_status, ignore_bad_datapoints=ignore_bad_datapoints
            )
            cursor = cast(str, batch.partitions[0].cursor)

            yield DatapointSubscriptionBatch(batch.updates, batch.subscription_changes, batch.has_next, cursor)

            current_partitions = batch.partitions

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> DatapointSubscriptionList:
        """`List data point subscriptions <https://api-docs.cognite.com/20230101/tag/Data-point-subscriptions/operation/listSubscriptions>`_

        Args:
            limit (int | None): Maximum number of subscriptions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
        Returns:
            DatapointSubscriptionList: List of requested datapoint subscriptions

        Examples:

            List 5 subscriptions:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> subscriptions = client.time_series.subscriptions.list(limit=5)

        """

        return self._list(
            method="GET", limit=limit, list_cls=DatapointSubscriptionList, resource_cls=DatapointSubscription
        )
