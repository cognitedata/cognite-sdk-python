from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import (
    DATAPOINT_SUBSCRIPTION_DATA_LIST_LIMIT_DEFAULT,
    DATAPOINT_SUBSCRIPTIONS_LIST_LIMIT_DEFAULT,
)
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscription,
    DataPointSubscriptionBatch,
    DataPointSubscriptionCreate,
    DataPointSubscriptionList,
    DataPointSubscriptionPartition,
    DataPointSubscriptionUpdate,
)
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class DatapointsSubscriptionAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/subscriptions"

    def __init__(self, config: ClientConfig, api_version: Optional[str], cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"
        self.show_experimental_warning = True

    def create(self, subscription: DataPointSubscriptionCreate) -> DatapointSubscription:
        """`Create a subscription <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/postSubscriptions>`_

        Create a subscription that can be used to listen for changes in data points for a set of time series.

        Args:
            subscription: Subscription to create.

        Returns:
            Created subscription
        """

        return self._create_multiple(
            subscription,
            list_cls=DataPointSubscriptionList,
            resource_cls=DatapointSubscription,
            input_resource_cls=DataPointSubscriptionCreate,
        )

    def delete(self, external_id: str | Sequence[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete subscription(s). This operation cannot be undone. <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/deleteSubscriptions>`_

        Args:
            external_id (str | Sequence[str]): External ID or list of external IDs of subscriptions to delete.
            ignore_unknown_ids (bool): Whether to ignore IDs and external IDs that are not found rather than throw an exception.

        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
            wrap_ids=True,
        )

    @overload
    def retrieve(
        self, external_id: list[str] | tuple[str], ignore_unknown_ids: bool = False
    ) -> DataPointSubscriptionList:
        ...

    @overload
    def retrieve(
        self, external_id: str | Sequence[str], ignore_unknown_ids: bool = False
    ) -> Optional[DatapointSubscription] | DataPointSubscriptionList:
        ...

    def retrieve(
        self, external_id: str | list[str] | tuple[str] | Sequence[str], ignore_unknown_ids: bool = False
    ) -> Optional[DatapointSubscription] | DataPointSubscriptionList:
        """`Retrieve one or more subscriptions by external ID. <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/getSubscriptionsByIds>`_

        Args:
            external_id (str | Sequence[str]): External ID or list of external IDs of subscriptions to retrieve.
            ignore_unknown_ids (bool): Whether to ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            The requested subscriptions.
        """
        return self._retrieve_multiple(
            list_cls=DataPointSubscriptionList,
            resource_cls=DatapointSubscription,
            identifiers=IdentifierSequence.load(external_ids=external_id),
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def update(self, update: DataPointSubscriptionUpdate) -> DatapointSubscription:
        """`Update a subscriptions <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/updateSubscriptions>`_

        Update a subscription. Note that Fields that are not included in the request are not changed.
        Furthermore, the subscription partition cannot be changed.

        Args:
            update: The subscription update.

        Returns:
            Updated subscription.
        """
        return self._update_multiple(
            items=update,
            list_cls=DataPointSubscriptionList,
            resource_cls=DatapointSubscription,
            update_cls=DataPointSubscriptionUpdate,
        )

    def list_data(
        self,
        external_id: str,
        partitions: Sequence[tuple[int, str] | int | DataPointSubscriptionPartition],
        limit: int = DATAPOINT_SUBSCRIPTION_DATA_LIST_LIMIT_DEFAULT,
    ) -> DataPointSubscriptionBatch:
        """`Fetch the next batch of data from a given subscription and partition(s). <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/listSubscriptionData>`_

        Data can be ingested datapoints and time ranges where data is deleted. This endpoint will also return changes to
        the subscription itself, that is, if time series are added or removed from the subscription.

        Args:
            external_id (str): The external ID provided by the client. Must be unique for the resource type.
            partitions (Sequence[tuple[int, str] | int | DataPointSubscriptionPartition]): Pairs of (partition, cursor) to fetch from.
            limit (int): Approximate number of results to return across all partitions.

        Returns:
           A datapoint subscription batch with data from the given subscription.
        """
        body = {
            "externalId": external_id,
            "partitions": [DataPointSubscriptionPartition.create(p).dump(camel_case=True) for p in partitions],
            "limit": limit,
        }

        res = self._post(url_path=self._RESOURCE_PATH + "/data/list", json=body)
        return DataPointSubscriptionBatch._load(res.json())

    def list(self, limit: int = DATAPOINT_SUBSCRIPTIONS_LIST_LIMIT_DEFAULT) -> DataPointSubscriptionList:
        """`List data point subscriptions <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/listSubscriptions>`_

        Args:
            limit (int, optional): Maximum number of subscriptions to return. Defaults to 100. Set to -1, float("inf") or None
                to return all items.
        Returns:
            DataPointSubscriptionList: List of requested datapoint subscriptions

        Examples:

            List 5 subscriptions:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> subscriptions = c.time_series.subscriptions.list(limit=5)

            Iterate over subscriptions:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for subscription in c.time_series.subscriptions:
                ...     subscription # do something with the view

            Iterate over chunks of subscriptions to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for subscription_list in c.time_series.subscriptions(chunk_size=10):
                ...     subscription_list # do something with the views
        """
        return self._list(
            method="GET", limit=limit, list_cls=DataPointSubscriptionList, resource_cls=DatapointSubscription
        )
