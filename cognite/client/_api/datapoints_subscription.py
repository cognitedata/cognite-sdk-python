from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

from cognite.client._api_client import APIClient
from cognite.client._constants import DATAPOINT_SUBSCRIPTIONS_LIST_LIMIT_DEFAULT
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscription,
    DatapointSubscriptionCreate,
    DatapointSubscriptionList,
    DataPointSubscriptionUpdate,
)

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class DatapointsSubscriptionAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/subscriptions"

    def __init__(self, config: ClientConfig, api_version: Optional[str], cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"

    def create(
        self, subscription: DatapointSubscriptionCreate | Sequence[DatapointSubscriptionCreate]
    ) -> DatapointSubscription | DatapointSubscriptionList:
        ...

    def delete(self, external_id: str | Sequence[str], ignore_unknown_ids: bool = False) -> None:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], ignore_unknown_ids: bool = False
    ) -> DatapointSubscription | DatapointSubscriptionList:
        ...

    def update(
        self, update: DataPointSubscriptionUpdate | Sequence[DataPointSubscriptionUpdate]
    ) -> DatapointSubscription | DatapointSubscriptionList:
        ...

    def list_data(self, external_id: str, partitions: list, limit: int) -> dict:
        ...

    def list(self, limit: int = DATAPOINT_SUBSCRIPTIONS_LIST_LIMIT_DEFAULT) -> DatapointSubscriptionList:
        """`List data point subscriptions <https://pr-2221.specs.preview.cogniteapp.com/20230101-beta.json.html#tag/Data-point-subscriptions/operation/listSubscriptions>`_

        Args:
            limit (int, optional): Maximum number of subscriptions to return. Defaults to 100. Set to -1, float("inf") or None
                to return all items.
        Returns:
            DatapointSubscriptionList: List of requested datapoint subscriptions

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
            method="GET", limit=limit, list_cls=DatapointSubscriptionList, resource_cls=DatapointSubscription
        )
