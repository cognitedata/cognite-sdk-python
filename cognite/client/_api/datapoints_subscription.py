from __future__ import annotations

from typing import Sequence

from cognite.client._api_client import APIClient
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscription,
    DatapointSubscriptionCreate,
    DatapointSubscriptionList,
    DataPointSubscriptionUpdate,
)


class DatapointsSubscriptionAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/subscriptions"

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

    def list(self, limit: int) -> DatapointSubscriptionList:
        ...
