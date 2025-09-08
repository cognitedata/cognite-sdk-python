from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    DatapointSubscription,
    DatapointSubscriptionList,
    DataPointSubscriptionCreate,
    DataPointSubscriptionUpdate,
    DataPointSubscriptionWrite,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncDatapointsSubscriptionAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/subscriptions"

    async def list(
        self,
        partition_id: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ
    ) -> DatapointSubscriptionList:
        """List datapoint subscriptions."""
        filter = {}
        if partition_id:
            filter["partitionId"] = partition_id
        return await self._list(
            list_cls=DatapointSubscriptionList,
            resource_cls=DatapointSubscription,
            method="POST",
            limit=limit,
            filter=filter,
        )

    async def retrieve(self, external_id: str) -> DatapointSubscription | None:
        """Retrieve datapoint subscription."""
        identifiers = IdentifierSequence.load(external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=DatapointSubscriptionList,
            resource_cls=DatapointSubscription,
            identifiers=identifiers,
        )

    async def create(
        self, 
        subscription: DataPointSubscriptionCreate | Sequence[DataPointSubscriptionCreate]
    ) -> DatapointSubscription | DatapointSubscriptionList:
        """Create datapoint subscriptions."""
        return await self._create_multiple(
            list_cls=DatapointSubscriptionList,
            resource_cls=DatapointSubscription,
            items=subscription,
        )

    async def delete(self, external_id: str | Sequence[str]) -> None:
        """Delete datapoint subscriptions."""
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
        )

    async def update(
        self, 
        subscription: DataPointSubscriptionUpdate | Sequence[DataPointSubscriptionUpdate]
    ) -> DatapointSubscription | DatapointSubscriptionList:
        """Update datapoint subscriptions."""
        return await self._update_multiple(
            list_cls=DatapointSubscriptionList,
            resource_cls=DatapointSubscription,
            update_cls=DataPointSubscriptionUpdate,
            items=subscription,
        )
