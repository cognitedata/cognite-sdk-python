from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ


class AsyncDatapointsSubscriptionAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/datapoints/subscriptions"

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ, **kwargs) -> dict:
        """`List datapoints/subscriptions <placeholder-api-docs>`_"""
        # Placeholder implementation - would need specific filters and data classes
        # return await self._list(
        #     list_cls=placeholder_list_cls,
        #     resource_cls=placeholder_resource_cls,
        #     method="POST",
        #     limit=limit,
        #     filter=kwargs,
        # )
        pass

    async def retrieve(self, id: int | None = None, external_id: str | None = None):
        """`Retrieve a single datapoints/subscriptions by id.`_"""
        # Placeholder implementation
        pass

    async def create(self, item):
        """`Create one or more datapoints/subscriptions.`_"""
        # Placeholder implementation  
        pass

    async def delete(self, id: int | Sequence[int] | None = None, external_id: str | None = None):
        """`Delete one or more datapoints/subscriptions`_"""
        # Placeholder implementation
        pass

    async def update(self, item):
        """`Update one or more datapoints/subscriptions`_"""
        # Placeholder implementation
        pass
