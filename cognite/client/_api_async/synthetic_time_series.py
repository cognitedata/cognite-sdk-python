from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Datapoints,
    DatapointsList,
)


class AsyncSyntheticTimeSeriesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/timeseries/synthetic"

    async def query(
        self,
        expressions: list[dict[str, Any]],
        start: int | str,
        end: int | str,
        limit: int | None = None,
        aggregates: list[str] | None = None,
        granularity: str | None = None,
    ) -> DatapointsList:
        """Query synthetic time series."""
        body = {
            "items": expressions,
            "start": start,
            "end": end,
            "limit": limit,
            "aggregates": aggregates,
            "granularity": granularity,
        }
        body = {k: v for k, v in body.items() if v is not None}
        
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/query", json=body)
        return DatapointsList._load(res.json()["items"], cognite_client=self._cognite_client)
