from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Datapoints,
    DatapointsList,
    DatapointsQuery,
    LatestDatapointQuery,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncDatapointsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/timeseries/data"

    async def retrieve(
        self,
        id: int | str | list[int] | list[str] | None = None,
        external_id: str | list[str] | None = None,
        start: int | str | None = None,
        end: int | str | None = None,
        aggregates: str | list[str] | None = None,
        granularity: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
    ) -> Datapoints | DatapointsList:
        """`Retrieve datapoints for time series <https://developer.cognite.com/api#tag/Time-series/operation/getDatapoints>`_"""
        query = DatapointsQuery(
            items=[{
                "id": id if isinstance(id, int) else None,
                "externalId": external_id if isinstance(external_id, str) else None,
                "start": start,
                "end": end,
                "aggregates": aggregates,
                "granularity": granularity,
                "limit": limit,
                "includeOutsidePoints": include_outside_points,
            }]
        )
        
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/list", json=query.dump(camel_case=True))
        
        if isinstance(id, (list, tuple)) or isinstance(external_id, (list, tuple)):
            return DatapointsList._load(res.json()["items"], cognite_client=self._cognite_client)
        else:
            items = res.json()["items"]
            if items:
                return Datapoints._load(items[0], cognite_client=self._cognite_client)
            return Datapoints(id=id, external_id=external_id, timestamp=[], value=[])

    async def retrieve_latest(
        self,
        id: int | str | list[int] | list[str] | None = None,
        external_id: str | list[str] | None = None,
        before: int | str | None = None,
    ) -> Datapoints | DatapointsList:
        """`Get latest datapoints for time series <https://developer.cognite.com/api#tag/Time-series/operation/getLatest>`_"""
        query = LatestDatapointQuery(
            items=[{
                "id": id if isinstance(id, int) else None,
                "externalId": external_id if isinstance(external_id, str) else None,
                "before": before,
            }]
        )
        
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/latest", json=query.dump(camel_case=True))
        
        if isinstance(id, (list, tuple)) or isinstance(external_id, (list, tuple)):
            return DatapointsList._load(res.json()["items"], cognite_client=self._cognite_client)
        else:
            items = res.json()["items"]
            if items:
                return Datapoints._load(items[0], cognite_client=self._cognite_client)
            return Datapoints(id=id, external_id=external_id, timestamp=[], value=[])

    async def insert(
        self, 
        datapoints: Sequence[Datapoints] | Datapoints,
    ) -> None:
        """`Insert datapoints for time series <https://developer.cognite.com/api#tag/Time-series/operation/postDatapoints>`_"""
        if isinstance(datapoints, Datapoints):
            datapoints = [datapoints]
        
        items = [dp.dump(camel_case=True) for dp in datapoints]
        await self._post(url_path=self._RESOURCE_PATH, json={"items": items})

    async def insert_multiple(
        self, 
        datapoints: Sequence[Datapoints],
    ) -> None:
        """`Insert datapoints for multiple time series <https://developer.cognite.com/api#tag/Time-series/operation/postDatapoints>`_"""
        await self.insert(datapoints)

    async def delete_range(
        self,
        id: int | None = None,
        external_id: str | None = None,
        start: int | str | None = None,
        end: int | str | None = None,
    ) -> None:
        """`Delete a range of datapoints from a time series <https://developer.cognite.com/api#tag/Time-series/operation/deleteDatapoints>`_"""
        body = {
            "items": [{
                "id": id,
                "externalId": external_id,
                "inclusiveBegin": start,
                "exclusiveEnd": end,
            }]
        }
        
        await self._post(url_path=f"{self._RESOURCE_PATH}/delete", json=body)
