from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ


class AsyncUnitsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/units"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.systems = AsyncUnitSystemAPI(self._config, self._api_version, self._cognite_client)

    async def list(self, name: str | None = None, symbol: str | None = None, limit: int | None = DEFAULT_LIMIT_READ) -> dict:
        """List units."""
        filter = {}
        if name:
            filter["name"] = name
        if symbol:
            filter["symbol"] = symbol
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/list", json={"filter": filter, "limit": limit})
        return res.json()


class AsyncUnitSystemAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/units/systems"

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> dict:
        """List unit systems."""
        res = await self._get(url_path=self._RESOURCE_PATH)
        return res.json()
