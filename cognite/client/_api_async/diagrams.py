from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ


class AsyncDiagramsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/context/diagram"

    async def detect(
        self,
        entities: list[dict[str, Any]],
        search_field: str = "name",
        partial_match: bool = False,
        min_tokens: int = 2,
    ) -> dict[str, Any]:
        """Detect entities in diagrams."""
        body = {
            "entities": entities,
            "searchField": search_field,
            "partialMatch": partial_match,
            "minTokens": min_tokens,
        }
        
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/detect", json=body)
        return res.json()

    async def convert(
        self,
        file_id: int | None = None,
        file_external_id: str | None = None,
    ) -> dict[str, Any]:
        """Convert diagram to interactive format."""
        body = {"items": [{}]}
        if file_id is not None:
            body["items"][0]["fileId"] = file_id
        if file_external_id is not None:
            body["items"][0]["fileExternalId"] = file_external_id
        
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/convert", json=body)
        return res.json()
