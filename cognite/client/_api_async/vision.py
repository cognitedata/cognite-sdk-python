from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ


class AsyncVisionAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/context/vision"

    async def extract(
        self,
        features: list[str],
        file_id: int | None = None,
        file_external_id: str | None = None,
    ) -> dict[str, Any]:
        """Extract features from images."""
        body = {
            "items": [{
                "fileId": file_id,
                "fileExternalId": file_external_id,
            }],
            "features": features,
        }
        body = {k: v for k, v in body.items() if v is not None}
        
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/extract", json=body)
        return res.json()

    async def extract_text(
        self,
        file_id: int | None = None,
        file_external_id: str | None = None,
    ) -> dict[str, Any]:
        """Extract text from images."""
        return await self.extract(
            features=["TextDetection"],
            file_id=file_id,
            file_external_id=file_external_id,
        )
