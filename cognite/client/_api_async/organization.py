from __future__ import annotations

from typing import Any

from cognite.client._async_api_client import AsyncAPIClient


class AsyncOrganizationAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/projects"

    async def retrieve(self) -> dict[str, Any]:
        """Get current project information."""
        res = await self._get(url_path=f"{self._RESOURCE_PATH}/{{project_name}}")
        return res.json()
