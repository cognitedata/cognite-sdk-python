from __future__ import annotations

from cognite.client import AsyncCogniteClient
from cognite.client.response import CogniteHTTPResponse


async def update_logs(async_client: AsyncCogniteClient, log_id: int, payload: list[dict]) -> CogniteHTTPResponse:
    items = {"items": [{"id": log_id, "update": {"data": {"add": payload}}}]}
    return await async_client.simulators._post("/simulators/logs/update", json=items)
