from __future__ import annotations

import httpx

from cognite.client import AsyncCogniteClient


async def update_logs(async_client: AsyncCogniteClient, log_id: int, payload: list[dict]) -> httpx.Response:
    items = {"items": [{"id": log_id, "update": {"data": {"add": payload}}}]}
    return await async_client.simulators._post("/simulators/logs/update", json=items)
