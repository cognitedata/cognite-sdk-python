import httpx

from cognite.client import CogniteClient


def update_logs(cognite_client: CogniteClient, log_id: int, payload: list[dict]) -> httpx.Response:
    items = {"items": [{"id": log_id, "update": {"data": {"add": payload}}}]}
    return cognite_client.simulators._post(
        "/simulators/logs/update",
        json=items,
    )
