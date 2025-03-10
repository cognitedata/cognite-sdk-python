from cognite.client._cognite_client import CogniteClient, Response


def update_logs(cognite_client: CogniteClient, log_id: int, payload: list[dict]) -> Response:
    items = {"items": [{"id": log_id, "update": {"data": {"add": payload}}}]}
    return cognite_client.post(
        url=f"/api/v1/projects/{cognite_client.config.project}/simulators/logs/update",
        json=items,
    )
