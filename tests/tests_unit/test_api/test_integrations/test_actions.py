from __future__ import annotations

import re

from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.integrations import Action, ActionList, ActionWrite
from tests.utils import get_url, jsgz_load

ACTION_RESPONSE = {
    "externalId": "my-action",
    "actionName": "restart",
    "status": "pending",
    "createdTime": 0,
    "lastUpdatedTime": 0,
}


class TestIntegrationActions:
    def test_create_single(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(get_url(async_client.integrations.actions, "/integrations/actions")) + r"\?.*"),
            json={"items": [ACTION_RESPONSE]},
            status_code=201,
        )

        res = cognite_client.integrations.actions.create(
            "my-integration", ActionWrite(external_id="my-action", action_name="restart")
        )

        assert isinstance(res, Action)
        assert res.status == "pending"

        request = httpx_mock.get_requests()[0]
        assert "externalId=my-integration" in str(request.url)
        body = jsgz_load(request.content)
        assert body == {"items": [{"externalId": "my-action", "actionName": "restart"}]}

    def test_list(self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock) -> None:
        url_pattern = re.compile(
            re.escape(get_url(async_client.integrations.actions, "/integrations/actions")) + r"(?:\?.*)?$"
        )
        httpx_mock.add_response(method="GET", url=url_pattern, json={"items": [ACTION_RESPONSE]})

        res = cognite_client.integrations.actions.list(external_id="my-integration", include_completed=False)

        assert isinstance(res, ActionList)
        assert len(res) == 1

        request = httpx_mock.get_requests()[0]
        assert "includeCompleted=false" in str(request.url)

    def test_retrieve(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.integrations.actions, "/integrations/actions/byids"),
            json={"items": [ACTION_RESPONSE]},
        )

        res = cognite_client.integrations.actions.retrieve("my-action")

        assert isinstance(res, Action)
        assert res.external_id == "my-action"

    def test_cancel(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        cancelled_response = {**ACTION_RESPONSE, "status": "cancel_pending"}
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.integrations.actions, "/integrations/actions/cancel"),
            json={"items": [cancelled_response]},
        )

        res = cognite_client.integrations.actions.cancel("my-action")

        assert isinstance(res, ActionList)
        assert res[0].status == "cancel_pending"

        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {"items": [{"externalId": "my-action"}]}
