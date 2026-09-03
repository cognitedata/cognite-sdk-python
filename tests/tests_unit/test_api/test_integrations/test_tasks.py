from __future__ import annotations

import re

from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.integrations.tasks import SyncResult, TaskHistoryList
from tests.utils import get_url

TASK_HISTORY_RESPONSE = {
    "externalId": "my-integration",
    "taskName": "poll",
    "startTime": 100,
    "endTime": 200,
    "errorCount": 0,
}


class TestIntegrationTasks:
    def test_list_history(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        url_pattern = re.compile(
            re.escape(get_url(async_client.integrations.tasks, "/integrations/history")) + r"(?:\?.*)?$"
        )
        httpx_mock.add_response(method="GET", url=url_pattern, json={"items": [TASK_HISTORY_RESPONSE]})

        res = cognite_client.integrations.tasks.list_history(external_id="my-integration", task_name="poll")

        assert isinstance(res, TaskHistoryList)
        assert len(res) == 1
        assert res[0].task_name == "poll"

        request = httpx_mock.get_requests()[0]
        assert "externalId=my-integration" in str(request.url)
        assert "taskName=poll" in str(request.url)

    def test_sync(self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock) -> None:
        url_pattern = re.compile(
            re.escape(get_url(async_client.integrations.tasks, "/integrations/sync")) + r"(?:\?.*)?$"
        )
        httpx_mock.add_response(
            method="GET",
            url=url_pattern,
            json={"nextCursor": "abc", "moreData": False, "history": [TASK_HISTORY_RESPONSE]},
        )

        res = cognite_client.integrations.tasks.sync(external_id="my-integration", include_task_updates=True)

        assert isinstance(res, SyncResult)
        assert res.next_cursor == "abc"
        assert res.more_data is False
        assert res.history is not None
        assert res.history[0].task_name == "poll"
        assert res.errors is None
