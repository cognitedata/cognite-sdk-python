from __future__ import annotations

import re

import pytest
from pytest_httpx2 import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.integrations.tasks import TaskHistoryList
from tests.utils import get_url

TASK_HISTORY_RESPONSE = {
    "externalId": "my-integration",
    "taskName": "poll",
    "startTime": 100,
    "endTime": 200,
    "errorCount": 0,
    "warningCount": 0,
    "fatalCount": 0,
}


class TestIntegrationTaskHistory:
    def test_list(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        url_pattern = re.compile(
            re.escape(get_url(async_client.integrations.tasks.history, "/integrations/history")) + r"(?:\?.*)?$"
        )
        httpx2_mock.add_response(method="GET", url=url_pattern, json={"items": [TASK_HISTORY_RESPONSE]})

        res = cognite_client.integrations.tasks.history.list(external_id="my-integration", task_name="poll")

        assert isinstance(res, TaskHistoryList)
        assert len(res) == 1
        assert res[0].task_name == "poll"

        request = httpx2_mock.get_requests()[0]
        assert "externalId=my-integration" in str(request.url)
        assert "taskName=poll" in str(request.url)

    def test_list_task_name_without_external_id_raises(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="'task_name' requires 'external_id'"):
            cognite_client.integrations.tasks.history.list(task_name="poll")
