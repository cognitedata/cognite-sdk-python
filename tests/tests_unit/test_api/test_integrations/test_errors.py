from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.integrations.errors import IntegrationErrorList
from tests.utils import get_url

ERROR_RESPONSE = {
    "externalId": "my-integration",
    "level": "error",
    "description": "Something went wrong",
    "startTime": 100,
    "task": "poll",
}


class TestIntegrationErrors:
    def test_list(self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock) -> None:
        url_pattern = re.compile(
            re.escape(get_url(async_client.integrations.errors, "/integrations/errors")) + r"(?:\?.*)?$"
        )
        httpx_mock.add_response(method="GET", url=url_pattern, json={"items": [ERROR_RESPONSE]})

        res = cognite_client.integrations.errors.list(external_id="my-integration", task="poll")

        assert isinstance(res, IntegrationErrorList)
        assert len(res) == 1
        assert res[0].level == "error"
        assert res[0].description == "Something went wrong"

        request = httpx_mock.get_requests()[0]
        assert "externalId=my-integration" in str(request.url)
        assert "task=poll" in str(request.url)

    def test_list_task_without_external_id_raises(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="'task' requires 'external_id'"):
            cognite_client.integrations.errors.list(task="poll")
