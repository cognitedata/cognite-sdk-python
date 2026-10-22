from __future__ import annotations

import re

import pytest
from pytest_httpx2 import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.integrations import (
    Extractor,
    Integration,
    IntegrationList,
    IntegrationUpdate,
    IntegrationWrite,
)
from cognite.client.data_classes.integrations.tasks import SyncResult
from tests.utils import get_url, jsgz_load

INTEGRATION_RESPONSE = {
    "externalId": "my-integration",
    "extractor": {"externalId": "cognite-simple-influxdb-extractor", "version": "1.0.0"},
    "name": "My integration",
    "createdTime": 0,
    "lastUpdatedTime": 0,
    "tasks": [{"type": "continuous", "name": "poll", "action": False}],
}


SYNC_TASK_HISTORY_RESPONSE = {
    "externalId": "my-integration",
    "taskName": "poll",
    "startTime": 100,
    "endTime": 200,
    "errorCount": 0,
    "warningCount": 0,
    "fatalCount": 0,
}


class TestIntegrations:
    def test_list(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        url_pattern = re.compile(re.escape(get_url(async_client.integrations, "/integrations")) + r"(?:\?.*)?$")
        httpx2_mock.add_response(method="GET", url=url_pattern, json={"items": [INTEGRATION_RESPONSE]})

        res = cognite_client.integrations.list(limit=10)

        assert isinstance(res, IntegrationList)
        assert len(res) == 1
        assert res[0].external_id == "my-integration"
        assert res[0].tasks[0].name == "poll"

        request = httpx2_mock.get_requests()[0]
        assert request.method == "GET"
        assert request.headers["cdf-version"] == "20230101-beta"

    def test_iterate(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        url_pattern = re.compile(re.escape(get_url(async_client.integrations, "/integrations")) + r"(?:\?.*)?$")
        httpx2_mock.add_response(method="GET", url=url_pattern, json={"items": [INTEGRATION_RESPONSE]})

        integrations = list(cognite_client.integrations())

        assert len(integrations) == 1
        assert isinstance(integrations[0], Integration)
        assert integrations[0].external_id == "my-integration"

    def test_create(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=get_url(async_client.integrations, "/integrations"),
            json={"items": [INTEGRATION_RESPONSE]},
        )
        integration = IntegrationWrite(
            external_id="my-integration",
            extractor=Extractor(external_id="cognite-simple-influxdb-extractor", version="1.0.0"),
            name="My integration",
        )

        res = cognite_client.integrations.create(integration)

        assert isinstance(res, Integration)
        assert res.external_id == "my-integration"

        request = httpx2_mock.get_requests()[0]
        assert request.headers["cdf-version"] == "20230101-beta"
        body = jsgz_load(request.content)
        assert body == {
            "items": [
                {
                    "externalId": "my-integration",
                    "name": "My integration",
                    "extractor": {"externalId": "cognite-simple-influxdb-extractor", "version": "1.0.0"},
                }
            ]
        }

    def test_retrieve(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=get_url(async_client.integrations, "/integrations/byids"),
            json={"items": [INTEGRATION_RESPONSE]},
        )

        res = cognite_client.integrations.retrieve("my-integration")

        assert isinstance(res, Integration)
        assert res.external_id == "my-integration"

        request = httpx2_mock.get_requests()[0]
        assert request.headers["cdf-version"] == "20230101-beta"
        body = jsgz_load(request.content)
        assert body == {"items": [{"externalId": "my-integration"}], "ignoreUnknownIds": False}

    def test_retrieve_ignore_unknown_ids(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=get_url(async_client.integrations, "/integrations/byids"),
            json={"items": []},
        )

        res = cognite_client.integrations.retrieve("missing-integration", ignore_unknown_ids=True)

        assert res is None

        request = httpx2_mock.get_requests()[0]
        assert request.headers["cdf-version"] == "20230101-beta"
        body = jsgz_load(request.content)
        assert body == {"items": [{"externalId": "missing-integration"}], "ignoreUnknownIds": True}

    def test_retrieve_multiple_empty_result(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=get_url(async_client.integrations, "/integrations/byids"),
            json={"items": []},
        )

        res = cognite_client.integrations.retrieve(["missing-integration"], ignore_unknown_ids=True)

        assert isinstance(res, IntegrationList)
        assert len(res) == 0

    def test_update(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=get_url(async_client.integrations, "/integrations/update"),
            json={"items": [INTEGRATION_RESPONSE]},
        )
        update = IntegrationUpdate(external_id="my-integration")
        update.description.set("My new description")

        res = cognite_client.integrations.update(update)

        assert isinstance(res, Integration)

        request = httpx2_mock.get_requests()[0]
        assert request.headers["cdf-version"] == "20230101-beta"
        body = jsgz_load(request.content)
        assert body == {
            "items": [{"externalId": "my-integration", "update": {"description": {"set": "My new description"}}}]
        }

    def test_delete(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        httpx2_mock.add_response(method="POST", url=get_url(async_client.integrations, "/integrations/delete"), json={})

        cognite_client.integrations.delete("my-integration")

        request = httpx2_mock.get_requests()[0]
        assert request.headers["cdf-version"] == "20230101-beta"
        body = jsgz_load(request.content)
        assert body == {"items": [{"externalId": "my-integration"}], "ignoreUnknownIds": False}

    def test_delete_ignore_unknown_ids(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        httpx2_mock.add_response(method="POST", url=get_url(async_client.integrations, "/integrations/delete"), json={})

        cognite_client.integrations.delete(["my-integration", "missing-integration"], ignore_unknown_ids=True)

        request = httpx2_mock.get_requests()[0]
        assert request.headers["cdf-version"] == "20230101-beta"
        body = jsgz_load(request.content)
        assert body == {
            "items": [{"externalId": "my-integration"}, {"externalId": "missing-integration"}],
            "ignoreUnknownIds": True,
        }

    def test_list_empty(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        url_pattern = re.compile(re.escape(get_url(async_client.integrations, "/integrations")) + r"(?:\?.*)?$")
        httpx2_mock.add_response(method="GET", url=url_pattern, json={"items": []})

        res = cognite_client.integrations.list(limit=10)

        assert isinstance(res, IntegrationList)
        assert len(res) == 0

        request = httpx2_mock.get_requests()[0]
        assert request.headers["cdf-version"] == "20230101-beta"

    def test_list_pagination(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        url_pattern = re.compile(re.escape(get_url(async_client.integrations, "/integrations")) + r"(?:\?.*)?$")
        second_integration = INTEGRATION_RESPONSE | {"externalId": "my-other-integration"}
        httpx2_mock.add_response(
            method="GET", url=url_pattern, json={"items": [INTEGRATION_RESPONSE], "nextCursor": "page2"}
        )
        httpx2_mock.add_response(method="GET", url=url_pattern, json={"items": [second_integration]})

        res = cognite_client.integrations.list(limit=None)

        assert isinstance(res, IntegrationList)
        assert [item.external_id for item in res] == ["my-integration", "my-other-integration"]

        requests = httpx2_mock.get_requests()
        assert len(requests) == 2
        assert "cursor=page2" in str(requests[1].url)

    def test_sync(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        url_pattern = re.compile(re.escape(get_url(async_client.integrations, "/integrations/sync")) + r"(?:\?.*)?$")
        httpx2_mock.add_response(
            method="GET",
            url=url_pattern,
            json={"nextCursor": "abc", "moreData": False, "history": [SYNC_TASK_HISTORY_RESPONSE]},
        )

        res = cognite_client.integrations.sync(external_id="my-integration", include_task_updates=True)

        assert isinstance(res, SyncResult)
        assert res.next_cursor == "abc"
        assert res.more_data is False
        assert res.history is not None
        assert res.history[0].task_name == "poll"
        assert res.errors is None

    def test_sync_requires_include_errors_or_include_task_updates(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="'include_errors' or 'include_task_updates'"):
            cognite_client.integrations.sync(external_id="my-integration")
