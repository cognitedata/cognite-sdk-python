from __future__ import annotations

import re

from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.integrations import (
    Extractor,
    Integration,
    IntegrationList,
    IntegrationUpdate,
    IntegrationWrite,
)
from tests.utils import get_url, jsgz_load

INTEGRATION_RESPONSE = {
    "externalId": "my-integration",
    "extractor": {"externalId": "cognite-simple-influxdb-extractor", "version": "1.0.0"},
    "name": "My integration",
    "createdTime": 0,
    "lastUpdatedTime": 0,
    "tasks": [{"type": "continuous", "name": "poll", "action": False}],
}


class TestIntegrations:
    def test_list(self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock) -> None:
        url_pattern = re.compile(re.escape(get_url(async_client.integrations, "/integrations")) + r"(?:\?.*)?$")
        httpx_mock.add_response(method="GET", url=url_pattern, json={"items": [INTEGRATION_RESPONSE]})

        res = cognite_client.integrations.list(limit=10)

        assert isinstance(res, IntegrationList)
        assert len(res) == 1
        assert res[0].external_id == "my-integration"
        assert res[0].tasks[0].name == "poll"

        request = httpx_mock.get_requests()[0]
        assert request.method == "GET"
        assert request.headers["cdf-version"] == async_client.integrations._alpha_version_header()["cdf-version"]

    def test_create(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
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

        body = jsgz_load(httpx_mock.get_requests()[0].content)
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
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.integrations, "/integrations/byids"),
            json={"items": [INTEGRATION_RESPONSE]},
        )

        res = cognite_client.integrations.retrieve("my-integration")

        assert isinstance(res, Integration)
        assert res.external_id == "my-integration"

        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {"items": [{"externalId": "my-integration"}], "ignoreUnknownIds": False}

    def test_update(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.integrations, "/integrations/update"),
            json={"items": [INTEGRATION_RESPONSE]},
        )
        update = IntegrationUpdate(external_id="my-integration")
        update.description.set("My new description")

        res = cognite_client.integrations.update(update)

        assert isinstance(res, Integration)

        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {
            "items": [{"externalId": "my-integration", "update": {"description": {"set": "My new description"}}}]
        }

    def test_delete(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(method="POST", url=get_url(async_client.integrations, "/integrations/delete"), json={})

        cognite_client.integrations.delete("my-integration")

        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {"items": [{"externalId": "my-integration"}], "ignoreUnknownIds": False}
