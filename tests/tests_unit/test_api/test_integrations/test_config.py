from __future__ import annotations

import re

from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.integrations import ConfigRevision, ConfigRevisionMetadataList, ConfigRevisionWrite
from tests.utils import get_url, jsgz_load

CONFIG_REVISION_RESPONSE = {
    "externalId": "my-integration",
    "revision": 1,
    "config": "my config contents",
    "createdTime": 0,
    "lastUpdatedTime": 0,
}
CONFIG_REVISION_METADATA_RESPONSE = {
    "externalId": "my-integration",
    "revision": 1,
    "createdTime": 0,
    "lastUpdatedTime": 0,
}


class TestIntegrationConfig:
    def test_create(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.integrations.config, "/integrations/config"),
            json=CONFIG_REVISION_RESPONSE,
        )

        res = cognite_client.integrations.config.create(
            ConfigRevisionWrite(external_id="my-integration", config="my config contents")
        )

        assert isinstance(res, ConfigRevision)
        assert res.revision == 1
        assert res.config == "my config contents"

        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {"externalId": "my-integration", "config": "my config contents"}

    def test_retrieve(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        url_pattern = re.compile(
            re.escape(get_url(async_client.integrations.config, "/integrations/config")) + r"(?:\?.*)?$"
        )
        httpx_mock.add_response(method="GET", url=url_pattern, json=CONFIG_REVISION_RESPONSE)

        res = cognite_client.integrations.config.retrieve("my-integration", revision=1)

        assert isinstance(res, ConfigRevision)
        assert res.revision == 1

        request = httpx_mock.get_requests()[0]
        assert "externalId=my-integration" in str(request.url)
        assert "revision=1" in str(request.url)

    def test_list(self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock) -> None:
        url_pattern = re.compile(
            re.escape(get_url(async_client.integrations.config, "/integrations/config/revisions")) + r"(?:\?.*)?$"
        )
        httpx_mock.add_response(method="GET", url=url_pattern, json={"items": [CONFIG_REVISION_METADATA_RESPONSE]})

        res = cognite_client.integrations.config.list(external_id="my-integration")

        assert isinstance(res, ConfigRevisionMetadataList)
        assert len(res) == 1
        assert res[0].revision == 1

        request = httpx_mock.get_requests()[0]
        assert "externalId=my-integration" in str(request.url)
        assert "limit=25" in str(request.url)
