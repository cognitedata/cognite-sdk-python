from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from tests.utils import get_url, jsgz_load


class TestIntegrations:
    def test_delete_integrations(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.simulators, "/simulators/integrations/delete"),
            json={},
            status_code=200,
        )
        cognite_client.simulators.integrations.delete(external_ids="test")

        exp_body = {"items": [{"externalId": "test"}]}
        assert exp_body == jsgz_load(httpx_mock.get_requests()[0].content)
