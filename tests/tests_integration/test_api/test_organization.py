from __future__ import annotations

import pytest

from cognite.client import AsyncCogniteClient
from cognite.client._org_client import OrgAPIClient


@pytest.fixture(scope="session")
def org_api(async_client_cog_idp: AsyncCogniteClient) -> OrgAPIClient:
    client = async_client_cog_idp
    return OrgAPIClient(client.config, client._API_VERSION, client)


class TestOrganizationAPI:
    def test_get_organization(self, org_api: OrgAPIClient) -> None:
        organization = org_api._organization
        assert isinstance(organization, str)
        assert organization != ""
        if org_api._config.project in {"python-sdk-contributor", "python-sdk-test", "python-sdk-test-prod"}:
            assert organization == "cog-python-sdk"

    async def test_get_request_org_endpoint(self, org_api: OrgAPIClient) -> None:
        response = await org_api._get("/principals")
        assert response.status_code == 200
