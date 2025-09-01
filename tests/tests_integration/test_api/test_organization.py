import pytest

from cognite.client import CogniteClient
from cognite.client._api.organization import OrgAPI


@pytest.fixture(scope="session")
def org_api(cognite_client_cdf_authenticated: CogniteClient) -> OrgAPI:
    client = cognite_client_cdf_authenticated
    return OrgAPI(client.config, client._API_VERSION, client)


class TestOrganizationAPI:
    def test_get_organization(self, org_api: OrgAPI) -> None:
        organization = org_api._organization
        assert isinstance(organization, str)
        assert organization != ""
        if org_api._config.project in {"python-sdk-contributor", "python-sdk-test", "python-sdk-test-prod"}:
            assert organization == "cog-python-sdk"
        else:
            pytest.skip("Organization not supported")

    def test_get_request_org_endpoint(self, org_api: OrgAPI) -> None:
        response = org_api._get("/principals")
        assert response.status_code == 200
