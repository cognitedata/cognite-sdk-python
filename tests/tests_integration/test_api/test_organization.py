from cognite.client import CogniteClient
from cognite.client._api.organization import OrgAPI


class TestOrganizationAPI:
    def test_get_organization(self, cognite_client: CogniteClient) -> None:
        org_api = OrgAPI(cognite_client.config, cognite_client._API_VERSION, cognite_client)

        organization = org_api._organization
        assert isinstance(organization, str)
        assert organization != ""
        if cognite_client.config.project in {"python-sdk-contributor", "python-sdk-test", "python-sdk-test-prod"}:
            assert organization == "cog-python-sdk"
