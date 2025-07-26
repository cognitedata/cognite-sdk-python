import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.principals import Principal, PrincipalList, ServicePrincipal
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture(scope="session")
def client(cognite_client_cdf_authenticated: CogniteClient) -> CogniteClient:
    """These endpoints requires an CDF authenticated client.
    Ref https://api-docs.cognite.com/20230101/tag/Principals#section/Authentication-for-this-API
    """
    return cognite_client_cdf_authenticated


@pytest.fixture(scope="session")
def me(client: CogniteClient) -> Principal:
    """Fixture to retrieve the principal of the user running the tests."""
    return client.iam.principals.me()


@pytest.fixture(scope="session")
def three_principals(client: CogniteClient) -> PrincipalList:
    """Fixture to retrieve a list of three principals."""
    principals = client.iam.principals.list(limit=3)
    assert len(principals) == 3, "Expected exactly three principals to be returned."
    return principals


@pytest.fixture(scope="session")
def service_account(client: CogniteClient) -> ServicePrincipal:
    """Fixture to retrieve a service account principal."""
    service_account = client.iam.principals.list(types="service_account", limit=1)
    assert len(service_account) == 1, "Expected exactly one service account principal to be returned."
    assert isinstance(service_account[0], ServicePrincipal), "Expected service account to be returned."
    return service_account[0]


class TestPrincipalsAPI:
    def test_me(self, client: CogniteClient, me: Principal) -> None:
        assert isinstance(me, Principal)

    def test_filter_principals_by_type(self, client: CogniteClient, me: Principal) -> None:
        type_ = me._type
        principals = client.iam.principals.list(types=type_, limit=-1)
        assert len(principals) > 0
        assert any(p.id == me.id for p in principals), "The principal should be included in the list of principals."

    def test_retrieve_principal(self, client: CogniteClient, three_principals: PrincipalList) -> None:
        retrieved = client.iam.principals.retrieve(id=three_principals[0].id)
        assert retrieved.dump() == three_principals[0].dump(), (
            "Retrieved principal should match the first principal in the list."
        )

    def test_retrieve_non_existing_principal(self, client: CogniteClient) -> None:
        assert client.iam.principals.retrieve(external_id="non-existing-principal-id") is None

    def test_retrieve_multiple_principals(self, client: CogniteClient, three_principals: PrincipalList) -> None:
        retrieved = client.iam.principals.retrieve_multiple(
            ids=three_principals.as_ids(), external_ids=["not_existing"], ignore_unknown_ids=True
        )
        assert isinstance(retrieved, PrincipalList)
        assert len(retrieved) == len(three_principals)
        assert retrieved.dump() == three_principals.dump(), "Retrieved principals should match the original list."

    def test_retrieve_multiple_non_existing_raise(self, client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as exc_info:
            client.iam.principals.retrieve_multiple(
                external_ids=["non-existing-principal-id"], ignore_unknown_ids=False
            )

        assert "not found" in str(exc_info.value), (
            "Should raise an error when trying to retrieve non-existing principals without ignoring unknown IDs."
        )

    def test_retrieve_by_external_id(self, client: CogniteClient, service_account: ServicePrincipal) -> None:
        assert service_account.external_id is not None, "Service account should have an external ID."
        retrieved = client.iam.principals.retrieve_multiple(external_ids=[service_account.external_id])
        assert isinstance(retrieved, PrincipalList)
        assert len(retrieved) == 1, "Expected exactly one principal to be retrieved by external ID."
        assert retrieved[0].dump() == service_account.dump(), (
            "Retrieved principal by external ID should match the service account principal."
        )
