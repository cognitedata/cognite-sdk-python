from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.principals import Principal, PrincipalList, ServicePrincipal
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture(scope="session")
def me(cognite_client_cog_idp: CogniteClient) -> Principal:
    """Fixture to retrieve the principal of the user running the tests."""
    return cognite_client_cog_idp.iam.principals.me()


@pytest.fixture(scope="session")
def three_principals(cognite_client_cog_idp: CogniteClient) -> PrincipalList:
    """Fixture to retrieve a list of three principals."""
    client = cognite_client_cog_idp
    principals = client.iam.principals.list(limit=3)
    assert len(principals) == 3, "Expected exactly three principals to be returned."
    return principals


@pytest.fixture(scope="session")
def service_account(cognite_client_cog_idp: CogniteClient) -> ServicePrincipal:
    """Fixture to retrieve a service account principal."""
    client = cognite_client_cog_idp
    service_account = client.iam.principals.list(types="service_account", limit=1)
    assert len(service_account) == 1, "Expected exactly one service account principal to be returned."
    serv_acc = service_account[0]
    assert isinstance(serv_acc, ServicePrincipal), "Expected service account to be returned."
    return serv_acc


class TestPrincipalsAPI:
    def test_me(self, me: Principal) -> None:
        assert isinstance(me, Principal)

    def test_filter_principals_by_type(self, cognite_client_cog_idp: CogniteClient, me: Principal) -> None:
        client = cognite_client_cog_idp
        type_ = me._type
        principals = client.iam.principals.list(types=type_, limit=-1)
        assert len(principals) > 0
        assert any(p.id == me.id for p in principals), "The principal should be included in the list of principals."

    def test_list_multiple_type(self, cognite_client_cog_idp: CogniteClient, me: Principal) -> None:
        client = cognite_client_cog_idp
        types = ["USER", "SERVICE_ACCOUNT"]
        principals = client.iam.principals.list(types=types, limit=-1)
        assert len(principals) > 0
        assert any(p.id == me.id for p in principals), "The principal should be included in the list of principals."

    def test_retrieve_principal(self, cognite_client_cog_idp: CogniteClient, three_principals: PrincipalList) -> None:
        client = cognite_client_cog_idp
        retrieved = client.iam.principals.retrieve(id=three_principals[0].id)
        assert isinstance(retrieved, Principal)
        assert retrieved.dump() == three_principals[0].dump(), (
            "Retrieved principal should match the first principal in the list."
        )

    def test_retrieve_non_existing_principal(self, cognite_client_cog_idp: CogniteClient) -> None:
        client = cognite_client_cog_idp
        assert client.iam.principals.retrieve(external_id="non-existing-principal-id") is None

    def test_retrieve_nothing(self, cognite_client_cog_idp: CogniteClient) -> None:
        client = cognite_client_cog_idp
        assert client.iam.principals.retrieve() == PrincipalList([])

    def test_retrieve_multiple_principals(
        self, cognite_client_cog_idp: CogniteClient, three_principals: PrincipalList
    ) -> None:
        client = cognite_client_cog_idp

        retrieved = client.iam.principals.retrieve(
            id=[*three_principals.as_ids(), "not_existing"], ignore_unknown_ids=True
        )
        assert isinstance(retrieved, PrincipalList)
        assert len(retrieved) == len(three_principals)
        assert retrieved.dump() == three_principals.dump(), "Retrieved principals should match the original list."

    def test_retrieve_multiple_non_existing_raise(self, cognite_client_cog_idp: CogniteClient) -> None:
        client = cognite_client_cog_idp
        with pytest.raises(CogniteAPIError) as exc_info:
            client.iam.principals.retrieve(external_id=["non-existing-principal-id"], ignore_unknown_ids=False)

        assert "not found" in str(exc_info.value), (
            "Should raise an error when trying to retrieve non-existing principals without ignoring unknown IDs."
        )

    def test_retrieve_by_external_id(
        self, cognite_client_cog_idp: CogniteClient, service_account: ServicePrincipal
    ) -> None:
        client = cognite_client_cog_idp
        assert service_account.external_id is not None, "Service account should have an external ID."
        retrieved = client.iam.principals.retrieve(external_id=[service_account.external_id])
        assert isinstance(retrieved, PrincipalList)
        assert len(retrieved) == 1, "Expected exactly one principal to be retrieved by external ID."
        assert retrieved[0].dump() == service_account.dump(), (
            "Retrieved principal by external ID should match the service account principal."
        )
