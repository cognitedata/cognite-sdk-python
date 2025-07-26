import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.principals import Principal, PrincipalList


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


class TestPrincipalsAPI:
    def test_me(self, client: CogniteClient, me: Principal) -> None:
        assert isinstance(me, Principal)

    def test_list_principals(self, client: CogniteClient, me: Principal) -> None:
        principals = client.iam.principals.list(limit=3)
        assert 0 < len(principals) <= 3

    def test_filter_principals_by_type(self, client: CogniteClient, me: Principal) -> None:
        type_ = me._type
        principals = client.iam.principals.list(types=type_, limit=-1)
        assert len(principals) > 0
        assert any(p.id == me.id for p in principals), "The principal should be included in the list of principals."

    def test_retrieve_principal(self, client: CogniteClient, me: Principal) -> None:
        retrieved = client.iam.principals.retrieve(id=me.id)
        assert retrieved.dump() == me.dump()

    def test_retrieve_multiple_principals(self, client: CogniteClient, me: Principal) -> None:
        retrieved = client.iam.principals.retrieve_multiple(id=[me.id])
        assert isinstance(retrieved, PrincipalList)
        assert len(retrieved) == 1
        assert retrieved[0].dump() == me.dump()
