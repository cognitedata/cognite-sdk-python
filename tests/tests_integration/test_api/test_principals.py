import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.principals import Principal, PrincipalList


@pytest.fixture(scope="session")
def me(cognite_client: CogniteClient) -> Principal:
    """Fixture to retrieve the principal of the user running the tests."""
    return cognite_client.iam.principals.me()


class TestPrincipalsAPI:
    def test_list_principals(self, cognite_client: CogniteClient, me: Principal) -> None:
        principals = cognite_client.iam.principals.list(limt=3)
        assert 0 < len(principals) <= 3

    def test_filter_principals_by_type(self, cognite_client: CogniteClient, me: Principal) -> None:
        type_ = me._type
        principals = cognite_client.iam.principals.list(types=type_, limit=-1)
        assert len(principals) > 0
        assert any(p.id == me.id for p in principals)

    def test_retrieve_principal(self, cognite_client: CogniteClient, me: Principal) -> None:
        retrieved = cognite_client.iam.principals.retrieve(id=me.id)
        assert retrieved.dump() == me.dump()

    def test_retrieve_multiple_principals(self, cognite_client: CogniteClient, me: Principal) -> None:
        retrieved = cognite_client.iam.principals.retrieve_multiple(id=[me.id])
        assert isinstance(retrieved, PrincipalList)
        assert len(retrieved) == 1
        assert retrieved[0].dump() == me.dump()
