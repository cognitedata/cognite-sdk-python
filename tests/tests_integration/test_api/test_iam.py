import pytest

from cognite.client.data_classes import APIKey, Group, SecurityCategory, ServiceAccount, ServiceAccountList
from cognite.client.utils._auxiliary import random_string


class TestServiceAccountAPI:
    @pytest.mark.skip("ServiceAccount methods failing, probably due to api keys deprecation")
    def test_list(self, cognite_client):
        res = cognite_client.iam.service_accounts.list()
        assert isinstance(res, ServiceAccountList)
        assert len(res) > 0

    def test_create_and_delete(self, cognite_client):
        name = "test_sa_" + random_string(10)
        sa = cognite_client.iam.service_accounts.create(ServiceAccount(name=name))
        assert isinstance(sa, ServiceAccount)
        assert sa.id in {s.id for s in cognite_client.iam.service_accounts.list()}
        cognite_client.iam.service_accounts.delete(sa.id)
        assert sa.id not in {s.id for s in cognite_client.iam.service_accounts.list()}


@pytest.fixture(scope="module")
def service_account_id(cognite_client):
    service_accounts = cognite_client.iam.service_accounts.list()
    return next(sa for sa in service_accounts if sa.name == "admin").id


@pytest.mark.skip("most API keys methods failing, probably due to deprecation")
class TestAPIKeysAPI:
    def test_list_with_sa_id(self, cognite_client, service_account_id):
        res = cognite_client.iam.api_keys.list(service_account_id=service_account_id)
        assert len(res) > 0

    def test_list_all(self, cognite_client):
        res = cognite_client.iam.api_keys.list(all=True)
        assert len(res) > 0

    def test_list_deleted(self, cognite_client):
        res = cognite_client.iam.api_keys.list(include_deleted=True, all=True)
        assert len(res) > 0

    def test_create_and_delete(self, cognite_client, service_account_id):
        res = cognite_client.iam.api_keys.create(service_account_id)
        assert isinstance(res, APIKey)
        assert res.id in {k.id for k in cognite_client.iam.api_keys.list(all=True)}
        cognite_client.iam.api_keys.delete(res.id)
        assert res.id not in {k.id for k in cognite_client.iam.api_keys.list(all=True)}


@pytest.fixture(scope="module")
def group_id(cognite_client):
    return cognite_client.iam.groups.list()[0].id


class TestGroupsAPI:
    def test_list(self, cognite_client):
        res = cognite_client.iam.groups.list(all=True)
        assert len(res) > 0

    def test_create(self, cognite_client):
        group = cognite_client.iam.groups.create(
            Group(name="bla", capabilities=[{"eventsAcl": {"actions": ["READ"], "scope": {"all": {}}}}])
        )
        assert "bla" == group.name
        cognite_client.iam.groups.delete(group.id)


class TestSecurityCategoriesAPI:
    def test_list(self, cognite_client):
        res = cognite_client.iam.security_categories.list()
        assert len(res) > 0

    def test_create_and_delete(self, cognite_client):
        random_name = "test_" + random_string(10)
        res = cognite_client.iam.security_categories.create(SecurityCategory(name=random_name))
        assert res.id in {s.id for s in cognite_client.iam.security_categories.list()}
        cognite_client.iam.security_categories.delete(res.id)
        assert res.id not in {s.id for s in cognite_client.iam.security_categories.list()}
