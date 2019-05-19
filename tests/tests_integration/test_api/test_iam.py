import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import APIKey, Group, SecurityCategory, ServiceAccount, ServiceAccountList
from cognite.client.utils._utils import random_string

COGNITE_CLIENT = CogniteClient()


class TestServiceAccountAPI:
    def test_list(self):
        res = COGNITE_CLIENT.iam.service_accounts.list()
        assert isinstance(res, ServiceAccountList)
        assert len(res) > 0

    def test_create_and_delete(self):
        name = "test_sa_" + random_string(10)
        sa = COGNITE_CLIENT.iam.service_accounts.create(ServiceAccount(name=name))
        assert isinstance(sa, ServiceAccount)
        assert sa.id in {s.id for s in COGNITE_CLIENT.iam.service_accounts.list()}
        COGNITE_CLIENT.iam.service_accounts.delete(sa.id)
        assert sa.id not in {s.id for s in COGNITE_CLIENT.iam.service_accounts.list()}


@pytest.fixture(scope="module")
def service_account_id():
    return COGNITE_CLIENT.iam.service_accounts.list()[0].id


class TestAPIKeysAPI:
    def test_list_with_sa_id(self, service_account_id):
        res = COGNITE_CLIENT.iam.api_keys.list(service_account_id=service_account_id)
        assert len(res) > 0

    def test_list_all(self):
        res = COGNITE_CLIENT.iam.api_keys.list(all=True)
        assert len(res) > 0

    def test_list_deleted(self):
        res = COGNITE_CLIENT.iam.api_keys.list(include_deleted=True)
        assert len(res) > 0

    def test_create_and_delete(self, service_account_id):
        res = COGNITE_CLIENT.iam.api_keys.create(service_account_id)
        assert isinstance(res, APIKey)
        assert res.id in {k.id for k in COGNITE_CLIENT.iam.api_keys.list(all=True)}
        COGNITE_CLIENT.iam.api_keys.delete(res.id)
        assert res.id not in {k.id for k in COGNITE_CLIENT.iam.api_keys.list(all=True)}


@pytest.fixture(scope="module")
def group_id():
    return COGNITE_CLIENT.iam.groups.list()[0].id


class TestGroupsAPI:
    @pytest.mark.xfail(strict=True)
    def test_list(self):
        res = COGNITE_CLIENT.iam.groups.list(all=True)
        assert len(res) > 0

    @pytest.mark.xfail(strict=True)
    def test_create_and_delete(self):
        group = COGNITE_CLIENT.iam.groups.create(Group(name="bla"))
        COGNITE_CLIENT.iam.groups.delete(group.id)
        assert group.id not in {g.id for g in COGNITE_CLIENT.iam.groups.list(all=True)}

    def test_list_service_accounts_in_group(self, group_id):
        service_accounts = COGNITE_CLIENT.iam.groups.list_service_accounts(group_id)
        assert len(service_accounts) > 0


class TestSecurityCategoriesAPI:
    def test_list(self):
        res = COGNITE_CLIENT.iam.security_categories.list()
        assert len(res) > 0

    def test_create_and_delete(self):
        random_name = "test_" + random_string(10)
        res = COGNITE_CLIENT.iam.security_categories.create(SecurityCategory(name=random_name))
        assert res.id in {s.id for s in COGNITE_CLIENT.iam.security_categories.list()}
        COGNITE_CLIENT.iam.security_categories.delete(res.id)
        assert res.id not in {s.id for s in COGNITE_CLIENT.iam.security_categories.list()}
