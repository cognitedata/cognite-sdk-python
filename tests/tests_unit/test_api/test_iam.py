import re

import pytest

from cognite.client import CogniteClient
from cognite.client._api.iam import APIKeyList, GroupList, SecurityCategoryList, ServiceAccountList
from cognite.client.data_classes import APIKey, Group, SecurityCategory, ServiceAccount
from tests.utils import jsgz_load

IAM_API = CogniteClient().iam


@pytest.fixture
def mock_service_accounts(rsps):
    response_body = {
        "items": [{"name": "service@bla.com", "groups": [1, 2, 3], "id": 0, "isDeleted": False, "deletedTime": 0}]
    }
    url_pattern = re.compile(re.escape(IAM_API._get_base_url_with_base_path()) + "/serviceaccounts.*")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestServiceAccounts:
    def test_list(self, mock_service_accounts):
        res = IAM_API.service_accounts.list()
        assert isinstance(res, ServiceAccountList)
        assert mock_service_accounts.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_create(self, mock_service_accounts):
        res = IAM_API.service_accounts.create(ServiceAccount(name="service@bla.com", groups=[1, 2, 3]))
        assert isinstance(res, ServiceAccount)
        assert {"items": [{"name": "service@bla.com", "groups": [1, 2, 3]}]} == jsgz_load(
            mock_service_accounts.calls[0].request.body
        )
        assert mock_service_accounts.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_service_accounts):
        res = IAM_API.service_accounts.create([ServiceAccount(name="service@bla.com", groups=[1, 2, 3])])
        assert isinstance(res, ServiceAccountList)
        assert {"items": [{"name": "service@bla.com", "groups": [1, 2, 3]}]} == jsgz_load(
            mock_service_accounts.calls[0].request.body
        )
        assert mock_service_accounts.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_delete(self, mock_service_accounts):
        res = IAM_API.service_accounts.delete(1)
        assert {"items": [1]} == jsgz_load(mock_service_accounts.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, mock_service_accounts):
        res = IAM_API.service_accounts.delete([1])
        assert {"items": [1]} == jsgz_load(mock_service_accounts.calls[0].request.body)
        assert res is None


@pytest.fixture
def mock_api_keys(rsps):
    response_body = {"items": [{"id": 1, "serviceAccountId": 1, "createdTime": 0, "status": "ACTIVE"}]}
    url_pattern = re.compile(re.escape(IAM_API._get_base_url_with_base_path()) + "/apikeys.*")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestAPIKeys:
    def test_list(self, mock_api_keys):
        res = IAM_API.api_keys.list()
        assert isinstance(res, APIKeyList)
        assert mock_api_keys.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_create(self, mock_api_keys):
        res = IAM_API.api_keys.create(1)
        assert isinstance(res, APIKey)
        assert {"items": [{"serviceAccountId": 1}]} == jsgz_load(mock_api_keys.calls[0].request.body)
        assert mock_api_keys.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_api_keys):
        res = IAM_API.api_keys.create([1])
        assert isinstance(res, APIKeyList)
        assert {"items": [{"serviceAccountId": 1}]} == jsgz_load(mock_api_keys.calls[0].request.body)
        assert mock_api_keys.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_delete(self, mock_api_keys):
        res = IAM_API.api_keys.delete(1)
        assert {"items": [1]} == jsgz_load(mock_api_keys.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, mock_api_keys):
        res = IAM_API.api_keys.delete([1])
        assert {"items": [1]} == jsgz_load(mock_api_keys.calls[0].request.body)
        assert res is None


@pytest.fixture
def mock_groups(rsps):
    response_body = {
        "items": [
            {
                "name": "Production Engineers",
                "sourceId": "b7c9a5a4-99c2-4785-bed3-5e6ad9a78603",
                "capabilities": [{"groupsAcl": {"actions": ["LIST"], "scope": {"all": {}}}}],
                "id": 0,
                "isDeleted": False,
                "deletedTime": 0,
            }
        ]
    }
    url_pattern = re.compile(re.escape(IAM_API._get_base_url_with_base_path()) + "/groups.*")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_group_service_account_response(rsps):
    response_body = {
        "items": [
            {
                "name": "some-internal-service@apple.com",
                "groups": [1, 2, 3],
                "id": 0,
                "isDeleted": False,
                "deletedTime": 0,
            }
        ]
    }
    url_pattern = re.compile(re.escape(IAM_API._get_base_url_with_base_path()) + "/groups/1/serviceaccounts.*")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestGroups:
    def test_list(self, mock_groups):
        res = IAM_API.groups.list()
        assert isinstance(res, GroupList)
        assert mock_groups.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_create(self, mock_groups):
        res = IAM_API.groups.create(1)
        assert isinstance(res, Group)
        assert {"items": [1]} == jsgz_load(mock_groups.calls[0].request.body)
        assert mock_groups.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_groups):
        res = IAM_API.groups.create([1])
        assert isinstance(res, GroupList)
        assert {"items": [1]} == jsgz_load(mock_groups.calls[0].request.body)
        assert mock_groups.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_delete(self, mock_groups):
        res = IAM_API.groups.delete(1)
        assert {"items": [1]} == jsgz_load(mock_groups.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, mock_groups):
        res = IAM_API.groups.delete([1])
        assert {"items": [1]} == jsgz_load(mock_groups.calls[0].request.body)
        assert res is None

    def test_list_service_accounts(self, mock_group_service_account_response):
        res = IAM_API.groups.list_service_accounts(1)
        assert isinstance(res, ServiceAccountList)
        assert mock_group_service_account_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_add_service_account(self, mock_group_service_account_response):
        res = IAM_API.groups.add_service_account(1, 1)
        assert res is None
        assert {"items": [1]} == jsgz_load(mock_group_service_account_response.calls[0].request.body)

    def test_add_service_account_multiple(self, mock_group_service_account_response):
        res = IAM_API.groups.add_service_account(1, [1])
        assert res is None
        assert {"items": [1]} == jsgz_load(mock_group_service_account_response.calls[0].request.body)

    def test_remove_service_account(self, mock_group_service_account_response):
        res = IAM_API.groups.remove_service_account(1, 1)
        assert res is None
        assert {"items": [1]} == jsgz_load(mock_group_service_account_response.calls[0].request.body)

    def test_remove_service_account_multiple(self, mock_group_service_account_response):
        res = IAM_API.groups.remove_service_account(1, [1])
        assert res is None
        assert {"items": [1]} == jsgz_load(mock_group_service_account_response.calls[0].request.body)


@pytest.fixture
def mock_security_categories(rsps):
    response_body = {"items": [{"name": "bla", "id": 1}]}
    url_pattern = re.compile(re.escape(IAM_API._get_base_url_with_base_path()) + "/securitycategories.*")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestSecurityCategories:
    def test_list(self, mock_security_categories):
        res = IAM_API.security_categories.list()
        assert isinstance(res, SecurityCategoryList)
        assert mock_security_categories.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_create(self, mock_security_categories):
        res = IAM_API.security_categories.create(1)
        assert isinstance(res, SecurityCategory)
        assert {"items": [1]} == jsgz_load(mock_security_categories.calls[0].request.body)
        assert mock_security_categories.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_security_categories):
        res = IAM_API.security_categories.create([1])
        assert isinstance(res, SecurityCategoryList)
        assert {"items": [1]} == jsgz_load(mock_security_categories.calls[0].request.body)
        assert mock_security_categories.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_delete(self, mock_security_categories):
        res = IAM_API.security_categories.delete(1)
        assert {"items": [1]} == jsgz_load(mock_security_categories.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, mock_security_categories):
        res = IAM_API.security_categories.delete([1])
        assert {"items": [1]} == jsgz_load(mock_security_categories.calls[0].request.body)
        assert res is None
