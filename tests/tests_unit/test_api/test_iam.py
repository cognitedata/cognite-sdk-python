import re

import pytest

from cognite.client._api.iam import APIKeyList, GroupList, SecurityCategoryList, ServiceAccountList
from cognite.client.data_classes import APIKey, Group, SecurityCategory, ServiceAccount
from cognite.client.data_classes.iam import ProjectSpec, TokenInspection
from tests.utils import jsgz_load


@pytest.fixture
def mock_service_accounts(rsps, cognite_client):
    response_body = {
        "items": [{"name": "service@bla.com", "groups": [1, 2, 3], "id": 0, "isDeleted": False, "deletedTime": 0}]
    }
    url_pattern = re.compile(re.escape(cognite_client.iam._get_base_url_with_base_path()) + "/serviceaccounts.*")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestServiceAccounts:
    def test_list(self, cognite_client, mock_service_accounts):
        res = cognite_client.iam.service_accounts.list()
        assert isinstance(res, ServiceAccountList)
        assert mock_service_accounts.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_create(self, cognite_client, mock_service_accounts):
        res = cognite_client.iam.service_accounts.create(ServiceAccount(name="service@bla.com", groups=[1, 2, 3]))
        assert isinstance(res, ServiceAccount)
        assert {"items": [{"name": "service@bla.com", "groups": [1, 2, 3]}]} == jsgz_load(
            mock_service_accounts.calls[0].request.body
        )
        assert mock_service_accounts.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_service_accounts):
        res = cognite_client.iam.service_accounts.create([ServiceAccount(name="service@bla.com", groups=[1, 2, 3])])
        assert isinstance(res, ServiceAccountList)
        assert {"items": [{"name": "service@bla.com", "groups": [1, 2, 3]}]} == jsgz_load(
            mock_service_accounts.calls[0].request.body
        )
        assert mock_service_accounts.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_delete(self, cognite_client, mock_service_accounts):
        res = cognite_client.iam.service_accounts.delete(1)
        assert {"items": [1]} == jsgz_load(mock_service_accounts.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_service_accounts):
        res = cognite_client.iam.service_accounts.delete([1])
        assert {"items": [1]} == jsgz_load(mock_service_accounts.calls[0].request.body)
        assert res is None


@pytest.fixture
def mock_api_keys(rsps, cognite_client):
    response_body = {"items": [{"id": 1, "serviceAccountId": 1, "createdTime": 0, "status": "ACTIVE"}]}
    url_pattern = re.compile(re.escape(cognite_client.iam._get_base_url_with_base_path()) + "/apikeys.*")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestAPIKeys:
    def test_list(self, cognite_client, mock_api_keys):
        res = cognite_client.iam.api_keys.list()
        assert isinstance(res, APIKeyList)
        assert mock_api_keys.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_create(self, cognite_client, mock_api_keys):
        res = cognite_client.iam.api_keys.create(1)
        assert isinstance(res, APIKey)
        assert {"items": [{"serviceAccountId": 1}]} == jsgz_load(mock_api_keys.calls[0].request.body)
        assert mock_api_keys.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_api_keys):
        res = cognite_client.iam.api_keys.create([1])
        assert isinstance(res, APIKeyList)
        assert {"items": [{"serviceAccountId": 1}]} == jsgz_load(mock_api_keys.calls[0].request.body)
        assert mock_api_keys.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_delete(self, cognite_client, mock_api_keys):
        res = cognite_client.iam.api_keys.delete(1)
        assert {"items": [1]} == jsgz_load(mock_api_keys.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_api_keys):
        res = cognite_client.iam.api_keys.delete([1])
        assert {"items": [1]} == jsgz_load(mock_api_keys.calls[0].request.body)
        assert res is None


@pytest.fixture
def mock_groups(rsps, cognite_client):
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
    url_pattern = re.compile(re.escape(cognite_client.iam._get_base_url_with_base_path()) + "/groups.*")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_group_service_account_response(rsps, cognite_client):
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
    url_pattern = re.compile(
        re.escape(cognite_client.iam._get_base_url_with_base_path()) + "/groups/1/serviceaccounts.*"
    )
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_empty_response(rsps, cognite_client):
    response_body = {}
    url_pattern = re.compile(
        re.escape(cognite_client.iam._get_base_url_with_base_path()) + "/groups/1/serviceaccounts.*"
    )
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestGroups:
    def test_list(self, cognite_client, mock_groups):
        res = cognite_client.iam.groups.list()
        assert isinstance(res, GroupList)
        assert mock_groups.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_create(self, cognite_client, mock_groups):
        my_capabilities = [{"groupsAcl": {"actions": ["LIST"], "scope": {"all": {}}}}]
        my_group = Group(name="My Group", capabilities=my_capabilities)
        res = cognite_client.iam.groups.create(my_group)
        assert isinstance(res, Group)
        assert {"items": [{"name": "My Group", "capabilities": my_capabilities}]} == jsgz_load(
            mock_groups.calls[0].request.body
        )
        assert mock_groups.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_groups):
        res = cognite_client.iam.groups.create([1])
        assert isinstance(res, GroupList)
        assert {"items": [1]} == jsgz_load(mock_groups.calls[0].request.body)
        assert mock_groups.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_delete(self, cognite_client, mock_groups):
        res = cognite_client.iam.groups.delete(1)
        assert {"items": [1]} == jsgz_load(mock_groups.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_groups):
        res = cognite_client.iam.groups.delete([1])
        assert {"items": [1]} == jsgz_load(mock_groups.calls[0].request.body)
        assert res is None

    def test_list_service_accounts(self, cognite_client, mock_group_service_account_response):
        res = cognite_client.iam.groups.list_service_accounts(1)
        assert isinstance(res, ServiceAccountList)
        assert mock_group_service_account_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_add_service_account(self, cognite_client, mock_empty_response):
        res = cognite_client.iam.groups.add_service_account(1, 1)
        assert res is None
        assert {"items": [1]} == jsgz_load(mock_empty_response.calls[0].request.body)

    def test_add_service_account_multiple(self, cognite_client, mock_empty_response):
        res = cognite_client.iam.groups.add_service_account(1, [1])
        assert res is None
        assert {"items": [1]} == jsgz_load(mock_empty_response.calls[0].request.body)

    def test_remove_service_account(self, cognite_client, mock_empty_response):
        res = cognite_client.iam.groups.remove_service_account(1, 1)
        assert res is None
        assert {"items": [1]} == jsgz_load(mock_empty_response.calls[0].request.body)

    def test_remove_service_account_multiple(self, cognite_client, mock_empty_response):
        res = cognite_client.iam.groups.remove_service_account(1, [1])
        assert res is None
        assert {"items": [1]} == jsgz_load(mock_empty_response.calls[0].request.body)


@pytest.fixture
def mock_security_categories(rsps, cognite_client):
    response_body = {"items": [{"name": "bla", "id": 1}]}
    url_pattern = re.compile(re.escape(cognite_client.iam._get_base_url_with_base_path()) + "/securitycategories.*")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestSecurityCategories:
    def test_list(self, cognite_client, mock_security_categories):
        res = cognite_client.iam.security_categories.list()
        assert isinstance(res, SecurityCategoryList)
        assert mock_security_categories.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_create(self, cognite_client, mock_security_categories):
        res = cognite_client.iam.security_categories.create(SecurityCategory(name="My Category"))
        assert isinstance(res, SecurityCategory)
        assert {"items": [{"name": "My Category"}]} == jsgz_load(mock_security_categories.calls[0].request.body)
        assert mock_security_categories.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_security_categories):
        res = cognite_client.iam.security_categories.create([1])
        assert isinstance(res, SecurityCategoryList)
        assert {"items": [1]} == jsgz_load(mock_security_categories.calls[0].request.body)
        assert mock_security_categories.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_delete(self, cognite_client, mock_security_categories):
        res = cognite_client.iam.security_categories.delete(1)
        assert {"items": [1]} == jsgz_load(mock_security_categories.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_security_categories):
        res = cognite_client.iam.security_categories.delete([1])
        assert {"items": [1]} == jsgz_load(mock_security_categories.calls[0].request.body)
        assert res is None


@pytest.fixture
def mock_token_inspect(rsps, cognite_client):
    response_body = {
        "subject": "someSubject",
        "projects": [{"projectUrlName": "veryGoodUrlName", "groups": [1, 2, 3]}],
        "capabilities": [{"groupsAcl": {"actions": ["LIST"], "scope": {"all": {}}}}],
    }
    url_pattern = re.compile(
        re.escape(cognite_client.iam.token._get_base_url_with_base_path()) + "/api/v1/token/inspect"
    )
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestTokenAPI:
    def test_token_inspect(self, cognite_client, mock_token_inspect):
        res = cognite_client.iam.token.inspect()
        assert isinstance(res, TokenInspection)
        assert res.subject == "someSubject"
        assert res.projects == [ProjectSpec(url_name="veryGoodUrlName", groups=[1, 2, 3])]
        assert res.capabilities == [{"groupsAcl": {"actions": ["LIST"], "scope": {"all": {}}}}]

    def test_token_inspection_dump(self):
        capabilities = [{"groupsAcl": {"actions": ["LIST"], "scope": {"all": {}}}}]
        groups = [1, 2, 3]
        obj = TokenInspection("subject", [ProjectSpec("urlName", groups)], capabilities)

        assert obj.dump() == {
            "subject": "subject",
            "projects": [{"url_name": "urlName", "groups": groups}],
            "capabilities": capabilities,
        }
        assert obj.dump(camel_case=True) == {
            "subject": "subject",
            "projects": [{"urlName": "urlName", "groups": groups}],
            "capabilities": capabilities,
        }
