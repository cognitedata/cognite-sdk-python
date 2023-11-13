import re

import pytest

from cognite.client.data_classes import Group, GroupList, SecurityCategory, SecurityCategoryList
from cognite.client.data_classes.capabilities import AllScope, GroupsAcl, ProjectCapability, ProjectCapabilityList
from cognite.client.data_classes.iam import ProjectSpec, TokenInspection
from tests.utils import jsgz_load


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


class TestGroups:
    def test_list(self, cognite_client, mock_groups):
        res = cognite_client.iam.groups.list()
        assert isinstance(res, GroupList)
        assert mock_groups.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_create(self, cognite_client, mock_groups):
        my_group = Group(name="My Group", capabilities=[GroupsAcl([GroupsAcl.Action.List], AllScope())])
        res = cognite_client.iam.groups.create(my_group)
        assert isinstance(res, Group)
        assert {
            "items": [
                {"name": "My Group", "capabilities": [{"groupsAcl": {"actions": ["LIST"], "scope": {"all": {}}}}]}
            ]
        } == jsgz_load(mock_groups.calls[0].request.body)
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
        "capabilities": [
            {"groupsAcl": {"actions": ["LIST"], "scope": {"all": {}}}, "projectScope": {"allProjects": {}}}
        ],
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
        assert res.capabilities == ProjectCapabilityList(
            ProjectCapabilityList(
                [
                    ProjectCapability(
                        capability=GroupsAcl([GroupsAcl.Action.List], GroupsAcl.Scope.All()),
                        project_scope=ProjectCapability.Scope.All(),
                    )
                ]
            )
        )

    def test_token_inspection_dump(self):
        capabilities = ProjectCapabilityList(
            [
                ProjectCapability(
                    GroupsAcl([GroupsAcl.Action.List], GroupsAcl.Scope.All()), ProjectCapability.Scope.All()
                )
            ]
        )
        groups = [1, 2, 3]
        obj = TokenInspection("subject", [ProjectSpec("urlName", groups)], capabilities)

        assert obj.dump(camel_case=False) == {
            "subject": "subject",
            "projects": [{"url_name": "urlName", "groups": groups}],
            "capabilities": capabilities.dump(camel_case=False),
        }
        assert obj.dump(camel_case=True) == {
            "subject": "subject",
            "projects": [{"urlName": "urlName", "groups": groups}],
            "capabilities": capabilities.dump(camel_case=True),
        }
