import re

import pytest

from cognite.client.data_classes import Group, GroupList, SecurityCategory, SecurityCategoryList
from cognite.client.data_classes.capabilities import AllScope, GroupsAcl, ProjectCapability, ProjectCapabilityList
from cognite.client.data_classes.iam import GroupAttributes, ProjectSpec, TokenInspection
from tests.utils import get_url, jsgz_load


@pytest.fixture
def mock_groups_response(httpx_mock, cognite_client):
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
    url_pattern = re.compile(re.escape(get_url(cognite_client.iam)) + "/groups.*")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield response_body


@pytest.fixture
def group_with_attributes():
     return {
        "name": "Production Engineers",
        "sourceId": "b7c9a5a4-99c2-4785-bed3-5e6ad9a78603",
        "capabilities": [{"groupsAcl": {"actions": ["LIST"], "scope": {"all": {}}}}],
        "id": 0,
        "isDeleted": False,
        "deletedTime": 0,
        "attributes": {
            "token": {
                "appIds": ["app1", "app2"],
            },
            "unknownProperty": "unknownValue",
        },
    }


@pytest.fixture
def mock_groups_with_attributes(group_with_attributes, httpx_mock, cognite_client):
    response_body = {"items": [group_with_attributes]}

    url_pattern = re.compile(re.escape(get_url(cognite_client.iam)) + "/groups.*")
    httpx_mock.add_response(
        method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True
    )
    httpx_mock.add_response(
        method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True
    )
    yield httpx_mock


class TestGroups:
    def test_list(self, cognite_client, mock_groups_response):
        res = cognite_client.iam.groups.list()
        assert isinstance(res, GroupList)
        assert mock_groups_response["items"] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_groups_with_attributes")
    def test_list_groups_with_attributes(self, cognite_client, group_with_attributes):
        res = cognite_client.iam.groups.list()
        assert isinstance(res, GroupList)
        assert res.dump(camel_case=True) == [group_with_attributes]

    def test_create(self, cognite_client, mock_groups_response, httpx_mock):
        my_group = Group(name="My Group", capabilities=[GroupsAcl([GroupsAcl.Action.List], AllScope())])
        res = cognite_client.iam.groups.create(my_group)
        assert isinstance(res, Group)
        assert {
            "items": [
                {"name": "My Group", "capabilities": [{"groupsAcl": {"actions": ["LIST"], "scope": {"all": {}}}}]}
            ]
        } == jsgz_load(httpx_mock.get_requests()[0].content)
        assert mock_groups_response["items"][0] == res.dump(camel_case=True)

    def test_create_with_attributes(self, cognite_client, mock_groups_with_attributes):
        # Construct attributes via loader to include unknown properties for pass-through
        attributes = GroupAttributes.load(
            {
                "token": {"appIds": ["app1", "app2"]},
                "unknownProperty": "unknownValue",
            }
        )
        my_group = Group(
            name="My Group",
            capabilities=[GroupsAcl([GroupsAcl.Action.List], AllScope())],
            attributes=attributes,
        )
        res = cognite_client.iam.groups.create(my_group)
        assert isinstance(res, Group)
        expected = [
            {
                "name": "My Group",
                "capabilities": [{"groupsAcl": {"actions": ["LIST"], "scope": {"all": {}}}}],
                "attributes": {"token": {"appIds": ["app1", "app2"]}, "unknownProperty": "unknownValue"},
            }
        ]
        assert expected == jsgz_load(mock_groups_with_attributes.get_requests()[0].content)["items"]

    def test_create_multiple(self, cognite_client, mock_groups_response, httpx_mock):
        res = cognite_client.iam.groups.create([1])
        assert isinstance(res, GroupList)
        assert {"items": [1]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert mock_groups_response["items"] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_groups_response")
    def test_delete(self, cognite_client, httpx_mock):
        res = cognite_client.iam.groups.delete(1)
        assert {"items": [1]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert res is None

    @pytest.mark.usefixtures("mock_groups_response")
    def test_delete_multiple(self, cognite_client, httpx_mock):
        res = cognite_client.iam.groups.delete([1])
        assert {"items": [1]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert res is None


@pytest.fixture
def mock_security_cats_response(httpx_mock, cognite_client):
    response_body = {"items": [{"name": "bla", "id": 1}]}
    url_pattern = re.compile(re.escape(get_url(cognite_client.iam)) + "/securitycategories.*")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield response_body


class TestSecurityCategories:
    def test_list(self, cognite_client, mock_security_cats_response):
        res = cognite_client.iam.security_categories.list()
        assert isinstance(res, SecurityCategoryList)
        assert mock_security_cats_response["items"] == res.dump(camel_case=True)

    def test_create(self, cognite_client, mock_security_cats_response, httpx_mock):
        res = cognite_client.iam.security_categories.create(SecurityCategory(name="My Category"))
        assert isinstance(res, SecurityCategory)
        assert {"items": [{"name": "My Category"}]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert mock_security_cats_response["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_security_cats_response, httpx_mock):
        res = cognite_client.iam.security_categories.create([1])
        assert isinstance(res, SecurityCategoryList)
        assert {"items": [1]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert mock_security_cats_response["items"] == res.dump(camel_case=True)

    def test_delete(self, cognite_client, mock_security_cats_response, httpx_mock):
        res = cognite_client.iam.security_categories.delete(1)
        assert {"items": [1]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_security_cats_response, httpx_mock):
        res = cognite_client.iam.security_categories.delete([1])
        assert {"items": [1]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert res is None


@pytest.fixture
def mock_token_inspect(httpx_mock, cognite_client):
    response_body = {
        "subject": "someSubject",
        "projects": [{"projectUrlName": "veryGoodUrlName", "groups": [1, 2, 3]}],
        "capabilities": [
            {"groupsAcl": {"actions": ["LIST"], "scope": {"all": {}}}, "projectScope": {"allProjects": {}}}
        ],
    }
    url_pattern = re.compile(re.escape(get_url(cognite_client.iam.token)) + "/api/v1/token/inspect")
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body)
    yield httpx_mock


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
            "projects": [{"project_url_name": "urlName", "groups": groups}],
            "capabilities": capabilities.dump(camel_case=False),
        }
        assert obj.dump(camel_case=True) == {
            "subject": "subject",
            "projects": [{"projectUrlName": "urlName", "groups": groups}],
            "capabilities": capabilities.dump(camel_case=True),
        }
