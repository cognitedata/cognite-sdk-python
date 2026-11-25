from __future__ import annotations

from typing import Any

import pytest
from pytest_httpx2 import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes.projects import Project, ProjectList
from cognite.client.data_classes.user_profiles import UserProfilesConfiguration
from cognite.client.utils._experimental import FeaturePreviewWarning

PROJECTS_URL = "https://api.cognitedata.com/api/v1/projects"


PROJECT_RESPONSE: dict[str, Any] = {
    "name": "My Project",
    "urlName": "dummy",
    "organization": "my-org",
    "oidcConfiguration": {"jwksUrl": "https://jwks", "skewMs": 0},
    "userProfilesConfiguration": {"enabled": True},
}


class TestProjectsAPI:
    def test_current(self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock) -> None:
        httpx2_mock.add_response(method="GET", url=f"{PROJECTS_URL}/dummy", json=PROJECT_RESPONSE)

        with pytest.warns(FeaturePreviewWarning, match="Projects API is using an internal, undocumented API"):
            res = cognite_client.projects.current()

        assert isinstance(res, Project)
        assert res.url_name == "dummy"
        assert res.organization == "my-org"
        assert isinstance(res.user_profiles_configuration, UserProfilesConfiguration)
        assert res.user_profiles_configuration.enabled is True
        # The OIDC configuration is passed through as-is:
        assert res.dump(camel_case=True) == PROJECT_RESPONSE
        assert res.dump(camel_case=False)["oidc_configuration"] == {"jwksUrl": "https://jwks", "skewMs": 0}
        assert res.dump(camel_case=False)["user_profiles_configuration"] == {"enabled": True}

    def test_list_requests_organization(self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock) -> None:
        httpx2_mock.add_response(
            method="GET",
            url=f"{PROJECTS_URL}?withOrganization=true",
            json={"items": [{"urlName": "p1", "organization": "org-1"}, {"urlName": "p2", "organization": "org-2"}]},
        )

        with pytest.warns(FeaturePreviewWarning):
            res = cognite_client.projects.list()

        assert isinstance(res, ProjectList)
        assert {p.url_name: p.organization for p in res} == {"p1": "org-1", "p2": "org-2"}


@pytest.mark.parametrize(
    "call, url, response",
    [
        pytest.param(
            lambda c: c.projects.current(),
            f"{PROJECTS_URL}/dummy",
            {k: v for k, v in PROJECT_RESPONSE.items() if k != "organization"},
            id="current",
        ),
        pytest.param(
            lambda c: c.projects.list(),
            f"{PROJECTS_URL}?withOrganization=true",
            {"items": [{"urlName": "dummy"}]},
            id="list",
        ),
    ],
)
def test_missing_organization_warns_and_uses_empty_string(
    cognite_client: CogniteClient, httpx2_mock: HTTPXMock, call: Any, url: str, response: dict[str, Any]
) -> None:
    httpx2_mock.add_response(method="GET", url=url, json=response)

    with pytest.warns(FeaturePreviewWarning), pytest.warns(UserWarning, match=r"'dummy'.*internal"):
        res = call(cognite_client)

    projects = res if isinstance(res, ProjectList) else [res]
    assert [(p.url_name, p.organization) for p in projects] == [("dummy", "")]
