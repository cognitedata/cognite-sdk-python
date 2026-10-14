from __future__ import annotations

import pytest
from pytest_httpx2 import HTTPXMock

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import Token


@pytest.mark.parametrize(
    "project, organization, expected_auth_url",
    [
        ("dev-project", "cog-dev-foo", "https://auth-dev.cognitedata-development.cognite.ai"),
        ("staging-project", "cog-staging-foo", "https://auth-staging.gc-bru-001.cogheim.net"),
        ("prod-project", "foo", "https://auth.cognite.com"),
    ],
)
@pytest.mark.allow_no_semaphore("_organization looks up the project's org via an internal request without a semaphore")
def test_principals_api_uses_auth_url_matching_org_prefix(
    httpx2_mock: HTTPXMock, project: str, organization: str, expected_auth_url: str
) -> None:
    config = ClientConfig(client_name="any", project=project, cluster="api", credentials=Token("bla"))
    httpx2_mock.add_response(
        method="GET",
        url=f"https://api.cognitedata.com/api/v1/projects/{project}",
        json={"organization": organization},
        is_optional=False,
        is_reusable=False,
    )
    httpx2_mock.add_response(
        method="GET",
        url=f"{expected_auth_url}/api/v1/principals/me",
        json={"id": "1", "type": "USER", "name": "n", "pictureUrl": ""},
        is_optional=False,
        is_reusable=False,
    )
    httpx2_mock.add_response(
        method="GET",
        url=f"{expected_auth_url}/api/v1/orgs/{organization}/principals?limit=25",
        json={"items": []},
        is_optional=False,
        is_reusable=False,
    )
    client = CogniteClient(config)

    # The mocked URLs are the assertion: httpx2_mock fails on unmatched requests and on unrequested responses.
    # Using the sync client also guards against the org lookup deadlocking on the SDK's event loop thread:
    client.iam.principals.list()
    client.iam.principals.me()
