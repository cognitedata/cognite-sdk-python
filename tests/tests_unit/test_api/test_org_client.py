from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from pytest_httpx2 import HTTPXMock

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import Token


@pytest.mark.parametrize(
    "project, organization, expected_auth_url",
    [
        ("dev-project-123", "cog-dev-foooooo", "https://auth-dev.cognitedata-development.cognite.ai"),
        ("staging-project", "cog-staging-foo", "https://auth-staging.gc-bru-001.cogheim.net"),
        ("prod-project-12", "foooooooooooooo", "https://auth.cognite.com"),
    ],
)
@pytest.mark.parametrize(
    "http_method, path_template, response, call",
    [
        pytest.param(
            "GET",
            "/api/v1/orgs/{org}/principals?limit=25",
            {"items": []},
            lambda client: client.iam.principals.list(),
            id="list",
        ),
        pytest.param(
            "GET",
            "/api/v1/principals/me",
            {"id": "1", "type": "USER", "name": "n", "pictureUrl": ""},
            lambda client: client.iam.principals.me(),
            id="me",
        ),
        pytest.param(
            "POST",
            "/api/v1/orgs/{org}/principals/byids",
            {"items": []},
            lambda client: client.iam.principals.retrieve(id="some-principal-id"),
            id="retrieve",
        ),
    ],
)
@pytest.mark.allow_no_semaphore("_organization looks up the project's org via an internal request without a semaphore")
def test_principals_api_resolves_org_and_uses_matching_auth_url(
    httpx2_mock: HTTPXMock,
    project: str,
    organization: str,
    expected_auth_url: str,
    http_method: str,
    path_template: str,
    response: dict[str, Any],
    call: Callable[[CogniteClient], Any],
) -> None:
    client = CogniteClient(ClientConfig(client_name="any", project=project, cluster="api", credentials=Token("bla")))
    async_client = client.get_async_client()

    httpx2_mock.add_response(
        method="GET",
        url=f"https://api.cognitedata.com/api/v1/projects/{project}",
        json={"organization": organization},
        is_optional=False,
        is_reusable=False,
    )
    httpx2_mock.add_response(
        method=http_method,
        url=expected_auth_url + path_template.format(org=organization),
        json=response,
        is_optional=False,
        is_reusable=False,
    )
    # A fresh client per case means the organization is unresolved, so each method must resolve it itself.
    # The mocked URLs are the assertion: httpx2_mock fails on unmatched requests and on unrequested responses.
    # Using the sync client also guards against the org lookup deadlocking on the SDK's event loop thread:
    assert async_client.iam.principals._resolved_organization is None
    call(client)
    assert async_client.iam.principals._resolved_organization == organization
