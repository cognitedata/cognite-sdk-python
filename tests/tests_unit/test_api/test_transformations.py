from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest
from pytest_httpx2 import HTTPXMock

from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.iam import ClientCredentials
from cognite.client.data_classes.transformations.common import OidcCredentials
from tests.utils import get_url, jsgz_load

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient, CogniteClient


@pytest.fixture
def oidc_credentials() -> OidcCredentials:
    return OidcCredentials(
        client_id="id", client_secret="secret", scopes=["impersonation"], token_uri="url", cdf_project_name="xyz"
    )


@pytest.mark.parametrize("scopes", ("comma,separated,scopes", ["comma", "separated", "scopes"]))
def test_oidc_credentials(scopes: list[str] | str) -> None:
    oidc_credentials = OidcCredentials(
        client_id="id", client_secret="secret", scopes=scopes, token_uri="url", cdf_project_name="zyx"
    )
    assert oidc_credentials.scopes == "comma,separated,scopes"


def test_oidc_credentials_no_scope() -> None:
    no_scopes = dict(
        clientId="the-id", clientSecret="the-secret", tokenUri="https://the-token-uri", cdfProjectName="my-project"
    )
    oidc_credentials = OidcCredentials.load(no_scopes)

    assert oidc_credentials.scopes is None
    with pytest.raises(ValueError) as e:
        oidc_credentials.as_credential_provider()
    assert isinstance(e.value, ValueError)
    assert str(e.value) == "Scopes must be provided to create OAuthClientCredentials"


def test_oidc_credentials_as_credential_provider(oidc_credentials: OidcCredentials) -> None:
    client_creds = oidc_credentials.as_credential_provider()

    assert isinstance(client_creds, OAuthClientCredentials)
    assert client_creds.token_url == oidc_credentials.token_uri
    assert client_creds.scopes == [oidc_credentials.scopes]
    assert client_creds.token_custom_args["audience"] is oidc_credentials.audience is None


def test_oidc_credentials_as_client_credentials(oidc_credentials: OidcCredentials) -> None:
    client_creds = oidc_credentials.as_client_credentials()

    assert isinstance(client_creds, ClientCredentials)
    assert client_creds == ClientCredentials("id", "secret")


@pytest.fixture
def mock_transformations_list_response(httpx2_mock: HTTPXMock, async_client: AsyncCogniteClient) -> HTTPXMock:
    url_pattern = re.compile(re.escape(get_url(async_client.transformations)) + r"/transformations/filter(?:\?.+)?$")
    httpx2_mock.add_response(method="POST", url=url_pattern, status_code=200, json={"items": []})
    return httpx2_mock


@pytest.mark.parametrize(
    "data_domain_external_ids, expected",
    [
        pytest.param(None, None, id="omitted when not given"),
        pytest.param([], None, id="omitted when empty list given"),
        pytest.param("my-domain", ["my-domain"], id="single string is wrapped in list"),
        pytest.param(["domain-a", "domain-b"], ["domain-a", "domain-b"], id="list of strings passed through"),
    ],
)
def test_list_data_domain_external_ids_filter(
    cognite_client: CogniteClient,
    mock_transformations_list_response: HTTPXMock,
    data_domain_external_ids: str | list[str] | None,
    expected: list[str] | None,
) -> None:
    cognite_client.transformations.list(data_domain_external_ids=data_domain_external_ids)

    sent_filter = jsgz_load(mock_transformations_list_response.get_requests()[0].content)["filter"]
    assert sent_filter.get("dataDomainExternalIds") == expected
