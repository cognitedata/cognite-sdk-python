import pytest

from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.iam import ClientCredentials
from cognite.client.data_classes.transformations.common import OidcCredentials


@pytest.fixture
def oidc_credentials():
    return OidcCredentials(
        client_id="id", client_secret="secret", scopes=["impersonation"], token_uri="url", cdf_project_name="xyz"
    )


@pytest.mark.parametrize("scopes", ("comma,separated,scopes", ["comma", "separated", "scopes"]))
def test_oidc_credentials(scopes):
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


def test_oidc_credentials_as_credential_provider(oidc_credentials):
    client_creds = oidc_credentials.as_credential_provider()

    assert isinstance(client_creds, OAuthClientCredentials)
    assert client_creds.token_url == oidc_credentials.token_uri
    assert client_creds.scopes == [oidc_credentials.scopes]
    assert client_creds.token_custom_args["audience"] is oidc_credentials.audience is None


def test_oidc_credentials_as_client_credentials(oidc_credentials):
    client_creds = oidc_credentials.as_client_credentials()

    assert isinstance(client_creds, ClientCredentials)
    assert client_creds == ClientCredentials("id", "secret")
