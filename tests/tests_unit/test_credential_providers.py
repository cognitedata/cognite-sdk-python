from typing import ClassVar
from unittest.mock import Mock, patch

import pytest
from oauthlib.oauth2 import InvalidClientIdError

from cognite.client.credentials import (
    CredentialProvider,
    OAuthClientCertificate,
    OAuthClientCredentials,
    Token,
)
from cognite.client.exceptions import CogniteAuthError


class TestCredentialProvider:
    INVALID_CREDENTIAL_ERROR = "Invalid credential provider type provided, the valid options are:"
    INVALID_INPUT_LENGTH_ERROR = "Credential provider configuration must be a dictionary containing exactly one of the following supported types as the top level key:"
    INVALID_INPUT_TYPE_ERROR = "Resource must be json or yaml str, or dict, not"

    @pytest.mark.parametrize(
        "config,error_type,error_message",
        [
            pytest.param({"foo": "abc"}, ValueError, INVALID_CREDENTIAL_ERROR, id="Invalid input: credential type"),
            pytest.param("token", TypeError, INVALID_INPUT_TYPE_ERROR, id="Invalid input: not a dict, str"),
            pytest.param({}, ValueError, INVALID_INPUT_LENGTH_ERROR, id="Invalid input: empty dict"),
            pytest.param(
                {"token": "abc", "client_credentials": {"client_id": "abc"}},
                ValueError,
                INVALID_INPUT_LENGTH_ERROR,
                id="Invalid input: multiple keys",
            ),
        ],
    )
    def test_invalid_not_dict(self, config, error_type, error_message):
        with pytest.raises(error_type, match=error_message):
            CredentialProvider.load(config)


class TestToken:
    def test_token_auth_header(self):
        creds = Token("abc")
        assert "Authorization", "Bearer abc" == creds.authorization_header()

    def test_token_factory_auth_header(self):
        creds = Token(lambda: "abc")
        assert "Authorization", "Bearer abc" == creds.authorization_header()

    def test_token_non_string(self):
        with pytest.raises(
            TypeError, match=r"'token' must be a string or a no-argument-callable returning a string, not .*"
        ):
            Token({"foo": "bar"})

    @pytest.mark.parametrize(
        "config",
        [
            {"token": "abc"},
            {"token": {"token": "abc"}},
            '{"token": "abc"}',
            '{"token": {"token": "abc"}}',
            {"token": (lambda: "abc")},
            {"token": {"token": (lambda: "abc")}},
        ],
    )
    def test_create_from_credential_provider(self, config):
        creds = CredentialProvider.load(config)
        assert isinstance(creds, Token)
        assert "Authorization", "Bearer abc" == creds.authorization_header()


class TestOauthClientCredentials:
    DEFAULT_PROVIDER_ARGS: ClassVar = {
        "client_id": "azure-client-id",
        "client_secret": "azure-client-secret",
        "token_url": "https://login.microsoftonline.com/testingabc123/oauth2/v2.0/token",
        "scopes": ["https://greenfield.cognitedata.com/.default"],
        "other_custom_arg": "some_value",
    }

    @patch("cognite.client.credentials.BackendApplicationClient")
    @patch("cognite.client.credentials.OAuth2Session")
    @pytest.mark.parametrize("expires_in", (1000, "1001"))  # some IDPs return as string
    def test_access_token_generated(self, mock_oauth_session, mock_backend_client, expires_in):
        mock_backend_client().return_value = Mock()
        mock_oauth_session().fetch_token.return_value = {"access_token": "azure_token", "expires_in": expires_in}
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.BackendApplicationClient")
    @patch("cognite.client.credentials.OAuth2Session")
    def test_access_token_not_generated_due_to_error(self, mock_oauth_session, mock_backend_client):
        mock_backend_client().return_value = Mock()
        mock_oauth_session().fetch_token.side_effect = InvalidClientIdError()
        with pytest.raises(
            CogniteAuthError,
            match="Error generating access token: invalid_request, 400, Invalid client_id parameter value.",
        ):
            creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
            creds._refresh_access_token()

    @patch("cognite.client.credentials.BackendApplicationClient")
    @patch("cognite.client.credentials.OAuth2Session")
    def test_access_token_expired(self, mock_oauth_session, mock_backend_client):
        mock_backend_client().return_value = Mock()
        mock_oauth_session().fetch_token.side_effect = [
            {"access_token": "azure_token_expired", "expires_in": -1000},
            {"access_token": "azure_token_refreshed", "expires_in": 1000},
        ]
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        assert "Authorization", "Bearer azure_token_expired" == creds.authorization_header()
        assert "Authorization", "Bearer azure_token_refreshed" == creds.authorization_header()

    def test_create_from_credential_provider(self):
        creds = CredentialProvider.load({"client_credentials": self.DEFAULT_PROVIDER_ARGS})
        assert isinstance(creds, OAuthClientCredentials)
        assert creds.client_id == "azure-client-id"
        assert creds.client_secret == "azure-client-secret"
        assert creds.token_url == "https://login.microsoftonline.com/testingabc123/oauth2/v2.0/token"
        assert creds.scopes == ["https://greenfield.cognitedata.com/.default"]
        assert creds.token_custom_args == {"other_custom_arg": "some_value"}


class TestOAuthClientCertificate:
    DEFAULT_PROVIDER_ARGS: ClassVar = {
        "authority_url": "https://login.microsoftonline.com/xyz",
        "client_id": "azure-client-id",
        "cert_thumbprint": "XYZ123",
        "certificate": "certificatecontents123",
        "scopes": ["https://greenfield.cognitedata.com/.default"],
    }

    @patch("cognite.client.credentials.ConfidentialClientApplication")
    def test_access_token_generated(self, mock_msal_app):
        mock_msal_app().acquire_token_for_client.return_value = {
            "access_token": "azure_token",
            "expires_in": 1000,
        }
        creds = OAuthClientCertificate(**self.DEFAULT_PROVIDER_ARGS)
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.ConfidentialClientApplication")
    def test_create_from_credential_provider(self, mock_msal_app):
        mock_msal_app().acquire_token_for_client.return_value = {
            "access_token": "azure_token",
            "expires_in": 1000,
        }
        creds = CredentialProvider.load({"client_certificate": self.DEFAULT_PROVIDER_ARGS})
        assert isinstance(creds, OAuthClientCertificate)
        assert creds.authority_url == "https://login.microsoftonline.com/xyz"
        assert creds.client_id == "azure-client-id"
        assert creds.cert_thumbprint == "XYZ123"
        assert creds.certificate == "certificatecontents123"
        assert creds.scopes == ["https://greenfield.cognitedata.com/.default"]
