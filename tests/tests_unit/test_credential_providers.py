from typing import ClassVar
from unittest.mock import Mock, patch

import pytest
from oauthlib.oauth2 import InvalidClientIdError

from cognite.client.credentials import (
    OAuthClientCertificate,
    OAuthClientCredentials,
    Token,
)
from cognite.client.exceptions import CogniteAuthError


class TestToken:
    def test_token_auth_header(self):
        creds = Token("abc")
        assert "Authorization", "Bearer abc" == creds.authorization_header()

    def test_token_factory_auth_header(self):
        creds = Token(lambda: "abc")
        assert "Authorization", "Bearer abc" == creds.authorization_header()


class TestOauthClientCredentials:
    DEFAULT_PROVIDER_ARGS: ClassVar = {
        "client_id": "azure-client-id",
        "client_secret": "azure-client-secret",
        "token_url": "https://login.microsoftonline.com/testingabc123/oauth2/v2.0/token",
        "scopes": ["https://greenfield.cognitedata.com/.default"],
    }

    @patch("cognite.client.credentials.BackendApplicationClient")
    @patch("cognite.client.credentials.OAuth2Session")
    def test_access_token_generated(self, mock_oauth_session, mock_backend_client):
        mock_backend_client().return_value = Mock()
        mock_oauth_session().fetch_token.return_value = {"access_token": "azure_token", "expires_in": 1000}
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
