from types import MappingProxyType
from typing import ClassVar
from unittest.mock import MagicMock, Mock, patch

import pytest
from authlib.integrations.httpx_client import OAuthError

from cognite.client.credentials import (
    CredentialProvider,
    OAuthClientCertificate,
    OAuthClientCredentials,
    OAuthDeviceCode,
    OAuthInteractive,
    Token,
)
from cognite.client.exceptions import CogniteAuthError


class TestCredentialProvider:
    INVALID_CREDENTIAL_ERROR = "Invalid credential provider type given, the valid options are:"
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
    def test_invalid_not_dict(self, config: dict, error_type: type[Exception], error_message: str) -> None:
        with pytest.raises(error_type, match=error_message):
            CredentialProvider.load(config)


class TestToken:
    def test_token_auth_header(self) -> None:
        creds = Token("abc")
        assert "Authorization", "Bearer abc" == creds.authorization_header()

    def test_token_factory_auth_header(self) -> None:
        creds = Token(lambda: "abc")
        assert "Authorization", "Bearer abc" == creds.authorization_header()

    def test_token_non_string(self) -> None:
        with pytest.raises(
            TypeError, match=r"'token' must be a string or a no-argument-callable returning a string, not"
        ):
            Token({"foo": "bar"})  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "config",
        [
            {"token": "abc"},
            '{"token": "abc"}',
            {"token": (lambda: "abc")},
        ],
    )
    def test_load(self, config: dict) -> None:
        creds = Token.load(config)
        assert isinstance(creds, Token)
        assert "Authorization", "Bearer abc" == creds.authorization_header()

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
    def test_create_from_credential_provider(self, config: dict) -> None:
        creds = CredentialProvider.load(config)
        assert isinstance(creds, Token)
        assert "Authorization", "Bearer abc" == creds.authorization_header()


class TestOAuthDeviceCode:
    DEFAULT_PROVIDER_ARGS: ClassVar = MappingProxyType(
        {
            "authority_url": "https://login.microsoftonline.com/xyz",
            "client_id": "azure-client-id",
            "scopes": ["https://greenfield.cognitedata.com/.default"],
        }
    )

    @patch("cognite.client.credentials.PublicClientApplication")
    @pytest.mark.parametrize("expires_in", (1000, "1001"))  # some IDPs return as string
    def test_access_token_generated(self, mock_public_client: MagicMock, expires_in: int | str) -> None:
        mock_response = Mock()
        mock_response.json.return_value = {
            "user_code": "ABCDEF",
            "message": "Follow the link and enter the code",
        }
        mock_public_client().http_client.post.return_value = mock_response
        mock_public_client().client.obtain_token_by_device_flow.return_value = {
            "access_token": "azure_token",
            "expires_in": expires_in,
        }
        creds = OAuthDeviceCode(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_load(self, mock_public_client: MagicMock) -> None:
        creds = OAuthDeviceCode.load(dict(self.DEFAULT_PROVIDER_ARGS))
        assert isinstance(creds, OAuthDeviceCode)
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_create_from_credential_provider(self, mock_public_client: MagicMock) -> None:
        config = {"device_code": dict(self.DEFAULT_PROVIDER_ARGS)}
        creds = CredentialProvider.load(config)
        assert isinstance(creds, OAuthDeviceCode)
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()


class TestOAuthInteractive:
    DEFAULT_PROVIDER_ARGS: ClassVar = MappingProxyType(
        {
            "authority_url": "https://login.microsoftonline.com/xyz",
            "client_id": "azure-client-id",
            "scopes": ["https://greenfield.cognitedata.com/.default"],
        }
    )

    @patch("cognite.client.credentials.PublicClientApplication")
    @pytest.mark.parametrize("expires_in", (1000, "1001"))  # some IDPs return as string
    def test_access_token_generated(self, mock_public_client: MagicMock, expires_in: int | str) -> None:
        mock_public_client().acquire_token_silent.return_value = {
            "access_token": "azure_token",
            "expires_in": expires_in,
        }
        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_load(self, mock_public_client: MagicMock) -> None:
        creds = OAuthInteractive.load(dict(self.DEFAULT_PROVIDER_ARGS))
        assert isinstance(creds, OAuthInteractive)
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_create_from_credential_provider(self, mock_public_client: MagicMock) -> None:
        config = {"interactive": dict(self.DEFAULT_PROVIDER_ARGS)}
        creds = CredentialProvider.load(config)
        assert isinstance(creds, OAuthInteractive)
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()


class TestOauthClientCredentials:
    DEFAULT_PROVIDER_ARGS: ClassVar = MappingProxyType(
        {
            "client_id": "azure-client-id",
            "client_secret": "azure-client-secret",
            "token_url": "https://login.microsoftonline.com/testingabc123/oauth2/v2.0/token",
            "scopes": ["https://greenfield.cognitedata.com/.default"],
            "other_custom_arg": "some_value",
        }
    )

    @patch("cognite.client.credentials.OAuth2Client")
    @pytest.mark.parametrize("expires_in", (1000, "1001"))  # some IDPs return as string
    def test_access_token_generated(self, mock_oauth_client: MagicMock, expires_in: int | str) -> None:
        mock_oauth_client().fetch_token.return_value = {"access_token": "azure_token", "expires_in": expires_in}
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.OAuth2Client")
    def test_access_token_not_generated_due_to_error(self, mock_oauth_client: MagicMock) -> None:
        mock_oauth_client().fetch_token.side_effect = OAuthError("very_invalid", "Invalid client_id parameter value.")
        with pytest.raises(
            CogniteAuthError,
            match=r"Error generating access token: 'very_invalid'. Description: Invalid client_id parameter value.",
        ):
            creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
            creds._refresh_access_token()

    @patch("cognite.client.credentials.OAuth2Client")
    def test_access_token_expired(self, mock_oauth_client: MagicMock) -> None:
        mock_oauth_client().fetch_token.side_effect = [
            {"access_token": "azure_token_expired", "expires_in": -1000},
            {"access_token": "azure_token_refreshed", "expires_in": 1000},
        ]
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        assert "Authorization", "Bearer azure_token_expired" == creds.authorization_header()
        assert "Authorization", "Bearer azure_token_refreshed" == creds.authorization_header()

    def test_load(self) -> None:
        creds = OAuthClientCredentials.load(dict(self.DEFAULT_PROVIDER_ARGS))
        assert isinstance(creds, OAuthClientCredentials)

    def test_create_from_credential_provider(self) -> None:
        creds = CredentialProvider.load({"client_credentials": dict(self.DEFAULT_PROVIDER_ARGS)})
        assert isinstance(creds, OAuthClientCredentials)
        assert creds.client_id == "azure-client-id"
        assert creds.client_secret == "azure-client-secret"
        assert creds.token_url == "https://login.microsoftonline.com/testingabc123/oauth2/v2.0/token"
        assert creds.scopes == ["https://greenfield.cognitedata.com/.default"]
        assert creds.token_custom_args == {"other_custom_arg": "some_value"}


class TestOAuthClientCertificate:
    DEFAULT_PROVIDER_ARGS: ClassVar = MappingProxyType(
        {
            "authority_url": "https://login.microsoftonline.com/xyz",
            "client_id": "azure-client-id",
            "cert_thumbprint": "XYZ123",
            "certificate": "certificatecontents123",
            "scopes": ["https://greenfield.cognitedata.com/.default"],
        }
    )

    @patch("cognite.client.credentials.ConfidentialClientApplication")
    def test_access_token_generated(self, mock_msal_app: MagicMock) -> None:
        mock_msal_app().acquire_token_for_client.return_value = {
            "access_token": "azure_token",
            "expires_in": 1000,
        }
        creds = OAuthClientCertificate(**self.DEFAULT_PROVIDER_ARGS)
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.ConfidentialClientApplication")
    def test_load(self, mock_msal_app: MagicMock) -> None:
        mock_msal_app().acquire_token_for_client.return_value = {
            "access_token": "azure_token",
            "expires_in": 1000,
        }
        creds = OAuthClientCertificate.load(dict(self.DEFAULT_PROVIDER_ARGS))
        assert isinstance(creds, OAuthClientCertificate)
        assert creds.authority_url == "https://login.microsoftonline.com/xyz"
        assert creds.client_id == "azure-client-id"
        assert creds.cert_thumbprint == "XYZ123"
        assert creds.certificate == "certificatecontents123"
        assert creds.scopes == ["https://greenfield.cognitedata.com/.default"]

    @patch("cognite.client.credentials.ConfidentialClientApplication")
    def test_create_from_credential_provider(self, mock_msal_app: MagicMock) -> None:
        mock_msal_app().acquire_token_for_client.return_value = {
            "access_token": "azure_token",
            "expires_in": 1000,
        }
        creds = CredentialProvider.load({"client_certificate": dict(self.DEFAULT_PROVIDER_ARGS)})
        assert isinstance(creds, OAuthClientCertificate)
        assert creds.authority_url == "https://login.microsoftonline.com/xyz"
        assert creds.client_id == "azure-client-id"
        assert creds.cert_thumbprint == "XYZ123"
        assert creds.certificate == "certificatecontents123"
        assert creds.scopes == ["https://greenfield.cognitedata.com/.default"]
