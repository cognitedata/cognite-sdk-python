import time
from json import JSONDecodeError
from types import MappingProxyType
from typing import Any, ClassVar
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests
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
    def test_entra_id_uses_authority_endpoint(self, mock_public_client: MagicMock) -> None:
        mock_device_response = Mock()
        mock_device_response.json.return_value = {
            "user_code": "ABCD-EFGH",
            "device_code": "device123",
            "verification_uri": "https://login.microsoftonline.com/activate",
            "expires_in": 900,
            "interval": 5,
        }

        mock_authority = Mock()
        mock_authority.device_authorization_endpoint = "https://login.microsoftonline.com/xyz/oauth2/v2.0/devicecode"
        mock_authority.instance = "instance"
        mock_public_client().authority = mock_authority
        mock_public_client().http_client.post.return_value = mock_device_response
        mock_public_client().client.obtain_token_by_device_flow.return_value = {
            "access_token": "azure_token",
            "expires_in": 3600,
        }

        creds = OAuthDeviceCode(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()

        # Verify the endpoint from MSAL authority was used (not the fallback)
        mock_public_client().http_client.post.assert_called_once()
        call_args = mock_public_client().http_client.post.call_args
        assert call_args[0][0] == "https://login.microsoftonline.com/xyz/oauth2/v2.0/devicecode"

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

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_oauth_discovery_url_device_flow(self, mock_public_client: MagicMock) -> None:
        mock_oidc_response = Mock()
        mock_oidc_response.json.return_value = {
            "device_authorization_endpoint": "https://auth0.example.com/oauth/device/code",
            "token_endpoint": "https://auth0.example.com/oauth/token",
        }

        mock_device_response = Mock()
        mock_device_response.json.return_value = {
            "user_code": "ABCD-EFGH",
            "device_code": "device123",
            "verification_uri": "https://auth0.example.com/activate",
            "expires_in": 900,
            "interval": 5,
        }

        # Configure http_client to return different responses for GET (discovery) and POST (device code)
        def http_client_side_effect(url: str, **kwargs: Any) -> Mock:
            if "openid-configuration" in url:
                return mock_oidc_response
            return mock_device_response

        mock_public_client().http_client.get.side_effect = http_client_side_effect
        mock_public_client().http_client.post.side_effect = http_client_side_effect
        mock_public_client().client.obtain_token_by_device_flow.return_value = {
            "access_token": "auth0_token",
            "expires_in": 3600,
        }

        # Mock authority to be None to simulate oauth_discovery_url behavior
        # But we need to mock instance for cache.add() call
        mock_authority = Mock()
        mock_authority.instance = None
        mock_authority.device_authorization_endpoint = None
        mock_public_client().authority = mock_authority

        creds = OAuthDeviceCode(
            authority_url=None,
            oauth_discovery_url="https://auth0.example.com/oauth",
            client_id="auth0-client-id",
            scopes=["openid", "profile"],
        )
        creds._refresh_access_token()

        # Verify OIDC discovery was fetched
        mock_public_client().http_client.get.assert_called_once_with(
            "https://auth0.example.com/oauth/.well-known/openid-configuration"
        )

        # Verify the device authorization endpoint from discovery was called
        mock_public_client().http_client.post.assert_called_once()
        call_args = mock_public_client().http_client.post.call_args
        assert call_args[0][0] == "https://auth0.example.com/oauth/device/code"

        assert "Authorization", "Bearer auth0_token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_device_code_msal_response(self, mock_public_client: MagicMock) -> None:
        class NormalizedResponse:
            """Simulate MSAL NormalizedResponse which exposes .text but no .json()."""

            def __init__(self, text: str) -> None:
                self.text = text

        mock_device_response = NormalizedResponse(
            '{"user_code": "ABCD", "device_code": "device123", '
            '"verification_uri": "https://example.com/activate", "expires_in": 900}'
        )

        mock_public_client().http_client.post.return_value = mock_device_response
        mock_public_client().client.obtain_token_by_device_flow.return_value = {
            "access_token": "token",
            "expires_in": 3600,
        }

        # Mock authority to trigger fallback endpoint
        mock_authority = Mock()
        mock_authority.device_authorization_endpoint = None
        mock_public_client().authority = mock_authority

        creds = OAuthDeviceCode(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()

        assert "Authorization", "Bearer token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_oidc_discovery_url_failure_error(self, mock_public_client: MagicMock) -> None:
        mock_public_client().http_client.get.side_effect = requests.exceptions.HTTPError("404 Client Error")
        mock_authority = Mock()
        mock_authority.instance = None
        mock_authority.device_authorization_endpoint = None
        mock_public_client().authority = mock_authority

        creds = OAuthDeviceCode(
            authority_url=None,
            oauth_discovery_url="https://auth0.example.com/oauth",
            client_id="auth0-client-id",
            scopes=["openid", "profile"],
        )

        with pytest.raises(CogniteAuthError, match="Error fetching device_authorization_endpoint from OIDC discovery"):
            creds._get_device_authorization_endpoint()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_device_flow_missing_endpoint(self, mock_public_client: MagicMock) -> None:
        mock_public_client().authority = Mock()
        mock_public_client().oauth_discovery_url = None
        mock_public_client().authority.device_authorization_endpoint = None

        # Bypass __init__ validation to simulate an object without authority_url or oauth_discovery_url
        creds = object.__new__(OAuthDeviceCode)
        creds._OAuthDeviceCode__authority_url = None  # type: ignore[attr-defined]
        creds._OAuthDeviceCode__oauth_discovery_url = None  # type: ignore[attr-defined]
        creds._OAuthDeviceCode__app = mock_public_client()  # type: ignore[attr-defined]

        with pytest.raises(CogniteAuthError, match="Unable to determine device authorization endpoint"):
            creds._get_device_authorization_endpoint()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_device_failed_discovery_error(self, mock_public_client: MagicMock) -> None:
        mock_public_client().http_client.get.side_effect = requests.exceptions.HTTPError("404 Client Error")
        mock_authority = Mock()
        mock_authority.instance = None
        mock_authority.device_authorization_endpoint = None
        mock_public_client().authority = mock_authority

        creds = OAuthDeviceCode(
            authority_url=None,
            oauth_discovery_url="https://auth0.example.com/oauth",
            client_id="auth0-client-id",
            scopes=["openid", "profile"],
        )

        with pytest.raises(CogniteAuthError, match="Error fetching device_authorization_endpoint from OIDC discovery"):
            creds._get_device_authorization_endpoint()

    @patch("cognite.client.credentials.PublicClientApplication")
    @pytest.mark.parametrize("error_type", (JSONDecodeError, TypeError, AttributeError))
    def test_device_discovery_invalid_json(self, mock_public_client: MagicMock, error_type: type[Exception]) -> None:
        mock_response = Mock()
        mock_response.json.side_effect = error_type("Expecting value", "", 0)
        mock_public_client().http_client.get.return_value = mock_response

        mock_authority = Mock()
        mock_authority.device_authorization_endpoint = None
        mock_public_client().authority = mock_authority

        creds = OAuthDeviceCode(
            authority_url=None,
            oauth_discovery_url="https://auth0.example.com/oauth",
            client_id="auth0-client-id",
            scopes=["openid", "profile"],
        )

        with pytest.raises(CogniteAuthError, match="Error parsing OIDC discovery document"):
            creds._get_device_authorization_endpoint()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_device_flow_response_invalid_client(self, mock_public_client: MagicMock) -> None:
        mock_device_response = Mock()
        mock_device_response.json.return_value = {
            "error": "invalid_client",
            "error_description": "Invalid client id",
        }

        mock_public_client().http_client.post.return_value = mock_device_response
        mock_authority = Mock()
        mock_authority.device_authorization_endpoint = None
        mock_public_client().authority = mock_authority

        creds = OAuthDeviceCode(**self.DEFAULT_PROVIDER_ARGS)

        with pytest.raises(CogniteAuthError, match=r"Error initiating device flow.*invalid_client"):
            creds._refresh_access_token()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_refresh_token_from_cache(self, mock_public_client: MagicMock) -> None:
        # Mock a valid refresh token in cache
        mock_refresh_token = {
            "secret": "refresh_token_secret",
            "expires_on": time.time() + 3600,  # Valid for 1 hour
        }

        mock_public_client().token_cache.search.return_value = [mock_refresh_token]
        mock_public_client().client.obtain_token_by_refresh_token.return_value = {
            "access_token": "new_access_token",
            "expires_in": 3600,
        }

        creds = OAuthDeviceCode(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()

        # Verify refresh token was used
        mock_public_client().client.obtain_token_by_refresh_token.assert_called_once_with("refresh_token_secret")
        assert "Authorization", "Bearer new_access_token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_access_token_from_cache(self, mock_public_client: MagicMock) -> None:
        # Mock no refresh token, but valid access token
        mock_public_client().token_cache.search.side_effect = [
            [],  # No refresh tokens
            [  # Access tokens
                {
                    "secret": "cached_access_token",
                    "expires_on": str(int(time.time() + 3600)),  # Valid for 1 hour
                }
            ],
        ]

        creds = OAuthDeviceCode(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()

        # Verify device flow was NOT triggered
        mock_public_client().http_client.post.assert_not_called()
        assert "Authorization", "Bearer cached_access_token" == creds.authorization_header()


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
