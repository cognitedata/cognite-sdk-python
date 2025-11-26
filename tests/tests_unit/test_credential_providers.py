import json
from types import MappingProxyType
from typing import ClassVar
from unittest.mock import Mock, patch

import pytest
import requests
from oauthlib.oauth2 import InvalidClientIdError

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
            TypeError, match=r"'token' must be a string or a no-argument-callable returning a string, not"
        ):
            Token({"foo": "bar"})

    @pytest.mark.parametrize(
        "config",
        [
            {"token": "abc"},
            '{"token": "abc"}',
            {"token": (lambda: "abc")},
        ],
    )
    def test_load(self, config):
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
    def test_create_from_credential_provider(self, config):
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
    def test_access_token_generated(self, mock_public_client, expires_in):
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
    @pytest.mark.parametrize(
        "authority_endpoint,authority_url,oauth_discovery_url,oidc_response,expected_endpoint,expected_error",
        [
            # MSAL authority has device_authorization_endpoint
            pytest.param(
                "https://login.microsoftonline.com/xyz/oauth2/v2.0/devicecode",
                "https://login.microsoftonline.com/xyz",
                None,
                None,
                "https://login.microsoftonline.com/xyz/oauth2/v2.0/devicecode",
                None,
                id="msal_authority_has_endpoint",
            ),
            # Authority URL fallback (no MSAL endpoint)
            pytest.param(
                None,
                "https://login.microsoftonline.com/xyz",
                None,
                None,
                "https://login.microsoftonline.com/xyz/oauth2/v2.0/devicecode",
                None,
                id="authority_url_fallback",
            ),
            # OIDC discovery with endpoint
            pytest.param(
                None,
                None,
                "https://auth0.example.com/oauth",
                {"device_authorization_endpoint": "https://auth0.example.com/oauth/device/code"},
                "https://auth0.example.com/oauth/device/code",
                None,
                id="oidc_discovery_with_endpoint",
            ),
            # OIDC discovery missing endpoint
            pytest.param(
                None,
                None,
                "https://auth0.example.com/oauth",
                {"token_endpoint": "https://auth0.example.com/oauth/token"},
                None,
                "device_authorization_endpoint not found",
                id="oidc_discovery_missing_endpoint",
            ),
            # OIDC discovery network error
            pytest.param(
                None,
                None,
                "https://auth0.example.com/oauth",
                requests.exceptions.RequestException("Network error"),
                None,
                "Error fetching device_authorization_endpoint from OIDC discovery",
                id="oidc_discovery_network_error",
            ),
        ],
    )
    def test_get_device_authorization_endpoint(
        self,
        mock_public_client,
        authority_endpoint,
        authority_url,
        oauth_discovery_url,
        oidc_response,
        expected_endpoint,
        expected_error,
    ):
        """Test _get_device_authorization_endpoint with various scenarios"""

        # Setup authority mock
        mock_authority = Mock()
        mock_authority.device_authorization_endpoint = authority_endpoint
        mock_authority.instance = "instance" if authority_url else None
        mock_public_client().authority = mock_authority

        # Setup OIDC discovery response
        if oidc_response is None:
            pass  # No OIDC discovery
        elif isinstance(oidc_response, Exception):
            mock_public_client().http_client.get.side_effect = oidc_response
        else:
            mock_oidc_response = Mock()
            mock_oidc_response.json.return_value = oidc_response
            mock_public_client().http_client.get.return_value = mock_oidc_response

        # Create OAuthDeviceCode instance (requires either authority_url or oauth_discovery_url)
        # For testing the method directly, we need at least one input
        if authority_url is None and oauth_discovery_url is None:
            # Skip this test case - can't create OAuthDeviceCode without inputs
            pytest.skip("Cannot test without authority_url or oauth_discovery_url")

        creds = OAuthDeviceCode(
            authority_url=authority_url,
            oauth_discovery_url=oauth_discovery_url,
            client_id="test-client-id",
            scopes=["test-scope"],
        )

        if expected_error:
            with pytest.raises(CogniteAuthError, match=expected_error):
                creds._get_device_authorization_endpoint()
        else:
            result = creds._get_device_authorization_endpoint()
            assert result == expected_endpoint

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_entra_id_uses_authority_endpoint(self, mock_public_client):
        """Test that Entra ID uses MSAL authority's device_authorization_endpoint when available"""
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
    def test_load(self, mock_public_client):
        creds = OAuthDeviceCode.load(dict(self.DEFAULT_PROVIDER_ARGS))
        assert isinstance(creds, OAuthDeviceCode)
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_create_from_credential_provider(self, mock_public_client):
        config = {"device_code": dict(self.DEFAULT_PROVIDER_ARGS)}
        creds = CredentialProvider.load(config)
        assert isinstance(creds, OAuthDeviceCode)
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_oauth_discovery_url_device_flow(self, mock_public_client):
        """Test that device code flow works with oauth_discovery_url (Auth0/Cognito)"""
        mock_oidc_response = Mock()
        mock_oidc_response.json.return_value = {
            "device_authorization_endpoint": "https://auth0.example.com/oauth/device/code",
            "token_endpoint": "https://auth0.example.com/oauth/token",
        }

        # Mock the device code response
        mock_device_response = Mock()
        mock_device_response.json.return_value = {
            "user_code": "ABCD-EFGH",
            "device_code": "device123",
            "verification_uri": "https://auth0.example.com/activate",
            "expires_in": 900,
            "interval": 5,
        }

        # Configure http_client to return different responses for GET (discovery) and POST (device code)
        def http_client_side_effect(url, **kwargs):
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
    def test_device_code_response_normalized_response(self, mock_public_client):
        """Test handling of MSAL NormalizedResponse (text fallback)"""
        mock_device_response = Mock()
        # Simulate NormalizedResponse that doesn't have .json() but has .text
        mock_device_response.json.side_effect = AttributeError("No json method")
        mock_device_response.text = '{"user_code": "ABCD", "device_code": "device123", "verification_uri": "https://example.com/activate", "expires_in": 900}'

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
    def test_refresh_token_from_cache(self, mock_public_client):
        """Test using refresh token from cache to get new access token"""
        import time

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
    def test_access_token_from_cache(self, mock_public_client):
        """Test using valid access token from cache"""
        import time

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

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_device_flow_error_response(self, mock_public_client):
        """Test handling of error response from device authorization endpoint"""
        mock_device_response = Mock()
        mock_device_response.json.return_value = {
            "error": "invalid_client",
            "error_description": "Invalid client credentials",
        }

        mock_public_client().http_client.post.return_value = mock_device_response
        mock_authority = Mock()
        mock_authority.device_authorization_endpoint = None
        mock_public_client().authority = mock_authority

        creds = OAuthDeviceCode(**self.DEFAULT_PROVIDER_ARGS)

        with pytest.raises(CogniteAuthError, match=r"Error initiating device flow.*invalid_client"):
            creds._refresh_access_token()

    @patch("cognite.client.credentials.PublicClientApplication")
    @pytest.mark.parametrize(
        "json_error,text_error,expected_error",
        [
            # JSONDecodeError when parsing .json()
            pytest.param(
                json.JSONDecodeError("Expecting value", "", 0),
                None,
                None,  # Should fallback to text parsing
                id="json_decode_error_fallback",
            ),
            # TypeError when parsing .json()
            pytest.param(
                TypeError("Not callable"),
                None,
                None,  # Should fallback to text parsing
                id="type_error_fallback",
            ),
            # Both .json() and .text parsing fail
            pytest.param(
                json.JSONDecodeError("Expecting value", "", 0),
                json.JSONDecodeError("Expecting value", "", 0),
                "Unable to parse device flow response",
                id="both_parsing_fail",
            ),
            # Missing .text attribute
            pytest.param(
                AttributeError("No json method"),
                AttributeError("No text attribute"),
                "Unable to parse device flow response",
                id="missing_text_attribute",
            ),
        ],
    )
    def test_device_code_response_parsing_errors(self, mock_public_client, json_error, text_error, expected_error):
        """Test error handling when parsing device code response fails"""
        mock_device_response = Mock()
        mock_device_response.json.side_effect = json_error

        if text_error is None:
            # Successful text fallback
            mock_device_response.text = '{"user_code": "ABCD", "device_code": "device123", "verification_uri": "https://example.com/activate", "expires_in": 900}'
        else:
            # Text parsing also fails
            if isinstance(text_error, AttributeError):
                del mock_device_response.text
            else:
                mock_device_response.text = "invalid json"

        mock_public_client().http_client.post.return_value = mock_device_response
        mock_authority = Mock()
        mock_authority.device_authorization_endpoint = None
        mock_public_client().authority = mock_authority

        if expected_error:
            mock_public_client().client.obtain_token_by_device_flow.return_value = {
                "access_token": "token",
                "expires_in": 3600,
            }

        creds = OAuthDeviceCode(**self.DEFAULT_PROVIDER_ARGS)

        if expected_error:
            with pytest.raises(CogniteAuthError, match=expected_error):
                creds._refresh_access_token()
        else:
            mock_public_client().client.obtain_token_by_device_flow.return_value = {
                "access_token": "token",
                "expires_in": 3600,
            }
            creds._refresh_access_token()
            assert "Authorization", "Bearer token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_oidc_discovery_json_parse_error(self, mock_public_client):
        """Test error handling when OIDC discovery document JSON parsing fails"""
        mock_oidc_response = Mock()
        mock_oidc_response.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)

        mock_public_client().http_client.get.return_value = mock_oidc_response
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

        with pytest.raises(CogniteAuthError, match="Error parsing OIDC discovery document"):
            creds._get_device_authorization_endpoint()


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
    def test_access_token_generated(self, mock_public_client, expires_in):
        mock_public_client().acquire_token_silent.return_value = {
            "access_token": "azure_token",
            "expires_in": expires_in,
        }
        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_load(self, mock_public_client):
        creds = OAuthInteractive.load(dict(self.DEFAULT_PROVIDER_ARGS))
        assert isinstance(creds, OAuthInteractive)
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_create_from_credential_provider(self, mock_public_client):
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

    @patch("cognite.client.credentials.BackendApplicationClient")
    @patch("cognite.client.credentials.OAuth2Session")
    @pytest.mark.parametrize("expires_in", (1000, "1001"))  # some IDPs return as string
    def test_access_token_generated(self, mock_oauth_session, mock_backend_client, expires_in):
        mock_oauth_session().fetch_token.return_value = {"access_token": "azure_token", "expires_in": expires_in}
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.BackendApplicationClient")
    @patch("cognite.client.credentials.OAuth2Session")
    def test_access_token_not_generated_due_to_error(self, mock_oauth_session, mock_backend_client):
        mock_oauth_session().fetch_token.side_effect = InvalidClientIdError()
        with pytest.raises(
            CogniteAuthError,
            match=r"Error generating access token: invalid_request, 400, Invalid client_id parameter value\.",
        ):
            creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
            creds._refresh_access_token()

    @patch("cognite.client.credentials.BackendApplicationClient")
    @patch("cognite.client.credentials.OAuth2Session")
    def test_access_token_expired(self, mock_oauth_session, mock_backend_client):
        mock_oauth_session().fetch_token.side_effect = [
            {"access_token": "azure_token_expired", "expires_in": -1000},
            {"access_token": "azure_token_refreshed", "expires_in": 1000},
        ]
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        assert "Authorization", "Bearer azure_token_expired" == creds.authorization_header()
        assert "Authorization", "Bearer azure_token_refreshed" == creds.authorization_header()

    def test_load(self):
        creds = OAuthClientCredentials.load(dict(self.DEFAULT_PROVIDER_ARGS))
        assert isinstance(creds, OAuthClientCredentials)

    def test_create_from_credential_provider(self):
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
    def test_access_token_generated(self, mock_msal_app):
        mock_msal_app().acquire_token_for_client.return_value = {
            "access_token": "azure_token",
            "expires_in": 1000,
        }
        creds = OAuthClientCertificate(**self.DEFAULT_PROVIDER_ARGS)
        assert "Authorization", "Bearer azure_token" == creds.authorization_header()

    @patch("cognite.client.credentials.ConfidentialClientApplication")
    def test_load(self, mock_msal_app):
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
    def test_create_from_credential_provider(self, mock_msal_app):
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
