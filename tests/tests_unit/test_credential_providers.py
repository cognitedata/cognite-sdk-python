from __future__ import annotations

import pickle
import sys
import tempfile
import time
from json import JSONDecodeError
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar
from unittest.mock import MagicMock, Mock, patch
from urllib.parse import parse_qs

import httpx2
import pytest
import requests
from pytest_httpx2 import HTTPXMock

from cognite.client.credentials import (
    CredentialProvider,
    OAuthClientCertificate,
    OAuthClientCredentials,
    OAuthDeviceCode,
    OAuthInteractive,
    Token,
    _WithMsalSerializableTokenCache,
)
from cognite.client.exceptions import CogniteAuthError, CogniteOAuthError


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
        assert creds.authorization_header() == ("Authorization", "Bearer abc")

    def test_token_factory_auth_header(self) -> None:
        creds = Token(lambda: "abc")
        assert creds.authorization_header() == ("Authorization", "Bearer abc")

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
        assert creds.authorization_header() == ("Authorization", "Bearer abc")

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
        assert creds.authorization_header() == ("Authorization", "Bearer abc")


class TestOAuthDeviceCode:
    DEFAULT_PROVIDER_ARGS: ClassVar[MappingProxyType[str, Any]] = MappingProxyType(
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
        assert creds.authorization_header() == ("Authorization", "Bearer azure_token")

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

        assert creds.authorization_header() == ("Authorization", "Bearer azure_token")

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_load(self, mock_public_client: MagicMock) -> None:
        creds = OAuthDeviceCode.load(dict(self.DEFAULT_PROVIDER_ARGS))
        assert isinstance(creds, OAuthDeviceCode)

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_create_from_credential_provider(self, mock_public_client: MagicMock) -> None:
        config = {"device_code": dict(self.DEFAULT_PROVIDER_ARGS)}
        creds = CredentialProvider.load(config)
        assert isinstance(creds, OAuthDeviceCode)

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
        def http_client_side_effect(url: str, **kwargs: Any) -> Any:
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

        assert creds.authorization_header() == ("Authorization", "Bearer auth0_token")

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

        assert creds.authorization_header() == ("Authorization", "Bearer token")

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
        creds._OAuthDeviceCode__authority_url = None  # type: ignore [attr-defined]
        creds._OAuthDeviceCode__oauth_discovery_url = None  # type: ignore [attr-defined]
        creds._OAuthDeviceCode__app = mock_public_client()  # type: ignore [attr-defined]

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
    def test_device_discovery_invalid_json(self, mock_public_client: MagicMock, error_type: type) -> None:
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

        with pytest.raises(CogniteAuthError, match=r"Error initiating device flow: invalid_client"):
            creds._refresh_access_token()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_refresh_token_from_cache(self, mock_public_client: MagicMock) -> None:
        # Mock a valid refresh token in cache
        mock_refresh_token = {
            "secret": "refresh_token_secret",
            "client_id": "test-client-id",
        }

        # AT search returns nothing; RT search returns the entry
        mock_public_client().token_cache.search.side_effect = [
            [],  # No valid access tokens
            [mock_refresh_token],  # Refresh token found
        ]
        mock_public_client().client.obtain_token_by_refresh_token.return_value = {
            "access_token": "new_access_token",
            "expires_in": 3600,
        }

        creds = OAuthDeviceCode(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()

        # Verify refresh token was used (full entry + rt_getter kwarg)
        call_args = mock_public_client().client.obtain_token_by_refresh_token.call_args
        assert call_args.args[0] == mock_refresh_token
        assert call_args.kwargs["rt_getter"](mock_refresh_token) == "refresh_token_secret"

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_access_token_from_cache(self, mock_public_client: MagicMock) -> None:
        # Mock no refresh token, but valid access token
        mock_public_client().token_cache.search.side_effect = [
            [  # Access token found on first search
                {
                    "secret": "cached_access_token",
                    "expires_on": str(int(time.time() + 3600)),  # Valid for 1 hour
                }
            ],
        ]

        creds = OAuthDeviceCode(**self.DEFAULT_PROVIDER_ARGS)

        # Verify device flow was NOT triggered
        assert creds.authorization_header() == ("Authorization", "Bearer cached_access_token")
        mock_public_client().http_client.post.assert_not_called()


class TestWithMsalSerializableTokenCache:
    def test_resolve_token_cache_path_default_not_in_system_tempdir(self) -> None:
        resolved = _WithMsalSerializableTokenCache._resolve_token_cache_path(None, "some-client-id")
        assert not str(resolved).startswith(str(Path(tempfile.gettempdir())))
        assert resolved.name == "cognitetokencache.some-client-id.bin"

    def test_resolve_token_cache_path_custom_path_passthrough(self, tmp_path: Path) -> None:
        custom = tmp_path / "custom-cache.bin"
        resolved = _WithMsalSerializableTokenCache._resolve_token_cache_path(custom, "some-client-id")
        assert resolved == custom

    @pytest.mark.skipif(sys.platform == "win32", reason="symlink semantics not applicable")
    def test_get_cached_token_symlink_is_ignored(self, tmp_path: Path) -> None:
        victim = tmp_path / "victim.txt"
        victim.write_text("not a token cache")
        link = tmp_path / "cache.bin"
        link.symlink_to(victim)

        with pytest.warns(UserWarning, match="refusing to follow a symlink"):
            result = _WithMsalSerializableTokenCache._get_cached_token(link)
        assert result == {}

    @patch("cognite.client.credentials.atexit.register")
    @patch("cognite.client.credentials.PublicClientApplication")
    @patch("cognite.client.credentials.SerializableTokenCache")
    def test_at_exit_callback_uses_secure_write(
        self,
        mock_token_cache_cls: MagicMock,
        mock_public_client: MagicMock,
        mock_atexit_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        fake_cache = MagicMock()
        fake_cache.has_state_changed = True
        fake_cache.serialize.return_value = "serialized-content"
        mock_token_cache_cls.return_value = fake_cache

        cache_path = tmp_path / "cache.bin"
        OAuthDeviceCode(token_cache_path=cache_path, **TestOAuthDeviceCode.DEFAULT_PROVIDER_ARGS)

        mock_atexit_register.assert_called_once()
        at_exit_callback = mock_atexit_register.call_args[0][0]

        with patch("cognite.client.credentials.write_securely") as mock_write_securely:
            at_exit_callback()
        mock_write_securely.assert_called_once_with(cache_path, "serialized-content")

    @patch("cognite.client.credentials.write_securely")
    @patch("cognite.client.credentials.PublicClientApplication")
    def test_get_token_second_write_site_uses_secure_write(
        self, mock_public_client: MagicMock, mock_write_securely: MagicMock, tmp_path: Path
    ) -> None:
        cache_path = tmp_path / "cache.bin"  # does not exist
        mock_public_client().token_cache.has_state_changed = True
        mock_public_client().token_cache.serialize.return_value = "serialized-content"

        creds = OAuthDeviceCode(token_cache_path=cache_path, **TestOAuthDeviceCode.DEFAULT_PROVIDER_ARGS)
        creds._get_token()

        mock_write_securely.assert_called_once_with(cache_path, "serialized-content")


class TestOAuthInteractive:
    DEFAULT_PROVIDER_ARGS: ClassVar[MappingProxyType[str, Any]] = MappingProxyType(
        {
            "authority_url": "https://login.microsoftonline.com/xyz",
            "client_id": "azure-client-id",
            "scopes": ["https://greenfield.cognitedata.com/.default"],
        }
    )

    @staticmethod
    def _mock_app(
        mock_public_client: MagicMock, accounts: list[dict[str, str]], signed_in_as: str = "jane@doe.com"
    ) -> MagicMock:
        app = mock_public_client()
        # MSAL matches the username case insensitively, as accounts from Entra ID may contain mixed case:
        app.get_accounts.side_effect = lambda username=None: (
            accounts if username is None else [acc for acc in accounts if acc["username"].lower() == username.lower()]
        )
        app.acquire_token_silent.return_value = {"access_token": "silent_token", "expires_in": 1000}
        app.acquire_token_interactive.return_value = {
            "access_token": "interactive_token",
            "expires_in": 1000,
            "id_token_claims": {"preferred_username": signed_in_as},
        }
        return app

    @patch("cognite.client.credentials.PublicClientApplication")
    @pytest.mark.parametrize("expires_in", (1000, "1001"))  # some IDPs return as string
    def test_access_token_generated(self, mock_public_client: MagicMock, expires_in: int | str) -> None:
        app = self._mock_app(mock_public_client, [{"username": "jane@doe.com"}])
        app.acquire_token_silent.return_value = {"access_token": "azure_token", "expires_in": expires_in}

        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS)
        creds._refresh_access_token()
        assert creds.authorization_header() == ("Authorization", "Bearer azure_token")

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_single_cached_account_is_reused_silently(self, mock_public_client: MagicMock) -> None:
        app = self._mock_app(mock_public_client, [{"username": "jane@doe.com"}])

        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS)
        assert creds.authorization_header() == ("Authorization", "Bearer silent_token")
        app.acquire_token_interactive.assert_not_called()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_no_cached_account_signs_in_without_a_prompt(self, mock_public_client: MagicMock) -> None:
        app = self._mock_app(mock_public_client, [])

        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS)
        assert creds.authorization_header() == ("Authorization", "Bearer interactive_token")
        app.acquire_token_silent.assert_not_called()
        assert app.acquire_token_interactive.call_args.kwargs["prompt"] is None

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_several_cached_accounts_lets_the_user_select(self, mock_public_client: MagicMock) -> None:
        app = self._mock_app(mock_public_client, [{"username": "jane@doe.com"}, {"username": "john@doe.com"}])

        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS)
        assert creds.authorization_header() == ("Authorization", "Bearer interactive_token")
        app.acquire_token_silent.assert_not_called()
        assert app.acquire_token_interactive.call_args.kwargs["prompt"] == "select_account"

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_selected_account_is_refreshed_silently(self, mock_public_client: MagicMock) -> None:
        app = self._mock_app(mock_public_client, [{"username": "jane@doe.com"}, {"username": "john@doe.com"}])

        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS)
        assert creds._refresh_access_token()[0] == "interactive_token"
        assert creds._refresh_access_token()[0] == "silent_token"

        app.acquire_token_interactive.assert_called_once()
        assert app.acquire_token_silent.call_args.kwargs["account"] == {"username": "jane@doe.com"}

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_signing_in_again_stays_on_the_same_account(self, mock_public_client: MagicMock) -> None:
        app = self._mock_app(mock_public_client, [{"username": "jane@doe.com"}, {"username": "john@doe.com"}])
        app.acquire_token_silent.return_value = None

        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS)
        assert creds._refresh_access_token()[0] == "interactive_token"
        assert creds._refresh_access_token()[0] == "interactive_token"

        assert app.acquire_token_interactive.call_count == 2
        assert app.acquire_token_interactive.call_args.kwargs["login_hint"] == "jane@doe.com"
        assert app.acquire_token_interactive.call_args.kwargs["prompt"] is None

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_login_hint_picks_account_among_several(self, mock_public_client: MagicMock) -> None:
        app = self._mock_app(mock_public_client, [{"username": "jane@doe.com"}, {"username": "john@doe.com"}])

        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS, login_hint="john@doe.com")
        assert creds.authorization_header() == ("Authorization", "Bearer silent_token")
        app.acquire_token_interactive.assert_not_called()
        assert app.acquire_token_silent.call_args.kwargs["account"] == {"username": "john@doe.com"}

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_login_hint_without_matching_account_goes_interactive(self, mock_public_client: MagicMock) -> None:
        app = self._mock_app(mock_public_client, [{"username": "jane@doe.com"}], signed_in_as="john@doe.com")

        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS, login_hint="john@doe.com")
        assert creds.authorization_header() == ("Authorization", "Bearer interactive_token")
        app.acquire_token_silent.assert_not_called()
        assert app.acquire_token_interactive.call_args.kwargs["login_hint"] == "john@doe.com"

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_explicit_prompt_applies_to_the_first_sign_in_only(self, mock_public_client: MagicMock) -> None:
        app = self._mock_app(mock_public_client, [{"username": "jane@doe.com"}])

        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS, prompt="select_account")
        assert creds._refresh_access_token()[0] == "interactive_token"
        app.acquire_token_silent.assert_not_called()
        assert app.acquire_token_interactive.call_args.kwargs["prompt"] == "select_account"

        assert creds._refresh_access_token()[0] == "silent_token"
        app.acquire_token_interactive.assert_called_once()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_sign_in_is_not_repeated_when_the_idp_omits_the_username(self, mock_public_client: MagicMock) -> None:
        app = self._mock_app(mock_public_client, [{"username": "jane@doe.com"}])
        app.acquire_token_interactive.return_value = {"access_token": "interactive_token", "expires_in": 1000}

        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS, prompt="login")
        assert creds._refresh_access_token()[0] == "interactive_token"
        assert creds._refresh_access_token()[0] == "silent_token"

        app.acquire_token_interactive.assert_called_once()

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_a_failed_sign_in_is_not_remembered(self, mock_public_client: MagicMock) -> None:
        app = self._mock_app(mock_public_client, [{"username": "jane@doe.com"}])
        app.acquire_token_interactive.return_value = {"error": "access_denied", "error_description": "nope"}

        creds = OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS, prompt="login")
        with pytest.raises(CogniteOAuthError):
            creds._refresh_access_token()
        with pytest.raises(CogniteOAuthError):
            creds._refresh_access_token()

        assert app.acquire_token_interactive.call_count == 2
        assert app.acquire_token_interactive.call_args.kwargs["prompt"] == "login"

    @patch("cognite.client.credentials.PublicClientApplication")
    @pytest.mark.parametrize("prompt", ("select-account", "none"))
    def test_invalid_prompt_raises(self, mock_public_client: MagicMock, prompt: str) -> None:
        with pytest.raises(ValueError, match=r"^'prompt' must be one of"):
            OAuthInteractive(**self.DEFAULT_PROVIDER_ARGS, prompt=prompt)  # type: ignore [arg-type]

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_default_for_entra_id_passes_on_account_selection(self, mock_public_client: MagicMock) -> None:
        creds = OAuthInteractive.default_for_entra_id(
            tenant_id="xyz", client_id="abcd", cdf_cluster="greenfield", prompt="select_account", login_hint="jane@doe"
        )
        assert creds.prompt == "select_account"
        assert creds.login_hint == "jane@doe"

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_load(self, mock_public_client: MagicMock) -> None:
        creds = OAuthInteractive.load(dict(self.DEFAULT_PROVIDER_ARGS))
        assert isinstance(creds, OAuthInteractive)
        assert creds.prompt is None
        assert creds.login_hint is None

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_load_with_account_selection(self, mock_public_client: MagicMock) -> None:
        config = dict(self.DEFAULT_PROVIDER_ARGS, prompt="select_account", login_hint="jane@doe.com")
        creds = OAuthInteractive.load(config)
        assert creds.prompt == "select_account"
        assert creds.login_hint == "jane@doe.com"

    @patch("cognite.client.credentials.PublicClientApplication")
    def test_create_from_credential_provider(self, mock_public_client: MagicMock) -> None:
        config = {"interactive": dict(self.DEFAULT_PROVIDER_ARGS)}
        creds = CredentialProvider.load(config)
        assert isinstance(creds, OAuthInteractive)


def _form_fields(request: httpx2.Request) -> dict[str, list[str]]:
    # keep_blank_values ensures we notice fields sent with an empty value:
    return parse_qs(request.content.decode(), keep_blank_values=True)


class TestOauthClientCredentials:
    TOKEN_URL: ClassVar[str] = "https://login.microsoftonline.com/testingabc123/oauth2/v2.0/token"
    DEFAULT_PROVIDER_ARGS: ClassVar[MappingProxyType[str, Any]] = MappingProxyType(
        {
            "client_id": "azure-client-id",
            "client_secret": "azure-client-secret",
            "token_url": TOKEN_URL,
            "scopes": ["https://greenfield.cognitedata.com/.default"],
            "other_custom_arg": "some_value",
        }
    )

    def add_token_response(self, httpx2_mock: HTTPXMock, access_token: str = "tok", expires_in: Any = 1000) -> None:
        httpx2_mock.add_response(
            method="POST", url=self.TOKEN_URL, json={"access_token": access_token, "expires_in": expires_in}
        )

    @pytest.mark.parametrize("expires_in", (1000, "1001"))  # some IDPs return as string
    def test_access_token_generated(self, httpx2_mock: HTTPXMock, expires_in: int | str) -> None:
        self.add_token_response(httpx2_mock, "azure_token", expires_in)
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        assert creds.authorization_header() == ("Authorization", "Bearer azure_token")

    def test_token_request(self, httpx2_mock: HTTPXMock) -> None:
        self.add_token_response(httpx2_mock)
        creds = OAuthClientCredentials(**{**self.DEFAULT_PROVIDER_ARGS, "scopes": ["a/.default", "b"]})
        creds.authorization_header()

        (request,) = httpx2_mock.get_requests()
        assert request.headers["Content-Type"] == "application/x-www-form-urlencoded"
        assert request.headers["Accept"] == "application/json"
        assert "Authorization" not in request.headers
        assert "Cookie" not in request.headers
        assert _form_fields(request) == {
            "grant_type": ["client_credentials"],
            "client_id": ["azure-client-id"],
            "client_secret": ["azure-client-secret"],
            "scope": ["a/.default b"],
            "other_custom_arg": ["some_value"],
        }

    @pytest.mark.parametrize("empty_scopes", (None, []))
    @pytest.mark.parametrize("falsy_value", (None, "", False, 0))
    def test_token_request_skips_falsy_fields(
        self, httpx2_mock: HTTPXMock, empty_scopes: list[str] | None, falsy_value: Any
    ) -> None:
        self.add_token_response(httpx2_mock)
        creds = OAuthClientCredentials(
            **{**self.DEFAULT_PROVIDER_ARGS, "scopes": empty_scopes, "other_custom_arg": falsy_value}
        )
        creds.authorization_header()

        (request,) = httpx2_mock.get_requests()
        assert set(_form_fields(request)) == {"grant_type", "client_id", "client_secret"}

    @pytest.mark.parametrize("client_secret", ("a+b&c=d%20e f", "æøå~!*'()", "%%=="))
    def test_client_secret_encoding(self, httpx2_mock: HTTPXMock, client_secret: str) -> None:
        self.add_token_response(httpx2_mock)
        creds = OAuthClientCredentials(**{**self.DEFAULT_PROVIDER_ARGS, "client_secret": client_secret})
        creds.authorization_header()

        (request,) = httpx2_mock.get_requests()
        assert _form_fields(request)["client_secret"] == [client_secret]

    @pytest.mark.parametrize(
        "value, expected",
        (
            ("x y&æ", "x y&æ"),
            ("a+b=c%d", "a+b=c%d"),
            (True, "True"),
            (1, "1"),
            (1.5, "1.5"),
            (b"aud", "aud"),
        ),
    )
    def test_token_custom_arg_encoding(self, httpx2_mock: HTTPXMock, value: Any, expected: str) -> None:
        self.add_token_response(httpx2_mock)
        creds = OAuthClientCredentials(**{**self.DEFAULT_PROVIDER_ARGS, "other_custom_arg": value})
        creds.authorization_header()

        (request,) = httpx2_mock.get_requests()
        assert _form_fields(request)["other_custom_arg"] == [expected]

    @pytest.mark.parametrize("value", ([1, 2], ("a", "b"), {"k": "v"}))
    def test_token_custom_arg_unsupported_type(self, value: Any) -> None:
        # Should fail already at init, not on the first token fetch:
        with pytest.raises(TypeError, match=r"^Token custom arg 'other_custom_arg' must be a str, bytes, int"):
            OAuthClientCredentials(**{**self.DEFAULT_PROVIDER_ARGS, "other_custom_arg": value})

    @pytest.mark.parametrize("scope", ("a/.default b", ["a/.default", "b"]))
    @pytest.mark.parametrize("scopes", (None, ["ignored"]))
    def test_scope_custom_arg_overrides_scopes(
        self, httpx2_mock: HTTPXMock, scopes: list[str] | None, scope: str | list[str]
    ) -> None:
        self.add_token_response(httpx2_mock)
        creds = OAuthClientCredentials(**{**self.DEFAULT_PROVIDER_ARGS, "scopes": scopes, "scope": scope})
        creds.authorization_header()

        (request,) = httpx2_mock.get_requests()
        assert _form_fields(request)["scope"] == ["a/.default b"]

    def test_http_options_not_sent_as_form_fields(self, httpx2_mock: HTTPXMock) -> None:
        redirected_url = "https://login.example.com/token"
        httpx2_mock.add_response(
            method="POST", url=self.TOKEN_URL, status_code=307, headers={"Location": redirected_url}
        )
        httpx2_mock.add_response(method="POST", url=redirected_url, json={"access_token": "tok", "expires_in": 1000})
        creds = OAuthClientCredentials(
            **self.DEFAULT_PROVIDER_ARGS, timeout=60, follow_redirects=True, cookies={"foo": "bar"}
        )
        assert creds.authorization_header() == ("Authorization", "Bearer tok")

        request = httpx2_mock.get_requests()[0]
        assert request.extensions["timeout"] == {"connect": 60, "read": 60, "write": 60, "pool": 60}
        assert request.headers["Cookie"] == "foo=bar"
        assert _form_fields(request) == {
            "grant_type": ["client_credentials"],
            "client_id": ["azure-client-id"],
            "client_secret": ["azure-client-secret"],
            "scope": ["https://greenfield.cognitedata.com/.default"],
            "other_custom_arg": ["some_value"],
        }

    @pytest.mark.parametrize("disable_ssl", (False, True))
    def test_disable_ssl_is_respected(
        self, httpx2_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch, disable_ssl: bool
    ) -> None:
        from cognite.client.config import global_config

        self.add_token_response(httpx2_mock)
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        # Read at refresh time, so this should take effect even after the credentials are created:
        monkeypatch.setattr(global_config, "disable_ssl", disable_ssl)
        with patch("cognite.client.credentials.httpx2.post", wraps=httpx2.post) as post_spy:
            creds.authorization_header()
        assert post_spy.call_args.kwargs["verify"] is not disable_ssl

    @pytest.mark.parametrize("status_code", (400, 401))
    def test_access_token_not_generated_due_to_error(self, httpx2_mock: HTTPXMock, status_code: int) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=self.TOKEN_URL,
            status_code=status_code,
            json={"error": "very_invalid", "error_description": "Invalid client_id parameter value."},
        )
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        with pytest.raises(
            CogniteOAuthError,
            match=r"Error generating access token: 'very_invalid'. Description: Invalid client_id parameter value.",
        ):
            creds._refresh_access_token()

    @pytest.mark.parametrize(
        "error_body", ({"error": "invalid_client"}, {"error": "invalid_client", "error_description": None})
    )
    def test_missing_error_description(self, httpx2_mock: HTTPXMock, error_body: dict[str, Any]) -> None:
        httpx2_mock.add_response(method="POST", url=self.TOKEN_URL, status_code=401, json=error_body)
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        with pytest.raises(CogniteOAuthError) as exc_info:
            creds._refresh_access_token()
        assert exc_info.value.description == ""
        assert str(exc_info.value) == "Error generating access token: 'invalid_client'. Description: "

    def test_server_error_raises(self, httpx2_mock: HTTPXMock) -> None:
        httpx2_mock.add_response(method="POST", url=self.TOKEN_URL, status_code=503, text="Service Unavailable")
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        with pytest.raises(httpx2.HTTPStatusError):
            creds._refresh_access_token()

    def test_valid_token_is_reused(self, httpx2_mock: HTTPXMock) -> None:
        self.add_token_response(httpx2_mock, "azure_token", expires_in=1000)
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        assert creds.authorization_header() == ("Authorization", "Bearer azure_token")
        assert creds.authorization_header() == ("Authorization", "Bearer azure_token")
        assert len(httpx2_mock.get_requests()) == 1

    @pytest.mark.parametrize("expires_in, should_refresh", ((-1000, True), (50, True), (70, False)))
    def test_token_refreshed_within_leeway(self, httpx2_mock: HTTPXMock, expires_in: int, should_refresh: bool) -> None:
        self.add_token_response(httpx2_mock, "first_token", expires_in)
        if should_refresh:
            self.add_token_response(httpx2_mock, "second_token", 1000)
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS, token_expiry_leeway_seconds=60)
        assert creds.authorization_header() == ("Authorization", "Bearer first_token")

        expected_token = "second_token" if should_refresh else "first_token"
        assert creds.authorization_header() == ("Authorization", f"Bearer {expected_token}")
        assert len(httpx2_mock.get_requests()) == (2 if should_refresh else 1)

    @pytest.mark.parametrize("reserved", ("auth", "body", "grant_type", "headers", "method", "state", "url"))
    def test_OAUTH_CC_RESERVED_TOKEN_CUSTOM_ARGS(self, reserved: str) -> None:
        with pytest.raises(TypeError, match=rf"reserved token custom arg\(s\) were passed: \['{reserved}'\]"):
            OAuthClientCredentials(**{**self.DEFAULT_PROVIDER_ARGS, reserved: "x"})

    def test_pickle_roundtrip_fetches_token(self, httpx2_mock: HTTPXMock) -> None:
        self.add_token_response(httpx2_mock)
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS, timeout=60)
        unpickled = pickle.loads(pickle.dumps(creds))
        assert unpickled.authorization_header() == ("Authorization", "Bearer tok")

        (request,) = httpx2_mock.get_requests()
        assert request.extensions["timeout"] == {"connect": 60, "read": 60, "write": 60, "pool": 60}
        assert _form_fields(request)["client_secret"] == ["azure-client-secret"]
        assert _form_fields(request)["other_custom_arg"] == ["some_value"]

    def test_pickle_roundtrip_preserves_cached_token(self, httpx2_mock: HTTPXMock) -> None:
        self.add_token_response(httpx2_mock, "cached_token")
        creds = OAuthClientCredentials(**self.DEFAULT_PROVIDER_ARGS)
        creds.authorization_header()

        unpickled = pickle.loads(pickle.dumps(creds))
        assert unpickled.authorization_header() == ("Authorization", "Bearer cached_token")
        assert len(httpx2_mock.get_requests()) == 1

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
    DEFAULT_PROVIDER_ARGS: ClassVar[MappingProxyType[str, Any]] = MappingProxyType(
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
        assert creds.authorization_header() == ("Authorization", "Bearer azure_token")

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
