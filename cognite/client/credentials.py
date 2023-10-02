from __future__ import annotations

import atexit
import inspect
import tempfile
import threading
import time
from abc import abstractmethod
from pathlib import Path
from typing import Any, Callable, Protocol

from msal import ConfidentialClientApplication, PublicClientApplication, SerializableTokenCache
from oauthlib.oauth2 import BackendApplicationClient, OAuth2Error
from requests_oauthlib import OAuth2Session

from cognite.client.exceptions import CogniteAuthError

_TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT = 15  # Do not change without also updating all the docstrings using it


class CredentialProvider(Protocol):
    @abstractmethod
    def authorization_header(self) -> tuple[str, str]:
        raise NotImplementedError


class Token(CredentialProvider):
    """Token credential provider

    Args:
        token (str | Callable[[], str]): A token or a token factory.

    Examples:

            >>> from cognite.client.credentials import Token
            >>> token_provider = Token("my secret token")
            >>> token_factory_provider = Token(lambda: "my secret token")

    Note:
        If you pass in a callable, we will expect that you supplied a function that may do a token refresh
        under the hood, so it will be called while holding a thread lock (threading.Lock()).
    """

    def __init__(self, token: str | Callable[[], str]) -> None:
        if isinstance(token, str):
            self.__token_factory = lambda: token

        elif callable(token):  # mypy flat out refuses variations of: isinstance(token, collections.abc.Callable)
            token_refresh_lock = threading.Lock()

            def thread_safe_get_token() -> str:
                assert not isinstance(token, str)  # unbelievable
                with token_refresh_lock:
                    return token()

            self.__token_factory = thread_safe_get_token
        else:
            raise TypeError(f"'token' must be a string or a no-argument-callable returning a string, not {type(token)}")

    def authorization_header(self) -> tuple[str, str]:
        return "Authorization", f"Bearer {self.__token_factory()}"


class _OAuthCredentialProviderWithTokenRefresh(CredentialProvider):
    def __init__(self, token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT) -> None:
        # This ensures we don't return a token which expires immediately:
        self.token_expiry_leeway_seconds = token_expiry_leeway_seconds

        self.__token_refresh_lock = threading.Lock()
        self.__access_token: str | None = None
        self.__access_token_expires_at: float | None = None

    def __getstate__(self) -> dict[str, Any]:
        # threading.Lock is not picklable, temporarily remove:
        lock_tmp, self.__token_refresh_lock = self.__token_refresh_lock, None  # type: ignore [assignment]
        state = self.__dict__.copy()
        self.__token_refresh_lock = lock_tmp
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__ = state
        self.__token_refresh_lock = threading.Lock()

    @abstractmethod
    def _refresh_access_token(self) -> tuple[str, float]:
        """This method should return the access_token and expiry time"""
        raise NotImplementedError

    def __should_refresh_token(self, token: str | None, expires_at: float | None) -> bool:
        no_token = token is None
        token_is_expired = expires_at is None or time.time() > expires_at - self.token_expiry_leeway_seconds
        return no_token or token_is_expired

    @staticmethod
    def _verify_credentials(credentials: dict[str, Any]) -> None:
        """The msal library doesn't raise anything when auth fails, but returns a dictionary with varying keys"""
        if "access_token" in credentials and "expires_in" in credentials:
            return

        # 'error_description' includes Windows-style newlines \r\n meant to print nicely. Prettify for exception:
        err_descr = " ".join(credentials.get("error_description", "").splitlines())
        raise CogniteAuthError(
            f"Error generating access token! Error: {credentials['error']}, error description: {err_descr}"
        )

    def authorization_header(self) -> tuple[str, str]:
        # We lock here to ensure we don't issue a herd of refresh requests in concurrent scenarios.
        # This means we will block all requests for the duration of the token refresh.
        # TODO: Consider instead having a background thread periodically refresh the token to avoid this blocking.
        with self.__token_refresh_lock:
            if self.__should_refresh_token(self.__access_token, self.__access_token_expires_at):
                self.__access_token, self.__access_token_expires_at = self._refresh_access_token()
        return "Authorization", f"Bearer {self.__access_token}"


class _WithMsalSerializableTokenCache:
    @staticmethod
    def _create_serializable_token_cache(cache_path: Path) -> SerializableTokenCache:
        token_cache = SerializableTokenCache()

        if cache_path.exists():
            with cache_path.open() as fh:
                token_cache.deserialize(fh.read())

        def __at_exit() -> None:
            if token_cache.has_state_changed:
                with open(cache_path, "w+") as fh:
                    fh.write(token_cache.serialize())

        atexit.register(__at_exit)
        return token_cache

    @staticmethod
    def _resolve_token_cache_path(token_cache_path: Path | None, client_id: str) -> Path:
        return token_cache_path or Path(tempfile.gettempdir()) / f"cognitetokencache.{client_id}.bin"

    def _create_client_app(self, token_cache_path: Path, client_id: str, authority_url: str) -> PublicClientApplication:
        from cognite.client.config import global_config

        # In addition to caching in memory, we also cache the token on disk so it can be reused across processes:
        serializable_token_cache = self._create_serializable_token_cache(token_cache_path)
        return PublicClientApplication(
            client_id=client_id,
            authority=authority_url,
            token_cache=serializable_token_cache,
            verify=not global_config.disable_ssl,
        )


class OAuthDeviceCode(_OAuthCredentialProviderWithTokenRefresh, _WithMsalSerializableTokenCache):
    """OAuth credential provider for the device code login flow.

    Args:
        authority_url (str): OAuth authority url
        client_id (str): Your application's client id.
        scopes (list[str]): A list of scopes.
        token_cache_path (Path | None): Location to store token cache, defaults to os temp directory/cognitetokencache.{client_id}.bin.
        token_expiry_leeway_seconds (int): The token is refreshed at the earliest when this number of seconds is left before expiry. Default: 15 sec

    Examples:

            >>> from cognite.client.credentials import OAuthDeviceCode
            >>> oauth_provider = OAuthDeviceCode(
            ...     authority_url="https://login.microsoftonline.com/xyz",
            ...     client_id="abcd",
            ...     scopes=["https://greenfield.cognitedata.com/.default"],
            ... )
    """

    def __init__(
        self,
        authority_url: str,
        client_id: str,
        scopes: list[str],
        token_cache_path: Path | None = None,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
    ) -> None:
        super().__init__(token_expiry_leeway_seconds)
        self.__authority_url = authority_url
        self.__client_id = client_id
        self.__scopes = scopes

        self._token_cache_path = self._resolve_token_cache_path(token_cache_path, client_id)
        self.__app = self._create_client_app(self._token_cache_path, client_id, authority_url)

    def __getstate__(self) -> dict[str, Any]:
        # PublicClientApplication is not picklable, temporarily remove:
        app_tmp, self.__app = self.__app, None
        state = super().__getstate__()
        self.__app = app_tmp
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        super().__setstate__(state)
        self.__app = self._create_client_app(self._token_cache_path, self.__client_id, self.__authority_url)

    @property
    def authority_url(self) -> str:
        return self.__authority_url

    @property
    def client_id(self) -> str:
        return self.__client_id

    @property
    def scopes(self) -> list[str]:
        return self.__scopes

    def _refresh_access_token(self) -> tuple[str, float]:
        # First check if a token cache exists on disk. If yes, find and use:
        # - A valid access token.
        # - A valid refresh token, and if so, use it automatically to redeem a new access token.
        credentials = None
        if accounts := self.__app.get_accounts():
            credentials = self.__app.acquire_token_silent(scopes=self.__scopes, account=accounts[0])

        # If we're unable to find (or acquire a new) access token, we initiate the device code auth flow:
        if credentials is None:
            device_flow = self.__app.initiate_device_flow(scopes=self.__scopes)
            # print device code user instructions to screen
            print(f"Device code: {device_flow['message']}")  # noqa: T201
            credentials = self.__app.acquire_token_by_device_flow(flow=device_flow)

        self._verify_credentials(credentials)
        return credentials["access_token"], time.time() + credentials["expires_in"]


class OAuthInteractive(_OAuthCredentialProviderWithTokenRefresh, _WithMsalSerializableTokenCache):
    """OAuth credential provider for an interactive login flow.

    Make sure you have http://localhost:port in Redirect URI in App Registration as type "Mobile and desktop applications".

    Args:
        authority_url (str): OAuth authority url
        client_id (str): Your application's client id.
        scopes (list[str]): A list of scopes.
        redirect_port (int): Redirect port defaults to 53000.
        token_cache_path (Path | None): Location to store token cache, defaults to os temp directory/cognitetokencache.{client_id}.bin.
        token_expiry_leeway_seconds (int): The token is refreshed at the earliest when this number of seconds is left before expiry. Default: 15 sec

    Examples:

            >>> from cognite.client.credentials import OAuthInteractive
            >>> oauth_provider = OAuthInteractive(
            ...     authority_url="https://login.microsoftonline.com/xyz",
            ...     client_id="abcd",
            ...     scopes=["https://greenfield.cognitedata.com/.default"],
            ... )
    """

    def __init__(
        self,
        authority_url: str,
        client_id: str,
        scopes: list[str],
        redirect_port: int = 53000,
        token_cache_path: Path | None = None,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
    ) -> None:
        super().__init__(token_expiry_leeway_seconds)
        self.__authority_url = authority_url
        self.__client_id = client_id
        self.__scopes = scopes
        self.__redirect_port = redirect_port

        self._token_cache_path = self._resolve_token_cache_path(token_cache_path, client_id)
        self.__app = self._create_client_app(self._token_cache_path, client_id, authority_url)

    def __getstate__(self) -> dict[str, Any]:
        # PublicClientApplication is not picklable, temporarily remove:
        app_tmp, self.__app = self.__app, None
        state = super().__getstate__()
        self.__app = app_tmp
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        super().__setstate__(state)
        self.__app = self._create_client_app(self._token_cache_path, self.__client_id, self.__authority_url)

    @property
    def authority_url(self) -> str:
        return self.__authority_url

    @property
    def client_id(self) -> str:
        return self.__client_id

    @property
    def scopes(self) -> list[str]:
        return self.__scopes

    def _refresh_access_token(self) -> tuple[str, float]:
        # First check if a token cache exists on disk. If yes, find and use:
        # - A valid access token.
        # - A valid refresh token, and if so, use it automatically to redeem a new access token.
        credentials = None
        if accounts := self.__app.get_accounts():
            credentials = self.__app.acquire_token_silent(scopes=self.__scopes, account=accounts[0])

        # If we're unable to find (or acquire a new) access token, we initiate the interactive auth flow:
        if credentials is None:
            credentials = self.__app.acquire_token_interactive(scopes=self.__scopes, port=self.__redirect_port)

        self._verify_credentials(credentials)
        return credentials["access_token"], time.time() + credentials["expires_in"]

    @classmethod
    def default_for_azure_ad(
        cls,
        tenant_id: str,
        client_id: str,
        cdf_cluster: str,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
        **token_custom_args: Any,
    ) -> OAuthInteractive:
        """
        Create an OAuthClientCredentials instance for Azure with default token URL and scopes.

        The default configuration creates the URLs based on the tenant id and cluster:

        * Authority URL: "https://login.microsoftonline.com/{tenant_id}"
        * Scopes: [f"https://{cdf_cluster}.cognitedata.com/.default"]

        Args:
            tenant_id (str): The Azure tenant id
            client_id (str): Your application's client id.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            token_expiry_leeway_seconds (int): The token is refreshed at the earliest when this number of seconds is left before expiry. Default: 15 sec
            **token_custom_args (Any): Optional additional arguments to pass as query parameters to the token fetch request.

        Returns:
            OAuthInteractive: An OAuthInteractive instance
        """
        return cls(
            authority_url=f"https://login.microsoftonline.com/{tenant_id}",
            client_id=client_id,
            scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
            token_expiry_leeway_seconds=token_expiry_leeway_seconds,
            **token_custom_args,
        )


class OAuthClientCredentials(_OAuthCredentialProviderWithTokenRefresh):
    """OAuth credential provider for the "Client Credentials" flow.

    Args:
        token_url (str): OAuth token url
        client_id (str): Your application's client id.
        client_secret (str): Your application's client secret
        scopes (list[str]): A list of scopes.
        token_expiry_leeway_seconds (int): The token is refreshed at the earliest when this number of seconds is left before expiry. Default: 15 sec
        **token_custom_args (Any): Optional additional arguments to pass as query parameters to the token fetch request.

    Examples:

            >>> from cognite.client.credentials import OAuthClientCredentials
            >>> import os
            >>> oauth_provider = OAuthClientCredentials(
            ...     token_url="https://login.microsoftonline.com/xyz/oauth2/v2.0/token",
            ...     client_id="abcd",
            ...     client_secret=os.environ["OAUTH_CLIENT_SECRET"],
            ...     scopes=["https://greenfield.cognitedata.com/.default"],
            ...     # Any additional IDP-specific token args. e.g.
            ...     audience="some-audience"
            ... )
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scopes: list[str],
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
        **token_custom_args: Any,
    ) -> None:
        super().__init__(token_expiry_leeway_seconds)
        self.__token_url = token_url
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__scopes = scopes
        self.__token_custom_args: dict[str, Any] = token_custom_args
        self.__oauth = self._create_oauth_session()
        self._validate_token_custom_args()

    def _create_oauth_session(self) -> OAuth2Session:
        return OAuth2Session(client=BackendApplicationClient(client_id=self.__client_id, scope=self.__scopes))

    def _validate_token_custom_args(self) -> None:
        # We make sure that whatever is passed as part of 'token_custom_args' can't set or override any of the
        # named parameters that 'fetch_token' accepts:
        reserved = set(inspect.signature(self.__oauth.fetch_token).parameters) - {"kwargs"}
        if bad_args := reserved.intersection(self.__token_custom_args):
            raise TypeError(
                f"The following reserved token custom arg(s) were passed: {sorted(bad_args)}. The full list of "
                f"reserved custom args is: {sorted(reserved)}."
            )

    def __getstate__(self) -> dict[str, Any]:
        # OAuth2Session is not picklable, temporarily remove:
        oauth_session_tmp, self.__oauth = self.__oauth, None
        state = super().__getstate__()
        self.__oauth = oauth_session_tmp
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        super().__setstate__(state)
        self.__oauth = self._create_oauth_session()

    @property
    def token_url(self) -> str:
        return self.__token_url

    @property
    def client_id(self) -> str:
        return self.__client_id

    @property
    def client_secret(self) -> str:
        return self.__client_secret

    @property
    def scopes(self) -> list[str]:
        return self.__scopes

    @property
    def token_custom_args(self) -> dict[str, Any]:
        return self.__token_custom_args

    def _refresh_access_token(self) -> tuple[str, float]:
        from cognite.client.config import global_config

        try:
            token_result = self.__oauth.fetch_token(
                token_url=self.__token_url,
                verify=not global_config.disable_ssl,
                client_secret=self.__client_secret,
                **self.__token_custom_args,
            )
            return token_result["access_token"], token_result["expires_at"]
        except OAuth2Error as oauth_err:
            raise CogniteAuthError(
                f"Error generating access token: {oauth_err.error}, {oauth_err.status_code}, {oauth_err.description}"
            ) from oauth_err

    @classmethod
    def default_for_azure_ad(
        cls,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        cdf_cluster: str,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
        **token_custom_args: Any,
    ) -> OAuthClientCredentials:
        """
        Create an OAuthClientCredentials instance for Azure with default token URL and scopes.

        The default configuration creates the URLs based on the tenant id and cluster/oauth2/v2.0/token:

        * Token URL: "https://login.microsoftonline.com/{tenant_id}"
        * Scopes: [f"https://{cdf_cluster}.cognitedata.com/.default"]

        Args:
            tenant_id (str): The Azure tenant id
            client_id (str): Your application's client id.
            client_secret (str): Your application's client secret.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            token_expiry_leeway_seconds (int): The token is refreshed at the earliest when this number of seconds is left before expiry. Default: 15 sec
            **token_custom_args (Any): Optional additional arguments to pass as query parameters to the token fetch request.

        Returns:
            OAuthClientCredentials: An OAuthClientCredentials instance
        """
        return cls(
            token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
            token_expiry_leeway_seconds=token_expiry_leeway_seconds,
            **token_custom_args,
        )


class OAuthClientCertificate(_OAuthCredentialProviderWithTokenRefresh):
    """OAuth credential provider for authenticating with a client certificate.

    Args:
        authority_url (str): OAuth authority url
        client_id (str): Your application's client id.
        cert_thumbprint (str): Your certificate's thumbprint. You get it when you upload your certificate to Azure AD.
        certificate (str): Your private certificate, typically read from a .pem file
        scopes (list[str]): A list of scopes.
        token_expiry_leeway_seconds (int): The token is refreshed at the earliest when this number of seconds is left before expiry. Default: 15 sec

    Examples:

            >>> from cognite.client.credentials import OAuthClientCertificate
            >>> from pathlib import Path
            >>> oauth_provider = OAuthClientCertificate(
            ...     authority_url="https://login.microsoftonline.com/xyz",
            ...     client_id="abcd",
            ...     cert_thumbprint="XYZ123",
            ...     certificate=Path("certificate.pem").read_text(),
            ...     scopes=["https://greenfield.cognitedata.com/.default"],
            ... )
    """

    def __init__(
        self,
        authority_url: str,
        client_id: str,
        cert_thumbprint: str,
        certificate: str,
        scopes: list[str],
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
    ) -> None:
        super().__init__(token_expiry_leeway_seconds)
        self.__authority_url = authority_url
        self.__client_id = client_id
        self.__cert_thumbprint = cert_thumbprint
        self.__certificate = certificate
        self.__scopes = scopes

        self.__app = ConfidentialClientApplication(
            client_id=self.__client_id,
            authority=self.__authority_url,
            client_credential={"thumbprint": self.__cert_thumbprint, "private_key": self.__certificate},
        )

    @property
    def authority_url(self) -> str:
        return self.__authority_url

    @property
    def client_id(self) -> str:
        return self.__client_id

    @property
    def cert_thumbprint(self) -> str:
        return self.__cert_thumbprint

    @property
    def certificate(self) -> str:
        return self.__certificate

    @property
    def scopes(self) -> list[str]:
        return self.__scopes

    def _refresh_access_token(self) -> tuple[str, float]:
        credentials = self.__app.acquire_token_for_client(scopes=self.__scopes)

        self._verify_credentials(credentials)
        return credentials["access_token"], time.time() + credentials["expires_in"]
