import atexit
import threading
from abc import abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple, Union

from msal import PublicClientApplication, SerializableTokenCache
from oauthlib.oauth2 import BackendApplicationClient, OAuth2Error
from requests_oauthlib import OAuth2Session

from cognite.client.exceptions import CogniteAuthError


class CredentialProvider(Protocol):
    @abstractmethod
    def authorization_header(self) -> Tuple[str, str]:
        raise NotImplementedError


class APIKey(CredentialProvider):
    """API key credential provider

    Args:
        api_key (str): The api key

    Examples:
        We recommend loading the API key from an environment variable. e.g.

            >>> from cognite.client.credentials import APIKey
            >>> import os
            >>> api_key_provider = APIKey(os.environ["MY_SECRET_CDF_API_KEY"])
    """

    def __init__(self, api_key: str) -> None:
        self.__api_key = api_key

    def authorization_header(self) -> Tuple[str, str]:
        return "api-key", self.__api_key


class Token(CredentialProvider):
    """Token credential provider

    Args:
        token (Union[str, Callable[[], str]): A token or a token factory.

    Examples:

            >>> from cognite.client.credentials import Token
            >>> token_provider = Token("my secret token")
            >>> token_factory_provider = Token(lambda: "my secret token")
    """

    def __init__(self, token: Union[str, Callable[[], str]]) -> None:
        if isinstance(token, str):
            self.__token_factory = lambda: token
        else:
            self.__token_factory = token

    def authorization_header(self) -> Tuple[str, str]:
        return "Authorization", f"Bearer {self.__token_factory()}"


class _OAuthCredentialProviderWithTokenRefresh(CredentialProvider):
    # This ensures we don't return a token which expires immediately, but within minimum 3 seconds.
    __TOKEN_EXPIRY_LEEWAY_SECONDS = 3

    def __init__(self) -> None:
        self.__token_refresh_lock = threading.Lock()
        self.__access_token: Optional[str] = None
        self.__access_token_expires_at: Optional[float] = None

    @abstractmethod
    def _refresh_access_token(self) -> Tuple[str, float]:
        """This method should return the access_token and expiry time"""
        raise NotImplementedError

    @classmethod
    def __should_refresh_token(cls, token: Optional[str], expires_at: Optional[float]) -> bool:
        no_token = token is None
        token_is_expired = (
            expires_at is None or datetime.now().timestamp() > expires_at - cls.__TOKEN_EXPIRY_LEEWAY_SECONDS
        )
        return no_token or token_is_expired

    def authorization_header(self) -> Tuple[str, str]:
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
                with open(cache_path, "w") as fh:
                    fh.write(token_cache.serialize())

        atexit.register(__at_exit)
        return token_cache


class OAuthDeviceCode(_OAuthCredentialProviderWithTokenRefresh, _WithMsalSerializableTokenCache):
    """OAuth credential provider for the device code login flow.

    Args:
        authority_url (str): OAuth authority url
        client_id (str): Your application's client id.
        scopes (List[str]): A list of scopes.

    Examples:

            >>> from cognite.client.credentials import OAuthInteractive
            >>> oauth_provider = OAuthDeviceCode(
            ...     authority_url="https://login.microsoftonline.com/xyz",
            ...     client_id="abcd",
            ...     scopes=["https://greenfield.cognitedata.com/.default"],
            ... )
    """

    def __init__(self, authority_url: str, client_id: str, scopes: List[str]) -> None:
        super().__init__()
        self.__authority_url = authority_url
        self.__client_id = client_id
        self.__scopes = scopes

        # In addition to caching in memory, we also cache the token on disk so it can be reused across processes.
        token_cache_path = Path(f"/tmp/cognitetokencache.{self.__client_id}.bin")
        serializable_token_cache = self._create_serializable_token_cache(token_cache_path)
        self.__app = PublicClientApplication(
            client_id=self.__client_id, authority=self.__authority_url, token_cache=serializable_token_cache
        )

    @property
    def authority_url(self) -> str:
        return self.__authority_url

    @property
    def client_id(self) -> str:
        return self.__client_id

    @property
    def scopes(self) -> List[str]:
        return self.__scopes

    def _refresh_access_token(self) -> Tuple[str, float]:
        # First check if there is a serialized token cached on disk.
        accounts = self.__app.get_accounts()
        credentials = self.__app.acquire_token_silent(scopes=self.__scopes, account=accounts[0]) if accounts else None

        # If not, we acquire a new token interactively
        if credentials is None:
            device_flow = self.__app.initiate_device_flow(scopes=self.__scopes)
            # print device code to screen
            print(f"Device code: {device_flow['message']}")
            credentials = self.__app.acquire_token_by_device_flow(flow=device_flow)

        return credentials["access_token"], datetime.now().timestamp() + credentials["expires_in"]


class OAuthInteractive(_OAuthCredentialProviderWithTokenRefresh, _WithMsalSerializableTokenCache):
    """OAuth credential provider for an interactive login flow.

    Make sure you have http://localhost:port in Redirect URI in App Registration as type "Mobile and desktop applications".

    Args:
        authority_url (str): OAuth authority url
        client_id (str): Your application's client id.
        scopes (List[str]): A list of scopes.
        redirect_port (List[str]): Redirect port defaults to 53000.

    Examples:

            >>> from cognite.client.credentials import OAuthInteractive
            >>> oauth_provider = OAuthInteractive(
            ...     authority_url="https://login.microsoftonline.com/xyz",
            ...     client_id="abcd",
            ...     scopes=["https://greenfield.cognitedata.com/.default"],
            ... )
    """

    def __init__(self, authority_url: str, client_id: str, scopes: List[str], redirect_port: int = 53000) -> None:
        super().__init__()
        self.__authority_url = authority_url
        self.__client_id = client_id
        self.__scopes = scopes
        self.__redirect_port = redirect_port

        # In addition to caching in memory, we also cache the token on disk so it can be reused across processes.
        token_cache_path = Path(f"/tmp/cognitetokencache.{self.__client_id}.bin")
        serializable_token_cache = self._create_serializable_token_cache(token_cache_path)
        self.__app = PublicClientApplication(
            client_id=self.__client_id, authority=self.__authority_url, token_cache=serializable_token_cache
        )

    @property
    def authority_url(self) -> str:
        return self.__authority_url

    @property
    def client_id(self) -> str:
        return self.__client_id

    @property
    def scopes(self) -> List[str]:
        return self.__scopes

    def _refresh_access_token(self) -> Tuple[str, float]:
        # First check if there is a serialized token cached on disk.
        accounts = self.__app.get_accounts()
        credentials = self.__app.acquire_token_silent(scopes=self.__scopes, account=accounts[0]) if accounts else None

        # If not, we acquire a new token interactively
        if credentials is None:
            credentials = self.__app.acquire_token_interactive(scopes=self.__scopes, port=self.__redirect_port)

        return credentials["access_token"], datetime.now().timestamp() + credentials["expires_in"]


class OAuthClientCredentials(_OAuthCredentialProviderWithTokenRefresh):
    """OAuth credential provider for the "Client Credentials" flow.

    Args:
        token_url (str): OAuth token url
        client_id (str): Your application's client id.
        client_secret (str): Your application's client secret
        scopes (List[str]): A list of scopes.
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
        scopes: List[str],
        **token_custom_args: Any,
    ):
        super().__init__()
        self.__token_url = token_url
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__scopes = scopes
        self.__token_custom_args: Dict[str, Any] = token_custom_args
        self.__oauth = OAuth2Session(client=BackendApplicationClient(client_id=self.__client_id))

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
    def scopes(self) -> List[str]:
        return self.__scopes

    @property
    def token_custom_args(self) -> Dict[str, Any]:
        return self.__token_custom_args

    def _refresh_access_token(self) -> Tuple[str, float]:
        from cognite.client.config import global_config

        try:
            # We need to explicitly pass all the arguments to fetch_token (even if they are the same as the defaults).
            # This will ensure that whatever is passed in token_custom_args can't set/override those args.
            token_result = self.__oauth.fetch_token(
                token_url=self.__token_url,
                code=None,
                authorization_response=None,
                body="",
                auth=None,
                username=None,
                password=None,
                method="POST",
                force_querystring=False,
                timeout=None,
                headers=None,
                verify=not global_config.disable_ssl,
                proxies=None,
                include_client_id=None,
                client_secret=self.__client_secret,
                cert=None,
                client_id=self.__client_id,
                scope=self.__scopes,
                **self.__token_custom_args,
            )
            return token_result["access_token"], token_result["expires_at"]
        except OAuth2Error as oauth_error:
            raise CogniteAuthError(
                "Error generating access token: {0}, {1}, {2}".format(
                    oauth_error.error, oauth_error.status_code, oauth_error.description
                )
            ) from oauth_error
