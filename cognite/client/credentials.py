import atexit
import inspect
import tempfile
import threading
import time
from abc import abstractmethod
from pathlib import Path

from msal import PublicClientApplication, SerializableTokenCache
from oauthlib.oauth2 import BackendApplicationClient, OAuth2Error
from requests_oauthlib import OAuth2Session

from cognite.client.exceptions import CogniteAuthError


class CredentialProvider(Protocol):
    @abstractmethod
    def authorization_header(self):
        raise NotImplementedError


class APIKey(CredentialProvider):
    def __init__(self, api_key):
        self.__api_key = api_key

    def authorization_header(self):
        return ("api-key", self.__api_key)


class Token(CredentialProvider):
    def __init__(self, token):
        if isinstance(token, str):
            self.__token_factory = lambda: token
        elif callable(token):
            token_refresh_lock = threading.Lock()

            def thread_safe_get_token() -> str:
                assert not isinstance(token, str)
                with token_refresh_lock:
                    return token()

            self.__token_factory = thread_safe_get_token
        else:
            raise TypeError(f"'token' must be a string or a no-argument-callable returning a string, not {type(token)}")

    def authorization_header(self):
        return ("Authorization", f"Bearer {self.__token_factory()}")


class _OAuthCredentialProviderWithTokenRefresh(CredentialProvider):
    __TOKEN_EXPIRY_LEEWAY_SECONDS = 3

    def __init__(self):
        self.__token_refresh_lock = threading.Lock()
        self.__access_token: Optional[str] = None
        self.__access_token_expires_at: Optional[float] = None

    @abstractmethod
    def _refresh_access_token(self):
        raise NotImplementedError

    @classmethod
    def __should_refresh_token(cls, token, expires_at):
        no_token = token is None
        token_is_expired = (expires_at is None) or (time.time() > (expires_at - cls.__TOKEN_EXPIRY_LEEWAY_SECONDS))
        return no_token or token_is_expired

    @staticmethod
    def _verify_credentials(credentials):
        if ("access_token" in credentials) and ("expires_in" in credentials):
            return
        err_descr = " ".join(credentials.get("error_description", "").splitlines())
        raise CogniteAuthError(
            f"Error generating access token! Error: {credentials['error']}, error description: {err_descr}"
        )

    def authorization_header(self):
        with self.__token_refresh_lock:
            if self.__should_refresh_token(self.__access_token, self.__access_token_expires_at):
                (self.__access_token, self.__access_token_expires_at) = self._refresh_access_token()
        return ("Authorization", f"Bearer {self.__access_token}")


class _WithMsalSerializableTokenCache:
    @staticmethod
    def _create_serializable_token_cache(cache_path):
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


class OAuthDeviceCode(_OAuthCredentialProviderWithTokenRefresh, _WithMsalSerializableTokenCache):
    def __init__(self, authority_url, client_id, scopes, token_cache_path=None):
        from cognite.client.config import global_config

        super().__init__()
        self.__authority_url = authority_url
        self.__client_id = client_id
        self.__scopes = scopes
        token_cache_path = token_cache_path or (
            Path(tempfile.gettempdir()) / f"cognitetokencache.{self.__client_id}.bin"
        )
        serializable_token_cache = self._create_serializable_token_cache(token_cache_path)
        self.__app = PublicClientApplication(
            client_id=self.__client_id,
            authority=self.__authority_url,
            token_cache=serializable_token_cache,
            verify=(not global_config.disable_ssl),
        )

    @property
    def authority_url(self):
        return self.__authority_url

    @property
    def client_id(self):
        return self.__client_id

    @property
    def scopes(self):
        return self.__scopes

    def _refresh_access_token(self):
        accounts = self.__app.get_accounts()
        credentials = self.__app.acquire_token_silent(scopes=self.__scopes, account=accounts[0]) if accounts else None
        if credentials is None:
            device_flow = self.__app.initiate_device_flow(scopes=self.__scopes)
            print(f"Device code: {device_flow['message']}")
            credentials = self.__app.acquire_token_by_device_flow(flow=device_flow)
        self._verify_credentials(credentials)
        return (credentials["access_token"], (time.time() + credentials["expires_in"]))


class OAuthInteractive(_OAuthCredentialProviderWithTokenRefresh, _WithMsalSerializableTokenCache):
    def __init__(self, authority_url, client_id, scopes, redirect_port=53000, token_cache_path=None):
        from cognite.client.config import global_config

        super().__init__()
        self.__authority_url = authority_url
        self.__client_id = client_id
        self.__scopes = scopes
        self.__redirect_port = redirect_port
        token_cache_path = token_cache_path or (
            Path(tempfile.gettempdir()) / f"cognitetokencache.{self.__client_id}.bin"
        )
        serializable_token_cache = self._create_serializable_token_cache(token_cache_path)
        self.__app = PublicClientApplication(
            client_id=self.__client_id,
            authority=self.__authority_url,
            token_cache=serializable_token_cache,
            verify=(not global_config.disable_ssl),
        )

    @property
    def authority_url(self):
        return self.__authority_url

    @property
    def client_id(self):
        return self.__client_id

    @property
    def scopes(self):
        return self.__scopes

    def _refresh_access_token(self):
        accounts = self.__app.get_accounts()
        credentials = self.__app.acquire_token_silent(scopes=self.__scopes, account=accounts[0]) if accounts else None
        if credentials is None:
            credentials = self.__app.acquire_token_interactive(scopes=self.__scopes, port=self.__redirect_port)
        self._verify_credentials(credentials)
        return (credentials["access_token"], (time.time() + credentials["expires_in"]))


class OAuthClientCredentials(_OAuthCredentialProviderWithTokenRefresh):
    def __init__(self, token_url, client_id, client_secret, scopes, **token_custom_args):
        super().__init__()
        self.__token_url = token_url
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__scopes = scopes
        self.__token_custom_args = token_custom_args
        self.__oauth = OAuth2Session(client=BackendApplicationClient(client_id=self.__client_id, scope=self.__scopes))
        self._validate_token_custom_args()

    def _validate_token_custom_args(self):
        reserved = set(inspect.signature(self.__oauth.fetch_token).parameters) - {"kwargs"}
        bad_args = reserved.intersection(self.__token_custom_args)
        if bad_args:
            raise TypeError(
                f"The following reserved token custom arg(s) were passed: {sorted(bad_args)}. The full list of reserved custom args is: {sorted(reserved)}."
            )

    @property
    def token_url(self):
        return self.__token_url

    @property
    def client_id(self):
        return self.__client_id

    @property
    def client_secret(self):
        return self.__client_secret

    @property
    def scopes(self):
        return self.__scopes

    @property
    def token_custom_args(self):
        return self.__token_custom_args

    def _refresh_access_token(self):
        from cognite.client.config import global_config

        try:
            token_result = self.__oauth.fetch_token(
                token_url=self.__token_url,
                verify=(not global_config.disable_ssl),
                client_secret=self.__client_secret,
                **self.__token_custom_args,
            )
            return (token_result["access_token"], token_result["expires_at"])
        except OAuth2Error as oauth_err:
            raise CogniteAuthError(
                f"Error generating access token: {oauth_err.error}, {oauth_err.status_code}, {oauth_err.description}"
            ) from oauth_err
