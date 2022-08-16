import pprint
from typing import Any, Callable, Dict, List, Optional, Set, Union

from cognite.client import utils
from cognite.client._version import __api_subversion__
from cognite.client.exceptions import CogniteAuthError


class GlobalConfig:
    """Global configuration object

    Attributes:
        proxies (Dict[str, str]): Dictionary mapping from protocol to url. e.g. {"https": "http://10.10.1.10:1080"}
    """

    def __init__(self) -> None:
        self.default_client_config: Optional[ClientConfig] = None
        self.disable_gzip: bool = False
        self.disable_pypi_version_check: bool = False
        self.status_forcelist: Set[int] = {429, 502, 503, 504}
        self.max_retries: int = 10
        self.max_retry_backoff: int = 30
        self.max_connection_pool_size: int = 50
        self.disable_ssl: bool = False
        self.proxies: Optional[Dict[str, str]] = {}


global_config = GlobalConfig()


class ClientConfig:
    """Configuration object for the client

    Args:
        client_name (str): A user-defined name for the client. Used to identify number of unique applications/scripts
            running on top of CDF.
        project (str): Project. Defaults to project of given API key.
        api_key (str): API key
        api_subversion (str): API subversion
        base_url (str): Base url to send requests to. Defaults to "https://api.cognitedata.com"
        max_workers (int): Max number of workers to spawn when parallelizing data fetching. Defaults to 10.
        headers (Dict): Additional headers to add to all requests.
        timeout (int): Timeout on requests sent to the api. Defaults to 30 seconds.
        file_transfer_timeout (int): Timeout on file upload/download requests. Defaults to 600 seconds.
        token (Union[str, Callable[[], str]]): A jwt or method which takes no arguments and returns a jwt to use for authentication.
            This will override any api-key set.
        token_url (str): Optional url to use for token generation.
            This will override the COGNITE_TOKEN_URL environment variable and only be used if both api-key and token are not set.
        token_client_id (str): Optional client id to use for token generation.
            This will override the COGNITE_CLIENT_ID environment variable and only be used if both api-key and token are not set.
        token_client_secret (str): Optional client secret to use for token generation.
            This will override the COGNITE_CLIENT_SECRET environment variable and only be used if both api-key and token are not set.
        token_scopes (list): Optional list of scopes to use for token generation.
            This will override the COGNITE_TOKEN_SCOPES environment variable and only be used if both api-key and token are not set.
        token_custom_args (Dict): Optional additional arguments to use for token generation.
            This will be passed in as optional additional kwargs to OAuth2Session fetch_token and will only be used if both api-key and token are not set.
        disable_pypi_version_check (bool): Don't check for newer versions of the SDK on client creation
        debug (bool): Configures logger to log extra request details to stderr.
    """

    def __init__(
        self,
        client_name: str,
        project: str,
        api_subversion: Optional[str] = None,
        base_url: Optional[str] = None,
        max_workers: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        file_transfer_timeout: Optional[int] = None,
        api_key: Optional[str] = None,
        token: Optional[Union[Callable[[], str], str]] = None,
        token_url: Optional[str] = None,
        token_client_id: Optional[str] = None,
        token_client_secret: Optional[str] = None,
        token_scopes: Optional[List[str]] = None,
        token_custom_args: Optional[Dict[str, Any]] = None,
        debug: bool = False,
    ) -> None:
        super().__init__()
        self.client_name = client_name
        self.project = project
        self.api_subversion = api_subversion or __api_subversion__
        self.base_url = (base_url or "https://api.cognitedata.com").rstrip("/")
        self.max_workers = max_workers if max_workers is not None else 10
        self.headers = headers or {}
        self.timeout = timeout or 30
        self.file_transfer_timeout = file_transfer_timeout or 600
        self.api_key = api_key
        self.token = token
        self.token_url = token_url
        self.token_client_id = token_client_id
        self.token_client_secret = token_client_secret
        self.token_scopes = token_scopes
        self.token_custom_args = token_custom_args or {}

        if self.api_key is None and self.token is None:
            self.token_custom_args.setdefault("verify", not global_config.disable_ssl)
            # If no api_key or token is present; try setting up a token generator
            token_generator = utils._token_generator.TokenGenerator(
                self.token_url,
                self.token_client_id,
                self.token_client_secret,
                self.token_scopes,
                self.token_custom_args,
            )

            if token_generator.token_params_set():
                self.token = token_generator.return_access_token

            if self.token is None:
                raise CogniteAuthError("No API key or token or token generation arguments have been specified")

        if debug:
            utils._logging._configure_logger_for_debug_mode()

        if not global_config.disable_pypi_version_check:
            try:
                utils._auxiliary._check_client_has_newest_major_version()
            except Exception:
                # PyPI is for some reason not reachable, skip version check
                pass

    def __str__(self) -> str:
        return pprint.pformat(self.__dict__, indent=4)

    def _repr_html_(self) -> str:
        return self.__str__()
