import pprint
from contextlib import suppress
from typing import Dict, Optional, Set

from cognite.client import utils
from cognite.client._version import __api_subversion__
from cognite.client.credentials import CredentialProvider


class GlobalConfig:
    """Global configuration object

    Attributes:
        default_client_config (Optional[ClientConfig]): A default instance of a client configuration. This will be used
            by the CogniteClient constructor if no config is passed directly. Defaults to None.
        disable_gzip (bool): Whether or not to disable gzipping of json bodies. Defaults to False.
        disable_pypi_version_check (bool): Whether or not to check for newer SDK versions when instantiating a new client.
            Defaults to False.
        status_forcelist (Set[int]): HTTP status codes to retry. Defaults to {429, 502, 503, 504}
        max_retries (int): Max number of retries on a given http request. Defaults to 10.
        max_retry_backoff (int): Retry strategy employs exponential backoff. This parameter sets a max on the amount of
            backoff after any request failure. Defaults to 30.
        max_connection_pool_size (int): The maximum number of connections which will be kept in the SDKs connection pool.
            Defaults to 50.
        disable_ssl (bool): Whether or not to disable SSL. Defaults to False
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
        project (str): CDF Project name.
        credentials (CredentialProvider): Credentials. e.g. APIKey, Token, ClientCredentials.
        api_subversion (str): API subversion
        base_url (str): Base url to send requests to. Defaults to "https://api.cognitedata.com"
        max_workers (int): Max number of workers to spawn when parallelizing data fetching. Defaults to 10.
        headers (Dict): Additional headers to add to all requests.
        timeout (int): Timeout on requests sent to the api. Defaults to 30 seconds.
        file_transfer_timeout (int): Timeout on file upload/download requests. Defaults to 600 seconds.
        debug (bool): Configures logger to log extra request details to stderr.
    """

    def __init__(
        self,
        client_name: str,
        project: str,
        credentials: CredentialProvider,
        api_subversion: Optional[str] = None,
        base_url: Optional[str] = None,
        max_workers: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        file_transfer_timeout: Optional[int] = None,
        debug: bool = False,
    ) -> None:
        self.client_name = client_name
        self.project = project
        self.credentials = credentials
        self.api_subversion = api_subversion or __api_subversion__
        self.base_url = (base_url or "https://api.cognitedata.com").rstrip("/")
        self.max_workers = max_workers if max_workers is not None else 10
        self.headers = headers or {}
        self.timeout = timeout or 30
        self.file_transfer_timeout = file_transfer_timeout or 600
        self.debug = debug

        if debug:
            utils._logging._configure_logger_for_debug_mode()

        if not global_config.disable_pypi_version_check:
            with suppress(Exception):  # PyPI might be unreachable, if so, skip version check
                utils._auxiliary._check_client_has_newest_major_version()

    def __str__(self) -> str:
        return pprint.pformat(self.__dict__, indent=4)

    def _repr_html_(self) -> str:
        return str(self)
