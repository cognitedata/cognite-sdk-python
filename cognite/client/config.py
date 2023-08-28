from __future__ import annotations

import getpass
import pprint
from contextlib import suppress

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
        max_retries_connect (int): Max number of retries on connection errors. Defaults to 3.
        max_retry_backoff (int): Retry strategy employs exponential backoff. This parameter sets a max on the amount of
            backoff after any request failure. Defaults to 30.
        max_connection_pool_size (int): The maximum number of connections which will be kept in the SDKs connection pool.
            Defaults to 50.
        disable_ssl (bool): Whether or not to disable SSL. Defaults to False
        proxies (Dict[str, str]): Dictionary mapping from protocol to url. e.g. {"https": "http://10.10.1.10:1080"}
    """

    def __init__(self) -> None:
        self.default_client_config: ClientConfig | None = None
        self.disable_gzip: bool = False
        self.disable_pypi_version_check: bool = False
        self.status_forcelist: set[int] = {429, 502, 503, 504}
        self.max_retries: int = 10
        self.max_retries_connect: int = 3
        self.max_retry_backoff: int = 30
        self.max_connection_pool_size: int = 50
        self.disable_ssl: bool = False
        self.proxies: dict[str, str] | None = {}


global_config = GlobalConfig()


class ClientConfig:
    """Configuration object for the client

    Args:
        client_name (str): A user-defined name for the client. Used to identify number of unique applications/scripts running on top of CDF.
        project (str): CDF Project name.
        credentials (CredentialProvider): Credentials. e.g. Token, ClientCredentials.
        api_subversion (str | None): API subversion
        base_url (str | None): Base url to send requests to. Defaults to "https://api.cognitedata.com"
        max_workers (int | None): Max number of workers to spawn when parallelizing data fetching. Defaults to 10.
        headers (dict[str, str] | None): Additional headers to add to all requests.
        timeout (int | None): Timeout on requests sent to the api. Defaults to 30 seconds.
        file_transfer_timeout (int | None): Timeout on file upload/download requests. Defaults to 600 seconds.
        debug (bool): Configures logger to log extra request details to stderr.
    """

    def __init__(
        self,
        client_name: str,
        project: str,
        credentials: CredentialProvider,
        api_subversion: str | None = None,
        base_url: str | None = None,
        max_workers: int | None = None,
        headers: dict[str, str] | None = None,
        timeout: int | None = None,
        file_transfer_timeout: int | None = None,
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
            from cognite.client.utils._logging import _configure_logger_for_debug_mode

            _configure_logger_for_debug_mode()

        if not global_config.disable_pypi_version_check:
            with suppress(Exception):  # PyPI might be unreachable, if so, skip version check
                from cognite.client.utils._auxiliary import _check_client_has_newest_major_version

                _check_client_has_newest_major_version()
        self._validate_config()

    def _validate_config(self) -> None:
        if not self.project:
            raise ValueError(f"Invalid value for ClientConfig.project: <{self.project}>")

    def __str__(self) -> str:
        return pprint.pformat(self.__dict__, indent=4)

    def _repr_html_(self) -> str:
        return str(self)

    @classmethod
    def default(
        cls, project: str, cdf_cluster: str, credentials: CredentialProvider, client_name: str | None = None
    ) -> ClientConfig:
        """Create a default client config object.

        Args:
            project (str): CDF Project name.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            credentials (CredentialProvider): Credentials. e.g. Token, ClientCredentials.
            client_name (str | None): A user-defined name for the client. Used to identify the number of unique applications/scripts running on top of CDF. If this is not set, the getpass.getuser() is used instead, meaning the username you are logged in with is used.

        Returns:
            ClientConfig: A default client config object.
        """

        return cls(
            client_name=client_name or getpass.getuser(),
            project=project,
            credentials=credentials,
            base_url=f"https://{cdf_cluster}.cognitedata.com/",
        )
