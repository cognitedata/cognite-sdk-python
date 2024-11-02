from __future__ import annotations

import getpass
import pprint
import re
import warnings
from contextlib import suppress
from typing import Any

from cognite.client._version import __api_subversion__
from cognite.client.credentials import CredentialProvider
from cognite.client.utils._auxiliary import load_resource_to_dict


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
        max_workers (int | None): Max number of workers to spawn when parallelizing API calls. Defaults to 5.
        silence_feature_preview_warnings (bool): Whether or not to silence warnings triggered by using alpha or beta
            features. Defaults to False.
    """

    def __new__(cls) -> GlobalConfig:
        if hasattr(cls, "_instance"):
            raise TypeError(
                "GlobalConfig is a singleton and cannot be instantiated directly. Use `global_config` instead, "
                "`from cognite.client import global_config`, then apply the wanted settings, e.g. `global_config.max_workers = 5`. "
                "Settings are only guaranteed to take effect if applied before instantiating a CogniteClient."
            )

        cls._instance = super().__new__(cls)
        return cls._instance

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
        self.max_workers: int = 5
        self.silence_feature_preview_warnings: bool = False

    def apply_settings(self, settings: dict[str, Any] | str) -> None:
        """Apply settings to the global configuration object from a YAML/JSON string or dict.

        Note:
            All settings in the dictionary will be applied unless an invalid key is provided, a ValueError will instead be raised and no settings will be applied.

        Warning:
            This must be done before instantiating a CogniteClient for the configuration to take effect.

        Args:
            settings (dict[str, Any] | str): A dictionary or YAML/JSON string containing configuration values defined in the GlobalConfig class.

        Examples:

            Apply settings to the global_config from a dictionary input:

                >>> from cognite.client import global_config
                >>> settings = {
                ...     "max_retries": 5,
                ...     "disable_ssl": True,
                ... }
                >>> global_config.apply_settings(settings)
        """

        loaded = load_resource_to_dict(settings).copy()  # doing a shallow copy to avoid mutating the user input config
        current_settings = vars(self)
        if not loaded.keys() <= current_settings.keys():
            raise ValueError(
                f"One or more invalid keys provided for global_config, no settings applied: {loaded.keys() - current_settings}"
            )

        if "default_client_config" in loaded:
            if not isinstance(loaded["default_client_config"], ClientConfig):
                loaded["default_client_config"] = ClientConfig.load(loaded["default_client_config"])

        current_settings.update(loaded)


global_config = GlobalConfig()


class ClientConfig:
    """Configuration object for the client

    Args:
        client_name (str): A user-defined name for the client. Used to identify number of unique applications/scripts running on top of CDF.
        project (str): CDF Project name.
        credentials (CredentialProvider): Credentials. e.g. Token, ClientCredentials.
        api_subversion (str | None): API subversion
        base_url (str | None): Base url to send requests to. Defaults to "https://api.cognitedata.com"
        max_workers (int | None): DEPRECATED. Use global_config.max_workers instead.
            Max number of workers to spawn when parallelizing data fetching. Defaults to 5.
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
        if max_workers is not None:
            # TODO: Remove max_workers from ClientConfig in next major version
            self.max_workers = max_workers  # Will trigger a deprecation warning
        self.headers = headers or {}
        self.timeout = timeout or 30
        self.file_transfer_timeout = file_transfer_timeout or 600

        if debug:
            self.debug = True

        if not global_config.disable_pypi_version_check:
            with suppress(Exception):  # PyPI might be unreachable, if so, skip version check
                from cognite.client.utils._auxiliary import _check_client_has_newest_major_version

                _check_client_has_newest_major_version()
        self._validate_config()

    @property
    def max_workers(self) -> int:
        return global_config.max_workers

    @max_workers.setter
    def max_workers(self, value: int) -> None:
        global_config.max_workers = value
        warnings.warn(
            "Passing (or setting) max_workers to ClientConfig is deprecated. Please use global_config.max_workers instead",
            DeprecationWarning,
        )

    @property
    def debug(self) -> bool:
        from cognite.client.utils._logging import _is_debug_logging_enabled

        return _is_debug_logging_enabled()

    @debug.setter
    def debug(self, value: bool) -> None:
        from cognite.client.utils._logging import _configure_logger_for_debug_mode, _disable_debug_logging

        if value:
            _configure_logger_for_debug_mode()
        else:
            _disable_debug_logging()

    def _validate_config(self) -> None:
        if not self.project:
            raise ValueError(f"Invalid value for ClientConfig.project: <{self.project}>")
        if not self.base_url:
            raise ValueError(f"Invalid value for ClientConfig.base_url: <{self.base_url}>")
        elif self.cdf_cluster is None:
            warnings.warn(f"Given base URL may be invalid, please double-check: {self.base_url!r}", UserWarning)

    def __str__(self) -> str:
        return pprint.pformat({"max_workers": self.max_workers, **self.__dict__}, indent=4)

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
            base_url=f"https://{cdf_cluster}.cognitedata.com",
        )

    @classmethod
    def load(cls, config: dict[str, Any] | str) -> ClientConfig:
        """Load a client config object from a YAML/JSON string or dict.

        Args:
            config (dict[str, Any] | str): A dictionary or YAML/JSON string containing configuration values defined in the ClientConfig class.

        Returns:
            ClientConfig: A client config object.

        Examples:

            Create a client config object from a dictionary input:

                >>> from cognite.client.config import ClientConfig
                >>> import os
                >>> config = {
                ...     "client_name": "abcd",
                ...     "project": "cdf-project",
                ...     "base_url": "https://api.cognitedata.com",
                ...     "credentials": {
                ...         "client_credentials": {
                ...             "client_id": "abcd",
                ...             "client_secret": os.environ["OAUTH_CLIENT_SECRET"],
                ...             "token_url": "https://login.microsoftonline.com/xyz/oauth2/v2.0/token",
                ...             "scopes": ["https://api.cognitedata.com/.default"],
                ...         },
                ...     },
                ... }
                >>> client_config = ClientConfig.load(config)
        """
        loaded = load_resource_to_dict(config)

        if isinstance(loaded["credentials"], CredentialProvider):
            credentials = loaded["credentials"]
        else:
            credentials = CredentialProvider.load(loaded["credentials"])

        return cls(
            client_name=loaded["client_name"],
            project=loaded["project"],
            credentials=credentials,
            api_subversion=loaded.get("api_subversion"),
            base_url=loaded.get("base_url"),
            max_workers=loaded.get("max_workers"),
            headers=loaded.get("headers"),
            timeout=loaded.get("timeout"),
            file_transfer_timeout=loaded.get("file_transfer_timeout"),
            debug=loaded.get("debug", False),
        )

    @property
    def cdf_cluster(self) -> str | None:
        # A best effort attempt to extract the cluster from the base url
        if match := re.match(r"https?://([^/\.\s]+)\.cognitedata\.com(?::\d+)?(?:/|$)", self.base_url):
            return match.group(1)
        return None
