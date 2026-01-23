from __future__ import annotations

import asyncio
import getpass
import pprint
import re
import warnings
from typing import Any, NoReturn, overload

from cognite.client._version import __api_subversion__
from cognite.client.credentials import CredentialProvider
from cognite.client.utils._auxiliary import load_resource_to_dict
from cognite.client.utils._concurrency import ConcurrencySettings
from cognite.client.utils._importing import local_import


class GlobalConfig:
    """Global configuration object

    Attributes:
        default_client_config (Optional[ClientConfig]): A default instance of a client configuration. This will be used
            by the AsyncCogniteClient or CogniteClient constructor if no config is passed directly. Defaults to None.
        disable_gzip (bool): Whether or not to disable gzipping of json bodies. Defaults to False.
        disable_pypi_version_check (bool): Whether or not to check for newer SDK versions when instantiating a new client.
            Defaults to False.
        status_forcelist (Set[int]): HTTP status codes to retry. Defaults to {429, 502, 503, 504}
        max_retries (int): Max number of retries on a given http request. Defaults to 10.
        max_retries_connect (int): Max number of retries on connection errors. Defaults to 3.
        max_retry_backoff (int): Retry strategy employs exponential backoff. This parameter sets a max on the amount of
            backoff after any request failure. Defaults to 60.
        max_connection_pool_size (int): The maximum number of connections which will be kept in the SDKs connection pool.
            Defaults to 20.
        disable_ssl (bool): Whether or not to disable SSL. Defaults to False
        proxy (str | None): Route all traffic (HTTP and HTTPS) via this proxy, e.g. "http://localhost:8030".
            For proxy authentication, embed credentials in the URL: "http://user:pass@localhost:8030".
            Defaults to None (no proxy).
        max_workers (int): DEPRECATED: Use 'concurrency_settings' instead. Maximum number of concurrent API calls. Defaults to 5.
        concurrency_settings (ConcurrencySettings): Settings controlling the maximum number of concurrent API requests
            for different API categories (general, raw, data_modeling etc.). These settings are frozen after the
            first API request is made. See https://cognite-sdk-python.readthedocs-hosted.com/en/latest/settings.html#concurrency-settings
        follow_redirects (bool): Whether or not to follow redirects. Defaults to False.
        file_download_chunk_size (int | None): Specify the file chunk size for streaming file downloads. When not specified
            (default is None), the actual chunk size is determined by the underlying transport, which in turn is based on the
            size of the data packets being read from the network socket. The chunks will be of a variable and unpredictable
            size, but optimized for network efficiency (best download speed).
        file_upload_chunk_size (int | None): Override the chunk size for streaming file uploads. Defaults to None, which
            translates to 65536 (64KiB chunks).
        silence_feature_preview_warnings (bool): Whether or not to silence warnings triggered by using alpha or beta
            features. Defaults to False.
        event_loop (asyncio.AbstractEventLoop | None): Override the default event loop used by the SDK.
    """

    def __new__(cls) -> GlobalConfig:
        if hasattr(cls, "_instance"):
            raise TypeError(
                "GlobalConfig is a singleton and cannot be instantiated directly. Use `global_config` instead, "
                "`from cognite.client import global_config`, then apply the wanted settings, e.g. `global_config.max_retries = 5`. "
                "Settings are only guaranteed to take effect if applied -before- instantiating an AsyncCogniteClient or CogniteClient."
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
        self.max_retry_backoff: int = 60
        self.max_connection_pool_size: int = 20
        self.disable_ssl: bool = False
        self.proxy: str | None = None
        self._max_workers: int = 5
        self._concurrency_settings: ConcurrencySettings = ConcurrencySettings()
        self.follow_redirects: bool = False
        self.file_download_chunk_size: int | None = None
        self.file_upload_chunk_size: int | None = None
        self.silence_feature_preview_warnings: bool = False
        self.event_loop: asyncio.AbstractEventLoop | None = None

    @property
    def max_workers(self) -> int:
        return self._max_workers

    @max_workers.setter
    def max_workers(self, value: int) -> None:
        warnings.warn(
            "'max_workers' is no longer in use in the SDK as of v8, and will be removed in the next major version. "
            "Use 'global_config.concurrency_settings' instead for fine-grained control. For more info: "
            "https://cognite-sdk-python.readthedocs-hosted.com/en/latest/settings.html#concurrency-settings",
            FutureWarning,
            stacklevel=2,
        )
        self._max_workers = value

    @property  # We do not want users to instantiate their own ConcurrencySettings
    def concurrency_settings(self) -> ConcurrencySettings:
        return self._concurrency_settings

    def __str__(self) -> str:
        return pprint.pformat(vars(self), indent=4)

    def _repr_html_(self) -> str:
        pd = local_import("pandas")
        return pd.Series(vars(self)).to_frame("GlobalConfig").sort_index()._repr_html_()

    def apply_settings(self, settings: dict[str, Any] | str) -> None:
        """Apply settings to the global configuration object from a YAML/JSON string or dict.

        Note:
            All settings in the dictionary will be applied unless an invalid key is provided, a ValueError will instead be raised and no settings will be applied.

        Warning:
            This must be done before instantiating an AsyncCogniteClient for the configuration to take effect.

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
        maybe_max_workers = loaded.pop("max_workers", None)
        current_settings = vars(self)
        if not loaded.keys() <= current_settings.keys():
            raise ValueError(
                f"One or more invalid keys provided for global_config, no settings applied: {loaded.keys() - current_settings}"
            )
        if "concurrency_settings" in loaded:
            raise ValueError(
                "Cannot apply 'concurrency_settings' via apply_settings. Modify the individual attributes on "
                "'global_config.concurrency_settings' instead."
            )
        if "default_client_config" in loaded:
            if not isinstance(loaded["default_client_config"], ClientConfig):
                loaded["default_client_config"] = ClientConfig.load(loaded["default_client_config"])

        current_settings.update(loaded)
        # Deprecated, stored using a property, hence the special treatment:
        if maybe_max_workers is not None:
            self.max_workers = maybe_max_workers


global_config = GlobalConfig()


class ClientConfig:
    """Configuration object for the client

    Args:
        client_name (str): A user-defined name for the client. Used to identify number of unique applications/scripts running on top of CDF.
        project (str): CDF Project name.
        credentials (CredentialProvider): Credentials. e.g. Token, ClientCredentials.
        api_subversion (str | None): API subversion
        base_url (str | None): Base url to send requests to. Typically on the form 'https://<cluster>.cognitedata.com'.
            Either base_url or cluster must be provided.
        cluster (str | None): The cluster where the CDF project is located. When passed, it is assumed that the base
            URL can be constructed as: 'https://<cluster>.cognitedata.com'. Either base_url or cluster must be provided.
        headers (dict[str, str] | None): Additional headers to add to all requests.
        timeout (int | None): Timeout on requests sent to the api. Defaults to 60 seconds.
        file_transfer_timeout (int | None): Timeout on file upload/download requests. Defaults to 600 seconds.
        debug (bool): Enables debug logging to stderr. This includes full request/response details and logs regarding retry
            attempts (e.g., on 429 throttling or 5xx errors).
    """

    def __init__(
        self,
        client_name: str,
        project: str,
        credentials: CredentialProvider,
        api_subversion: str | None = None,
        base_url: str | None = None,
        cluster: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: int | None = None,
        file_transfer_timeout: int | None = None,
        debug: bool = False,
    ) -> None:
        self.client_name = client_name
        self.project = project
        self.credentials = credentials
        self.api_subversion = api_subversion or __api_subversion__
        self.base_url = self._validate_base_url_or_cluster(base_url, cluster)
        self._cluster = cluster
        self.headers = headers or {}
        self.timeout = timeout or 60
        self.file_transfer_timeout = file_transfer_timeout or 600
        if debug:
            self.debug = True
        self._validate_config()

        if not global_config.disable_pypi_version_check:
            from cognite.client.utils._version_checker import check_client_is_running_latest_version

            check_client_is_running_latest_version()

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
            raise ValueError(f"Invalid value for ClientConfig.project: {self.project!r}")
        elif self.cdf_cluster is None:
            warnings.warn(f"Given base URL may be invalid, please double-check: {self.base_url!r}", UserWarning)

    @overload
    def _validate_base_url_or_cluster(self, base_url: None, cluster: None) -> NoReturn: ...

    @overload
    def _validate_base_url_or_cluster(self, base_url: str | None, cluster: str | None) -> str: ...

    def _validate_base_url_or_cluster(self, base_url: str | None, cluster: str | None) -> str:
        match base_url, cluster:
            case None, str():
                return f"https://{cluster}.cognitedata.com"
            case str(), _:
                if cluster is not None:
                    warnings.warn("'cluster' parameter is ignored when 'base_url' is provided.", UserWarning)
                return base_url.rstrip("/")
            case _:
                raise ValueError(
                    "Either 'base_url' or 'cluster' must be provided. Passing 'cluster' assumes the base URL "
                    "is of the form: https://<cluster>.cognitedata.com."
                )

    def __str__(self) -> str:
        return pprint.pformat(vars(self), indent=4)

    def _repr_html_(self) -> str:
        pd = local_import("pandas")
        return pd.Series(vars(self)).to_frame("ClientConfig").sort_index()._repr_html_()

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
            cluster=loaded.get("cluster"),
            headers=loaded.get("headers"),
            timeout=loaded.get("timeout"),
            file_transfer_timeout=loaded.get("file_transfer_timeout"),
            debug=loaded.get("debug", False),
        )

    @property
    def cdf_cluster(self) -> str | None:
        if self._cluster is not None:
            return self._cluster

        # A best effort attempt to extract the cluster from the base url
        if match := re.match(
            r"https?://([^/\.\s]*\.plink\.)?([^/\.\s]+)\.cognitedata\.com(?::\d+)?(?:/|$)", self.base_url
        ):
            return match.group(2)
        return None
