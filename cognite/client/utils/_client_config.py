import os
import pprint
import sys
from typing import *

from requests.exceptions import ConnectionError

from cognite.client import utils
from cognite.client.exceptions import CogniteAPIKeyError


class _ThreadLocalConfig:
    def __init__(self):
        self.api_key = None
        self.project = None
        if "cognite._thread_local" in sys.modules:
            from cognite._thread_local import credentials

            thread_local_api_key = getattr(credentials, "api_key", None)
            thread_local_project = getattr(credentials, "project", None)
            self.api_key = thread_local_api_key
            self.project = thread_local_project


class _DefaultConfig:
    def __init__(self):
        thread_local = _ThreadLocalConfig()

        # Per client
        self.api_key = thread_local.api_key or os.getenv("COGNITE_API_KEY")
        self.project = thread_local.project or os.getenv("COGNITE_PROJECT")
        self.client_name = os.getenv("COGNITE_CLIENT_NAME")
        self.base_url = os.getenv("COGNITE_BASE_URL", "https://api.cognitedata.com")
        self.max_workers = int(os.getenv("COGNITE_MAX_WORKERS", 10))
        self.headers = {}
        self.timeout = int(os.getenv("COGNITE_TIMEOUT", 30))

        # Global
        self.disable_gzip = os.getenv("COGNITE_DISABLE_GZIP", False)
        self.disable_pypi_version_check = os.getenv("COGNITE_DISABLE_PYPI_VERSION_CHECK", False)
        self.status_forcelist = self._get_status_forcelist()
        self.max_retries = int(os.getenv("COGNITE_MAX_RETRIES", 10))
        self.max_retry_backoff = int(os.getenv("COGNITE_MAX_RETRY_BACKOFF", 30))
        self.max_connection_pool_size = int(os.getenv("COGNITE_MAX_CONNECTION_POOL_SIZE", 50))
        self.disable_ssl = os.getenv("COGNITE_DISABLE_SSL", False)

    @staticmethod
    def _get_status_forcelist():
        env_forcelist = os.getenv("COGNITE_STATUS_FORCELIST")
        if env_forcelist is None:
            return [429, 502, 503, 504]
        return [int(c) for c in env_forcelist.split(",")]


class ClientConfig(_DefaultConfig):
    def __init__(
        self,
        api_key: str = None,
        project: str = None,
        client_name: str = None,
        base_url: str = None,
        max_workers: int = None,
        headers: Dict[str, str] = None,
        timeout: int = None,
        token: Union[Callable[[], str], str] = None,
        disable_pypi_version_check: Optional[bool] = None,
        debug: bool = False,
    ):
        super().__init__()

        self.api_key = api_key or self.api_key
        self.project = project or self.project
        self.client_name = client_name or self.client_name
        self.base_url = (base_url or self.base_url).rstrip("/")
        self.max_workers = max_workers or self.max_workers
        self.headers = headers or self.headers
        self.timeout = timeout or self.timeout
        self.token = token
        self.disable_pypi_version_check = (
            disable_pypi_version_check if disable_pypi_version_check is not None else self.disable_pypi_version_check
        )

        if self.api_key is None and self.token is None:
            raise CogniteAPIKeyError("No API key or token has been specified")

        if self.client_name is None:
            raise ValueError(
                "No client name has been specified. Pass it to the CogniteClient or set the environment variable "
                "'COGNITE_CLIENT_NAME'."
            )

        if debug:
            utils._logging._configure_logger_for_debug_mode()

        if not self.disable_pypi_version_check:
            try:
                utils._auxiliary._check_client_has_newest_major_version()
            except ConnectionError:
                # PyPI is for some reason not reachable, skip version check
                pass

    def __str__(self):
        return pprint.pformat(self.__dict__, indent=4)

    def _repr_html_(self):
        return self.__str__()
