import os
import sys
import warnings
from typing import *

from cognite.client import utils
from cognite.client.exceptions import CogniteAPIKeyError


class ThreadLocalConfig:
    def __init__(self):
        self.api_key = None
        self.project = None
        if "cognite._thread_local" in sys.modules:
            from cognite._thread_local import credentials

            thread_local_api_key = getattr(credentials, "api_key", None)
            thread_local_project = getattr(credentials, "project", None)
            self.api_key = thread_local_api_key
            self.project = thread_local_project


class DefaultConfig:
    def __init__(self):
        # Per client
        self.api_key = os.getenv("COGNITE_API_KEY")
        self.project = os.getenv("COGNITE_PROJECT")
        self.base_url = os.getenv("COGNITE_BASE_URL", "https://api.cognitedata.com")
        self.max_workers = int(os.getenv("COGNITE_MAX_WORKERS", 10))
        self.timeout = int(os.getenv("COGNITE_TIMEOUT", 30))
        self.client_name = os.getenv("COGNITE_CLIENT_NAME")
        self.headers = {}

        # Global
        self.disable_gzip = os.getenv("COGNITE_DISABLE_GZIP", False)
        self.disable_pypi_version_check = os.getenv("COGNITE_DISABLE_PYPI_VERSION_CHECK", False)
        self.status_forcelist = self._get_status_forcelist()
        self.max_retries = int(os.getenv("COGNITE_MAX_RETRIES", 10))
        self.max_retry_backoff = int(os.getenv("COGNITE_MAX_RETRY_BACKOFF", 30))
        self.max_connection_pool_size = int(os.getenv("COGNITE_MAX_CONNECTION_POOL_SIZE", 50))

    @staticmethod
    def _get_status_forcelist():
        env_forcelist = os.getenv("COGNITE_STATUS_FORCELIST")
        if env_forcelist is None:
            return [429, 500, 502, 503]
        return [int(c) for c in env_forcelist.split(",")]


class ClientConfig:
    def __init__(
        self,
        api_key: str = None,
        project: str = None,
        client_name: str = None,
        base_url: str = None,
        max_workers: int = None,
        headers: Dict[str, str] = None,
        timeout: int = None,
        debug: bool = False,
    ):
        default = DefaultConfig()
        thread_local = ThreadLocalConfig()

        self.api_key = api_key or thread_local.api_key or default.api_key
        self.client_name = client_name or default.client_name
        self.base_url = base_url or default.base_url
        self.max_workers = max_workers or default.max_workers
        self.headers = headers or default.headers
        self.timeout = timeout or default.timeout

        if self.api_key is None:
            raise CogniteAPIKeyError("No API key has been specified")

        if self.client_name is None:
            raise ValueError(
                "No client name has been specified. Pass it to the CogniteClient or set the environment variable "
                "'COGNITE_CLIENT_NAME'."
            )

        if debug:
            utils._logging._configure_logger_for_debug_mode()

        self.project = project or thread_local.project or default.project or self._infer_project()

        if not default.disable_pypi_version_check:
            utils._auxiliary._check_client_has_newest_major_version()

    def _infer_project(self):
        from cognite.client._api.login import LoginAPI

        login_status = LoginAPI(self, cognite_client=self).status()
        if login_status.logged_in:
            warnings.warn(
                "Authenticated towards inferred project '{}'. Pass project to the CogniteClient constructor or set"
                " the environment variable 'COGNITE_PROJECT' to suppress this warning.".format(login_status.project),
                stacklevel=2,
            )
            return login_status.project

        else:
            raise CogniteAPIKeyError("Invalid API key")
