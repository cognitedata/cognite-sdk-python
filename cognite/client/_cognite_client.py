import logging
import os
import sys
import warnings
from typing import Any, Dict

from cognite.client._api.assets import AssetsAPI
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.iam import IAMAPI
from cognite.client._api.login import LoginAPI
from cognite.client._api.raw import RawAPI
from cognite.client._api.three_d import ThreeDAPI
from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client._api_client import APIClient
from cognite.client.exceptions import CogniteAPIKeyError
from cognite.client.utils._auxiliary import get_current_sdk_version
from cognite.client.utils._logging import DebugLogFormatter
from cognite.client.utils._version_checker import get_newest_version_in_major_release

DEFAULT_BASE_URL = "https://api.cognitedata.com"
DEFAULT_MAX_WORKERS = 10
DEFAULT_TIMEOUT = 30


class CogniteClient:
    """Main entrypoint into Cognite Python SDK.

    All services are made available through this object. See examples below.

    Args:
        api_key (str): API key
        project (str): Project. Defaults to project of given API key.
        client_name (str): A user-defined name for the client. Used to identify number of unique applications/scripts
            running on top of CDF.
        base_url (str): Base url to send requests to. Defaults to "https://api.cognitedata.com"
        max_workers (int): Max number of workers to spawn when parallelizing data fetching. Defaults to 10.
        headers (Dict): Additional headers to add to all requests.
        timeout (int): Timeout on requests sent to the api. Defaults to 30 seconds.
        debug (bool): Configures logger to log extra request details to stderr.
    """

    def __init__(
        self,
        api_key: str = None,
        project: str = None,
        client_name: str = None,
        base_url: str = None,
        max_workers: int = None,
        headers: Dict[str, str] = None,
        timeout: int = None,
        debug: bool = None,
    ):
        thread_local_api_key, thread_local_project = self._get_thread_local_credentials()
        environment_api_key = os.getenv("COGNITE_API_KEY")
        environment_project = os.getenv("COGNITE_PROJECT")
        environment_base_url = os.getenv("COGNITE_BASE_URL")
        environment_max_workers = os.getenv("COGNITE_MAX_WORKERS")
        environment_timeout = os.getenv("COGNITE_TIMEOUT")
        environment_client_name = os.getenv("COGNITE_CLIENT_NAME")

        self.__api_key = api_key or thread_local_api_key or environment_api_key
        if self.__api_key is None:
            raise ValueError("No API Key has been specified")

        self._base_url = base_url or environment_base_url or DEFAULT_BASE_URL

        self._max_workers = int(max_workers or environment_max_workers or DEFAULT_MAX_WORKERS)

        self._headers = headers or {}

        self._client_name = client_name if client_name is not None else environment_client_name
        if self._client_name is None:
            raise ValueError(
                "No client name has been specified. Pass it to the CogniteClient or set the environment variable 'COGNITE_CLIENT_NAME'."
            )
        self._headers["x-cdp-app"] = client_name

        self._timeout = int(timeout or environment_timeout or DEFAULT_TIMEOUT)

        if debug:
            self._configure_logger_for_debug_mode()

        __api_version = "v1"

        self.project = project or thread_local_project or environment_project
        self.login = LoginAPI(
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )

        if self.project is None:
            login_status = self.login.status()
            if login_status.logged_in:
                self.project = login_status.project
                warnings.warn(
                    "Authenticated towards inferred project '{}'. Pass project to the CogniteClient constructor or set"
                    " the environment variable 'COGNITE_PROJECT' to suppress this warning.".format(self.project),
                    stacklevel=2,
                )
            else:
                raise CogniteAPIKeyError
        self._check_client_has_newest_major_version()

        self.assets = AssetsAPI(
            version=__api_version,
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )
        self.datapoints = DatapointsAPI(
            version=__api_version,
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )
        self.events = EventsAPI(
            version=__api_version,
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )
        self.files = FilesAPI(
            version=__api_version,
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )
        self.iam = IAMAPI(
            version=__api_version,
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )
        self.time_series = TimeSeriesAPI(
            version=__api_version,
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )
        self.raw = RawAPI(
            version=__api_version,
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )
        self.three_d = ThreeDAPI(
            version=__api_version,
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )
        self._api_client = APIClient(
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )

    def get(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a GET request to an arbitrary path in the API."""
        return self._api_client._get(url, params=params, headers=headers)

    def post(self, url: str, json: Dict[str, Any], params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a POST request to an arbitrary path in the API."""
        return self._api_client._post(url, json=json, params=params, headers=headers)

    def put(self, url: str, json: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a PUT request to an arbitrary path in the API."""
        return self._api_client._put(url, json=json, headers=headers)

    def delete(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a DELETE request to an arbitrary path in the API."""
        return self._api_client._delete(url, params=params, headers=headers)

    @property
    def version(self) -> str:
        """Returns the current SDK version.

        Returns:
            str: The current SDK version
        """
        return get_current_sdk_version()

    @staticmethod
    def _get_thread_local_credentials():
        if "cognite._thread_local" in sys.modules:
            from cognite._thread_local import credentials

            thread_local_api_key = getattr(credentials, "api_key", None)
            thread_local_project = getattr(credentials, "project", None)
            return thread_local_api_key, thread_local_project
        return None, None

    def _configure_logger_for_debug_mode(self):
        logger = logging.getLogger("cognite-sdk")
        logger.setLevel("DEBUG")
        log_handler = logging.StreamHandler()
        formatter = DebugLogFormatter()
        log_handler.setFormatter(formatter)
        logger.handlers = []
        logger.propagate = False
        logger.addHandler(log_handler)

    def _check_client_has_newest_major_version(self):
        newest_version = get_newest_version_in_major_release("cognite-sdk", self.version)
        if newest_version != self.version:
            warnings.warn(
                "You are using version {} of the SDK, however version {} is available. "
                "Upgrade to suppress this warning.".format(self.version, newest_version),
                stacklevel=3,
            )
