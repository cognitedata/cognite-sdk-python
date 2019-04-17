import logging
import os
import sys
from typing import Any, Dict

from cognite.client._utils.api_client import APIClient
from cognite.client._utils.utils import DebugLogFormatter
from cognite.client.api.assets import AssetsAPI
from cognite.client.api.datapoints import DatapointsAPI
from cognite.client.api.events import EventsAPI
from cognite.client.api.files import FilesAPI
from cognite.client.api.login import LoginAPI
from cognite.client.api.raw import RawAPI
from cognite.client.api.time_series import TimeSeriesAPI

DEFAULT_BASE_URL = "https://api.cognitedata.com"
DEFAULT_MAX_WORKERS = 10
DEFAULT_TIMEOUT = 20


class CogniteClient:
    """Main entrypoint into Cognite Python SDK.

    All services are made available through this object. See examples below.

    Args:
        api_key (str): API key
        project (str): Project. Defaults to project of given API key.
        base_url (str): Base url to send requests to. Defaults to "https://api.cognitedata.com"
        max_workers (int): Max number of workers to spawn when parallelizing data fetching. Defaults to 10.
        cookies (Dict): Cookies to append to all requests. Defaults to {}
        headers (Dict): Additional headers to add to all requests.
        timeout (int): Timeout on requests sent to the api. Defaults to 20 seconds.
        debug (bool): Configures logger to log extra request details to stderr.


    Examples:
            The CogniteClient is instantiated and used like this. This and all other examples assumes that the
            environment variable 'COGNITE_API_KEY' has been set::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.search(name="some name")

            Default configurations may be set using the following environment variables::

                export COGNITE_API_KEY = <your-api-key>
                export COGNITE_BASE_URL = http://<host>:<port>
                export COGNITE_MAX_RETRIES = <number-of-retries>
                export COGNITE_MAX_WORKERS = <number-of-workers>
                export COGNITE_TIMEOUT = <num-of-seconds>
                export COGNITE_DISABLE_GZIP = "1"
    """

    def __init__(
        self,
        api_key: str = None,
        project: str = None,
        base_url: str = None,
        max_workers: int = None,
        headers: Dict[str, str] = None,
        cookies: Dict[str, str] = None,
        timeout: int = None,
        debug: bool = None,
    ):
        thread_local_api_key, thread_local_project = self._get_thread_local_credentials()

        environment_api_key = os.getenv("COGNITE_API_KEY")
        environment_base_url = os.getenv("COGNITE_BASE_URL")
        environment_max_workers = os.getenv("COGNITE_MAX_WORKERS")
        environment_timeout = os.getenv("COGNITE_TIMEOUT")

        self.__api_key = api_key or thread_local_api_key or environment_api_key
        if self.__api_key is None:
            raise ValueError("No API Key has been specified")

        self._base_url = base_url or environment_base_url or DEFAULT_BASE_URL

        self._max_workers = int(max_workers or environment_max_workers or DEFAULT_MAX_WORKERS)

        self._headers = headers or {}

        self._cookies = cookies or {}

        self._timeout = int(timeout or environment_timeout or DEFAULT_TIMEOUT)

        if debug:
            self._configure_logger_for_debug_mode()

        __api_version = "1.0"

        self.project = project or thread_local_project
        self.login = LoginAPI(
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )

        if self.project is None:
            self.project = self.login.status().project

        self.assets = AssetsAPI(
            version=__api_version,
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            cookies=self._cookies,
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
            cookies=self._cookies,
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
            cookies=self._cookies,
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
            cookies=self._cookies,
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
            cookies=self._cookies,
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
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )
        self._api_client = APIClient(
            project=self.project,
            api_key=self.__api_key,
            base_url=self._base_url,
            max_workers=self._max_workers,
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
            cognite_client=self,
        )
        self._set_global_client()

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

    @staticmethod
    def _get_thread_local_credentials():
        if "cognite._thread_local" in sys.modules:
            from cognite._thread_local import credentials

            thread_local_api_key = getattr(credentials, "api_key", None)
            thread_local_project = getattr(credentials, "project", None)
            return thread_local_api_key, thread_local_project
        return None, None

    _GLOBAL_CLIENT_SET = False

    def _set_global_client(self):
        if CogniteClient._GLOBAL_CLIENT_SET is True:
            return
        from cognite.client._utils.utils import global_client

        global_client.set(self)
        CogniteClient._GLOBAL_CLIENT_SET = True

    def _configure_logger_for_debug_mode(self):
        logger = logging.getLogger("cognite-sdk")
        logger.setLevel("INFO")
        log_handler = logging.StreamHandler()
        formatter = DebugLogFormatter()
        log_handler.setFormatter(formatter)
        logger.handlers = []
        logger.propagate = False
        logger.addHandler(log_handler)
