import os
import sys
from typing import Any, Dict

import requests

from cognite.client._utils.api_client import APIClient
from cognite.client._utils.utils import get_user_agent
from cognite.client.api.assets import AssetsApi
from cognite.client.api.datapoints import DatapointsApi
from cognite.client.api.events import EventsApi
from cognite.client.api.files import FilesApi
from cognite.client.api.login import LoginApi
from cognite.client.api.raw import RawApi
from cognite.client.api.time_series import TimeSeriesApi
from cognite.logger import configure_logger

DEFAULT_BASE_URL = "https://api.cognitedata.com"
DEFAULT_NUM_OF_WORKERS = 10
DEFAULT_TIMEOUT = 30


class CogniteClient:
    """Main entrypoint into Cognite Python SDK.

    All services are made available through this object. See examples below.

    Args:
        api_key (str): API key
        project (str): Project. Defaults to project of given API key.
        base_url (str): Base url to send requests to. Defaults to "https://api.cognitedata.com"
        num_of_workers (int): Number of workers to spawn when parallelizing data fetching. Defaults to 10.
        cookies (Dict): Cookies to append to all requests. Defaults to {}
        headers (Dict): Additional headers to add to all requests. Defaults are:
                 {"api-key": self.api_key, "content-type": "application/json", "accept": "application/json"}
        timeout (int): Timeout on requests sent to the api. Defaults to 60 seconds.
        debug (bool): Configures logger to log extra request details to stdout.


    Examples:
            The CogniteClient is instantiated and used like this. This example assumes that the environment variable
            COGNITE_API_KEY has been set::

                from cognite.client import CogniteClient
                client = CogniteClient()
                res = client.time_series.get_time_series()
                print(res.to_pandas())

            Certain experimental features are made available through this client as follows::

                from cognite.client import CogniteClient
                client = CogniteClient()
                res = client.experimental.model_hosting.models.list_models()
                print(res)

            Default configurations may be set using the following environment variables::

                export COGNITE_API_KEY = <your-api-key>
                export COGNITE_BASE_URL = http://<host>:<port>
                export COGNITE_NUM_RETRIES = <number-of-retries>
                export COGNITE_NUM_WORKERS = <number-of-workers>
                export COGNITE_TIMEOUT = <num-of-seconds>
                export COGNITE_DISABLE_GZIP = "1"
    """

    def __init__(
        self,
        api_key: str = None,
        project: str = None,
        base_url: str = None,
        num_of_workers: int = None,
        headers: Dict[str, str] = None,
        cookies: Dict[str, str] = None,
        timeout: int = None,
        debug: bool = None,
    ):
        thread_local_api_key, thread_local_project = self._get_thread_local_credentials()

        environment_api_key = os.getenv("COGNITE_API_KEY")
        environment_base_url = os.getenv("COGNITE_BASE_URL")
        environment_num_of_workers = os.getenv("COGNITE_NUM_WORKERS")
        environment_timeout = os.getenv("COGNITE_TIMEOUT")

        self.__api_key = api_key or thread_local_api_key or environment_api_key
        if self.__api_key is None:
            raise ValueError("No Api Key has been specified")

        self._base_url = base_url or environment_base_url or DEFAULT_BASE_URL

        self._num_of_workers = int(num_of_workers or environment_num_of_workers or DEFAULT_NUM_OF_WORKERS)

        self._configure_headers(headers)

        self._cookies = cookies or {}

        self._timeout = int(timeout or environment_timeout or DEFAULT_TIMEOUT)

        if debug:
            configure_logger("cognite-sdk", log_level="INFO", log_json=True)

        __api_version = "0.6"
        self._project = project or thread_local_project
        self.login = LoginApi(
            project=self._project,
            base_url=self._base_url,
            num_of_workers=self._num_of_workers,
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
        )
        if self._project is None:
            self._project = self.login.status().project

        self.assets = AssetsApi(
            version=__api_version,
            project=self._project,
            base_url=self._base_url,
            num_of_workers=self._num_of_workers,
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
        )
        self.datapoints = DatapointsApi(
            version=__api_version,
            project=self._project,
            base_url=self._base_url,
            num_of_workers=self._num_of_workers,
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
        )
        self.events = EventsApi(
            version=__api_version,
            project=self._project,
            base_url=self._base_url,
            num_of_workers=self._num_of_workers,
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
        )
        self.files = FilesApi(
            version=__api_version,
            project=self._project,
            base_url=self._base_url,
            num_of_workers=self._num_of_workers,
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
        )
        self.raw = RawApi(
            version=__api_version,
            project=self._project,
            base_url=self._base_url,
            num_of_workers=self._num_of_workers,
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
        )
        self.time_series = TimeSeriesApi(
            version=__api_version,
            project=self._project,
            base_url=self._base_url,
            num_of_workers=self._num_of_workers,
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
        )

        self._api_client = APIClient(
            project=self._project,
            base_url=self._base_url,
            num_of_workers=self._num_of_workers,
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
        )

    def get(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a GET request to a path in the API.

        Comes in handy if the endpoint you want to reach is not currently supported by the SDK.
        """
        return self._api_client._get(url, params=params, headers=headers)

    def post(self, url: str, json: Dict[str, Any], params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a POST request to a path in the API.

        Comes in handy if the endpoint you want to reach is not currently supported by the SDK.
        """
        return self._api_client._post(url, json=json, params=params, headers=headers)

    def put(self, url: str, json: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a PUT request to a path in the API.

        Comes in handy if the endpoint you want to reach is not currently supported by the SDK.
        """
        return self._api_client._put(url, json=json, headers=headers)

    def delete(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a DELETE request to a path in the API.

        Comes in handy if the endpoint you want to reach is not currently supported by the SDK.
        """
        return self._api_client._delete(url, params=params, headers=headers)

    def _configure_headers(self, user_defined_headers):
        self._headers = {}
        self._headers.update(requests.utils.default_headers())
        self._headers.update(
            {"api-key": self.__api_key, "content-type": "application/json", "accept": "application/json"}
        )

        if "User-Agent" in self._headers:
            self._headers["User-Agent"] += " " + get_user_agent()
        else:
            self._headers["User-Agent"] = get_user_agent()

        if user_defined_headers:
            self._headers.update(user_defined_headers)

    @staticmethod
    def _get_thread_local_credentials():
        if "cognite._thread_local" in sys.modules:
            from cognite._thread_local import credentials

            thread_local_api_key = getattr(credentials, "api_key", None)
            thread_local_project = getattr(credentials, "project", None)
            return thread_local_api_key, thread_local_project
        return None, None
