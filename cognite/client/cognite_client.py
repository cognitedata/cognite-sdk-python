import os
import sys
from typing import Any, Dict

import requests
from cognite_logger import cognite_logger
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from cognite.client._api_client import APIClient
from cognite.client._utils import get_user_agent
from cognite.client.experimental import ExperimentalClient
from cognite.client.stable.assets import AssetsClient
from cognite.client.stable.datapoints import DatapointsClient
from cognite.client.stable.events import EventsClient
from cognite.client.stable.files import FilesClient
from cognite.client.stable.login import LoginClient
from cognite.client.stable.raw import RawClient
from cognite.client.stable.tagmatching import TagMatchingClient
from cognite.client.stable.time_series import TimeSeriesClient

STATUS_FORCELIST = [429, 500, 502, 503]

DEFAULT_BASE_URL = "https://api.cognitedata.com"
DEFAULT_NUM_OF_RETRIES = 5
DEFAULT_NUM_OF_WORKERS = 10
DEFAULT_TIMEOUT = 60


class CogniteClient:
    """Main entrypoint into Cognite Python SDK.

    All services are made available through this object. See examples below.

    Args:
        api_key (str): API key
        project (str): Project. Defaults to project of given API key.
        base_url (str): Base url to send requests to. Defaults to "https://api.cognitedata.com"
        num_of_retries (int): Number of times to retry failed requests. Defaults to 5.
                        Will only retry status codes 401, 429, 500, 502, and 503.
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
                res = client.experimental.analytics.models.get_models()
                print(res)

            Default configurations may be set using the following environment variables::

                export COGNITE_API_KEY = <your-api-key>
                export COGNITE_BASE_URL = http://<host>:<port>
                export COGNITE_NUM_RETRIES = <number-of-retries>
                export COGNITE_NUM_WORKERS = <number-of-workers>
                export COGNITE_TIMEOUT = <num-of-seconds>
    """

    def __init__(
        self,
        api_key: str = None,
        project: str = None,
        base_url: str = None,
        num_of_retries: int = None,
        num_of_workers: int = None,
        headers: Dict[str, str] = None,
        cookies: Dict[str, str] = None,
        timeout: int = None,
        debug: bool = None,
    ):
        thread_local_api_key, thread_local_project = self._get_thread_local_credentials()

        environment_api_key = os.getenv("COGNITE_API_KEY")
        environment_base_url = os.getenv("COGNITE_BASE_URL")
        environment_num_of_retries = os.getenv("COGNITE_NUM_RETRIES")
        environment_num_of_workers = os.getenv("COGNITE_NUM_WORKERS")
        environment_timeout = os.getenv("COGNITE_TIMEOUT")

        self.__api_key = api_key or thread_local_api_key or environment_api_key
        if self.__api_key is None:
            raise ValueError("No Api Key has been specified")

        self._base_url = base_url or environment_base_url or DEFAULT_BASE_URL

        if num_of_retries is not None:
            self._num_of_retries = num_of_retries
        elif environment_num_of_retries is not None:
            self._num_of_retries = int(environment_num_of_retries)
        else:
            self._num_of_retries = DEFAULT_NUM_OF_RETRIES

        self._num_of_workers = int(num_of_workers or environment_num_of_workers or DEFAULT_NUM_OF_WORKERS)

        self._configure_headers(headers)

        self._cookies = cookies or {}

        self._timeout = int(timeout or environment_timeout or DEFAULT_TIMEOUT)

        self._requests_session = self._requests_retry_session()

        self._project = project or thread_local_project
        if self._project is None:
            self._project = self.login.status().project

        self._api_client = self._client_factory(APIClient)

        if debug:
            cognite_logger.configure_logger("cognite-sdk", log_level="INFO", log_json=True)

    @property
    def assets(self) -> AssetsClient:
        return self._client_factory(AssetsClient)

    @property
    def datapoints(self) -> DatapointsClient:
        return self._client_factory(DatapointsClient)

    @property
    def events(self) -> EventsClient:
        return self._client_factory(EventsClient)

    @property
    def files(self) -> FilesClient:
        return self._client_factory(FilesClient)

    @property
    def login(self) -> LoginClient:
        return self._client_factory(LoginClient)

    @property
    def raw(self) -> RawClient:
        return self._client_factory(RawClient)

    @property
    def tag_matching(self) -> TagMatchingClient:
        return self._client_factory(TagMatchingClient)

    @property
    def time_series(self) -> TimeSeriesClient:
        return self._client_factory(TimeSeriesClient)

    @property
    def experimental(self) -> ExperimentalClient:
        return ExperimentalClient(self._client_factory)

    def get(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a GET request to a path in the API.

        Comes in handy if the endpoint you want to reach is not currently supported by the SDK.
        """
        return self._api_client._get(url, params=params, headers=headers)

    def post(
        self,
        url: str,
        body: Dict[str, Any],
        params: Dict[str, Any] = None,
        use_gzip: bool = True,
        headers: Dict[str, Any] = None,
    ):
        """Perform a POST request to a path in the API.

        Comes in handy if the endpoint you want to reach is not currently supported by the SDK.
        """
        return self._api_client._post(url, body=body, params=params, use_gzip=use_gzip, headers=headers)

    def put(self, url: str, body: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a PUT request to a path in the API.

        Comes in handy if the endpoint you want to reach is not currently supported by the SDK.
        """
        return self._api_client._put(url, body=body, headers=headers)

    def delete(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a DELETE request to a path in the API.

        Comes in handy if the endpoint you want to reach is not currently supported by the SDK.
        """
        return self._api_client._delete(url, params=params, headers=headers)

    def _client_factory(self, client):
        return client(
            request_session=self._requests_session,
            project=self._project,
            base_url=self._base_url,
            num_of_workers=self._num_of_workers,
            cookies=self._cookies,
            headers=self._headers,
            timeout=self._timeout,
        )

    def _requests_retry_session(self):
        session = Session()
        retry = Retry(
            total=self._num_of_retries,
            read=self._num_of_retries,
            connect=self._num_of_retries,
            backoff_factor=0.5,
            status_forcelist=STATUS_FORCELIST,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _configure_headers(self, user_defined_headers):
        self._headers = requests.utils.default_headers()
        self._headers.update(
            {"api-key": self.__api_key, "content-type": "application/json", "accept": "application/json"}
        )

        if "User-Agent" in self._headers:
            self._headers["User-Agent"] += " " + get_user_agent()
        else:
            self._headers["User-Agent"] = get_user_agent()

        if user_defined_headers:
            self._headers.update(user_defined_headers)

    def _get_thread_local_credentials(self):
        if "cognite._thread_local" in sys.modules:
            from cognite._thread_local import credentials

            thread_local_api_key = getattr(credentials, "api_key", None)
            thread_local_project = getattr(credentials, "project", None)
            return thread_local_api_key, thread_local_project
        return None, None
