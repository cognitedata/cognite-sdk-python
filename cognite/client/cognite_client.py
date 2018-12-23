import logging
import os
from typing import Any, Dict

from cognite_logger import cognite_logger

from cognite.client._api_client import APIClient
from cognite.client.experimental import ExperimentalClient
from cognite.client.stable.assets import AssetsClient
from cognite.client.stable.datapoints import DatapointsClient
from cognite.client.stable.events import EventsClient
from cognite.client.stable.files import FilesClient
from cognite.client.stable.login import LoginClient
from cognite.client.stable.raw import RawClient
from cognite.client.stable.tagmatching import TagMatchingClient
from cognite.client.stable.time_series import TimeSeriesClient

DEFAULT_BASE_URL = "https://api.cognitedata.com"
DEFAULT_NUM_OF_RETRIES = 5
DEFAULT_NUM_OF_WORKERS = 10
DEFAULT_LOG_LEVEL = "DEBUG"

ENVIRONMENT_API_KEY = os.getenv("COGNITE_API_KEY")
ENVIRONMENT_BASE_URL = os.getenv("COGNITE_BASE_URL")
ENVIRONMENT_NUM_OF_RETRIES = os.getenv("COGNITE_NUM_RETRIES")
ENVIRONMENT_NUM_OF_WORKERS = os.getenv("COGNITE_NUM_WORKERS")


class CogniteClient:
    """Main entrypoint into Cognite Python SDK.

    All services are made available through this object. See examples below.

    Args:
        api_key (str): API key
        project (str): Project. Defaults to project of given API key.
        base_url (str): Base url to send requests to. Defaults to "https://api.cognitedata.com"
        num_of_retries: Number of times to retry failed requests. Defaults to 5.
                        Will only retry status codes 401, 429, 500, 502, and 503.
        num_of_workers: Number of workers to spawn when parallelizing data fetching. Defaults to 10.
        cookies: Cookies to append to all requests. Defaults to {}
        headers: Additional headers to add to all requests. Defaults are:
                 {"api-key": self.api_key, "content-type": "application/json", "accept": "application/json"}
        log_level: Which log level to log request details to. Defaults to DEBUG.

    Examples:
            The CogniteClient is instantiated and used like this::

                client = CogniteClient()
                res = client.time_series.get_timeseries()
                print(res.to_pandas())

            Certain experimental features are made available through this client as follows::

                client = CogniteClient()
                res = client.experimental.analytics.models.get_models()
                print(res)

            Certain experimental features are made available through this client as follows::

                client = CogniteClient()
                res = client.experimental.analytics.models.get_models()
                print(res)

            Default configurations may be set using the following environment variables::

                export COGNITE_API_KEY = <your-api-key>
                COGNITE_BASE_URL = http://<host>:<port>
                COGNITE_NUM_RETRIES = <number-of-retries>
                COGNITE_NUM_WORKERS = <number-of-workers>
    """

    def __init__(
        self,
        api_key: str = None,
        project: str = None,
        base_url: str = None,
        num_of_retries: int = None,
        num_of_workers: int = None,
        cookies: Dict[str, str] = None,
        headers: Dict[str, str] = None,
        log_level: str = None,
        debug: bool = None,
    ):
        self.__api_key = api_key or ENVIRONMENT_API_KEY
        if self.__api_key is None:
            raise ValueError("No Api Key has been specified")

        self._base_url = base_url or ENVIRONMENT_BASE_URL or DEFAULT_BASE_URL

        self._num_of_retries = num_of_retries or ENVIRONMENT_NUM_OF_RETRIES or DEFAULT_NUM_OF_RETRIES

        self._num_of_workers = num_of_workers or ENVIRONMENT_NUM_OF_WORKERS or DEFAULT_NUM_OF_WORKERS

        self._cookies = cookies or {}

        self._headers = {"api-key": self.__api_key, "content-type": "application/json", "accept": "application/json"}
        if headers:
            self._headers.update(headers)

        self._log_level = log_level or DEFAULT_LOG_LEVEL
        __valid_log_levels = [logging.getLevelName(i) for i in range(10, 51, 10)]
        if self._log_level not in __valid_log_levels:
            raise ValueError(f"{self._log_level} is not a valid log level")

        self._project = project
        if project is None:
            __login_client = self.client_factory(LoginClient)
            self._project = __login_client.status().project

        self._api_client = self.client_factory(APIClient)

        if debug:
            cognite_logger.configure_logger("cognite-sdk", log_level="DEBUG", log_json=True)

    @property
    def assets(self) -> AssetsClient:
        return self.client_factory(AssetsClient, "0.5")

    @property
    def datapoints(self) -> DatapointsClient:
        return self.client_factory(DatapointsClient, "0.5")

    @property
    def events(self) -> EventsClient:
        return self.client_factory(EventsClient, "0.5")

    @property
    def files(self) -> FilesClient:
        return self.client_factory(FilesClient, "0.5")

    @property
    def login(self) -> LoginClient:
        return self.client_factory(LoginClient)

    @property
    def raw(self) -> RawClient:
        return self.client_factory(RawClient, "0.5")

    @property
    def tag_matching(self) -> TagMatchingClient:
        return self.client_factory(TagMatchingClient, "0.5")

    @property
    def time_series(self) -> TimeSeriesClient:
        return self.client_factory(TimeSeriesClient, "0.5")

    @property
    def experimental(self) -> ExperimentalClient:
        return ExperimentalClient(self.client_factory)

    def get(self, url, params=None, headers=None):
        return self._api_client._get(url, params, headers)

    def post(
        self,
        url: str,
        body: Dict[str, Any],
        params: Dict[str, Any] = None,
        use_gzip: bool = False,
        headers: Dict[str, Any] = None,
    ):
        return self._api_client._post(url, body, params, use_gzip, headers)

    def put(self, url: str, body: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        return self._api_client._post(url, body, headers)

    def delete(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        return self._api_client._delete(url, params, headers)

    def client_factory(self, client, version=None):
        return client(
            version=version,
            project=self._project,
            base_url=self._base_url,
            num_of_retries=self._num_of_retries,
            num_of_workers=self._num_of_workers,
            cookies=self._cookies,
            headers=self._headers,
            log_level=self._log_level,
        )
