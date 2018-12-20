import enum
import logging
import os
import re
from typing import Dict, List

from cognite.client.v0_5.assets import AssetsClientV0_5
from cognite.client.v0_5.datapoints import DatapointsClientV0_5
from cognite.client.v0_5.events import EventsClientV0_5
from cognite.client.v0_5.files import FilesClientV0_5
from cognite.client.v0_5.login import LoginClientV0_5
from cognite.client.v0_5.raw import RawClientV0_5
from cognite.client.v0_5.tagmatching import TagMatchingClientV0_5
from cognite.client.v0_5.timeseries import TimeSeriesClientV0_5
from cognite.client.v0_6.analytics import AnalyticsClient
from cognite.client.v0_6.sequences import SequencesClientV0_6

DEFAULT_BASE_URL = "https://api.cognitedata.com"
DEFAULT_NUM_OF_RETRIES = 5
DEFAULT_NUM_OF_WORKERS = 10
DEFAULT_LOG_LEVEL = "DEBUG"

ENVIRONMENT_API_KEY = os.getenv("COGNITE_API_KEY")
ENVIRONMENT_BASE_URL = os.getenv("COGNITE_BASE_URL")
ENVIRONMENT_NUM_OF_RETRIES = os.getenv("COGNITE_NUM_RETRIES")
ENVIRONMENT_NUM_OF_WORKERS = os.getenv("COGNITE_NUM_WORKERS")


class _CogniteAPIVersion(enum.Enum):
    V0_5 = "0.5"
    V0_6 = "0.6"

    @staticmethod
    def get_versions() -> List[str]:
        return [e.value for e in _CogniteAPIVersion]


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
            raise ValueError(f"{self.log_level} is not a valid log level")

        self._project = project
        if project is None:
            __login_client = self.client_factory(LoginClientV0_5)
            self._project = __login_client.status().project

    @property
    def assets(self) -> AssetsClientV0_5:
        return self.client_factory(AssetsClientV0_5)

    @property
    def analytics(self) -> AnalyticsClient:
        return AnalyticsClient(self)

    @property
    def datapoints(self) -> DatapointsClientV0_5:
        return self.client_factory(DatapointsClientV0_5)

    @property
    def events(self) -> EventsClientV0_5:
        return self.client_factory(EventsClientV0_5)

    @property
    def files(self) -> FilesClientV0_5:
        return self.client_factory(FilesClientV0_5)

    @property
    def login(self) -> LoginClientV0_5:
        return self.client_factory(LoginClientV0_5)

    @property
    def raw(self) -> RawClientV0_5:
        return self.client_factory(RawClientV0_5)

    @property
    def sequences(self) -> SequencesClientV0_6:
        return self.client_factory(SequencesClientV0_6)

    @property
    def tag_matching(self) -> TagMatchingClientV0_5:
        return self.client_factory(TagMatchingClientV0_5)

    @property
    def time_series(self) -> TimeSeriesClientV0_5:
        return self.client_factory(TimeSeriesClientV0_5)

    def client_factory(self, client):
        version_pattern = r".v(\d+)_(\d+)."

        res = re.search(version_pattern, client.__module__)
        major = res.group(1)
        minor = res.group(2)
        version = f"{major}.{minor}"

        base_url = self._base_url
        if self._project is not None:
            base_url = base_url + f"/api/{version}/projects/{self._project}"

        config = {
            "project": self._project,
            "base_url": base_url,
            "num_of_retries": self._num_of_retries,
            "num_of_workers": self._num_of_workers,
            "cookies": self._cookies,
            "headers": self._headers,
            "log_level": self._log_level,
        }
        return client(**config)
