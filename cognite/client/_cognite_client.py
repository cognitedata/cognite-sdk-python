import warnings
from typing import Any, Dict

from cognite.client import utils
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
from cognite.client.utils._client_config import ClientConfig

API_VERSION = "v1"


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
        debug: bool = False,
    ):
        self._config = ClientConfig(
            api_key=api_key,
            project=project,
            client_name=client_name,
            base_url=base_url,
            max_workers=max_workers,
            headers=headers,
            timeout=timeout,
            debug=debug,
        )
        self.login = LoginAPI(self._config, cognite_client=self)
        if self._config.project is None:
            self._config.project = self._infer_project()
        self.assets = AssetsAPI(self._config, api_version=API_VERSION, cognite_client=self)
        self.datapoints = DatapointsAPI(self._config, api_version=API_VERSION, cognite_client=self)
        self.events = EventsAPI(self._config, api_version=API_VERSION, cognite_client=self)
        self.files = FilesAPI(self._config, api_version=API_VERSION, cognite_client=self)
        self.iam = IAMAPI(self._config, api_version=API_VERSION, cognite_client=self)
        self.time_series = TimeSeriesAPI(self._config, api_version=API_VERSION, cognite_client=self)
        self.raw = RawAPI(self._config, api_version=API_VERSION, cognite_client=self)
        self.three_d = ThreeDAPI(self._config, api_version=API_VERSION, cognite_client=self)
        self._api_client = APIClient(self._config, cognite_client=self)

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
        return utils._auxiliary.get_current_sdk_version()

    @property
    def project(self) -> str:
        """Returns the project you are currently authenticated towards.

        Returns:
            str: The current project you are authenticated to.
        """
        return self._config.project

    @property
    def config(self) -> ClientConfig:
        """Returns a config object containing the configuration for the current client.

        Returns:
            ClientConfig: The configuration object.
        """
        return self._config

    def _infer_project(self):
        login_status = self.login.status()
        if login_status.logged_in:
            warnings.warn(
                "Authenticated towards inferred project '{}'. Pass project to the CogniteClient constructor or set"
                " the environment variable 'COGNITE_PROJECT' to suppress this warning.".format(login_status.project),
                stacklevel=3,
            )
            return login_status.project
        else:
            raise CogniteAPIKeyError("Invalid API key")
