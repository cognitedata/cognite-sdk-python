import warnings
from typing import Any, Callable, Dict, List, Optional, Union

from cognite.client import utils
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.data_sets import DataSetsAPI
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.entity_matching import EntityMatchingAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.iam import IAMAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.login import LoginAPI
from cognite.client._api.raw import RawAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._api.sequences import SequencesAPI
from cognite.client._api.templates import TemplatesAPI
from cognite.client._api.three_d import ThreeDAPI
from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client._api_client import APIClient
from cognite.client.exceptions import CogniteAPIKeyError
from cognite.client.utils._client_config import ClientConfig


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
        file_transfer_timeout (int): Timeout on file upload/download requests. Defaults to 600 seconds.
        proxies (Dict[str, str]): Dictionary mapping from protocol to url. e.g. {"https": "http://10.10.1.10:1080"}
        token (Union[str, Callable[[], str]]): A jwt or method which takes no arguments and returns a jwt to use for authentication.
            This will override any api-key set.
        token_url (str): Optional url to use for token generation.
            This will override the COGNITE_TOKEN_URL environment variable and only be used if both api-key and token are not set.
        token_client_id (str): Optional client id to use for token generation.
            This will override the COGNITE_CLIENT_ID environment variable and only be used if both api-key and token are not set.
        token_client_secret (str): Optional client secret to use for token generation.
            This will override the COGNITE_CLIENT_SECRET environment variable and only be used if both api-key and token are not set.
        token_scopes (list): Optional list of scopes to use for token generation.
            This will override the COGNITE_TOKEN_SCOPES environment variable and only be used if both api-key and token are not set.
        token_custom_args (Dict): Optional additional arguments to use for token generation.
            This will be passed in as optional additional kwargs to OAuth2Session fetch_token and will only be used if both api-key and token are not set.
        disable_pypi_version_check (bool): Don't check for newer versions of the SDK on client creation
        debug (bool): Configures logger to log extra request details to stderr.
    """

    _API_VERSION = "v1"

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_subversion: Optional[str] = None,
        project: Optional[str] = None,
        client_name: Optional[str] = None,
        base_url: Optional[str] = None,
        max_workers: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        file_transfer_timeout: Optional[int] = None,
        proxies: Optional[Dict[str, str]] = None,
        token: Optional[Union[str, Callable[[], str], None]] = None,
        token_url: Optional[str] = None,
        token_client_id: Optional[str] = None,
        token_client_secret: Optional[str] = None,
        token_scopes: Optional[List[str]] = None,
        token_custom_args: Optional[Dict[str, str]] = None,
        disable_pypi_version_check: Optional[bool] = None,
        debug: bool = False,
    ):
        self._config = ClientConfig(
            api_key=api_key,
            api_subversion=api_subversion,
            project=project,
            client_name=client_name,
            base_url=base_url,
            max_workers=max_workers,
            headers=headers,
            timeout=timeout,
            file_transfer_timeout=file_transfer_timeout,
            proxies=proxies,
            token=token,
            token_url=token_url,
            token_client_id=token_client_id,
            token_client_secret=token_client_secret,
            token_scopes=token_scopes,
            token_custom_args=token_custom_args,
            disable_pypi_version_check=disable_pypi_version_check,
            debug=debug,
        )
        self.login = LoginAPI(self._config, cognite_client=self)
        if self._config.project is None:
            self._config.project = self._infer_project()
        self.assets = AssetsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.datapoints = DatapointsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.events = EventsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.files = FilesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.iam = IAMAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.data_sets = DataSetsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.sequences = SequencesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.time_series = TimeSeriesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.raw = RawAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.three_d = ThreeDAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.labels = LabelsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.relationships = RelationshipsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.entity_matching = EntityMatchingAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.templates = TemplatesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
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
