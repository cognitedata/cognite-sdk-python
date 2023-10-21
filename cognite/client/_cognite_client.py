from __future__ import annotations

from typing import Any

from requests import Response

from cognite.client._api.annotations import AnnotationsAPI
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.data_modeling import DataModelingAPI
from cognite.client._api.data_sets import DataSetsAPI
from cognite.client._api.diagrams import DiagramsAPI
from cognite.client._api.documents import DocumentsAPI
from cognite.client._api.entity_matching import EntityMatchingAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.extractionpipelines import ExtractionPipelinesAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.functions import FunctionsAPI
from cognite.client._api.geospatial import GeospatialAPI
from cognite.client._api.iam import IAMAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.raw import RawAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._api.sequences import SequencesAPI
from cognite.client._api.templates import TemplatesAPI
from cognite.client._api.three_d import ThreeDAPI
from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client._api.transformations import TransformationsAPI
from cognite.client._api.units import UnitAPI
from cognite.client._api.vision import VisionAPI
from cognite.client._api.workflows import WorkflowAPI
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig, global_config
from cognite.client.credentials import CredentialProvider, OAuthClientCredentials, OAuthInteractive
from cognite.client.utils._auxiliary import get_current_sdk_version


class CogniteClient:
    """Main entrypoint into Cognite Python SDK.

    All services are made available through this object. See examples below.

    Args:
        config (ClientConfig | None): The configuration for this client.
    """

    _API_VERSION = "v1"

    def __init__(self, config: ClientConfig | None = None) -> None:
        if (client_config := config or global_config.default_client_config) is None:
            raise ValueError(
                "No ClientConfig has been provided, either pass it directly to CogniteClient "
                "or set global_config.default_client_config."
            )
        else:
            self._config = client_config

        # APIs using base_url / resource path:
        self.assets = AssetsAPI(self._config, self._API_VERSION, self)
        self.events = EventsAPI(self._config, self._API_VERSION, self)
        self.files = FilesAPI(self._config, self._API_VERSION, self)
        self.iam = IAMAPI(self._config, self._API_VERSION, self)
        self.data_sets = DataSetsAPI(self._config, self._API_VERSION, self)
        self.sequences = SequencesAPI(self._config, self._API_VERSION, self)
        self.time_series = TimeSeriesAPI(self._config, self._API_VERSION, self)
        self.geospatial = GeospatialAPI(self._config, self._API_VERSION, self)
        self.raw = RawAPI(self._config, self._API_VERSION, self)
        self.three_d = ThreeDAPI(self._config, self._API_VERSION, self)
        self.labels = LabelsAPI(self._config, self._API_VERSION, self)
        self.relationships = RelationshipsAPI(self._config, self._API_VERSION, self)
        self.entity_matching = EntityMatchingAPI(self._config, self._API_VERSION, self)
        self.templates = TemplatesAPI(self._config, self._API_VERSION, self)
        self.vision = VisionAPI(self._config, self._API_VERSION, self)
        self.extraction_pipelines = ExtractionPipelinesAPI(self._config, self._API_VERSION, self)
        self.transformations = TransformationsAPI(self._config, self._API_VERSION, self)
        self.diagrams = DiagramsAPI(self._config, self._API_VERSION, self)
        self.annotations = AnnotationsAPI(self._config, self._API_VERSION, self)
        self.functions = FunctionsAPI(self._config, self._API_VERSION, self)
        self.data_modeling = DataModelingAPI(self._config, self._API_VERSION, self)
        self.documents = DocumentsAPI(self._config, self._API_VERSION, self)
        self.workflows = WorkflowAPI(self._config, self._API_VERSION, self)
        self.units = UnitAPI(self._config, self._API_VERSION, self)
        # APIs just using base_url:
        self._api_client = APIClient(self._config, api_version=None, cognite_client=self)

    def get(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> Response:
        """Perform a GET request to an arbitrary path in the API."""
        return self._api_client._get(url, params=params, headers=headers)

    def post(
        self,
        url: str,
        json: dict[str, Any],
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> Response:
        """Perform a POST request to an arbitrary path in the API."""
        return self._api_client._post(url, json=json, params=params, headers=headers)

    def put(self, url: str, json: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> Response:
        """Perform a PUT request to an arbitrary path in the API."""
        return self._api_client._put(url, json=json, headers=headers)

    def delete(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> Response:
        """Perform a DELETE request to an arbitrary path in the API."""
        return self._api_client._delete(url, params=params, headers=headers)

    @property
    def version(self) -> str:
        """Returns the current SDK version.

        Returns:
            str: The current SDK version
        """
        return get_current_sdk_version()

    @property
    def config(self) -> ClientConfig:
        """Returns a config object containing the configuration for the current client.

        Returns:
            ClientConfig: The configuration object.
        """
        return self._config

    @classmethod
    def default(
        cls,
        project: str,
        cdf_cluster: str,
        credentials: CredentialProvider,
        client_name: str | None = None,
    ) -> CogniteClient:
        """
        Create a CogniteClient with default configuration.

        The default configuration creates the URLs based on the project and cluster:

        * Base URL: "https://{cdf_cluster}.cognitedata.com/

        Args:
            project (str): The CDF project.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            credentials (CredentialProvider): Credentials. e.g. Token, ClientCredentials.
            client_name (str | None): A user-defined name for the client. Used to identify the number of unique applications/scripts running on top of CDF. If this is not set, the getpass.getuser() is used instead, meaning the username you are logged in with is used.

        Returns:
            CogniteClient: A CogniteClient instance with default configurations.
        """
        return cls(ClientConfig.default(project, cdf_cluster, credentials, client_name=client_name))

    @classmethod
    def default_oauth_client_credentials(
        cls,
        project: str,
        cdf_cluster: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        client_name: str | None = None,
    ) -> CogniteClient:
        """
        Create a CogniteClient with default configuration using a client credentials flow.

        The default configuration creates the URLs based on the project and cluster:

        * Base URL: "https://{cdf_cluster}.cognitedata.com/
        * Token URL: "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        * Scopes: [f"https://{cdf_cluster}.cognitedata.com/.default"]

        Args:
            project (str): The CDF project.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            tenant_id (str): The Azure tenant ID.
            client_id (str): The Azure client ID.
            client_secret (str): The Azure client secret.
            client_name (str | None): A user-defined name for the client. Used to identify the number of unique applications/scripts running on top of CDF. If this is not set, the getpass.getuser() is used instead, meaning the username you are logged in with is used.

        Returns:
            CogniteClient: A CogniteClient instance with default configurations.
        """

        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)

        return cls.default(project, cdf_cluster, credentials, client_name)

    @classmethod
    def default_oauth_interactive(
        cls,
        project: str,
        cdf_cluster: str,
        tenant_id: str,
        client_id: str,
        client_name: str | None = None,
    ) -> CogniteClient:
        """
        Create a CogniteClient with default configuration using the interactive flow.

        The default configuration creates the URLs based on the tenant_id and cluster:

        * Base URL: "https://{cdf_cluster}.cognitedata.com/
        * Authority URL: "https://login.microsoftonline.com/{tenant_id}"
        * Scopes: [f"https://{cdf_cluster}.cognitedata.com/.default"]

        Args:
            project (str): The CDF project.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            tenant_id (str): The Azure tenant ID.
            client_id (str): The Azure client ID.
            client_name (str | None): A user-defined name for the client. Used to identify the number of unique applications/scripts running on top of CDF. If this is not set, the getpass.getuser() is used instead, meaning the username you are logged in with is used.

        Returns:
            CogniteClient: A CogniteClient instance with default configurations.
        """
        credentials = OAuthInteractive.default_for_azure_ad(tenant_id, client_id, cdf_cluster)
        return cls.default(project, cdf_cluster, credentials, client_name)
