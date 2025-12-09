"""
===================================================
This file is auto-generated - do not edit manually!
===================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import httpx

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.agents.agents import SyncAgentsAPI
from cognite.client._sync_api.ai import SyncAIAPI
from cognite.client._sync_api.annotations import SyncAnnotationsAPI
from cognite.client._sync_api.assets import SyncAssetsAPI
from cognite.client._sync_api.data_modeling import SyncDataModelingAPI
from cognite.client._sync_api.data_sets import SyncDataSetsAPI
from cognite.client._sync_api.diagrams import SyncDiagramsAPI
from cognite.client._sync_api.documents import SyncDocumentsAPI
from cognite.client._sync_api.entity_matching import SyncEntityMatchingAPI
from cognite.client._sync_api.events import SyncEventsAPI
from cognite.client._sync_api.extractionpipelines import SyncExtractionPipelinesAPI
from cognite.client._sync_api.files import SyncFilesAPI
from cognite.client._sync_api.functions import SyncFunctionsAPI
from cognite.client._sync_api.geospatial import SyncGeospatialAPI
from cognite.client._sync_api.hosted_extractors import SyncHostedExtractorsAPI
from cognite.client._sync_api.iam import SyncIAMAPI
from cognite.client._sync_api.labels import SyncLabelsAPI
from cognite.client._sync_api.postgres_gateway import SyncPostgresGatewaysAPI
from cognite.client._sync_api.raw import SyncRawAPI
from cognite.client._sync_api.relationships import SyncRelationshipsAPI
from cognite.client._sync_api.sequences import SyncSequencesAPI
from cognite.client._sync_api.simulators import SyncSimulatorsAPI
from cognite.client._sync_api.three_d import Sync3DAPI
from cognite.client._sync_api.time_series import SyncTimeSeriesAPI
from cognite.client._sync_api.transformations import SyncTransformationsAPI
from cognite.client._sync_api.units import SyncUnitAPI
from cognite.client._sync_api.vision import SyncVisionAPI
from cognite.client._sync_api.workflows import SyncWorkflowAPI
from cognite.client.credentials import CredentialProvider, OAuthClientCredentials, OAuthInteractive
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils._auxiliary import load_resource_to_dict

if TYPE_CHECKING:
    from cognite.client import ClientConfig


class CogniteClient:
    """Main entrypoint into the Cognite Python SDK.

    All Cognite Data Fusion APIs are accessible through this synchronous client.
    For the asynchronous client, see :class:`~cognite.client._cognite_client.AsyncCogniteClient`.

    Args:
        config (ClientConfig | None): The configuration for this client.
    """

    def __init__(self, config: ClientConfig | None = None) -> None:
        self.__async_client = async_client = AsyncCogniteClient(config)

        # Initialize all sync. APIs:
        self.ai = SyncAIAPI(async_client)
        self.agents = SyncAgentsAPI(async_client)
        self.annotations = SyncAnnotationsAPI(async_client)
        self.assets = SyncAssetsAPI(async_client)
        self.data_modeling = SyncDataModelingAPI(async_client)
        self.data_sets = SyncDataSetsAPI(async_client)
        self.diagrams = SyncDiagramsAPI(async_client)
        self.documents = SyncDocumentsAPI(async_client)
        self.entity_matching = SyncEntityMatchingAPI(async_client)
        self.events = SyncEventsAPI(async_client)
        self.extraction_pipelines = SyncExtractionPipelinesAPI(async_client)
        self.files = SyncFilesAPI(async_client)
        self.functions = SyncFunctionsAPI(async_client)
        self.geospatial = SyncGeospatialAPI(async_client)
        self.hosted_extractors = SyncHostedExtractorsAPI(async_client)
        self.iam = SyncIAMAPI(async_client)
        self.labels = SyncLabelsAPI(async_client)
        self.postgres_gateway = SyncPostgresGatewaysAPI(async_client)
        self.raw = SyncRawAPI(async_client)
        self.relationships = SyncRelationshipsAPI(async_client)
        self.sequences = SyncSequencesAPI(async_client)
        self.simulators = SyncSimulatorsAPI(async_client)
        self.three_d = Sync3DAPI(async_client)
        self.time_series = SyncTimeSeriesAPI(async_client)
        self.transformations = SyncTransformationsAPI(async_client)
        self.units = SyncUnitAPI(async_client)
        self.vision = SyncVisionAPI(async_client)
        self.workflows = SyncWorkflowAPI(async_client)

    def get(
        self, url: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None
    ) -> httpx.Response:
        """Perform a GET request to an arbitrary path in the API."""
        return run_sync(self.__async_client.get(url, params=params, headers=headers))

    def post(
        self,
        url: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Perform a POST request to an arbitrary path in the API."""
        return run_sync(self.__async_client.post(url, json=json, params=params, headers=headers))

    def put(
        self,
        url: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Perform a PUT request to an arbitrary path in the API."""
        return run_sync(self.__async_client.put(url, json=json, params=params, headers=headers))

    @property
    def version(self) -> str:
        """Returns the current SDK version.

        Returns:
            str: The current SDK version
        """
        from cognite.client import __version__

        return __version__

    @property
    def config(self) -> ClientConfig:
        """Returns a config object containing the configuration for the current client.

        Returns:
            ClientConfig: The configuration object.
        """
        return self.__async_client._config

    @classmethod
    def default(
        cls,
        project: str,
        cdf_cluster: str,
        credentials: CredentialProvider,
        client_name: str | None = None,
    ) -> CogniteClient:
        """
        Create an CogniteClient with default configuration.

        The default configuration creates the URLs based on the project and cluster:

        * Base URL: "https://{cdf_cluster}.cognitedata.com/

        Args:
            project (str): The CDF project.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            credentials (CredentialProvider): Credentials. e.g. Token, ClientCredentials.
            client_name (str | None): A user-defined name for the client. Used to identify the number of unique applications/scripts running on top of CDF. If this is not set, the getpass.getuser() is used instead, meaning the username you are logged in with is used.

        Returns:
            CogniteClient: An CogniteClient instance with default configurations.
        """
        from cognite.client import ClientConfig

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
        Create an CogniteClient with default configuration using a client credentials flow.

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
            CogniteClient: An CogniteClient instance with default configurations.
        """
        credentials = OAuthClientCredentials.default_for_entra_id(tenant_id, client_id, client_secret, cdf_cluster)
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
        Create an CogniteClient with default configuration using the interactive flow.

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
            CogniteClient: An CogniteClient instance with default configurations.
        """
        credentials = OAuthInteractive.default_for_entra_id(tenant_id, client_id, cdf_cluster)
        return cls.default(project, cdf_cluster, credentials, client_name)

    @classmethod
    def load(cls, config: dict[str, Any] | str) -> CogniteClient:
        """Load a cognite client object from a YAML/JSON string or dict.

        Args:
            config (dict[str, Any] | str): A dictionary or YAML/JSON string containing configuration values defined in the CogniteClient class.

        Returns:
            CogniteClient: A cognite client object.

        Examples:

            Create a cognite client object from a dictionary input:

                >>> from cognite.client import CogniteClient
                >>> import os
                >>> config = {
                ...     "client_name": "abcd",
                ...     "project": "cdf-project",
                ...     "base_url": "https://api.cognitedata.com/",
                ...     "credentials": {
                ...         "client_credentials": {
                ...             "client_id": "abcd",
                ...             "client_secret": os.environ["OAUTH_CLIENT_SECRET"],
                ...             "token_url": "https://login.microsoftonline.com/xyz/oauth2/v2.0/token",
                ...             "scopes": ["https://api.cognitedata.com/.default"],
                ...         },
                ...     },
                ... }
                >>> client = CogniteClient.load(config)
        """
        from cognite.client import ClientConfig

        loaded = load_resource_to_dict(config)
        return cls(config=ClientConfig.load(loaded))
