from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from cognite.client._api.agents import AgentsAPI
from cognite.client._api.ai import AIAPI
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
from cognite.client._api.hosted_extractors import HostedExtractorsAPI
from cognite.client._api.iam import IAMAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.limits import LimitsAPI
from cognite.client._api.postgres_gateway import PostgresGatewaysAPI
from cognite.client._api.raw import RawAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._api.sequences import SequencesAPI
from cognite.client._api.simulators import SimulatorsAPI
from cognite.client._api.three_d import ThreeDAPI
from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client._api.transformations import TransformationsAPI
from cognite.client._api.units import UnitAPI
from cognite.client._api.vision import VisionAPI
from cognite.client._api.workflows import WorkflowAPI
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig, global_config
from cognite.client.credentials import CredentialProvider, OAuthClientCredentials, OAuthInteractive
from cognite.client.utils._auxiliary import load_resource_to_dict

if TYPE_CHECKING:
    from cognite.client._sync_cognite_client import CogniteClient
    from cognite.client.response import CogniteHTTPResponse


if _should_build_docs := os.getenv("BUILD_COGNITE_SDK_DOCS") == "true":
    from cognite.client._api.ai.tools import AIToolsAPI
    from cognite.client._api.ai.tools.documents import AIDocumentsAPI
    from cognite.client._api.data_modeling.containers import ContainersAPI
    from cognite.client._api.data_modeling.data_models import DataModelsAPI
    from cognite.client._api.data_modeling.graphql import DataModelingGraphQLAPI
    from cognite.client._api.data_modeling.instances import InstancesAPI
    from cognite.client._api.data_modeling.space_statistics import SpaceStatisticsAPI
    from cognite.client._api.data_modeling.spaces import SpacesAPI
    from cognite.client._api.data_modeling.statistics import StatisticsAPI
    from cognite.client._api.data_modeling.views import ViewsAPI
    from cognite.client._api.datapoints import DatapointsAPI
    from cognite.client._api.datapoints_subscriptions import DatapointsSubscriptionAPI
    from cognite.client._api.documents import DocumentPreviewAPI  # type: ignore[attr-defined]
    from cognite.client._api.extractionpipelines import (  # type: ignore[attr-defined]
        ExtractionPipelineConfigsAPI,
        ExtractionPipelineRunsAPI,
    )
    from cognite.client._api.functions import FunctionCallsAPI, FunctionSchedulesAPI  # type: ignore[attr-defined]
    from cognite.client._api.hosted_extractors import (  # type: ignore[attr-defined]
        DestinationsAPI,
        JobsAPI,
        MappingsAPI,
        SourcesAPI,
    )
    from cognite.client._api.iam import (  # type: ignore[attr-defined]
        GroupsAPI,
        PrincipalsAPI,
        SecurityCategoriesAPI,
        SessionsAPI,
        TokenAPI,
    )
    from cognite.client._api.postgres_gateway.tables import TablesAPI
    from cognite.client._api.postgres_gateway.users import UsersAPI
    from cognite.client._api.raw import RawDatabasesAPI, RawRowsAPI, RawTablesAPI  # type: ignore[attr-defined]
    from cognite.client._api.sequences import SequencesDataAPI  # type: ignore[attr-defined]
    from cognite.client._api.simulators import (  # type: ignore[attr-defined]
        SimulatorIntegrationsAPI,
        SimulatorLogsAPI,
        SimulatorModelsAPI,
        SimulatorRoutinesAPI,
        SimulatorRunsAPI,
    )
    from cognite.client._api.simulators.models import SimulatorModelRevisionsAPI  # type: ignore[attr-defined]
    from cognite.client._api.simulators.routines import SimulatorRoutineRevisionsAPI  # type: ignore[attr-defined]
    from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
    from cognite.client._api.three_d import (  # type: ignore[attr-defined]
        ThreeDAssetMappingAPI,
        ThreeDFilesAPI,
        ThreeDModelsAPI,
        ThreeDRevisionsAPI,
    )
    from cognite.client._api.transformations import (  # type: ignore[attr-defined]
        TransformationJobsAPI,
        TransformationNotificationsAPI,
        TransformationSchedulesAPI,
        TransformationSchemaAPI,
    )
    from cognite.client._api.units import UnitSystemAPI  # type: ignore[attr-defined]
    from cognite.client._api.user_profiles import UserProfilesAPI
    from cognite.client._api.workflows import (  # type: ignore[attr-defined]
        WorkflowExecutionAPI,
        WorkflowTaskAPI,
        WorkflowTriggerAPI,
        WorkflowVersionAPI,
    )


class AsyncCogniteClient:
    """Main entrypoint into the Cognite Python SDK.

    All Cognite Data Fusion APIs are accessible through this asynchronous client.
    For the synchronous client, see :class:`~cognite.client._cognite_client.CogniteClient`.

    Args:
        config (ClientConfig | None): The configuration for this client.
    """

    _API_VERSION = "v1"

    def __init__(self, config: ClientConfig | None = None) -> None:
        if (client_config := config or global_config.default_client_config) is None:
            raise ValueError(
                "No ClientConfig has been provided, either pass it directly to the client on "
                "initialization or set global_config.default_client_config."
            )
        else:
            self._config = client_config

        # APIs using base_url / resource path:
        self.agents = AgentsAPI(self._config, self._API_VERSION, self)
        self.ai = AIAPI(self._config, self._API_VERSION, self)
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
        self.limits = LimitsAPI(self._config, self._API_VERSION, self)
        self.relationships = RelationshipsAPI(self._config, self._API_VERSION, self)
        self.entity_matching = EntityMatchingAPI(self._config, self._API_VERSION, self)
        self.vision = VisionAPI(self._config, self._API_VERSION, self)
        self.extraction_pipelines = ExtractionPipelinesAPI(self._config, self._API_VERSION, self)
        self.hosted_extractors = HostedExtractorsAPI(self._config, self._API_VERSION, self)
        self.postgres_gateway = PostgresGatewaysAPI(self._config, self._API_VERSION, self)
        self.transformations = TransformationsAPI(self._config, self._API_VERSION, self)
        self.diagrams = DiagramsAPI(self._config, self._API_VERSION, self)
        self.annotations = AnnotationsAPI(self._config, self._API_VERSION, self)
        self.functions = FunctionsAPI(self._config, self._API_VERSION, self)
        self.data_modeling = DataModelingAPI(self._config, self._API_VERSION, self)
        self.documents = DocumentsAPI(self._config, self._API_VERSION, self)
        self.workflows = WorkflowAPI(self._config, self._API_VERSION, self)
        self.units = UnitAPI(self._config, self._API_VERSION, self)
        self.simulators = SimulatorsAPI(self._config, self._API_VERSION, self)
        # APIs just using base_url:
        self._api_client = APIClient(self._config, api_version=None, cognite_client=self)

        self.__sync_client: CogniteClient | None = None

    @property
    def api_client(self) -> APIClient:
        """Returns the underlying API client used for HTTP requests.

        Returns:
            APIClient: The API client instance.
        """
        return self._api_client

    def get_sync_client(self) -> CogniteClient:
        """Returns a synchronous CogniteClient with the same configuration.

        Returns:
            CogniteClient: A sync client with the same configuration.
        """
        from cognite.client._sync_cognite_client import CogniteClient

        if self.__sync_client is None:
            self.__sync_client = CogniteClient(self._config)
        return self.__sync_client

    async def get(
        self, url: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None
    ) -> CogniteHTTPResponse:
        """Perform a GET request to an arbitrary path in the API."""
        return await self._api_client._get(url, params=params, headers=headers, semaphore=None)

    async def post(
        self,
        url: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> CogniteHTTPResponse:
        """Perform a POST request to an arbitrary path in the API."""
        return await self._api_client._post(url, json=json, params=params, headers=headers, semaphore=None)

    async def put(
        self,
        url: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> CogniteHTTPResponse:
        """Perform a PUT request to an arbitrary path in the API."""
        return await self._api_client._put(url, json=json, params=params, headers=headers, semaphore=None)

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
        return self._config

    @classmethod
    def default(
        cls,
        project: str,
        cdf_cluster: str,
        credentials: CredentialProvider,
        client_name: str | None = None,
    ) -> AsyncCogniteClient:
        """
        Create an AsyncCogniteClient with default configuration.

        The default configuration creates the URLs based on the project and cluster:

        * Base URL: "https://{cdf_cluster}.cognitedata.com/

        Args:
            project (str): The CDF project.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            credentials (CredentialProvider): Credentials. e.g. Token, ClientCredentials.
            client_name (str | None): A user-defined name for the client. Used to identify the number of unique applications/scripts running on top of CDF. If this is not set, the getpass.getuser() is used instead, meaning the username you are logged in with is used.

        Returns:
            AsyncCogniteClient: An AsyncCogniteClient instance with default configurations.
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
    ) -> AsyncCogniteClient:
        """
        Create an AsyncCogniteClient with default configuration using a client credentials flow.

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
            AsyncCogniteClient: An AsyncCogniteClient instance with default configurations.
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
    ) -> AsyncCogniteClient:
        """
        Create an AsyncCogniteClient with default configuration using the interactive flow.

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
            AsyncCogniteClient: An AsyncCogniteClient instance with default configurations.
        """
        credentials = OAuthInteractive.default_for_entra_id(tenant_id, client_id, cdf_cluster)
        return cls.default(project, cdf_cluster, credentials, client_name)

    @classmethod
    def load(cls, config: dict[str, Any] | str) -> AsyncCogniteClient:
        """Load a cognite client object from a YAML/JSON string or dict.

        Args:
            config (dict[str, Any] | str): A dictionary or YAML/JSON string containing configuration values defined in the AsyncCogniteClient class.

        Returns:
            AsyncCogniteClient: A cognite client object.

        Examples:

            Create a cognite client object from a dictionary input:

                >>> from cognite.client import AsyncCogniteClient
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
                >>> client = AsyncCogniteClient.load(config)
        """
        loaded = load_resource_to_dict(config)
        return cls(config=ClientConfig.load(loaded))


def _make_accessors_for_building_docs() -> None:
    AsyncCogniteClient.agents = AgentsAPI  # type: ignore
    AsyncCogniteClient.ai = AIAPI  # type: ignore
    AsyncCogniteClient.ai.tools = AIToolsAPI  # type: ignore
    AsyncCogniteClient.ai.tools.documents = AIDocumentsAPI  # type: ignore
    AsyncCogniteClient.assets = AssetsAPI  # type: ignore
    AsyncCogniteClient.events = EventsAPI  # type: ignore
    AsyncCogniteClient.files = FilesAPI  # type: ignore
    AsyncCogniteClient.iam = IAMAPI  # type: ignore
    AsyncCogniteClient.iam.token = TokenAPI  # type: ignore
    AsyncCogniteClient.iam.groups = GroupsAPI  # type: ignore
    AsyncCogniteClient.iam.security_categories = SecurityCategoriesAPI  # type: ignore
    AsyncCogniteClient.iam.sessions = SessionsAPI  # type: ignore
    AsyncCogniteClient.iam.user_profiles = UserProfilesAPI  # type: ignore
    AsyncCogniteClient.iam.principals = PrincipalsAPI  # type: ignore
    AsyncCogniteClient.data_sets = DataSetsAPI  # type: ignore
    AsyncCogniteClient.sequences = SequencesAPI  # type: ignore
    AsyncCogniteClient.sequences.data = SequencesDataAPI  # type: ignore
    AsyncCogniteClient.time_series = TimeSeriesAPI  # type: ignore
    AsyncCogniteClient.time_series.data = DatapointsAPI  # type: ignore
    AsyncCogniteClient.time_series.data.synthetic = SyntheticDatapointsAPI  # type: ignore
    AsyncCogniteClient.time_series.subscriptions = DatapointsSubscriptionAPI  # type: ignore
    AsyncCogniteClient.geospatial = GeospatialAPI  # type: ignore
    AsyncCogniteClient.raw = RawAPI  # type: ignore
    AsyncCogniteClient.raw.databases = RawDatabasesAPI  # type: ignore
    AsyncCogniteClient.raw.tables = RawTablesAPI  # type: ignore
    AsyncCogniteClient.raw.rows = RawRowsAPI  # type: ignore
    AsyncCogniteClient.three_d = ThreeDAPI  # type: ignore
    AsyncCogniteClient.three_d.models = ThreeDModelsAPI  # type: ignore
    AsyncCogniteClient.three_d.revisions = ThreeDRevisionsAPI  # type: ignore
    AsyncCogniteClient.three_d.files = ThreeDFilesAPI  # type: ignore
    AsyncCogniteClient.three_d.asset_mappings = ThreeDAssetMappingAPI  # type: ignore
    AsyncCogniteClient.labels = LabelsAPI  #  type: ignore
    AsyncCogniteClient.limits = LimitsAPI  # type: ignore
    AsyncCogniteClient.relationships = RelationshipsAPI  # type: ignore
    AsyncCogniteClient.entity_matching = EntityMatchingAPI  # type: ignore
    AsyncCogniteClient.vision = VisionAPI  # type: ignore
    AsyncCogniteClient.extraction_pipelines = ExtractionPipelinesAPI  # type: ignore
    AsyncCogniteClient.extraction_pipelines.runs = ExtractionPipelineRunsAPI  # type: ignore
    AsyncCogniteClient.extraction_pipelines.config = ExtractionPipelineConfigsAPI  # type: ignore
    AsyncCogniteClient.hosted_extractors = HostedExtractorsAPI  # type: ignore
    AsyncCogniteClient.hosted_extractors.sources = SourcesAPI  # type: ignore
    AsyncCogniteClient.hosted_extractors.jobs = JobsAPI  # type: ignore
    AsyncCogniteClient.hosted_extractors.destinations = DestinationsAPI  # type: ignore
    AsyncCogniteClient.hosted_extractors.mappings = MappingsAPI  # type: ignore
    AsyncCogniteClient.postgres_gateway = PostgresGatewaysAPI  # type: ignore
    AsyncCogniteClient.postgres_gateway.users = UsersAPI  # type: ignore
    AsyncCogniteClient.postgres_gateway.tables = TablesAPI  # type: ignore
    AsyncCogniteClient.transformations = TransformationsAPI  # type: ignore
    AsyncCogniteClient.transformations.schedules = TransformationSchedulesAPI  # type: ignore
    AsyncCogniteClient.transformations.notifications = TransformationNotificationsAPI  # type: ignore
    AsyncCogniteClient.transformations.jobs = TransformationJobsAPI  # type: ignore
    AsyncCogniteClient.transformations.schema = TransformationSchemaAPI  # type: ignore
    AsyncCogniteClient.diagrams = DiagramsAPI  # type: ignore
    AsyncCogniteClient.annotations = AnnotationsAPI  # type: ignore
    AsyncCogniteClient.functions = FunctionsAPI  # type: ignore
    AsyncCogniteClient.functions.calls = FunctionCallsAPI  # type: ignore
    AsyncCogniteClient.functions.schedules = FunctionSchedulesAPI  # type: ignore
    AsyncCogniteClient.data_modeling = DataModelingAPI  # type: ignore
    AsyncCogniteClient.data_modeling.data_models = DataModelsAPI  # type: ignore
    AsyncCogniteClient.data_modeling.spaces = SpacesAPI  # type: ignore
    AsyncCogniteClient.data_modeling.views = ViewsAPI  # type: ignore
    AsyncCogniteClient.data_modeling.containers = ContainersAPI  # type: ignore
    AsyncCogniteClient.data_modeling.instances = InstancesAPI  # type: ignore
    AsyncCogniteClient.data_modeling.graphql = DataModelingGraphQLAPI  # type: ignore
    AsyncCogniteClient.data_modeling.statistics = StatisticsAPI  # type: ignore
    AsyncCogniteClient.data_modeling.statistics.spaces = SpaceStatisticsAPI  # type: ignore
    AsyncCogniteClient.documents = DocumentsAPI  # type: ignore
    AsyncCogniteClient.documents.previews = DocumentPreviewAPI  # type: ignore
    AsyncCogniteClient.workflows = WorkflowAPI  # type: ignore
    AsyncCogniteClient.workflows.versions = WorkflowVersionAPI  # type: ignore
    AsyncCogniteClient.workflows.executions = WorkflowExecutionAPI  # type: ignore
    AsyncCogniteClient.workflows.tasks = WorkflowTaskAPI  # type: ignore
    AsyncCogniteClient.workflows.triggers = WorkflowTriggerAPI  # type: ignore
    AsyncCogniteClient.units = UnitAPI  # type: ignore
    AsyncCogniteClient.units.systems = UnitSystemAPI  # type: ignore
    AsyncCogniteClient.simulators = SimulatorsAPI  # type: ignore
    AsyncCogniteClient.simulators.integrations = SimulatorIntegrationsAPI  # type: ignore
    AsyncCogniteClient.simulators.models = SimulatorModelsAPI  # type: ignore
    AsyncCogniteClient.simulators.models.revisions = SimulatorModelRevisionsAPI  # type: ignore
    AsyncCogniteClient.simulators.routines = SimulatorRoutinesAPI  # type: ignore
    AsyncCogniteClient.simulators.routines.revisions = SimulatorRoutineRevisionsAPI  # type: ignore
    AsyncCogniteClient.simulators.runs = SimulatorRunsAPI  # type: ignore
    AsyncCogniteClient.simulators.logs = SimulatorLogsAPI  # type: ignore


if _should_build_docs:
    _make_accessors_for_building_docs()
