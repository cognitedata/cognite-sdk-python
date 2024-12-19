from __future__ import annotations

import os
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
from cognite.client._api.hosted_extractors import HostedExtractorsAPI
from cognite.client._api.iam import IAMAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.postgres_gateway import PostgresGatewaysAPI
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
from cognite.client.utils._auxiliary import get_current_sdk_version, load_resource_to_dict

_build_docs = os.getenv("BUILD_COGNITE_SDK_DOCS")
if _build_docs:
    from cognite.client._api.data_modeling.containers import ContainersAPI
    from cognite.client._api.data_modeling.data_models import DataModelsAPI
    from cognite.client._api.data_modeling.graphql import DataModelingGraphQLAPI
    from cognite.client._api.data_modeling.instances import InstancesAPI
    from cognite.client._api.data_modeling.spaces import SpacesAPI
    from cognite.client._api.data_modeling.views import ViewsAPI
    from cognite.client._api.datapoints import DatapointsAPI
    from cognite.client._api.datapoints_subscriptions import DatapointsSubscriptionAPI
    from cognite.client._api.documents import DocumentPreviewAPI
    from cognite.client._api.extractionpipelines import (
        ExtractionPipelineConfigsAPI,
        ExtractionPipelineRunsAPI,
    )
    from cognite.client._api.functions import FunctionCallsAPI, FunctionSchedulesAPI
    from cognite.client._api.hosted_extractors import DestinationsAPI, JobsAPI, MappingsAPI, SourcesAPI
    from cognite.client._api.iam import GroupsAPI, SecurityCategoriesAPI, SessionsAPI, TokenAPI
    from cognite.client._api.raw import RawDatabasesAPI, RawRowsAPI, RawTablesAPI
    from cognite.client._api.sequences import SequencesDataAPI
    from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
    from cognite.client._api.templates import (
        TemplateGroupsAPI,
        TemplateGroupVersionsAPI,
        TemplateInstancesAPI,
        TemplateViewsAPI,
    )
    from cognite.client._api.three_d import (
        ThreeDAssetMappingAPI,
        ThreeDFilesAPI,
        ThreeDModelsAPI,
        ThreeDRevisionsAPI,
    )
    from cognite.client._api.transformations import (
        TransformationJobsAPI,
        TransformationNotificationsAPI,
        TransformationSchedulesAPI,
        TransformationSchemaAPI,
    )
    from cognite.client._api.units import UnitSystemAPI
    from cognite.client._api.user_profiles import UserProfilesAPI
    from cognite.client._api.workflows import (
        WorkflowExecutionAPI,
        WorkflowTaskAPI,
        WorkflowTriggerAPI,
        WorkflowVersionAPI,
    )


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


def _make_accessors_for_building_docs() -> None:
    CogniteClient.assets = AssetsAPI  # type: ignore
    CogniteClient.events = EventsAPI  # type: ignore
    CogniteClient.files = FilesAPI  # type: ignore
    CogniteClient.iam = IAMAPI  # type: ignore
    CogniteClient.iam.token = TokenAPI  # type: ignore
    CogniteClient.iam.groups = GroupsAPI  # type: ignore
    CogniteClient.iam.security_categories = SecurityCategoriesAPI  # type: ignore
    CogniteClient.iam.sessions = SessionsAPI  # type: ignore
    CogniteClient.iam.user_profiles = UserProfilesAPI  # type: ignore
    CogniteClient.data_sets = DataSetsAPI  # type: ignore
    CogniteClient.sequences = SequencesAPI  # type: ignore
    CogniteClient.sequences.data = SequencesDataAPI  # type: ignore
    CogniteClient.time_series = TimeSeriesAPI  # type: ignore
    CogniteClient.time_series.data = DatapointsAPI  # type: ignore
    CogniteClient.time_series.data.synthetic = SyntheticDatapointsAPI  # type: ignore
    CogniteClient.time_series.subscriptions = DatapointsSubscriptionAPI  # type: ignore
    CogniteClient.geospatial = GeospatialAPI  # type: ignore
    CogniteClient.raw = RawAPI  # type: ignore
    CogniteClient.raw.databases = RawDatabasesAPI  # type: ignore
    CogniteClient.raw.tables = RawTablesAPI  # type: ignore
    CogniteClient.raw.rows = RawRowsAPI  # type: ignore
    CogniteClient.three_d = ThreeDAPI  # type: ignore
    CogniteClient.three_d.models = ThreeDModelsAPI  # type: ignore
    CogniteClient.three_d.revisions = ThreeDRevisionsAPI  # type: ignore
    CogniteClient.three_d.files = ThreeDFilesAPI  # type: ignore
    CogniteClient.three_d.asset_mappings = ThreeDAssetMappingAPI  # type: ignore
    CogniteClient.labels = LabelsAPI  #  type: ignore
    CogniteClient.relationships = RelationshipsAPI  # type: ignore
    CogniteClient.entity_matching = EntityMatchingAPI  # type: ignore
    CogniteClient.templates = TemplatesAPI  # type: ignore
    CogniteClient.templates.groups = TemplateGroupsAPI  # type: ignore
    CogniteClient.templates.versions = TemplateGroupVersionsAPI  # type: ignore
    CogniteClient.templates.instances = TemplateInstancesAPI  # type: ignore
    CogniteClient.templates.views = TemplateViewsAPI  # type: ignore
    CogniteClient.vision = VisionAPI  # type: ignore
    CogniteClient.extraction_pipelines = ExtractionPipelinesAPI  # type: ignore
    CogniteClient.extraction_pipelines.runs = ExtractionPipelineRunsAPI  # type: ignore
    CogniteClient.extraction_pipelines.config = ExtractionPipelineConfigsAPI  # type: ignore
    CogniteClient.hosted_extractors = HostedExtractorsAPI  # type: ignore
    CogniteClient.hosted_extractors.sources = SourcesAPI  # type: ignore
    CogniteClient.hosted_extractors.jobs = JobsAPI  # type: ignore
    CogniteClient.hosted_extractors.destinations = DestinationsAPI  # type: ignore
    CogniteClient.hosted_extractors.mappings = MappingsAPI  # type: ignore
    CogniteClient.transformations = TransformationsAPI  # type: ignore
    CogniteClient.transformations.schedules = TransformationSchedulesAPI  # type: ignore
    CogniteClient.transformations.notifications = TransformationNotificationsAPI  # type: ignore
    CogniteClient.transformations.jobs = TransformationJobsAPI  # type: ignore
    CogniteClient.transformations.schema = TransformationSchemaAPI  # type: ignore
    CogniteClient.diagrams = DiagramsAPI  # type: ignore
    CogniteClient.annotations = AnnotationsAPI  # type: ignore
    CogniteClient.functions = FunctionsAPI  # type: ignore
    CogniteClient.functions.calls = FunctionCallsAPI  # type: ignore
    CogniteClient.functions.schedules = FunctionSchedulesAPI  # type: ignore
    CogniteClient.data_modeling = DataModelingAPI  # type: ignore
    CogniteClient.data_modeling.data_models = DataModelsAPI  # type: ignore
    CogniteClient.data_modeling.spaces = SpacesAPI  # type: ignore
    CogniteClient.data_modeling.views = ViewsAPI  # type: ignore
    CogniteClient.data_modeling.containers = ContainersAPI  # type: ignore
    CogniteClient.data_modeling.instances = InstancesAPI  # type: ignore
    CogniteClient.data_modeling.graphql = DataModelingGraphQLAPI  # type: ignore
    CogniteClient.documents = DocumentsAPI  # type: ignore
    CogniteClient.documents.previews = DocumentPreviewAPI  # type: ignore
    CogniteClient.workflows = WorkflowAPI  # type: ignore
    CogniteClient.workflows.versions = WorkflowVersionAPI  # type: ignore
    CogniteClient.workflows.executions = WorkflowExecutionAPI  # type: ignore
    CogniteClient.workflows.tasks = WorkflowTaskAPI  # type: ignore
    CogniteClient.workflows.triggers = WorkflowTriggerAPI  # type: ignore
    CogniteClient.units = UnitAPI  # type: ignore
    CogniteClient.units.systems = UnitSystemAPI  # type: ignore


if _build_docs == "true":
    _make_accessors_for_building_docs()

    @classmethod  # type: ignore
    def load(cls: type[CogniteClient], config: dict[str, Any] | str) -> CogniteClient:
        """Load a cognite client object from a YAML/JSON string or dict.

        Args:
            cls (type[CogniteClient]): The CogniteClient class.
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
        loaded = load_resource_to_dict(config)
        return cls(config=ClientConfig.load(loaded))
