from __future__ import annotations

import os
from typing import Any, Dict, Optional

from requests import Response

from cognite.client import utils
from cognite.client._api.annotations import AnnotationsAPI
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.data_modeling import DataModelingAPI
from cognite.client._api.data_modeling.containers import ContainersAPI
from cognite.client._api.data_modeling.data_models import DataModelsAPI
from cognite.client._api.data_modeling.spaces import SpacesAPI
from cognite.client._api.data_modeling.views import ViewsAPI
from cognite.client._api.data_sets import DataSetsAPI
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.diagrams import DiagramsAPI
from cognite.client._api.entity_matching import EntityMatchingAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.extractionpipelines import (
    ExtractionPipelineConfigsAPI,
    ExtractionPipelineRunsAPI,
    ExtractionPipelinesAPI,
)
from cognite.client._api.files import FilesAPI
from cognite.client._api.functions import FunctionCallsAPI, FunctionsAPI, FunctionSchedulesAPI
from cognite.client._api.geospatial import GeospatialAPI
from cognite.client._api.iam import IAMAPI, GroupsAPI, SecurityCategoriesAPI, SessionsAPI, TokenAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.raw import RawAPI, RawDatabasesAPI, RawRowsAPI, RawTablesAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._api.sequences import SequencesAPI, SequencesDataAPI
from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
from cognite.client._api.templates import (
    TemplateGroupsAPI,
    TemplateGroupVersionsAPI,
    TemplateInstancesAPI,
    TemplatesAPI,
    TemplateViewsAPI,
)
from cognite.client._api.three_d import (
    ThreeDAPI,
    ThreeDAssetMappingAPI,
    ThreeDFilesAPI,
    ThreeDModelsAPI,
    ThreeDRevisionsAPI,
)
from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client._api.transformations import (
    TransformationJobsAPI,
    TransformationNotificationsAPI,
    TransformationsAPI,
    TransformationSchedulesAPI,
    TransformationSchemaAPI,
)
from cognite.client._api.vision import VisionAPI
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig, global_config

build_docs = os.getenv("BUILD_COGNITE_SDK_DOCS")


class CogniteClient:
    """Main entrypoint into Cognite Python SDK.

    All services are made available through this object. See examples below.

    Args:
        config (ClientConfig): The configuration for this client.
    """

    _API_VERSION = "v1"

    def __init__(self, config: Optional[ClientConfig] = None) -> None:
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

        # APIs just using base_url:
        self._api_client = APIClient(self._config, api_version=None, cognite_client=self)

    def get(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None) -> Response:
        """Perform a GET request to an arbitrary path in the API."""
        return self._api_client._get(url, params=params, headers=headers)

    def post(
        self, url: str, json: Dict[str, Any], params: Dict[str, Any] = None, headers: Dict[str, Any] = None
    ) -> Response:
        """Perform a POST request to an arbitrary path in the API."""
        return self._api_client._post(url, json=json, params=params, headers=headers)

    def put(self, url: str, json: Dict[str, Any] = None, headers: Dict[str, Any] = None) -> Response:
        """Perform a PUT request to an arbitrary path in the API."""
        return self._api_client._put(url, json=json, headers=headers)

    def delete(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None) -> Response:
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


def make_accessors_for_building_docs() -> None:
    CogniteClient.assets = AssetsAPI  # type: ignore
    CogniteClient.events = EventsAPI  # type: ignore
    CogniteClient.files = FilesAPI  # type: ignore
    CogniteClient.iam = IAMAPI  # type: ignore
    CogniteClient.iam.token = TokenAPI  # type: ignore
    CogniteClient.iam.groups = GroupsAPI  # type: ignore
    CogniteClient.iam.security_categories = SecurityCategoriesAPI  # type: ignore
    CogniteClient.iam.sessions = SessionsAPI  # type: ignore
    CogniteClient.data_sets = DataSetsAPI  # type: ignore
    CogniteClient.sequences = SequencesAPI  # type: ignore
    CogniteClient.sequences.data = SequencesDataAPI  # type: ignore
    CogniteClient.time_series = TimeSeriesAPI  # type: ignore
    CogniteClient.time_series.data = DatapointsAPI  # type: ignore
    CogniteClient.time_series.data.synthetic = SyntheticDatapointsAPI  # type: ignore
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


if build_docs == "true":
    make_accessors_for_building_docs()
