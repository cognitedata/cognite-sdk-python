from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Dict, Optional

from requests import Response

from cognite.client import utils
from cognite.client._api.annotations import AnnotationsAPI
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.data_sets import DataSetsAPI
from cognite.client._api.diagrams import DiagramsAPI
from cognite.client._api.entity_matching import EntityMatchingAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.extractionpipelines import ExtractionPipelineRunsAPI, ExtractionPipelinesAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.functions import FunctionsAPI
from cognite.client._api.geospatial import GeospatialAPI
from cognite.client._api.iam import IAMAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.login import LoginAPI
from cognite.client._api.raw import RawAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._api.sequences import SequencesAPI
from cognite.client._api.templates import TemplatesAPI
from cognite.client._api.three_d import ThreeDAPI
from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client._api.transformations import TransformationsAPI
from cognite.client._api.vision import VisionAPI
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig, global_config

if TYPE_CHECKING:
    from cognite.client._api.datapoints import DatapointsAPI


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
        self.login = LoginAPI(self._config, cognite_client=self)
        self.assets = AssetsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.events = EventsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.files = FilesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.iam = IAMAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.data_sets = DataSetsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.sequences = SequencesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.time_series = TimeSeriesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.geospatial = GeospatialAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.raw = RawAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.three_d = ThreeDAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.labels = LabelsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.relationships = RelationshipsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.entity_matching = EntityMatchingAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.templates = TemplatesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.vision = VisionAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.extraction_pipelines = ExtractionPipelinesAPI(
            self._config, api_version=self._API_VERSION, cognite_client=self
        )
        self.transformations = TransformationsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.diagrams = DiagramsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.annotations = AnnotationsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.functions = FunctionsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)

        self._api_client = APIClient(self._config, cognite_client=self)

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

    @property  # TODO (v6.0.0): Delete this whole property
    def datapoints(self) -> DatapointsAPI:
        if int(self.version.split(".")[0]) >= 6:
            raise AttributeError(  # ...in case we forget to delete this property in v6...
                "'CogniteClient' object has no attribute 'datapoints'. Use 'time_series.data' instead."
            )
        warnings.warn(
            "Accessing the DatapointsAPI through `client.datapoints` is deprecated and will be removed "
            "in major version 6.0.0. Use `client.time_series.data` instead.",
            DeprecationWarning,
        )
        return self.time_series.data

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

    @property  # TODO (v6.0.0): Delete this whole property
    def extraction_pipeline_runs(self) -> ExtractionPipelineRunsAPI:
        if int(self.version.split(".")[0]) >= 6:
            raise AttributeError(
                "'CogniteClient' object has no attribute 'extraction_pipeline_runs'. Use 'extraction_pipelines.runs' instead."
            )
        warnings.warn(
            "Accessing the ExtractionPipelineRunsAPI through `client.extraction_pipeline_runs` is deprecated and will be removed "
            "in major version 6.0.0. Use `client.extraction_pipelines.runs` instead.",
            DeprecationWarning,
        )
        return self.extraction_pipelines.runs
