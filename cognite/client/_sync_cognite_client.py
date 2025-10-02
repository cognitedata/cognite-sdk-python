"""
===================================================
This file is auto-generated - do not edit manually!
===================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

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
