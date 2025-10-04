from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client._api.agents import AgentsAPI
from cognite.client._api.ai import AIAPI
from cognite.client._api.ai.tools import AIToolsAPI
from cognite.client._api.ai.tools.documents import AIDocumentsAPI
from cognite.client._api.annotations import AnnotationsAPI
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.data_modeling import DataModelingAPI
from cognite.client._api.data_modeling.containers import ContainersAPI
from cognite.client._api.data_modeling.data_models import DataModelsAPI
from cognite.client._api.data_modeling.graphql import DataModelingGraphQLAPI
from cognite.client._api.data_modeling.instances import InstancesAPI
from cognite.client._api.data_modeling.space_statistics import SpaceStatisticsAPI
from cognite.client._api.data_modeling.spaces import SpacesAPI
from cognite.client._api.data_modeling.statistics import StatisticsAPI
from cognite.client._api.data_modeling.views import ViewsAPI
from cognite.client._api.data_sets import DataSetsAPI
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.datapoints_subscriptions import DatapointsSubscriptionAPI
from cognite.client._api.diagrams import DiagramsAPI
from cognite.client._api.document_preview import DocumentPreviewAPI
from cognite.client._api.documents import DocumentsAPI
from cognite.client._api.entity_matching import EntityMatchingAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.extractionpipelines import ExtractionPipelinesAPI
from cognite.client._api.extractionpipelines.configs import ExtractionPipelineConfigsAPI
from cognite.client._api.extractionpipelines.runs import ExtractionPipelineRunsAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.functions import FunctionsAPI
from cognite.client._api.functions.calls import FunctionCallsAPI
from cognite.client._api.functions.schedules import FunctionSchedulesAPI
from cognite.client._api.geospatial import GeospatialAPI
from cognite.client._api.hosted_extractors import HostedExtractorsAPI
from cognite.client._api.hosted_extractors.destinations import DestinationsAPI
from cognite.client._api.hosted_extractors.jobs import JobsAPI
from cognite.client._api.hosted_extractors.mappings import MappingsAPI
from cognite.client._api.hosted_extractors.sources import SourcesAPI
from cognite.client._api.iam import IAMAPI
from cognite.client._api.iam.groups import GroupsAPI
from cognite.client._api.iam.security_categories import SecurityCategoriesAPI
from cognite.client._api.iam.sessions import SessionsAPI
from cognite.client._api.iam.token import TokenAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.org_apis.principals import PrincipalsAPI
from cognite.client._api.postgres_gateway import PostgresGatewaysAPI
from cognite.client._api.postgres_gateway.tables import TablesAPI as PostgresTablesAPI
from cognite.client._api.postgres_gateway.users import UsersAPI as PostgresUsersAPI
from cognite.client._api.raw import RawAPI
from cognite.client._api.raw.databases import RawDatabasesAPI
from cognite.client._api.raw.rows import RawRowsAPI
from cognite.client._api.raw.tables import RawTablesAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._api.sequence_data import SequencesDataAPI
from cognite.client._api.sequences import SequencesAPI
from cognite.client._api.simulators import SimulatorsAPI
from cognite.client._api.simulators.integrations import SimulatorIntegrationsAPI
from cognite.client._api.simulators.logs import SimulatorLogsAPI
from cognite.client._api.simulators.models import SimulatorModelsAPI
from cognite.client._api.simulators.models_revisions import SimulatorModelRevisionsAPI
from cognite.client._api.simulators.routine_revisions import SimulatorRoutineRevisionsAPI
from cognite.client._api.simulators.routines import SimulatorRoutinesAPI
from cognite.client._api.simulators.runs import SimulatorRunsAPI
from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
from cognite.client._api.three_d import ThreeDAPI
from cognite.client._api.three_d.asset_mapping import ThreeDAssetMappingAPI
from cognite.client._api.three_d.files import ThreeDFilesAPI
from cognite.client._api.three_d.models import ThreeDModelsAPI
from cognite.client._api.three_d.revisions import ThreeDRevisionsAPI
from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client._api.transformations import TransformationsAPI
from cognite.client._api.transformations.jobs import TransformationJobsAPI
from cognite.client._api.transformations.notifications import TransformationNotificationsAPI
from cognite.client._api.transformations.schedules import TransformationSchedulesAPI
from cognite.client._api.transformations.schema import TransformationSchemaAPI
from cognite.client._api.unit_system import UnitSystemAPI
from cognite.client._api.units import UnitAPI
from cognite.client._api.user_profiles import UserProfilesAPI
from cognite.client._api.vision import VisionAPI
from cognite.client._api.workflows import WorkflowAPI
from cognite.client._api.workflows.executions import WorkflowExecutionAPI
from cognite.client._api.workflows.tasks import WorkflowTaskAPI
from cognite.client._api.workflows.triggers import WorkflowTriggerAPI
from cognite.client._api.workflows.versions import WorkflowVersionAPI
from cognite.client._sync_api.agents.agents import SyncAgentsAPI
from cognite.client._sync_api.ai import SyncAIAPI
from cognite.client._sync_api.ai.tools import SyncAIToolsAPI
from cognite.client._sync_api.ai.tools.documents import SyncAIDocumentsAPI
from cognite.client._sync_api.annotations import SyncAnnotationsAPI
from cognite.client._sync_api.assets import SyncAssetsAPI
from cognite.client._sync_api.data_modeling import SyncDataModelingAPI
from cognite.client._sync_api.data_modeling.containers import SyncContainersAPI
from cognite.client._sync_api.data_modeling.data_models import SyncDataModelsAPI
from cognite.client._sync_api.data_modeling.graphql import SyncDataModelingGraphQLAPI
from cognite.client._sync_api.data_modeling.instances import SyncInstancesAPI
from cognite.client._sync_api.data_modeling.space_statistics import SyncSpaceStatisticsAPI
from cognite.client._sync_api.data_modeling.spaces import SyncSpacesAPI
from cognite.client._sync_api.data_modeling.statistics import SyncStatisticsAPI
from cognite.client._sync_api.data_modeling.views import SyncViewsAPI
from cognite.client._sync_api.data_sets import SyncDataSetsAPI
from cognite.client._sync_api.datapoints import SyncDatapointsAPI
from cognite.client._sync_api.datapoints_subscriptions import SyncDatapointsSubscriptionAPI
from cognite.client._sync_api.diagrams import SyncDiagramsAPI
from cognite.client._sync_api.document_preview import SyncDocumentPreviewAPI
from cognite.client._sync_api.documents import SyncDocumentsAPI
from cognite.client._sync_api.entity_matching import SyncEntityMatchingAPI
from cognite.client._sync_api.events import SyncEventsAPI
from cognite.client._sync_api.extractionpipelines import SyncExtractionPipelinesAPI
from cognite.client._sync_api.extractionpipelines.configs import SyncExtractionPipelineConfigsAPI
from cognite.client._sync_api.extractionpipelines.runs import SyncExtractionPipelineRunsAPI
from cognite.client._sync_api.files import SyncFilesAPI
from cognite.client._sync_api.functions import SyncFunctionsAPI
from cognite.client._sync_api.functions.calls import SyncFunctionCallsAPI
from cognite.client._sync_api.functions.schedules import SyncFunctionSchedulesAPI
from cognite.client._sync_api.geospatial import SyncGeospatialAPI
from cognite.client._sync_api.hosted_extractors import SyncHostedExtractorsAPI
from cognite.client._sync_api.hosted_extractors.destinations import SyncDestinationsAPI
from cognite.client._sync_api.hosted_extractors.jobs import SyncJobsAPI
from cognite.client._sync_api.hosted_extractors.mappings import SyncMappingsAPI
from cognite.client._sync_api.hosted_extractors.sources import SyncSourcesAPI
from cognite.client._sync_api.iam import SyncIAMAPI
from cognite.client._sync_api.iam.groups import SyncGroupsAPI
from cognite.client._sync_api.iam.security_categories import SyncSecurityCategoriesAPI
from cognite.client._sync_api.iam.sessions import SyncSessionsAPI
from cognite.client._sync_api.iam.token import SyncTokenAPI
from cognite.client._sync_api.labels import SyncLabelsAPI
from cognite.client._sync_api.postgres_gateway import SyncPostgresGatewaysAPI
from cognite.client._sync_api.postgres_gateway.tables import SyncTablesAPI as SyncPostgresTablesAPI
from cognite.client._sync_api.postgres_gateway.users import SyncUsersAPI as SyncPostgresUsersAPI
from cognite.client._sync_api.raw import SyncRawAPI
from cognite.client._sync_api.raw.databases import SyncRawDatabasesAPI
from cognite.client._sync_api.raw.rows import SyncRawRowsAPI
from cognite.client._sync_api.raw.tables import SyncRawTablesAPI
from cognite.client._sync_api.relationships import SyncRelationshipsAPI
from cognite.client._sync_api.sequence_data import SyncSequencesDataAPI
from cognite.client._sync_api.sequences import SyncSequencesAPI
from cognite.client._sync_api.simulators import SyncSimulatorsAPI
from cognite.client._sync_api.simulators.integrations import SyncSimulatorIntegrationsAPI
from cognite.client._sync_api.simulators.logs import SyncSimulatorLogsAPI
from cognite.client._sync_api.simulators.models import SyncSimulatorModelsAPI
from cognite.client._sync_api.simulators.models_revisions import SyncSimulatorModelRevisionsAPI
from cognite.client._sync_api.simulators.routine_revisions import SyncSimulatorRoutineRevisionsAPI
from cognite.client._sync_api.simulators.routines import SyncSimulatorRoutinesAPI
from cognite.client._sync_api.simulators.runs import SyncSimulatorRunsAPI
from cognite.client._sync_api.synthetic_time_series import SyncSyntheticDatapointsAPI
from cognite.client._sync_api.three_d import Sync3DAPI
from cognite.client._sync_api.three_d.asset_mapping import Sync3DAssetMappingAPI
from cognite.client._sync_api.three_d.files import Sync3DFilesAPI
from cognite.client._sync_api.three_d.models import Sync3DModelsAPI
from cognite.client._sync_api.three_d.revisions import Sync3DRevisionsAPI
from cognite.client._sync_api.time_series import SyncTimeSeriesAPI
from cognite.client._sync_api.transformations import SyncTransformationsAPI
from cognite.client._sync_api.transformations.jobs import SyncTransformationJobsAPI
from cognite.client._sync_api.transformations.notifications import SyncTransformationNotificationsAPI
from cognite.client._sync_api.transformations.schedules import SyncTransformationSchedulesAPI
from cognite.client._sync_api.transformations.schema import SyncTransformationSchemaAPI
from cognite.client._sync_api.unit_system import SyncUnitSystemAPI
from cognite.client._sync_api.units import SyncUnitAPI
from cognite.client._sync_api.user_profiles import SyncUserProfilesAPI
from cognite.client._sync_api.vision import SyncVisionAPI
from cognite.client._sync_api.workflows import SyncWorkflowAPI
from cognite.client._sync_api.workflows.executions import SyncWorkflowExecutionAPI
from cognite.client._sync_api.workflows.tasks import SyncWorkflowTaskAPI
from cognite.client._sync_api.workflows.triggers import SyncWorkflowTriggerAPI
from cognite.client._sync_api.workflows.versions import SyncWorkflowVersionAPI


# TODO: Async methods should be AsyncMocks, so this needs some improvement:
class AsyncCogniteClientMock(MagicMock):
    """Mock for AsyncCogniteClient object

    All APIs are replaced with specced MagicMock objects and all async methods with AsyncMocks.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(spec=AsyncCogniteClient, *args, **kwargs)
        # Developer note:
        # - Please add your mocked APIs in chronological order
        # - For nested APIs:
        #   - Add spacing above and below
        #   - Use `spec=MyAPI` only for "top level"
        #   - Use `spec_set=MyNestedAPI` for all nested APIs
        self.ai = MagicMock(spec=AIAPI)
        self.ai.tools = MagicMock(spec=AIToolsAPI)
        self.ai.tools.documents = MagicMock(spec_set=AIDocumentsAPI)

        self.agents = MagicMock(spec_set=AgentsAPI)
        self.annotations = MagicMock(spec_set=AnnotationsAPI)
        self.assets = MagicMock(spec_set=AssetsAPI)

        self.data_modeling = MagicMock(spec=DataModelingAPI)
        self.data_modeling.containers = MagicMock(spec_set=ContainersAPI)
        self.data_modeling.data_models = MagicMock(spec_set=DataModelsAPI)
        self.data_modeling.spaces = MagicMock(spec_set=SpacesAPI)
        self.data_modeling.views = MagicMock(spec_set=ViewsAPI)
        self.data_modeling.instances = MagicMock(spec_set=InstancesAPI)
        self.data_modeling.graphql = MagicMock(spec_set=DataModelingGraphQLAPI)
        self.data_modeling.statistics = MagicMock(spec=StatisticsAPI)
        self.data_modeling.statistics.spaces = MagicMock(spec_set=SpaceStatisticsAPI)

        self.data_sets = MagicMock(spec_set=DataSetsAPI)

        self.diagrams = MagicMock(spec_set=DiagramsAPI)
        self.documents = MagicMock(spec=DocumentsAPI)
        self.documents.previews = MagicMock(spec_set=DocumentPreviewAPI)
        self.entity_matching = MagicMock(spec_set=EntityMatchingAPI)
        self.events = MagicMock(spec_set=EventsAPI)

        self.extraction_pipelines = MagicMock(spec=ExtractionPipelinesAPI)
        self.extraction_pipelines.config = MagicMock(spec_set=ExtractionPipelineConfigsAPI)
        self.extraction_pipelines.runs = MagicMock(spec_set=ExtractionPipelineRunsAPI)

        self.files = MagicMock(spec_set=FilesAPI)

        self.functions = MagicMock(spec=FunctionsAPI)
        self.functions.calls = MagicMock(spec_set=FunctionCallsAPI)
        self.functions.schedules = MagicMock(spec_set=FunctionSchedulesAPI)

        self.geospatial = MagicMock(spec_set=GeospatialAPI)

        self.iam = MagicMock(spec=IAMAPI)
        self.iam.groups = MagicMock(spec_set=GroupsAPI)
        self.iam.security_categories = MagicMock(spec_set=SecurityCategoriesAPI)
        self.iam.sessions = MagicMock(spec_set=SessionsAPI)
        self.iam.principals = MagicMock(spec_set=PrincipalsAPI)
        self.iam.user_profiles = MagicMock(spec_set=UserProfilesAPI)
        self.iam.token = MagicMock(spec_set=TokenAPI)

        self.labels = MagicMock(spec_set=LabelsAPI)

        self.raw = MagicMock(spec=RawAPI)
        self.raw.databases = MagicMock(spec_set=RawDatabasesAPI)
        self.raw.rows = MagicMock(spec_set=RawRowsAPI)
        self.raw.tables = MagicMock(spec_set=RawTablesAPI)

        self.relationships = MagicMock(spec_set=RelationshipsAPI)

        self.simulators = MagicMock(spec=SimulatorsAPI)
        self.simulators.integrations = MagicMock(spec_set=SimulatorIntegrationsAPI)
        self.simulators.models = MagicMock(spec=SimulatorModelsAPI)
        self.simulators.models.revisions = MagicMock(spec_set=SimulatorModelRevisionsAPI)
        self.simulators.runs = MagicMock(spec_set=SimulatorRunsAPI)
        self.simulators.routines = MagicMock(spec=SimulatorRoutinesAPI)
        self.simulators.routines.revisions = MagicMock(spec_set=SimulatorRoutineRevisionsAPI)
        self.simulators.logs = MagicMock(spec_set=SimulatorLogsAPI)

        self.sequences = MagicMock(spec=SequencesAPI)
        self.sequences.data = MagicMock(spec_set=SequencesDataAPI)

        self.hosted_extractors = MagicMock(spec=HostedExtractorsAPI)
        self.hosted_extractors.sources = MagicMock(spec_set=SourcesAPI)
        self.hosted_extractors.destinations = MagicMock(spec_set=DestinationsAPI)
        self.hosted_extractors.jobs = MagicMock(spec_set=JobsAPI)
        self.hosted_extractors.mappings = MagicMock(spec_set=MappingsAPI)

        self.postgres_gateway = MagicMock(spec=PostgresGatewaysAPI)
        self.postgres_gateway.users = MagicMock(spec_set=PostgresUsersAPI)
        self.postgres_gateway.tables = MagicMock(spec_set=PostgresTablesAPI)

        self.three_d = MagicMock(spec=ThreeDAPI)
        self.three_d.asset_mappings = MagicMock(spec_set=ThreeDAssetMappingAPI)
        self.three_d.files = MagicMock(spec_set=ThreeDFilesAPI)
        self.three_d.models = MagicMock(spec_set=ThreeDModelsAPI)
        self.three_d.revisions = MagicMock(spec_set=ThreeDRevisionsAPI)

        self.time_series = MagicMock(spec=TimeSeriesAPI)
        self.time_series.data = MagicMock(spec=DatapointsAPI)
        self.time_series.data.synthetic = MagicMock(spec_set=SyntheticDatapointsAPI)
        self.time_series.subscriptions = MagicMock(spec_set=DatapointsSubscriptionAPI)

        self.transformations = MagicMock(spec=TransformationsAPI)
        self.transformations.jobs = MagicMock(spec_set=TransformationJobsAPI)
        self.transformations.notifications = MagicMock(spec_set=TransformationNotificationsAPI)
        self.transformations.schedules = MagicMock(spec_set=TransformationSchedulesAPI)
        self.transformations.schema = MagicMock(spec_set=TransformationSchemaAPI)

        self.vision = MagicMock(spec_set=VisionAPI)

        self.workflows = MagicMock(spec=WorkflowAPI)
        self.workflows.versions = MagicMock(spec_set=WorkflowVersionAPI)
        self.workflows.executions = MagicMock(spec_set=WorkflowExecutionAPI)
        self.workflows.tasks = MagicMock(spec_set=WorkflowTaskAPI)
        self.workflows.triggers = MagicMock(spec_set=WorkflowTriggerAPI)

        self.units = MagicMock(spec=UnitAPI)
        self.units.systems = MagicMock(spec_set=UnitSystemAPI)


class CogniteClientMock(MagicMock):
    """Mock for CogniteClient object

    All APIs are replaced with specced MagicMock objects.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(spec=CogniteClient, *args, **kwargs)
        # Developer note:
        # - Please add your mocked APIs in chronological order
        # - For nested APIs:
        #   - Add spacing above and below
        #   - Use `spec=MyAPI` only for "top level"
        #   - Use `spec_set=MyNestedAPI` for all nested APIs
        self.ai = MagicMock(spec=SyncAIAPI)
        self.ai.tools = MagicMock(spec=SyncAIToolsAPI)
        self.ai.tools.documents = MagicMock(spec_set=SyncAIDocumentsAPI)

        self.agents = MagicMock(spec_set=SyncAgentsAPI)
        self.annotations = MagicMock(spec_set=SyncAnnotationsAPI)
        self.assets = MagicMock(spec_set=SyncAssetsAPI)

        self.data_modeling = MagicMock(spec=SyncDataModelingAPI)
        self.data_modeling.containers = MagicMock(spec_set=SyncContainersAPI)
        self.data_modeling.data_models = MagicMock(spec_set=SyncDataModelsAPI)
        self.data_modeling.spaces = MagicMock(spec_set=SyncSpacesAPI)
        self.data_modeling.views = MagicMock(spec_set=SyncViewsAPI)
        self.data_modeling.instances = MagicMock(spec_set=SyncInstancesAPI)
        self.data_modeling.graphql = MagicMock(spec_set=SyncDataModelingGraphQLAPI)
        self.data_modeling.statistics = MagicMock(spec=SyncStatisticsAPI)
        self.data_modeling.statistics.spaces = MagicMock(spec_set=SyncSpaceStatisticsAPI)

        self.data_sets = MagicMock(spec_set=SyncDataSetsAPI)

        self.diagrams = MagicMock(spec_set=SyncDiagramsAPI)
        self.documents = MagicMock(spec=SyncDocumentsAPI)
        self.documents.previews = MagicMock(spec_set=SyncDocumentPreviewAPI)
        self.entity_matching = MagicMock(spec_set=SyncEntityMatchingAPI)
        self.events = MagicMock(spec_set=SyncEventsAPI)

        self.extraction_pipelines = MagicMock(spec=SyncExtractionPipelinesAPI)
        self.extraction_pipelines.config = MagicMock(spec_set=SyncExtractionPipelineConfigsAPI)
        self.extraction_pipelines.runs = MagicMock(spec_set=SyncExtractionPipelineRunsAPI)

        self.files = MagicMock(spec_set=SyncFilesAPI)

        self.functions = MagicMock(spec=SyncFunctionsAPI)
        self.functions.calls = MagicMock(spec_set=SyncFunctionCallsAPI)
        self.functions.schedules = MagicMock(spec_set=SyncFunctionSchedulesAPI)

        self.geospatial = MagicMock(spec_set=SyncGeospatialAPI)

        self.iam = MagicMock(spec=SyncIAMAPI)
        self.iam.groups = MagicMock(spec_set=SyncGroupsAPI)
        self.iam.security_categories = MagicMock(spec_set=SyncSecurityCategoriesAPI)
        self.iam.sessions = MagicMock(spec_set=SyncSessionsAPI)
        self.iam.user_profiles = MagicMock(spec_set=SyncUserProfilesAPI)
        self.iam.token = MagicMock(spec_set=SyncTokenAPI)

        self.labels = MagicMock(spec_set=SyncLabelsAPI)

        self.raw = MagicMock(spec=SyncRawAPI)
        self.raw.databases = MagicMock(spec_set=SyncRawDatabasesAPI)
        self.raw.rows = MagicMock(spec_set=SyncRawRowsAPI)
        self.raw.tables = MagicMock(spec_set=SyncRawTablesAPI)

        self.relationships = MagicMock(spec_set=SyncRelationshipsAPI)

        self.simulators = MagicMock(spec=SyncSimulatorsAPI)
        self.simulators.integrations = MagicMock(spec_set=SyncSimulatorIntegrationsAPI)
        self.simulators.models = MagicMock(spec=SyncSimulatorModelsAPI)
        self.simulators.models.revisions = MagicMock(spec_set=SyncSimulatorModelRevisionsAPI)
        self.simulators.runs = MagicMock(spec_set=SyncSimulatorRunsAPI)
        self.simulators.routines = MagicMock(spec=SyncSimulatorRoutinesAPI)
        self.simulators.routines.revisions = MagicMock(spec_set=SyncSimulatorRoutineRevisionsAPI)
        self.simulators.logs = MagicMock(spec_set=SyncSimulatorLogsAPI)

        self.sequences = MagicMock(spec=SyncSequencesAPI)
        self.sequences.data = MagicMock(spec_set=SyncSequencesDataAPI)

        self.hosted_extractors = MagicMock(spec=SyncHostedExtractorsAPI)
        self.hosted_extractors.sources = MagicMock(spec_set=SyncSourcesAPI)
        self.hosted_extractors.destinations = MagicMock(spec_set=SyncDestinationsAPI)
        self.hosted_extractors.jobs = MagicMock(spec_set=SyncJobsAPI)
        self.hosted_extractors.mappings = MagicMock(spec_set=SyncMappingsAPI)

        self.postgres_gateway = MagicMock(spec=SyncPostgresGatewaysAPI)
        self.postgres_gateway.users = MagicMock(spec_set=SyncPostgresUsersAPI)
        self.postgres_gateway.tables = MagicMock(spec_set=SyncPostgresTablesAPI)

        self.three_d = MagicMock(spec=Sync3DAPI)
        self.three_d.asset_mappings = MagicMock(spec_set=Sync3DAssetMappingAPI)
        self.three_d.files = MagicMock(spec_set=Sync3DFilesAPI)
        self.three_d.models = MagicMock(spec_set=Sync3DModelsAPI)
        self.three_d.revisions = MagicMock(spec_set=Sync3DRevisionsAPI)

        self.time_series = MagicMock(spec=SyncTimeSeriesAPI)
        self.time_series.data = MagicMock(spec=SyncDatapointsAPI)
        self.time_series.data.synthetic = MagicMock(spec_set=SyncSyntheticDatapointsAPI)
        self.time_series.subscriptions = MagicMock(spec_set=SyncDatapointsSubscriptionAPI)

        self.transformations = MagicMock(spec=SyncTransformationsAPI)
        self.transformations.jobs = MagicMock(spec_set=SyncTransformationJobsAPI)
        self.transformations.notifications = MagicMock(spec_set=SyncTransformationNotificationsAPI)
        self.transformations.schedules = MagicMock(spec_set=SyncTransformationSchedulesAPI)
        self.transformations.schema = MagicMock(spec_set=SyncTransformationSchemaAPI)

        self.vision = MagicMock(spec_set=SyncVisionAPI)

        self.workflows = MagicMock(spec=SyncWorkflowAPI)
        self.workflows.versions = MagicMock(spec_set=SyncWorkflowVersionAPI)
        self.workflows.executions = MagicMock(spec_set=SyncWorkflowExecutionAPI)
        self.workflows.tasks = MagicMock(spec_set=SyncWorkflowTaskAPI)
        self.workflows.triggers = MagicMock(spec_set=SyncWorkflowTriggerAPI)

        self.units = MagicMock(spec=SyncUnitAPI)
        self.units.systems = MagicMock(spec_set=SyncUnitSystemAPI)


@contextmanager
def monkeypatch_cognite_client() -> Iterator[AsyncCogniteClientMock]:
    """Context manager for monkeypatching the AsyncCogniteClient.

    Will patch all clients and replace them with specced MagicMock objects.

    Yields:
        AsyncCogniteClientMock: The mock with which the AsyncCogniteClient has been replaced

    Examples:

        In this example we can run the following code without actually executing the underlying API calls::

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import TimeSeriesWrite
            >>> from cognite.client.testing import monkeypatch_cognite_client
            >>>
            >>> with monkeypatch_cognite_client():
            >>>     client = AsyncCogniteClient()
            >>>     client.time_series.create(TimeSeriesWrite(external_id="blabla"))

        This example shows how to set the return value of a given method::

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.iam import TokenInspection
            >>> from cognite.client.testing import monkeypatch_cognite_client
            >>>
            >>> with monkeypatch_cognite_client() as c_mock:
            >>>     c_mock.iam.token.inspect.return_value = TokenInspection(
            >>>         subject="subject", projects=[], capabilities=[]
            >>>     )
            >>>     client = AsyncCogniteClient()
            >>>     res = client.iam.token.inspect()
            >>>     assert "subject" == res.subject

        Here you can see how to have a given method raise an exception::

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.exceptions import CogniteAPIError
            >>> from cognite.client.testing import monkeypatch_cognite_client
            >>>
            >>> with monkeypatch_cognite_client() as c_mock:
            >>>     c_mock.iam.token.inspect.side_effect = CogniteAPIError(message="Something went wrong", code=400)
            >>>     client = AsyncCogniteClient()
            >>>     try:
            >>>         res = client.iam.token.inspect()
            >>>     except CogniteAPIError as e:
            >>>         assert 400 == e.code
            >>>         assert "Something went wrong" == e.message
    """
    cognite_client_mock = AsyncCogniteClientMock()
    AsyncCogniteClient.__new__ = lambda *args, **kwargs: cognite_client_mock  # type: ignore[method-assign]
    yield cognite_client_mock
    AsyncCogniteClient.__new__ = lambda cls, *args, **kwargs: object.__new__(cls)  # type: ignore[method-assign]
