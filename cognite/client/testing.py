from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, create_autospec

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
from cognite.client._sync_api.org_apis.principals import SyncPrincipalsAPI
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


def flip_spec_set_on(*mocked_apis: MagicMock) -> None:
    for m in mocked_apis:
        m._spec_set = True


class _SpecSetEnforcer(type):
    """Metaclass that enforces spec_set=True on the top-level object (our client)"""

    # This is called when users do AsyncCogniteClientMock():
    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        instance = super().__call__(*args, **kwargs)
        # Now that the instance is fully constructed, we can freeze attribute assignments:
        instance._spec_set = True
        return instance


class AsyncCogniteClientMock(MagicMock, metaclass=_SpecSetEnforcer):
    """Mock for AsyncCogniteClient object

    All APIs are replaced with specced MagicMock objects and all async methods with AsyncMocks.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(spec=AsyncCogniteClient, *args, **kwargs)
        # Developer note:
        # - Please add your mocked APIs in chronological order
        # - Use create_autospec with instance=True for better type safety and accurate mocking.
        #     For simple APIs, also pass spec_set=True to block arbitrary assignments.
        # - Build composite APIs bottom-up (you can compose by passing kwargs to create_autospec
        #     as long as you don't pass spec_set=True).
        # - Use flip_spec_set_on afterwards for proper spec enforcement on composite APIs
        # (- Now repeat for CogniteClientMock)

        ai_tools_documents = create_autospec(AIDocumentsAPI, instance=True, spec_set=True)
        ai_tools = create_autospec(AIToolsAPI, instance=True, documents=ai_tools_documents)
        self.ai = create_autospec(AIAPI, instance=True, tools=ai_tools)
        flip_spec_set_on(self.ai, ai_tools)

        self.agents = create_autospec(AgentsAPI, instance=True, spec_set=True)
        self.annotations = create_autospec(AnnotationsAPI, instance=True, spec_set=True)
        self.assets = create_autospec(AssetsAPI, instance=True, spec_set=True)

        dm_space_statistics = create_autospec(SpaceStatisticsAPI, instance=True, spec_set=True)
        dm_statistics = create_autospec(StatisticsAPI, instance=True, spaces=dm_space_statistics)
        dm_containers = create_autospec(ContainersAPI, instance=True, spec_set=True)
        dm_data_models = create_autospec(DataModelsAPI, instance=True, spec_set=True)
        dm_spaces = create_autospec(SpacesAPI, instance=True, spec_set=True)
        dm_views = create_autospec(ViewsAPI, instance=True, spec_set=True)
        dm_instances = create_autospec(InstancesAPI, instance=True, spec_set=True)
        dm_graphql = create_autospec(DataModelingGraphQLAPI, instance=True, spec_set=True)
        self.data_modeling = create_autospec(
            DataModelingAPI,
            instance=True,
            containers=dm_containers,
            data_models=dm_data_models,
            spaces=dm_spaces,
            views=dm_views,
            instances=dm_instances,
            graphql=dm_graphql,
            statistics=dm_statistics,
        )
        flip_spec_set_on(self.data_modeling, dm_statistics)

        self.data_sets = create_autospec(DataSetsAPI, instance=True, spec_set=True)

        self.diagrams = create_autospec(DiagramsAPI, instance=True, spec_set=True)
        documents_previews = create_autospec(DocumentPreviewAPI, instance=True, spec_set=True)
        self.documents = create_autospec(DocumentsAPI, instance=True, previews=documents_previews)
        self.entity_matching = create_autospec(EntityMatchingAPI, instance=True, spec_set=True)
        self.events = create_autospec(EventsAPI, instance=True, spec_set=True)
        flip_spec_set_on(self.documents)

        extpipes_config = create_autospec(ExtractionPipelineConfigsAPI, instance=True, spec_set=True)
        extpipes_runs = create_autospec(ExtractionPipelineRunsAPI, instance=True, spec_set=True)
        self.extraction_pipelines = create_autospec(
            ExtractionPipelinesAPI, instance=True, config=extpipes_config, runs=extpipes_runs
        )
        flip_spec_set_on(self.extraction_pipelines)

        self.files = create_autospec(FilesAPI, instance=True, spec_set=True)

        fns_calls = create_autospec(FunctionCallsAPI, instance=True, spec_set=True)
        fns_schedules = create_autospec(FunctionSchedulesAPI, instance=True, spec_set=True)
        self.functions = create_autospec(FunctionsAPI, instance=True, calls=fns_calls, schedules=fns_schedules)
        flip_spec_set_on(self.functions)

        self.geospatial = create_autospec(GeospatialAPI, instance=True, spec_set=True)

        iam_groups = create_autospec(GroupsAPI, instance=True, spec_set=True)
        iam_security_categories = create_autospec(SecurityCategoriesAPI, instance=True, spec_set=True)
        iam_sessions = create_autospec(SessionsAPI, instance=True, spec_set=True)
        iam_principals = create_autospec(PrincipalsAPI, instance=True, spec_set=True)
        iam_user_profiles = create_autospec(UserProfilesAPI, instance=True, spec_set=True)
        iam_token = create_autospec(TokenAPI, instance=True, spec_set=True)
        self.iam = create_autospec(
            IAMAPI,
            instance=True,
            groups=iam_groups,
            security_categories=iam_security_categories,
            sessions=iam_sessions,
            principals=iam_principals,
            user_profiles=iam_user_profiles,
            token=iam_token,
        )
        flip_spec_set_on(self.iam)

        self.labels = create_autospec(LabelsAPI, instance=True, spec_set=True)

        raw_databases = create_autospec(RawDatabasesAPI, instance=True, spec_set=True)
        raw_rows = create_autospec(RawRowsAPI, instance=True, spec_set=True)
        raw_tables = create_autospec(RawTablesAPI, instance=True, spec_set=True)
        self.raw = create_autospec(RawAPI, instance=True, databases=raw_databases, rows=raw_rows, tables=raw_tables)
        flip_spec_set_on(self.raw)

        self.relationships = create_autospec(RelationshipsAPI, instance=True, spec_set=True)

        sim_integrations = create_autospec(SimulatorIntegrationsAPI, instance=True, spec_set=True)
        sim_models_revisions = create_autospec(SimulatorModelRevisionsAPI, instance=True, spec_set=True)
        sim_models = create_autospec(SimulatorModelsAPI, instance=True, revisions=sim_models_revisions)
        sim_runs = create_autospec(SimulatorRunsAPI, instance=True, spec_set=True)
        sim_routines_revisions = create_autospec(SimulatorRoutineRevisionsAPI, instance=True, spec_set=True)
        sim_routines = create_autospec(SimulatorRoutinesAPI, instance=True, revisions=sim_routines_revisions)
        sim_logs = create_autospec(SimulatorLogsAPI, instance=True, spec_set=True)
        self.simulators = create_autospec(
            SimulatorsAPI,
            instance=True,
            integrations=sim_integrations,
            models=sim_models,
            runs=sim_runs,
            routines=sim_routines,
            logs=sim_logs,
        )
        flip_spec_set_on(self.simulators, sim_models, sim_routines)

        sequences_data = create_autospec(SequencesDataAPI, instance=True, spec_set=True)
        self.sequences = create_autospec(SequencesAPI, instance=True, data=sequences_data)
        flip_spec_set_on(self.sequences)

        ho_ex_sources = create_autospec(SourcesAPI, instance=True, spec_set=True)
        ho_ex_destinations = create_autospec(DestinationsAPI, instance=True, spec_set=True)
        ho_ex_jobs = create_autospec(JobsAPI, instance=True, spec_set=True)
        ho_ex_mappings = create_autospec(MappingsAPI, instance=True, spec_set=True)
        self.hosted_extractors = create_autospec(
            HostedExtractorsAPI,
            instance=True,
            sources=ho_ex_sources,
            destinations=ho_ex_destinations,
            jobs=ho_ex_jobs,
            mappings=ho_ex_mappings,
        )
        flip_spec_set_on(self.hosted_extractors)

        pg_gw_users = create_autospec(PostgresUsersAPI, instance=True, spec_set=True)
        pg_gw_tables = create_autospec(PostgresTablesAPI, instance=True, spec_set=True)
        self.postgres_gateway = create_autospec(
            PostgresGatewaysAPI, instance=True, users=pg_gw_users, tables=pg_gw_tables
        )
        flip_spec_set_on(self.postgres_gateway)

        three_d_asset_mappings = create_autospec(ThreeDAssetMappingAPI, instance=True, spec_set=True)
        three_d_files = create_autospec(ThreeDFilesAPI, instance=True, spec_set=True)
        three_d_models = create_autospec(ThreeDModelsAPI, instance=True, spec_set=True)
        three_d_revisions = create_autospec(ThreeDRevisionsAPI, instance=True, spec_set=True)
        self.three_d = create_autospec(
            ThreeDAPI,
            instance=True,
            asset_mappings=three_d_asset_mappings,
            files=three_d_files,
            models=three_d_models,
            revisions=three_d_revisions,
        )
        flip_spec_set_on(self.three_d)

        ts_synthetic = create_autospec(SyntheticDatapointsAPI, instance=True, spec_set=True)
        ts_data = create_autospec(DatapointsAPI, instance=True, synthetic=ts_synthetic)
        ts_subscriptions = create_autospec(DatapointsSubscriptionAPI, instance=True, spec_set=True)
        self.time_series = create_autospec(TimeSeriesAPI, instance=True, data=ts_data, subscriptions=ts_subscriptions)
        flip_spec_set_on(self.time_series, ts_data)

        tr_jobs = create_autospec(TransformationJobsAPI, instance=True, spec_set=True)
        tr_notifications = create_autospec(TransformationNotificationsAPI, instance=True, spec_set=True)
        tr_schedules = create_autospec(TransformationSchedulesAPI, instance=True, spec_set=True)
        tr_schema = create_autospec(TransformationSchemaAPI, instance=True, spec_set=True)
        self.transformations = create_autospec(
            TransformationsAPI,
            instance=True,
            jobs=tr_jobs,
            notifications=tr_notifications,
            schedules=tr_schedules,
            schema=tr_schema,
        )
        flip_spec_set_on(self.transformations)

        self.vision = create_autospec(VisionAPI, instance=True, spec_set=True)

        wf_versions = create_autospec(WorkflowVersionAPI, instance=True, spec_set=True)
        wf_executions = create_autospec(WorkflowExecutionAPI, instance=True, spec_set=True)
        wf_tasks = create_autospec(WorkflowTaskAPI, instance=True, spec_set=True)
        wf_triggers = create_autospec(WorkflowTriggerAPI, instance=True, spec_set=True)
        self.workflows = create_autospec(
            WorkflowAPI,
            instance=True,
            versions=wf_versions,
            executions=wf_executions,
            tasks=wf_tasks,
            triggers=wf_triggers,
        )
        flip_spec_set_on(self.workflows)

        units_systems = create_autospec(UnitSystemAPI, instance=True, spec_set=True)
        self.units = create_autospec(UnitAPI, instance=True, systems=units_systems)
        flip_spec_set_on(self.units)


class CogniteClientMock(MagicMock, metaclass=_SpecSetEnforcer):
    """Mock for CogniteClient object

    All APIs are replaced with specced MagicMock objects.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(spec=CogniteClient, *args, **kwargs)
        # Developer note:
        # - Please add your mocked APIs in chronological order
        # - Use create_autospec with instance=True for better type safety and accurate mocking.
        #     For simple APIs, also pass spec_set=True to block arbitrary assignments.
        # - Build composite APIs bottom-up (you can compose by passing kwargs to create_autospec
        #     as long as you don't pass spec_set=True).
        # - Use flip_spec_set_on afterwards for proper spec enforcement on composite APIs
        # (- Now repeat for AsyncCogniteClientMock)

        ai_tools_documents = create_autospec(SyncAIDocumentsAPI, instance=True, spec_set=True)
        ai_tools = create_autospec(SyncAIToolsAPI, instance=True, documents=ai_tools_documents)
        self.ai = create_autospec(SyncAIAPI, instance=True, tools=ai_tools)
        flip_spec_set_on(self.ai, ai_tools)

        self.agents = create_autospec(SyncAgentsAPI, instance=True, spec_set=True)
        self.annotations = create_autospec(SyncAnnotationsAPI, instance=True, spec_set=True)
        self.assets = create_autospec(SyncAssetsAPI, instance=True, spec_set=True)

        dm_space_statistics = create_autospec(SyncSpaceStatisticsAPI, instance=True, spec_set=True)
        dm_statistics = create_autospec(SyncStatisticsAPI, instance=True, spaces=dm_space_statistics)
        dm_containers = create_autospec(SyncContainersAPI, instance=True, spec_set=True)
        dm_data_models = create_autospec(SyncDataModelsAPI, instance=True, spec_set=True)
        dm_spaces = create_autospec(SyncSpacesAPI, instance=True, spec_set=True)
        dm_views = create_autospec(SyncViewsAPI, instance=True, spec_set=True)
        dm_instances = create_autospec(SyncInstancesAPI, instance=True, spec_set=True)
        dm_graphql = create_autospec(SyncDataModelingGraphQLAPI, instance=True, spec_set=True)
        self.data_modeling = create_autospec(
            SyncDataModelingAPI,
            instance=True,
            containers=dm_containers,
            data_models=dm_data_models,
            spaces=dm_spaces,
            views=dm_views,
            instances=dm_instances,
            graphql=dm_graphql,
            statistics=dm_statistics,
        )
        flip_spec_set_on(self.data_modeling, dm_statistics)

        self.data_sets = create_autospec(SyncDataSetsAPI, instance=True, spec_set=True)

        self.diagrams = create_autospec(SyncDiagramsAPI, instance=True, spec_set=True)
        documents_previews = create_autospec(SyncDocumentPreviewAPI, instance=True, spec_set=True)
        self.documents = create_autospec(SyncDocumentsAPI, instance=True, previews=documents_previews)
        self.entity_matching = create_autospec(SyncEntityMatchingAPI, instance=True, spec_set=True)
        self.events = create_autospec(SyncEventsAPI, instance=True, spec_set=True)
        flip_spec_set_on(self.documents)

        extpipes_config = create_autospec(SyncExtractionPipelineConfigsAPI, instance=True, spec_set=True)
        extpipes_runs = create_autospec(SyncExtractionPipelineRunsAPI, instance=True, spec_set=True)
        self.extraction_pipelines = create_autospec(
            SyncExtractionPipelinesAPI, instance=True, config=extpipes_config, runs=extpipes_runs
        )
        flip_spec_set_on(self.extraction_pipelines)

        self.files = create_autospec(SyncFilesAPI, instance=True, spec_set=True)

        fns_calls = create_autospec(SyncFunctionCallsAPI, instance=True, spec_set=True)
        fns_schedules = create_autospec(SyncFunctionSchedulesAPI, instance=True, spec_set=True)
        self.functions = create_autospec(SyncFunctionsAPI, instance=True, calls=fns_calls, schedules=fns_schedules)
        flip_spec_set_on(self.functions)

        self.geospatial = create_autospec(SyncGeospatialAPI, instance=True, spec_set=True)

        iam_groups = create_autospec(SyncGroupsAPI, instance=True, spec_set=True)
        iam_security_categories = create_autospec(SyncSecurityCategoriesAPI, instance=True, spec_set=True)
        iam_sessions = create_autospec(SyncSessionsAPI, instance=True, spec_set=True)
        iam_principals = create_autospec(SyncPrincipalsAPI, instance=True, spec_set=True)
        iam_user_profiles = create_autospec(SyncUserProfilesAPI, instance=True, spec_set=True)
        iam_token = create_autospec(SyncTokenAPI, instance=True, spec_set=True)
        self.iam = create_autospec(
            SyncIAMAPI,
            instance=True,
            groups=iam_groups,
            security_categories=iam_security_categories,
            sessions=iam_sessions,
            principals=iam_principals,
            user_profiles=iam_user_profiles,
            token=iam_token,
        )
        flip_spec_set_on(self.iam)

        self.labels = create_autospec(SyncLabelsAPI, instance=True, spec_set=True)

        raw_databases = create_autospec(SyncRawDatabasesAPI, instance=True, spec_set=True)
        raw_rows = create_autospec(SyncRawRowsAPI, instance=True, spec_set=True)
        raw_tables = create_autospec(SyncRawTablesAPI, instance=True, spec_set=True)
        self.raw = create_autospec(SyncRawAPI, instance=True, databases=raw_databases, rows=raw_rows, tables=raw_tables)
        flip_spec_set_on(self.raw)

        self.relationships = create_autospec(SyncRelationshipsAPI, instance=True, spec_set=True)

        sim_integrations = create_autospec(SyncSimulatorIntegrationsAPI, instance=True, spec_set=True)
        sim_models_revisions = create_autospec(SyncSimulatorModelRevisionsAPI, instance=True, spec_set=True)
        sim_models = create_autospec(SyncSimulatorModelsAPI, instance=True, revisions=sim_models_revisions)
        sim_runs = create_autospec(SyncSimulatorRunsAPI, instance=True, spec_set=True)
        sim_routines_revisions = create_autospec(SyncSimulatorRoutineRevisionsAPI, instance=True, spec_set=True)
        sim_routines = create_autospec(SyncSimulatorRoutinesAPI, instance=True, revisions=sim_routines_revisions)
        sim_logs = create_autospec(SyncSimulatorLogsAPI, instance=True, spec_set=True)
        self.simulators = create_autospec(
            SyncSimulatorsAPI,
            instance=True,
            integrations=sim_integrations,
            models=sim_models,
            runs=sim_runs,
            routines=sim_routines,
            logs=sim_logs,
        )
        flip_spec_set_on(self.simulators, sim_models)

        sequences_data = create_autospec(SyncSequencesDataAPI, instance=True, spec_set=True)
        self.sequences = create_autospec(SyncSequencesAPI, instance=True, data=sequences_data)
        flip_spec_set_on(self.sequences)

        ho_ex_sources = create_autospec(SyncSourcesAPI, instance=True, spec_set=True)
        ho_ex_destinations = create_autospec(SyncDestinationsAPI, instance=True, spec_set=True)
        ho_ex_jobs = create_autospec(SyncJobsAPI, instance=True, spec_set=True)
        ho_ex_mappings = create_autospec(SyncMappingsAPI, instance=True, spec_set=True)
        self.hosted_extractors = create_autospec(
            SyncHostedExtractorsAPI,
            instance=True,
            sources=ho_ex_sources,
            destinations=ho_ex_destinations,
            jobs=ho_ex_jobs,
            mappings=ho_ex_mappings,
        )
        flip_spec_set_on(self.hosted_extractors)

        pg_gw_users = create_autospec(SyncPostgresUsersAPI, instance=True, spec_set=True)
        pg_gw_tables = create_autospec(SyncPostgresTablesAPI, instance=True, spec_set=True)
        self.postgres_gateway = create_autospec(
            SyncPostgresGatewaysAPI, instance=True, users=pg_gw_users, tables=pg_gw_tables
        )
        flip_spec_set_on(self.postgres_gateway)

        three_d_asset_mappings = create_autospec(Sync3DAssetMappingAPI, instance=True, spec_set=True)
        three_d_files = create_autospec(Sync3DFilesAPI, instance=True, spec_set=True)
        three_d_models = create_autospec(Sync3DModelsAPI, instance=True, spec_set=True)
        three_d_revisions = create_autospec(Sync3DRevisionsAPI, instance=True, spec_set=True)
        self.three_d = create_autospec(
            Sync3DAPI,
            instance=True,
            asset_mappings=three_d_asset_mappings,
            files=three_d_files,
            models=three_d_models,
            revisions=three_d_revisions,
        )
        flip_spec_set_on(self.three_d)

        ts_synthetic = create_autospec(SyncSyntheticDatapointsAPI, instance=True, spec_set=True)
        ts_data = create_autospec(SyncDatapointsAPI, instance=True, synthetic=ts_synthetic)
        ts_subscriptions = create_autospec(SyncDatapointsSubscriptionAPI, instance=True, spec_set=True)
        self.time_series = create_autospec(
            SyncTimeSeriesAPI, instance=True, data=ts_data, subscriptions=ts_subscriptions
        )
        flip_spec_set_on(self.time_series, ts_data)

        tr_jobs = create_autospec(SyncTransformationJobsAPI, instance=True, spec_set=True)
        tr_notifications = create_autospec(SyncTransformationNotificationsAPI, instance=True, spec_set=True)
        tr_schedules = create_autospec(SyncTransformationSchedulesAPI, instance=True, spec_set=True)
        tr_schema = create_autospec(SyncTransformationSchemaAPI, instance=True, spec_set=True)
        self.transformations = create_autospec(
            SyncTransformationsAPI,
            instance=True,
            jobs=tr_jobs,
            notifications=tr_notifications,
            schedules=tr_schedules,
            schema=tr_schema,
        )
        flip_spec_set_on(self.transformations)

        self.vision = create_autospec(SyncVisionAPI, instance=True, spec_set=True)

        wf_versions = create_autospec(SyncWorkflowVersionAPI, instance=True, spec_set=True)
        wf_executions = create_autospec(SyncWorkflowExecutionAPI, instance=True, spec_set=True)
        wf_tasks = create_autospec(SyncWorkflowTaskAPI, instance=True, spec_set=True)
        wf_triggers = create_autospec(SyncWorkflowTriggerAPI, instance=True, spec_set=True)
        self.workflows = create_autospec(
            SyncWorkflowAPI,
            instance=True,
            versions=wf_versions,
            executions=wf_executions,
            tasks=wf_tasks,
            triggers=wf_triggers,
        )
        flip_spec_set_on(self.workflows)

        units_systems = create_autospec(SyncUnitSystemAPI, instance=True, spec_set=True)
        self.units = create_autospec(SyncUnitAPI, instance=True, systems=units_systems)
        flip_spec_set_on(self.units)


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
