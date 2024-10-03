from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

from cognite.client import CogniteClient
from cognite.client._api.annotations import AnnotationsAPI
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.data_modeling import DataModelingAPI
from cognite.client._api.data_modeling.containers import ContainersAPI
from cognite.client._api.data_modeling.data_models import DataModelsAPI
from cognite.client._api.data_modeling.graphql import DataModelingGraphQLAPI
from cognite.client._api.data_modeling.instances import InstancesAPI
from cognite.client._api.data_modeling.spaces import SpacesAPI
from cognite.client._api.data_modeling.views import ViewsAPI
from cognite.client._api.data_sets import DataSetsAPI
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.datapoints_subscriptions import DatapointsSubscriptionAPI
from cognite.client._api.diagrams import DiagramsAPI
from cognite.client._api.documents import DocumentPreviewAPI, DocumentsAPI
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
from cognite.client._api.hosted_extractors import HostedExtractorsAPI
from cognite.client._api.hosted_extractors.destinations import DestinationsAPI
from cognite.client._api.hosted_extractors.jobs import JobsAPI
from cognite.client._api.hosted_extractors.mappings import MappingsAPI
from cognite.client._api.hosted_extractors.sources import SourcesAPI
from cognite.client._api.iam import IAMAPI, GroupsAPI, SecurityCategoriesAPI, SessionsAPI, TokenAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.postgres_gateway import PostgresGatewaysAPI
from cognite.client._api.postgres_gateway.users import UsersAPI as PostgresUsersAPI
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
from cognite.client._api.units import UnitAPI, UnitSystemAPI
from cognite.client._api.user_profiles import UserProfilesAPI
from cognite.client._api.vision import VisionAPI
from cognite.client._api.workflows import (
    WorkflowAPI,
    WorkflowExecutionAPI,
    WorkflowTaskAPI,
    WorkflowTriggerAPI,
    WorkflowVersionAPI,
)


class CogniteClientMock(MagicMock):
    """Mock for CogniteClient object

    All APIs are replaced with specced MagicMock objects.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "parent" in kwargs:
            super().__init__(*args, **kwargs)
            return None
        super().__init__(spec=CogniteClient, *args, **kwargs)
        # Developer note:
        # - Please add your mocked APIs in chronological order
        # - For nested APIs:
        #   - Add spacing above and below
        #   - Use `spec=MyAPI` only for "top level"
        #   - Use `spec_set=MyNestedAPI` for all nested APIs
        self.annotations = MagicMock(spec_set=AnnotationsAPI)
        self.assets = MagicMock(spec_set=AssetsAPI)

        self.data_modeling = MagicMock(spec=DataModelingAPI)
        self.data_modeling.containers = MagicMock(spec_set=ContainersAPI)
        self.data_modeling.data_models = MagicMock(spec_set=DataModelsAPI)
        self.data_modeling.spaces = MagicMock(spec_set=SpacesAPI)
        self.data_modeling.views = MagicMock(spec_set=ViewsAPI)
        self.data_modeling.instances = MagicMock(spec_set=InstancesAPI)
        self.data_modeling.graphql = MagicMock(spec_set=DataModelingGraphQLAPI)

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
        self.iam.user_profiles = MagicMock(spec_set=UserProfilesAPI)
        self.iam.token = MagicMock(spec_set=TokenAPI)

        self.labels = MagicMock(spec_set=LabelsAPI)

        self.raw = MagicMock(spec=RawAPI)
        self.raw.databases = MagicMock(spec_set=RawDatabasesAPI)
        self.raw.rows = MagicMock(spec_set=RawRowsAPI)
        self.raw.tables = MagicMock(spec_set=RawTablesAPI)

        self.relationships = MagicMock(spec_set=RelationshipsAPI)

        self.sequences = MagicMock(spec=SequencesAPI)
        self.sequences.data = MagicMock(spec_set=SequencesDataAPI)

        self.hosted_extractors = MagicMock(spec=HostedExtractorsAPI)
        self.hosted_extractors.sources = MagicMock(spec_set=SourcesAPI)
        self.hosted_extractors.destinations = MagicMock(spec_set=DestinationsAPI)
        self.hosted_extractors.jobs = MagicMock(spec_set=JobsAPI)
        self.hosted_extractors.mappings = MagicMock(spec_set=MappingsAPI)

        self.postgres_gateway = MagicMock(spec=PostgresGatewaysAPI)
        self.postgres_gateway.users = MagicMock(spec_set=PostgresUsersAPI)

        self.templates = MagicMock(spec=TemplatesAPI)
        self.templates.groups = MagicMock(spec_set=TemplateGroupsAPI)
        self.templates.instances = MagicMock(spec_set=TemplateInstancesAPI)
        self.templates.versions = MagicMock(spec_set=TemplateGroupVersionsAPI)
        self.templates.views = MagicMock(spec_set=TemplateViewsAPI)

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


@contextmanager
def monkeypatch_cognite_client() -> Iterator[CogniteClientMock]:
    """Context manager for monkeypatching the CogniteClient.

    Will patch all clients and replace them with specced MagicMock objects.

    Yields:
        CogniteClientMock: The mock with which the CogniteClient has been replaced

    Examples:

        In this example we can run the following code without actually executing the underlying API calls::

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import TimeSeries
            >>> from cognite.client.testing import monkeypatch_cognite_client
            >>>
            >>> with monkeypatch_cognite_client():
            >>>     client = CogniteClient()
            >>>     client.time_series.create(TimeSeries(external_id="blabla"))

        This example shows how to set the return value of a given method::

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.iam import TokenInspection
            >>> from cognite.client.testing import monkeypatch_cognite_client
            >>>
            >>> with monkeypatch_cognite_client() as c_mock:
            >>>     c_mock.iam.token.inspect.return_value = TokenInspection(
            >>>         subject="subject", projects=[], capabilities=[]
            >>>     )
            >>>     client = CogniteClient()
            >>>     res = client.iam.token.inspect()
            >>>     assert "subject" == res.subject

        Here you can see how to have a given method raise an exception::

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.exceptions import CogniteAPIError
            >>> from cognite.client.testing import monkeypatch_cognite_client
            >>>
            >>> with monkeypatch_cognite_client() as c_mock:
            >>>     c_mock.iam.token.inspect.side_effect = CogniteAPIError(message="Something went wrong", code=400)
            >>>     client = CogniteClient()
            >>>     try:
            >>>         res = client.iam.token.inspect()
            >>>     except CogniteAPIError as e:
            >>>         assert 400 == e.code
            >>>         assert "Something went wrong" == e.message
    """
    cognite_client_mock = CogniteClientMock()
    CogniteClient.__new__ = lambda *args, **kwargs: cognite_client_mock  # type: ignore[method-assign]
    yield cognite_client_mock
    CogniteClient.__new__ = lambda cls, *args, **kwargs: object.__new__(cls)  # type: ignore[method-assign]
