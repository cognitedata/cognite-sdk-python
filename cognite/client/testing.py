from contextlib import contextmanager
from typing import Any, Iterator
from unittest.mock import MagicMock

from cognite.client import CogniteClient
from cognite.client._api.annotations import AnnotationsAPI
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.data_sets import DataSetsAPI
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.diagrams import DiagramsAPI
from cognite.client._api.entity_matching import EntityMatchingAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.extractionpipelines import ExtractionPipelineRunsAPI, ExtractionPipelinesAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.functions import FunctionCallsAPI, FunctionsAPI, FunctionSchedulesAPI
from cognite.client._api.geospatial import GeospatialAPI
from cognite.client._api.iam import IAMAPI, APIKeysAPI, GroupsAPI, SecurityCategoriesAPI, ServiceAccountsAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.login import LoginAPI
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


class CogniteClientMock(MagicMock):
    """Mock for CogniteClient object

    All APIs are replaced with specced MagicMock objects.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "parent" in kwargs:
            super().__init__(*args, **kwargs)
            return None
        super().__init__(spec=CogniteClient, *args, **kwargs)

        self.datapoints = MagicMock(spec=DatapointsAPI)
        self.datapoints.synthetic = MagicMock(spec_set=SyntheticDatapointsAPI)

        self.time_series = MagicMock(spec=TimeSeriesAPI)
        self.time_series.data = self.datapoints

        self.assets = MagicMock(spec_set=AssetsAPI)
        self.events = MagicMock(spec_set=EventsAPI)
        self.data_sets = MagicMock(spec_set=DataSetsAPI)
        self.files = MagicMock(spec_set=FilesAPI)
        self.labels = MagicMock(spec_set=LabelsAPI)
        self.login = MagicMock(spec_set=LoginAPI)

        self.three_d = MagicMock(spec=ThreeDAPI)
        self.three_d.models = MagicMock(spec_set=ThreeDModelsAPI)
        self.three_d.revisions = MagicMock(spec_set=ThreeDRevisionsAPI)
        self.three_d.files = MagicMock(spec_set=ThreeDFilesAPI)
        self.three_d.asset_mappings = MagicMock(spec_set=ThreeDAssetMappingAPI)

        self.iam = MagicMock(spec=IAMAPI)
        self.iam.service_accounts = MagicMock(spec_set=ServiceAccountsAPI)
        self.iam.api_keys = MagicMock(spec_set=APIKeysAPI)
        self.iam.groups = MagicMock(spec_set=GroupsAPI)
        self.iam.security_categories = MagicMock(spec_set=SecurityCategoriesAPI)

        self.raw = MagicMock(spec=RawAPI)
        self.raw.databases = MagicMock(spec_set=RawDatabasesAPI)
        self.raw.tables = MagicMock(spec_set=RawTablesAPI)
        self.raw.rows = MagicMock(spec_set=RawRowsAPI)

        self.relationships = MagicMock(spec_set=RelationshipsAPI)

        self.sequences = MagicMock(spec=SequencesAPI)
        self.sequences.data = MagicMock(spec_set=SequencesDataAPI)

        self.entity_matching = MagicMock(spec_set=EntityMatchingAPI)
        self.extraction_pipelines = MagicMock(spec_set=ExtractionPipelinesAPI)
        self.extraction_pipeline_runs = MagicMock(spec_set=ExtractionPipelineRunsAPI)
        self.geospatial = MagicMock(spec_set=GeospatialAPI)

        self.templates = MagicMock(spec=TemplatesAPI)
        self.templates.groups = MagicMock(spec_set=TemplateGroupsAPI)
        self.templates.versions = MagicMock(spec_set=TemplateGroupVersionsAPI)
        self.templates.instances = MagicMock(spec_set=TemplateInstancesAPI)
        self.templates.views = MagicMock(spec_set=TemplateViewsAPI)

        self.transformations = MagicMock(spec=TransformationsAPI)
        self.transformations.jobs = MagicMock(spec_set=TransformationJobsAPI)
        self.transformations.schedules = MagicMock(spec_set=TransformationSchedulesAPI)
        self.transformations.schema = MagicMock(spec_set=TransformationSchemaAPI)
        self.transformations.notifications = MagicMock(spec_set=TransformationNotificationsAPI)

        self.diagrams = MagicMock(spec_set=DiagramsAPI)
        self.annotations = MagicMock(spec_set=AnnotationsAPI)

        self.functions = MagicMock(spec=FunctionsAPI)
        self.functions.calls = MagicMock(spec_set=FunctionCallsAPI)
        self.functions.schedules = MagicMock(spec_set=FunctionSchedulesAPI)

        self.vision = MagicMock(spec_set=VisionAPI)


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
            >>>     c = CogniteClient()
            >>>     c.time_series.create(TimeSeries(external_id="blabla"))

        This example shows how to set the return value of a given method::

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import TimeSeries
            >>> from cognite.client.data_classes import LoginStatus
            >>> from cognite.client.testing import monkeypatch_cognite_client
            >>>
            >>> with monkeypatch_cognite_client() as c_mock:
            >>>     c_mock.login.status.return_value = LoginStatus(
            >>>         user="user", project="dummy", project_id=1, logged_in=True, api_key_id=1
            >>>     )
            >>>     c = CogniteClient()
            >>>     res = c.login.status()
            >>>     assert "user" == res.user

        Here you can see how to have a given method raise an exception::

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.exceptions import CogniteAPIError
            >>> from cognite.client.testing import monkeypatch_cognite_client
            >>>
            >>> with monkeypatch_cognite_client() as c_mock:
            >>>     c_mock.login.status.side_effect = CogniteAPIError(message="Something went wrong", code=400)
            >>>     c = CogniteClient()
            >>>     try:
            >>>         res = c.login.status()
            >>>     except CogniteAPIError as e:
            >>>         assert 400 == e.code
            >>>         assert "Something went wrong" == e.message
    """
    cognite_client_mock = CogniteClientMock()
    CogniteClient.__new__ = lambda *args, **kwargs: cognite_client_mock  # type: ignore[assignment]
    yield cognite_client_mock
    CogniteClient.__new__ = lambda cls, *args, **kwargs: super(CogniteClient, cls).__new__(cls)  # type: ignore[assignment]
