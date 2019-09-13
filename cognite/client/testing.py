from contextlib import contextmanager
from unittest import mock

from cognite.client import CogniteClient
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.iam import IAMAPI, APIKeysAPI, GroupsAPI, SecurityCategoriesAPI, ServiceAccountsAPI
from cognite.client._api.login import LoginAPI
from cognite.client._api.raw import RawAPI, RawDatabasesAPI, RawRowsAPI, RawTablesAPI
from cognite.client._api.three_d import (
    ThreeDAPI,
    ThreeDAssetMappingAPI,
    ThreeDFilesAPI,
    ThreeDModelsAPI,
    ThreeDRevisionsAPI,
)
from cognite.client._api.time_series import TimeSeriesAPI


@contextmanager
def mock_cognite_client():
    """Context manager for mocking the CogniteClient.

    Will patch all APIs and replace the client with specced MagicMock objects.

    Examples:

        In this example we can run the following code without actually executing the underlying API calls::

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import TimeSeries
            >>> from cognite.client.testing import mock_cognite_client
            >>>
            >>> with mock_cognite_client():
            >>>     c = CogniteClient()
            >>>     c.time_series.create(TimeSeries(external_id="blabla"))

        This example shows how to set the return value of a given method::

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import TimeSeries
            >>> from cognite.client.data_classes import LoginStatus
            >>> from cognite.client.testing import mock_cognite_client
            >>>
            >>> with mock_cognite_client() as c_mock:
            >>>     c_mock.login.status.return_value = LoginStatus(
            >>>         user="user", project="dummy", project_id=1, logged_in=True, api_key_id=1
            >>>     )
            >>>     c = CogniteClient()
            >>>     res = c.login.status()
            >>>     assert "user" == res.user

        Here you can see how to have a given method raise an exception::

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.exceptions import CogniteAPIError
            >>> from cognite.client.testing import mock_cognite_client
            >>>
            >>> with mock_cognite_client() as c_mock:
            >>>     c_mock.login.status.side_effect = CogniteAPIError(message="Something went wrong", code=400)
            >>>     c = CogniteClient()
            >>>     try:
            >>>         res = c.login.status()
            >>>     except CogniteAPIError as e:
            >>>         assert 400 == e.code
            >>>         assert "Something went wrong" == e.message
    """
    cog_client_mock = mock.MagicMock(spec=CogniteClient)
    cog_client_mock.time_series = mock.MagicMock(spec_set=TimeSeriesAPI)
    cog_client_mock.datapoints = mock.MagicMock(spec_set=DatapointsAPI)
    cog_client_mock.assets = mock.MagicMock(spec_set=AssetsAPI)
    cog_client_mock.events = mock.MagicMock(spec_set=EventsAPI)
    cog_client_mock.files = mock.MagicMock(spec_set=FilesAPI)
    cog_client_mock.login = mock.MagicMock(spec_set=LoginAPI)
    cog_client_mock.three_d = mock.MagicMock(spec=ThreeDAPI)
    cog_client_mock.three_d.models = mock.MagicMock(spec_set=ThreeDModelsAPI)
    cog_client_mock.three_d.revisions = mock.MagicMock(spec_set=ThreeDRevisionsAPI)
    cog_client_mock.three_d.files = mock.MagicMock(spec_set=ThreeDFilesAPI)
    cog_client_mock.three_d.asset_mappings = mock.MagicMock(spec_set=ThreeDAssetMappingAPI)
    cog_client_mock.iam = mock.MagicMock(spec=IAMAPI)
    cog_client_mock.iam.service_accounts = mock.MagicMock(spec=ServiceAccountsAPI)
    cog_client_mock.iam.api_keys = mock.MagicMock(spec_set=APIKeysAPI)
    cog_client_mock.iam.groups = mock.MagicMock(spec_set=GroupsAPI)
    cog_client_mock.iam.security_categories = mock.MagicMock(spec_set=SecurityCategoriesAPI)
    cog_client_mock.raw = mock.MagicMock(spec=RawAPI)
    cog_client_mock.raw.databases = mock.MagicMock(spec_set=RawDatabasesAPI)
    cog_client_mock.raw.tables = mock.MagicMock(spec_set=RawTablesAPI)
    cog_client_mock.raw.rows = mock.MagicMock(spec_set=RawRowsAPI)

    CogniteClient.__new__ = lambda *args, **kwargs: cog_client_mock
    yield cog_client_mock
    CogniteClient.__new__ = lambda cls, *args, **kwargs: super(CogniteClient, cls).__new__(cls)
