import os
from unittest import mock

import pytest
import responses

from cognite.client import CogniteClient
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.data_sets import DataSetsAPI
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.entity_matching import EntityMatchingAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.iam import IAMAPI, APIKeysAPI, GroupsAPI, SecurityCategoriesAPI, ServiceAccountsAPI
from cognite.client._api.login import LoginAPI
from cognite.client._api.raw import RawAPI, RawDatabasesAPI, RawRowsAPI, RawTablesAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._api.sequences import SequencesAPI, SequencesDataAPI
from cognite.client._api.three_d import (
    ThreeDAPI,
    ThreeDAssetMappingAPI,
    ThreeDFilesAPI,
    ThreeDModelsAPI,
    ThreeDRevisionsAPI,
)
from cognite.client._api.time_series import TimeSeriesAPI
from tests.utils import BASE_URL


@pytest.fixture
def rsps_with_login_mock():
    with responses.RequestsMock() as rsps:
        rsps.add(rsps.GET, "https://pypi.python.org/simple/cognite-sdk/#history", status=200, body="")
        rsps.add(
            rsps.GET,
            BASE_URL + "/login/status",
            status=200,
            json={"data": {"project": "test", "loggedIn": True, "user": "bla", "projectId": "bla", "apiKeyId": 1}},
        )
        yield rsps


@pytest.fixture
def mock_cognite_client():
    with mock.patch("cognite.client.CogniteClient") as client_mock:
        cog_client_mock = mock.MagicMock(spec=CogniteClient)
        cog_client_mock.time_series = mock.MagicMock(spec=TimeSeriesAPI)
        cog_client_mock.datapoints = mock.MagicMock(spec=DatapointsAPI)
        cog_client_mock.assets = mock.MagicMock(spec=AssetsAPI)
        cog_client_mock.events = mock.MagicMock(spec=EventsAPI)
        cog_client_mock.data_sets = mock.MagicMock(spec=DataSetsAPI)
        cog_client_mock.files = mock.MagicMock(spec=FilesAPI)
        cog_client_mock.login = mock.MagicMock(spec=LoginAPI)
        cog_client_mock.three_d = mock.MagicMock(spec=ThreeDAPI)
        cog_client_mock.three_d.models = mock.MagicMock(spec=ThreeDModelsAPI)
        cog_client_mock.three_d.revisions = mock.MagicMock(spec=ThreeDRevisionsAPI)
        cog_client_mock.three_d.files = mock.MagicMock(spec=ThreeDFilesAPI)
        cog_client_mock.three_d.asset_mappings = mock.MagicMock(spec=ThreeDAssetMappingAPI)
        cog_client_mock.iam = mock.MagicMock(spec=IAMAPI)
        cog_client_mock.iam.service_accounts = mock.MagicMock(spec=ServiceAccountsAPI)
        cog_client_mock.iam.api_keys = mock.MagicMock(spec=APIKeysAPI)
        cog_client_mock.iam.groups = mock.MagicMock(spec=GroupsAPI)
        cog_client_mock.iam.security_categories = mock.MagicMock(spec=SecurityCategoriesAPI)
        cog_client_mock.sequences = mock.MagicMock(spec=SequencesAPI)
        cog_client_mock.sequences.data = mock.MagicMock(spec=SequencesDataAPI)
        cog_client_mock.relationships = mock.MagicMock(spec=RelationshipsAPI)
        raw_mock = mock.MagicMock(spec=RawAPI)
        raw_mock.databases = mock.MagicMock(spec=RawDatabasesAPI)
        raw_mock.tables = mock.MagicMock(spec=RawTablesAPI)
        raw_mock.rows = mock.MagicMock(spec=RawRowsAPI)
        cog_client_mock.raw = raw_mock
        client_mock.return_value = cog_client_mock
        yield


@pytest.fixture
def mock_cognite_beta_client(mock_cognite_client):
    with mock.patch("cognite.client.beta.CogniteClient") as client_mock:
        cog_client_mock = mock.MagicMock(spec=CogniteClient)
        cog_client_mock.entity_matching = mock.MagicMock(spec=EntityMatchingAPI)
        client_mock.return_value = cog_client_mock
        yield


@pytest.fixture
def rsps():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def disable_gzip():
    os.environ["COGNITE_DISABLE_GZIP"] = "1"
    yield
    del os.environ["COGNITE_DISABLE_GZIP"]


def pytest_addoption(parser):
    parser.addoption(
        "--test-deps-only-core", action="store_true", default=False, help="Test only core deps are installed"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--test-deps-only-core"):
        return
    skip_core = pytest.mark.skip(reason="need --test-deps-only-core option to run")
    for item in items:
        if "coredeps" in item.keywords:
            item.add_marker(skip_core)
