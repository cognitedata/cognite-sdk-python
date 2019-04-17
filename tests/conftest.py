import os
from unittest import mock

import pytest
import responses

from cognite.client import CogniteClient
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.login import LoginAPI
from cognite.client._api.time_series import TimeSeriesAPI
from tests.utils import BASE_URL


@pytest.fixture
def client(rsps_with_login_mock):
    yield CogniteClient()


@pytest.fixture
def rsps_with_login_mock():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            BASE_URL + "/login/status",
            status=200,
            json={"data": {"project": "test", "loggedIn": True, "user": "bla", "projectId": "bla"}},
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
        cog_client_mock.files = mock.MagicMock(spec=FilesAPI)
        cog_client_mock.login = mock.MagicMock(spec=LoginAPI)
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
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need ----test-deps-only-core option to run")
    for item in items:
        if "coredeps" in item.keywords:
            item.add_marker(skip_slow)
