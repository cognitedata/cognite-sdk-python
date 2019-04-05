import os
from unittest import mock

import pytest
import responses

from cognite.client import CogniteClient
from tests.utils import BASE_URL


@pytest.fixture
def client():
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
        client_mock.return_value = mock.MagicMock()
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
