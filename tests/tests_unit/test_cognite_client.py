import logging
import os
import random
import sys
import threading
import types
from multiprocessing.pool import ThreadPool
from time import sleep
from unittest.mock import Mock, patch

import pytest

from cognite.client import CogniteClient, utils
from cognite.client._api.assets import AssetList
from cognite.client._api.files import FileMetadataList
from cognite.client._api.time_series import TimeSeriesList
from cognite.client.data_classes import Asset, Event, FileMetadata, TimeSeries
from cognite.client.exceptions import CogniteAPIKeyError
from cognite.client.utils._logging import DebugLogFormatter
from tests.utils import BASE_URL, set_env_var, unset_env_var

_PYPI_ADDRESS = "https://pypi.python.org/simple/cognite-sdk/#history"


@pytest.fixture
def default_client_config():
    default_config = utils._client_config._DefaultConfig()

    yield (
        "https://greenfield.cognitedata.com",
        default_config.max_workers,
        default_config.timeout,
        "python-sdk-integration-tests",
        "test",
    )


@pytest.fixture
def environment_client_config():
    base_url = "blabla"
    num_of_workers = 1
    timeout = 10
    client_name = "test-client"
    project = "test-project"

    tmp_base_url = os.environ["COGNITE_BASE_URL"]
    tmp_client_name = os.environ["COGNITE_CLIENT_NAME"]
    tmp_project_name = os.getenv("COGNITE_PROJECT")

    os.environ["COGNITE_BASE_URL"] = base_url
    os.environ["COGNITE_MAX_WORKERS"] = str(num_of_workers)
    os.environ["COGNITE_TIMEOUT"] = str(timeout)
    os.environ["COGNITE_CLIENT_NAME"] = client_name
    os.environ["COGNITE_PROJECT"] = project

    yield base_url, num_of_workers, timeout, client_name, project

    os.environ["COGNITE_BASE_URL"] = tmp_base_url
    del os.environ["COGNITE_MAX_WORKERS"]
    del os.environ["COGNITE_TIMEOUT"]
    os.environ["COGNITE_CLIENT_NAME"] = tmp_client_name

    if tmp_project_name is not None:
        os.environ["COGNITE_PROJECT"] = tmp_project_name
    else:
        del os.environ["COGNITE_PROJECT"]


class TestCogniteClient:
    def test_project_is_correct(self, rsps_with_login_mock):
        with unset_env_var("COGNITE_PROJECT"):
            c = CogniteClient()
        assert c.config.project == "test"

    def test_no_api_key_set(self):
        with unset_env_var("COGNITE_API_KEY"):
            with pytest.raises(CogniteAPIKeyError, match="No API key has been specified"):
                CogniteClient()

    def test_invalid_api_key(self, rsps):
        rsps.add(rsps.GET, _PYPI_ADDRESS, status=200, body="")
        rsps.add(
            rsps.GET,
            BASE_URL + "/login/status",
            status=200,
            json={"data": {"project": "", "loggedIn": False, "user": "", "projectId": -1}},
        )
        with unset_env_var("COGNITE_PROJECT"):
            with pytest.raises(CogniteAPIKeyError):
                CogniteClient()

    def test_no_client_name(self):
        with unset_env_var("COGNITE_CLIENT_NAME"):
            with pytest.raises(ValueError, match="No client name has been specified"):
                CogniteClient()

    def assert_config_is_correct(self, config, base_url, max_workers, timeout, client_name, project):
        assert config.base_url == base_url
        assert type(config.base_url) is str

        assert config.max_workers == max_workers
        assert type(config.max_workers) is int

        assert config.timeout == timeout
        assert type(config.timeout) is int

        assert config.client_name == client_name
        assert type(config.client_name) is str

        assert config.project == project
        assert type(config.project) is str

    def test_default_config(self, rsps_with_login_mock, default_client_config):
        with unset_env_var("COGNITE_PROJECT"):
            client = CogniteClient()
        self.assert_config_is_correct(client.config, *default_client_config)

    def test_parameter_config(self):
        base_url = "blabla"
        max_workers = 1
        timeout = 10
        client_name = "test-client"
        project = "something"

        client = CogniteClient(
            project=project, base_url=base_url, max_workers=max_workers, timeout=timeout, client_name=client_name
        )
        self.assert_config_is_correct(client._config, base_url, max_workers, timeout, client_name, project)

    def test_environment_config(self, environment_client_config):
        client = CogniteClient()
        self.assert_config_is_correct(client._config, *environment_client_config)

    @pytest.fixture
    def thread_local_credentials_module(self):
        credentials_module = types.ModuleType("cognite._thread_local")
        credentials_module.credentials = threading.local()
        sys.modules["cognite._thread_local"] = credentials_module
        yield
        del sys.modules["cognite._thread_local"]

    def create_client_and_check_config(self, i):
        from cognite._thread_local import credentials

        api_key = "thread-local-api-key{}".format(i)
        project = "thread-local-project{}".format(i)

        credentials.api_key = api_key
        credentials.project = project

        sleep(random.random())
        client = CogniteClient()

        assert api_key == client.config.api_key
        assert project == client.config.project

    def test_create_client_thread_local_config(self, thread_local_credentials_module):
        with ThreadPool() as pool:
            pool.map(self.create_client_and_check_config, list(range(16)))

    def test_client_debug_mode(self, rsps_with_login_mock):
        with unset_env_var("COGNITE_PROJECT"):
            CogniteClient(debug=True)
        log = logging.getLogger("cognite-sdk")
        assert isinstance(log.handlers[0].formatter, DebugLogFormatter)
        log.handlers = []
        log.propagate = False

    def test_version_check_disabled(self, rsps_with_login_mock):
        rsps_with_login_mock.assert_all_requests_are_fired = False
        with unset_env_var("COGNITE_PROJECT"):
            with set_env_var("COGNITE_DISABLE_PYPI_VERSION_CHECK", "1"):
                CogniteClient()
        assert len(rsps_with_login_mock.calls) == 1
        assert rsps_with_login_mock.calls[0].request.url.startswith("https://greenfield.cognitedata.com")

    def test_version_check_enabled(self, rsps_with_login_mock):
        with unset_env_var("COGNITE_PROJECT"):
            CogniteClient()
        assert len(rsps_with_login_mock.calls) == 2

    @patch("cognite.client.utils._version_checker.re.findall")
    @patch("cognite.client.utils._version_checker.requests")
    def test_verify_ssl_enabled_by_default(self, mock_requests, mock_findall):

        c = CogniteClient()

        mock_requests.get.assert_called_with(_PYPI_ADDRESS, verify=True)
        assert c._api_client._request_session.verify == True
        assert c._api_client._request_session_with_retry.verify == True

    @patch("cognite.client.utils._version_checker.re.findall")
    @patch("cognite.client.utils._version_checker.requests")
    def test_verify_ssl_disabled(self, mock_requests, mock_findall):
        with set_env_var("COGNITE_DISABLE_SSL", "1"):
            c = CogniteClient()
            mock_requests.get.assert_called_with(_PYPI_ADDRESS, verify=False)


class TestInstantiateWithClient:
    @pytest.mark.parametrize("cls", [Asset, Event, FileMetadata, TimeSeries])
    def test_instantiate_resources_with_cognite_client(self, cls):
        c = CogniteClient()
        assert cls(cognite_client=c)._cognite_client == c

    @pytest.mark.parametrize("cls", [AssetList, Event, FileMetadataList, TimeSeriesList])
    def test_intantiate_resource_lists_with_cognite_client(self, cls):
        c = CogniteClient()
        assert cls([], cognite_client=c)._cognite_client == c
