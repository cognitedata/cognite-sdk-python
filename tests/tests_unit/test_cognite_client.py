import logging
import os
import random
import sys
import threading
import types
from multiprocessing.pool import ThreadPool
from time import sleep

import pytest

from cognite.client import CogniteClient
from cognite.client._api.assets import AssetList
from cognite.client._api.files import FileMetadataList
from cognite.client._api.time_series import TimeSeriesList
from cognite.client._utils import DebugLogFormatter
from cognite.client.data_classes import Asset, Event, FileMetadata, TimeSeries


@pytest.fixture
def default_client_config():
    from cognite.client._cognite_client import DEFAULT_MAX_WORKERS, DEFAULT_TIMEOUT

    yield "https://greenfield.cognitedata.com", DEFAULT_MAX_WORKERS, DEFAULT_TIMEOUT


@pytest.fixture
def environment_client_config():
    base_url = "blabla"
    num_of_workers = 1
    timeout = 10

    tmp_base_url = os.environ["COGNITE_BASE_URL"]
    os.environ["COGNITE_BASE_URL"] = base_url
    os.environ["COGNITE_MAX_WORKERS"] = str(num_of_workers)
    os.environ["COGNITE_TIMEOUT"] = str(timeout)

    yield base_url, num_of_workers, timeout

    os.environ["COGNITE_BASE_URL"] = tmp_base_url
    del os.environ["COGNITE_MAX_WORKERS"]
    del os.environ["COGNITE_TIMEOUT"]


class TestCogniteClient:
    def test_project_is_correct(self, rsps_with_login_mock):
        c = CogniteClient()
        assert c.project == "test"

    @pytest.fixture
    def unset_env_api_key(self):
        tmp = os.environ["COGNITE_API_KEY"]
        del os.environ["COGNITE_API_KEY"]
        yield
        os.environ["COGNITE_API_KEY"] = tmp

    def test_no_api_key_set(self, unset_env_api_key):
        with pytest.raises(ValueError, match="No API Key has been specified"):
            CogniteClient()

    def assert_config_is_correct(self, client, base_url, max_workers, timeout):
        assert client._base_url == base_url
        assert type(client._base_url) is str

        assert client._max_workers == max_workers
        assert type(client._max_workers) is int

        assert client._timeout == timeout
        assert type(client._timeout) is int

    def test_default_config(self, client, default_client_config):
        self.assert_config_is_correct(client, *default_client_config)

    def test_parameter_config(self):
        base_url = "blabla"
        max_workers = 1
        timeout = 10

        client = CogniteClient(project="something", base_url=base_url, max_workers=max_workers, timeout=timeout)
        self.assert_config_is_correct(client, base_url, max_workers, timeout)

    def test_environment_config(self, environment_client_config):
        client = CogniteClient(project="something")
        self.assert_config_is_correct(client, *environment_client_config)

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

        assert api_key == client._CogniteClient__api_key
        assert project == client.project

    def test_create_client_thread_local_config(self, thread_local_credentials_module):
        with ThreadPool() as pool:
            pool.map(self.create_client_and_check_config, list(range(16)))

    def test_client_debug_mode(self, rsps_with_login_mock):
        CogniteClient(debug=True)
        log = logging.getLogger("cognite-sdk")
        assert isinstance(log.handlers[0].formatter, DebugLogFormatter)
        log.handlers = []
        log.propagate = False


class TestInstantiateWithClient:
    @pytest.mark.parametrize("cls", [Asset, Event, FileMetadata, TimeSeries])
    def test_instantiate_resources_with_cognite_client(self, cls):
        c = CogniteClient()
        assert cls(cognite_client=c)._cognite_client == c

    @pytest.mark.parametrize("cls", [AssetList, Event, FileMetadataList, TimeSeriesList])
    def test_intantiate_resource_lists_with_cognite_client(self, cls):
        c = CogniteClient()
        assert cls([], cognite_client=c)._cognite_client == c
