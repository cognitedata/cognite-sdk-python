import datetime
import logging
import os
import random
import sys
import threading
import types
from multiprocessing.pool import ThreadPool
from time import sleep
from unittest.mock import patch

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
TOKEN_URL = "https://test.com/token"


@pytest.fixture
def default_client_config():
    default_config = utils._client_config._DefaultConfig()

    yield (
        "https://greenfield.cognitedata.com",
        default_config.max_workers,
        default_config.timeout,
        "python-sdk-integration-tests",
        "test",
        default_config.token_client_id,
        default_config.token_client_secret,
        default_config.token_url,
        default_config.token_scopes,
    )


@pytest.fixture
def environment_client_config():
    base_url = "blabla"
    num_of_workers = 1
    timeout = 10
    client_name = "test-client"
    project = "test-project"
    client_id = "test-client-id"
    client_secret = "test-client-secret"
    token_url = TOKEN_URL
    token_scopes = "https://test.com/.default,https://test.com/.admin"

    tmp_base_url = os.environ["COGNITE_BASE_URL"]
    tmp_client_name = os.environ["COGNITE_CLIENT_NAME"]
    tmp_project_name = os.getenv("COGNITE_PROJECT")

    os.environ["COGNITE_BASE_URL"] = base_url
    os.environ["COGNITE_MAX_WORKERS"] = str(num_of_workers)
    os.environ["COGNITE_TIMEOUT"] = str(timeout)
    os.environ["COGNITE_CLIENT_NAME"] = client_name
    os.environ["COGNITE_PROJECT"] = project
    os.environ["COGNITE_CLIENT_ID"] = client_id
    os.environ["COGNITE_CLIENT_SECRET"] = client_secret
    os.environ["COGNITE_TOKEN_URL"] = token_url
    os.environ["COGNITE_TOKEN_SCOPES"] = token_scopes

    token_scope_list = token_scopes.split(",")
    yield base_url, num_of_workers, timeout, client_name, project, client_id, client_secret, token_url, token_scope_list

    os.environ["COGNITE_BASE_URL"] = tmp_base_url
    del os.environ["COGNITE_MAX_WORKERS"]
    del os.environ["COGNITE_TIMEOUT"]
    del os.environ["COGNITE_CLIENT_ID"]
    del os.environ["COGNITE_CLIENT_SECRET"]
    del os.environ["COGNITE_TOKEN_URL"]
    del os.environ["COGNITE_TOKEN_SCOPES"]
    os.environ["COGNITE_CLIENT_NAME"] = tmp_client_name

    if tmp_project_name is not None:
        os.environ["COGNITE_PROJECT"] = tmp_project_name
    else:
        del os.environ["COGNITE_PROJECT"]


class TestCogniteClient:
    def test_project_is_correct(self, rsps_with_login_mock):
        with unset_env_var("COGNITE_PROJECT"), set_env_var("COGNITE_API_KEY", "bla"):
            c = CogniteClient()
        assert c.config.project == "test"

    def test_no_api_key_no_token_set(self):
        with unset_env_var("COGNITE_TOKEN_URL", "COGNITE_API_KEY"):
            with pytest.raises(
                CogniteAPIKeyError, match="No API key or token or token generation arguments have been specified"
            ):
                CogniteClient()

    def test_token_factory_set_no_api_key(self):
        c = CogniteClient(token=lambda: "abc")
        assert c.config.token() == "abc"

    def test_token_set_no_api_key(self):
        c = CogniteClient(token="abc")
        assert c.config.token == "abc"

    def test_token_gen_no_api_key(self, rsps, environment_client_config):
        rsps.add(rsps.GET, "https://pypi.python.org/simple/cognite-sdk/#history", status=200, body="")
        rsps.add(
            rsps.POST,
            TOKEN_URL,
            status=200,
            json={"access_token": "abc", "expires_at": datetime.datetime.now().timestamp() + 1000},
        )

        c = CogniteClient()

        assert c.config.token() == "abc"

    def test_token_factory_set_no_api_key_and_no_project(self, rsps_with_login_mock):
        with unset_env_var("COGNITE_PROJECT"):
            c = CogniteClient(token=lambda: "abc")
        assert c.config.project == "test"

    def test_invalid_api_key(self, rsps):
        rsps.add(rsps.GET, _PYPI_ADDRESS, status=200, body="")
        rsps.add(
            rsps.GET,
            BASE_URL + "/login/status",
            status=200,
            json={"data": {"project": "", "loggedIn": False, "user": "", "projectId": -1}},
        )
        with unset_env_var("COGNITE_PROJECT"), set_env_var("COGNITE_API_KEY", "invalid"):
            with pytest.raises(CogniteAPIKeyError):
                CogniteClient()

    def test_no_client_name(self):
        with unset_env_var("COGNITE_CLIENT_NAME"), set_env_var("COGNITE_API_KEY", "bla"):
            with pytest.raises(ValueError, match="No client name has been specified"):
                CogniteClient()

    def assert_config_is_correct(
        self,
        config,
        base_url,
        max_workers,
        timeout,
        client_name,
        project,
        token_client_id,
        token_client_secret,
        token_url,
        token_scopes,
    ):
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

        assert config.token_client_id == token_client_id
        assert config.token_client_secret == token_client_secret
        assert config.token_url == token_url
        assert config.token_scopes == token_scopes

    def test_default_config(self, rsps_with_login_mock, default_client_config):
        with unset_env_var("COGNITE_PROJECT"), set_env_var("COGNITE_API_KEY", "bla"):
            client = CogniteClient()
        self.assert_config_is_correct(client.config, *default_client_config)

    def test_parameter_config(self):
        base_url = "blabla"
        max_workers = 1
        timeout = 10
        client_name = "test-client"
        project = "something"
        token_client_id = "test-client-id"
        token_client_secret = "test-client-secret"
        token_url = "https://param-test.com/token"
        token_scopes = ["test-scope", "second-test-scope"]
        with set_env_var("COGNITE_API_KEY", "bla"):
            client = CogniteClient(
                project=project,
                base_url=base_url,
                max_workers=max_workers,
                timeout=timeout,
                client_name=client_name,
                token_client_id=token_client_id,
                token_client_secret=token_client_secret,
                token_url=token_url,
                token_scopes=token_scopes,
            )
        self.assert_config_is_correct(
            client._config,
            base_url,
            max_workers,
            timeout,
            client_name,
            project,
            token_client_id,
            token_client_secret,
            token_url,
            token_scopes,
        )

    def test_environment_config(self, environment_client_config):
        with set_env_var("COGNITE_API_KEY", "bla"):
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
        with unset_env_var("COGNITE_PROJECT"), set_env_var("COGNITE_API_KEY", "bla"):
            CogniteClient(debug=True)
        log = logging.getLogger("cognite-sdk")
        assert isinstance(log.handlers[0].formatter, DebugLogFormatter)
        log.handlers = []
        log.propagate = False

    def test_version_check_disabled_env(self, rsps_with_login_mock):
        rsps_with_login_mock.assert_all_requests_are_fired = False
        with unset_env_var("COGNITE_PROJECT"), set_env_var("COGNITE_API_KEY", "bla"):
            with set_env_var("COGNITE_DISABLE_PYPI_VERSION_CHECK", "1"):
                CogniteClient()
        assert len(rsps_with_login_mock.calls) == 1
        assert rsps_with_login_mock.calls[0].request.url.startswith("https://greenfield.cognitedata.com")

    def test_version_check_disabled_arg(self, rsps_with_login_mock):
        rsps_with_login_mock.assert_all_requests_are_fired = False
        with unset_env_var("COGNITE_PROJECT", "COGNITE_DISABLE_PYPI_VERSION_CHECK"), set_env_var(
            "COGNITE_API_KEY", "bla"
        ):
            CogniteClient(disable_pypi_version_check=True)
        assert len(rsps_with_login_mock.calls) == 1
        assert rsps_with_login_mock.calls[0].request.url.startswith("https://greenfield.cognitedata.com")

    def test_api_version_present_in_header(self, rsps_with_login_mock, default_client_config):
        with set_env_var("COGNITE_API_KEY", "bla"):
            c = CogniteClient()
        c.login.status()
        assert rsps_with_login_mock.calls[1].request.headers["cdf-version"] == c.config.api_subversion

    def test_beta_header_for_beta_client(self, rsps_with_login_mock, default_client_config):
        from cognite.client.beta import CogniteClient as BetaClient

        with set_env_var("COGNITE_API_KEY", "bla"):
            c = BetaClient()
        c.login.status()
        assert rsps_with_login_mock.calls[1].request.headers["cdf-version"] == "beta"

    def test_version_check_enabled(self, rsps_with_login_mock):
        with unset_env_var("COGNITE_PROJECT"), set_env_var("COGNITE_API_KEY", "bla"):
            CogniteClient()
        assert len(rsps_with_login_mock.calls) == 2

    @patch("cognite.client.utils._version_checker.re.findall")
    @patch("cognite.client.utils._version_checker.requests")
    def test_verify_ssl_enabled_by_default(self, mock_requests, mock_findall):
        with set_env_var("COGNITE_API_KEY", "bla"):
            c = CogniteClient()

        mock_requests.get.assert_called_with(_PYPI_ADDRESS, verify=True, timeout=5)
        assert c._api_client._http_client_with_retry.session.verify is True
        assert c._api_client._http_client.session.verify is True

    @patch("cognite.client.utils._version_checker.re.findall")
    @patch("cognite.client.utils._version_checker.requests")
    def test_verify_ssl_disabled(self, mock_requests, mock_findall):
        with set_env_var("COGNITE_DISABLE_SSL", "1"):
            with set_env_var("COGNITE_API_KEY", "bla"):
                CogniteClient()
            mock_requests.get.assert_called_with(_PYPI_ADDRESS, verify=False, timeout=5)


class TestInstantiateWithClient:
    @pytest.mark.parametrize("cls", [Asset, Event, FileMetadata, TimeSeries])
    def test_instantiate_resources_with_cognite_client(self, cls):
        with set_env_var("COGNITE_API_KEY", "bla"):
            c = CogniteClient()
        assert cls(cognite_client=c)._cognite_client == c

    @pytest.mark.parametrize("cls", [AssetList, Event, FileMetadataList, TimeSeriesList])
    def test_intantiate_resource_lists_with_cognite_client(self, cls):
        with set_env_var("COGNITE_API_KEY", "bla"):
            c = CogniteClient()
        assert cls([], cognite_client=c)._cognite_client == c
