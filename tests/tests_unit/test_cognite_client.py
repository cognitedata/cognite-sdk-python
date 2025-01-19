import logging

import pytest

from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client._api.assets import AssetList
from cognite.client._api.files import FileMetadataList
from cognite.client._api.time_series import TimeSeriesList
from cognite.client.credentials import OAuthClientCredentials, Token
from cognite.client.data_classes import Asset, Event, FileMetadata, TimeSeries
from cognite.client.utils._logging import DebugLogFormatter

BASE_URL = "http://blabla.cognitedata.com"
TOKEN_URL = "https://test.com/token"


@pytest.fixture
def client_config_w_token_factory():
    return ClientConfig(
        client_name="test-client",
        project="test-project",
        base_url=BASE_URL,
        max_workers=1,
        timeout=10,
        credentials=Token(lambda: "abc"),
    )


@pytest.fixture
def client_config_w_client_credentials():
    return ClientConfig(
        client_name="test-client",
        project="test-project",
        base_url=BASE_URL,
        max_workers=1,
        timeout=10,
        credentials=OAuthClientCredentials(
            client_id="test-client-id",
            client_secret="test-client-secret",
            token_url=TOKEN_URL,
            scopes=["https://test.com/.default", "https://test.com/.admin"],
        ),
    )


@pytest.fixture
def mock_token_inspect(httpx_mock) -> None:
    httpx_mock.add_response(
        method="GET",
        url=BASE_URL + "/api/v1/token/inspect",
        status_code=200,
        json={"subject": "bla", "capabilities": [], "projects": []},
    )
    yield


class TestCogniteClient:
    def test_project_is_empty(self):
        with pytest.raises(ValueError, match="Invalid value for ClientConfig.project: <>"):
            CogniteClient(ClientConfig(client_name="", project="", credentials=Token("bla")))
        with pytest.raises(ValueError, match="Invalid value for ClientConfig.project: <None>"):
            CogniteClient(ClientConfig(client_name="", project=None, credentials=Token("bla")))

    def test_project_is_correct(self, client_config_w_token_factory):
        client = CogniteClient(client_config_w_token_factory)
        assert client.config.project == "test-project"

    def test_default_client_config_set(self, client_config_w_token_factory) -> None:
        global_config.default_client_config = client_config_w_token_factory
        client = CogniteClient()
        assert client.config == client_config_w_token_factory
        global_config.default_client_config = None

    def test_default_client_config_not_set(self) -> None:
        with pytest.raises(ValueError, match="No ClientConfig has been provided"):
            CogniteClient()

    def test_client_debug_mode(self):
        CogniteClient(ClientConfig(client_name="bla", project="bla", credentials=Token("bla"), debug=True))
        log = logging.getLogger("cognite.client")
        assert isinstance(log.handlers[0].formatter, DebugLogFormatter)
        log.handlers = []
        log.propagate = False

    def test_api_version_present_in_header(self, httpx_mock, client_config_w_token_factory, mock_token_inspect):
        client = CogniteClient(client_config_w_token_factory)
        client.iam.token.inspect()
        assert httpx_mock.get_requests()[0].headers["cdf-version"] == client.config.api_subversion

    def test_beta_header_for_beta_client(self, httpx_mock, client_config_w_token_factory, mock_token_inspect):
        from cognite.client.beta import CogniteClient as BetaClient

        client = BetaClient(client_config_w_token_factory)
        client.iam.token.inspect()
        assert httpx_mock.get_requests()[0].headers["cdf-version"] == "beta"

    def test_verify_ssl_enabled_by_default(self, httpx_mock, client_config_w_token_factory, mock_token_inspect):
        client = CogniteClient(client_config_w_token_factory)
        client.iam.token.inspect()

        assert httpx_mock.get_requests()[0][0].req_kwargs["verify"] is True
        assert client._api_client._http_client_with_retry.session.verify is True
        assert client._api_client._http_client.session.verify is True

    def test_client_load(self):
        config = {
            "project": "test-project",
            "client_name": "cognite-sdk-python",
            "debug": True,
            "credentials": {
                "client_credentials": {
                    "client_id": "test-client-id",
                    "client_secret": "test-client-secret",
                    "token_url": TOKEN_URL,
                    "scopes": ["https://test.com/.default", "https://test.com/.admin"],
                }
            },
        }
        client = CogniteClient.load(config)
        assert client.config.project == "test-project"
        assert client.config.credentials.client_id == "test-client-id"
        assert client.config.credentials.client_secret == "test-client-secret"
        assert client.config.credentials.token_url == TOKEN_URL
        assert client.config.credentials.scopes == ["https://test.com/.default", "https://test.com/.admin"]
        assert client.config.debug is True
        log = logging.getLogger("cognite.client")
        log.handlers = []
        log.propagate = False


class TestInstantiateWithClient:
    @pytest.mark.parametrize("cls", [Asset, Event, FileMetadata, TimeSeries])
    def test_instantiate_resources_with_cognite_client(self, cls, client_config_w_token_factory):
        client = CogniteClient(client_config_w_token_factory)
        assert cls(cognite_client=client)._cognite_client == client

    @pytest.mark.parametrize("cls", [AssetList, Event, FileMetadataList, TimeSeriesList])
    def test_instantiate_resource_lists_with_cognite_client(self, cls, client_config_w_token_factory):
        client = CogniteClient(client_config_w_token_factory)
        assert cls([], cognite_client=client)._cognite_client == client
