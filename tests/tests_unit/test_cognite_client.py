from __future__ import annotations

import logging
import ssl
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, ClientConfig, CogniteClient, global_config
from cognite.client._http_client import get_global_async_httpx_client
from cognite.client.credentials import OAuthClientCredentials, Token
from cognite.client.utils._logging import DebugLogFormatter

BASE_URL = "http://blabla.cognitedata.com"
TOKEN_URL = "https://test.com/token"


@pytest.fixture
def client_config_w_token_factory() -> ClientConfig:
    return ClientConfig(
        client_name="test-client",
        project="test-project",
        base_url=BASE_URL,
        timeout=10,
        credentials=Token(lambda: "abc"),
    )


@pytest.fixture
def client_config_w_client_credentials() -> ClientConfig:
    return ClientConfig(
        client_name="test-client",
        project="test-project",
        base_url=BASE_URL,
        timeout=10,
        credentials=OAuthClientCredentials(
            client_id="test-client-id",
            client_secret="test-client-secret",
            token_url=TOKEN_URL,
            scopes=["https://test.com/.default", "https://test.com/.admin"],
        ),
    )


@pytest.fixture
def mock_token_inspect(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="GET",
        url=BASE_URL + "/api/v1/token/inspect",
        status_code=200,
        json={"subject": "bla", "capabilities": [], "projects": []},
    )


class TestCogniteClient:
    def test_project_is_empty(self) -> None:
        with pytest.raises(ValueError, match=r"Invalid value for ClientConfig.project: ''"):
            CogniteClient(ClientConfig(client_name="", project="", cluster="foo", credentials=Token("bla")))
        with pytest.raises(ValueError, match=r"Invalid value for ClientConfig.project: None"):
            CogniteClient(ClientConfig(client_name="", project=None, cluster="foo", credentials=Token("bla")))  # type: ignore[arg-type]

    def test_cluster_or_base_url_are_empty(self) -> None:
        CogniteClient(ClientConfig(client_name="", project="a", cluster="x", credentials=Token("bla")))
        CogniteClient(
            ClientConfig(client_name="", project="a", base_url="https://x.cognitedata.com", credentials=Token("bla"))
        )

        # Passing both should ignore cluster with a warning:
        with pytest.warns(UserWarning, match="parameter is ignored when"):
            CogniteClient(
                ClientConfig(
                    client_name="",
                    project="a",
                    cluster="foo",
                    base_url="https://x.cognitedata.com",
                    credentials=Token("bla"),
                )
            )

        # Passing neither should raise:
        with pytest.raises(ValueError, match=r"must be provided. Passing"):
            CogniteClient(ClientConfig(client_name="", project="a", credentials=Token("bla")))

    def test_project_is_correct(self, client_config_w_token_factory: ClientConfig) -> None:
        async_client = AsyncCogniteClient(client_config_w_token_factory)
        assert async_client.config.project == "test-project"

    def test_default_client_config_set(self, client_config_w_token_factory: ClientConfig) -> None:
        global_config.default_client_config = client_config_w_token_factory
        async_client = AsyncCogniteClient()
        assert async_client.config == client_config_w_token_factory
        global_config.default_client_config = None

    def test_default_client_config_not_set(self) -> None:
        with pytest.raises(ValueError, match="No ClientConfig has been provided"):
            CogniteClient()

    def test_client_debug_mode(self) -> None:
        CogniteClient(ClientConfig(client_name="bla", project="bla", cluster="x", credentials=Token("bla"), debug=True))
        log = logging.getLogger("cognite.client")
        assert isinstance(log.handlers[0].formatter, DebugLogFormatter)
        log.handlers = []
        log.propagate = False

    async def test_api_version_present_in_header(
        self, httpx_mock: HTTPXMock, client_config_w_token_factory: ClientConfig, mock_token_inspect: Any
    ) -> None:
        async_client = AsyncCogniteClient(client_config_w_token_factory)
        await async_client.iam.token.inspect()
        assert httpx_mock.get_requests()[0].headers["cdf-version"] == async_client.config.api_subversion

    def test_verify_ssl_enabled_by_default(self, async_client: AsyncCogniteClient) -> None:
        httpx_client = get_global_async_httpx_client()
        assert ssl.CERT_REQUIRED is httpx_client._transport._pool._ssl_context.verify_mode  # type: ignore[attr-defined]

        # Using iam is just a random choice here, could be any APIClient:
        assert httpx_client is async_client.iam._http_client.httpx_async_client
        assert httpx_client is async_client.iam._http_client_with_retry.httpx_async_client

    @pytest.mark.parametrize("which_client", [CogniteClient, AsyncCogniteClient])
    def test_client_load(self, which_client: type[CogniteClient | AsyncCogniteClient]) -> None:
        config = {
            "project": "test-project",
            "client_name": "cognite-sdk-python",
            "debug": False,
            "cluster": "foo",
            "credentials": {
                "client_credentials": {
                    "client_id": "test-client-id",
                    "client_secret": "test-client-secret",
                    "token_url": TOKEN_URL,
                    "scopes": ["https://test.com/.default", "https://test.com/.admin"],
                }
            },
        }
        client = which_client.load(config)
        assert client.config.project == "test-project"
        assert client.config.base_url == "https://foo.cognitedata.com"
        creds = client.config.credentials
        assert isinstance(creds, OAuthClientCredentials)
        assert creds.client_id == "test-client-id"
        assert creds.client_secret == "test-client-secret"
        assert creds.token_url == TOKEN_URL
        assert creds.scopes == ["https://test.com/.default", "https://test.com/.admin"]
        assert client.config.debug is False
