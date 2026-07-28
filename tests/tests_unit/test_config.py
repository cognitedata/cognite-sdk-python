from __future__ import annotations

from collections import OrderedDict
from contextlib import nullcontext as does_not_raise

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite.client import global_config
from cognite.client.config import ClientConfig, GlobalConfig
from cognite.client.credentials import Token

_LOAD_RESOURCE_TO_DICT_ERROR = r"Resource must be json or yaml str, or dict, not"


class TestGlobalConfig:
    def test_global_config_singleton(self) -> None:
        with pytest.raises(
            TypeError,
            match=r"GlobalConfig is a singleton and cannot be instantiated directly. Use `global_config` instead,",
        ):
            GlobalConfig()

    @pytest.mark.parametrize(
        "client_config",
        [
            {
                "project": "test-project",
                "base_url": "https://test-cluster.cognitedata.com/",
                "credentials": {"token": "abc"},
                "client_name": "test-client",
            },
            ClientConfig(
                project="test-project",
                base_url="https://test-cluster.cognitedata.com/",
                credentials=Token("abc"),
                client_name="test-client",
            ),
            None,
        ],
    )
    def test_apply_settings(self, monkeypatch: MonkeyPatch, client_config: ClientConfig) -> None:
        monkeypatch.delattr(GlobalConfig, "_instance")  # ensure that the singleton is re-instantiated
        gc = GlobalConfig()
        assert gc.max_workers == 5
        assert gc.max_retries == 10

        settings = {
            "max_workers": 6,
            "max_retries": 11,
            "default_client_config": client_config,
        }
        with pytest.raises(TypeError, match=_LOAD_RESOURCE_TO_DICT_ERROR) if not client_config else does_not_raise():
            gc.apply_settings(settings)
            assert gc.max_workers == 6
            assert gc.max_retries == 11
            assert isinstance(gc.default_client_config, ClientConfig)
            assert isinstance(gc.default_client_config.credentials, Token)
            assert gc.default_client_config.project == "test-project"

    @pytest.mark.parametrize(
        "attr, value",
        [
            ("max_retries", 0),
            ("max_retries", 10),
            ("max_retries_connect", 0),
            ("max_retries_connect", 3),
            ("max_retry_backoff", 0),
            ("max_retry_backoff", 60),
            ("max_connection_pool_size", 1),
            ("max_connection_pool_size", 20),
            ("file_download_chunk_size", None),
            ("file_download_chunk_size", 1024),
            ("file_upload_chunk_size", None),
            ("file_upload_chunk_size", 65536),
        ],
    )
    def test_validated_attrs_valid(self, monkeypatch: MonkeyPatch, attr: str, value: object) -> None:
        # raising=True ensures the attribute actually exists on global_config, catching typos in parameters
        monkeypatch.setattr(global_config, attr, value, raising=True)
        assert getattr(global_config, attr) == value

    @pytest.mark.parametrize(
        "attr, value, match",
        [
            ("max_retries", -1, "non-negative integer"),
            ("max_retries", 1.5, "non-negative integer"),
            ("max_retries", None, "non-negative integer"),
            ("max_retries_connect", -1, "non-negative integer"),
            ("max_retry_backoff", -1, "non-negative integer"),
            ("max_connection_pool_size", 0, "positive integer"),
            ("max_connection_pool_size", -1, "positive integer"),
            ("file_download_chunk_size", 0, "positive integer or None"),
            ("file_download_chunk_size", -1, "positive integer or None"),
            ("file_upload_chunk_size", 0, "positive integer or None"),
        ],
    )
    def test_validated_attrs_invalid(self, attr: str, value: object, match: str) -> None:
        with pytest.raises(ValueError, match=match):
            setattr(global_config, attr, value)

    def test_apply_settings_invalid_value_is_rolled_back(self) -> None:
        original_retries = global_config.max_retries
        original_retries_connect = global_config.max_retries_connect
        # OrderedDict guarantees max_retries (valid) is applied before max_retries_connect (invalid),
        # so the rollback has something to actually undo:
        with pytest.raises(ValueError, match="non-negative integer"):
            global_config.apply_settings(
                OrderedDict([("max_retries", original_retries + 1), ("max_retries_connect", -1)])
            )

        assert global_config.max_retries == original_retries
        assert global_config.max_retries_connect == original_retries_connect

    def test_load_non_existent_attr(self) -> None:
        settings = {
            "max_workers": 0,  # use a nonsensical value to ensure that it is not applied without assuming other tests kept the default value
            "invalid_1": 10,
            "invalid_2": "foo",
        }

        with pytest.raises(ValueError, match=r"One or more invalid keys provided for global_config"):
            global_config.apply_settings(settings)

        # confirm that the valid keys were not applied since we don't want a partial application
        assert global_config.max_workers != 0


@pytest.fixture
def client_config() -> ClientConfig:
    return ClientConfig.default(
        project="test-project",
        cdf_cluster="test-cluster",
        credentials=Token("abc"),
        client_name="test-client",
    )


class TestClientConfig:
    def test_default(self, client_config: ClientConfig) -> None:
        assert client_config.project == "test-project"
        assert client_config.base_url == "https://test-cluster.cognitedata.com"
        assert isinstance(client_config.credentials, Token)
        assert client_config.client_name == "test-client"

    @pytest.mark.parametrize(
        "credentials",
        [{"token": "abc"}, '{"token": "abc"}', {"token": (lambda: "abc")}, Token("abc"), Token(lambda: "abc"), None],
    )
    def test_load(self, credentials: dict) -> None:
        config = {
            "project": "test-project",
            "base_url": "https://test-cluster.cognitedata.com/",
            "credentials": credentials,
            "client_name": "test-client",
        }
        with pytest.raises(TypeError, match=_LOAD_RESOURCE_TO_DICT_ERROR) if not credentials else does_not_raise():
            client_config = ClientConfig.load(config)
            assert client_config.project == "test-project"
            assert client_config.base_url == "https://test-cluster.cognitedata.com"
            assert isinstance(client_config.credentials, Token)
            assert "Authorization", "Bearer abc" == client_config.credentials.authorization_header()
            assert client_config.client_name == "test-client"

    @pytest.mark.parametrize("protocol", ("http", "https"))
    @pytest.mark.parametrize("end", ("", "/", ":8080", "/api/v1/", ":8080/api/v1/"))
    @pytest.mark.parametrize("subdomain", ("", "p001.plink."))
    @pytest.mark.parametrize(
        "cluster", ("3D", "my_clus-ter", "jazz-testing-asia-northeast1-1", "trial-00ed82e12d9cbadfe28e4")
    )
    def test_extract_valid_cdf_cluster(
        self, client_config: ClientConfig, protocol: str, end: str, subdomain: str, cluster: str
    ) -> None:
        client_config.base_url = f"{protocol}://{subdomain}{cluster}.cognitedata.com{end}"
        assert client_config.cdf_cluster == cluster

    @pytest.mark.parametrize("protocol", ("http", "https"))
    @pytest.mark.parametrize("end", ("", "/", ":8080", "/api/v1/", ":8080/api/v1/"))
    @pytest.mark.parametrize("subdomain", ("", "p001.plink."))
    @pytest.mark.parametrize("cluster", ("", ".", "..", "huh.my_cluster."))
    def test_extract_invalid_cdf_cluster(
        self, client_config: ClientConfig, protocol: str, end: str, subdomain: str, cluster: str
    ) -> None:
        client_config.base_url = f"{protocol}://{subdomain}{cluster}cognitedata.com{end}"
        assert client_config.cdf_cluster is None

    def test_extract_invalid_url(self, client_config: ClientConfig) -> None:
        client_config.base_url = "invalid"
        assert client_config.cdf_cluster is None
