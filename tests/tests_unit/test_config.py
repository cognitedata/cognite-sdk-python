from contextlib import nullcontext as does_not_raise

import pytest

from cognite.client import global_config
from cognite.client.config import ClientConfig, GlobalConfig
from cognite.client.credentials import Token

_LOAD_RESOURCE_TO_DICT_ERROR = r"Resource must be json or yaml str, or dict, not"


class TestGlobalConfig:
    def test_global_config_singleton(self):
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
    def test_apply_settings(self, monkeypatch, client_config):
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

    def test_load_non_existent_attr(self):
        settings = {
            "max_workers": 0,  # use a nonsensical value to ensure that it is not applied without assuming other tests kept the default value
            "invalid_1": 10,
            "invalid_2": "foo",
        }

        with pytest.raises(ValueError, match=r"One or more invalid keys provided for global_config"):
            global_config.apply_settings(settings)

        # confirm that the valid keys were not applied since we don't want a partial application
        assert global_config.max_workers != 0


class TestClientConfig:
    def test_default(self):
        config = {
            "project": "test-project",
            "cdf_cluster": "test-cluster",
            "credentials": Token("abc"),
            "client_name": "test-client",
        }
        client_config = ClientConfig.default(**config)
        assert client_config.project == "test-project"
        assert client_config.base_url == "https://test-cluster.cognitedata.com"
        assert isinstance(client_config.credentials, Token)
        assert client_config.client_name == "test-client"

    @pytest.mark.parametrize(
        "credentials",
        [{"token": "abc"}, '{"token": "abc"}', {"token": (lambda: "abc")}, Token("abc"), Token(lambda: "abc"), None],
    )
    def test_load(self, credentials):
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
