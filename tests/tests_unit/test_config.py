import pytest

from cognite.client import global_config
from cognite.client.config import ClientConfig, GlobalConfig
from cognite.client.credentials import Token


class TestGlobalConfig:
    def test_global_config_singleton(self):
        with pytest.raises(
            ValueError,
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
        gc.apply_settings(settings)
        assert gc.max_workers == 6
        assert gc.max_retries == 11

        if client_config:
            assert isinstance(gc.default_client_config, ClientConfig)
            assert isinstance(gc.default_client_config.credentials, Token)
            assert gc.default_client_config.project == "test-project"
        else:
            assert gc.default_client_config is None

    def test_load_non_existent_attr(self):
        settings = {
            "max_workers": 0,  # use a nonsensical value to ensure that it is not applied without assuming other tests kept the default value
            "invalid_1": 10,
            "invalid_2": "foo",
        }

        with pytest.raises(ValueError, match=r"Invalid keys provided for global_config, no settings applied:"):
            global_config.apply_settings(settings)

        assert (
            global_config.max_workers != 0
        )  # confirm that the valid keys were not applied since we don't want a partial application


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

    # TODO: do we want this to raise an error when credentials is None?
    def test_credentials_none(self):
        config = {
            "project": "test-project",
            "cdf_cluster": "test-cluster",
            "credentials": None,
            "client_name": "test-client",
        }
        client_config = ClientConfig.default(**config)
        assert client_config.project == "test-project"
        assert client_config.base_url == "https://test-cluster.cognitedata.com"
        assert client_config.credentials is None
        assert client_config.client_name == "test-client"

    @pytest.mark.parametrize(
        "credentials",
        [{"token": "abc"}, '{"token": "abc"}', {"token": (lambda: "abc")}, Token("abc"), Token(lambda: "abc")],
    )
    def test_load(self, credentials):
        config = {
            "project": "test-project",
            "base_url": "https://test-cluster.cognitedata.com/",
            "credentials": credentials,
            "client_name": "test-client",
        }
        client_config = ClientConfig.load(config)
        assert client_config.project == "test-project"
        assert client_config.base_url == "https://test-cluster.cognitedata.com"
        assert isinstance(client_config.credentials, Token)
        assert "Authorization", "Bearer abc" == client_config.credentials.authorization_header()
        assert client_config.client_name == "test-client"
