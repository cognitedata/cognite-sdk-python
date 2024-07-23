import pytest

from cognite.client.config import ClientConfig, GlobalConfig
from cognite.client.credentials import Token


class TestGlobalConfig:
    def test_load(self):
        config = {
            "max_workers": 5,
            "max_retries": 3,
        }
        global_config = GlobalConfig.load(config)
        assert global_config.max_workers == 5
        assert global_config.max_retries == 3

    def test_load_non_existent_attr(self):
        config = {
            "test": 10,
        }
        with pytest.raises(ValueError, match=r"Invalid key in global config: .*"):
            GlobalConfig.load(config)


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

    def test_load(self):
        config = {
            "project": "test-project",
            "base_url": "https://test-cluster.cognitedata.com/",
            "credentials": {"token": "abc"},
            "client_name": "test-client",
        }
        client_config = ClientConfig.load(config)
        assert client_config.project == "test-project"
        assert client_config.base_url == "https://test-cluster.cognitedata.com"
        assert isinstance(client_config.credentials, Token)
        assert client_config.client_name == "test-client"
