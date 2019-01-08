import os

import pytest

from cognite import APIError, CogniteClient


@pytest.fixture
def client():
    yield CogniteClient()


@pytest.fixture
def default_client_config():
    from cognite.client.cognite_client import (
        DEFAULT_BASE_URL,
        DEFAULT_NUM_OF_RETRIES,
        DEFAULT_NUM_OF_WORKERS,
        DEFAULT_TIMEOUT,
    )

    yield DEFAULT_BASE_URL, DEFAULT_NUM_OF_RETRIES, DEFAULT_NUM_OF_WORKERS, DEFAULT_TIMEOUT


class TestCogniteClient:
    def test_get(self, client):
        res = client.get("/login/status")
        assert res.status_code == 200

    def test_post(self, client):
        res = client.post("/login", body={"apiKey": client._CogniteClient__api_key})
        assert res.status_code == 200

    def test_put(self, client):
        with pytest.raises(APIError) as e:
            client.put("/login")
        assert e.value.code == 405

    def test_delete(self, client):
        with pytest.raises(APIError) as e:
            client.delete("/login")
        assert e.value.code == 405

    def test_project_is_correct(self, client):
        assert client._project == "mltest"

    def assert_config_is_correct(self, client, base_url, num_of_retries, num_of_workers, timeout):
        assert client._base_url == base_url
        assert client._num_of_retries == num_of_retries
        assert client._num_of_workers == num_of_workers
        assert client._timeout == timeout

    def test_default_config(self, client, default_client_config):
        self.assert_config_is_correct(client, *default_client_config)

    def test_parameter_config(self):
        base_url = "blabla"
        num_of_retries = 1
        num_of_workers = 1
        timeout = 10

        client = CogniteClient(
            project="something",
            base_url=base_url,
            num_of_retries=num_of_retries,
            num_of_workers=num_of_workers,
            timeout=timeout,
        )
        self.assert_config_is_correct(client, base_url, num_of_retries, num_of_workers, timeout)
