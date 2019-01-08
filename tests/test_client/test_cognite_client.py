import pytest

from cognite import APIError, CogniteClient


@pytest.fixture()
def client():
    yield CogniteClient()


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
