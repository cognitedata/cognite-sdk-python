import pytest

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError

c = CogniteClient(debug=True)


class TestCogniteClient:
    def test_get(self):
        res = c.get("/login/status")
        assert res.status_code == 200

    def test_post(self):
        with pytest.raises(CogniteAPIError) as e:
            c.post("/login", json={})
        assert e.value.code == 404

    def test_put(self):
        with pytest.raises(CogniteAPIError) as e:
            c.put("/login")
        assert e.value.code == 404

    def test_delete(self):
        with pytest.raises(CogniteAPIError) as e:
            c.delete("/login")
        assert e.value.code == 404
