from cognite.client import CogniteClient

c = CogniteClient()


class TestLoginAPI:
    def test_login_status(self):
        result = c.login.status().dump()
        assert result["logged_in"] is True
        assert 2561337318642649 == result["project_id"]
        assert "python-sdk-test" == result["project"]
