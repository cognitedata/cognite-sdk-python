from cognite.client import CogniteClient

c = CogniteClient()


class TestLoginAPI:
    def test_login_status(self):
        assert {
            "user": "python-sdk-integration-tester",
            "project": "python-sdk-test",
            "project_id": 2561337318642649,
            "logged_in": True,
        } == c.login.status().dump()
