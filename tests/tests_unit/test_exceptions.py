from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from tests.utils import set_env_var


class TestAPIError:
    def test_api_error(self):
        e = CogniteAPIError(
            message="bla",
            code=200,
            x_request_id="abc",
            missing=[{"id": 123}],
            duplicated=[{"externalId": "abc"}],
            successful=["bla"],
        )
        assert "bla" == e.message
        assert 200 == e.code
        assert "abc" == e.x_request_id
        assert [{"id": 123}] == e.missing
        assert [{"externalId": "abc"}] == e.duplicated

        assert "bla" in e.__str__()

    def test_unknown_fields_in_api_error(self, rsps):
        with set_env_var("COGNITE_DISABLE_PYPI_VERSION_CHECK", "1"), set_env_var("COGNITE_API_KEY", "BLA"):
            c = CogniteClient()
        rsps.add(
            rsps.GET,
            c.assets._get_base_url_with_base_path() + "/any",
            status=400,
            json={"error": {"message": "bla", "extra": {"haha": "blabla"}, "other": "yup"}},
        )
        try:
            c.get(url="/api/v1/projects/{}/any".format(c.config.project))
            assert False, "Call did not raise exception"
        except CogniteAPIError as e:
            assert {"extra": {"haha": "blabla"}, "other": "yup"} == e.extra
