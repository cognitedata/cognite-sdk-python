import pytest

from cognite.client.exceptions import CogniteAPIError


@pytest.fixture
def mock_get_400_error(rsps, cognite_client):
    rsps.add(
        rsps.GET,
        cognite_client.assets._get_base_url_with_base_path() + "/any",
        status=400,
        json={"error": {"message": "bla", "extra": {"haha": "blabla"}, "other": "yup"}},
    )


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

        assert "bla" in str(e)

    def test_unknown_fields_in_api_error(self, mock_get_400_error, cognite_client):
        try:
            cognite_client.get(url=f"/api/v1/projects/{cognite_client.config.project}/any")
            assert False, "Call did not raise exception"
        except CogniteAPIError as e:
            assert {"extra": {"haha": "blabla"}, "other": "yup"} == e.extra

    def test_api_errors_contain_cluster_info(self, mock_get_400_error, cognite_client):
        try:
            cognite_client.get(url=f"/api/v1/projects/{cognite_client.config.project}/any")
            assert False, "Call did not raise exception"
        except CogniteAPIError as e:
            assert e.cluster == "api"
