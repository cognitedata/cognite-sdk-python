from cognite.client import APIError


class TestAPIError:
    def test_api_error(self):
        e = APIError(message="bla", code=200, x_request_id="abc", extra={"k": "v"})
        assert "bla" == e.message
        assert 200 == e.code
        assert "abc" == e.x_request_id
        assert {"k": "v"} == e.extra

        assert "bla" in e.__str__()
        assert '"k": "v"' in e.__str__()
