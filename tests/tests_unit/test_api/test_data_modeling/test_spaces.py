import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from tests.utils import get_url


class TestSpaces:
    @pytest.fixture
    def mock_spaces_delete_raise_error(self, httpx_mock: HTTPXMock, cognite_client: CogniteClient) -> HTTPXMock:
        response_body = {"error": "smth"}
        url_pattern = re.compile(re.escape(get_url(cognite_client.data_modeling.spaces)) + "/models/spaces/delete")
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=400, json=response_body)
        yield httpx_mock

    def test_failed_delete_task(self, cognite_client: CogniteClient, mock_spaces_delete_raise_error: HTTPXMock) -> None:
        some_space = "i-dont-actually-exist"
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.spaces.delete(some_space)
        assert error.value.code == 400
