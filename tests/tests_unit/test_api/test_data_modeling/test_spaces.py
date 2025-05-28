import re

import pytest
import respx
from respx import MockRouter

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError


class TestSpaces:
    @pytest.fixture
    @respx.mock
    def mock_spaces_delete_raise_error(self, respx_mock: MockRouter, cognite_client: CogniteClient) -> MockRouter:
        response_body = {"error": "smth"}
        url_pattern = re.compile(
            re.escape(cognite_client.data_modeling.spaces._get_base_url_with_base_path()) + "/models/spaces/delete"
        )
        respx_mock.post(url_pattern).respond(status_code=400, json=response_body)
        yield respx_mock

    def test_failed_delete_task(
        self, cognite_client: CogniteClient, mock_spaces_delete_raise_error: MockRouter
    ) -> None:
        some_space = "i-dont-actually-exist"
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.spaces.delete(some_space)
        assert error.value.code == 400
