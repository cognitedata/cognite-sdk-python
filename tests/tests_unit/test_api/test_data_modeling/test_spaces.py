import re

import pytest
from responses import RequestsMock

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError


class TestSpaces:
    @pytest.fixture
    def mock_spaces_delete_raise_error(self, rsps: RequestsMock, cognite_client: CogniteClient) -> RequestsMock:
        response_body = {"error": "smth"}
        url_pattern = re.compile(
            re.escape(cognite_client.data_modeling.spaces._get_base_url_with_base_path()) + "/models/spaces/delete"
        )
        rsps.add(rsps.POST, url_pattern, status=400, json=response_body)
        yield rsps

    def test_failed_delete_task(
        self, cognite_client: CogniteClient, mock_spaces_delete_raise_error: RequestsMock
    ) -> None:
        some_space = "i-dont-actually-exist"
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.spaces.delete(some_space)
        assert error.value.code == 400
