from unittest import mock

import pytest
from requests.structures import CaseInsensitiveDict

from cognite.config import configure_session

MOCK_API_KEY = 'SOME_KEY'
MOCK_PROJECT = 'SOME_PROJECT'


@pytest.fixture
def configure_test_session():
    configure_session(MOCK_API_KEY, MOCK_PROJECT)
    yield
    configure_session('', '')  # teardown


class MockReturnValue(mock.Mock):
    """Helper class for building mock request responses.

    Should be assigned to MagicMock.return_value
    """

    def __init__(self, status=200, content="CONTENT", json_data=None, raise_for_status=None,
                 headers=CaseInsensitiveDict()
                 ):
        mock.Mock.__init__(self)
        if "X-Request-Id" not in headers:
            headers["X-Request-Id"] = "1234567890"

        # mock raise_for_status call w/optional error
        self.raise_for_status = mock.Mock()
        if raise_for_status:
            self.raise_for_status.side_effect = raise_for_status

        # set status code and content
        self.status_code = status

        # requests.models.Response.ok mock
        self.ok = status < 400
        self.content = content
        self.headers = headers

        # add json data if provided
        if json_data:
            self.json = mock.Mock(
                return_value=json_data
            )
