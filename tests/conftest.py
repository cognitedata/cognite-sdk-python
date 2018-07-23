import os
from unittest import mock

import pytest
from requests.structures import CaseInsensitiveDict

from cognite.config import configure_session

TEST_API_KEY = os.getenv('COGNITE_TEST_API_KEY')
TEST_PROJECT = 'mltest'


@pytest.fixture(scope='session', autouse=True)
def configure_test_session():
    configure_session(TEST_API_KEY, TEST_PROJECT)
    yield
    configure_session('', '')  # teardown


@pytest.fixture
def unset_config_variables():
    configure_session('', '')
    print('SETUP unset_config_variables')
    yield (TEST_API_KEY, TEST_PROJECT)
    print('TEARDOWN unset_config_variables')
    configure_session(TEST_API_KEY, TEST_PROJECT)


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
