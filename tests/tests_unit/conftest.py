import pytest

from cognite.client import CogniteClient
from tests.utils import set_env_var


@pytest.fixture(scope="module")
def cognite_client():
    with set_env_var("COGNITE_API_KEY", "bla"):
        yield CogniteClient()
