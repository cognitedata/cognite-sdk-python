from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest

from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client.credentials import Token

# Files to exclude test directories or modules
collect_ignore = ["test_api/function_test_resources"]


# TODO: This class-scoped client causes side-effects between tests...
@pytest.fixture(scope="class")
def cognite_client() -> Iterator[CogniteClient]:
    with pytest.MonkeyPatch.context() as mp:
        # When writing unit tests, typcally with mocked responses, we don't want to wait unnecessarily:
        mp.setattr(global_config, "max_retries", 0)
        mp.setattr(global_config, "max_retries_connect", 0)
        mp.setattr(global_config, "max_retry_backoff", 0)

        cnf = ClientConfig(
            client_name="any",
            project="dummy",
            credentials=Token("bla"),
            timeout=1,
        )
        yield CogniteClient(cnf)


@pytest.fixture(scope="session")
def cognite_mock_client_placeholder() -> CogniteClient:
    """
    This is used for test cases where we need to pass a CogniteClient instance, but we don't actually use it.

    It is a performance optimization to avoid creating a CogniteClientMock for every test case:

    * CogniteClientMock is slow to create, but is stateful so must be created for every test case.

    Quick demo of difference:

    * Unit test with CogniteClientMock: 27s 407ms
    * Unit test with only creating CogniteClientMock once: 19s 765s
    """

    # We allow the mock to pass isinstance checks
    client = MagicMock()
    client.__class__ = CogniteClient
    return client
