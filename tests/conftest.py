import platform
from collections.abc import Callable, Iterator

import dotenv
import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite.client import global_config
from cognite.client._api_client import APIClient

dotenv.load_dotenv()

global_config.disable_pypi_version_check = True


_STANDARD_API_LIMIT_NAMES = [
    "_CREATE_LIMIT",
    "_LIST_LIMIT",
    "_RETRIEVE_LIMIT",
    "_UPDATE_LIMIT",
    "_DELETE_LIMIT",
]


@pytest.fixture
def set_request_limit(monkeypatch: pytest.MonkeyPatch) -> Callable[[APIClient, int], None]:
    """
    Pytest fixture that provides a factory function to temporarily set API limits
    on a client instance for the duration of a single test.
    """

    def _setter(client: APIClient, limit: int) -> None:
        for limit_name in _STANDARD_API_LIMIT_NAMES:
            # We use raising=False to prevents an error if the attribute doesn't exist:
            monkeypatch.setattr(client, limit_name, limit, raising=False)

    return _setter


@pytest.fixture
def disable_gzip(monkeypatch: MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(global_config, "disable_gzip", True)
    yield


@pytest.fixture(scope="session")
def os_and_py_version() -> str:
    # Nice to use to create resources that is unique to each test runner
    return f"{platform.system()}-{platform.python_version()}"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--test-deps-only-core", action="store_true", default=False, help="Test only core deps are installed"
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--test-deps-only-core"):
        return None
    skip_core = pytest.mark.skip(reason="need --test-deps-only-core option to run")
    for item in items:
        if "coredeps" in item.keywords:
            item.add_marker(skip_core)
