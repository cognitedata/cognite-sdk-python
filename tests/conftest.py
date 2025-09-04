import platform

import dotenv
import pytest

from cognite.client import global_config

dotenv.load_dotenv()

global_config.disable_pypi_version_check = True


@pytest.fixture
def disable_gzip(monkeypatch):
    monkeypatch.setattr(global_config, "disable_gzip", True)
    yield


@pytest.fixture(scope="session")
def os_and_py_version() -> str:
    # Nice to use to create resources that is unique to each test runner
    return f"{platform.system()}-{platform.python_version()}"


def pytest_addoption(parser):
    parser.addoption(
        "--test-deps-only-core", action="store_true", default=False, help="Test only core deps are installed"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--test-deps-only-core"):
        return None
    skip_core = pytest.mark.skip(reason="need --test-deps-only-core option to run")
    for item in items:
        if "coredeps" in item.keywords:
            item.add_marker(skip_core)
