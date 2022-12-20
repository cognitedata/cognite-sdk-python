import dotenv
import pytest
import responses

from cognite.client import global_config

dotenv.load_dotenv()

global_config.disable_pypi_version_check = True


@pytest.fixture
def rsps():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def disable_gzip():
    old = global_config.disable_gzip
    global_config.disable_gzip = True
    yield
    global_config.disable_gzip = old


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
