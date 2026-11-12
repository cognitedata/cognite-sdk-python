from __future__ import annotations

import os
import platform
import random
from collections import defaultdict
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import dotenv
import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite.client import global_config
from cognite.client._api_client import APIClient
from cognite.client._http_client import AsyncHTTPClientWithRetry

dotenv.load_dotenv()

global_config.disable_pypi_version_check = True

ALLOW_NO_SEMAPHORE_MARKER = "allow_no_semaphore"


@pytest.fixture(autouse=True)
def require_semaphore_on_every_request(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """As a main rule, all API calls in the test suite MUST route through a semaphore.

    Production code falls back to ``nullcontext()`` when ``semaphore is None``; that path
    exists for users calling top-level ``client.post(...)``/``client.get(...)`` with raw URLs,
    but no internal API method should ever hit it. Patching here turns a missing semaphore
    into a hard failure so a regression (forgotten ``semaphore=...`` arg) shows up loudly.

    Tests that legitimately exercise the None path opt out via ``@pytest.mark.allow_no_semaphore("<reason>")``.
    These are the top-level methods mentioned above plus any method entering the semaphore
    at a higher level than at the HTTP request level (e.g. datapoints.insert)
    """
    if request.node.get_closest_marker(ALLOW_NO_SEMAPHORE_MARKER):
        return

    original = AsyncHTTPClientWithRetry._with_retry

    async def strict(
        self: AsyncHTTPClientWithRetry, coro_factory: Any, *, url: str, headers: Any, semaphore: Any
    ) -> Any:
        if semaphore is None:
            pytest.fail(
                f"Internal API call to {url!r} did not pass a semaphore. "
                "All endpoints behind client.<api>.<method> must route through a semaphore — "
                "the nullcontext fallback is reserved for top-level client.post/get calls only. "
                "If this call legitimately holds the semaphore at a higher level, mark the test "
                f"with @pytest.mark.{ALLOW_NO_SEMAPHORE_MARKER}('<reason>')."
            )
        return await original(self, coro_factory, url=url, headers=headers, semaphore=semaphore)

    monkeypatch.setattr(AsyncHTTPClientWithRetry, "_with_retry", strict)


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    # The anyio package by default runs all async tests using all backends like trio and asyncio
    # but we just want to use asyncio:
    return "asyncio"


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
        assert isinstance(client, APIClient), "Did you mean to pass e.g. async_client.<some_api>?"

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


@pytest.fixture(scope="session")
def sdk_version() -> tuple[str, str, str]:
    # Nice to use to create resources that is unique per e.g. major version of the SDK
    from cognite.client import __version__

    return tuple(__version__.split(".", 2))  # type: ignore [return-value]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--test-deps-only-core", action="store_true", default=False, help="Test only core deps are installed"
    )


def apply_coredeps_skips(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip @pytest.mark.coredeps tests unless --test-deps-only-core is set."""
    if config.getoption("--test-deps-only-core"):
        return

    skip_core = pytest.mark.skip(reason="need --test-deps-only-core option to run")
    for item in items:
        if "coredeps" in item.keywords:
            item.add_marker(skip_core)


def shuffle_test_modules(items: list[pytest.Item]) -> None:
    """Shuffles test file (module) execution order in CI to prevent issues and smear out load.

    Why this exists:
    When GitHub Actions launches a matrix build (OS x Python version), dozens of worker processes
    execute the test suite in lockstep (currently 2 x 5 = 10). Because collection is deterministic by
    default, every runner hits the exact same Cognite CDF endpoints simultaneously, causing high peak
    traffic, increasing the chance for rate-limit errors on shared resources.

    How this function works:
    - Deterministic per matrix job: Generates a seed based on GITHUB_RUN_ID, GITHUB_JOB,
      RUNNER_OS, and the Python version. This forces each matrix runner (e.g., Linux 3.10
      and Windows 3.14) to run test files in a completely different order.
    - xdist-safe: All 8 workers inside the SAME matrix runner calculate the exact same
      seed, preserving `--dist loadscope` scheduling efficiency.
    - Module-level only: Shuffles test files while preserving the internal test order
      within each file, protecting sequential tests and module-scoped fixtures.

    When running tests locally this logic is skipped entirely. There's no "matrix parallelism",
    so just using the standard & predictable test ordering makes debugging easier.
    """
    if os.getenv("GITHUB_ACTIONS") != "true":
        return

    # Build a seed unique to each matrix runner using Github Actions env vars + the Python runtime:
    seed_parts = (
        os.getenv("GITHUB_RUN_ID", ""),  # Differentiates workflow runs
        os.getenv("GITHUB_RUN_ATTEMPT", ""),  # Differentiates workflow retries
        os.getenv("GITHUB_JOB", ""),  # Job ID defined in your workflow file
        os.getenv("RUNNER_OS", ""),  # Operating system (Linux/Windows)
        platform.python_version(),
    )
    rng = random.Random(":".join(seed_parts))

    # We need to group all tests by their file paths:
    modules: defaultdict[Path, list[pytest.Item]] = defaultdict(list)
    for item in items:
        modules[item.path].append(item)

    shuffled_modules = list(modules.keys())
    rng.shuffle(shuffled_modules)

    # We need to mutate 'items' in-place with our new ordering:
    items.clear()
    items.extend(item for module in shuffled_modules for item in modules[module])


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    apply_coredeps_skips(config, items)
    if items:
        shuffle_test_modules(items)
