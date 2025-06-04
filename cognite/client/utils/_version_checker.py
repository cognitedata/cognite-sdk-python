from __future__ import annotations

import functools
import re
import warnings
from contextlib import suppress
from threading import Thread

import httpx
from packaging import version


def get_all_sdk_versions() -> list[version.Version]:
    from cognite.client.config import global_config

    verify_ssl = not global_config.disable_ssl
    # httpx uses 'verify' for SSL verification, similar to requests.
    # The default timeout for httpx is 5 seconds, matching the existing requests call.
    res = httpx.get("https://pypi.org/simple/cognite-sdk/", verify=verify_ssl, timeout=5)
    return list(map(version.parse, re.findall(r"cognite[_-]sdk-(\d+\.\d+.[\dabrc]+)", res.text)))


def get_latest_released_stable_version() -> version.Version:
    # Filter only stable versions (no pre-releases or dev releases, but post-releases are ok)
    return max(v for v in get_all_sdk_versions() if not v.is_prerelease)


# Wrap in a cache to ensure we only ever run the version check once.
@functools.cache
def check_client_is_running_latest_version() -> None:
    def run() -> None:
        from packaging import version

        from cognite.client import __version__

        with suppress(Exception):  # PyPI might be unreachable, if so, skip version check
            newest_version = get_latest_released_stable_version()
            if version.parse(__version__) < newest_version:
                warnings.warn(
                    f"You are using version={__version__!r} of the SDK, however version={newest_version.public!r} is "
                    "available. To suppress this warning, either upgrade or do the following:\n"
                    ">>> from cognite.client.config import global_config\n"
                    ">>> global_config.disable_pypi_version_check = True",
                    stacklevel=3,
                )

    Thread(target=run, daemon=True).start()
