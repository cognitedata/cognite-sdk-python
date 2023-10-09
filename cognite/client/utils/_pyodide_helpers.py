from __future__ import annotations

import os
import warnings
from typing import TYPE_CHECKING, Any, Callable, MutableMapping

import cognite.client as cc  # Do not import individual entities
from cognite.client._http_client import _RetryTracker
from cognite.client.config import ClientConfig
from cognite.client.credentials import CredentialProvider

if TYPE_CHECKING:
    from requests import Session

    from cognite.client._http_client import HTTPClient, HTTPClientConfig


def patch_sdk_for_pyodide() -> None:
    # -------------------
    # Patch Pyodide related issues
    # - Patch 'requests' as it does not work in pyodide (socket not implemented):
    from pyodide_http import patch_all

    patch_all()

    # -----------------
    # Patch Cognite SDK
    # - For good measure ;)
    os.environ["COGNITE_DISABLE_PYPI_VERSION_CHECK"] = "1"

    # - Disable gzip, not supported:
    cc.config.global_config.disable_gzip = True

    # - Use another HTTP adapter:
    cc._http_client.HTTPClient._old__init__ = cc._http_client.HTTPClient.__init__  # type: ignore [attr-defined]
    cc._http_client.HTTPClient.__init__ = http_client__init__  # type: ignore [method-assign]

    # - Inject these magic classes into the correct modules so that the user may import them normally:
    cc.config.FusionNotebookConfig = FusionNotebookConfig  # type: ignore [attr-defined]

    # - Set all usage of thread pool executors to use dummy/serial-implementations:
    cc.utils._concurrency.ConcurrencySettings.executor_type = "mainthread"
    cc.utils._concurrency.ConcurrencySettings.priority_executor_type = "mainthread"

    # - Auto-ignore protobuf warning for the user (as they can't fix this):
    warnings.filterwarnings(
        action="ignore",
        category=UserWarning,
        message="Your installation of 'protobuf' is missing compiled C binaries",
    )

    # - If we are running inside of a JupyterLite Notebook spawned from Cognite Data Fusion, we set
    #   the default config to FusionNotebookConfig(). This allows the user to:
    #   >>> from cognite.client import CogniteClient
    #   >>> client = CogniteClient()
    if os.getenv("COGNITE_FUSION_NOTEBOOK") is not None:
        cc.config.global_config.default_client_config = FusionNotebookConfig()


def http_client__init__(
    self: HTTPClient,
    config: HTTPClientConfig,
    session: Session,
    refresh_auth_header: Callable[[MutableMapping[str, Any]], None],
    retry_tracker_factory: Callable[[HTTPClientConfig], _RetryTracker] = _RetryTracker,
) -> None:
    import pyodide_http

    self._old__init__(config, session, refresh_auth_header, retry_tracker_factory)  # type: ignore [attr-defined]
    self.session.mount("https://", pyodide_http._requests.PyodideHTTPAdapter())
    self.session.mount("http://", pyodide_http._requests.PyodideHTTPAdapter())


class EnvVarToken(CredentialProvider):
    """Credential provider that always reads token from an environment variable just-in-time,
    allowing refreshing the value by another entity.
    Args:
        key (str): The name of the env.var. to read from. Default: 'COGNITE_TOKEN'
    Raises:
        KeyError: If the env.var. is not set.
    """

    def __init__(self, key: str = "COGNITE_TOKEN") -> None:
        self.key = key

    def __token_factory(self) -> str:
        return os.environ[self.key]

    def authorization_header(self) -> tuple[str, str]:
        return "Authorization", f"Bearer {self.__token_factory()}"


class FusionNotebookConfig(ClientConfig):
    def __init__(
        self,
        client_name: str = "DSHubLite",
        api_subversion: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: int | None = None,
        file_transfer_timeout: int | None = None,
        debug: bool = False,
    ) -> None:
        super().__init__(
            client_name=client_name,
            api_subversion=api_subversion,
            headers=headers,
            timeout=timeout,
            file_transfer_timeout=file_transfer_timeout,
            debug=debug,
            project=os.environ["COGNITE_PROJECT"],
            credentials=EnvVarToken(),  # Magic!
            base_url=os.environ["COGNITE_BASE_URL"],
            max_workers=1,
        )
