from __future__ import annotations

import contextlib
import functools
import inspect
import json
import os
import warnings
from base64 import urlsafe_b64decode
from concurrent.futures import CancelledError
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, Optional, Tuple

from msal import PublicClientApplication

import cognite.client as cc
from cognite.client._http_client import _RetryTracker
from cognite.client.config import ClientConfig
from cognite.client.credentials import CredentialProvider, OAuthInteractive
from cognite.client.utils._concurrency import T_Result, TaskExecutor, TaskFuture

if TYPE_CHECKING:
    from types import TracebackType

    from requests import PreparedRequest, Response, Session

    from cognite.client._http_client import HTTPClient, HTTPClientConfig


def patch_sdk_for_pyodide() -> None:
    # -------------------
    # Patch Pyodide stuff
    # - Patch 'requests' as it does not work in pyodide (socket not implemented):
    import pyodide_http  # type: ignore [import]

    pyodide_http.patch_all()

    # - Patch the patched requests (from pyodide-http) as the SDK expects responses to have '.raw' attr:
    pyo_http = pyodide_http._requests.PyodideHTTPAdapter
    pyo_http._old_send = pyo_http.send
    pyo_http.send = pyodide_http_adapter_send

    # -----------------
    # Patch Cognite SDK
    # - For good measure ;)
    os.environ["COGNITE_DISABLE_PYPI_VERSION_CHECK"] = "1"

    # - Disable gzip, not supported:
    cc.config.global_config.disable_gzip = True

    # - Use another HTTP adapter:
    cc._http_client.HTTPClient._old__init__ = cc._http_client.HTTPClient.__init__  # type: ignore [attr-defined]
    cc._http_client.HTTPClient.__init__ = http_client__init__  # type: ignore [assignment]

    # - If we are running inside of a JupyterLite Notebook spawned from Cognite Data Fusion, we set
    #   the default config to FusionNotebookConfig(). This allows the user to:
    #   >>> from cognite.client import CogniteClient
    #   >>> client = CogniteClient()
    if os.getenv("COGNITE_FUSION_NOTEBOOK") is not None:
        cc.config.global_config.default_client_config = FusionNotebookConfig()

    # - Inject these magic classes into the correct modules so that the user may import them normally:
    cc.config.FusionNotebookConfig = FusionNotebookConfig  # type: ignore [attr-defined]
    # TODO: Add once token refresh is working:
    # cc.credentials.OAuthInteractiveFusionNotebook = OAuthInteractiveFusionNotebook  # type: ignore [attr-defined]

    # - Set all usage of ThreadPoolExecutor to use a dummy, serial-implementation:
    cc.utils._concurrency.ConcurrencySettings.executor_type = "mainthread"

    # - Auto-ignore protobuf warning for the user (as they can't fix this):
    warnings.filterwarnings(
        action="ignore",
        category=UserWarning,
        message="Your installation of 'protobuf' is missing compiled C binaries",
    )
    # - AssetsAPI.create_hierarchy is not implemented yet (custom threading solution)
    cc._api.assets._AssetPoster = NotImplementedAssetPoster  # type: ignore [assignment, misc]

    # - DatapointsAPI.retrieve/retrieve_arrays/retrieve_dataframe:
    cc._api.datapoints.PriorityThreadPoolExecutor = SerialPriorityThreadPoolExecutor  # type: ignore [assignment, misc]
    cc._api.datapoints.as_completed = serial_as_completed  # type: ignore [assignment]


def pyodide_http_adapter_send(self: Any, request: PreparedRequest, **kwargs: Any) -> Response:
    # 'self' is actually `pyodide_http._requests.PyodideHTTPAdapter`, but we don't want it as a
    # dependency just for the sake of mypy; after all, we are patching like monkeys 🙈
    (response := self._old_send(request, **kwargs)).raw.version = ""
    return response


def http_client__init__(
    self: HTTPClient,
    config: HTTPClientConfig,
    session: Session,
    retry_tracker_factory: Callable[[HTTPClientConfig], _RetryTracker] = _RetryTracker,
) -> None:
    import pyodide_http  # type: ignore [import]

    self._old__init__(config, session, retry_tracker_factory)  # type: ignore [attr-defined]
    self.session.mount("https://", pyodide_http._requests.PyodideHTTPAdapter())
    self.session.mount("http://", pyodide_http._requests.PyodideHTTPAdapter())


class NotImplementedAssetPoster:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("AssetsAPI.create_hierarchy is not pyodide/web-browser compatible yet! Stay tuned!")


class DummyFuture(TaskFuture):
    def __init__(self, fn: Callable[..., T_Result], *args: Any, **kwargs: Any):
        self._task = functools.partial(fn, *args, **kwargs)
        self._result: Optional[T_Result] = None
        self._is_cancelled = False

    def result(self) -> T_Result:
        if self._is_cancelled:
            raise CancelledError
        if self._result is None:
            self._result = self._task()
        return self._result

    def cancel(self) -> None:
        self._is_cancelled = True


def serial_as_completed(dct: Dict[DummyFuture, Any]) -> Iterator[DummyFuture]:
    return iter(dct.copy())  # Will raise StopIteration when done (we want this):


class SerialPriorityThreadPoolExecutor(TaskExecutor):
    def __init__(self, max_workers: int = None) -> None:
        if max_workers != 1:
            raise RuntimeError("Max_workers must be == -e^(i*pi), or 1 if you wondered")

    def submit(self, fn: Callable[..., T_Result], *args: Any, **kwargs: Any) -> DummyFuture:
        if "priority" in inspect.signature(fn).parameters:
            raise TypeError(f"Given function {fn} cannot accept reserved parameter name `priority`")
        kwargs.pop("priority", None)
        return DummyFuture(fn, *args, **kwargs)

    def shutdown(self, wait: bool = False) -> None:
        return None

    def __enter__(self) -> SerialPriorityThreadPoolExecutor:
        return self

    def __exit__(
        self,
        type: Optional[type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.shutdown()


class EnvVarToken(CredentialProvider):
    """Credential provider that always reads token from an environment variable just-in-time,
    allowing refreshing the value by another entity.
    Args:
        key (str | None): The name of the env.var. to read from. Default: 'COGNITE_TOKEN'
    Raises:
        KeyError: If the env.var. is not set.
    """

    def __init__(self, key: str = None) -> None:
        self.key = "COGNITE_TOKEN" if key is None else key

    def __token_factory(self) -> str:
        return os.environ[self.key]

    def authorization_header(self) -> Tuple[str, str]:
        return "Authorization", f"Bearer {self.__token_factory()}"


class OAuthInteractiveFusionNotebook(OAuthInteractive):
    # TODO: PublicClientApplication is broken in pyodide because of requests?
    def __init__(self, redirect_port: int = 53000) -> None:
        token, base_url = os.environ["COGNITE_TOKEN"], os.environ["COGNITE_BASE_URL"]
        payload = self._load_payload_from_jwt(token)
        client_id = payload["appid"]
        authority_url = f"https://login.microsoftonline.com/{payload['tid']}"

        # Note: Death to name mangling:
        self._OAuthCredentialProviderWithTokenRefresh__access_token = token
        self._OAuthCredentialProviderWithTokenRefresh__token_refresh_lock = contextlib.nullcontext()
        self._OAuthCredentialProviderWithTokenRefresh__access_token_expires_at = payload["exp"]

        self._OAuthInteractive__authority_url = authority_url
        self._OAuthInteractive__client_id = client_id
        self._OAuthInteractive__scopes = [f"{base_url}/.default"]
        self._OAuthInteractive__redirect_port = redirect_port
        self._OAuthInteractive__app = PublicClientApplication(client_id=client_id, authority=authority_url)

    @staticmethod
    def _load_payload_from_jwt(token: str) -> Dict[str, Any]:
        """A JWT has the structure: `<header>.<payload>.<signature>`. This method returns payload as a dict"""
        _, payload, _ = token.split(".")
        if remainder := len(payload_bytes := payload.encode()) % 4:
            payload_bytes += b"=" * (4 - remainder)
        return json.loads(urlsafe_b64decode(payload_bytes))

    @staticmethod
    def _create_serializable_token_cache(cache_path: Path) -> None:
        return None


class FusionNotebookConfig(ClientConfig):
    def __init__(
        self,
        client_name: str = "DSHubLite",
        api_subversion: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        file_transfer_timeout: Optional[int] = None,
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
            # TODO: Fix 'PublicClientApplication' for token refresh:
            # credentials=OAuthInteractiveFusionNotebook(),  # Magic! 🪄
            credentials=EnvVarToken(),  # Less magical, but still!
            base_url=os.environ["COGNITE_BASE_URL"],
            max_workers=1,
        )
