from __future__ import annotations

import email.parser
import os
from collections.abc import AsyncIterator, Awaitable, Iterable, Iterator
from types import TracebackType
from typing import TYPE_CHECKING, TypeAlias, TypeVar

try:
    import js  # type: ignore [import-not-found]
    import pyodide  # type: ignore [import-not-found]
except ModuleNotFoundError:
    pass

from httpx import (
    AsyncBaseTransport,
    AsyncByteStream,
    BaseTransport,
    ConnectError,
    ConnectTimeout,
    Limits,
    ReadError,
    ReadTimeout,
    Request,
    RequestError,
    Response,
    SyncByteStream,
)

try:
    from httpx._config import DEFAULT_LIMITS
except ImportError:
    DEFAULT_LIMITS = Limits(max_connections=100, max_keepalive_connections=20)

import cognite.client as cc  # Do not import individual entities
from cognite.client.config import ClientConfig, global_config
from cognite.client.credentials import CredentialProvider

if TYPE_CHECKING:
    import ssl

    try:
        from httpx._types import CertTypes, ProxyTypes
    except ImportError:
        from httpx import URL, Proxy

        ProxyTypes: TypeAlias = URL | str | Proxy  # type: ignore [misc, no-redef]
        CertTypes: TypeAlias = str | tuple[str, str] | tuple[str, str, str]  # type: ignore [misc, no-redef]


def patch_sdk_for_pyodide() -> None:
    # -------------------
    # Patch Pyodide related issues
    # - Patch 'httpx' as it does not work natively in emscripten/pyodide, by using a special transport:
    cc._http_client._get_default_transport = _get_default_transport_pyodide

    # -----------------
    # Patch Cognite SDK
    # - For good measure ;)
    global_config.disable_pypi_version_check = True

    # - Disable gzip, not supported:
    # TODO: Are we blocking ourselves here? Error from the browser seem to indicate so, example:
    # Access to XMLHttpRequest at 'https://greenfield.cognitedata.com/api/v1/projects/forge-sandbox/assets/list'
    # from origin 'https://notebook-standalone.cogniteapp.com' has been blocked by CORS policy: Request header
    # field content-encoding is not allowed by Access-Control-Allow-Headers in preflight response.
    global_config.disable_gzip = True

    # - Inject these magic classes into the correct modules so that the user may import them normally:
    cc.config.FusionNotebookConfig = FusionNotebookConfig  # type: ignore [attr-defined]

    # - Set all usage of thread pool executors to use dummy/serial-implementations:
    # TODO: With httpx.AsyncClient added later, can we make use of the browser event loop?
    cc.utils._concurrency.ConcurrencySettings.executor_type = "mainthread"

    # - If we are running inside of a JupyterLite Notebook spawned from Cognite Data Fusion, we set
    #   the default config to FusionNotebookConfig(). This allows the user to:
    #   >>> from cognite.client import CogniteClient
    #   >>> client = CogniteClient()
    if os.getenv("COGNITE_FUSION_NOTEBOOK") is not None:
        global_config.default_client_config = FusionNotebookConfig()


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
        )


def _get_default_transport_pyodide() -> BaseTransport:
    return JavascriptFetchTransport()


# -------------------
# The below sync- and async transports for httpx are copied from: https://github.com/encode/httpx/pull/3330
# The transports are modified to work with Emscripten.
# -------------------
# Custom transport for Pyodide on Emscripten.
# In sync mode it relies on use of the Javascript Promise Integration
# feature which is currently experimental in webassembly and only works
# in some places. Specifically in chrome JSPI is behind either a flag or an
# origin trial, in node 20 or newer you need the --experimental-wasm-stack-switching
# flag. Firefox is not currently supported.
# See https://github.com/WebAssembly/js-promise-integration/
# In async mode it uses the standard fetch api, which should work
# anywhere that pyodide works.

T = TypeVar("T", bound="JavascriptFetchTransport")
A = TypeVar("A", bound="AsyncJavascriptFetchTransport")

SOCKET_OPTION = tuple[int, int, int] | tuple[int, int, bytes | bytearray] | tuple[int, int, None, int]


# There are some headers that trigger unintended CORS preflight requests.
# See also https://github.com/koenvo/pyodide-http/issues/22
HEADERS_TO_IGNORE = ("user-agent",)


def _run_sync_with_timeout(
    promise: Awaitable[pyodide.ffi.JsProxy],
    timeout: float,
    abort_controller_js: pyodide.ffi.JsProxy,
    TimeoutExceptionType: type[RequestError],
    ErrorExceptionType: type[RequestError],
) -> pyodide.ffi.JsProxy:
    """await a javascript promise synchronously with a timeout set via the
        AbortController and return the resulting javascript proxy

    Args:
        promise (Awaitable[pyodide.ffi.JsProxy]): Javascript promise to await
        timeout (float): Timeout in seconds
        abort_controller_js (pyodide.ffi.JsProxy): A javascript AbortController object, used on timeout
        TimeoutExceptionType (type[RequestError]): An exception type to raise on timeout
        ErrorExceptionType (type[RequestError]): An exception type to raise on error

    Raises:
        TimeoutExceptionType: If the request times out
        ErrorExceptionType: If the request raises a Javascript exception

    Returns:
        pyodide.ffi.JsProxy: The result of awaiting the promise.
    """
    timer_id = None
    if timeout > 0:
        timer_id = js.setTimeout(abort_controller_js.abort.bind(abort_controller_js), int(timeout * 1000))
    try:
        from pyodide.ffi import run_sync  # type: ignore [import-not-found]

        # run_sync here uses WebAssembly Javascript Promise Integration to
        # suspend python until the Javascript promise resolves.
        return run_sync(promise)
    except pyodide.ffi.JsException as err:
        if err.name == "AbortError":
            raise TimeoutExceptionType(message="Request timed out")
            timer_id = None
        else:
            raise ErrorExceptionType(message=err.message)
    finally:
        if timer_id is not None:
            js.clearTimeout(timer_id)


async def _run_async_with_timeout(
    promise: Awaitable[pyodide.ffi.JsProxy],
    timeout: float,
    abort_controller_js: pyodide.ffi.JsProxy,
    TimeoutExceptionType: type[RequestError],
    ErrorExceptionType: type[RequestError],
) -> pyodide.ffi.JsProxy:
    """await a javascript promise asynchronously with a timeout set via the AbortController

    Args:
        promise (Awaitable[pyodide.ffi.JsProxy]): Javascript promise to await
        timeout (float): Timeout in seconds
        abort_controller_js (pyodide.ffi.JsProxy): A javascript AbortController object, used on timeout
        TimeoutExceptionType (type[RequestError]): An exception type to raise on timeout
        ErrorExceptionType (type[RequestError]): An exception type to raise on error

    Raises:
        TimeoutException: If the request times out
        NetworkError: If the request raises a Javascript exception

    Returns:
        pyodide.ffi.JsProxy: The result of awaiting the promise.
    """
    timer_id = None
    if timeout > 0:
        timer_id = js.setTimeout(abort_controller_js.abort.bind(abort_controller_js), int(timeout * 1000))
    try:
        return await promise
    except pyodide.ffi.JsException as err:
        if err.name == "AbortError":
            raise TimeoutExceptionType(message="Request timed out")
            timer_id = None
        else:
            raise ErrorExceptionType(message=err.message)
    finally:
        if timer_id is not None:
            js.clearTimeout(timer_id)


class EmscriptenStream(SyncByteStream):
    def __init__(
        self,
        response_stream_js: pyodide.ffi.JsProxy,
        timeout: float,
        abort_controller_js: pyodide.ffi.JsProxy,
    ) -> None:
        self._stream_js = response_stream_js
        self.timeout = timeout
        self.abort_controller_js = abort_controller_js

    def __iter__(self) -> Iterator[bytes]:
        while True:
            result_js = _run_sync_with_timeout(
                self._stream_js.read(),
                self.timeout,
                self.abort_controller_js,
                ReadTimeout,
                ReadError,
            )
            if result_js.done:
                return
            else:
                this_buffer = result_js.value.to_py()
                yield this_buffer

    def close(self) -> None:
        self._stream_js = None


class JavascriptFetchTransport(BaseTransport):
    def __init__(
        self,
        verify: ssl.SSLContext | str | bool = True,
        cert: CertTypes | None = None,
        trust_env: bool = True,
        http1: bool = True,
        http2: bool = False,
        limits: Limits = DEFAULT_LIMITS,
        proxy: ProxyTypes | None = None,
        uds: str | None = None,
        local_address: str | None = None,
        retries: int = 0,
        socket_options: Iterable[SOCKET_OPTION] | None = None,
    ) -> None:
        pass

    def __enter__(self: T) -> T:  # Use generics for subclass support.
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        pass

    def handle_request(
        self,
        request: Request,
    ) -> Response:
        assert isinstance(request.stream, SyncByteStream)
        if not self._can_use_jspi():
            return self._no_jspi_fallback(request)
        req_body: bytes | None = b"".join(request.stream)
        if req_body is not None and len(req_body) == 0:
            req_body = None
        conn_timeout = 0.0
        read_timeout = 0.0
        if "timeout" in request.extensions:
            timeout_dict = request.extensions["timeout"]
            if timeout_dict is not None:
                if "connect" in timeout_dict:
                    conn_timeout = timeout_dict["connect"]
                if "read" in timeout_dict:
                    read_timeout = timeout_dict["connect"]
        abort_controller_js = js.AbortController.new()
        headers = {k: v for k, v in request.headers.items() if k not in HEADERS_TO_IGNORE}
        fetch_data = {
            "headers": headers,
            "body": pyodide.ffi.to_js(req_body),
            "method": request.method,
            "signal": abort_controller_js.signal,
        }

        fetcher_promise_js = js.fetch(
            request.url,
            pyodide.ffi.to_js(fetch_data, dict_converter=js.Object.fromEntries),
        )

        response_js = _run_sync_with_timeout(
            fetcher_promise_js,
            conn_timeout,
            abort_controller_js,
            ConnectTimeout,
            ConnectError,
        )

        headers = {}
        header_iter = response_js.headers.entries()
        while True:
            iter_value_js = header_iter.next()
            if getattr(iter_value_js, "done", False):
                break
            else:
                headers[str(iter_value_js.value[0])] = str(iter_value_js.value[1])
        # fix content-encoding headers because the javascript fetch handles that
        headers["content-encoding"] = "identity"
        status_code = response_js.status

        # get a reader from the fetch response
        body_stream_js = response_js.body.getReader()
        return Response(
            status_code=status_code,
            headers=headers,
            stream=EmscriptenStream(body_stream_js, read_timeout, abort_controller_js),
        )

    def _can_use_jspi(self) -> bool:
        """Returns true if the pyodide environment allows for use
        of synchronous javascript promise calls. If not we have to
        fall back to the browser XMLHttpRequest api.
        """
        # Ignore this next if statement from coverage because only one part
        # will be run depending on the pyodide version
        if hasattr(pyodide.ffi, "can_run_sync"):
            return bool(pyodide.ffi.can_run_sync())  # pragma: no cover
        else:
            from pyodide_js._module import validSuspender  # type: ignore [import-not-found]

            return bool(validSuspender.value)  # pragma: no cover

    def _is_in_browser_main_thread(self) -> bool:
        return hasattr(js, "window") and hasattr(js, "self") and js.self == js.window

    def _no_jspi_fallback(self, request: Request) -> Response:
        assert isinstance(request.stream, SyncByteStream)
        try:
            js_xhr = js.XMLHttpRequest.new()

            req_body: bytes | None = b"".join(request.stream)
            if req_body is not None and len(req_body) == 0:
                req_body = None

            timeout = 0.0
            if "timeout" in request.extensions:
                timeout_dict = request.extensions["timeout"]
                if timeout_dict is not None:
                    if "connect" in timeout_dict:
                        timeout = timeout_dict["connect"]
                    if "read" in timeout_dict:
                        timeout = timeout_dict["connect"]

            # XHMLHttpRequest only supports timeouts and proper
            # binary file reading in web-workers
            if not self._is_in_browser_main_thread():
                js_xhr.responseType = "arraybuffer"
                if timeout > 0.0:
                    js_xhr.timeout = int(timeout * 1000)
            else:
                # this is a nasty hack to be able to read binary files on
                # main browser thread using xmlhttprequest
                js_xhr.overrideMimeType("text/plain; charset=ISO-8859-15")

            js_xhr.open(request.method, request.url, False)

            for name, value in request.headers.items():
                if name.lower() not in HEADERS_TO_IGNORE:
                    js_xhr.setRequestHeader(name, value)

            js_xhr.send(pyodide.ffi.to_js(req_body))

            headers = dict(email.parser.Parser().parsestr(js_xhr.getAllResponseHeaders()))

            if not self._is_in_browser_main_thread():
                body = js_xhr.response.to_py().tobytes()
            else:
                body = js_xhr.response.encode("ISO-8859-15")

            return Response(status_code=js_xhr.status, headers=headers, content=body)
        except pyodide.ffi.JsException as err:
            if err.name == "TimeoutError":
                raise ConnectTimeout(message="Request timed out")
            else:
                raise ConnectError(message=err.message)

    def close(self) -> None:
        pass  # pragma: nocover


class AsyncEmscriptenStream(AsyncByteStream):
    def __init__(
        self,
        response_stream_js: pyodide.ffi.JsProxy,
        timeout: float,
        abort_controller_js: pyodide.ffi.JsProxy,
    ) -> None:
        self._stream_js = response_stream_js
        self.timeout = timeout
        self.abort_controller_js = abort_controller_js

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while self._stream_js is not None:
            result_js = await _run_async_with_timeout(
                self._stream_js.read(),
                self.timeout,
                self.abort_controller_js,
                ReadTimeout,
                ReadError,
            )
            if result_js.done:
                return
            else:
                this_buffer = result_js.value.to_py()
                yield this_buffer

    async def aclose(self) -> None:
        self._stream_js = None


class AsyncJavascriptFetchTransport(AsyncBaseTransport):
    def __init__(
        self,
        verify: ssl.SSLContext | str | bool = True,
        cert: CertTypes | None = None,
        trust_env: bool = True,
        http1: bool = True,
        http2: bool = False,
        limits: Limits = DEFAULT_LIMITS,
        proxy: ProxyTypes | None = None,
        uds: str | None = None,
        local_address: str | None = None,
        retries: int = 0,
        socket_options: Iterable[SOCKET_OPTION] | None = None,
    ) -> None:
        pass

    async def __aenter__(self: A) -> A:  # Use generics for subclass support.
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        pass

    async def handle_async_request(
        self,
        request: Request,
    ) -> Response:
        assert isinstance(request.stream, AsyncByteStream)
        body_data: bytes = b""
        async for x in request.stream:
            body_data += x
        if len(body_data) == 0:
            req_body = None
        else:
            req_body = body_data
        conn_timeout = 0.0
        read_timeout = 0.0
        if "timeout" in request.extensions:
            timeout_dict = request.extensions["timeout"]
            if timeout_dict is not None:
                if "connect" in timeout_dict:
                    conn_timeout = timeout_dict["connect"]
                if "read" in timeout_dict:
                    read_timeout = timeout_dict["connect"]

        abort_controller_js = js.AbortController.new()
        headers = {k: v for k, v in request.headers.items() if k not in HEADERS_TO_IGNORE}
        fetch_data = {
            "headers": headers,
            "body": pyodide.ffi.to_js(req_body),
            "method": request.method,
            "signal": abort_controller_js.signal,
        }

        fetcher_promise_js = js.fetch(
            request.url,
            pyodide.ffi.to_js(fetch_data, dict_converter=js.Object.fromEntries),
        )
        response_js = await _run_async_with_timeout(
            fetcher_promise_js,
            conn_timeout,
            abort_controller_js,
            ConnectTimeout,
            ConnectError,
        )

        headers = {}
        header_iter = response_js.headers.entries()
        while True:
            iter_value_js = header_iter.next()
            if getattr(iter_value_js, "done", False):
                break
            else:
                headers[str(iter_value_js.value[0])] = str(iter_value_js.value[1])
        status_code = response_js.status

        # get a reader from the fetch response
        body_stream_js = response_js.body.getReader()
        return Response(
            status_code=status_code,
            headers=headers,
            stream=AsyncEmscriptenStream(body_stream_js, read_timeout, abort_controller_js),
        )

    async def aclose(self) -> None:
        pass  # pragma: nocover
