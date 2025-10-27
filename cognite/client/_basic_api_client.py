from __future__ import annotations

import functools
import gzip
import logging
import platform
from collections.abc import Iterable, Iterator, MutableMapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, NoReturn, cast

import httpx
from typing_extensions import Self

from cognite.client._http_client import HTTPClientWithRetry, HTTPClientWithRetryConfig
from cognite.client.config import global_config
from cognite.client.exceptions import (
    CogniteAPIError,
    CogniteConnectionError,
    CogniteDuplicatedError,
    CogniteNotFoundError,
    CogniteProjectAccessError,
    CogniteRequestError,
)
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import drop_none_values
from cognite.client.utils._text import shorten
from cognite.client.utils._url import resolve_url

if TYPE_CHECKING:
    from cognite.client._cognite_client import CogniteClient
    from cognite.client.config import ClientConfig


logger = logging.getLogger(__name__)


@dataclass
class FailedRequestHandler:
    message: str
    status_code: int
    missing: list[str] | None
    duplicated: list[str] | None
    x_request_id: str | None
    headers: dict[str, str] | httpx.Headers
    response_headers: dict[str, str] | httpx.Headers
    extra: dict[str, Any]
    cause: httpx.HTTPStatusError
    stream: bool

    def __post_init__(self) -> None:
        self.headers = BasicAPIClient._sanitize_headers(self.headers)
        self.response_headers = BasicAPIClient._sanitize_headers(self.response_headers)

    @classmethod
    async def from_status_error(cls, err: httpx.HTTPStatusError, stream: bool) -> Self:
        response = err.response
        error, missing, duplicated = {}, None, None

        if stream:
            await response.aread()
        try:
            error = response.json()["error"]
        except (_json.JSONDecodeError, KeyError):
            message = response.text
        else:
            match error:
                case str():
                    message = error
                case dict():
                    error.pop("code", None)  # some APIs also return status code here
                    message = error.pop("message")
                    missing = error.pop("missing", None) or None  # no empty lists wanted
                    duplicated = error.pop("duplicated", None) or None
                case _:
                    message = response.text

        return cls(
            message=message,
            status_code=response.status_code,
            missing=missing,
            duplicated=duplicated,
            x_request_id=response.headers.get("x-request-id"),
            headers=err.request.headers,
            response_headers=response.headers,
            extra=error,
            cause=err,
            stream=stream,
        )

    def log_failed_request(self, payload: dict | None = None) -> None:
        response, request = self.cause.response, self.cause.request
        extra: dict[str, Any] = {
            "payload": payload,
            "missing": self.missing,
            "duplicated": self.duplicated,
            "headers": self.headers,
            "response-headers": self.response_headers,
        }
        if not self.stream:
            extra["response-payload"] = shorten(response.text, 1_000)

        if response.history:
            for res_hist in response.history:
                logger.debug(
                    f"REDIRECT AFTER HTTP Error {res_hist.status_code} {res_hist.request.method} "
                    f"{res_hist.request.url}: {res_hist.text}"
                )
        logger.debug(
            f"HTTP Error {self.status_code} {request.method} {request.url}: {self.message}",
            extra=drop_none_values(extra),
        )

    def raise_api_error(self, cognite_client: CogniteClient) -> NoReturn:
        cluster = cognite_client._config.cdf_cluster
        project = cognite_client._config.project

        match self.status_code, self.duplicated, self.missing:
            case 401, *_:
                self._raise_no_project_access_error(cognite_client, cluster, project)
            case 409, list(), None:
                self._raise_api_error(CogniteDuplicatedError, cluster, project)
            case 400 | 422, None, list():
                self._raise_api_error(CogniteNotFoundError, cluster, project)
            case _:
                self._raise_api_error(CogniteAPIError, cluster, project)

    def _raise_no_project_access_error(
        self, cognite_client: CogniteClient, cluster: str | None, project: str
    ) -> NoReturn:
        raise CogniteProjectAccessError(
            client=cognite_client,
            project=project,
            x_request_id=self.x_request_id,
            cluster=cluster,
        ) from None  # we don't surface the underlying httpx.HTTPStatusError

    def _raise_api_error(self, err_type: type[CogniteAPIError], cluster: str | None, project: str) -> NoReturn:
        raise err_type(
            message=self.message,
            code=self.status_code,
            x_request_id=self.x_request_id,
            missing=self.missing,
            duplicated=self.duplicated,
            extra=self.extra,
            cluster=cluster,
            project=project,
        ) from None


@functools.cache
def get_user_agent() -> str:
    from cognite.client import __version__

    try:
        from httpx._client import USER_AGENT
    except ImportError:
        USER_AGENT = "python-httpx/<unknown>"

    sdk_version = f"CognitePythonSDK/{__version__}"
    python_version = (
        f"{platform.python_implementation()}/{platform.python_version()} "
        f"({platform.python_build()};{platform.python_compiler()})"
    )
    os_version_info = [platform.release(), platform.machine(), platform.architecture()[0]]
    operating_system = f"{platform.system()}/{'-'.join(s for s in os_version_info if s)}"

    return f"{USER_AGENT} {sdk_version} {python_version} {operating_system}"


class BasicAPIClient:
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        self._config = config
        self._api_version = api_version
        self._api_subversion = config.api_subversion
        self._cognite_client = cognite_client
        self._init_http_clients()

        self._CREATE_LIMIT = 1000
        self._LIST_LIMIT = 1000
        self._RETRIEVE_LIMIT = 1000
        self._DELETE_LIMIT = 1000
        self._UPDATE_LIMIT = 1000

    def _init_http_clients(self) -> None:
        self._http_client = HTTPClientWithRetry(
            config=HTTPClientWithRetryConfig(status_codes_to_retry={429}, max_retries_read=0),
            refresh_auth_header=self._refresh_auth_header,
        )
        self._http_client_with_retry = HTTPClientWithRetry(
            config=HTTPClientWithRetryConfig(),
            refresh_auth_header=self._refresh_auth_header,
        )

    def _select_http_client(self, is_retryable: bool) -> HTTPClientWithRetry:
        return self._http_client_with_retry if is_retryable else self._http_client

    def _request(
        self,
        method: Literal["GET", "PUT", "HEAD"],
        /,
        full_url: str,
        content: str | bytes | Iterable[bytes] | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        """Make a request to something that is outside Cognite Data Fusion"""
        client = self._select_http_client(method in {"GET", "PUT", "HEAD"})
        try:
            res = client(method, full_url, content=content, headers=headers, timeout=timeout or self._config.timeout)
        except httpx.HTTPStatusError as err:
            self._handle_status_error(err)

        self._log_successful_request(res)
        return res

    @contextmanager
    def _stream(
        self,
        method: Literal["GET", "POST"],
        /,
        *,
        url_path: str | None = None,
        full_url: str | None = None,
        json: Any = None,
        headers: dict[str, Any] | None = None,
        full_headers: dict[str, Any] | None = None,
        timeout: float | None = None,
        api_subversion: str | None = None,
    ) -> Iterator[httpx.Response]:
        assert url_path or full_url
        full_url = full_url or resolve_url("GET", cast(str, url_path), self._api_version, self._config)[1]
        if full_headers is None:
            full_headers = self._configure_headers(headers, api_subversion)
        try:
            with self._http_client_with_retry.stream(
                method, full_url, json=json, headers=full_headers, timeout=timeout or self._config.timeout
            ) as resp:
                try:
                    resp.raise_for_status()
                except httpx.HTTPStatusError as err:
                    self._handle_status_error(err, stream=True)
                else:
                    self._log_successful_request(resp, stream=True)
                    yield resp
        except (httpx.NetworkError, httpx.DecodingError) as err:
            raise CogniteConnectionError(err)
        except httpx.RequestError as err:
            raise CogniteRequestError from err

    def _get(
        self,
        url_path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        follow_redirects: bool = False,
        api_subversion: str | None = None,
    ) -> httpx.Response:
        _, full_url = resolve_url("GET", url_path, self._api_version, self._config)
        full_headers = self._configure_headers(additional_headers=headers, api_subversion=api_subversion)
        try:
            res = self._http_client_with_retry(
                "GET",
                full_url,
                params=params,
                headers=full_headers,
                follow_redirects=follow_redirects,
                timeout=self._config.timeout,
            )
        except httpx.HTTPStatusError as err:
            self._handle_status_error(err)

        self._log_successful_request(res)
        return res

    def _post(
        self,
        url_path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        follow_redirects: bool = False,
        api_subversion: str | None = None,
    ) -> httpx.Response:
        is_retryable, full_url = resolve_url("POST", url_path, self._api_version, self._config)
        full_headers = self._configure_headers(additional_headers=headers, api_subversion=api_subversion)
        # We want to control json dumping, so we pass it along to httpx.Client.post as 'content'
        content = self._handle_json_dump(json, full_headers)

        http_client = self._select_http_client(is_retryable)
        try:
            res = http_client(
                "POST",
                full_url,
                content=content,
                params=params,
                headers=full_headers,
                follow_redirects=follow_redirects,
                timeout=self._config.timeout,
            )
        except httpx.HTTPStatusError as err:
            self._handle_status_error(err)

        self._log_successful_request(res, payload=json)
        return res

    def _put(
        self,
        url_path: str,
        content: str | bytes | Iterable[bytes] | None = None,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        follow_redirects: bool = False,
        api_subversion: str | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        _, full_url = resolve_url("PUT", url_path, self._api_version, self._config)
        full_headers = self._configure_headers(additional_headers=headers, api_subversion=api_subversion)
        if content is None:
            content = self._handle_json_dump(json, full_headers)

        try:
            res = self._http_client_with_retry(
                "PUT",
                full_url,
                content=content,
                params=params,
                headers=full_headers,
                follow_redirects=follow_redirects,
                timeout=timeout or self._config.timeout,
            )
        except httpx.HTTPStatusError as err:
            self._handle_status_error(err)

        self._log_successful_request(res, payload=json)
        return res

    def _configure_headers(
        self, additional_headers: dict[str, str] | None, api_subversion: str | None
    ) -> dict[str, str]:
        from cognite.client import __version__

        headers = {
            "content-type": "application/json",
            "accept": "application/json",
            "x-cdp-sdk": "CognitePythonSDK:" + __version__,
            "x-cdp-app": self._config.client_name,
            "cdf-version": api_subversion or self._api_subversion,
            "user-agent": get_user_agent(),
            **self._config.headers,
            **(additional_headers or {}),
        }
        self._refresh_auth_header(headers)
        return headers

    def _refresh_auth_header(self, headers: MutableMapping[str, Any]) -> None:
        auth_header_name, auth_header_value = self._config.credentials.authorization_header()
        headers[auth_header_name] = auth_header_value

    async def _handle_status_error(
        self, error: httpx.HTTPStatusError, payload: dict | None = None, stream: bool = False
    ) -> NoReturn:
        """The response had an HTTP status code of 4xx or 5xx"""
        handler = await FailedRequestHandler.from_status_error(error, stream=stream)
        handler.log_failed_request(payload)
        handler.raise_api_error(self._cognite_client)

    def _log_successful_request(
        self, res: httpx.Response, payload: dict[str, Any] | None = None, stream: bool = False
    ) -> None:
        extra: dict[str, Any] = {
            "headers": self._sanitize_headers(res.request.headers),
            "payload": payload,
            "response-headers": dict(res.headers),
        }
        if not stream and self._config.debug:
            extra["response-payload"] = shorten(res.text, 1_000)
        try:
            http_protocol = res.http_version
        except AttributeError:
            # If this fails, it prob. means we are running in a browser (pyodide) with patched httpx package:
            http_protocol = "XMLHTTP"

        logger.debug(
            f"{http_protocol} {res.request.method} {res.url} {res.status_code}",
            extra=drop_none_values(extra),
        )

    @staticmethod
    def _handle_json_dump(json: dict[str, Any] | None, full_headers: MutableMapping[str, str]) -> bytes | str | None:
        if json is None:
            return None

        content = _json.dumps_no_nan_or_inf(json)
        if global_config.disable_gzip:
            return content

        full_headers["Content-Encoding"] = "gzip"
        return gzip.compress(content.encode())

    @staticmethod
    def _sanitize_headers(headers: httpx.Headers | dict[str, str]) -> dict[str, str]:
        sanitized = dict(headers)
        for k, v in sanitized.items():
            if k.lower() in {"authorization", "proxy-authorization"}:
                sanitized[k] = "***"
        return sanitized
