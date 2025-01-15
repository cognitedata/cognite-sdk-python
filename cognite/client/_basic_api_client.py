from __future__ import annotations

import functools
import gzip
import logging
import platform
from collections.abc import Iterable, Iterator, MutableMapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, NoReturn

import httpx
from typing_extensions import Self

from cognite.client._http_client import HTTPClientWithRetry, HTTPClientWithRetryConfig
from cognite.client.config import global_config
from cognite.client.exceptions import (
    CogniteAPIError,
    CogniteDuplicatedError,
    CogniteNotFoundError,
    CogniteProjectAccessError,
)
from cognite.client.utils import _json
from cognite.client.utils._text import shorten
from cognite.client.utils._url import resolve_url

if TYPE_CHECKING:
    from cognite.client._cognite_client import CogniteClient
    from cognite.client.config import ClientConfig


logger = logging.getLogger(__name__)


@dataclass
class FailedRequestDetails:
    message: str
    status_code: int
    missing: list[str] | None
    duplicated: list[str] | None
    x_request_id: str | None
    headers: dict[str, str]
    response_payload: str
    response_headers: dict[str, str]
    extra: dict[str, Any]
    cause: httpx.HTTPStatusError

    @classmethod
    def from_status_error(cls, err: httpx.HTTPStatusError) -> Self:
        response, extra, missing, duplicated = err.response, {}, None, None
        try:
            match error := response.json()["error"]:
                case str():
                    msg = error
                case dict():
                    extra = error.copy()
                    extra.pop("code", None)  # some APIs also return status code here
                    msg = extra.pop("message")
                    missing = extra.pop("missing", None) or None  # if empty list, make it None
                    duplicated = extra.pop("duplicated", None) or None
                case _:
                    msg = response.text
        except (KeyError, _json.JSONDecodeError):
            msg = response.text

        return cls(
            message=msg,
            status_code=response.status_code,
            missing=missing,
            duplicated=duplicated,
            x_request_id=response.headers.get("x-request-id"),
            headers=BasicAPIClient._sanitize_headers(err.request.headers),
            response_payload=shorten(BasicAPIClient._get_response_content_safe(response), 1000),
            response_headers=dict(response.headers),
            extra=extra,
            cause=err,
        )

    def raise_api_error(self, cognite_client: CogniteClient) -> NoReturn:
        cluster = cognite_client._config.cdf_cluster
        match self.status_code, self.duplicated, self.missing:
            case 401, *_:
                self._raise_no_project_access_error(cognite_client)
            case 409, list(), None:
                self._raise_api_error(CogniteDuplicatedError, cluster)
            case 400 | 422, None, list():
                self._raise_api_error(CogniteNotFoundError, cluster)
            case _:
                self._raise_api_error(CogniteAPIError, cluster)

    def _raise_no_project_access_error(self, cognite_client: CogniteClient) -> NoReturn:
        raise CogniteProjectAccessError(
            client=cognite_client,
            project=cognite_client._config.project,
            x_request_id=self.x_request_id,
            cluster=cognite_client._config.cdf_cluster,
        ) from None  # hide httpx.HTTPStatusError from SDK users

    def _raise_api_error(self, err_type: type[CogniteAPIError], cluster: str | None) -> NoReturn:
        raise err_type(
            self.message,
            code=self.status_code,
            x_request_id=self.x_request_id,
            missing=self.missing,
            duplicated=self.duplicated,
            extra=self.extra,
            cluster=cluster,
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
        full_url: str,
        json: Any = None,
        headers: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Iterator[httpx.Response]:
        """Make a request to something that is outside Cognite Data Fusion and stream response content"""
        try:
            with self._http_client_with_retry.stream(
                method, full_url, json=json, headers=headers, timeout=timeout or self._config.timeout
            ) as resp:
                self._log_successful_request(resp, stream=True)
                yield resp
        except httpx.HTTPStatusError as err:
            self._handle_status_error(err)

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

    def _handle_status_error(self, err: httpx.HTTPStatusError, payload: dict | None = None) -> NoReturn:
        """The response had an err HTTP status of 4xx or 5xx"""
        error_details = FailedRequestDetails.from_status_error(err)
        self._log_failed_request(error_details, payload)
        error_details.raise_api_error(self._cognite_client)

    @staticmethod
    def _log_failed_request(error_details: FailedRequestDetails, payload: dict | None = None) -> None:
        extra = {}
        if payload:
            extra["payload"] = payload
        if error_details.missing:
            extra["missing"] = error_details.missing
        if error_details.duplicated:
            extra["duplicated"] = error_details.duplicated

        response = (error := error_details.cause).response
        if response.history:
            for res_hist in response.history:
                logger.debug(
                    f"REDIRECT AFTER HTTP Error {res_hist.status_code} {res_hist.request.method} "
                    f"{res_hist.request.url}: {res_hist.content.decode()}"
                )
        request, message = error.request, error_details.message
        logger.debug(f"HTTP Error {response.status_code} {request.method} {request.url}: {message}", extra=extra)

    def _log_successful_request(
        self, res: httpx.Response, payload: dict[str, Any] | None = None, stream: bool = False
    ) -> None:
        extra: dict[str, Any] = {
            "headers": self._sanitize_headers(res.request.headers),
            "response_headers": dict(res.headers),
        }
        if payload:
            extra["payload"] = payload
        if not stream and self._config.debug:
            extra["response_payload"] = shorten(self._get_response_content_safe(res), 1_000)
        try:
            http_protocol = res.http_version
        except AttributeError:
            # If this fails, it prob. means we are running in a browser (pyodide) with patched httpx package:
            http_protocol = "XMLHTTP"

        logger.debug(f"{http_protocol} {res.request.method} {res.url} {res.status_code}", extra=extra)

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
    def _get_response_content_safe(res: httpx.Response) -> str:
        try:
            return _json.dumps(res.json())
        except _json.JSONDecodeError:
            try:
                return res.text
            except UnicodeDecodeError:
                return "<binary>"

    @staticmethod
    def _sanitize_headers(headers: httpx.Headers) -> dict[str, str]:
        sanitized = dict(headers)
        for k, v in sanitized.items():
            if k.lower() in {"authorization", "proxy-authorization"}:
                sanitized[k] = "***"
        return sanitized
