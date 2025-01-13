from __future__ import annotations

import functools
import gzip
import logging
import platform
from collections.abc import Iterable, Iterator, MutableMapping
from typing import TYPE_CHECKING, Any, Literal, NoReturn

import httpx

from cognite.client._http_client import HTTPClientWithRetry, HTTPClientWithRetryConfig
from cognite.client.config import global_config
from cognite.client.exceptions import CogniteAPIError, CogniteProjectAccessError
from cognite.client.utils import _json
from cognite.client.utils._text import shorten
from cognite.client.utils._url import resolve_url

if TYPE_CHECKING:
    from cognite.client._cognite_client import CogniteClient
    from cognite.client.config import ClientConfig


logger = logging.getLogger(__name__)


def handle_json_dump(json: dict[str, Any] | None, full_headers: MutableMapping[str, str]) -> bytes | str | None:
    if json is None:
        return None

    content = _json.dumps_no_nan_or_inf(json)
    if global_config.disable_gzip:
        return content

    full_headers["Content-Encoding"] = "gzip"
    return gzip.compress(content.encode())


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

    def select_http_client(self, is_retryable: bool) -> HTTPClientWithRetry:
        return self._http_client_with_retry if is_retryable else self._http_client

    def _request(
        self,
        method: str,
        /,
        full_url: str,
        headers: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        """Make a request to something that is outside Cognite Data Fusion"""
        client = self.select_http_client(method in {"GET", "PUT", "HEAD"})
        try:
            res = client("GET", full_url, headers=headers, timeout=timeout or self._config.timeout)
        except httpx.HTTPStatusError as err:
            self._handle_status_error(err)

        self._log_successful_request(res)
        return res

    def _stream(
        self,
        method: Literal["GET", "POST"],
        /,
        full_url: str,
        headers: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Iterator[httpx.Response]:
        try:
            res = self._http_client_with_retry.stream(
                method, full_url, headers=headers, timeout=timeout or self._config.timeout
            )
        except httpx.HTTPStatusError as err:
            self._handle_status_error(err)

        self._log_successful_request(res, stream=True)
        return res

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
        content = handle_json_dump(json, full_headers)

        http_client = self.select_http_client(is_retryable)
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
        is_retryable, full_url = resolve_url("PUT", url_path, self._api_version, self._config)
        full_headers = self._configure_headers(additional_headers=headers, api_subversion=api_subversion)
        if content is None:
            content = handle_json_dump(json, full_headers)

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
            "accept": "*/*",
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

    def _handle_status_error(self, error: httpx.HTTPStatusError, payload: dict | None = None) -> NoReturn:
        # The response had an error HTTP status of 4xx or 5xx:
        match error.response.status_code:
            case 401:
                self._raise_no_project_access_error(error, payload)
            case _:
                self._raise_api_error(error, payload)

    def _raise_no_project_access_error(self, err: httpx.HTTPStatusError, payload: dict | None = None) -> NoReturn:
        self._log_failed_request(err, *self._extract_error_details(err), payload)
        raise CogniteProjectAccessError(
            client=self._cognite_client,
            project=self._cognite_client._config.project,
            x_request_id=err.response.headers.get("x-request-id"),
            cluster=self._config.cdf_cluster,
        )

    def _raise_api_error(self, err: httpx.HTTPStatusError, payload: dict | None = None) -> NoReturn:
        msg, error_details, missing, duplicated = self._extract_error_details(err)
        self._log_failed_request(err, msg, error_details, missing, duplicated, payload)
        # TODO: We should throw "CogniteNotFoundError" if missing is populated and CogniteDuplicatedError when duplicated...
        raise CogniteAPIError(
            msg,
            code=err.response.status_code,
            x_request_id=error_details.get("x-request-id"),
            missing=missing,
            duplicated=duplicated,
            extra=error_details,
            cluster=self._config.cdf_cluster,
        ) from err

    def _extract_error_details(
        self, err: httpx.HTTPStatusError
    ) -> tuple[str, dict[str, Any], list[str] | None, list[str] | None]:
        response, request = err.response, err.request
        extra, missing, duplicated = {}, None, None
        try:
            match error := response.json()["error"]:
                case str():
                    msg = error
                case dict():
                    extra = error.copy()
                    msg = extra.pop("message")
                    missing = extra.pop("missing", None)
                    duplicated = extra.pop("duplicated", None)
                case _:
                    msg = response.text
        except (KeyError, _json.JSONDecodeError):
            msg = response.text

        error_details: dict[str, Any] = {
            "x-request-id": response.headers.get("x-request-id"),
            "headers": self._sanitize_headers(request.headers),
            "response_payload": shorten(self._get_response_content_safe(response), 1000),
            "response_headers": response.headers,
        }
        return msg, error_details, missing, duplicated

    @staticmethod
    def _log_failed_request(
        err: httpx.HTTPStatusError,
        msg: str,
        error_details: dict[str, Any],
        missing: list[str] | None,
        duplicated: list[str] | None,
        payload: dict | None = None,
    ) -> None:
        if payload:
            error_details["payload"] = payload
        if missing:
            error_details["missing"] = missing
        if duplicated:
            error_details["duplicated"] = duplicated

        response, request = err.response, err.request
        if response.history:
            for res_hist in response.history:
                logger.debug(
                    f"REDIRECT AFTER HTTP Error {res_hist.status_code} {res_hist.request.method} "
                    f"{res_hist.request.url}: {res_hist.content.decode()}"
                )
        logger.debug(f"HTTP Error {response.status_code} {request.method} {request.url}: {msg}", extra=error_details)

    def _log_successful_request(
        self, res: httpx.Response, payload: dict[str, Any] | None = None, stream: bool = False
    ) -> None:
        extra: dict[str, Any] = {
            "headers": self._sanitize_headers(res.request.headers.copy()),
            "response_headers": res.headers,
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
    def _get_response_content_safe(res: httpx.Response) -> str:
        try:
            return _json.dumps(res.json())
        except _json.JSONDecodeError:
            try:
                return res.text
            except UnicodeDecodeError:
                return "<binary>"

    @staticmethod
    def _sanitize_headers(headers: httpx.Headers | None) -> httpx.Headers | None:
        if headers and "Authorization" in headers:
            headers["Authorization"] = "***"
        return headers
