from __future__ import annotations

import gzip
import logging
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, NoReturn

import httpx

from cognite.client._http_client import HTTPClientWithRetry, HTTPClientWithRetryConfig
from cognite.client.config import global_config
from cognite.client.exceptions import CogniteAPIError, CogniteProjectAccessError
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import get_current_sdk_version, get_user_agent
from cognite.client.utils._text import shorten
from cognite.client.utils._url import resolve_url

if TYPE_CHECKING:
    from cognite.client._cognite_client import CogniteClient
    from cognite.client.config import ClientConfig


logger = logging.getLogger(__name__)

try:
    from httpx._client import USER_AGENT
except ImportError:
    USER_AGENT = "python-httpx/<unknown>"


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

    def _delete(
        self, url_path: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None
    ) -> httpx.Response:
        return self._do_request("DELETE", url_path, params=params, headers=headers, timeout=self._config.timeout)

    def _get(
        self,
        url_path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        follow_redirects=False,
        api_subversion: str | None = None,
    ) -> httpx.Response:
        # full_url = resolve_url(url_path, self._api_version, self._config)
        full_headers = self._configure_headers(additional_headers=headers, api_subversion=api_subversion)
        try:
            res = self._http_client_with_retry.get(
                full_url,
                params=params,
                headers=full_headers,
                follow_redirects=follow_redirects,
                timeout=self._config.timeout,
            )
        except httpx.HTTPStatusError as err:
            self._handle_status_error(err)

        self._log_request(res, payload=None, stream=None)
        return res

    def _post(
        self,
        url_path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        api_subversion: str | None = None,
    ) -> httpx.Response:
        return self._do_request(
            "POST",
            url_path,
            json=json,
            headers=headers,
            params=params,
            timeout=self._config.timeout,
            api_subversion=api_subversion,
        )

    def _put(
        self, url_path: str, json: dict[str, Any] | None = None, headers: dict[str, Any] | None = None
    ) -> httpx.Response:
        return self._do_request("PUT", url_path, json=json, headers=headers, timeout=self._config.timeout)

    def _configure_headers(
        self, additional_headers: dict[str, str] | None, api_subversion: str | None
    ) -> dict[str, str]:
        headers = {
            "content-type": "application/json",
            "accept": "*/*",
            "x-cdp-sdk": "CognitePythonSDK:" + get_current_sdk_version(),
            "x-cdp-app": self._config.client_name,
            "cdf-version": api_subversion or self._api_subversion,
            "user-agent": USER_AGENT + get_user_agent(),
            **self._config.headers,
            **(additional_headers or {}),
        }
        self._refresh_auth_header(headers)
        return headers

    def _refresh_auth_header(self, headers: MutableMapping[str, Any]) -> None:
        auth_header_name, auth_header_value = self._config.credentials.authorization_header()
        headers[auth_header_name] = auth_header_value

    def _prepare_request(
        self,
        method: str,
        url_path: str,
        api_subversion: str | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> Any:
        is_retryable, full_url = resolve_url(method, url_path, self._api_version, self._config)
        full_headers = self._configure_headers(additional_headers=headers, api_subversion=api_subversion)
        if json is not None:
            try:
                data = _json.dumps(json, allow_nan=False)
            except ValueError as e:
                # A lot of work to give a more human friendly error message when nans and infs are present:
                msg = "Out of range float values are not JSON compliant"
                if msg in str(e):  # exc. might e.g. contain an extra ": nan", depending on build (_json.make_encoder)
                    raise ValueError(f"{msg}. Make sure your data does not contain NaN(s) or +/- Inf!").with_traceback(
                        e.__traceback__
                    ) from None
                raise

            if method in ("PUT", "POST") and not global_config.disable_gzip:
                data = gzip.compress(data.encode())
                full_headers["Content-Encoding"] = "gzip"

        return full_url, full_headers, data, is_retryable

        # if is_retryable:
        #     res = self._http_client_with_retry.request(method=method, url=full_url, **kwargs)
        # else:
        #     res = self._http_client.request(method=method, url=full_url, **kwargs)

        # match res.status_code:
        #     case 200 | 201 | 202 | 204:
        #         pass
        #     case 401:
        #         self._raise_no_project_access_error(res)
        #     case _:
        #         self._raise_api_error(res, payload=json_payload)

        # stream = kwargs.get("stream")
        # self._log_request(res, payload=json_payload, stream=stream)
        # return res

    # def _inspect_response(
    #     self,
    #     response: httpx.Response,
    #     json_payload: dict[str, Any] | None = None,
    #     stream: Any = None,
    # ) -> None:
    #     if not self._status_ok(response.status_code):
    #         self._raise_api_error(response, payload=json_payload)
    #     self._log_request(response, payload=json_payload, stream=stream)
    #     return response

    def _handle_status_error(self, error: httpx.HTTPStatusError, payload: dict | None = None) -> NoReturn:
        # The response had an error HTTP status of 4xx or 5xx:
        match error.response.status_code:
            case 401:
                self._raise_no_project_access_error(error.response)
            case _:
                self._raise_api_error(error, payload)

    def _raise_no_project_access_error(self, response: httpx.Response) -> NoReturn:
        raise CogniteProjectAccessError(
            client=self._cognite_client,
            project=self._cognite_client._config.project,
            x_request_id=response.headers.get("x-request-id"),
            cluster=self._config.cdf_cluster,
        )

    def _raise_api_error(self, err: httpx.HTTPStatusError, payload: dict | None = None) -> NoReturn:
        response, request = err.response, err.request
        x_request_id = response.headers.get("X-Request-Id")
        code, extra, missing, duplicated = response.status_code, {}, None, None
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
                    msg = response.content.decode()
        except KeyError | _json.JSONDecodeError:
            msg = response.content.decode()

        error_details: dict[str, Any] = {
            "X-Request-ID": x_request_id,
            "headers": self._sanitize_headers(request.headers),
            "response_payload": shorten(self._get_response_content_safe(response), 1000),
            "response_headers": response.headers,
        }
        if payload:
            error_details["payload"] = payload
        if missing:
            error_details["missing"] = missing
        if duplicated:
            error_details["duplicated"] = duplicated

        if response.history:
            for res_hist in response.history:
                logger.debug(
                    f"REDIRECT AFTER HTTP Error {res_hist.status_code} {res_hist.request.method} "
                    f"{res_hist.request.url}: {res_hist.content.decode()}"
                )
        logger.debug(f"HTTP Error {code} {res.request.method} {res.request.url}: {msg}", extra=error_details)
        # TODO: We should throw "CogniteNotFoundError" if missing is populated and CogniteDuplicatedError when duplicated...
        raise CogniteAPIError(
            msg,
            code,
            x_request_id,
            missing=missing,
            duplicated=duplicated,
            extra=extra,
            cluster=self._config.cdf_cluster,
        ) from err

    def _log_request(self, res: httpx.Response, **kwargs: Any) -> None:
        method = res.request.method
        url = res.request.url
        status_code = res.status_code

        extra = kwargs.copy()
        extra["headers"] = res.request.headers.copy()
        self._sanitize_headers(extra["headers"])
        if extra["payload"] is None:
            del extra["payload"]

        stream = kwargs.get("stream")
        if not stream and self._config.debug is True:
            extra["response_payload"] = shorten(self._get_response_content_safe(res), 500)
        extra["response_headers"] = res.headers

        try:
            http_protocol = res.http_version
        except AttributeError:
            # If this fails, it means we are running in a browser (pyodide) with patched requests package:
            http_protocol = "XMLHTTP"

        logger.debug(f"{http_protocol} {method} {url} {status_code}", extra=extra)

    @staticmethod
    def _get_response_content_safe(res: httpx.Response) -> str:
        try:
            return _json.dumps(res.json())
        except _json.JSONDecodeError:
            pass

        try:
            return res.content.decode()
        except UnicodeDecodeError:
            pass

        return "<binary>"

    @staticmethod
    def _sanitize_headers(headers: httpx.Headers | None) -> httpx.Headers | None:
        if headers and "Authorization" in headers:
            headers["Authorization"] = "***"
        return headers
