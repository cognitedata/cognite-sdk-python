from __future__ import annotations

import typing
from collections.abc import Iterator
from datetime import timedelta
from typing import Any, Final

# We import the specific types used by httpx for accurate type hinting
from httpx import URL, Cookies, Headers, HTTPStatusError
from httpx import Request as HttpxRequest
from httpx import Response as HttpxResponse


class CogniteSDKResponse:
    """
    A wrapper class for httpx.Response to isolate the SDK from the
    underlying HTTP library's public interface.
    """

    def __init__(self, response: HttpxResponse) -> None:
        # Store the underlying response object privately
        self._response: Final[HttpxResponse] = response

    @property
    def httpx_response(self) -> HttpxResponse:
        """
        Direct access to the Response object from the underlying http library (currently httpx).

        Disclaimer: Usage is neither backwards- nor forwards-compatible.
        """
        return self._response

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def reason_phrase(self) -> str:
        return self._response.reason_phrase

    @property
    def http_version(self) -> str:
        return self._response.http_version

    @property
    def url(self) -> URL:
        return self._response.url

    @property
    def headers(self) -> Headers:
        return self._response.headers

    @property
    def cookies(self) -> Cookies:
        # Note: httpx.Response.cookies returns httpx.Cookies, which is a wrapper around SimpleCookie
        return self._response.cookies

    @property
    def history(self) -> list[CogniteSDKResponse]:
        return [CogniteSDKResponse(r) for r in self._response.history]

    @property
    def request(self) -> HttpxRequest:
        return self._response.request

    @property
    def content(self) -> bytes:
        return self._response.content

    @property
    def text(self) -> str:
        return self._response.text

    @property
    def encoding(self) -> str | None:
        return self._response.encoding

    @encoding.setter
    def encoding(self, value: str) -> None:
        self._response.encoding = value

    @property
    def is_success(self) -> bool:
        return self._response.is_success

    @property
    def is_error(self) -> bool:
        return self._response.is_error

    @property
    def elapsed(self) -> timedelta:
        return self._response.elapsed

    @property
    def next_request(self) -> HttpxRequest | None:
        return self._response.next_request

    @property
    def stream(self) -> typing.Any:
        # 'stream' typing is tricky; we leave it at Any as users should consume from the stream
        # using the provided methods like iter_bytes, iter_lines, iter_text, etc.
        return self._response.stream

    @property
    def num_bytes_downloaded(self) -> int:
        return self._response.num_bytes_downloaded

    def json(self, **kwargs: Any) -> Any:
        return self._response.json(**kwargs)

    def raise_for_status(self) -> CogniteSDKResponse:
        """
        Raises a CogniteHTTPStatusError if the response status code is 4xx or 5xx.
        If successful (2xx), returns the response object (self) for chaining.
        """
        from cognite.client.exceptions import CogniteHTTPStatusError

        try:
            self._response.raise_for_status()
            return self
        except HTTPStatusError as e:
            message = e.args[0]  # We forward the original error message from httpx which is nice
            raise CogniteHTTPStatusError(message, request=self.request, response=self) from None

    def iter_bytes(self, chunk_size: int | None = None) -> Iterator[bytes]:
        return self._response.iter_bytes(chunk_size)

    def read(self) -> bytes:
        return self._response.read()

    def close(self) -> None:
        self._response.close()

    async def aread(self) -> bytes:
        return await self._response.aread()

    async def aclose(self) -> None:
        await self._response.aclose()
