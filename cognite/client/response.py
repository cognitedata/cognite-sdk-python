from __future__ import annotations

import typing
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from typing import Any, Final

from httpx import URL, Cookies, Headers, HTTPStatusError
from httpx import Request as HttpxRequest
from httpx import Response as HttpxResponse


class CogniteHTTPResponse:
    """
    A wrapper class (currently around `httpx.Response`) to isolate the SDK from the
    underlying HTTP library's public interface.
    """

    # Every single response is wrapped in this class, so squeeze out every bit of perf:
    __slots__ = ("_json_cache", "_response")
    _json_cache: Any

    def __init__(self, response: HttpxResponse) -> None:
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
    def reason(self) -> str:
        return self.reason_phrase  # requests backwards compatibility that we keep

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
    def history(self) -> list[CogniteHTTPResponse]:
        return [CogniteHTTPResponse(r) for r in self._response.history]

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

    def json(self) -> Any:
        try:
            return self._json_cache
        except AttributeError:
            self._json_cache = self._response.json()
            return self._json_cache

    def raise_for_status(self) -> CogniteHTTPResponse:
        """
        Raises a CogniteHTTPStatusError if the response status code is 4xx or 5xx.
        If successful (2xx), returns the response object (self) for chaining.
        """
        from cognite.client.exceptions import CogniteHTTPStatusError

        try:
            self._response.raise_for_status()
            return self
        except HTTPStatusError:
            raise CogniteHTTPStatusError(self.status_code, request=self.request, response=self) from None

    def iter_bytes(self, chunk_size: int | None = None) -> Iterator[bytes]:
        return self._response.iter_bytes(chunk_size)

    def read(self) -> bytes:
        return self._response.read()

    def close(self) -> None:
        self._response.close()

    async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        async for chunk in self._response.aiter_bytes(chunk_size):
            yield chunk

    async def aiter_text(self, chunk_size: int | None = None) -> AsyncIterator[str]:
        async for text in self._response.aiter_text(chunk_size):
            yield text

    async def aiter_lines(self) -> AsyncIterator[str]:
        async for line in self._response.aiter_lines():
            yield line

    async def aiter_raw(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        async for chunk in self._response.aiter_raw(chunk_size):
            yield chunk

    async def aread(self) -> bytes:
        return await self._response.aread()

    async def aclose(self) -> None:
        await self._response.aclose()
