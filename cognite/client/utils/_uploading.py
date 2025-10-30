from __future__ import annotations

import os
import warnings
from collections.abc import AsyncIterable, AsyncIterator
from io import BufferedReader, BytesIO, StringIO, TextIOBase, UnsupportedOperation
from typing import Any, BinaryIO


def prepare_content_for_upload(
    content: str | bytes | BinaryIO | AsyncIterator[bytes],
) -> tuple[int | None, AsyncFileChunker | bytes | AsyncIterator[bytes]]:
    # Note: future dev, please no "hasattr read" checks here, we are strict on what we accept
    match content:
        case BufferedReader() | BytesIO():
            file_size = peek_filelike_length(content)
            content = AsyncFileChunker(content)
        case bytes():
            file_size = len(content)
        case str():
            content = content.encode("utf-8")
            file_size = len(content)
        case AsyncIterable():
            file_size = None
        case StringIO():
            raise TypeError("File uploads using 'io.StringIO' is not supported, please use 'io.BytesIO' instead.")
        case TextIOBase():
            raise TypeError("The file to upload must be opened in binary mode, not text mode.")
        case _:
            raise TypeError(
                f"Attempted to upload unsupported content of type {type(content)}. Supported types are bytes, "
                "str (will be encoded using UTF-8), io.BytesIO, and file handles (must be opened in binary mode). "
                "Async iterators yielding bytes are also supported for certain cloud providers."
            )
    if file_size is None:
        # We allow this as it works flawlessly on GCP
        warnings.warn(
            "Could not determine the size of the upload content. This leads to using chunked transfer encoding which "
            "may fail for certain cloud providers like Azure or AWS.",
            RuntimeWarning,
        )
    return file_size, content


class AsyncFileChunker(AsyncIterator[bytes]):
    """
    An asynchronous iterator for reading a file in chunks. Needed because httpx does not support
    file handles in a way that doesn't involve HTTP multipart encoding (as opposed to requests).

    Args:
        file_handle (BinaryIO): An open file handle.
    """

    CHUNK_SIZE = 64 * 1024  # 64 KiB chunks by default, copying httpx default

    def __init__(self, file_handle: BinaryIO) -> None:
        from cognite.client import global_config

        self._file_handle = file_handle
        self._chunk_size = global_config.file_upload_chunk_size or self.CHUNK_SIZE

        # Read from beginning of file if possible:
        if hasattr(self._file_handle, "seek"):
            try:
                self._file_handle.seek(0)
            except UnsupportedOperation:
                pass

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        if chunk := self._file_handle.read(self._chunk_size):
            return chunk
        raise StopAsyncIteration


# Straight from httpx/_utils.py (comments removed) as it's currently not
# exposed - unlike requests and its super_len function:
def peek_filelike_length(stream: Any) -> int | None:
    """
    Given a file-like stream object, return its length in number of bytes
    without reading it into memory.
    """
    try:
        fd = stream.fileno()
        length = os.fstat(fd).st_size
    except (AttributeError, OSError):
        try:
            offset = stream.tell()
            length = stream.seek(0, os.SEEK_END)
            stream.seek(offset)
        except (AttributeError, OSError):
            return None
    return length
