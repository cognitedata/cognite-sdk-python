"""
===============================================================================
a613b724c1f2c9708b74ff1e9f99b3aa
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from pathlib import Path
from typing import IO

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.documents import TemporaryLink
from cognite.client.utils._async_helpers import run_sync


class SyncDocumentPreviewAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client

    def download_page_as_png_bytes(self, id: int, page_number: int = 1) -> bytes:
        """
        `Downloads an image preview for a specific page of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewImagePage>`_

        Args:
            id (int): The server-generated ID for the document you want to retrieve the preview of.
            page_number (int): Page number to preview. Starting at 1 for first page.

        Returns:
            bytes: The png preview of the document.

        Examples:

            Download image preview of page 5 of file with id 123:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> content = client.documents.previews.download_page_as_png_bytes(id=123, page_number=5)

            Download an image preview and display using IPython.display.Image (for example in a Jupyter Notebook):

                >>> from IPython.display import Image
                >>> binary_png = client.documents.previews.download_page_as_png_bytes(id=123, page_number=5)
                >>> Image(binary_png)
        """
        return run_sync(
            self.__async_client.documents.previews.download_page_as_png_bytes(id=id, page_number=page_number)
        )

    def download_page_as_png(
        self, path: Path | str | IO, id: int, page_number: int = 1, overwrite: bool = False
    ) -> None:
        """
        `Downloads an image preview for a specific page of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewImagePage>`_

        Args:
            path (Path | str | IO): The path to save the png preview of the document. If the path is a directory, the file name will be '[id]_page[page_number].png'.
            id (int): The server-generated ID for the document you want to retrieve the preview of.
            page_number (int): Page number to preview. Starting at 1 for first page.
            overwrite (bool): Whether to overwrite existing file at the given path. Defaults to False.

        Examples:

            Download Image preview of page 5 of file with id 123 to folder "previews":

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.documents.previews.download_page_as_png("previews", id=123, page_number=5)
        """
        return run_sync(
            self.__async_client.documents.previews.download_page_as_png(
                path=path, id=id, page_number=page_number, overwrite=overwrite
            )
        )

    def download_document_as_pdf_bytes(self, id: int) -> bytes:
        """
        `Downloads a pdf preview of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewPdf>`_

        Previews will be rendered if necessary during the request. Be prepared for the request to take a few seconds to complete.

        Args:
            id (int): The server-generated ID for the document you want to retrieve the preview of.

        Returns:
            bytes: The pdf preview of the document.

        Examples:

            Download PDF preview of file with id 123:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> content = client.documents.previews.download_document_as_pdf_bytes(id=123)
        """
        return run_sync(self.__async_client.documents.previews.download_document_as_pdf_bytes(id=id))

    def download_document_as_pdf(self, path: Path | str | IO, id: int, overwrite: bool = False) -> None:
        """
        `Downloads a pdf preview of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewPdf>`_

        Previews will be rendered if necessary during the request. Be prepared for the request to take a few seconds to complete.

        Args:
            path (Path | str | IO): The path to save the pdf preview of the document. If the path is a directory, the file name will be '[id].pdf'.
            id (int): The server-generated ID for the document you want to retrieve the preview of.
            overwrite (bool): Whether to overwrite existing file at the given path. Defaults to False.

        Examples:

            Download PDF preview of file with id 123 to folder "previews":

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.documents.previews.download_document_as_pdf("previews", id=123)
        """
        return run_sync(
            self.__async_client.documents.previews.download_document_as_pdf(path=path, id=id, overwrite=overwrite)
        )

    def retrieve_pdf_link(self, id: int) -> TemporaryLink:
        """
        `Retrieve a Temporary link to download pdf preview <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewPdfTemporaryLink>`_

        Args:
            id (int): The server-generated ID for the document you want to retrieve the preview of.

        Returns:
            TemporaryLink: A temporary link to download the pdf preview.

        Examples:

            Retrieve the PDF preview download link for document with id 123:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> link = client.documents.previews.retrieve_pdf_link(id=123)
        """
        return run_sync(self.__async_client.documents.previews.retrieve_pdf_link(id=id))
