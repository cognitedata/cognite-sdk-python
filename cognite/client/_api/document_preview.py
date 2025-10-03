from __future__ import annotations

from pathlib import Path
from typing import IO

from cognite.client._api_client import APIClient
from cognite.client.data_classes.documents import (
    TemporaryLink,
)


class DocumentPreviewAPI(APIClient):
    _RESOURCE_PATH = "/documents"

    def download_page_as_png_bytes(self, id: int, page_number: int = 1) -> bytes:
        """`Downloads an image preview for a specific page of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewImagePage>`_

        Args:
            id (int): The server-generated ID for the document you want to retrieve the preview of.
            page_number (int): Page number to preview. Starting at 1 for first page.

        Returns:
            bytes: The png preview of the document.

        Examples:

            Download image preview of page 5 of file with id 123:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> content = client.documents.previews.download_page_as_png_bytes(id=123, page_number=5)

            Download an image preview and display using IPython.display.Image (for example in a Jupyter Notebook):

                >>> from IPython.display import Image
                >>> binary_png = client.documents.previews.download_page_as_png_bytes(id=123, page_number=5)
                >>> Image(binary_png)
        """
        res = self._do_request(
            "GET", f"{self._RESOURCE_PATH}/{id}/preview/image/pages/{page_number}", accept="image/png"
        )
        return res.content

    def download_page_as_png(
        self, path: Path | str | IO, id: int, page_number: int = 1, overwrite: bool = False
    ) -> None:
        """`Downloads an image preview for a specific page of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewImagePage>`_

        Args:
            path (Path | str | IO): The path to save the png preview of the document. If the path is a directory, the file name will be '[id]_page[page_number].png'.
            id (int): The server-generated ID for the document you want to retrieve the preview of.
            page_number (int): Page number to preview. Starting at 1 for first page.
            overwrite (bool): Whether to overwrite existing file at the given path. Defaults to False.

        Examples:

            Download Image preview of page 5 of file with id 123 to folder "previews":

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.documents.previews.download_page_as_png("previews", id=123, page_number=5)
        """
        if isinstance(path, IO):
            content = self.download_page_as_png_bytes(id)
            path.write(content)
            return

        if (path := Path(path)).is_dir():
            path /= f"{id}_page{page_number}.png"
        elif path.suffix != ".png":
            raise ValueError("Path must be a directory or end with .png")
        if not overwrite and path.exists():
            raise FileExistsError(f"File {path} already exists. Use overwrite=True to overwrite existing file.")
        content = self.download_page_as_png_bytes(id, page_number)
        path.write_bytes(content)

    def download_document_as_pdf_bytes(self, id: int) -> bytes:
        """`Downloads a pdf preview of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewPdf>`_

        Previews will be rendered if necessary during the request. Be prepared for the request to take a few seconds to complete.

        Args:
            id (int): The server-generated ID for the document you want to retrieve the preview of.

        Returns:
            bytes: The pdf preview of the document.

        Examples:

            Download PDF preview of file with id 123:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> content = client.documents.previews.download_document_as_pdf_bytes(id=123)
        """
        res = self._do_request("GET", f"{self._RESOURCE_PATH}/{id}/preview/pdf", accept="application/pdf")
        return res.content

    def download_document_as_pdf(self, path: Path | str | IO, id: int, overwrite: bool = False) -> None:
        """`Downloads a pdf preview of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewPdf>`_

        Previews will be rendered if necessary during the request. Be prepared for the request to take a few seconds to complete.

        Args:
            path (Path | str | IO): The path to save the pdf preview of the document. If the path is a directory, the file name will be '[id].pdf'.
            id (int): The server-generated ID for the document you want to retrieve the preview of.
            overwrite (bool): Whether to overwrite existing file at the given path. Defaults to False.

        Examples:

            Download PDF preview of file with id 123 to folder "previews":

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.documents.previews.download_document_as_pdf("previews", id=123)
        """
        if isinstance(path, IO):
            content = self.download_document_as_pdf_bytes(id)
            path.write(content)
            return

        if (path := Path(path)).is_dir():
            path /= f"{id}.pdf"
        elif path.suffix != ".pdf":
            raise ValueError("Path must be a directory or end with .pdf")
        if not overwrite and path.exists():
            raise FileExistsError(f"File {path} already exists. Use overwrite=True to overwrite existing file.")
        content = self.download_document_as_pdf_bytes(id)
        path.write_bytes(content)

    def retrieve_pdf_link(self, id: int) -> TemporaryLink:
        """`Retrieve a Temporary link to download pdf preview <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewPdfTemporaryLink>`_

        Args:
            id (int): The server-generated ID for the document you want to retrieve the preview of.

        Returns:
            TemporaryLink: A temporary link to download the pdf preview.

        Examples:

            Retrieve the PDF preview download link for document with id 123:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> link = client.documents.previews.retrieve_pdf_link(id=123)
        """
        res = self._get(f"{self._RESOURCE_PATH}/{id}/preview/pdf/temporarylink")
        return TemporaryLink.load(res.json())
