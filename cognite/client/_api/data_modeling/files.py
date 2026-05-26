from __future__ import annotations

from typing import TYPE_CHECKING, Any, NoReturn

from cognite.client._api_client import APIClient

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class DataModelingFilesAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._files_api = cognite_client.files

    async def retrieve_download_urls(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def download(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def download_to_path(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def download_bytes(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def upload(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def upload_bytes(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def upload_content(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def upload_content_bytes(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def __call__(self) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def retrieve(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def list(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")
