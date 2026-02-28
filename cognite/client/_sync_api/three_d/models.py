"""
===============================================================================
c0fdd9917d4201cf971771159e7740c9
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import ThreeDModel, ThreeDModelList, ThreeDModelUpdate, ThreeDModelWrite
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr


class Sync3DModelsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self, chunk_size: None = None, published: bool | None = None, limit: int | None = None
    ) -> Iterator[ThreeDModel]: ...

    @overload
    def __call__(
        self, chunk_size: int, published: bool | None = None, limit: int | None = None
    ) -> Iterator[ThreeDModelList]: ...

    def __call__(
        self, chunk_size: int | None = None, published: bool | None = None, limit: int | None = None
    ) -> Iterator[ThreeDModel] | Iterator[ThreeDModelList]:
        """
        Iterate over 3d models

        Fetches 3d models as they are iterated over, so you keep a limited number of 3d models in memory.

        Args:
            chunk_size (int | None): Number of 3d models to return in each chunk. Defaults to yielding one model a time.
            published (bool | None): Filter based on whether or not the model has published revisions.
            limit (int | None): Maximum number of 3d models to return. Defaults to return all items.

        Yields:
            ThreeDModel | ThreeDModelList: yields ThreeDModel one by one if chunk is not specified, else ThreeDModelList objects.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.three_d.models(chunk_size=chunk_size, published=published, limit=limit)
        )  # type: ignore [misc]

    def retrieve(self, id: int) -> ThreeDModel | None:
        """
        `Retrieve a 3d model by id <https://api-docs.cognite.com/20230101/tag/3D-Models/operation/get3DModel>`_

        Args:
            id (int): Get the model with this id.

        Returns:
            ThreeDModel | None: The requested 3d model.

        Example:

            Get 3d model by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.three_d.models.retrieve(id=1)
        """
        return run_sync(self.__async_client.three_d.models.retrieve(id=id))

    def list(self, published: bool | None = None, limit: int | None = DEFAULT_LIMIT_READ) -> ThreeDModelList:
        """
        `List 3d models. <https://api-docs.cognite.com/20230101/tag/3D-Models/operation/get3DModels>`_

        Args:
            published (bool | None): Filter based on whether or not the model has published revisions.
            limit (int | None): Maximum number of models to retrieve. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ThreeDModelList: The list of 3d models.

        Examples:

            List 3d models:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> model_list = client.three_d.models.list()

            Iterate over 3d models, one-by-one:

                >>> for model in client.three_d.models():
                ...     model  # do something with the 3d model

            Iterate over chunks of 3d models to reduce memory load:

                >>> for model in client.three_d.models(chunk_size=50):
                ...     model # do something with the 3d model
        """
        return run_sync(self.__async_client.three_d.models.list(published=published, limit=limit))

    @overload
    def create(
        self, name: str | ThreeDModelWrite, data_set_id: int | None = None, metadata: dict[str, str] | None = None
    ) -> ThreeDModel: ...

    @overload
    def create(
        self,
        name: SequenceNotStr[str | ThreeDModelWrite],
        data_set_id: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> ThreeDModelList: ...

    def create(
        self,
        name: str | ThreeDModelWrite | SequenceNotStr[str | ThreeDModelWrite],
        data_set_id: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> ThreeDModel | ThreeDModelList:
        """
        `Create new 3d models. <https://api-docs.cognite.com/20230101/tag/3D-Models/operation/create3DModels>`_

        Args:
            name (str | ThreeDModelWrite | SequenceNotStr[str | ThreeDModelWrite]): The name of the 3d model(s) or 3D
                model object to create. If a 3D model object is provided, the other arguments are ignored.
            data_set_id (int | None): The id of the dataset this 3D model belongs to.
            metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value.
                Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.

        Returns:
            ThreeDModel | ThreeDModelList: The created 3d model(s).

        Example:

            Create new 3d models:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.three_d.models.create(name="My Model", data_set_id=1, metadata={"key1": "value1", "key2": "value2"})

            Create multiple new 3D Models:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ThreeDModelWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> my_model = ThreeDModelWrite(name="My Model", data_set_id=1, metadata={"key1": "value1", "key2": "value2"})
                >>> my_other_model = ThreeDModelWrite(name="My Other Model", data_set_id=1, metadata={"key1": "value1", "key2": "value2"})
                >>> res = client.three_d.models.create([my_model, my_other_model])
        """
        return run_sync(
            self.__async_client.three_d.models.create(name=name, data_set_id=data_set_id, metadata=metadata)
        )

    @overload
    def update(
        self,
        item: ThreeDModel | ThreeDModelUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> ThreeDModel: ...

    @overload
    def update(
        self,
        item: Sequence[ThreeDModel | ThreeDModelUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> ThreeDModelList: ...

    def update(
        self,
        item: ThreeDModel | ThreeDModelUpdate | Sequence[ThreeDModel | ThreeDModelUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> ThreeDModel | ThreeDModelList:
        """
        `Update 3d models. <https://api-docs.cognite.com/20230101/tag/3D-Models/operation/update3DModels>`_

        Args:
            item (ThreeDModel | ThreeDModelUpdate | Sequence[ThreeDModel | ThreeDModelUpdate]): ThreeDModel(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (ThreeDModel or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            ThreeDModel | ThreeDModelList: Updated ThreeDModel(s)

        Examples:

            Update 3d model that you have fetched. This will perform a full update of the model:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> model = client.three_d.models.retrieve(id=1)
                >>> model.name = "New Name"
                >>> res = client.three_d.models.update(model)

            Perform a partial update on a 3d model:

                >>> from cognite.client.data_classes import ThreeDModelUpdate
                >>> my_update = ThreeDModelUpdate(id=1).name.set("New Name")
                >>> res = client.three_d.models.update(my_update)
        """
        return run_sync(self.__async_client.three_d.models.update(item=item, mode=mode))

    def delete(self, id: int | Sequence[int]) -> None:
        """
        `Delete 3d models. <https://api-docs.cognite.com/20230101/tag/3D-Models/operation/delete3DModels>`_

        Args:
            id (int | Sequence[int]): ID or list of IDs to delete.

        Example:

            Delete 3d model by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.three_d.models.delete(id=1)
        """
        return run_sync(self.__async_client.three_d.models.delete(id=id))
