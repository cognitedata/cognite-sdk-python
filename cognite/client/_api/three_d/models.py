from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    ThreeDModel,
    ThreeDModelList,
    ThreeDModelUpdate,
    ThreeDModelWrite,
)
from cognite.client.utils._identifier import IdentifierSequence, InternalId
from cognite.client.utils.useful_types import SequenceNotStr


class ThreeDModelsAPI(APIClient):
    _RESOURCE_PATH = "/3d/models"

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
        """Iterate over 3d models

        Fetches 3d models as they are iterated over, so you keep a limited number of 3d models in memory.

        Args:
            chunk_size (int | None): Number of 3d models to return in each chunk. Defaults to yielding one model a time.
            published (bool | None): Filter based on whether or not the model has published revisions.
            limit (int | None): Maximum number of 3d models to return. Defaults to return all items.

        Returns:
            Iterator[ThreeDModel] | Iterator[ThreeDModelList]: yields ThreeDModel one by one if chunk is not specified, else ThreeDModelList objects.
        """
        return self._list_generator(
            list_cls=ThreeDModelList,
            resource_cls=ThreeDModel,
            method="GET",
            chunk_size=chunk_size,
            filter={"published": published},
            limit=limit,
        )

    def __iter__(self) -> Iterator[ThreeDModel]:
        """Iterate over 3d models

        Fetches models as they are iterated over, so you keep a limited number of models in memory.

        Returns:
            Iterator[ThreeDModel]: yields models one by one.
        """
        return self()

    def retrieve(self, id: int) -> ThreeDModel | None:
        """`Retrieve a 3d model by id <https://developer.cognite.com/api#tag/3D-Models/operation/get3DModel>`_

        Args:
            id (int): Get the model with this id.

        Returns:
            ThreeDModel | None: The requested 3d model.

        Example:

            Get 3d model by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.models.retrieve(id=1)
        """
        return self._retrieve(cls=ThreeDModel, identifier=InternalId(id))

    def list(self, published: bool | None = None, limit: int | None = DEFAULT_LIMIT_READ) -> ThreeDModelList:
        """`List 3d models. <https://developer.cognite.com/api#tag/3D-Models/operation/get3DModels>`_

        Args:
            published (bool | None): Filter based on whether or not the model has published revisions.
            limit (int | None): Maximum number of models to retrieve. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ThreeDModelList: The list of 3d models.

        Examples:

            List 3d models:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> three_d_model_list = client.three_d.models.list()

            Iterate over 3d models:

                >>> for three_d_model in client.three_d.models:
                ...     three_d_model # do something with the 3d model

            Iterate over chunks of 3d models to reduce memory load:

                >>> for three_d_model in client.three_d.models(chunk_size=50):
                ...     three_d_model # do something with the 3d model
        """
        return self._list(
            list_cls=ThreeDModelList,
            resource_cls=ThreeDModel,
            method="GET",
            filter={"published": published},
            limit=limit,
        )

    def create(
        self,
        name: str | ThreeDModelWrite | SequenceNotStr[str | ThreeDModelWrite],
        data_set_id: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> ThreeDModel | ThreeDModelList:
        """`Create new 3d models. <https://developer.cognite.com/api#tag/3D-Models/operation/create3DModels>`_

        Args:
            name (str | ThreeDModelWrite | SequenceNotStr[str | ThreeDModelWrite]): The name of the 3d model(s) or 3D
                model object to create. If a 3D model object is provided, the other arguments are ignored.
            data_set_id (int | None): The id of the dataset this 3D model belongs to.
            metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value.
                Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.

        Returns:
            ThreeDModel | ThreeDModelList: The created 3d model(s).

        Example:

            Create new 3d models::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.models.create(name="My Model", data_set_id=1, metadata={"key1": "value1", "key2": "value2"})

            Create multiple new 3D Models::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ThreeDModelWrite
                >>> client = CogniteClient()
                >>> my_model = ThreeDModelWrite(name="My Model", data_set_id=1, metadata={"key1": "value1", "key2": "value2"})
                >>> my_other_model = ThreeDModelWrite(name="My Other Model", data_set_id=1, metadata={"key1": "value1", "key2": "value2"})
                >>> res = client.three_d.models.create([my_model, my_other_model])

        """
        items: ThreeDModelWrite | list[ThreeDModelWrite]
        if isinstance(name, str):
            items = ThreeDModelWrite(name, data_set_id, metadata)
        elif isinstance(name, ThreeDModelWrite):
            items = name
        else:
            items = [ThreeDModelWrite(n, data_set_id, metadata) if isinstance(n, str) else n for n in name]
        return self._create_multiple(list_cls=ThreeDModelList, resource_cls=ThreeDModel, items=items)

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
        """`Update 3d models. <https://developer.cognite.com/api#tag/3D-Models/operation/update3DModels>`_

        Args:
            item (ThreeDModel | ThreeDModelUpdate | Sequence[ThreeDModel | ThreeDModelUpdate]): ThreeDModel(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (ThreeDModel or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            ThreeDModel | ThreeDModelList: Updated ThreeDModel(s)

        Examples:

            Update 3d model that you have fetched. This will perform a full update of the model:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> three_d_model = client.three_d.models.retrieve(id=1)
                >>> three_d_model.name = "New Name"
                >>> res = client.three_d.models.update(three_d_model)

            Perform a partial update on a 3d model:

                >>> from cognite.client.data_classes import ThreeDModelUpdate
                >>> my_update = ThreeDModelUpdate(id=1).name.set("New Name")
                >>> res = client.three_d.models.update(my_update)

        """
        # Note that we cannot use the ThreeDModelWrite to update as the write format of a 3D model
        # does not have ID or External ID, thus no identifier to know which model to update.
        return self._update_multiple(
            list_cls=ThreeDModelList,
            resource_cls=ThreeDModel,
            update_cls=ThreeDModelUpdate,
            items=item,
            mode=mode,
        )

    def delete(self, id: int | Sequence[int]) -> None:
        """`Delete 3d models. <https://developer.cognite.com/api#tag/3D-Models/operation/delete3DModels>`_

        Args:
            id (int | Sequence[int]): ID or list of IDs to delete.

        Example:

            Delete 3d model by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.models.delete(id=1)
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=True)
