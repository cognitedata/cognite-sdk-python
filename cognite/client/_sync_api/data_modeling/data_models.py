"""
===============================================================================
c4692a5230018a16c1090d437d36ff14
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DATA_MODELING_DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.data_models import (
    DataModel,
    DataModelApply,
    DataModelList,
)
from cognite.client.data_classes.data_modeling.ids import DataModelId, DataModelIdentifier, ViewId
from cognite.client.data_classes.data_modeling.views import View
from cognite.client.utils._async_helpers import SyncIterator, run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncDataModelsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
        space: str | None = None,
        inline_views: bool = False,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> Iterator[DataModel]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
        space: str | None = None,
        inline_views: bool = False,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> Iterator[DataModelList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
        space: str | None = None,
        inline_views: bool = False,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> Iterator[DataModel | DataModelList]:
        """
        Iterate over data model

        Fetches data model as they are iterated over, so you keep a limited number of data model in memory.

        Args:
            chunk_size (int | None): Number of data model to return in each chunk. Defaults to yielding one data_model a time.
            limit (int | None): Maximum number of data model to return. Defaults to returning all items.
            space (str | None): The space to query.
            inline_views (bool): Whether to expand the referenced views inline in the returned result.
            all_versions (bool): Whether to return all versions. If false, only the newest version is returned, which is determined based on the 'createdTime' field.
            include_global (bool): Whether to include global views.

        Yields:
            DataModel | DataModelList: yields DataModel one by one if chunk_size is not specified, else DataModelList objects.
        """
        yield from SyncIterator(
            self.__async_client.data_modeling.data_models(
                chunk_size=chunk_size,
                limit=limit,
                space=space,
                inline_views=inline_views,
                all_versions=all_versions,
                include_global=include_global,
            )
        )  # type: ignore [misc]

    @overload
    def retrieve(
        self, ids: DataModelIdentifier | Sequence[DataModelIdentifier], inline_views: Literal[True]
    ) -> DataModelList[View]: ...

    @overload
    def retrieve(
        self, ids: DataModelIdentifier | Sequence[DataModelIdentifier], inline_views: Literal[False] = False
    ) -> DataModelList[ViewId]: ...

    def retrieve(
        self, ids: DataModelIdentifier | Sequence[DataModelIdentifier], inline_views: bool = False
    ) -> DataModelList[ViewId] | DataModelList[View]:
        """
        `Retrieve data_model(s) by id(s). <https://developer.cognite.com/api#tag/Data-models/operation/byExternalIdsDataModels>`_

        Args:
            ids (DataModelIdentifier | Sequence[DataModelIdentifier]): Data Model identifier(s).
            inline_views (bool): Whether to expand the referenced views inline in the returned result.

        Returns:
            DataModelList[ViewId] | DataModelList[View]: Requested data model(s) or empty if none exist.

        Examples:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.data_models.retrieve(("mySpace", "myDataModel", "v1"))
        """
        return run_sync(
            self.__async_client.data_modeling.data_models.retrieve(  # type: ignore [call-overload]
                ids=ids, inline_views=inline_views
            )
        )

    def delete(self, ids: DataModelIdentifier | Sequence[DataModelIdentifier]) -> list[DataModelId]:
        """
        `Delete one or more data model <https://developer.cognite.com/api#tag/Data-models/operation/deleteDataModels>`_

        Args:
            ids (DataModelIdentifier | Sequence[DataModelIdentifier]): Data Model identifier(s).
        Returns:
            list[DataModelId]: The data_model(s) which has been deleted. None if nothing was deleted.
        Examples:

            Delete data model by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.data_modeling.data_models.delete(("mySpace", "myDataModel", "v1"))
        """
        return run_sync(self.__async_client.data_modeling.data_models.delete(ids=ids))

    @overload
    def list(
        self,
        inline_views: Literal[True],
        limit: int | None = DATA_MODELING_DEFAULT_LIMIT_READ,
        space: str | None = None,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> DataModelList[View]: ...

    @overload
    def list(
        self,
        inline_views: Literal[False] = False,
        limit: int | None = DATA_MODELING_DEFAULT_LIMIT_READ,
        space: str | None = None,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> DataModelList[ViewId]: ...

    def list(
        self,
        inline_views: bool = False,
        limit: int | None = DATA_MODELING_DEFAULT_LIMIT_READ,
        space: str | None = None,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> DataModelList[View] | DataModelList[ViewId]:
        """
        `List data models <https://developer.cognite.com/api#tag/Data-models/operation/listDataModels>`_

        Args:
            inline_views (bool): Whether to expand the referenced views inline in the returned result.
            limit (int | None): Maximum number of data model to return. Defaults to 10. Set to -1, float("inf") or None to return all items.
            space (str | None): The space to query.
            all_versions (bool): Whether to return all versions. If false, only the newest version is returned, which is determined based on the 'createdTime' field.
            include_global (bool): Whether to include global data models.

        Returns:
            DataModelList[View] | DataModelList[ViewId]: List of requested data models

        Examples:

            List 5 data model:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> data_model_list = client.data_modeling.data_models.list(limit=5)

            Iterate over data model, one-by-one:

                >>> for data_model in client.data_modeling.data_models():
                ...     data_model  # do something with the data model

            Iterate over chunks of data model to reduce memory load:

                >>> for data_model_list in client.data_modeling.data_models(chunk_size=10):
                ...     data_model_list # do something with the data model
        """
        return run_sync(
            self.__async_client.data_modeling.data_models.list(  # type: ignore [call-overload]
                inline_views=inline_views,
                limit=limit,
                space=space,
                all_versions=all_versions,
                include_global=include_global,
            )
        )

    @overload
    def apply(self, data_model: Sequence[DataModelApply]) -> DataModelList: ...

    @overload
    def apply(self, data_model: DataModelApply) -> DataModel: ...

    def apply(self, data_model: DataModelApply | Sequence[DataModelApply]) -> DataModel | DataModelList:
        """
        `Create or update one or more data model. <https://developer.cognite.com/api#tag/Data-models/operation/createDataModels>`_

        Args:
            data_model (DataModelApply | Sequence[DataModelApply]): Data model(s) to create or update (upsert).

        Returns:
            DataModel | DataModelList: Created data model(s)

        Examples:

            Create new data model:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import DataModelApply, ViewId
                >>> client = CogniteClient()
                >>> data_models = [
                ...     DataModelApply(space="mySpace",external_id="myDataModel",version="v1",views=[ViewId("mySpace","myView","v1")]),
                ...     DataModelApply(space="mySpace",external_id="myOtherDataModel",version="v1",views=[ViewId("mySpace","myView","v1")])]
                >>> res = client.data_modeling.data_models.apply(data_models)
        """
        return run_sync(self.__async_client.data_modeling.data_models.apply(data_model=data_model))
