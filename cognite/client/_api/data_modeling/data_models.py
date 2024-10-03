from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DATA_MODELING_DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling.data_models import (
    DataModel,
    DataModelApply,
    DataModelFilter,
    DataModelList,
)
from cognite.client.data_classes.data_modeling.ids import DataModelId, DataModelIdentifier, ViewId, _load_identifier
from cognite.client.data_classes.data_modeling.views import View
from cognite.client.utils._concurrency import ConcurrencySettings

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class DataModelsAPI(APIClient):
    _RESOURCE_PATH = "/models/datamodels"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._DELETE_LIMIT = 100
        self._RETRIEVE_LIMIT = 100
        self._CREATE_LIMIT = 100

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
    ) -> Iterator[DataModel] | Iterator[DataModelList]:
        """Iterate over data model

        Fetches data model as they are iterated over, so you keep a limited number of data model in memory.

        Args:
            chunk_size (int | None): Number of data model to return in each chunk. Defaults to yielding one data_model a time.
            limit (int | None): Maximum number of data model to return. Defaults to returning all items.
            space (str | None): The space to query.
            inline_views (bool): Whether to expand the referenced views inline in the returned result.
            all_versions (bool): Whether to return all versions. If false, only the newest version is returned, which is determined based on the 'createdTime' field.
            include_global (bool): Whether to include global views.

        Returns:
            Iterator[DataModel] | Iterator[DataModelList]: yields DataModel one by one if chunk_size is not specified, else DataModelList objects.
        """
        filter = DataModelFilter(space, inline_views, all_versions, include_global)

        return self._list_generator(
            list_cls=DataModelList,
            resource_cls=DataModel,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            filter=filter.dump(camel_case=True),
        )

    def __iter__(self) -> Iterator[DataModel]:
        """Iterate over data model

        Fetches data model as they are iterated over, so you keep a limited number of data model in memory.

        Returns:
            Iterator[DataModel]: yields DataModels one by one.
        """
        return self()

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
        """`Retrieve data_model(s) by id(s). <https://developer.cognite.com/api#tag/Data-models/operation/byExternalIdsDataModels>`_

        Args:
            ids (DataModelIdentifier | Sequence[DataModelIdentifier]): Data Model identifier(s).
            inline_views (bool): Whether to expand the referenced views inline in the returned result.

        Returns:
            DataModelList[ViewId] | DataModelList[View]: Requested data model(s) or empty if none exist.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.data_models.retrieve(("mySpace", "myDataModel", "v1"))
        """
        identifier = _load_identifier(ids, "data_model")
        return self._retrieve_multiple(
            list_cls=DataModelList,
            resource_cls=DataModel,
            identifiers=identifier,
            params={"inlineViews": inline_views},
            executor=ConcurrencySettings.get_data_modeling_executor(),
        )

    def delete(self, ids: DataModelIdentifier | Sequence[DataModelIdentifier]) -> list[DataModelId]:
        """`Delete one or more data model <https://developer.cognite.com/api#tag/Data-models/operation/deleteDataModels>`_

        Args:
            ids (DataModelIdentifier | Sequence[DataModelIdentifier]): Data Model identifier(s).
        Returns:
            list[DataModelId]: The data_model(s) which has been deleted. None if nothing was deleted.
        Examples:

            Delete data model by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.data_modeling.data_models.delete(("mySpace", "myDataModel", "v1"))
        """
        deleted_data_models = cast(
            list,
            self._delete_multiple(
                identifiers=_load_identifier(ids, "data_model"),
                wrap_ids=True,
                returns_items=True,
                executor=ConcurrencySettings.get_data_modeling_executor(),
            ),
        )
        return [DataModelId(item["space"], item["externalId"], item["version"]) for item in deleted_data_models]

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
        """`List data models <https://developer.cognite.com/api#tag/Data-models/operation/listDataModels>`_

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

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> data_model_list = client.data_modeling.data_models.list(limit=5)

            Iterate over data model:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for data_model in client.data_modeling.data_models:
                ...     data_model # do something with the data_model

            Iterate over chunks of data model to reduce memory load:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for data_model_list in client.data_modeling.data_models(chunk_size=10):
                ...     data_model_list # do something with the data model
        """
        filter = DataModelFilter(space, inline_views, all_versions, include_global)

        return self._list(
            list_cls=DataModelList,
            resource_cls=DataModel,
            method="GET",
            limit=limit,
            filter=filter.dump(camel_case=True),
        )

    @overload
    def apply(self, data_model: Sequence[DataModelApply]) -> DataModelList: ...

    @overload
    def apply(self, data_model: DataModelApply) -> DataModel: ...

    def apply(self, data_model: DataModelApply | Sequence[DataModelApply]) -> DataModel | DataModelList:
        """`Create or update one or more data model. <https://developer.cognite.com/api#tag/Data-models/operation/createDataModels>`_

        Args:
            data_model (DataModelApply | Sequence[DataModelApply]): Data model(s) to create or update (upsert).

        Returns:
            DataModel | DataModelList: Created data model(s)

        Examples:

            Create new data model::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import DataModelApply
                >>> client = CogniteClient()
                >>> data_models = [
                ...     DataModelApply(space="mySpace",external_id="myDataModel",version="v1"),
                ...     DataModelApply(space="mySpace",external_id="myOtherDataModel",version="v1")]
                >>> res = client.data_modeling.data_models.apply(data_models)
        """
        return self._create_multiple(
            list_cls=DataModelList,
            resource_cls=DataModel,
            items=data_model,
            input_resource_cls=DataModelApply,
            executor=ConcurrencySettings.get_data_modeling_executor(),
        )
