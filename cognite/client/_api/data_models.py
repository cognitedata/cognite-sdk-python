from __future__ import annotations

from typing import Iterator, Sequence, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DATA_MODEL_LIST_LIMIT_DEFAULT
from cognite.client.data_classes.data_modeling.data_models import DataModel, DataModelFilter, DataModelList
from cognite.client.data_classes.data_modeling.ids import DataModelId, VersionedDataModelingId
from cognite.client.utils._identifier import DataModelingIdentifierSequence


class DataModelsAPI(APIClient):
    _RESOURCE_PATH = "/models/datamodels"

    def __call__(
        self,
        chunk_size: int = None,
        limit: int = None,
        space: str | None = None,
        inline_views: bool = False,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> Iterator[DataModel] | Iterator[DataModelList]:
        """Iterate over data_models

        Fetches data_models as they are iterated over, so you keep a limited number of data_models in memory.

        Args:
            chunk_size (int, optional): Number of data_models to return in each chunk. Defaults to yielding one data_model a time.
            limit (int, optional): Maximum number of data_models to return. Default to return all items.
            space: (str | None): The space to query.
            inline_views (bool): Whether to expand the referenced views inline in the returned result.
            all_versions (bool): Whether to return all versions. If false, only the newest version is returned,
                                 which is determined based on the 'createdTime' field.
            include_global (bool): Whether to include global views.

        Yields:
            Union[DataModel, DataModelList]: yields DataModel one by one if chunk_size is not specified, else DataModelList objects.
        """
        DataModelFilter(space, inline_views, all_versions, include_global)

        return self._list_generator(
            list_cls=DataModelList,
            resource_cls=DataModel,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
        )

    def __iter__(self) -> Iterator[DataModel]:
        """Iterate over data_models

        Fetches data_models as they are iterated over, so you keep a limited number of data_models in memory.

        Yields:
            DataModel: yields DataModels one by one.
        """
        return cast(Iterator[DataModel], self())

    @overload
    def retrieve(self, ids: DataModelId) -> DataModel | None:
        ...

    @overload
    def retrieve(self, ids: Sequence[DataModelId]) -> DataModelList:
        ...

    def retrieve(self, ids: DataModelId | Sequence[DataModelId]) -> DataModel | DataModelList | None:
        """`Retrieve a single data_model by id. <https://docs.cognite.com/api/v1/#tag/DataModels/operation/byDataModelIdsDataModels>`_

        Args:
            ids (DataModelId | Sequence[DataModelId]): Data Model identifier(s).

        Returns:
            Optional[DataModel]: Requested data_model or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_modeling.data_models.retrieve(data_model='myDataModel')

        """
        identifier = DataModelingIdentifierSequence.load(ids)
        return self._retrieve_multiple(list_cls=DataModelList, resource_cls=DataModel, identifiers=identifier)

    def delete(self, ids: DataModelId | Sequence[DataModelId]) -> list[VersionedDataModelingId]:
        """`Delete one or more data_models <https://docs.cognite.com/api/v1/#tag/DataModels/operation/deleteDataModelsV3>`_

        Args:
            ids (DataModelId | Sequence[DataModelId]): Data Model identifier(s).
        Returns:
            The data_model(s) which has been deleted. None if nothing was deleted.
        Examples:

            Delete data_models by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.data_modeling.data_models.delete(data_model=["myDataModel", "myOtherDataModel"])
        """
        deleted_data_models = cast(
            list,
            self._delete_multiple(
                identifiers=DataModelingIdentifierSequence.load(ids),
                wrap_ids=True,
            ),
        )
        return [
            VersionedDataModelingId(item["space"], item["externalId"], item["version"]) for item in deleted_data_models
        ]

    def list(
        self,
        limit: int = DATA_MODEL_LIST_LIMIT_DEFAULT,
        space: str | None = None,
        inline_views: bool = False,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> DataModelList:
        """`List data_models <https://docs.cognite.com/api/v1/#tag/DataModels/operation/listDataModelsV3>`_

        Args:
            limit (int, optional): Maximum number of data_models to return. Default to 10. Set to -1, float("inf") or None
                to return all items.
            space: (str | None): The space to query.
            inline_views (bool): Whether to expand the referenced views inline in the returned result.
            all_versions (bool): Whether to return all versions. If false, only the newest version is returned,
                                 which is determined based on the 'createdTime' field.
            include_global (bool): Whether to include global views.

        Returns:
            DataModelList: List of requested data_models

        Examples:

            List data_models and filter on max start time::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> data_model_list = c.data_modeling.data_models.list(limit=5)

            Iterate over data_models::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for data_model in c.data_modeling.data_models:
                ...     data_model # do something with the data_model

            Iterate over chunks of data_models to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for data_model_list in c.data_modeling.data_models(chunk_size=2500):
                ...     data_model_list # do something with the data_models
        """
        filter_ = DataModelFilter(space, inline_views, all_versions, include_global)

        return self._list(
            list_cls=DataModelList,
            resource_cls=DataModel,
            method="GET",
            limit=limit,
            filter=filter_.dump(camel_case=True),
        )

    @overload
    def apply(self, data_model: Sequence[DataModel]) -> DataModelList:
        ...

    @overload
    def apply(self, data_model: DataModel) -> DataModel:
        ...

    def apply(self, data_model: DataModel | Sequence[DataModel]) -> DataModel | DataModelList:
        """`Create or patch one or more data_models. <https://docs.cognite.com/api/v1/#tag/DataModels/operation/ApplyDataModels>`_

        Args:
            data_model (data_model: DataModel | Sequence[DataModel]): DataModel or data_models of data_modelsda to create or update.

        Returns:
            DataModel | DataModelList: Created data_model(s)

        Examples:

            Create new data_models::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_models import DataModel
                >>> c = CogniteClient()
                >>> data_models = [DataModel(data_model="myDataModel", description="My first data_model", name="My DataModel"),
                ... DataModel(data_model="myOtherDataModel", description="My second data_model", name="My Other DataModel")]
                >>> res = c.data_modeling.data_models.create(data_models)
        """
        return self._create_multiple(list_cls=DataModelList, resource_cls=DataModel, items=data_model)
