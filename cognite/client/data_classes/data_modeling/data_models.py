from __future__ import annotations

import json
from operator import attrgetter
from typing import Any, Generic, Literal, TypeVar, Union, cast

from cognite.client.data_classes._base import CogniteFilter, CogniteResourceList
from cognite.client.data_classes.data_modeling._core import DataModelingResource, DataModelingSort
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.ids import DataModelId, ViewId
from cognite.client.data_classes.data_modeling.views import View, ViewApply


class DataModelCore(DataModelingResource):
    """A group of views.

    Args:
        space (str): The workspace for the data model, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the data model.
        version (str): DMS version.
        description (str | None): Textual description of the data model
        name (str | None): Human readable name for the data model.
        **_ (Any): No description.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        description: str | None = None,
        name: str | None = None,
        **_: Any,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.description = description
        self.name = name
        self.version = version

    def as_id(self) -> DataModelId:
        return DataModelId(space=self.space, external_id=self.external_id, version=self.version)


class DataModelApply(DataModelCore):
    """A group of views. This is the write version of a Data Model.

    Args:
        space (str): The workspace for the data model, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the data model.
        version (str): DMS version.
        description (str | None): Textual description of the data model
        name (str | None): Human readable name for the data model.
        views (list[ViewId | ViewApply] | None): List of views included in this data model.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        description: str | None = None,
        name: str | None = None,
        views: list[ViewId | ViewApply] | None = None,
    ) -> None:
        validate_data_modeling_identifier(space, external_id)
        super().__init__(space, external_id, version, description, name)
        self.views = views

    @classmethod
    def _load_view(cls, view_data: dict) -> ViewId | ViewApply:
        if "type" in view_data:
            return ViewId.load(view_data)
        else:
            return ViewApply.load(view_data)

    @classmethod
    def load(cls, resource: dict | str) -> DataModelApply:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "views" in data:
            data["views"] = [cls._load_view(v) for v in data["views"]] or None
        return super().load(data)

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)

        if self.views:
            output["views"] = [v.dump(camel_case) for v in self.views]

        return output


T_View = TypeVar("T_View", bound=Union[ViewId, View])


class DataModel(DataModelCore, Generic[T_View]):
    """A group of views. This is the read version of a Data Model

    Args:
        space (str): The workspace for the data model, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the data model.
        version (str): DMS version.
        is_global (bool): Whether this is a global data model.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description (str | None): Textual description of the data model
        name (str | None): Human readable name for the data model.
        views (list[T_View] | None): List of views included in this data model.
        **_ (Any): No description.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        is_global: bool,
        last_updated_time: int,
        created_time: int,
        description: str | None = None,
        name: str | None = None,
        views: list[T_View] | None = None,
        **_: Any,
    ) -> None:
        super().__init__(space, external_id, version, description, name)
        self.views: list[T_View] = views or []
        self.is_global = is_global
        self.last_updated_time = last_updated_time
        self.created_time = created_time

    @classmethod
    def _load_view(cls, view_data: dict) -> ViewId | View:
        if "type" in view_data:
            return ViewId.load(view_data)
        else:
            return View.load(view_data)

    @classmethod
    def load(cls, resource: dict | str) -> DataModel:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "views" in data:
            data["views"] = [cls._load_view(v) for v in data["views"]] or None

        return cast(DataModel, super().load(data))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)

        if self.views:
            output["views"] = [v.dump(camel_case) for v in self.views]

        return output

    def as_apply(self) -> DataModelApply:
        views: list[ViewId | ViewApply] = []
        for view in self.views:
            if isinstance(view, View):
                views.append(view.as_apply())
            elif isinstance(view, ViewId):
                views.append(view)
            else:
                raise ValueError(f"Unexpected type {type(view)}")

        return DataModelApply(
            space=self.space,
            external_id=self.external_id,
            version=self.version,
            description=self.description,
            name=self.name,
            views=views,
        )


class DataModelApplyList(CogniteResourceList[DataModelApply]):
    _RESOURCE = DataModelApply

    def as_ids(self) -> list[DataModelId]:
        """
        Convert the list of data models to a list of data model ids.

        Returns:
            list[DataModelId]: The list of data model ids.
        """
        return [d.as_id() for d in self]


class DataModelList(CogniteResourceList[DataModel[T_View]]):
    _RESOURCE = DataModel

    def as_apply(self) -> DataModelApplyList:
        """
        Convert the list of data models to a list of data model applies.

        Returns:
            DataModelApplyList: The list of data model applies.
        """
        return DataModelApplyList([d.as_apply() for d in self])

    def latest_version(self, key: Literal["created_time", "last_updated_time"] = "created_time") -> DataModel[T_View]:
        """
        Get the data model in the list with the latest version. The latest version is determined based on the
        created_time or last_updated_time field.

        Args:
            key (Literal["created_time", "last_updated_time"]): The field to use for determining the latest version.

        Returns:
            DataModel[T_View]: The data model with the latest version.
        """
        if not self:
            raise ValueError("No data models in list")
        if key not in ("created_time", "last_updated_time"):
            raise ValueError(f"Unexpected key {key}")
        return max(self, key=attrgetter(key))

    def as_ids(self) -> list[DataModelId]:
        """
        Convert the list of data models to a list of data model ids.

        Returns:
            list[DataModelId]: The list of data model ids.
        """
        return [d.as_id() for d in self]


class DataModelFilter(CogniteFilter):
    """Represent the filer arguments for the list endpoint.

    Args:
        space (str | None): The space to query
        inline_views (bool): Whether to expand the referenced views inline in the returned result.
        all_versions (bool): Whether to return all versions. If false, only the newest version is returned, which is determined based on the 'createdTime' field.
        include_global (bool): Whether to include global views.
    """

    def __init__(
        self,
        space: str | None = None,
        inline_views: bool = False,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> None:
        self.space = space
        self.inline_views = inline_views
        self.all_versions = all_versions
        self.include_global = include_global


class DataModelsSort(DataModelingSort):
    def __init__(
        self,
        property: Literal[
            "space", "external_id", "name", "description", "version", "created_time", "last_updated_time"
        ],
        direction: Literal["ascending", "descending"] = "ascending",
        nulls_first: bool = False,
    ) -> None:
        super().__init__(property, direction, nulls_first)
