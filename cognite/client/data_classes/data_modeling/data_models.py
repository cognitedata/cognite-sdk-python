from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, List, Literal, Union, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.shared import DataModeling, ViewReference
from cognite.client.data_classes.data_modeling.views import View, ViewApply

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class DataModelCore(DataModeling):
    """A group of views.

    Args:
        space (str): The workspace for the data model, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the data model.
        description (str): Textual description of the data model
        name (str): Human readable name for the data model.
        version (str): DMS version.
    """

    def __init__(
        self,
        space: str = None,
        external_id: str = None,
        description: str = None,
        name: str = None,
        version: str = None,
        cognite_client: CogniteClient = None,
    ):
        validate_data_modeling_identifier(space, external_id)
        self.space = space
        self.external_id = external_id
        self.description = description
        self.name = name
        self.version = version
        self._cognite_client = cast("CogniteClient", cognite_client)


class DataModelApply(DataModelCore):
    """A group of views. This is the write version of a Data Model.

    Args:
        space (str): The workspace for the data model, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the data model.
        description (str): Textual description of the data model
        name (str): Human readable name for the data model.
        version (str): DMS version.
        views (list[ViewReference | ViewApply]): List of views included in this data model.
    """

    def __init__(
        self,
        space: str = None,
        external_id: str = None,
        description: str = None,
        name: str = None,
        version: str = None,
        views: list[ViewReference | ViewApply] = None,
        cognite_client: CogniteClient = None,
    ):
        super().__init__(space, external_id, description, name, version, cognite_client)
        self.views = views

    @classmethod
    def _load_view(cls, view_data: dict, cognite_client: CogniteClient = None) -> ViewReference | ViewApply:
        if "type" in view_data:
            return ViewReference.load(view_data)
        else:
            return ViewApply._load(view_data, cognite_client)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient = None) -> DataModelApply:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "views" in data:
            data["views"] = [cls._load_view(v) for v in data["views"]] or None

        return cast(DataModelApply, super()._load(data, cognite_client))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)

        if self.views:
            output["views"] = [v.dump(camel_case) for v in self.views]

        return output


class DataModel(DataModelCore):
    """A group of views. This is the read version of a Data Model

    Args:
        space (str): The workspace for the data model, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the data model.
        description (str): Textual description of the data model
        name (str): Human readable name for the data model.
        version (str): DMS version.
        views (list): List of views included in this data model.
        is_global (bool): Whether this is a global data model.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        space: str = None,
        external_id: str = None,
        description: str = None,
        name: str = None,
        version: str = None,
        views: list[ViewReference | View] = None,
        is_global: bool = False,
        last_updated_time: int = None,
        created_time: int = None,
        cognite_client: CogniteClient = None,
    ):
        super().__init__(space, external_id, description, name, version, cognite_client)
        self.views = views
        self.is_global = is_global
        self.last_updated_time = last_updated_time
        self.created_time = created_time

    @classmethod
    def _load_view(cls, view_data: dict, cognite_client: CogniteClient = None) -> ViewReference | ViewApply:
        if "type" in view_data:
            return ViewReference.load(view_data)
        else:
            return View._load(view_data, cognite_client)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient = None) -> DataModel:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "views" in data:
            data["views"] = [cls._load_view(v) for v in data["views"]] or None

        return cast(DataModel, super()._load(data, cognite_client))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)

        if self.views:
            output["views"] = [v.dump(camel_case) for v in self.views]

        return output

    def to_data_model_apply(self) -> DataModelApply:
        views = None
        if self.views:
            views = cast(
                List[Union[ViewReference, ViewApply]],
                [v.to_view_apply() if isinstance(v, View) else v for v in self.views],
            )

        return DataModelApply(
            space=self.space,
            external_id=self.external_id,
            description=self.description,
            name=self.name,
            version=self.version,
            views=views,
            cognite_client=self._cognite_client,
        )


class DataModelApplyList(CogniteResourceList):
    _RESOURCE = DataModelApply


class DataModelList(CogniteResourceList):
    _RESOURCE = DataModel

    def to_data_model_apply_list(self) -> DataModelApplyList:
        return DataModelApplyList(resources=[d.to_data_model_apply() for d in self.items])


class DataModelFilter(CogniteFilter):
    """Represent the filer arguments for the list endpoint.

    Args:
        space (str | None): The space to query
        inline_views (bool): Whether to expand the referenced views inline in the returned result.
        all_versions (bool): Whether to return all versions. If false, only the newest version is returned,
                             which is determined based on the 'createdTime' field.
        include_global (bool): Whether to include global views.
    """

    def __init__(
        self,
        space: str = None,
        inline_views: bool = False,
        all_versions: bool = False,
        include_global: bool = False,
    ):
        self.space = space
        self.inline_views = inline_views
        self.all_versions = all_versions
        self.include_global = include_global


class DataModelsSort(CogniteFilter):
    def __init__(
        self,
        property: Literal[
            "space", "external_id", "name", "description", "version", "created_time", "last_updated_time"
        ],
        direction: Literal["ascending", "descending"] = "ascending",
        nulls_first: bool = False,
    ):
        self.property = property
        self.direction = direction
        self.nulls_first = nulls_first
