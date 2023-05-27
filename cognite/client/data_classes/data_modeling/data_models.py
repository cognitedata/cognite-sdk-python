from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling.core import ViewReference
from cognite.client.data_classes.data_modeling.views import View
from cognite.client.utils._validation import validate_data_modeling_identifier

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class DataModel(CogniteResource):
    """A group of properties.

    Args:
        space (str): The workspace for the view.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        description (str): Textual description of the view
        name (str): Human readable name for the view.
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
        validate_data_modeling_identifier(space, external_id)
        self.space = space
        self.external_id = external_id
        self.description = description
        self.name = name
        self.version = version
        self.views = views
        self.is_global = is_global
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load_view(cls, view_data: dict, cognite_client: CogniteClient = None) -> ViewReference | View:
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

    def dump(self, camel_case: bool = False, exclude_not_supported_by_apply_endpoint: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)

        if "views" in output:
            output["views"] = [v.dump(camel_case, exclude_not_supported_by_apply_endpoint) for v in output["views"]]

        if exclude_not_supported_by_apply_endpoint:
            for exclude in [
                "isGlobal",
                "lastUpdatedTime",
                "createdTime",
                "is_global",
                "last_updated_time",
                "created_time",
            ]:
                output.pop(exclude, None)
        return output


class DataModelList(CogniteResourceList):
    _RESOURCE = DataModel


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
        property: Literal["space", "externalId", "name", "description", "version", "createdTime", "lastUpdatedTime"],
        direction: Literal["ascending", "descending"] = "ascending",
        nulls_first: bool = False,
    ):
        self.property = property
        self.direction = direction
        self.nulls_first = nulls_first
