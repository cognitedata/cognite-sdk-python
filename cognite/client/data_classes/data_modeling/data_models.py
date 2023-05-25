from __future__ import annotations

from typing import TYPE_CHECKING, cast

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
)
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
        views: list = None,
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
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)


class DataModelList(CogniteResourceList):
    _RESOURCE = DataModel
