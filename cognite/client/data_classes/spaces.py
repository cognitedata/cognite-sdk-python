from __future__ import annotations

from typing import TYPE_CHECKING, cast

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient

_reserved_space_ids = {"space", "cdf", "dms", "pg3", "shared", "system", "node", "edge"}


class Space(CogniteResource):
    """A workspace for data models and instances.

    Args:
        id (str): An unique identifier for the space.
        description (str): Textual description of the space
        name (str): Human readable name for the space.
    """

    def __init__(
        self,
        id: str = None,
        description: str = None,
        name: str = None,
        cognite_client: CogniteClient = None,
    ):
        if id in _reserved_space_ids:
            raise ValueError(f"The ID: {id} is reserved. Please use another ID.")
        self.id = id
        self.description = description
        self.name = name
        self._cognite_client = cast("CogniteClient", cognite_client)


class SpaceList(CogniteResourceList):
    _RESOURCE = Space
