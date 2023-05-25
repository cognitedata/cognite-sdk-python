from __future__ import annotations

from typing import TYPE_CHECKING, cast

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient

_RESERVED_SPACE_IDS = frozenset({"space", "cdf", "dms", "pg3", "shared", "system", "node", "edge"})


class Space(CogniteResource):
    """A workspace for data models and instances.

    Args:
        space (str): a unique identifier for the space.
        description (str): Textual description of the space
        name (str): Human readable name for the space.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        space: str = None,
        description: str = None,
        name: str = None,
        last_updated_time: int = None,
        created_time: int = None,
        cognite_client: CogniteClient = None,
    ):
        if space in _RESERVED_SPACE_IDS:
            raise ValueError(f"The space ID: {space} is reserved. Please use another ID.")
        self.space = space
        self.description = description
        self.name = name
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def __repr__(self) -> str:
        space = self.space
        return f"{type(self).__name__}({space=}) at 0x{hex(id(self)).upper().zfill(16)}"


class SpaceList(CogniteResourceList):
    _RESOURCE = Space
