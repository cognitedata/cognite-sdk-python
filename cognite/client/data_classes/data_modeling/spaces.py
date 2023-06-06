from __future__ import annotations

from typing import TYPE_CHECKING, cast

from cognite.client.data_classes._base import (
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.shared import DataModelingResource

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class SpaceCore(DataModelingResource):
    """A workspace for data models and instances.

    Args:
        space (str): A unique identifier for space.
        description (str): Textual description of the space
        name (str): Human readable name for the space.
    """

    def __init__(
        self,
        space: str = None,
        description: str = None,
        name: str = None,
        cognite_client: CogniteClient = None,
    ):
        validate_data_modeling_identifier(space)
        self.space = space
        self.description = description
        self.name = name
        self._cognite_client = cast("CogniteClient", cognite_client)


class SpaceApply(SpaceCore):
    """A workspace for data models and instances. This is the write version"""

    ...


class Space(SpaceCore):
    """A workspace for data models and instances. This is the read version.

    Args:
        space (str): a unique identifier for the space.
        description (str): Textual description of the space
        name (str): Human readable name for the space.
        is_global (bool): Whether the space is global or not.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        space: str = None,
        description: str = None,
        name: str = None,
        is_global: bool = False,
        last_updated_time: int = None,
        created_time: int = None,
        cognite_client: CogniteClient = None,
    ):
        super().__init__(space, description, name, cognite_client)
        self.is_global = is_global
        self.last_updated_time = last_updated_time
        self.created_time = created_time

    def as_apply(self) -> SpaceApply:
        return SpaceApply(
            space=self.space,
            description=self.description,
            name=self.name,
            cognite_client=self._cognite_client,
        )


class SpaceApplyList(CogniteResourceList):
    _RESOURCE = SpaceApply


class SpaceList(CogniteResourceList):
    _RESOURCE = Space

    def to_space_apply_list(self) -> SpaceApplyList:
        return SpaceApplyList(
            resources=[item.as_apply() for item in self.items],
            cognite_client=self._cognite_client,
        )
