from __future__ import annotations

from typing import Any

from cognite.client.data_classes._base import (
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling._core import DataModelingResource
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier


class SpaceCore(DataModelingResource):
    """A workspace for data models and instances.

    Args:
        space (str): A unique identifier for space.
        description (str | None): Textual description of the space
        name (str | None): Human readable name for the space.
    """

    def __init__(self, space: str, description: str | None = None, name: str | None = None) -> None:
        self.space = space
        self.description = description
        self.name = name

    def as_id(self) -> str:
        return self.space


class SpaceApply(SpaceCore):
    """A workspace for data models and instances. This is the write version

    Args:
        space (str): A unique identifier for space.
        description (str | None): Textual description of the space
        name (str | None): Human readable name for the space.
        **_ (Any): No description.
    """

    def __init__(
        self,
        space: str,
        description: str | None = None,
        name: str | None = None,
        **_: Any,
    ) -> None:
        validate_data_modeling_identifier(space)
        super().__init__(space, description, name)


class Space(SpaceCore):
    """A workspace for data models and instances. This is the read version.

    Args:
        space (str): a unique identifier for the space.
        is_global (bool): Whether the space is global or not.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description (str | None): Textual description of the space
        name (str | None): Human readable name for the space.
        **_ (Any): No description.
    """

    def __init__(
        self,
        space: str,
        is_global: bool,
        last_updated_time: int,
        created_time: int,
        description: str | None = None,
        name: str | None = None,
        **_: Any,
    ) -> None:
        super().__init__(space, description, name)
        self.is_global = is_global
        self.last_updated_time = last_updated_time
        self.created_time = created_time

    def as_apply(self) -> SpaceApply:
        return SpaceApply(
            space=self.space,
            description=self.description,
            name=self.name,
        )


class SpaceApplyList(CogniteResourceList[SpaceApply]):
    _RESOURCE = SpaceApply

    def as_ids(self) -> list[str]:
        """
        Converts all the spaces to a space id list.

        Returns:
            list[str]: A list of space ids.
        """
        return [item.space for item in self]


class SpaceList(CogniteResourceList[Space]):
    _RESOURCE = Space

    def as_ids(self) -> list[str]:
        """
        Converts all the spaces to a space id list..

        Returns:
            list[str]: A list of space ids.
        """
        return [item.space for item in self]

    def as_apply(self) -> SpaceApplyList:
        """
        Converts all the spaces to a space apply list.

        Returns:
            SpaceApplyList: A list of space applies.
        """
        return SpaceApplyList(
            resources=[item.as_apply() for item in self],
            cognite_client=self._cognite_client,
        )
