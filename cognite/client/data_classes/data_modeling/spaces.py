from __future__ import annotations

from abc import ABC
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.core import WritableDataModelingResource

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class SpaceCore(WritableDataModelingResource["SpaceApply"], ABC):
    """A workspace for data models and instances.

    Args:
        space (str): A unique identifier for the space.
        description (str | None): Textual description of the space
        name (str | None): Human readable name for the space.
    """

    def __init__(self, space: str, description: str | None, name: str | None) -> None:
        super().__init__(space)
        self.description = description
        self.name = name

    def as_id(self) -> str:
        return self.space


class SpaceApply(SpaceCore):
    """A workspace for data models and instances. This is the write version

    Args:
        space (str): A unique identifier for the space.
        description (str | None): Textual description of the space
        name (str | None): Human readable name for the space.
    """

    def __init__(self, space: str, description: str | None = None, name: str | None = None) -> None:
        validate_data_modeling_identifier(space)
        super().__init__(space, description, name)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            space=resource["space"],
            description=resource.get("description"),
            name=resource.get("name"),
        )

    def as_write(self) -> SpaceApply:
        """Returns this SpaceApply instance."""
        return self


class Space(SpaceCore):
    """A workspace for data models and instances. This is the read version.

    Args:
        space (str): A unique identifier for the space.
        is_global (bool): Whether the space is global or not.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description (str | None): Textual description of the space
        name (str | None): Human readable name for the space.
    """

    def __init__(
        self,
        space: str,
        is_global: bool,
        last_updated_time: int,
        created_time: int,
        description: str | None = None,
        name: str | None = None,
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

    def as_write(self) -> SpaceApply:
        return self.as_apply()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            space=resource["space"],
            is_global=resource["isGlobal"],
            last_updated_time=resource["lastUpdatedTime"],
            created_time=resource["createdTime"],
            description=resource.get("description"),
            name=resource.get("name"),
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


class SpaceList(WriteableCogniteResourceList[SpaceApply, Space]):
    _RESOURCE = Space

    def _build_id_mappings(self) -> None:
        self._space_to_item = {inst.space: inst for inst in self.data}

    def get(self, space: str) -> Space | None:  # type: ignore [override]
        """Get a space object from this list by space ID.

        Args:
            space (str): The space identifier to get.

        Returns:
            Space | None: The requested space if present, else None
        """
        return self._space_to_item.get(space)

    def extend(self, other: Iterable[Any]) -> None:
        other_res_list = type(self)(other)  # See if we can accept the types
        if self._space_to_item.keys().isdisjoint(other_res_list._space_to_item):
            self.data.extend(other_res_list.data)
            self._space_to_item.update(other_res_list._space_to_item)
        else:
            raise ValueError("Unable to extend as this would introduce duplicates")

    def as_ids(self) -> list[str]:
        """
        Converts all the spaces to a space id list.

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
            cognite_client=self._get_cognite_client(),
        )

    def as_write(self) -> SpaceApplyList:
        return self.as_apply()
