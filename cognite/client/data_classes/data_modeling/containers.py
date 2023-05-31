from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Type, TypeVar, cast

from cognite.client.data_classes._base import (
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling.core import (
    ConstraintIdentifier,
    ContainerPropertyIdentifier,
    DataModelingResource,
    IndexIdentifier,
)
from cognite.client.utils._text import to_snake_case
from cognite.client.utils._validation import validate_data_modeling_identifier

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Container(DataModelingResource):
    """Represent the physical storage of data.

    Args:
        space (str): The workspace for the view.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        description (str): Textual description of the view
        name (str): Human readable name for the view.
        used_for (Literal['node', 'edge', 'all']): Should this operation apply to nodes, edges or both.
        properties (dict[str, ContainerPropertyIdentifier]): We index the property by a local unique identifier.
        constraints (dict[str, ConstraintIdentifier]): Set of constraints to apply to the container
        indexes (dict[str, IndexIdentifier]): Set of indexes to apply to the container.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        space: str = None,
        external_id: str = None,
        description: str = None,
        name: str = None,
        used_for: Literal["node", "edge", "all"] = None,
        properties: dict[str, ContainerPropertyIdentifier] = None,
        constraints: dict[str, ConstraintIdentifier] = None,
        indexes: dict[str, IndexIdentifier] = None,
        last_updated_time: int = None,
        created_time: int = None,
        cognite_client: CogniteClient = None,
    ):
        validate_data_modeling_identifier(space, external_id)
        self.space = space
        self.external_id = external_id
        self.description = description
        self.name = name
        self.used_for = used_for
        self.properties = properties
        self.constraints = constraints
        self.indexes = indexes
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)


T_ResourceClass = TypeVar("T_ResourceClass")


def unwrap_if_dict(value: dict[str, Any], resource_class: Type[T_ResourceClass]) -> dict[str, T_ResourceClass] | None:
    if not value:
        return None
    return (
        {k: resource_class(**{to_snake_case(kk): vv for kk, vv in v.items()}) for k, v in value.items()}
        if isinstance(value, dict)
        else value
    )


class ContainerList(CogniteResourceList):
    _RESOURCE = Container
