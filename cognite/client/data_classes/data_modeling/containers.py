from __future__ import annotations

import json
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Literal, cast

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling.core import (
    ConstraintIdentifier,
    ContainerPropertyIdentifier,
    IndexIdentifier,
    load_constraint_identifier,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case_nested
from cognite.client.utils._validation import validate_data_modeling_identifier

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Container(CogniteResource):
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
        is_global: bool = False,
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
        self.is_global = is_global
        self.properties = properties
        self.constraints = constraints
        self.indexes = indexes
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient = None) -> Container:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "properties" in data:
            data["properties"] = {k: ContainerPropertyIdentifier.load(v) for k, v in data["properties"].items()} or None
        if "constraints" in data:
            data["constraints"] = {k: load_constraint_identifier(v) for k, v in data["constraints"].items()} or None
        if "indexes" in data:
            data["indexes"] = {k: IndexIdentifier.load(v) for k, v in data["indexes"].items()} or None

        return super()._load(data, cognite_client)

    def dump(self, camel_case: bool = False, exclude_not_supported_by_apply_endpoint: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        for field in ["properties", "constraints", "indexes"]:
            if field not in output:
                continue
            output[field] = {
                k: convert_all_keys_to_camel_case_nested(asdict(v)) if camel_case else asdict(v)
                for k, v in output[field].items()
            }

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


class ContainerList(CogniteResourceList):
    _RESOURCE = Container
