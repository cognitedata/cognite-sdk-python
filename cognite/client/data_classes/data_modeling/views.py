from __future__ import annotations

import json
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Literal, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling.core import (
    ViewPropertyDefinition,
    ViewReference,
    load_view_property_definition,
)
from cognite.client.data_classes.data_modeling.dsl_filter import DSLFilter, dump_dsl_filter, load_dsl_filter
from cognite.client.utils._text import (
    convert_all_keys_to_camel_case,
    convert_all_keys_to_snake_case,
)
from cognite.client.utils._validation import validate_data_modeling_identifier

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class View(CogniteResource):
    """A group of properties.

    Args:
        space (str): The workspace for the view.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        description (str): Textual description of the view
        name (str): Human readable name for the view.
        filter (dict): A filter Domain Specific Language (DSL) used to create advanced filter queries.
        implements (list): References to the views from where this view will inherit properties and edges.
        version (str): DMS version.
        writable (bool): Whether the view supports write operations.
        used_for (Literal["node", "edge", "all"]): Does this view apply to nodes, edges or both.
        is_global (bool): Whether this is a global container, i.e., one of the out-of-the-box models.
        properties (dict): View with included properties and expected edges, indexed by a unique space-local identifier.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        space: str = None,
        external_id: str = None,
        description: str = None,
        name: str = None,
        filter: DSLFilter | None = None,
        implements: list[ViewReference] = None,
        version: str = None,
        writable: bool = False,
        used_for: Literal["node", "edge", "all"] = "node",
        is_global: bool = False,
        properties: dict[str, ViewPropertyDefinition] = None,
        last_updated_time: int = None,
        created_time: int = None,
        cognite_client: CogniteClient = None,
    ):
        validate_data_modeling_identifier(space, external_id)
        self.space = space
        self.external_id = external_id
        self.description = description
        self.name = name
        self.filter = filter
        self.implements = implements
        self.version = version
        self.writable = writable
        self.used_for = used_for
        self.is_global = is_global
        self.properties = properties
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient = None) -> View:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "properties" in data:
            data["properties"] = {k: load_view_property_definition(v) for k, v in data["properties"].items()} or None
        if "implements" in data:
            data["implements"] = [
                ViewReference(**convert_all_keys_to_snake_case(v)) for v in data["implements"]
            ] or None
        if "filter" in data:
            data["filter"] = load_dsl_filter(data["filter"])

        return cast(View, super()._load(data, cognite_client))

    def dump(self, camel_case: bool = False, exclude_not_supported_by_apply_endpoint: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)

        if "implements" in output:
            output["implements"] = [
                convert_all_keys_to_camel_case(asdict(v)) if camel_case else asdict(v) for v in output["implements"]
            ]
        if "filter" in output:
            output["filter"] = dump_dsl_filter(output["filter"])

        if "properties" in output:
            output["properties"] = {
                k: v.dump(camel_case, exclude_not_supported_by_apply_endpoint) for k, v in output["properties"].items()
            }

        if exclude_not_supported_by_apply_endpoint:
            for exclude in [
                "writable",
                "usedFor",
                "isGlobal",
                "lastUpdatedTime",
                "createdTime",
                "is_global",
                "used_for",
                "last_updated_time",
                "created_time",
            ]:
                output.pop(exclude, None)
        return output


class ViewList(CogniteResourceList):
    _RESOURCE = View


class ViewFilter(CogniteFilter):
    """Represent the filer arguments for the list endpoint.

    Args:
        space (str | None): The space to query
        include_inherited_properties (bool): Whether to include properties inherited from views this view implements.
        all_versions (bool): Whether to return all versions. If false, only the newest version is returned,
                             which is determined based on the 'createdTime' field.
        include_global (bool): Whether to include global views.
    """

    def __init__(
        self,
        space: str = None,
        include_inherited_properties: bool = True,
        all_versions: bool = False,
        include_global: bool = False,
    ):
        self.space = space
        self.include_inherited_properties = include_inherited_properties
        self.all_versions = all_versions
        self.include_global = include_global
