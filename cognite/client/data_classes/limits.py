from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

from cognite.client.data_classes._base import CogniteObject, CogniteResource, CogniteResourceList

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class LimitValue(CogniteResource):
    """A singular representation of a Limit.

    Limits are identified by an id containing the service name and a service-scoped limit name.
    For instance `atlas.monthly_ai_tokens` is the id of the `atlas` service limit `monthly_ai_tokens`.
    Service and limit names are always in `lower_snake_case`.

    Args:
        limit_id (str | None): Limits are identified by an id containing the service name and a service-scoped limit name.
        value (float | int | None): The numeric value of the limit.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        limit_id: str | None = None,
        value: float | int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.limit_id = limit_id
        self.value = value
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> LimitValue:
        return cls(
            limit_id=resource.get("limitId"),
            value=resource.get("value"),
            cognite_client=cognite_client,
        )


class LimitValueList(CogniteResourceList[LimitValue]):
    """A list of flattened limit items.

    Args:
        resources (Sequence[LimitValue]): List of limit values.
        next_cursor (str | None): Cursor to get the next page of results (if available).
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    _RESOURCE = LimitValue

    def __init__(
        self,
        resources: Sequence[LimitValue],
        next_cursor: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(resources, cognite_client=cognite_client)
        self.next_cursor = next_cursor

    @classmethod
    def _load(
        cls,
        resource_list: Iterable[dict[str, Any]],
        cognite_client: CogniteClient | None = None,
    ) -> LimitValueList:
        # Handle case where we get a dict with items and nextCursor (from API response)
        if isinstance(resource_list, dict):
            items = resource_list.get("items", [])
            next_cursor = resource_list.get("nextCursor")
            resources = [cls._RESOURCE._load(resource, cognite_client=cognite_client) for resource in items]
            return cls(resources=resources, next_cursor=next_cursor, cognite_client=cognite_client)

        # Handle case where we get a list/iterable of items directly
        resources = [cls._RESOURCE._load(resource, cognite_client=cognite_client) for resource in resource_list]
        return cls(resources=resources, cognite_client=cognite_client)

    def dump_raw(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the list with nextCursor in addition to items.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representation of the list with items and nextCursor.
        """
        output: dict[str, Any] = {"items": self.dump(camel_case)}
        if self.next_cursor is not None:
            output["nextCursor"] = self.next_cursor
        return output


class LimitValuePrefixFilter(CogniteObject):
    """Prefix filter for limit values.

    Args:
        property (list[str] | None): List of properties to filter on.
        value (str | None): The prefix value to filter by.
    """

    def __init__(self, property: list[str] | None = None, value: str | None = None) -> None:
        self.property = property
        self.value = value

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.property is not None:
            result["property"] = self.property
        if self.value is not None:
            result["value"] = self.value
        return result


class LimitValueFilter(CogniteObject):
    """Filter to apply to the list operation.

    To retrieve all limits for a specific service, use the "prefix" operator where the property
    is the limit's key, e.g., `{"prefix": {"property": ["limitId"], "value": "atlas."}}`

    Args:
        prefix (LimitValuePrefixFilter | dict[str, Any] | None): Prefix filter object or dict.
    """

    prefix: LimitValuePrefixFilter | None

    def __init__(self, prefix: LimitValuePrefixFilter | dict[str, Any] | None = None) -> None:
        if isinstance(prefix, dict):
            self.prefix = LimitValuePrefixFilter(
                property=prefix.get("property"),
                value=prefix.get("value"),
            )
        else:
            self.prefix = prefix

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> LimitValueFilter:
        instance = super()._load(resource, cognite_client)
        if instance.prefix is not None and isinstance(instance.prefix, dict):
            instance.prefix = LimitValuePrefixFilter(
                property=instance.prefix.get("property"),
                value=instance.prefix.get("value"),
            )
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if self.prefix is None:
            return {}
        return {"prefix": self.prefix.dump(camel_case)}
