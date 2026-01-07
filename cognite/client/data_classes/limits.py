from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class Limit(CogniteResource):
    """A singular representation of a Limit.

    Limits are identified by an id containing the service name and a service-scoped limit name.
    For instance `atlas.monthly_ai_tokens` is the id of the `atlas` service limit `monthly_ai_tokens`.
    Service and limit names are always in `lower_snake_case`.

    Args:
        limit_id (str): Limits are identified by an id containing the service name and a service-scoped limit name.
        value (float | int): The numeric value of the limit.
        cognite_client (AsyncCogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        limit_id: str,
        value: float | int,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        self.limit_id = limit_id
        self.value = value
        self._cognite_client = cognite_client

    def as_id(self) -> str:
        """Returns the limit ID."""
        return self.limit_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Limit:
        return cls(
            limit_id=resource["limitId"],
            value=resource["value"],
            cognite_client=cognite_client,
        )


class LimitList(CogniteResourceList[Limit]):
    _RESOURCE = Limit

    def as_ids(self) -> list[str]:
        """Returns a list of limit IDs."""
        return [limit.limit_id for limit in self]
