from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cognite.client.data_classes._base import CogniteResource

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class LimitValue(CogniteResource):
    """A singular representation of a Limit.

    Limits are identified by an id containing the service name and a service-scoped limit name.
    For instance `atlas.monthly_ai_tokens` is the id of the `atlas` service limit `monthly_ai_tokens`.
    Service and limit names are always in `lower_snake_case`.

    Args:
        limit_id (str | None): Limits are identified by an id containing the service name and a service-scoped limit name.
        value (float | int | None): The numeric value of the limit.
        cognite_client (AsyncCogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        limit_id: str | None = None,
        value: float | int | None = None,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        self.limit_id = limit_id
        self.value = value
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> LimitValue:
        return cls(
            limit_id=resource.get("limitId"),
            value=resource.get("value"),
            cognite_client=cognite_client,
        )
