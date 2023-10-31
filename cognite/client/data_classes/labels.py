from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class LabelDefinition(CogniteResource):
    """A label definition is a globally defined label that can later be attached to resources (e.g., assets). For example, can you define a "Pump" label definition and attach that label to your pump assets.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): Name of the label.
        description (str | None): Description of the label.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        data_set_id (int | None): The id of the dataset this label belongs to.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        created_time: int | None = None,
        data_set_id: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.description = description
        self.created_time = created_time
        self.data_set_id = data_set_id
        self._cognite_client = cast("CogniteClient", cognite_client)


class LabelDefinitionFilter(CogniteFilter):
    """Filter on labels definitions with strict matching.

    Args:
        name (str | None): Returns the label definitions matching that name.
        external_id_prefix (str | None): filter label definitions with external ids starting with the prefix specified
        data_set_ids (list[dict[str, Any]] | None): Only include labels that belong to these datasets.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        data_set_ids: list[dict[str, Any]] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.name = name
        self.external_id_prefix = external_id_prefix
        self.data_set_ids = data_set_ids
        self._cognite_client = cast("CogniteClient", cognite_client)


class LabelDefinitionList(CogniteResourceList[LabelDefinition]):
    _RESOURCE = LabelDefinition


class Label(CogniteObject):
    """A label assigned to a resource.

    Args:
        external_id (str | None): The external id to the attached label.
        **_ (Any): No description.
    """

    def __init__(self, external_id: str | None = None, **_: Any) -> None:
        self.external_id = external_id

    @classmethod
    def _load_list(cls, labels: Sequence[str | dict | LabelDefinition | Label] | None) -> list[Label] | None:
        def convert_label(label: Label | str | LabelDefinition | dict) -> Label:
            if isinstance(label, Label):
                return label
            elif isinstance(label, str):
                return Label(label)
            elif isinstance(label, LabelDefinition):
                return Label(label.external_id)
            elif isinstance(label, dict):
                if "externalId" in label:
                    return Label(label["externalId"])
            raise ValueError(f"Could not parse label: {label}")

        if labels is None:
            return None
        return [convert_label(label) for label in labels]


class LabelFilter(CogniteFilter):
    """Return only the resource matching the specified label constraints.

    Args:
        contains_any (list[str] | None): The resource item contains at least one of the listed labels. The labels are defined by a list of external ids.
        contains_all (list[str] | None): The resource item contains all the listed labels. The labels are defined by a list of external ids.
        cognite_client (CogniteClient | None): The client to associate with this object.

    Examples:

            List only resources marked as a PUMP and VERIFIED::

                >>> from cognite.client.data_classes import LabelFilter
                >>> my_label_filter = LabelFilter(contains_all=["PUMP", "VERIFIED"])

            List only resources marked as a PUMP or as a VALVE::

                >>> from cognite.client.data_classes import LabelFilter
                >>> my_label_filter = LabelFilter(contains_any=["PUMP", "VALVE"])
    """

    def __init__(
        self,
        contains_any: list[str] | None = None,
        contains_all: list[str] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.contains_any = contains_any
        self.contains_all = contains_all
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, label_filter: dict[str, Any]) -> LabelFilter:
        return cls(
            contains_any=(any_ := label_filter.get("containsAny")) and [item["externalId"] for item in any_],
            contains_all=(all_ := label_filter.get("containsAll")) and [item["externalId"] for item in all_],
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        dumped: dict[str, Any] = {}
        if self.contains_any:
            dumped["containsAny"] = [
                {"externalId" if camel_case else "external_id": item} for item in self.contains_any
            ]
        if self.contains_all:
            dumped["containsAll"] = [
                {"externalId" if camel_case else "external_id": item} for item in self.contains_all
            ]
        return dumped
