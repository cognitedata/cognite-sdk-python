from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case, to_camel_case

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


class Label(dict):
    """A label assigned to a resource.

    Args:
        external_id (str | None): The external id to the attached label.
        **kwargs (Any): No description.
    """

    def __init__(self, external_id: str | None = None, **kwargs: Any) -> None:
        self.external_id = external_id
        self.update(kwargs)

    external_id = CognitePropertyClassUtil.declare_property("externalId")

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

    @classmethod
    def _load(cls, raw_label: dict[str, Any]) -> Label:
        return cls(external_id=raw_label["externalId"])

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)


class LabelFilter(dict, CogniteFilter):
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

    @staticmethod
    def _wrap_labels(values: list[str] | None) -> list[dict[str, str]] | None:
        if values is None:
            return None
        return [{"externalId": v} for v in values]

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        keys = map(to_camel_case, self.keys()) if camel_case else self.keys()
        return dict(zip(keys, map(self._wrap_labels, self.values())))

    contains_any = CognitePropertyClassUtil.declare_property("containsAny")
    contains_all = CognitePropertyClassUtil.declare_property("containsAll")
