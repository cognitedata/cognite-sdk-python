from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from typing import Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils.useful_types import SequenceNotStr


class LabelDefinitionCore(WriteableCogniteResource["LabelDefinitionWrite"], ABC):
    """A label definition is a globally defined label that can later be attached to resources (e.g., assets). For example, can you define a "Pump" label definition and attach that label to your pump assets.
    This is the parent for the reading and write versions.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): Name of the label.
        description (str | None): Description of the label.
        data_set_id (int | None): The id of the dataset this label belongs to.
    """

    def __init__(
        self,
        external_id: str,
        name: str | None,
        description: str | None,
        data_set_id: int | None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.description = description
        self.data_set_id = data_set_id


class LabelDefinition(LabelDefinitionCore):
    """A label definition is a globally defined label that can later be attached to resources (e.g., assets). For example, can you define a "Pump" label definition and attach that label to your pump assets.
    This is the read version of the LabelDefinition class. It is used when retrieving existing label definitions.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): Name of the label.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description (str | None): Description of the label.
        data_set_id (int | None): The id of the dataset this label belongs to.
    """

    def __init__(
        self,
        external_id: str,
        name: str,
        created_time: int,
        description: str | None,
        data_set_id: int | None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            description=description,
            data_set_id=data_set_id,
        )
        self.created_time = created_time

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            created_time=resource["createdTime"],
            description=resource.get("description"),
            data_set_id=resource.get("dataSetId"),
        )

    def as_write(self) -> LabelDefinitionWrite:
        """Returns this LabelDefinition in its write version."""
        if self.external_id is None or self.name is None:
            raise ValueError("External ID and name are required for the write version of a label definition.")
        return LabelDefinitionWrite(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            data_set_id=self.data_set_id,
        )


class LabelDefinitionWrite(LabelDefinitionCore):
    """A label definition is a globally defined label that can later be attached to resources (e.g., assets). For example, can you define a "Pump" label definition and attach that label to your pump assets.
    This is the write version of the LabelDefinition class. It is used when creating new label definitions.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): Name of the label.
        description (str | None): Description of the label.
        data_set_id (int | None): The id of the dataset this label belongs to.
    """

    def __init__(
        self,
        external_id: str,
        name: str,
        description: str | None = None,
        data_set_id: int | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            description=description,
            data_set_id=data_set_id,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> LabelDefinitionWrite:
        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            description=resource.get("description"),
            data_set_id=resource.get("dataSetId"),
        )

    def as_write(self) -> LabelDefinitionWrite:
        """Returns this LabelDefinitionWrite instance."""
        return self


class LabelDefinitionFilter(CogniteFilter):
    """Filter on labels definitions with strict matching.

    Args:
        name (str | None): Returns the label definitions matching that name.
        external_id_prefix (str | None): filter label definitions with external ids starting with the prefix specified
        data_set_ids (list[dict[str, Any]] | None): Only include labels that belong to these datasets.
    """

    def __init__(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        data_set_ids: list[dict[str, Any]] | None = None,
    ) -> None:
        self.name = name
        self.external_id_prefix = external_id_prefix
        self.data_set_ids = data_set_ids


class LabelDefinitionWriteList(CogniteResourceList[LabelDefinitionWrite], ExternalIDTransformerMixin):
    _RESOURCE = LabelDefinitionWrite


class LabelDefinitionList(
    WriteableCogniteResourceList[LabelDefinitionWrite, LabelDefinition], ExternalIDTransformerMixin
):
    _RESOURCE = LabelDefinition

    def as_write(self) -> LabelDefinitionWriteList:
        """Returns this LabelDefinitionList in its write version."""
        return LabelDefinitionWriteList([item.as_write() for item in self.data])


class Label(CogniteResource):
    """A label assigned to a resource.

    Args:
        external_id (str): The external id to the attached label.
    """

    def __init__(self, external_id: str) -> None:
        self.external_id = external_id

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(external_id=resource["externalId"])

    @classmethod
    def _load_list(
        cls,
        labels: SequenceNotStr[str | dict | LabelDefinitionCore | Label]
        | Sequence[dict | LabelDefinitionCore | Label]
        | None,
    ) -> list[Label] | None:
        def convert_label(label: Label | str | LabelDefinitionCore | dict) -> Label:
            if isinstance(label, Label):
                return label
            elif isinstance(label, str):
                return Label(label)
            elif isinstance(label, LabelDefinitionCore):
                return Label(label.external_id)
            elif isinstance(label, dict):
                if "externalId" in label:
                    return Label(label["externalId"])
                if "external_id" in label:
                    return Label(label["external_id"])
            raise ValueError(f"Could not parse label: {label}")

        if labels is None:
            return None
        return [convert_label(label) for label in labels]


class LabelFilter(CogniteFilter):
    """Return only the resource matching the specified label constraints.

    Args:
        contains_any (list[str] | None): The resource item contains at least one of the listed labels. The labels are defined by a list of external ids.
        contains_all (list[str] | None): The resource item contains all the listed labels. The labels are defined by a list of external ids.

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
    ) -> None:
        self.contains_any = contains_any
        self.contains_all = contains_all

    @classmethod
    def _load(cls, label_filter: dict[str, Any]) -> LabelFilter:
        return cls(
            contains_any=(any_ := label_filter.get("containsAny")) and [item["externalId"] for item in any_],
            contains_all=(all_ := label_filter.get("containsAll")) and [item["externalId"] for item in all_],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
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
