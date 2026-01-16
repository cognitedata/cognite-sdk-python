from __future__ import annotations

import copy
import typing
from abc import ABC
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

from typing_extensions import Self

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.assets import Asset
from cognite.client.data_classes.events import Event
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.labels import Label, LabelDefinition, LabelDefinitionWrite, LabelFilter
from cognite.client.data_classes.sequences import Sequence
from cognite.client.data_classes.time_series import TimeSeries
from cognite.client.utils.useful_types import SequenceNotStr

RelationshipType: TypeAlias = Literal["asset", "timeseries", "file", "event", "sequence"]


class RelationshipCore(WriteableCogniteResource["RelationshipWrite"], ABC):
    """Representation of a relationship in CDF, consists of a source and a target and some additional parameters.

    Args:
        external_id (str): External id of the relationship, must be unique within the project.
        source_external_id (str): External id of the CDF resource that constitutes the relationship source.
        source_type (str): The CDF resource type of the relationship source. Must be one of the specified values.
        target_external_id (str): External id of the CDF resource that constitutes the relationship target.
        target_type (str): The CDF resource type of the relationship target. Must be one of the specified values.
        start_time (int | None): Time, in milliseconds since Jan. 1, 1970, when the relationship became active. If there is no startTime, relationship is active from the beginning of time until endTime.
        end_time (int | None): Time, in milliseconds since Jan. 1, 1970, when the relationship became inactive. If there is no endTime, relationship is active from startTime until the present or any point in the future. If endTime and startTime are set, then endTime must be strictly greater than startTime.
        confidence (float | None): Confidence value of the existence of this relationship. Generated relationships should provide a realistic score on the likelihood of the existence of the relationship. Relationships without a confidence value can be interpreted at the discretion of each project.
        data_set_id (int | None): The id of the dataset this relationship belongs to.
        labels (list[Label] | None): A list of the labels associated with this resource item.
    """

    _RESOURCE_TYPE_MAP: typing.ClassVar[dict[str, type[Asset | TimeSeries | FileMetadata | Event | Sequence]]] = {
        "asset": Asset,
        "timeseries": TimeSeries,
        "file": FileMetadata,
        "event": Event,
        "sequence": Sequence,
    }

    def __init__(
        self,
        external_id: str,
        source_external_id: str,
        source_type: str,
        target_external_id: str,
        target_type: str,
        start_time: int | None,
        end_time: int | None,
        confidence: float | None,
        data_set_id: int | None,
        labels: list[Label] | None,
    ) -> None:
        self.external_id = external_id
        self.source_external_id = source_external_id
        self.source_type = source_type
        self.target_external_id = target_external_id
        self.target_type = target_type
        self.start_time = start_time
        self.end_time = end_time
        self.confidence = confidence
        self.data_set_id = data_set_id
        self.labels = labels

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = super().dump(camel_case)
        if self.labels is not None:
            result["labels"] = [label.dump(camel_case) for label in self.labels]
        return result

    def _validate_resource_types(self) -> Self:
        rel = copy.copy(self)
        self._validate_resource_type(rel.source_type)
        self._validate_resource_type(rel.target_type)
        return rel

    def _validate_resource_type(self, resource_type: str | None) -> None:
        if resource_type is None or resource_type.lower() not in self._RESOURCE_TYPE_MAP:
            raise TypeError(f"Invalid source or target '{resource_type}' in relationship")


class Relationship(RelationshipCore):
    """Representation of a relationship in CDF, consists of a source and a target and some additional parameters.
    This is the read version of the relationship class, it is used when retrieving from CDF.

    Args:
        external_id (str): External id of the relationship, must be unique within the project.
        created_time (int): Time, in milliseconds since Jan. 1, 1970, when this relationship was created in CDF.
        last_updated_time (int): Time, in milliseconds since Jan. 1, 1970, when this relationship was last updated in CDF.
        source_external_id (str): External id of the CDF resource that constitutes the relationship source.
        source_type (str): The CDF resource type of the relationship source. Must be one of the specified values.
        source (Asset | TimeSeries | FileMetadata | Sequence | Event | dict[str, Any] | None): The full resource referenced by the source_external_id and source_type fields.
        target_external_id (str): External id of the CDF resource that constitutes the relationship target.
        target_type (str): The CDF resource type of the relationship target. Must be one of the specified values.
        target (Asset | TimeSeries | FileMetadata | Sequence | Event | dict[str, Any] | None): The full resource referenced by the target_external_id and target_type fields.
        start_time (int | None): Time, in milliseconds since Jan. 1, 1970, when the relationship became active. If there is no startTime, relationship is active from the beginning of time until endTime.
        end_time (int | None): Time, in milliseconds since Jan. 1, 1970, when the relationship became inactive. If there is no endTime, relationship is active from startTime until the present or any point in the future. If endTime and startTime are set, then endTime must be strictly greater than startTime.
        confidence (float | None): Confidence value of the existence of this relationship. Generated relationships should provide a realistic score on the likelihood of the existence of the relationship. Relationships without a confidence value can be interpreted at the discretion of each project.
        data_set_id (int | None): The id of the dataset this relationship belongs to.
        labels (SequenceNotStr[Label | str | LabelDefinition | dict] | None): A list of the labels associated with this resource item.
    """

    def __init__(
        self,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        source_external_id: str,
        source_type: str,
        source: Asset | TimeSeries | FileMetadata | Sequence | Event | dict[str, Any] | None,
        target_external_id: str,
        target_type: str,
        target: Asset | TimeSeries | FileMetadata | Sequence | Event | dict[str, Any] | None,
        start_time: int | None,
        end_time: int | None,
        confidence: float | None,
        data_set_id: int | None,
        labels: SequenceNotStr[Label | str | LabelDefinition | dict] | None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            source_external_id=source_external_id,
            source_type=source_type,
            target_external_id=target_external_id,
            target_type=target_type,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            data_set_id=data_set_id,
            labels=Label._load_list(labels),
        )
        self.source = source
        self.target = target
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    def as_write(self) -> RelationshipWrite:
        """Returns this Relationship in its write version."""
        if self.external_id is None:
            raise ValueError("External ID is required for the write version of a relationship.")
        source_external_id, source_type = self._get_external_id_and_type(
            self.source_external_id, self.source_type, self.source
        )
        target_external_id, target_type = self._get_external_id_and_type(
            self.target_external_id, self.target_type, self.target
        )
        return RelationshipWrite(
            external_id=self.external_id,
            source_external_id=source_external_id,
            source_type=source_type,
            target_external_id=target_external_id,
            target_type=target_type,
            start_time=self.start_time,
            end_time=self.end_time,
            confidence=self.confidence,
            data_set_id=self.data_set_id,
            labels=self.labels,
        )

    @staticmethod
    def _get_external_id_and_type(
        external_id: str | None,
        resource_type: str | None,
        resource: Asset | TimeSeries | FileMetadata | Sequence | Event | dict | None,
    ) -> tuple[str, RelationshipType]:
        if external_id is None and (resource is None or isinstance(resource, dict)):
            raise ValueError("Creating a relationship requires either an external id or a loaded resource")
        external_id = external_id if isinstance(external_id, str) else (resource and resource.external_id)  # type: ignore[union-attr,assignment]
        if external_id is None:
            raise ValueError("Missing external id on loaded resource")
        resource_type = resource_type or type(resource).__name__.lower()
        return external_id, cast(RelationshipType, resource_type)

    @classmethod
    def _load(cls, resource: dict) -> Relationship:
        return cls(
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            source_external_id=resource["sourceExternalId"],
            source_type=resource["sourceType"],
            source=(source := resource.get("source")) and cls._convert_resource(source, resource["sourceType"]),
            target_external_id=resource["targetExternalId"],
            target_type=resource["targetType"],
            target=(target := resource.get("target")) and cls._convert_resource(target, resource["targetType"]),
            start_time=resource.get("startTime"),
            end_time=resource.get("endTime"),
            confidence=resource.get("confidence"),
            data_set_id=resource.get("dataSetId"),
            labels=(labels := resource.get("labels")) and Label._load_list(labels),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = super().dump(camel_case)
        if self.source is not None and not isinstance(self.source, dict):
            result["source"] = self.source.dump(camel_case)
        if self.target is not None and not isinstance(self.target, dict):
            result["target"] = self.target.dump(camel_case)
        return result

    def _maybe_set_client_ref(self, client: AsyncCogniteClient) -> Self:
        # We do not use the client ref, but our source and target resources most likely do, so we
        # propagate the reference to them:
        if self.source is not None and not isinstance(self.source, dict):
            self.source._maybe_set_client_ref(client)
        if self.target is not None and not isinstance(self.target, dict):
            self.target._maybe_set_client_ref(client)
        return self

    @classmethod
    def _convert_resource(
        cls, resource: dict[str, Any], resource_type: str | None
    ) -> dict[str, Any] | TimeSeries | Asset | Sequence | FileMetadata | Event:
        if resource_type and (resource_cls := cls._RESOURCE_TYPE_MAP.get(resource_type.lower())):
            return resource_cls._load(resource)
        return resource


class RelationshipWrite(RelationshipCore):
    """Representation of a relationship in CDF, consists of a source and a target and some additional parameters.
    This is the write version of the relationship class, and is used when creating new relationships.

    Args:
        external_id (str): External id of the relationship, must be unique within the project.
        source_external_id (str): External id of the CDF resource that constitutes the relationship source.
        source_type (RelationshipType): The CDF resource type of the relationship source. Must be one of the specified values.
        target_external_id (str): External id of the CDF resource that constitutes the relationship target.
        target_type (RelationshipType): The CDF resource type of the relationship target. Must be one of the specified values.
        start_time (int | None): Time, in milliseconds since Jan. 1, 1970, when the relationship became active. If there is no startTime, relationship is active from the beginning of time until endTime.
        end_time (int | None): Time, in milliseconds since Jan. 1, 1970, when the relationship became inactive. If there is no endTime, relationship is active from startTime until the present or any point in the future. If endTime and startTime are set, then endTime must be strictly greater than startTime.
        confidence (float | None): Confidence value of the existence of this relationship. Generated relationships should provide a realistic score on the likelihood of the existence of the relationship. Relationships without a confidence value can be interpreted at the discretion of each project.
        data_set_id (int | None): The id of the dataset this relationship belongs to.
        labels (SequenceNotStr[Label | str | LabelDefinitionWrite | dict] | None): A list of the labels associated with this resource item.
    """

    def __init__(
        self,
        external_id: str,
        source_external_id: str,
        source_type: RelationshipType,
        target_external_id: str,
        target_type: RelationshipType,
        start_time: int | None = None,
        end_time: int | None = None,
        confidence: float | None = None,
        data_set_id: int | None = None,
        labels: SequenceNotStr[Label | str | LabelDefinitionWrite | dict] | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            source_external_id=source_external_id,
            source_type=source_type,
            target_external_id=target_external_id,
            target_type=target_type,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            data_set_id=data_set_id,
            labels=Label._load_list(labels),
        )

    @classmethod
    def _load(cls, resource: dict) -> RelationshipWrite:
        return cls(
            external_id=resource["externalId"],
            source_external_id=resource["sourceExternalId"],
            source_type=resource["sourceType"],
            target_external_id=resource["targetExternalId"],
            target_type=resource["targetType"],
            start_time=resource.get("startTime"),
            end_time=resource.get("endTime"),
            confidence=resource.get("confidence"),
            data_set_id=resource.get("dataSetId"),
            labels=(labels := resource.get("labels")) and Label._load_list(labels),
        )

    def as_write(self) -> RelationshipWrite:
        """Returns this RelationshipWrite instance."""
        return self


class RelationshipFilter(CogniteFilter):
    """Filter on relationships with exact match. Multiple filter elements in one property, e.g. `sourceExternalIds: [ "a", "b" ]`, will return all relationships where the `sourceExternalId` field is either `a` or `b`. Filters in multiple properties will return the relationships that match all criteria. If the filter is not specified it default to an empty filter.

    Args:
        source_external_ids (SequenceNotStr[str] | None): Include relationships that have any of these values in their `sourceExternalId` field
        source_types (SequenceNotStr[str] | None): Include relationships that have any of these values in their `sourceType` field
        target_external_ids (SequenceNotStr[str] | None): Include relationships that have any of these values in their `targetExternalId` field
        target_types (SequenceNotStr[str] | None): Include relationships that have any of these values in their `targetType` field
        data_set_ids (typing.Sequence[dict[str, Any]] | None): Either one of `internalId` (int) or `externalId` (str)
        start_time (dict[str, int] | None): Range between two timestamps, minimum and maximum milliseconds (inclusive)
        end_time (dict[str, int] | None): Range between two timestamps, minimum and maximum milliseconds (inclusive)
        confidence (dict[str, int] | None): Range to filter the field for (inclusive).
        last_updated_time (dict[str, int] | None): Range to filter the field for (inclusive).
        created_time (dict[str, int] | None): Range to filter the field for (inclusive).
        active_at_time (dict[str, int] | None): Limits results to those active at any point within the given time range, i.e. if there is any overlap in the intervals [activeAtTime.min, activeAtTime.max] and [startTime, endTime], where both intervals are inclusive. If a relationship does not have a startTime, it is regarded as active from the beginning of time by this filter. If it does not have an endTime is will be regarded as active until the end of time. Similarly, if a min is not supplied to the filter, the min will be implicitly set to the beginning of time, and if a max is not supplied, the max will be implicitly set to the end of time.
        labels (LabelFilter | None): Return only the resource matching the specified label constraints.
    """

    def __init__(
        self,
        source_external_ids: SequenceNotStr[str] | None = None,
        source_types: SequenceNotStr[str] | None = None,
        target_external_ids: SequenceNotStr[str] | None = None,
        target_types: SequenceNotStr[str] | None = None,
        data_set_ids: typing.Sequence[dict[str, Any]] | None = None,
        start_time: dict[str, int] | None = None,
        end_time: dict[str, int] | None = None,
        confidence: dict[str, int] | None = None,
        last_updated_time: dict[str, int] | None = None,
        created_time: dict[str, int] | None = None,
        active_at_time: dict[str, int] | None = None,
        labels: LabelFilter | None = None,
    ) -> None:
        self.source_external_ids = source_external_ids
        self.source_types = source_types
        self.target_external_ids = target_external_ids
        self.target_types = target_types
        self.data_set_ids = data_set_ids
        self.start_time = start_time
        self.end_time = end_time
        self.confidence = confidence
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self.active_at_time = active_at_time
        self.labels = labels

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        if isinstance(self.labels, LabelFilter):
            result["labels"] = self.labels.dump(camel_case)
        return result


class RelationshipUpdate(CogniteUpdate):
    """Update applied to a single relationship

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.

    """

    # Relationships have only an external_id and do not expose id
    def __init__(self, external_id: str) -> None:
        self._id = None
        self._external_id = external_id
        self._update_object = {}

    class _PrimitiveRelationshipUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> RelationshipUpdate:
            return self._set(value)

    class _LabelRelationshipUpdate(CogniteLabelUpdate):
        def add(self, value: str | list[str]) -> RelationshipUpdate:
            return self._add(value)

        def remove(self, value: str | list[str]) -> RelationshipUpdate:
            return self._remove(value)

    @property
    def source_external_id(self) -> _PrimitiveRelationshipUpdate:
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "sourceExternalId")

    @property
    def source_type(self) -> _PrimitiveRelationshipUpdate:
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "sourceType")

    @property
    def target_external_id(self) -> _PrimitiveRelationshipUpdate:
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "targetExternalId")

    @property
    def target_type(self) -> _PrimitiveRelationshipUpdate:
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "targetType")

    @property
    def start_time(self) -> _PrimitiveRelationshipUpdate:
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "startTime")

    @property
    def end_time(self) -> _PrimitiveRelationshipUpdate:
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "endTime")

    @property
    def data_set_id(self) -> _PrimitiveRelationshipUpdate:
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "dataSetId")

    @property
    def confidence(self) -> _PrimitiveRelationshipUpdate:
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "confidence")

    @property
    def labels(self) -> _LabelRelationshipUpdate:
        return RelationshipUpdate._LabelRelationshipUpdate(self, "labels")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("source_type", is_nullable=False),
            PropertySpec("source_external_id", is_nullable=False),
            PropertySpec("target_type", is_nullable=False),
            PropertySpec("target_external_id", is_nullable=False),
            PropertySpec("confidence"),
            PropertySpec("start_time"),
            PropertySpec("end_time"),
            PropertySpec("data_set_id"),
            PropertySpec("labels", is_list=True),
        ]


class RelationshipWriteList(CogniteResourceList[RelationshipWrite], ExternalIDTransformerMixin):
    _RESOURCE = RelationshipWrite


class RelationshipList(WriteableCogniteResourceList[RelationshipWrite, Relationship], ExternalIDTransformerMixin):
    _RESOURCE = Relationship

    def as_write(self) -> RelationshipWriteList:
        """Returns this RelationshipList in its write version."""
        return RelationshipWriteList([item.as_write() for item in self.data])
