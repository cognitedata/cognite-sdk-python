from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any, cast
from typing import Sequence as SequenceType

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
)
from cognite.client.data_classes.assets import Asset
from cognite.client.data_classes.events import Event
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.labels import Label, LabelDefinition, LabelFilter
from cognite.client.data_classes.sequences import Sequence
from cognite.client.data_classes.time_series import TimeSeries

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Relationship(CogniteResource):
    """Representation of a relationship in CDF, consists of a source and a target and some additional parameters.

    Args:
        external_id (str | None): External id of the relationship, must be unique within the project.
        source_external_id (str | None): External id of the CDF resource that constitutes the relationship source.
        source_type (str | None): The CDF resource type of the relationship source. Must be one of the specified values.
        source (Asset | TimeSeries | FileMetadata | Sequence | Event | dict | None): The full resource referenced by the source_external_id and source_type fields.
        target_external_id (str | None): External id of the CDF resource that constitutes the relationship target.
        target_type (str | None): The CDF resource type of the relationship target. Must be one of the specified values.
        target (Asset | TimeSeries | FileMetadata | Sequence | Event | dict | None): The full resource referenced by the target_external_id and target_type fields.
        start_time (int | None): Time, in milliseconds since Jan. 1, 1970, when the relationship became active. If there is no startTime, relationship is active from the beginning of time until endTime.
        end_time (int | None): Time, in milliseconds since Jan. 1, 1970, when the relationship became inactive. If there is no endTime, relationship is active from startTime until the present or any point in the future. If endTime and startTime are set, then endTime must be strictly greater than startTime.
        confidence (float | None): Confidence value of the existence of this relationship. Generated relationships should provide a realistic score on the likelihood of the existence of the relationship. Relationships without a confidence value can be interpreted at the discretion of each project.
        data_set_id (int | None): The id of the dataset this relationship belongs to.
        labels (SequenceType[Label | str | LabelDefinition | dict] | None): A list of the labels associated with this resource item.
        created_time (int | None): Time, in milliseconds since Jan. 1, 1970, when this relationship was created in CDF.
        last_updated_time (int | None): Time, in milliseconds since Jan. 1, 1970, when this relationship was last updated in CDF.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    _RESOURCE_TYPES = frozenset({"asset", "timeseries", "file", "event", "sequence"})

    def __init__(
        self,
        external_id: str | None = None,
        source_external_id: str | None = None,
        source_type: str | None = None,
        source: Asset | TimeSeries | FileMetadata | Sequence | Event | dict | None = None,
        target_external_id: str | None = None,
        target_type: str | None = None,
        target: Asset | TimeSeries | FileMetadata | Sequence | Event | dict | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        confidence: float | None = None,
        data_set_id: int | None = None,
        labels: SequenceType[Label | str | LabelDefinition | dict] | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.external_id = external_id
        self.source_external_id = source_external_id
        self.source_type = source_type
        self.source = source
        self.target_external_id = target_external_id
        self.target_type = target_type
        self.target = target
        self.start_time = start_time
        self.end_time = end_time
        self.confidence = confidence
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.labels = Label._load_list(labels)
        self._cognite_client = cast("CogniteClient", cognite_client)

    def _validate_resource_types(self) -> Relationship:
        rel = copy.copy(self)
        self._validate_resource_type(rel.source_type)
        self._validate_resource_type(rel.target_type)
        return rel

    def _validate_resource_type(self, resource_type: str | None) -> None:
        if resource_type is None or resource_type.lower() not in self._RESOURCE_TYPES:
            raise TypeError(f"Invalid source or target '{resource_type}' in relationship")

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> Relationship:
        instance = super()._load(resource, cognite_client)
        if instance.source is not None:
            instance.source = instance._convert_resource(instance.source, instance.source_type)  # type: ignore
        if instance.target is not None:
            instance.target = instance._convert_resource(instance.target, instance.target_type)  # type: ignore
        instance.labels = Label._load_list(instance.labels)
        return instance

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        result: dict[str, Any] = super().dump(camel_case)
        if self.labels is not None:
            result["labels"] = [label.dump(camel_case) for label in self.labels]
        return result

    def _convert_resource(
        self, resource: dict[str, Any], resource_type: str | None
    ) -> dict[str, Any] | CogniteResource:
        resource_map: dict[str, type[CogniteResource]] = {
            "timeSeries": TimeSeries,
            "asset": Asset,
            "sequence": Sequence,
            "file": FileMetadata,
            "event": Event,
        }
        if resource_type in resource_map:
            return resource_map[resource_type]._load(resource, self._cognite_client)
        return resource


class RelationshipFilter(CogniteFilter):
    """Filter on relationships with exact match. Multiple filter elements in one property, e.g. `sourceExternalIds: [ "a", "b" ]`, will return all relationships where the `sourceExternalId` field is either `a` or `b`. Filters in multiple properties will return the relationships that match all criteria. If the filter is not specified it default to an empty filter.

    Args:
        source_external_ids (SequenceType[str] | None): Include relationships that have any of these values in their `sourceExternalId` field
        source_types (SequenceType[str] | None): Include relationships that have any of these values in their `sourceType` field
        target_external_ids (SequenceType[str] | None): Include relationships that have any of these values in their `targetExternalId` field
        target_types (SequenceType[str] | None): Include relationships that have any of these values in their `targetType` field
        data_set_ids (SequenceType[dict[str, Any]] | None): Either one of `internalId` (int) or `externalId` (str)
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
        source_external_ids: SequenceType[str] | None = None,
        source_types: SequenceType[str] | None = None,
        target_external_ids: SequenceType[str] | None = None,
        target_types: SequenceType[str] | None = None,
        data_set_ids: SequenceType[dict[str, Any]] | None = None,
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

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
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
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("source_type", is_nullable=False),
            PropertySpec("source_external_id", is_nullable=False),
            PropertySpec("target_type", is_nullable=False),
            PropertySpec("target_external_id", is_nullable=False),
            PropertySpec("confidence"),
            PropertySpec("start_time"),
            PropertySpec("end_time"),
            PropertySpec("data_set_id"),
            PropertySpec("labels", is_container=True),
        ]


class RelationshipList(CogniteResourceList[Relationship]):
    _RESOURCE = Relationship
