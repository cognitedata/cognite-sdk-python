import copy
from typing import Dict, Type

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)
from cognite.client.data_classes.assets import Asset
from cognite.client.data_classes.events import Event
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.labels import Label, LabelFilter
from cognite.client.data_classes.sequences import Sequence
from cognite.client.data_classes.time_series import TimeSeries


class Relationship(CogniteResource):
    _RESOURCE_TYPES = frozenset({"asset", "timeseries", "file", "event", "sequence"})

    def __init__(
        self,
        external_id=None,
        source_external_id=None,
        source_type=None,
        source=None,
        target_external_id=None,
        target_type=None,
        target=None,
        start_time=None,
        end_time=None,
        confidence=None,
        data_set_id=None,
        labels=None,
        created_time=None,
        last_updated_time=None,
        cognite_client=None,
    ):
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
        self._cognite_client = cognite_client

    def _validate_resource_types(self):
        rel = copy.copy(self)
        self._validate_resource_type(rel.source_type)
        self._validate_resource_type(rel.target_type)
        return rel

    def _validate_resource_type(self, resource_type):
        if (resource_type is None) or (resource_type.lower() not in self._RESOURCE_TYPES):
            raise TypeError(f"Invalid source or target '{resource_type}' in relationship")

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = super()._load(resource, cognite_client)
        if instance.source is not None:
            instance.source = instance._convert_resource(instance.source, instance.source_type)
        if instance.target is not None:
            instance.target = instance._convert_resource(instance.target, instance.target_type)
        instance.labels = Label._load_list(instance.labels)
        return instance

    def _convert_resource(self, resource, resource_type):
        resource_map: Dict[(str, Type[CogniteResource])] = {
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
    def __init__(
        self,
        source_external_ids=None,
        source_types=None,
        target_external_ids=None,
        target_types=None,
        data_set_ids=None,
        start_time=None,
        end_time=None,
        confidence=None,
        last_updated_time=None,
        created_time=None,
        active_at_time=None,
        labels=None,
        cognite_client=None,
    ):
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
        self._cognite_client = cognite_client

    def dump(self, camel_case=False):
        result = super().dump(camel_case)
        if isinstance(self.labels, LabelFilter):
            result["labels"] = self.labels.dump(camel_case)
        return result


class RelationshipUpdate(CogniteUpdate):
    def __init__(self, external_id):
        self._id = None
        self._external_id = external_id
        self._update_object = {}

    class _PrimitiveRelationshipUpdate(CognitePrimitiveUpdate):
        def set(self, value):
            return self._set(value)

    class _LabelRelationshipUpdate(CogniteLabelUpdate):
        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    @property
    def source_external_id(self):
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "sourceExternalId")

    @property
    def source_type(self):
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "sourceType")

    @property
    def target_external_id(self):
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "targetExternalId")

    @property
    def target_type(self):
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "targetType")

    @property
    def start_time(self):
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "startTime")

    @property
    def end_time(self):
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "endTime")

    @property
    def data_set_id(self):
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "dataSetId")

    @property
    def confidence(self):
        return RelationshipUpdate._PrimitiveRelationshipUpdate(self, "confidence")

    @property
    def labels(self):
        return RelationshipUpdate._LabelRelationshipUpdate(self, "labels")


class RelationshipList(CogniteResourceList):
    _RESOURCE = Relationship
