import copy
from collections import defaultdict

from cognite.client.data_classes._base import *
from cognite.client.data_classes.assets import Asset
from cognite.client.data_classes.events import Event
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.labels import Label, LabelDefinition, LabelFilter
from cognite.client.data_classes.sequences import Sequence
from cognite.client.data_classes.time_series import TimeSeries


class Relationship(CogniteResource):
    """Representation of a relationship in CDF, consists of a source and a target and some additional parameters.

    Args:
        external_id (str): External id of the relationship, must be unique within the project.
        source_external_id (str): External id of the CDF resource that constitutes the relationship source.
        source_type (str): The CDF resource type of the relationship source. Must be one of the specified values.
        source (Union[Asset, TimeSeries, FileMetadata, Event, Sequence, Dict]): The full resource referenced by the source_external_id and source_type fields.
        target_external_id (str): External id of the CDF resource that constitutes the relationship target.
        target_type (str): The CDF resource type of the relationship target. Must be one of the specified values.
        target (Union[Asset, TimeSeries, FileMetadata, Event, Sequence, Dict]): The full resource referenced by the target_external_id and target_type fields.
        start_time (int): Time, in milliseconds since Jan. 1, 1970, when the relationship became active. If there is no startTime, relationship is active from the beginning of time until endTime.
        end_time (int): Time, in milliseconds since Jan. 1, 1970, when the relationship became inactive. If there is no endTime, relationship is active from startTime until the present or any point in the future. If endTime and startTime are set, then endTime must be strictly greater than startTime.
        confidence (float): Confidence value of the existence of this relationship. Generated relationships should provide a realistic score on the likelihood of the existence of the relationship. Relationships without a confidence value can be interpreted at the discretion of each project.
        data_set_id (int): The id of the dataset this relationship belongs to.
        labels (List[Label]): A list of the labels associated with this resource item.
        created_time (int): Time, in milliseconds since Jan. 1, 1970, when this relationship was created in CDF.
        last_updated_time (int): Time, in milliseconds since Jan. 1, 1970, when this relationship was last updated in CDF.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        source_external_id: str = None,
        source_type: str = None,
        source: Union[Asset, TimeSeries, FileMetadata, Sequence, Event, Dict] = None,
        target_external_id: str = None,
        target_type: str = None,
        target: Union[Asset, TimeSeries, FileMetadata, Sequence, Event, Dict] = None,
        start_time: int = None,
        end_time: int = None,
        confidence: float = None,
        data_set_id: int = None,
        labels: List[Union[Label, str, LabelDefinition]] = None,
        created_time: int = None,
        last_updated_time: int = None,
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

    @staticmethod
    def _validate_resource_type(resource_type):
        _RESOURCE_TYPES = {"asset", "timeseries", "file", "event", "sequence", "geospatial"}
        if resource_type.lower() not in _RESOURCE_TYPES:
            raise TypeError("Invalid source or target '{}' in relationship".format(resource_type))

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(Relationship, cls)._load(resource, cognite_client)
        if instance.source is not None:
            instance.source = instance._convert_resource(instance.source, instance.source_type)
        if instance.target is not None:
            instance.target = instance._convert_resource(instance.target, instance.target_type)
        instance.labels = Label._load_list(instance.labels)
        return instance

    def _convert_resource(self, resource: Dict, resource_type: str):
        resource_map = {
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
    """Filter on relationships with exact match. Multiple filter elments in one property, e.g. `sourceExternalIds: [ "a", "b" ]`, will return all relationships where the `sourceExternalId` field is either `a` or `b`. Filters in multiple properties will return the relationships that match all criteria. If the filter is not specified it default to an empty filter.

    Args:
        source_external_ids (List[str]): Include relationships that have any of these values in their `sourceExternalId` field
        source_types (List[str]): Include relationships that have any of these values in their `sourceType` field
        target_external_ids (List[str]): Include relationships that have any of these values in their `targetExternalId` field
        target_types (List[str]): Include relationships that have any of these values in their `targetType` field
        data_set_ids (List[Dict[str, Any]]): Either one of `internalId` (int) or `externalId` (str)
        start_time (Dict[str, int]): Range between two timestamps, minimum and maximum milli seconds (inclusive)
        end_time (Dict[str, int]): Range between two timestamps, minimum and maximum milli seconds (inclusive)
        confidence (Dict[str, int]): Range to filter the field for. (inclusive)
        last_updated_time (Dict[str, Any]): Range to filter the field for. (inclusive)
        created_time (Dict[str, int]): Range to filter the field for. (inclusive)
        active_at_time (Dict[str, int]): Limits results to those active at any point within the given time range, i.e. if there is any overlap in the intervals [activeAtTime.min, activeAtTime.max] and [startTime, endTime], where both intervals are inclusive. If a relationship does not have a startTime, it is regarded as active from the begining of time by this filter. If it does not have an endTime is will be regarded as active until the end of time. Similarly, if a min is not supplied to the filter, the min will be implicitly set to the beginning of time, and if a max is not supplied, the max will be implicitly set to the end of time.
        labels (LabelFilter): Return only the resource matching the specified label constraints.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        source_external_ids: List[str] = None,
        source_types: List[str] = None,
        target_external_ids: List[str] = None,
        target_types: List[str] = None,
        data_set_ids: List[Dict[str, Any]] = None,
        start_time: Dict[str, int] = None,
        end_time: Dict[str, int] = None,
        confidence: Dict[str, int] = None,
        last_updated_time: Dict[str, int] = None,
        created_time: Dict[str, int] = None,
        active_at_time: Dict[str, int] = None,
        labels: LabelFilter = None,
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

    def dump(self, camel_case: bool = False):
        result = super(RelationshipFilter, self).dump(camel_case)
        if isinstance(self.labels, LabelFilter):
            result["labels"] = self.labels.dump(camel_case)
        return result


class RelationshipUpdate(CogniteUpdate):
    """Update applied to a single relationship

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.

    """

    # Relationships have only an external_id and do not expose id
    def __init__(self, external_id: str):
        self._id = None
        self._external_id = external_id
        self._update_object = {}

    class _PrimitiveRelationshipUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "RelationshipUpdate":
            return self._set(value)

    class _LabelRelationshipUpdate(CogniteLabelUpdate):
        def add(self, value: Union[str, List[str]]) -> "RelationshipUpdate":
            return self._add(value)

        def remove(self, value: Union[str, List[str]]) -> "RelationshipUpdate":
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
    _UPDATE = RelationshipUpdate
