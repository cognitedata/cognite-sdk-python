import copy
from typing import *

from cognite.client.data_classes._base import *


class Relationship(CogniteResource):
    """Representation of a relationship in CDF, consists of a source and a target and some additional parameters.

    Args:
        source (Dict[str, Any]): Reference by external id to the source of the relationship. Since it is a reference by external id, the targeted resource may or may not exist in CDF.  If resource is `threeD` or `threeDRevision` the `resourceId` is a set of internal ids concatenated by a colons. Otherwise, the resourceId follows the formatting rules as described in `resourceId`.  If resource id of type `threeD`, the externalId must follow the pattern `<nodeId>:<modelId>:<revisionId>`. If resource id of type `threeDRevision`, the externalId must follow the pattern `<revisionId>:<modelId>`. The values `<nodeId>`, `<modelId>` and `<revisionId>` are the corresponding internal ids to identify the referenced resource uniquely.
        target (Dict[str, Any]): Reference by external id to the target of the relationship. Since it is a reference by external id, the targeted resource may or may not exist in CDF.  If resource is `threeD` or `threeDRevision` the `resourceId` is a set of internal ids concatenated by a colons. Otherwise, the resourceId follows the formatting rules as described in `resourceId`.  If resource id of type `threeD`, the externalId must follow the pattern `<nodeId>:<modelId>:<revisionId>`. If resource id of type `threeDRevision`, the externalId must follow the pattern `<revisionId>:<modelId>`. The values `<nodeId>`, `<modelId>` and `<revisionId>` are the corresponding internal ids to identify the referenced resource uniquely.
        start_time (int): Time, in milliseconds since Jan. 1, 1970, when relationship became active. If there is no startTime, relationship is active from the beginning of time until endTime.
        end_time (int): Time, in milliseconds since Jan. 1, 1970,  when relationship became inactive. If there is no endTime, relationship is active from startTime until the present or any point in the future. If endTime and startTime are set, then endTime must be strictly greater than startTime
        confidence (float): Confidence value of the existence of this relationship. Humans should enter 1.0 usually, generated relationships should provide a realistic score on the likelihood of the existence of the relationship. Generated relationships should never have the a confidence score of 1.0.
        data_set (str): String describing the source system storing or generating the relationship.
        external_id (str): Disallowing leading and trailing whitespaces. Case sensitive. The external Id must be unique within the project.
        relationship_type (str): Type of the relationship in order to distinguish between different relationships. In general relationship types should reflect references as they are expressed in natural sentences.  E.g. a flow through a pipe can be naturally represented by a `flowsTo`-relationship. On the other hand an alternative asset hierarchy can be represented with the `isParentOf`-relationship. The `implements`-relationship is intended to reflect references between a functional asset hierarchy and its implementation.
        created_time (int): Time, in milliseconds since Jan. 1, 1970, when this relationship was created in CDF.
        last_updated_time (int): Time, in milliseconds since Jan. 1, 1970, when this relationship was last updated in CDF.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        source: Dict[str, Any] = None,
        target: Dict[str, Any] = None,
        start_time: int = None,
        end_time: int = None,
        confidence: float = None,
        data_set: str = None,
        external_id: str = None,
        relationship_type: str = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client=None,
    ):
        self.source = source
        self.target = target
        self.start_time = start_time
        self.end_time = end_time
        self.confidence = confidence
        self.data_set = data_set
        self.external_id = external_id
        self.relationship_type = relationship_type
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    def _copy_resolve_targets(self):
        rel = copy.copy(self)
        rel.source = self._resolve_target(rel.source)
        rel.target = self._resolve_target(rel.target)
        return rel

    @staticmethod
    def _resolve_target(target):
        if isinstance(target, dict) or target is None:
            return target

        from cognite.client.data_classes import Asset, Event, FileMetadata, TimeSeries, Sequence

        _TARGET_TYPES = {
            Asset: "Asset",
            TimeSeries: "TimeSeries",
            FileMetadata: "File",
            Event: "Event",
            Sequence: "Sequence",
        }
        typestr = _TARGET_TYPES.get(target.__class__)
        if typestr:
            return {"resource": typestr, "resourceId": target.external_id}
        raise ValueError("Invalid source or target '{}' of type {} in relationship".format(target, target.__class__))


class RelationshipFilter(CogniteFilter):
    """Filter on relationships with exact match. Multiple filter elments in one property, e.g. `dataSets: [ "a", "b" ]`, will return all relationships where the dataSet field is either `a` or `b`. Filters in multiple properties will return the relationships that match all criteria. Filters on a `resourceId` without a `resource` (type) in sources and targets will return relationships that match the resourceId and match any resource type. If the filter is not specified it default to an empty filter.

    Args:
        sources (List[Dict[str, Any]]): Include relationships that have any of these values in their `source` field
        targets (List[Dict[str, Any]]): Include relationships that have any of these values in their `target` field
        relationship_types (List[str]): Include relationships that have any of these values in their `relationshipType` field
        data_sets (List[str]): Include relationships that have any of these values in their `dataSet` field
        start_time (Dict[str, Any]): Range to filter the field for. (inclusive)
        end_time (Dict[str, Any]): Range to filter the field for. (inclusive)
        confidence (Dict[str, Any]): Range to filter the field for. (inclusive)
        last_updated_time (Dict[str, Any]): Range to filter the field for. (inclusive)
        created_time (Dict[str, Any]): Range to filter the field for. (inclusive)
        active_at_time (int): Limits results to those active at this time, i.e. `activeAtTime` falls between `startTime` and `endTime` `startTime` is treated as inclusive (if `activeAtTime` is equal to `startTime` then the relationship will be included). `endTime` is treated as exclusive (if `activeTime` is equal to `endTime` then the relationship will NOT be included). If a relationship has neither `startTime` nor `endTime`, the relationship is active at all times
        source_resource (str): Resource type of the source node.
        source_resource_id (str): Resource ID of the source node.
        target_resource (str): Resource type of the target node.
        target_resource_id (str): Resource ID of the target node.
        data_set (str): String describing the source system storing or generating the relationship.
        relationship_type (str): Type of the relationship in order to distinguish between different relationships. In general relationship types should reflect references as they are expressed in natural sentences.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        sources: List[Dict[str, Any]] = None,
        targets: List[Dict[str, Any]] = None,
        relationship_types: List[str] = None,
        data_sets: List[str] = None,
        start_time: Dict[str, Any] = None,
        end_time: Dict[str, Any] = None,
        confidence: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        created_time: Dict[str, Any] = None,
        active_at_time: int = None,
        source_resource: str = None,
        source_resource_id: str = None,
        target_resource: str = None,
        target_resource_id: str = None,
        data_set: str = None,
        relationship_type: str = None,
        cognite_client=None,
    ):
        self.sources = sources
        self.targets = targets
        self.relationship_types = relationship_types
        self.data_sets = data_sets
        self.start_time = start_time
        self.end_time = end_time
        self.confidence = confidence
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self.active_at_time = active_at_time
        self.source_resource = source_resource
        self.source_resource_id = source_resource_id
        self.target_resource = target_resource
        self.target_resource_id = target_resource_id
        self.data_set = data_set
        self.relationship_type = relationship_type
        self._cognite_client = cognite_client


class RelationshipUpdate(CogniteUpdate):
    pass


class RelationshipList(CogniteResourceList):
    _RESOURCE = Relationship
    _UPDATE = RelationshipUpdate
