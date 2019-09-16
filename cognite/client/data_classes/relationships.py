import copy
from typing import *

from cognite.client.data_classes._base import *


# GenClass: relationshipResponse, relationship
class Relationship(CogniteResource):
    """No description.

    Args:
        source (Dict[str, Any]): Reference by external id to the source of the relationship. Since it is a reference by external id, the targeted resource may or may not exist in CDF.  If resource is `threeD` or `threeDRevision` the `resourceId` is a set of internal ids concatenated by a colons. Otherwise, the resourceId follows the formatting rules as described in `resourceId`.  If resource id of type `threeD`, the externalId must follow the pattern `<nodeId>:<modelId>:<revisionId>`. If resource id of type `threeDRevision`, the externalId must follow the pattern `<revisionId>:<modelId>`. The values `<nodeId>`, `<modelId>` and `<revisionId>` are the corresponding internal ids to identify the referenced resource uniquely.
        target (Dict[str, Any]): Reference by external id to the target of the relationship. Since it is a reference by external id, the targeted resource may or may not exist in CDF.  If resource is `threeD` or `threeDRevision` the `resourceId` is a set of internal ids concatenated by a colons. Otherwise, the resourceId follows the formatting rules as described in `resourceId`.  If resource id of type `threeD`, the externalId must follow the pattern `<nodeId>:<modelId>:<revisionId>`. If resource id of type `threeDRevision`, the externalId must follow the pattern `<revisionId>:<modelId>`. The values `<nodeId>`, `<modelId>` and `<revisionId>` are the corresponding internal ids to identify the referenced resource uniquely.
        start_time (float): Time when this relationship was established in milliseconds since Jan 1, 1970.
        end_time (float): Time when this relationship was ceased to exist in milliseconds since Jan 1, 1970.
        confidence (float): Confidence value of the existence of this relationship. Humans should enter 1.0 usually, generated relationships should provide a realistic score on the likelihood of the existence of the relationship. Generated relationships should never have the a confidence score of 1.0.
        data_set (str): String describing the source system storing or generating the relationship.
        external_id (str): Disallowing leading and trailing whitespaces. Case sensitive. The external Id must be unique within the project.
        relationship_type (str): Type of the relationship in order to distinguish between different relationships. In general relationship types should reflect references as they are expressed in natural sentences.  E.g. a flow through a pipe can be naturally represented by a `flowsTo`-relationship. On the other hand an alternative asset hierarchy can be represented with the `isParentOf`-relationship. The `implements`-relationship is intended to reflect references between a functional asset hierarchy and its implementation.
        created_time (float): Time when this relationship was created in CDF in milliseconds since Jan 1, 1970.
        last_updated_time (float): Time when this relationship was last updated in CDF in milliseconds since Jan 1, 1970.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        source: Dict[str, Any] = None,
        target: Dict[str, Any] = None,
        start_time: float = None,
        end_time: float = None,
        confidence: float = None,
        data_set: str = None,
        external_id: str = None,
        relationship_type: str = None,
        created_time: float = None,
        last_updated_time: float = None,
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

    # GenStop

    def _copy_resolve_targets(self):
        rel = copy.copy(self)
        rel.source = self._resolve_target(rel.source)
        rel.target = self._resolve_target(rel.target)
        return rel

    @staticmethod
    def _resolve_target(target):
        if isinstance(target, dict):
            return target

        from cognite.client.data_classes import Asset, Event, FileMetadata, TimeSeries

        _TARGET_TYPES = {Asset: "Asset", TimeSeries: "TimeSeries", FileMetadata: "File", Event: "Event"}
        typestr = _TARGET_TYPES.get(target.__class__)
        if typestr:
            return {"resource": typestr, "resourceId": target.external_id}
        raise ValueError("Invalid source or target '{}' of type {} in relationship".format(target, target.__class__))


class RelationshipUpdate(CogniteUpdate):
    pass


class RelationshipList(CogniteResourceList):
    _RESOURCE = Relationship
    _UPDATE = RelationshipUpdate
