import copy
from typing import *

from cognite.client.data_classes._base import *


# GenClass: GetSequenceDTO
class Relationship(CogniteResource):
    def __init__(
        self,
        external_id: str = None,
        source: str = None,
        target: int = None,
        start_time: int = None,
        end_time: int = None,
        confidence: float = None,
        data_set: str = None,
        relationship_type: str = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.source = source
        self.target = target
        self.start_time = start_time
        self.end_time = end_time
        self.confidence = confidence
        self.data_set = data_set
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
