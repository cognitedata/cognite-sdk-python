from cognite.client.data_classes._base import (
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)


class TransformationSchedule(CogniteResource):
    def __init__(
        self,
        id=None,
        external_id=None,
        created_time=None,
        last_updated_time=None,
        interval=None,
        is_paused=False,
        cognite_client=None,
    ):
        self.id = id
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.interval = interval
        self.is_paused = is_paused
        self._cognite_client = cognite_client

    def __hash__(self):
        return hash(self.id)


class TransformationScheduleUpdate(CogniteUpdate):
    class _PrimitiveTransformationScheduleUpdate(CognitePrimitiveUpdate):
        def set(self, value):
            return self._set(value)

    @property
    def interval(self):
        return TransformationScheduleUpdate._PrimitiveTransformationScheduleUpdate(self, "interval")

    @property
    def is_paused(self):
        return TransformationScheduleUpdate._PrimitiveTransformationScheduleUpdate(self, "isPaused")


class TransformationScheduleList(CogniteResourceList):
    _RESOURCE = TransformationSchedule
